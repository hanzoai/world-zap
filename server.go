package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/hanzoai/world-zap/auth"
	"github.com/hanzoai/world-zap/handlers"
	"github.com/hanzoai/world-zap/hub"
	"github.com/hanzoai/world-zap/ratelimit"
)

// serverDeps bundles everything the HTTP handlers need.
type serverDeps struct {
	cfg     Config
	logger  *slog.Logger
	auth    *auth.Validator
	hub     *hub.Hub
	limiter *ratelimit.Limiter
	backend BackendHealthChecker
	ready   *atomicBool
}

// BackendHealthChecker is satisfied by any object exposing Healthy(ctx).
type BackendHealthChecker interface {
	Healthy(ctx context.Context) bool
}

type atomicBool struct {
	mu sync.RWMutex
	v  bool
}

func (a *atomicBool) set(v bool) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomicBool) get() bool  { a.mu.RLock(); defer a.mu.RUnlock(); return a.v }

func newMux(d *serverDeps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !d.ready.get() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		ok := d.backend == nil || d.backend.Healthy(ctx)
		status := http.StatusOK
		body := map[string]any{"status": "ready"}
		if !ok {
			status = http.StatusServiceUnavailable
			body["status"] = "backend_unreachable"
		}
		writeJSON(w, status, body)
	})
	mux.HandleFunc("/metrics/topics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, d.hub.TopicStats())
	})
	mux.HandleFunc("/zap", func(w http.ResponseWriter, r *http.Request) {
		zapHandler(d, w, r)
	})
	// MCP (Model Context Protocol) — same binary, same auth, same tools.
	mh := newMCPHandler(d)
	mux.HandleFunc("/mcp", mh.serveHTTP)
	mux.HandleFunc("/mcp/", mh.serveHTTP)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			writeJSON(w, http.StatusOK, map[string]any{
				"service": "world-gw",
				"version": version,
				"endpoints": map[string]any{
					"zap": "wss://zap.world.hanzo.ai/zap",
					"mcp": "https://mcp.world.hanzo.ai/mcp",
				},
				"topics": hub.TopicNames(),
				"tools":  mcpToolNames(),
			})
			return
		}
		http.NotFound(w, r)
	})
	return mux
}

func mcpToolNames() []string {
	out := make([]string, 0, len(mcpToolCatalog))
	for _, t := range mcpToolCatalog {
		out = append(out, t.Name)
	}
	return out
}

// zapHandler upgrades the HTTP request to a websocket and delegates to a
// Session.
func zapHandler(d *serverDeps, w http.ResponseWriter, r *http.Request) {
	token := auth.ExtractToken(r)
	if token == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "missing_token"})
		return
	}
	p, err := d.auth.Validate(r.Context(), token)
	if err != nil {
		status := http.StatusUnauthorized
		if errors.Is(err, auth.ErrUpstream) {
			status = http.StatusBadGateway
		}
		writeJSON(w, status, map[string]any{"error": err.Error()})
		return
	}

	up := websocket.Upgrader{
		ReadBufferSize:  1 << 15,
		WriteBufferSize: 1 << 15,
		CheckOrigin: func(req *http.Request) bool {
			// Accept any origin — downstream is behind ingress and token-auth.
			// Ingress strips origin spoofing at the cluster edge.
			_ = req
			return true
		},
	}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade writes its own error response.
		return
	}
	conn.SetReadLimit(1 << 22) // 4 MiB ceiling, matches proto.MaxPayload

	d.logger.Info("zap: session open",
		"user_id", p.UserID, "org", p.Org, "plan", p.Plan, "remote", clientIP(r))

	sess := handlers.New(conn, p, handlers.Deps{
		Hub:       d.hub,
		Auth:      d.auth,
		Limiter:   d.limiter,
		Logger:    d.logger,
		WriteWait: 10 * time.Second,
		PingEvery: 30 * time.Second,
	})
	sess.Run(r.Context())
	d.logger.Info("zap: session closed", "user_id", p.UserID)
}

func clientIP(r *http.Request) string {
	if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
		if i := strings.IndexByte(xf, ','); i >= 0 {
			return strings.TrimSpace(xf[:i])
		}
		return strings.TrimSpace(xf)
	}
	return r.RemoteAddr
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// backendHealthClient implements BackendHealthChecker against an HTTP URL.
type backendHealthClient struct {
	base   string
	client *http.Client
}

func newBackendHealthClient(base string) *backendHealthClient {
	return &backendHealthClient{
		base:   strings.TrimRight(base, "/"),
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

func (b *backendHealthClient) Healthy(ctx context.Context) bool {
	if b.base == "" {
		return true
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.base+"/healthz", nil)
	if err != nil {
		return false
	}
	res, err := b.client.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	return res.StatusCode >= 200 && res.StatusCode < 300
}

