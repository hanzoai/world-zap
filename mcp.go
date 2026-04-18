package main

// MCP (Model Context Protocol) HTTP handler.
//
// Streamable-HTTP transport: single endpoint that accepts JSON-RPC 2.0
// POST requests and responds with either application/json or text/event-stream.
// Spec: https://modelcontextprotocol.io/specification/2024-11-05/basic/transports
//
// Clients connect with:
//
//	POST https://mcp.world.hanzo.ai/mcp
//	Authorization: Bearer <IAM_TOKEN>
//	{"jsonrpc":"2.0","id":1,"method":"initialize", ...}
//
// Supported methods: initialize, tools/list, tools/call, ping.
// Tools proxy to the worldmonitor backend /v1/world/* endpoints.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hanzoai/world-zap/auth"
)

const (
	mcpProtocolVersion = "2024-11-05"
	mcpServerName      = "hanzo-world"
	mcpServerVersion   = version // from main.go
)

// jsonrpcRequest is a minimal JSON-RPC 2.0 request.
type jsonrpcRequest struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonrpcResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// mcpTool describes one MCP tool exposed to agents.
type mcpTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// mcpContent is the common content block type for tool results.
type mcpContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type mcpToolResult struct {
	Content []mcpContent `json:"content"`
	IsError bool         `json:"isError,omitempty"`
}

// mcpHandler wires tools to an HTTP endpoint.
type mcpHandler struct {
	deps    *serverDeps
	tools   []mcpTool
	backend string
	client  *http.Client
}

func newMCPHandler(d *serverDeps) *mcpHandler {
	return &mcpHandler{
		deps:    d,
		tools:   mcpToolCatalog,
		backend: strings.TrimRight(d.cfg.BackendBase, "/"),
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

func (h *mcpHandler) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// Optional SSE stream for server→client notifications. We don't
		// push unsolicited events today; return a simple heartbeat stream
		// so clients that GET /mcp first don't blow up.
		sseHeartbeat(w, r)
	case http.MethodPost:
		h.handleJSONRPC(w, r)
	case http.MethodOptions:
		corsPreflight(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *mcpHandler) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	// Auth (skip for initialize + ping so agents can probe)
	token := auth.ExtractToken(r)
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MiB
	if err != nil {
		writeJSONRPCError(w, nil, -32700, "parse error: "+err.Error())
		return
	}
	var req jsonrpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONRPCError(w, nil, -32700, "invalid json")
		return
	}

	switch req.Method {
	case "initialize":
		writeJSONRPCResult(w, req.ID, map[string]any{
			"protocolVersion": mcpProtocolVersion,
			"serverInfo":      map[string]any{"name": mcpServerName, "version": mcpServerVersion},
			"capabilities": map[string]any{
				"tools": map[string]any{"listChanged": false},
			},
		})
	case "ping":
		writeJSONRPCResult(w, req.ID, map[string]any{})
	case "tools/list":
		if err := h.authenticate(r.Context(), token); err != nil {
			writeJSONRPCError(w, req.ID, -32001, "unauthenticated: "+err.Error())
			return
		}
		writeJSONRPCResult(w, req.ID, map[string]any{"tools": h.tools})
	case "tools/call":
		principal, err := h.authenticatePrincipal(r.Context(), token)
		if err != nil {
			writeJSONRPCError(w, req.ID, -32001, "unauthenticated: "+err.Error())
			return
		}
		var call struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &call); err != nil {
			writeJSONRPCError(w, req.ID, -32602, "invalid params")
			return
		}
		// Rate limit by plan (reuse the same token bucket used for ZAP).
		decision := h.deps.limiter.TryAcquire(principal.UserID, principal.Plan)
		if !decision.Allowed {
			writeJSONRPCError(w, req.ID, -32005, fmt.Sprintf("rate_limited retry_in=%s", decision.RetryIn))
			return
		}
		result, err := h.dispatchTool(r.Context(), token, call.Name, call.Arguments)
		if err != nil {
			writeJSONRPCResult(w, req.ID, mcpToolResult{
				Content: []mcpContent{{Type: "text", Text: err.Error()}},
				IsError: true,
			})
			return
		}
		writeJSONRPCResult(w, req.ID, result)
	case "notifications/initialized":
		// No-op notification.
		w.WriteHeader(http.StatusAccepted)
	default:
		writeJSONRPCError(w, req.ID, -32601, "method not found: "+req.Method)
	}
}

// authenticate validates the bearer token and returns nothing on success.
func (h *mcpHandler) authenticate(ctx context.Context, token string) error {
	_, err := h.authenticatePrincipal(ctx, token)
	return err
}

// authenticatePrincipal returns the IAM-resolved principal.
func (h *mcpHandler) authenticatePrincipal(ctx context.Context, token string) (*auth.Principal, error) {
	if token == "" {
		return nil, fmt.Errorf("missing token")
	}
	p, err := h.deps.auth.Validate(ctx, token)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// dispatchTool maps a tool name + args to a backend call.
func (h *mcpHandler) dispatchTool(ctx context.Context, token, name string, args map[string]any) (mcpToolResult, error) {
	path, err := routeTool(name, args)
	if err != nil {
		return mcpToolResult{}, err
	}

	// Build request to worldmonitor backend
	reqURL := h.backend + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return mcpToolResult{}, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return mcpToolResult{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20)) // 4 MiB cap

	if resp.StatusCode >= 400 {
		return mcpToolResult{
			Content: []mcpContent{{Type: "text", Text: fmt.Sprintf("upstream %d: %s", resp.StatusCode, string(body))}},
			IsError: true,
		}, nil
	}
	// Return the raw JSON body as a text block. Agents can parse.
	return mcpToolResult{
		Content: []mcpContent{{Type: "text", Text: string(body)}},
	}, nil
}

// routeTool maps a tool name to a /v1/world/* query path with arguments.
func routeTool(name string, args map[string]any) (string, error) {
	q := url.Values{}
	add := func(k string) {
		if v, ok := args[k]; ok {
			q.Set(k, fmt.Sprint(v))
		}
	}
	switch name {
	case "get_events":
		add("region")
		add("layers")
		add("since")
		add("limit")
		return "/v1/world/events?" + q.Encode(), nil
	case "query_conflicts":
		add("country")
		add("since")
		add("severity")
		return "/v1/world/conflicts?" + q.Encode(), nil
	case "query_infrastructure":
		add("type")
		add("near")
		return "/v1/world/infra?" + q.Encode(), nil
	case "track_vessel":
		add("mmsi")
		add("imo")
		add("name")
		return "/v1/world/vessel?" + q.Encode(), nil
	case "list_live_news":
		add("category")
		add("limit")
		return "/v1/world/news?" + q.Encode(), nil
	case "get_markets":
		add("ticker")
		add("category")
		return "/v1/world/markets?" + q.Encode(), nil
	case "list_feeds":
		return "/v1/world/feeds", nil
	case "ask_analyst":
		// POST passthrough; we encode question into a POST body via a
		// secondary path. Keep GET semantics simple — proxy to chat with the
		// question as a system input.
		add("question")
		return "/v1/world/analyst?" + q.Encode(), nil
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

// mcpToolCatalog describes the 8 tools exposed to agents. Input schemas are
// minimal — the backend does the real validation.
var mcpToolCatalog = []mcpTool{
	{
		Name:        "get_events",
		Description: "Fetch current global events from OSINT feeds, optionally filtered by region or layer.",
		InputSchema: schemaObject(map[string]any{
			"region": schemaString("Geographic region hint (e.g. 'Middle East')"),
			"layers": schemaString("Comma-separated layer names (e.g. 'conflicts,fires')"),
			"since":  schemaString("ISO-8601 timestamp — events after this moment"),
			"limit":  schemaNumber("Max events to return (default 50)"),
		}, nil),
	},
	{
		Name:        "query_conflicts",
		Description: "Look up recent conflict/violence events (ACLED-style).",
		InputSchema: schemaObject(map[string]any{
			"country":  schemaString("ISO country code"),
			"since":    schemaString("ISO-8601 timestamp"),
			"severity": schemaString("minor|major|critical"),
		}, nil),
	},
	{
		Name:        "query_infrastructure",
		Description: "Query critical infrastructure (undersea cables, military bases, nuclear sites, power grids).",
		InputSchema: schemaObject(map[string]any{
			"type": schemaString("cable|base|nuclear|power|telecom"),
			"near": schemaString("lat,lon,radiusKm (e.g. '48.85,2.35,50')"),
		}, []string{"type"}),
	},
	{
		Name:        "track_vessel",
		Description: "Track a maritime vessel by MMSI, IMO, or name.",
		InputSchema: schemaObject(map[string]any{
			"mmsi": schemaString("9-digit MMSI"),
			"imo":  schemaString("7-digit IMO"),
			"name": schemaString("vessel name"),
		}, nil),
	},
	{
		Name:        "list_live_news",
		Description: "Aggregate live news items from configured sources.",
		InputSchema: schemaObject(map[string]any{
			"category": schemaString("world|markets|tech|finance|happy"),
			"limit":    schemaNumber("Max items (default 20)"),
		}, nil),
	},
	{
		Name:        "get_markets",
		Description: "Fetch market quotes — equities, fx, commodities, crypto.",
		InputSchema: schemaObject(map[string]any{
			"ticker":   schemaString("Single ticker symbol"),
			"category": schemaString("equities|fx|commodities|crypto"),
		}, nil),
	},
	{
		Name:        "list_feeds",
		Description: "List all feed channels the caller may subscribe to (ZAP topics, MCP tools).",
		InputSchema: schemaObject(nil, nil),
	},
	{
		Name:        "ask_analyst",
		Description: "Ask the Zen AI analyst a grounded question about the current map context.",
		InputSchema: schemaObject(map[string]any{
			"question": schemaString("Natural-language question"),
		}, []string{"question"}),
	},
}

// schemaObject builds a minimal JSON Schema for a tool's input.
func schemaObject(props map[string]any, required []string) map[string]any {
	if props == nil {
		props = map[string]any{}
	}
	m := map[string]any{
		"type":       "object",
		"properties": props,
	}
	if len(required) > 0 {
		m["required"] = required
	}
	return m
}

func schemaString(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
func schemaNumber(desc string) map[string]any { return map[string]any{"type": "number", "description": desc} }

func writeJSONRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	resp := jsonrpcResponse{Jsonrpc: "2.0", ID: id, Result: result}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func writeJSONRPCError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	resp := jsonrpcResponse{Jsonrpc: "2.0", ID: id, Error: &jsonrpcError{Code: code, Message: msg}}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// sseHeartbeat serves a no-op SSE stream used by clients that GET /mcp first.
func sseHeartbeat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		}
	}
}

func corsPreflight(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Mcp-Session-Id")
	w.WriteHeader(http.StatusNoContent)
}

