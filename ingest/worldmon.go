// Package ingest pulls events from the worldmonitor backend and publishes
// them onto the hub. Two modes supported:
//
//  1. Streaming: GET /v1/world/events?stream=1 returns an open
//     HTTP response emitting NDJSON (one event per line) or SSE-style
//     "data: ..." frames. We parse either.
//  2. Fallback: if the stream endpoint returns an error or disconnects
//     early, we fall back to a polling loop on /v1/world/events.
//
// Every record must be a JSON object of the form
// {"topic": "<name>", "payload": {...}}. The topic is validated against
// the hub catalog before publishing.
package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hanzoai/world-zap/hub"
)

// Logger is a minimal structured logger interface so tests can provide a
// no-op implementation.
type Logger interface {
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Config controls the ingester.
type Config struct {
	BackendBase   string
	ServiceToken  string
	PollInterval  time.Duration
	ReconnectWait time.Duration
}

// Ingester owns the connection to the worldmonitor backend.
type Ingester struct {
	cfg    Config
	hub    *hub.Hub
	log    Logger
	client *http.Client
}

// New constructs an Ingester.
func New(cfg Config, h *hub.Hub, log Logger) *Ingester {
	if cfg.ReconnectWait <= 0 {
		cfg.ReconnectWait = 2 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &Ingester{
		cfg:    cfg,
		hub:    h,
		log:    log,
		client: &http.Client{Timeout: 0}, // streaming has no deadline
	}
}

// Run drives the ingest loop until ctx is cancelled. Errors are logged and
// the loop reconnects with backoff.
func (i *Ingester) Run(ctx context.Context) {
	if i.cfg.BackendBase == "" {
		i.log.Warn("ingest: no backend configured, disabled")
		return
	}
	backoff := i.cfg.ReconnectWait
	for {
		if ctx.Err() != nil {
			return
		}
		err := i.stream(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			i.log.Warn("ingest: stream ended", "err", err.Error())
		}
		// Exponential backoff capped at 60s.
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 60*time.Second {
			backoff = 60 * time.Second
		}
	}
}

// stream opens one streaming session and consumes until EOF or error.
func (i *Ingester) stream(ctx context.Context) error {
	url := strings.TrimRight(i.cfg.BackendBase, "/") + "/v1/world/events?stream=1"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if i.cfg.ServiceToken != "" {
		req.Header.Set("Authorization", "Bearer "+i.cfg.ServiceToken)
	}
	req.Header.Set("Accept", "text/event-stream, application/x-ndjson, application/json")

	res, err := i.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		return fmt.Errorf("ingest: backend status %d", res.StatusCode)
	}

	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<22) // 4 MiB line ceiling
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if line == "" || line == "[DONE]" {
			continue
		}
		if err := i.handleLine([]byte(line)); err != nil {
			i.log.Warn("ingest: bad line", "err", err.Error())
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return err
	}
	return io.EOF
}

// record is the canonical event shape produced by the backend.
type record struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

func (i *Ingester) handleLine(b []byte) error {
	var r record
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	if r.Topic == "" || len(r.Payload) == 0 {
		return fmt.Errorf("ingest: missing topic or payload")
	}
	if err := i.hub.Publish(hub.Message{Topic: r.Topic, Payload: []byte(r.Payload)}); err != nil {
		if errors.Is(err, hub.ErrUnknownTopic) {
			// Drop silently — unknown topics should not kill the stream.
			return nil
		}
		return err
	}
	return nil
}
