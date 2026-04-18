package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/hanzoai/world-zap/auth"
	"github.com/hanzoai/world-zap/hub"
	"github.com/hanzoai/world-zap/proto"
	"github.com/hanzoai/world-zap/ratelimit"
)

func mustUpgrade(t *testing.T, w http.ResponseWriter, r *http.Request) *websocket.Conn {
	t.Helper()
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	c, err := up.Upgrade(w, r, nil)
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	return c
}

func startSessionServer(t *testing.T, p auth.Principal, h *hub.Hub) (*httptest.Server, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	deps := Deps{
		Hub:       h,
		Auth:      auth.New(auth.Config{Endpoint: "http://unused"}),
		Limiter:   ratelimit.New(),
		Logger:    slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		WriteWait: 2 * time.Second,
		PingEvery: time.Hour,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn := mustUpgrade(t, w, r)
		sess := New(conn, p, deps)
		go sess.Run(ctx)
	}))
	return srv, cancel
}

func dial(t *testing.T, base string) *websocket.Conn {
	t.Helper()
	u, _ := url.Parse(base)
	u.Scheme = "ws"
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}

func readFrame(t *testing.T, c *websocket.Conn) proto.Frame {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, b, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	f, _, err := proto.Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return f
}

func writeFrame(t *testing.T, c *websocket.Conn, typ byte, v any) {
	t.Helper()
	b, err := proto.EncodeJSON(typ, v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := c.WriteMessage(websocket.BinaryMessage, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestSessionInitAck(t *testing.T) {
	h := hub.New(8)
	srv, cancel := startSessionServer(t, auth.Principal{UserID: "u", Org: "hanzo", Plan: "pro"}, h)
	defer srv.Close()
	defer cancel()
	c := dial(t, srv.URL)
	defer c.Close()

	f := readFrame(t, c)
	if f.Type != proto.TypeINITACK {
		t.Fatalf("want INIT_ACK got %s", proto.TypeName(f.Type))
	}
	var body map[string]any
	_ = json.Unmarshal(f.Payload, &body)
	if body["plan"] != "pro" {
		t.Fatalf("plan: %v", body["plan"])
	}
}

func TestSessionListTopics(t *testing.T) {
	h := hub.New(8)
	srv, cancel := startSessionServer(t, auth.Principal{UserID: "u", Org: "hanzo", Plan: "team"}, h)
	defer srv.Close()
	defer cancel()
	c := dial(t, srv.URL)
	defer c.Close()
	_ = readFrame(t, c) // INIT_ACK

	writeFrame(t, c, proto.TypeCallTool, map[string]any{
		"id":   "1",
		"name": "list_topics",
		"args": map[string]any{},
	})
	f := readFrame(t, c)
	if f.Type != proto.TypeRESOLVE {
		t.Fatalf("want RESOLVE got %s", proto.TypeName(f.Type))
	}
	var resp struct {
		ID      string          `json:"id"`
		Error   bool            `json:"error"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(f.Payload, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error || resp.ID != "1" {
		t.Fatalf("unexpected resp: %+v", resp)
	}
	if !strings.Contains(string(resp.Content), "world.events.earthquakes") {
		t.Fatalf("topic list missing earthquakes: %s", resp.Content)
	}
}

func TestSessionSubscribeAndReceivePush(t *testing.T) {
	h := hub.New(8)
	srv, cancel := startSessionServer(t, auth.Principal{UserID: "u", Org: "hanzo", Plan: "pro"}, h)
	defer srv.Close()
	defer cancel()
	c := dial(t, srv.URL)
	defer c.Close()
	_ = readFrame(t, c) // INIT_ACK

	writeFrame(t, c, proto.TypeCallTool, map[string]any{
		"id":   "sub1",
		"name": "subscribe",
		"args": map[string]any{"topic": "world.events.earthquakes"},
	})
	resolve := readFrame(t, c)
	if resolve.Type != proto.TypeRESOLVE {
		t.Fatalf("expected RESOLVE, got %s", proto.TypeName(resolve.Type))
	}

	// Publish through the hub directly; session should forward.
	_ = h.Publish(hub.Message{
		Topic:   "world.events.earthquakes",
		Payload: []byte(`{"id":"usgs:1","mag":4.2}`),
	})

	// pumpSubs polls every 100ms, so we allow up to a second.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, b, err := c.ReadMessage()
		if err != nil {
			continue
		}
		f, _, err := proto.Decode(b)
		if err != nil {
			continue
		}
		if f.Type == proto.TypePUSH {
			if !strings.Contains(string(f.Payload), "usgs:1") {
				t.Fatalf("unexpected push payload: %s", f.Payload)
			}
			return
		}
	}
	t.Fatalf("no PUSH received in time")
}

func TestSessionSubscribePlanTooLow(t *testing.T) {
	h := hub.New(8)
	srv, cancel := startSessionServer(t, auth.Principal{UserID: "u", Org: "acme", Plan: "free"}, h)
	defer srv.Close()
	defer cancel()
	c := dial(t, srv.URL)
	defer c.Close()
	_ = readFrame(t, c) // INIT_ACK

	writeFrame(t, c, proto.TypeCallTool, map[string]any{
		"id":   "x",
		"name": "subscribe",
		"args": map[string]any{"topic": "world.ships.ais"}, // requires team
	})
	f := readFrame(t, c)
	if f.Type != proto.TypeRESOLVE {
		t.Fatalf("want RESOLVE got %s", proto.TypeName(f.Type))
	}
	if !strings.Contains(string(f.Payload), "plan_too_low") {
		t.Fatalf("expected plan_too_low error: %s", f.Payload)
	}
}

func TestSessionAdminCanPublish(t *testing.T) {
	h := hub.New(8)
	srv, cancel := startSessionServer(t, auth.Principal{UserID: "admin", Org: "hanzo", Plan: "enterprise", IsAdmin: true}, h)
	defer srv.Close()
	defer cancel()
	c := dial(t, srv.URL)
	defer c.Close()
	_ = readFrame(t, c) // INIT_ACK

	// Subscribe first to verify publish reaches subscribers.
	writeFrame(t, c, proto.TypeCallTool, map[string]any{
		"id": "s", "name": "subscribe",
		"args": map[string]any{"topic": "world.news.live"},
	})
	_ = readFrame(t, c)

	writeFrame(t, c, proto.TypeCallTool, map[string]any{
		"id": "p", "name": "publish",
		"args": map[string]any{"topic": "world.news.live", "payload": map[string]any{"headline": "test"}},
	})
	// Expect ack RESOLVE + at some point a PUSH.
	gotPush := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !gotPush {
		c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		_, b, err := c.ReadMessage()
		if err != nil {
			continue
		}
		f, _, err := proto.Decode(b)
		if err != nil {
			continue
		}
		if f.Type == proto.TypePUSH {
			gotPush = true
		}
	}
	if !gotPush {
		t.Fatalf("admin publish did not fan out")
	}
}

func TestSessionRejectsNonAdminPublish(t *testing.T) {
	h := hub.New(8)
	srv, cancel := startSessionServer(t, auth.Principal{UserID: "u", Org: "acme", Plan: "enterprise"}, h)
	defer srv.Close()
	defer cancel()
	c := dial(t, srv.URL)
	defer c.Close()
	_ = readFrame(t, c) // INIT_ACK

	writeFrame(t, c, proto.TypeCallTool, map[string]any{
		"id": "p", "name": "publish",
		"args": map[string]any{"topic": "world.news.live", "payload": map[string]any{}},
	})
	f := readFrame(t, c)
	if !strings.Contains(string(f.Payload), "forbidden") {
		t.Fatalf("expected forbidden: %s", f.Payload)
	}
}
