// Package handlers wires ZAP frames to hub and auth operations for a single
// websocket session. One Session per connection.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/hanzoai/world-zap/auth"
	"github.com/hanzoai/world-zap/hub"
	"github.com/hanzoai/world-zap/proto"
	"github.com/hanzoai/world-zap/ratelimit"
)

// Deps are the shared runtime dependencies a session needs.
type Deps struct {
	Hub       *hub.Hub
	Auth      *auth.Validator
	Limiter   *ratelimit.Limiter
	Logger    *slog.Logger
	WriteWait time.Duration
	PingEvery time.Duration
}

// Session manages a single websocket connection.
type Session struct {
	conn      *websocket.Conn
	principal auth.Principal
	deps      Deps

	mu       sync.Mutex
	subs     map[string]*hub.Subscription // topic -> sub
	writeMu  sync.Mutex                   // serializes writes to conn
}

// New constructs a Session.
func New(conn *websocket.Conn, p auth.Principal, deps Deps) *Session {
	if deps.WriteWait <= 0 {
		deps.WriteWait = 10 * time.Second
	}
	if deps.PingEvery <= 0 {
		deps.PingEvery = 30 * time.Second
	}
	return &Session{conn: conn, principal: p, deps: deps, subs: map[string]*hub.Subscription{}}
}

// Run is the session event loop. It terminates when ctx is cancelled, the
// connection closes, or a protocol error occurs.
func (s *Session) Run(ctx context.Context) {
	defer s.shutdown()

	// Send INIT_ACK immediately so clients can confirm the handshake.
	if err := s.sendFrame(proto.TypeINITACK, map[string]any{
		"user_id":  s.principal.UserID,
		"org":      s.principal.Org,
		"plan":     s.principal.Plan,
		"capacity": ratelimit.PlanCapacity(s.principal.Plan),
	}); err != nil {
		s.deps.Logger.Warn("zap: init_ack failed", "err", err.Error())
		return
	}

	// Pump outbound PUSH frames from each subscription.
	go s.pumpSubs(ctx)

	// Application-layer keepalive.
	ticker := time.NewTicker(s.deps.PingEvery)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.sendFrame(proto.TypePING, nil)
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		_, msg, err := s.conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				s.deps.Logger.Debug("zap: read", "err", err.Error())
			}
			return
		}
		f, _, err := proto.Decode(msg)
		if err != nil {
			_ = s.sendError("bad_frame", err.Error())
			return
		}
		if err := s.handle(ctx, f); err != nil {
			_ = s.sendError("handler_error", err.Error())
			if errors.Is(err, errFatal) {
				return
			}
		}
	}
}

// handle dispatches a single decoded frame.
func (s *Session) handle(ctx context.Context, f proto.Frame) error {
	switch f.Type {
	case proto.TypeINIT:
		// Re-INIT simply re-acks.
		return s.sendFrame(proto.TypeINITACK, map[string]any{"ok": true})
	case proto.TypePING:
		return s.sendFrame(proto.TypePONG, nil)
	case proto.TypePONG:
		return nil
	case proto.TypeListTools:
		return s.handleListTools()
	case proto.TypeCallTool:
		return s.handleCallTool(ctx, f.Payload)
	default:
		return fmt.Errorf("unsupported frame: %s", proto.TypeName(f.Type))
	}
}

// handleListTools responds with the tool catalog. Currently the only session
// tools are subscribe/unsubscribe/list_topics and (admin only) publish.
func (s *Session) handleListTools() error {
	tools := []map[string]any{
		{
			"name":        "subscribe",
			"description": "Subscribe to a world topic. Emits PUSH frames until unsubscribed.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic": map[string]any{"type": "string"},
				},
				"required": []string{"topic"},
			},
		},
		{
			"name":        "unsubscribe",
			"description": "Cancel a prior subscribe.",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"topic": map[string]any{"type": "string"}},
				"required":   []string{"topic"},
			},
		},
		{
			"name":        "list_topics",
			"description": "List available topics and required plan.",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
		},
	}
	if s.principal.IsAdmin {
		tools = append(tools, map[string]any{
			"name":        "publish",
			"description": "Publish an event to a topic (admin only).",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic":   map[string]any{"type": "string"},
					"payload": map[string]any{"type": "object"},
				},
				"required": []string{"topic", "payload"},
			},
		})
	}
	return s.sendFrame(proto.TypeListTools, map[string]any{"tools": tools})
}

// errFatal marks an error that should terminate the session.
var errFatal = errors.New("fatal")

// callToolPayload is the on-the-wire shape of a CALL_TOOL frame's payload.
type callToolPayload struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
	ID   string          `json:"id"`
}

func (s *Session) handleCallTool(ctx context.Context, raw []byte) error {
	gate := s.deps.Limiter.TryAcquire(s.principal.UserID, s.principal.Plan)
	if !gate.Allowed {
		return s.sendFrame(proto.TypeERROR, map[string]any{
			"code":     "rate_limited",
			"plan":     s.principal.Plan,
			"capacity": ratelimit.PlanCapacity(s.principal.Plan),
			"retry_ms": gate.RetryIn.Milliseconds(),
		})
	}

	var p callToolPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("call_tool: decode: %w", err)
	}

	switch p.Name {
	case "subscribe":
		return s.toolSubscribe(ctx, p)
	case "unsubscribe":
		return s.toolUnsubscribe(p)
	case "list_topics":
		return s.toolListTopics(p)
	case "publish":
		if !s.principal.IsAdmin {
			return s.sendCallResult(p.ID, map[string]any{"error": "forbidden"}, true)
		}
		return s.toolPublish(p)
	default:
		return s.sendCallResult(p.ID, map[string]any{"error": "unknown tool: " + p.Name}, true)
	}
}

func planLevel(p string) int {
	switch p {
	case "enterprise":
		return 4
	case "team":
		return 3
	case "pro":
		return 2
	default:
		return 1
	}
}

func (s *Session) toolSubscribe(_ context.Context, call callToolPayload) error {
	var args struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(call.Args, &args); err != nil {
		return s.sendCallResult(call.ID, map[string]any{"error": "bad args: " + err.Error()}, true)
	}
	required := hub.TopicPlan(args.Topic)
	if required == "" {
		return s.sendCallResult(call.ID, map[string]any{"error": "unknown topic"}, true)
	}
	if planLevel(s.principal.Plan) < planLevel(required) {
		return s.sendCallResult(call.ID, map[string]any{
			"error":    "plan_too_low",
			"required": required,
			"current":  s.principal.Plan,
		}, true)
	}

	s.mu.Lock()
	if _, exists := s.subs[args.Topic]; exists {
		s.mu.Unlock()
		return s.sendCallResult(call.ID, map[string]any{"already_subscribed": true, "topic": args.Topic}, false)
	}
	s.mu.Unlock()

	sub, err := s.deps.Hub.Subscribe(args.Topic)
	if err != nil {
		return s.sendCallResult(call.ID, map[string]any{"error": err.Error()}, true)
	}
	s.mu.Lock()
	s.subs[args.Topic] = sub
	s.mu.Unlock()
	return s.sendCallResult(call.ID, map[string]any{"subscribed": true, "topic": args.Topic}, false)
}

func (s *Session) toolUnsubscribe(call callToolPayload) error {
	var args struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(call.Args, &args); err != nil {
		return s.sendCallResult(call.ID, map[string]any{"error": "bad args: " + err.Error()}, true)
	}
	s.mu.Lock()
	sub, ok := s.subs[args.Topic]
	if ok {
		delete(s.subs, args.Topic)
	}
	s.mu.Unlock()
	if !ok {
		return s.sendCallResult(call.ID, map[string]any{"error": "not subscribed"}, true)
	}
	s.deps.Hub.Unsubscribe(sub)
	return s.sendCallResult(call.ID, map[string]any{"unsubscribed": true, "topic": args.Topic}, false)
}

func (s *Session) toolListTopics(call callToolPayload) error {
	names := hub.TopicNames()
	list := make([]map[string]string, 0, len(names))
	for _, n := range names {
		list = append(list, map[string]string{"topic": n, "required_plan": hub.TopicPlan(n)})
	}
	return s.sendCallResult(call.ID, map[string]any{"topics": list}, false)
}

func (s *Session) toolPublish(call callToolPayload) error {
	var args struct {
		Topic   string          `json:"topic"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(call.Args, &args); err != nil {
		return s.sendCallResult(call.ID, map[string]any{"error": "bad args: " + err.Error()}, true)
	}
	if args.Topic == "" || len(args.Payload) == 0 {
		return s.sendCallResult(call.ID, map[string]any{"error": "topic and payload required"}, true)
	}
	if err := s.deps.Hub.Publish(hub.Message{Topic: args.Topic, Payload: []byte(args.Payload)}); err != nil {
		return s.sendCallResult(call.ID, map[string]any{"error": err.Error()}, true)
	}
	return s.sendCallResult(call.ID, map[string]any{"published": true, "topic": args.Topic}, false)
}

// pumpSubs forwards hub messages to the client as PUSH frames.
func (s *Session) pumpSubs(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		// Snapshot current subs.
		s.mu.Lock()
		subs := make([]*hub.Subscription, 0, len(s.subs))
		for _, sub := range s.subs {
			subs = append(subs, sub)
		}
		s.mu.Unlock()
		if len(subs) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}
		// Non-blocking drain of each subscription.
		for _, sub := range subs {
			select {
			case msg, ok := <-sub.Channel():
				if !ok {
					continue
				}
				if err := s.sendPush(msg); err != nil {
					return
				}
			default:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Session) sendPush(msg hub.Message) error {
	body, err := json.Marshal(map[string]any{
		"topic": msg.Topic,
		"event": json.RawMessage(msg.Payload),
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	return s.writeFrame(proto.TypePUSH, body)
}

// sendFrame JSON-encodes v into a frame and writes it.
func (s *Session) sendFrame(t byte, v any) error {
	b, err := proto.EncodeJSON(t, v)
	if err != nil {
		return err
	}
	return s.writeRaw(b)
}

func (s *Session) sendError(code, detail string) error {
	return s.sendFrame(proto.TypeERROR, map[string]string{"code": code, "detail": detail})
}

// sendCallResult packages a tool response as a RESOLVE frame.
func (s *Session) sendCallResult(id string, result any, isError bool) error {
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return s.sendFrame(proto.TypeRESOLVE, map[string]any{
		"id":      id,
		"error":   isError,
		"content": json.RawMessage(body),
	})
}

// writeFrame assembles a frame from a precomputed body and sends it.
func (s *Session) writeFrame(t byte, body []byte) error {
	b, err := proto.Encode(proto.Frame{Type: t, Payload: body})
	if err != nil {
		return err
	}
	return s.writeRaw(b)
}

func (s *Session) writeRaw(b []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.deps.WriteWait))
	return s.conn.WriteMessage(websocket.BinaryMessage, b)
}

// shutdown tears down all subscriptions and closes the connection.
func (s *Session) shutdown() {
	s.mu.Lock()
	subs := make([]*hub.Subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.subs = nil
	s.mu.Unlock()
	for _, sub := range subs {
		s.deps.Hub.Unsubscribe(sub)
	}
	_ = s.conn.Close()
}
