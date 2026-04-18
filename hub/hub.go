// Package hub implements a fan-out pub/sub hub for world-zap topics.
//
// Subscribers get their own bounded channel of frames; slow subscribers are
// dropped rather than blocking the publisher. Topics are authoritative
// strings — unknown topics return an error at subscribe time.
package hub

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Required plan levels for each canonical topic.
// Any new topic MUST be registered here; publishing to an unregistered topic
// fails.
var topicPlans = map[string]string{
	"world.events.all":         "free",
	"world.events.conflicts":   "pro",
	"world.events.earthquakes": "free",
	"world.events.fires":       "free",
	"world.markets.quotes":     "pro",
	"world.markets.crypto":     "free",
	"world.ships.ais":          "team",
	"world.aviation.opensky":   "pro",
	"world.news.live":          "free",
	"world.weather.alerts":     "free",
}

// TopicNames returns the canonical topic list in a stable order.
func TopicNames() []string {
	out := make([]string, 0, len(topicPlans))
	// Emit in insertion-order of map keys sorted to be deterministic.
	seen := map[string]bool{}
	ordered := []string{
		"world.events.all",
		"world.events.conflicts",
		"world.events.earthquakes",
		"world.events.fires",
		"world.markets.quotes",
		"world.markets.crypto",
		"world.ships.ais",
		"world.aviation.opensky",
		"world.news.live",
		"world.weather.alerts",
	}
	for _, t := range ordered {
		if _, ok := topicPlans[t]; ok && !seen[t] {
			out = append(out, t)
			seen[t] = true
		}
	}
	return out
}

// TopicPlan returns the minimum plan required for a topic, or the empty
// string if unknown.
func TopicPlan(topic string) string {
	return topicPlans[topic]
}

// ErrUnknownTopic is returned when a topic is not in the catalog.
var ErrUnknownTopic = errors.New("hub: unknown topic")

// Message is the raw event forwarded to subscribers. Payload is already
// marshalled JSON ready to be wrapped in a PUSH frame.
type Message struct {
	Topic   string
	Payload []byte
}

// Subscription represents a single subscriber's bounded channel.
type Subscription struct {
	id      uint64
	topic   string
	ch      chan Message
	dropped atomic.Uint64
}

// ID returns the subscription identifier.
func (s *Subscription) ID() uint64 { return s.id }

// Topic returns the subscribed topic.
func (s *Subscription) Topic() string { return s.topic }

// Channel returns the message channel.
func (s *Subscription) Channel() <-chan Message { return s.ch }

// DroppedCount returns the number of messages dropped for this subscriber
// due to channel overflow.
func (s *Subscription) DroppedCount() uint64 { return s.dropped.Load() }

// Hub is a topic-keyed pub/sub registry.
type Hub struct {
	mu        sync.RWMutex
	byTopic   map[string]map[uint64]*Subscription
	nextID    uint64
	chanDepth int
}

// New creates an empty Hub. chanDepth caps each subscriber's buffer.
func New(chanDepth int) *Hub {
	if chanDepth <= 0 {
		chanDepth = 256
	}
	return &Hub{
		byTopic:   make(map[string]map[uint64]*Subscription),
		chanDepth: chanDepth,
	}
}

// Subscribe registers a new subscriber to a topic.
func (h *Hub) Subscribe(topic string) (*Subscription, error) {
	if _, ok := topicPlans[topic]; !ok {
		return nil, ErrUnknownTopic
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	sub := &Subscription{id: h.nextID, topic: topic, ch: make(chan Message, h.chanDepth)}
	subs, ok := h.byTopic[topic]
	if !ok {
		subs = make(map[uint64]*Subscription)
		h.byTopic[topic] = subs
	}
	subs[sub.id] = sub
	return sub, nil
}

// Unsubscribe removes a subscription and closes its channel.
func (h *Hub) Unsubscribe(sub *Subscription) {
	if sub == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	subs, ok := h.byTopic[sub.topic]
	if !ok {
		return
	}
	if _, present := subs[sub.id]; !present {
		return
	}
	delete(subs, sub.id)
	close(sub.ch)
	if len(subs) == 0 {
		delete(h.byTopic, sub.topic)
	}
}

// Publish fans out a message to all subscribers of its topic. Non-blocking:
// if a subscriber's channel is full the message is dropped and counted.
func (h *Hub) Publish(msg Message) error {
	if _, ok := topicPlans[msg.Topic]; !ok {
		return ErrUnknownTopic
	}
	h.mu.RLock()
	subs := h.byTopic[msg.Topic]
	targets := make([]*Subscription, 0, len(subs))
	for _, s := range subs {
		targets = append(targets, s)
	}
	h.mu.RUnlock()

	for _, s := range targets {
		select {
		case s.ch <- msg:
		default:
			s.dropped.Add(1)
		}
	}
	return nil
}

// SubscriberCount returns the number of subscribers to a topic.
func (h *Hub) SubscriberCount(topic string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.byTopic[topic])
}

// TopicStats reports per-topic subscriber counts for observability.
func (h *Hub) TopicStats() map[string]int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]int, len(h.byTopic))
	for t, subs := range h.byTopic {
		out[t] = len(subs)
	}
	return out
}
