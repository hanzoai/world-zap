package hub

import (
	"testing"
	"time"
)

func TestPublishDelivers(t *testing.T) {
	h := New(8)
	sub, err := h.Subscribe("world.events.earthquakes")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer h.Unsubscribe(sub)

	if err := h.Publish(Message{Topic: "world.events.earthquakes", Payload: []byte(`{"mag":5.1}`)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case m := <-sub.Channel():
		if m.Topic != "world.events.earthquakes" {
			t.Fatalf("topic: %q", m.Topic)
		}
		if string(m.Payload) != `{"mag":5.1}` {
			t.Fatalf("payload: %s", m.Payload)
		}
	case <-time.After(time.Second):
		t.Fatalf("did not receive")
	}
}

func TestSubscribeUnknownTopic(t *testing.T) {
	h := New(8)
	if _, err := h.Subscribe("bogus"); err != ErrUnknownTopic {
		t.Fatalf("want ErrUnknownTopic, got %v", err)
	}
}

func TestPublishUnknownTopic(t *testing.T) {
	h := New(8)
	if err := h.Publish(Message{Topic: "bogus"}); err != ErrUnknownTopic {
		t.Fatalf("want ErrUnknownTopic, got %v", err)
	}
}

func TestSlowSubscriberDropped(t *testing.T) {
	h := New(2) // tiny buffer
	sub, err := h.Subscribe("world.events.all")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Do not drain sub.Channel — force overflow.
	for i := 0; i < 10; i++ {
		_ = h.Publish(Message{Topic: "world.events.all", Payload: []byte(`{}`)})
	}
	if sub.DroppedCount() == 0 {
		t.Fatalf("expected at least one drop")
	}
	// Drain what is buffered, then unsubscribe to close the channel.
	go func() {
		for range sub.Channel() {
		}
	}()
	h.Unsubscribe(sub)
}

func TestMultipleSubscribersFanOut(t *testing.T) {
	h := New(8)
	a, _ := h.Subscribe("world.events.all")
	b, _ := h.Subscribe("world.events.all")
	defer h.Unsubscribe(a)
	defer h.Unsubscribe(b)

	if err := h.Publish(Message{Topic: "world.events.all", Payload: []byte(`1`)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	for _, s := range []*Subscription{a, b} {
		select {
		case m := <-s.Channel():
			if string(m.Payload) != "1" {
				t.Fatalf("payload %s", m.Payload)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber did not receive")
		}
	}
	if got := h.SubscriberCount("world.events.all"); got != 2 {
		t.Fatalf("subscriber count: %d", got)
	}
}

func TestTopicNames(t *testing.T) {
	names := TopicNames()
	if len(names) != 10 {
		t.Fatalf("expected 10 topics, got %d: %v", len(names), names)
	}
	// Each advertised topic must have a plan.
	for _, n := range names {
		if TopicPlan(n) == "" {
			t.Fatalf("missing plan for %q", n)
		}
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	h := New(8)
	s, _ := h.Subscribe("world.events.all")
	h.Unsubscribe(s)
	_, ok := <-s.Channel()
	if ok {
		t.Fatalf("channel should be closed")
	}
}
