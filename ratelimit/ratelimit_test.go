package ratelimit

import (
	"testing"
)

func TestPlanCapacity(t *testing.T) {
	cases := map[string]int{
		"free":       30,
		"pro":        3000,
		"team":       15000,
		"enterprise": 60000,
		"":           30,
		"unknown":    30,
	}
	for plan, want := range cases {
		if got := PlanCapacity(plan); got != want {
			t.Errorf("plan %q: got %d want %d", plan, got, want)
		}
	}
}

func TestFreeBurstThenBlock(t *testing.T) {
	l := New()
	for i := 0; i < 30; i++ {
		d := l.TryAcquire("u", "free")
		if !d.Allowed {
			t.Fatalf("request %d denied: %+v", i, d)
		}
	}
	d := l.TryAcquire("u", "free")
	if d.Allowed {
		t.Fatalf("31st request should be denied")
	}
	if d.RetryIn <= 0 {
		t.Fatalf("expected retry hint")
	}
}

func TestPerUserIsolation(t *testing.T) {
	l := New()
	for i := 0; i < 30; i++ {
		l.TryAcquire("a", "free")
	}
	if l.TryAcquire("a", "free").Allowed {
		t.Fatalf("a should be throttled")
	}
	if !l.TryAcquire("b", "free").Allowed {
		t.Fatalf("b should be allowed")
	}
}

func TestPlanUpgradeResetsBucket(t *testing.T) {
	l := New()
	// burn free tokens
	for i := 0; i < 30; i++ {
		l.TryAcquire("u", "free")
	}
	if l.TryAcquire("u", "free").Allowed {
		t.Fatalf("free should be empty")
	}
	// Upgrade to pro; new bucket at pro capacity
	d := l.TryAcquire("u", "pro")
	if !d.Allowed {
		t.Fatalf("pro upgrade should grant tokens: %+v", d)
	}
}
