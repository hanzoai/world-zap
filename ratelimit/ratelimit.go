// Package ratelimit implements a per-user token bucket rate limiter with
// plan-based capacities.
//
// Free: 30/min. Pro: 3000/min. Team: 15000/min. Enterprise: 60000/min.
package ratelimit

import (
	"sync"
	"time"
)

// PlanCapacity returns the per-minute capacity for a plan. Unknown plans
// fall back to "free".
func PlanCapacity(plan string) int {
	switch plan {
	case "pro":
		return 3000
	case "team":
		return 15000
	case "enterprise":
		return 60000
	default:
		return 30
	}
}

type bucket struct {
	tokens  float64
	updated time.Time
	plan    string
}

// Limiter is a concurrency-safe token bucket keyed by user ID.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	clock   func() time.Time
}

// New constructs an empty Limiter.
func New() *Limiter {
	return &Limiter{
		buckets: make(map[string]*bucket),
		clock:   time.Now,
	}
}

// Decision is the result of a rate-limit check.
type Decision struct {
	Allowed   bool
	Remaining int
	RetryIn   time.Duration
}

// TryAcquire attempts to consume 1 token. Returns a Decision describing the
// outcome. If plan changes, capacity resizes at the next request.
func (l *Limiter) TryAcquire(userID, plan string) Decision {
	cap := PlanCapacity(plan)
	refillPerSec := float64(cap) / 60.0

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock()
	b, ok := l.buckets[userID]
	if !ok || b.plan != plan {
		b = &bucket{tokens: float64(cap), updated: now, plan: plan}
		l.buckets[userID] = b
	}

	elapsed := now.Sub(b.updated).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * refillPerSec
		if b.tokens > float64(cap) {
			b.tokens = float64(cap)
		}
		b.updated = now
	}

	if b.tokens < 1.0 {
		missing := 1.0 - b.tokens
		retry := time.Duration(missing/refillPerSec*float64(time.Second)) + time.Millisecond
		return Decision{Allowed: false, Remaining: 0, RetryIn: retry}
	}

	b.tokens -= 1.0
	return Decision{Allowed: true, Remaining: int(b.tokens), RetryIn: 0}
}
