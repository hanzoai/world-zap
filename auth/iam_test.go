package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExtractTokenHeader(t *testing.T) {
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set("Authorization", "Bearer abcd")
	if got := ExtractToken(r); got != "abcd" {
		t.Fatalf("got %q", got)
	}
}

func TestExtractTokenQuery(t *testing.T) {
	r := httptest.NewRequest("GET", "/x?token=xyz", nil)
	if got := ExtractToken(r); got != "xyz" {
		t.Fatalf("got %q", got)
	}
}

func TestExtractTokenEmpty(t *testing.T) {
	r := httptest.NewRequest("GET", "/x", nil)
	if got := ExtractToken(r); got != "" {
		t.Fatalf("got %q", got)
	}
}

func TestValidateCachedAndAdmin(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/oauth/userinfo" {
			t.Errorf("wrong path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("missing bearer: %q", got)
		}
		fmt.Fprint(w, `{"sub":"user_1","email":"z@hanzo.ai","owner":"hanzo","plan":"team"}`)
	}))
	defer srv.Close()

	v := New(Config{Endpoint: srv.URL, TTL: time.Minute, AdminOrgs: []string{"hanzo"}})
	p1, err := v.Validate(context.Background(), "tok")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if p1.UserID != "user_1" || p1.Org != "hanzo" || p1.Plan != "team" || !p1.IsAdmin {
		t.Fatalf("bad principal: %+v", p1)
	}
	// Second call should be cached.
	p2, err := v.Validate(context.Background(), "tok")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if p2.UserID != p1.UserID || p2.Org != p1.Org || p2.Plan != p1.Plan {
		t.Fatalf("cache miss: %+v vs %+v", p2, p1)
	}
	if calls != 1 {
		t.Fatalf("expected 1 upstream call, got %d", calls)
	}
}

func TestValidate401(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	v := New(Config{Endpoint: srv.URL})
	_, err := v.Validate(context.Background(), "bad")
	if err != ErrInvalidToken {
		t.Fatalf("want ErrInvalidToken got %v", err)
	}
}

func TestValidateMissingToken(t *testing.T) {
	v := New(Config{Endpoint: "http://nowhere"})
	_, err := v.Validate(context.Background(), "")
	if err != ErrInvalidToken {
		t.Fatalf("want ErrInvalidToken got %v", err)
	}
}

func TestValidatePlanDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"sub":"u","email":"e","owner":"acme"}`)
	}))
	defer srv.Close()
	v := New(Config{Endpoint: srv.URL})
	p, err := v.Validate(context.Background(), "tok")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if p.Plan != "free" {
		t.Fatalf("default plan: %q", p.Plan)
	}
	if p.IsAdmin {
		t.Fatalf("unexpected admin")
	}
}
