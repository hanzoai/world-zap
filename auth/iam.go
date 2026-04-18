// Package auth validates IAM bearer tokens via the hanzo.id /oauth/userinfo
// endpoint and returns a Principal that downstream components use for
// authorization.
//
// Tokens are cached in process for 5 minutes by default. The cache is
// concurrency-safe; entries are evicted lazily on lookup.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Principal is the authenticated caller.
type Principal struct {
	UserID       string   `json:"user_id"`
	Email        string   `json:"email"`
	Org          string   `json:"org"`
	Plan         string   `json:"plan"` // free|pro|team|enterprise
	Entitlements []string `json:"entitlements"`
	IsAdmin      bool     `json:"is_admin"`
}

// ErrInvalidToken is returned when the token is missing or the IAM rejects it.
var ErrInvalidToken = errors.New("auth: invalid token")

// ErrUpstream is returned when IAM is unreachable or returns a server error.
var ErrUpstream = errors.New("auth: upstream error")

type cacheEntry struct {
	principal Principal
	expiresAt time.Time
}

// Validator validates bearer tokens against an IAM endpoint.
type Validator struct {
	endpoint   string
	httpClient *http.Client
	ttl        time.Duration
	adminOrgs  map[string]struct{}

	mu    sync.Mutex
	cache map[string]cacheEntry
}

// Config configures a Validator.
type Config struct {
	Endpoint   string        // e.g. https://hanzo.id
	TTL        time.Duration // token cache ttl
	HTTPClient *http.Client  // optional custom client
	AdminOrgs  []string      // orgs whose members are admins (e.g. "hanzo")
}

// New constructs a Validator with sensible defaults.
func New(cfg Config) *Validator {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	admins := make(map[string]struct{}, len(cfg.AdminOrgs))
	for _, o := range cfg.AdminOrgs {
		admins[strings.ToLower(o)] = struct{}{}
	}
	return &Validator{
		endpoint:   strings.TrimRight(cfg.Endpoint, "/"),
		httpClient: client,
		ttl:        ttl,
		adminOrgs:  admins,
		cache:      make(map[string]cacheEntry),
	}
}

// ExtractToken returns the bearer token from an HTTP request.
// Priority: Authorization header > ?token= query param.
func ExtractToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(h), "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	if t := r.URL.Query().Get("token"); t != "" {
		return t
	}
	return ""
}

// Validate looks up the token in cache, falling back to /oauth/userinfo.
func (v *Validator) Validate(ctx context.Context, token string) (Principal, error) {
	if token == "" {
		return Principal{}, ErrInvalidToken
	}

	v.mu.Lock()
	if e, ok := v.cache[token]; ok && time.Now().Before(e.expiresAt) {
		v.mu.Unlock()
		return e.principal, nil
	}
	v.mu.Unlock()

	p, err := v.fetchUserInfo(ctx, token)
	if err != nil {
		return Principal{}, err
	}

	v.mu.Lock()
	v.cache[token] = cacheEntry{principal: p, expiresAt: time.Now().Add(v.ttl)}
	v.gcLocked()
	v.mu.Unlock()
	return p, nil
}

func (v *Validator) fetchUserInfo(ctx context.Context, token string) (Principal, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.endpoint+"/oauth/userinfo", nil)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrUpstream, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrUpstream, err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden {
		return Principal{}, ErrInvalidToken
	}
	if res.StatusCode >= 500 || res.StatusCode == http.StatusBadGateway {
		return Principal{}, ErrUpstream
	}
	if res.StatusCode >= 400 {
		return Principal{}, fmt.Errorf("%w: status %d", ErrUpstream, res.StatusCode)
	}

	var body struct {
		Sub          string   `json:"sub"`
		ID           string   `json:"id"`
		Email        string   `json:"email"`
		Owner        string   `json:"owner"`
		Org          string   `json:"org"`
		Plan         string   `json:"plan"`
		Tier         string   `json:"tier"`
		Entitlements []string `json:"entitlements"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return Principal{}, fmt.Errorf("%w: decode: %v", ErrUpstream, err)
	}

	userID := body.Sub
	if userID == "" {
		userID = body.ID
	}
	if userID == "" {
		return Principal{}, ErrInvalidToken
	}

	org := body.Owner
	if org == "" {
		org = body.Org
	}
	if org == "" {
		org = "default"
	}

	plan := normalizePlan(body.Plan, body.Tier)

	p := Principal{
		UserID:       userID,
		Email:        body.Email,
		Org:          strings.ToLower(org),
		Plan:         plan,
		Entitlements: body.Entitlements,
	}
	_, p.IsAdmin = v.adminOrgs[p.Org]
	return p, nil
}

func normalizePlan(plan, tier string) string {
	s := strings.ToLower(plan)
	if s == "" {
		s = strings.ToLower(tier)
	}
	switch s {
	case "pro", "team", "enterprise":
		return s
	default:
		return "free"
	}
}

// gcLocked evicts expired entries when the cache is large enough to matter.
// Caller must hold v.mu.
func (v *Validator) gcLocked() {
	if len(v.cache) < 10000 {
		return
	}
	now := time.Now()
	for k, e := range v.cache {
		if now.After(e.expiresAt) {
			delete(v.cache, k)
		}
	}
}
