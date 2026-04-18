package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the runtime configuration.
type Config struct {
	Addr         string
	IAMEndpoint  string
	BackendBase  string
	ServiceToken string
	AdminOrgs    []string
	OtelEndpoint string
	ServiceName  string
	LogLevel     string
	AuthTTL      time.Duration
}

func loadConfig() Config {
	return Config{
		Addr:         envOr("ADDR", ":9999"),
		IAMEndpoint:  envOr("IAM_ENDPOINT", "https://hanzo.id"),
		BackendBase:  envOr("WORLD_BACKEND", "http://world.hanzo.svc"),
		ServiceToken: os.Getenv("WORLD_SERVICE_TOKEN"),
		AdminOrgs:    splitCSV(envOr("ADMIN_ORGS", "hanzo")),
		OtelEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		ServiceName:  envOr("OTEL_SERVICE_NAME", "world-zap"),
		LogLevel:     envOr("LOG_LEVEL", "info"),
		AuthTTL:      parseDurationOr("AUTH_CACHE_TTL", 5*time.Minute),
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func parseDurationOr(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
