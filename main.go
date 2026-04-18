// Command world-zap bridges the worldmonitor backend to a ZAP-over-WebSocket
// gateway for real-time feed fan-out.
//
//	wss://zap.world.hanzo.ai/zap?token=<IAM_TOKEN>
//
// Topics are published by an ingester goroutine that streams from
// WORLD_BACKEND/v1/world/events?stream=1 and fans out via the hub.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanzoai/world-zap/auth"
	"github.com/hanzoai/world-zap/hub"
	"github.com/hanzoai/world-zap/ingest"
	"github.com/hanzoai/world-zap/ratelimit"
)

const version = "0.1.0"

func main() {
	cfg := loadConfig()
	logger := newLogger(cfg.LogLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownOtel, err := initOtel(ctx, cfg.OtelEndpoint, cfg.ServiceName, version)
	if err != nil {
		logger.Warn("otel init failed", "err", err.Error())
	}

	av := auth.New(auth.Config{
		Endpoint:  cfg.IAMEndpoint,
		TTL:       cfg.AuthTTL,
		AdminOrgs: cfg.AdminOrgs,
	})
	h := hub.New(1024)
	limiter := ratelimit.New()

	ing := ingest.New(ingest.Config{
		BackendBase:  cfg.BackendBase,
		ServiceToken: cfg.ServiceToken,
	}, h, slogAdapter{logger})
	go ing.Run(ctx)

	deps := &serverDeps{
		cfg:     cfg,
		logger:  logger,
		auth:    av,
		hub:     h,
		limiter: limiter,
		backend: newBackendHealthClient(cfg.BackendBase),
		ready:   &atomicBool{v: true},
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           newMux(deps),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("world-zap listening", "addr", cfg.Addr, "topics", len(hub.TopicNames()))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http serve", "err", err.Error())
			cancel()
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutdown requested")
	deps.ready.set(false)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("server shutdown", "err", err.Error())
	}
	cancel()
	if shutdownOtel != nil {
		_ = shutdownOtel(shutdownCtx)
	}
	logger.Info("bye")
}

func newLogger(level string) *slog.Logger {
	lvl := slog.LevelInfo
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// slogAdapter bridges the structured slog logger to the ingest.Logger
// interface.
type slogAdapter struct{ l *slog.Logger }

func (s slogAdapter) Info(msg string, kv ...any)  { s.l.Info(msg, kv...) }
func (s slogAdapter) Warn(msg string, kv ...any)  { s.l.Warn(msg, kv...) }
func (s slogAdapter) Error(msg string, kv ...any) { s.l.Error(msg, kv...) }
