package main

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	luxtrace "github.com/luxfi/trace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// initOtel configures tracing when OTEL_EXPORTER_OTLP_ENDPOINT is set.
// Returns a shutdown function that must be called on process exit.
func initOtel(ctx context.Context, endpoint, serviceName, version string) (func(context.Context) error, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	// ZAP takes host:port; a URL scheme has no meaning on this wire.
	ep := strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
	exp, err := luxtrace.NewZAPExporter(
		luxtrace.ExporterConfig{Type: luxtrace.ZAP, Endpoint: ep},
		serviceName, version,
	)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
		),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
