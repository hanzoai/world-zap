package ingest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hanzoai/world-zap/hub"
)

type testLogger struct{}

func (testLogger) Info(string, ...any)  {}
func (testLogger) Warn(string, ...any)  {}
func (testLogger) Error(string, ...any) {}

func TestStreamPublishesEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stream") != "1" {
			t.Errorf("expected stream=1")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		fmt.Fprintln(w, `{"topic":"world.events.earthquakes","payload":{"id":"usgs:1","mag":4.2}}`)
		flusher.Flush()
		fmt.Fprintln(w, `data: {"topic":"world.markets.crypto","payload":{"symbol":"BTCUSD","price":50000}}`)
		flusher.Flush()
		// close the connection after a short delay to let the ingester read
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	h := hub.New(8)
	subEq, _ := h.Subscribe("world.events.earthquakes")
	defer h.Unsubscribe(subEq)
	subCr, _ := h.Subscribe("world.markets.crypto")
	defer h.Unsubscribe(subCr)

	ing := New(Config{BackendBase: srv.URL}, h, testLogger{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := ing.stream(ctx)
	// EOF is the expected outcome when the server closes after writing.
	if err != nil && err != io.EOF && err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("stream: %v", err)
	}

	select {
	case m := <-subEq.Channel():
		if m.Topic != "world.events.earthquakes" {
			t.Fatalf("topic: %q", m.Topic)
		}
	case <-time.After(time.Second):
		t.Fatalf("earthquake message missing")
	}

	select {
	case m := <-subCr.Channel():
		if m.Topic != "world.markets.crypto" {
			t.Fatalf("topic: %q", m.Topic)
		}
	case <-time.After(time.Second):
		t.Fatalf("crypto message missing")
	}
}
