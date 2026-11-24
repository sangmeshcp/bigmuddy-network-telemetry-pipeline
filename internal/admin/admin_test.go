package admin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestEndpoints(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "pipeline_test_total", Help: "h"})
	reg.MustRegister(c)
	c.Add(3)
	var ready atomic.Bool
	s, err := New("127.0.0.1:0", true, reg, ready.Load, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	defer s.Shutdown(context.Background())
	base := "http://" + s.Addr().String()

	if code, body := get(t, base+"/healthz"); code != 200 || body != "ok\n" {
		t.Errorf("healthz: %d %q", code, body)
	}
	if code, _ := get(t, base+"/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("readyz before ready: %d", code)
	}
	ready.Store(true)
	if code, _ := get(t, base+"/readyz"); code != 200 {
		t.Errorf("readyz after ready: %d", code)
	}
	if code, body := get(t, base+"/metrics"); code != 200 || !strings.Contains(body, "pipeline_test_total 3") {
		t.Errorf("metrics: %d %s", code, body)
	}
	if code, _ := get(t, base+"/debug/pprof/"); code != 200 {
		t.Errorf("pprof enabled but returned %d", code)
	}
}

func TestPprofDisabledAndListenError(t *testing.T) {
	s, err := New("127.0.0.1:0", false, prometheus.NewRegistry(), func() bool { return true }, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	defer s.Shutdown(context.Background())
	if code, _ := get(t, "http://"+s.Addr().String()+"/debug/pprof/"); code != 404 {
		t.Errorf("pprof should be off, got %d", code)
	}
	if _, err := New(s.Addr().String(), false, prometheus.NewRegistry(), nil, slog.Default()); err == nil {
		t.Error("expected address-in-use error")
	}
}
