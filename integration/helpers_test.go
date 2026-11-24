//go:build integration

// Package integration exercises the pipeline against real backends and
// devices. Run with:
//
//	go test -tags integration ./integration/...
//
// Each test is skipped unless its environment variables are set; see
// .github/workflows/ci.yml for how CI starts the services.
package integration

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"

	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/input/gnmi"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/influxdb"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/kafka"
	_ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/otlp"
)

func env(t *testing.T, keys ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, k := range keys {
		v := os.Getenv(k)
		if v == "" {
			t.Skipf("%s not set", k)
		}
		out[k] = v
	}
	return out
}

func testEnv() pipeline.Env {
	return pipeline.Env{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Registry: prometheus.NewRegistry()}
}

// plugin parses a single plugin section from YAML.
func plugin(t *testing.T, kind, yaml string) *config.Plugin {
	t.Helper()
	doc := "inputs: [{type: placeholder}]\noutputs: [{type: placeholder}]\n"
	if kind == "input" {
		doc = "outputs: [{type: placeholder}]\ninputs:\n  - " + yaml
	} else {
		doc = "inputs: [{type: placeholder}]\noutputs:\n  - " + yaml
	}
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if kind == "input" {
		return &cfg.Inputs[0]
	}
	return &cfg.Outputs[0]
}

// runID makes each test run's data distinguishable in shared backends.
func runID() string { return fmt.Sprintf("itest-%d", time.Now().UnixNano()) }

func sampleMetric(id string, v uint64) *telemetry.Metric {
	m := telemetry.New("openconfig:/interfaces/interface", time.Now())
	m.Tags = map[string]string{"source": "r1", "interface_name": id}
	m.Fields = map[string]any{"state/counters/in-octets": v, "state/oper-status": "UP", "state/mtu": int64(9000)}
	return m
}

func eventually(t *testing.T, timeout time.Duration, what string, f func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		ok, info := f()
		if ok {
			return
		}
		last = info
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s; last: %s", timeout, what, last)
		}
		time.Sleep(time.Second)
	}
}
