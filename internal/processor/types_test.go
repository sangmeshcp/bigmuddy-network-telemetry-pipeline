package processor

import (
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func newTypes(t *testing.T, yaml string) pipeline.Processor {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs: [{type: y}]\nprocessors:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	p, err := newMetricTypes(&cfg.Processors[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMetricTypesNetworkPreset(t *testing.T) {
	p := newTypes(t, `{type: metric_types, presets: [network], gauges: ["^state/mtu$"]}`)
	oc := metric("openconfig:/interfaces/interface", "r1", map[string]any{
		"state/counters/in-octets":  uint64(1),
		"state/counters/in-errors":  uint64(1),
		"state/counters/last-clear": "2026-01-01", // string: never typed
		"state/mtu":                 int64(9000),
		"state/oper-status":         "UP",
		"state/enabled":             true, // bool: never typed
	})
	xr := metric("Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters", "r2", map[string]any{
		"bytes-received": uint64(1), "packets-sent": uint64(1), "crc-errors": uint64(0),
		"input-drops": uint64(0), "carrier-transitions": uint64(0), "applique": uint64(0),
	})
	p.Process([]*telemetry.Metric{oc, xr})

	want := map[*telemetry.Metric]map[string]telemetry.Kind{
		oc: {"state/counters/in-octets": telemetry.KindCounter, "state/counters/in-errors": telemetry.KindCounter,
			"state/mtu": telemetry.KindGauge},
		xr: {"bytes-received": telemetry.KindCounter, "packets-sent": telemetry.KindCounter, "crc-errors": telemetry.KindCounter,
			"input-drops": telemetry.KindCounter, "carrier-transitions": telemetry.KindCounter},
	}
	for m, kinds := range want {
		if len(m.Kinds) != len(kinds) {
			t.Errorf("%s: got kinds %v, want %v", m.Name, m.Kinds, kinds)
		}
		for f, k := range kinds {
			if m.KindOf(f) != k {
				t.Errorf("%s %s: %v, want %v", m.Name, f, m.KindOf(f), k)
			}
		}
	}
	if xr.KindOf("applique") != telemetry.KindUnknown {
		t.Error("unrelated field typed")
	}
}

func TestMetricTypesNamesAndErrors(t *testing.T) {
	p := newTypes(t, `{type: metric_types, counters: ["^hits$"], names: ["^/only"]}`)
	a := metric("/only/this", "r", map[string]any{"hits": int64(1)})
	b := metric("/other", "r", map[string]any{"hits": int64(1)})
	p.Process([]*telemetry.Metric{a, b})
	if a.KindOf("hits") != telemetry.KindCounter || b.Kinds != nil {
		t.Errorf("names filter: a=%v b=%v", a.Kinds, b.Kinds)
	}
	for _, bad := range []string{`{type: metric_types}`, `{type: metric_types, presets: [nope]}`, `{type: metric_types, counters: ["("]}`} {
		cfg, _ := config.Parse([]byte("inputs: [{type: x}]\noutputs: [{type: y}]\nprocessors:\n  - " + bad))
		if _, err := newMetricTypes(&cfg.Processors[0], pipeline.Env{}); err == nil {
			t.Errorf("expected error for %s", bad)
		}
	}
}

func TestRenameCarriesKinds(t *testing.T) {
	p := build(t, `type: rename
    fields: [{match: "^state/counters/", replace: ""}]`)
	m := metric("x", "r", map[string]any{"state/counters/in-octets": uint64(1), "other": int64(1)})
	m.SetKind("state/counters/in-octets", telemetry.KindCounter)
	p.Process([]*telemetry.Metric{m})
	if m.KindOf("in-octets") != telemetry.KindCounter || len(m.Kinds) != 1 {
		t.Errorf("kinds not renamed: %v", m.Kinds)
	}
}
