package processor

import (
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func newDS(t *testing.T, yaml string) pipeline.Processor {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs: [{type: y}]\nprocessors:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	p, err := newDownsample(&cfg.Processors[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func at(sec int, src string, fields map[string]any) *telemetry.Metric {
	m := telemetry.New("/if", time.Unix(int64(sec), 0))
	m.Tags["source"] = src
	for k, v := range fields {
		m.Fields[k] = v
	}
	return m
}

func TestDownsample(t *testing.T) {
	p := newDS(t, `{type: downsample, interval: 60s}`)
	// Device sends every 10s; expect one sample per minute per series.
	var kept []int64
	for sec := 0; sec < 180; sec += 10 {
		out := p.Process([]*telemetry.Metric{at(sec, "r1", map[string]any{"in": int64(sec)})})
		for _, m := range out {
			kept = append(kept, m.Fields["in"].(int64))
		}
	}
	if len(kept) != 3 || kept[0] != 0 || kept[1] != 60 || kept[2] != 120 {
		t.Errorf("kept %v, want [0 60 120]", kept)
	}
	// Different series are independent.
	if out := p.Process([]*telemetry.Metric{at(5, "r2", map[string]any{"in": int64(1)})}); len(out) != 1 {
		t.Error("new series was downsampled away")
	}
	// Fields split across notifications in the same window are each kept once.
	a := at(200, "r3", map[string]any{"in": int64(1)})
	b := at(201, "r3", map[string]any{"out": int64(2), "in": int64(3)})
	b.SetKind("in", telemetry.KindCounter)
	out := p.Process([]*telemetry.Metric{a, b})
	if len(out) != 2 || len(out[1].Fields) != 1 || out[1].Fields["out"] != int64(2) || out[1].Kinds["in"] != 0 {
		t.Errorf("split fields: %+v", out)
	}
}

func TestDownsampleNamesAndConfig(t *testing.T) {
	p := newDS(t, `{type: downsample, interval: 1h, names: ["^/only$"]}`)
	named := func(name string, sec int) *telemetry.Metric {
		m := at(sec, "r", map[string]any{"v": 1.0})
		m.Name = name
		return m
	}
	// Two samples in the same hour: /only is downsampled, /other passes through.
	out := p.Process([]*telemetry.Metric{named("/only", 0), named("/other", 0)})
	out = append(out, p.Process([]*telemetry.Metric{named("/only", 10), named("/other", 10)})...)
	got := map[string]int{}
	for _, m := range out {
		got[m.Name]++
	}
	if got["/only"] != 1 || got["/other"] != 2 {
		t.Errorf("got %v, want /only once and /other twice", got)
	}
	cfg, _ := config.Parse([]byte("inputs: [{type: x}]\noutputs: [{type: y}]\nprocessors: [{type: downsample}]"))
	if _, err := newDownsample(&cfg.Processors[0], pipeline.Env{}); err == nil {
		t.Error("missing interval accepted")
	}
}
