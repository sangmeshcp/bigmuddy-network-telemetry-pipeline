// Package outputtest is the conformance kit for analytical sinks (Parquet,
// ClickHouse, Iceberg, ...). Each sink's tests call Run with a way to build
// the output and a way to read back what it stored; Run writes a reference
// batch and checks every row round-trips exactly, and that retries don't
// duplicate data when the sink claims idempotency.
package outputtest

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// Options configures a conformance run.
type Options struct {
	// New builds a fresh output writing to an empty destination.
	New func(t *testing.T) pipeline.Output
	// ReadBack returns every row stored at the destination.
	ReadBack func(t *testing.T) []telemetry.Row
	// Idempotent: writing the same batch twice must store it once.
	Idempotent bool
	// TimePrecision the sink stores timestamps at (default: nanoseconds).
	TimePrecision time.Duration
}

// GoldenBatch is the reference data: every value type, a uint64 above
// MaxInt64, counter/gauge kinds, Unicode and punctuation in tags, several
// sources and metric names, and timestamps spanning an hour boundary (to
// exercise time partitioning).
func GoldenBatch() []*telemetry.Metric {
	base := time.Date(2026, 11, 24, 9, 59, 59, 123456789, time.UTC)
	var out []*telemetry.Metric
	for i, src := range []string{"core1", "edge-2"} {
		m := telemetry.New("openconfig:/interfaces/interface", base.Add(time.Duration(i)*time.Second))
		m.Tags = map[string]string{"source": src, "interface_name": "Ethernet1/1", "subscription": "if",
			"site": "dfw-1 \"east\", rack=4 ✓"}
		m.Fields = map[string]any{
			"state/counters/in-octets": uint64(1234567890123),
			"state/counters/huge":      uint64(math.MaxUint64),
			"state/counters/in-errors": int64(0),
			"state/mtu":                int64(9000),
			"state/load":               0.375,
			"state/enabled":            true,
			"state/oper-status":        "UP",
			"state/description":        "",
		}
		m.SetKind("state/counters/in-octets", telemetry.KindCounter)
		m.SetKind("state/counters/in-errors", telemetry.KindCounter)
		m.SetKind("state/mtu", telemetry.KindGauge)
		out = append(out, m)
	}
	xr := telemetry.New("Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters",
		base.Add(2*time.Second)) // next hour
	xr.Tags = map[string]string{"source": "dr02", "interface-name": "HundredGigE0/0/0/0"}
	xr.Fields = map[string]any{"bytes-received": uint64(8270089146), "crc-errors": uint64(0), "load": -1.5}
	out = append(out, xr)
	return out
}

// Run executes the conformance checks.
func Run(t *testing.T, o Options) {
	t.Helper()
	if o.TimePrecision <= 0 {
		o.TimePrecision = time.Nanosecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("round-trip", func(t *testing.T) {
		out := o.New(t)
		defer out.Close(ctx)
		if err := out.Write(ctx, nil); err != nil {
			t.Fatalf("empty batch: %v", err)
		}
		if err := out.Write(ctx, GoldenBatch()); err != nil {
			t.Fatalf("write: %v", err)
		}
		Compare(t, telemetry.RowsOf(GoldenBatch()), o.ReadBack(t), o.TimePrecision)
	})
	if o.Idempotent {
		t.Run("retry-is-idempotent", func(t *testing.T) {
			out := o.New(t)
			defer out.Close(ctx)
			for i := 0; i < 2; i++ {
				if err := out.Write(ctx, GoldenBatch()); err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
			}
			Compare(t, telemetry.RowsOf(GoldenBatch()), o.ReadBack(t), o.TimePrecision)
		})
	}
}

// Compare checks that got holds exactly the rows in want (order-insensitive).
func Compare(t *testing.T, want, got []telemetry.Row, precision time.Duration) {
	t.Helper()
	w, g := keys(want, precision), keys(got, precision)
	if len(g) != len(w) {
		t.Errorf("stored %d rows, want %d", len(g), len(w))
	}
	for i := 0; i < len(w) && i < len(g); i++ {
		if w[i] != g[i] {
			t.Errorf("row %d differs:\n want %s\n got  %s", i, w[i], g[i])
			return
		}
	}
}

func keys(rows []telemetry.Row, precision time.Duration) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		tags := make([]string, 0, len(r.Tags))
		for k, v := range r.Tags {
			tags = append(tags, k+"="+v)
		}
		sort.Strings(tags)
		out[i] = fmt.Sprintf("%d|%s|%s|%s|%s|%s|%s|%s|%s|%s",
			r.Timestamp.Truncate(precision).UnixNano(), r.Source, r.Name, r.Field, strings.Join(tags, ","),
			r.MetricType, r.ValueType, fmtp(r.ValueDouble), fmtp(r.ValueInt), fmtp(r.ValueString))
	}
	sort.Strings(out)
	return out
}

func fmtp[T any](p *T) string {
	if p == nil {
		return "NULL"
	}
	return fmt.Sprint(*p)
}
