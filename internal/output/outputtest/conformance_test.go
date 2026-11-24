package outputtest

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// memSink is the simplest idempotent analytical sink: rows keyed by the
// batch fingerprint, as a file sink with content-addressed names would be.
type memSink struct {
	mu      sync.Mutex
	batches map[[32]byte][]telemetry.Row
}

func (s *memSink) Write(_ context.Context, b []*telemetry.Metric) error {
	if len(b) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := telemetry.RowsOf(b)
	s.batches[sha256.Sum256([]byte(telemetry.Fingerprint(rows)))] = rows
	return nil
}
func (s *memSink) Close(context.Context) error { return nil }

func TestKitAgainstReferenceSink(t *testing.T) {
	var cur *memSink
	Run(t, Options{
		New: func(t *testing.T) pipeline.Output {
			cur = &memSink{batches: map[[32]byte][]telemetry.Row{}}
			return cur
		},
		ReadBack: func(t *testing.T) []telemetry.Row {
			var all []telemetry.Row
			for _, rs := range cur.batches {
				all = append(all, rs...)
			}
			return all
		},
		Idempotent: true,
	})
}

func TestGoldenBatchCoversTheEdgeCases(t *testing.T) {
	rows := telemetry.RowsOf(GoldenBatch())
	types := map[string]bool{}
	var big, counter, hours bool
	hourSeen := map[int]bool{}
	for _, r := range rows {
		types[r.ValueType] = true
		if r.ValueString != nil && *r.ValueString == "18446744073709551615" {
			big = true
		}
		if r.MetricType == "counter" {
			counter = true
		}
		hourSeen[r.Timestamp.Hour()] = true
	}
	hours = len(hourSeen) == 2
	for _, vt := range []string{"int", "uint", "float", "bool", "string"} {
		if !types[vt] {
			t.Errorf("golden batch lacks value type %s", vt)
		}
	}
	if !big || !counter || !hours {
		t.Errorf("golden batch coverage: big uint=%v counter=%v two hours=%v", big, counter, hours)
	}
}
