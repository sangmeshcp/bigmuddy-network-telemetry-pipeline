package telemetry

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func sampleBatch() []*Metric {
	a := New("openconfig:/interfaces/interface", time.Unix(1700000000, 123456789))
	a.Tags = map[string]string{"source": "r1", "interface_name": "Eth1/1", "": "empty-key"}
	a.Fields = map[string]any{
		"int": int64(-42), "uint": uint64(math.MaxUint64), "float": math.Pi, "nan-free": -0.5,
		"t": true, "f": false, "s": "UP ✓", "empty": "",
	}
	a.SetKind("uint", KindCounter)
	a.SetKind("float", KindGauge)
	b := New("", time.Unix(0, 0)) // edge case: empty metric
	return []*Metric{a, b}
}

func TestCodecRoundTrip(t *testing.T) {
	in := sampleBatch()
	enc := EncodeBatch(nil, in)
	out, err := DecodeBatch(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d metrics", len(out))
	}
	for i := range in {
		a, b := in[i], out[i]
		if a.Name != b.Name || !a.Timestamp.Equal(b.Timestamp) || !reflect.DeepEqual(a.Tags, b.Tags) ||
			!reflect.DeepEqual(a.Fields, b.Fields) || !reflect.DeepEqual(a.Kinds, b.Kinds) {
			t.Errorf("metric %d differs:\n in  %+v\n out %+v", i, a, b)
		}
	}
	// Types are preserved exactly.
	if _, ok := out[0].Fields["uint"].(uint64); !ok {
		t.Error("uint64 type lost")
	}
	if _, ok := out[0].Fields["int"].(int64); !ok {
		t.Error("int64 type lost")
	}
}

func TestCodecRejectsCorruption(t *testing.T) {
	enc := EncodeBatch(nil, sampleBatch())
	for cut := 0; cut < len(enc); cut++ {
		if _, err := DecodeBatch(enc[:cut]); err == nil {
			t.Fatalf("truncation at %d/%d not detected", cut, len(enc))
		}
	}
	bad := append([]byte{}, enc...)
	bad[0] = 99
	if _, err := DecodeBatch(bad); err == nil {
		t.Error("bad version accepted")
	}
	if _, err := DecodeBatch(append(enc, 0)); err == nil {
		t.Error("trailing bytes accepted")
	}
}

func FuzzDecodeBatch(f *testing.F) {
	f.Add(EncodeBatch(nil, sampleBatch()))
	f.Add([]byte{codecVersion, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, b []byte) {
		ms, err := DecodeBatch(b) // must never panic
		if err == nil {
			// Whatever decodes must re-encode to something that decodes the same.
			again, err := DecodeBatch(EncodeBatch(nil, ms))
			if err != nil || len(again) != len(ms) {
				t.Fatalf("re-encode mismatch: %v", err)
			}
		}
	})
}
