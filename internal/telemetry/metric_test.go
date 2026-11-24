package telemetry

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSanitizeName(t *testing.T) {
	cases := map[string]string{
		"openconfig:/interfaces/interface/state/counters/in-octets":  "interfaces_interface_state_counters_in_octets",
		"/openconfig-interfaces:interfaces/interface/state":          "interfaces_interface_state",
		"Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces": "Cisco_IOS_XR_infra_statsd_oper_infra_statistics_interfaces",
		"/a//b/":            "a_b",
		"":                  "value",
		"/0/x":              "_0_x",
		"class_stats[2]/id": "class_stats_2_id",
	}
	for in, want := range cases {
		if got := SanitizeName(in); got != want {
			t.Errorf("SanitizeName(%q) = %q, want %q", in, got, want)
		}
	}
	if got := SanitizeLabel("__name"); strings.HasPrefix(got, "__") {
		t.Errorf("SanitizeLabel kept reserved prefix: %q", got)
	}
}

func TestFlattenJSON(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"a":{"b":1,"c":[true,"x"]},"big":18446744073709551615,"f":1.5,"e":[null]}`))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatal(err)
	}
	f := map[string]any{}
	FlattenJSON("", v, f)
	want := map[string]any{"a/b": int64(1), "a/c/0": true, "a/c/1": "x",
		"big": uint64(18446744073709551615), "f": 1.5, "e/0": true}
	if len(f) != len(want) {
		t.Fatalf("got %v want %v", f, want)
	}
	for k, v := range want {
		if f[k] != v {
			t.Errorf("%s: got %#v want %#v", k, f[k], v)
		}
	}
}

func TestCloneAndSeriesKey(t *testing.T) {
	m := New("x", time.Unix(1, 0))
	m.Tags["b"] = "2"
	m.Tags["a"] = "1"
	m.Fields["v"] = int64(1)
	c := m.Clone()
	c.Tags["a"] = "changed"
	c.Fields["v"] = int64(2)
	if m.Tags["a"] != "1" || m.Fields["v"] != int64(1) {
		t.Fatal("clone shares maps with original")
	}
	if m.SeriesKey() != "x\x00a=1\x00b=2" {
		t.Fatalf("unexpected series key %q", m.SeriesKey())
	}
}

func TestToFloatAndNormalize(t *testing.T) {
	floats := map[any]float64{int64(-2): -2, uint64(3): 3, 1.5: 1.5, float32(0.5): 0.5, 7: 7,
		int32(8): 8, uint32(9): 9, true: 1, false: 0}
	for in, want := range floats {
		if got, ok := ToFloat(in); !ok || got != want {
			t.Errorf("ToFloat(%#v) = %v,%v", in, got, ok)
		}
	}
	if _, ok := ToFloat("x"); ok {
		t.Error("string must not convert")
	}
	norm := map[any]any{int(1): int64(1), int32(1): int64(1), int16(1): int64(1), int8(1): int64(1),
		uint(1): uint64(1), uint32(1): uint64(1), uint16(1): uint64(1), uint8(1): uint64(1),
		float32(0.5): 0.5, "s": "s", true: true, int64(2): int64(2)}
	for in, want := range norm {
		if got, ok := Normalize(in); !ok || got != want {
			t.Errorf("Normalize(%#v) = %#v,%v", in, got, ok)
		}
	}
	if got, ok := Normalize([]byte("b")); !ok || got != "b" {
		t.Error("[]byte should become string")
	}
	if _, ok := Normalize(struct{}{}); ok {
		t.Error("struct must not normalize")
	}
}

func TestParseNumberAndJoin(t *testing.T) {
	if ParseNumber("-5") != int64(-5) || ParseNumber("1e3") != 1000.0 || ParseNumber("abc") != "abc" {
		t.Error("ParseNumber")
	}
	if JoinPath("", "b") != "b" || JoinPath("a", "") != "a" || JoinPath("a", "b") != "a/b" {
		t.Error("JoinPath")
	}
	m := New("n", time.Now())
	m.Tags["b"], m.Tags["a"] = "1", "2"
	m.Fields["z"], m.Fields["y"] = 1, 2
	if k := m.SortedTagKeys(); k[0] != "a" || m.SortedFieldKeys()[0] != "y" || m.Source() != "" {
		t.Error("sorted keys / source")
	}
	f := map[string]any{}
	FlattenJSON("", nil, f)
	if f["value"] != true {
		t.Errorf("top-level null: %v", f)
	}
}
