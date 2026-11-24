package telemetry

import (
	"math"
	"testing"
	"time"
)

func TestRows(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	m := New("/interfaces/interface", ts)
	m.Tags = map[string]string{"source": "r1", "interface_name": "Eth1"}
	m.Fields = map[string]any{
		"a-int": int64(-3), "b-uint": uint64(7), "c-big": uint64(math.MaxUint64),
		"d-float": 2.5, "e-bool": true, "f-str": "UP", "g-bad": []int{1},
	}
	m.SetKind("b-uint", KindCounter)
	rows := Rows(m)
	if len(rows) != 6 {
		t.Fatalf("got %d rows", len(rows))
	}
	byField := map[string]Row{}
	for _, r := range rows {
		if r.Source != "r1" || r.Name != m.Name || !r.Timestamp.Equal(ts) || r.Tags["interface_name"] != "Eth1" {
			t.Fatalf("identity: %+v", r)
		}
		if _, ok := r.Tags["source"]; ok {
			t.Fatal("source must not be duplicated into tags")
		}
		byField[r.Field] = r
	}
	check := func(f, vt string, d *float64, i *int64, s *string) {
		t.Helper()
		r := byField[f]
		if r.ValueType != vt || !eqp(r.ValueDouble, d) || !eqp(r.ValueInt, i) || !eqp(r.ValueString, s) {
			t.Errorf("%s: %+v", f, r)
		}
	}
	check("a-int", "int", ptr(-3.0), ptr(int64(-3)), nil)
	check("b-uint", "uint", ptr(7.0), ptr(int64(7)), nil)
	check("c-big", "uint", ptr(float64(uint64(math.MaxUint64))), nil, ptr("18446744073709551615"))
	check("d-float", "float", ptr(2.5), nil, nil)
	check("e-bool", "bool", ptr(1.0), ptr(int64(1)), nil)
	check("f-str", "string", nil, nil, ptr("UP"))
	if byField["b-uint"].MetricType != "counter" || byField["a-int"].MetricType != "" {
		t.Error("metric_type not carried")
	}
}

func TestRowsOfSorted(t *testing.T) {
	a := New("b", time.Unix(2, 0))
	a.Tags["source"] = "r2"
	a.Fields["x"] = 1.0
	b := New("a", time.Unix(3, 0))
	b.Tags["source"] = "r1"
	b.Fields["y"], b.Fields["x"] = 1.0, 2.0
	c := New("a", time.Unix(1, 0))
	c.Tags["source"] = "r1"
	c.Fields["x"] = 3.0
	rows := RowsOf([]*Metric{a, b, c})
	got := ""
	for _, r := range rows {
		got += r.Name + r.Source + r.Field + string(rune('0'+r.Timestamp.Unix())) + " "
	}
	if want := "ar1x1 ar1x3 ar1y3 br2x2 "; got != want {
		t.Errorf("order %q, want %q", got, want)
	}
}

func eqp[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
