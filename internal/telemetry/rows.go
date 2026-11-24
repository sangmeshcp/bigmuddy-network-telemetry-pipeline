package telemetry

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"sort"
	"strconv"
	"time"
)

// SchemaVersion identifies the analytical row layout below. Bump it on any
// incompatible change; sinks record it so tables can be migrated knowingly.
const SchemaVersion = 1

// Row is one value in the canonical analytical ("long") layout shared by
// warehouse and lakehouse sinks (ClickHouse, Parquet/Iceberg, ...):
//
//	timestamp | source | name | field | tags | metric_type | value_type | value_double | value_int | value_string
//
// One row per field keeps the table schema fixed forever: a new YANG model
// or a new leaf never needs an ALTER TABLE. Wide, per-model views can be
// built on top in the warehouse.
type Row struct {
	Timestamp time.Time
	Source    string            // device (the "source" tag)
	Name      string            // metric name: schema path of the instance
	Field     string            // leaf path relative to Name
	Tags      map[string]string // all other tags (list keys, subscription, enrichment)
	// MetricType is "counter", "gauge" or "" (untyped), from metric_types.
	MetricType string
	// ValueType is the original type: "int", "uint", "float", "bool" or "string".
	ValueType string
	// ValueDouble holds every numeric and bool value (bool as 0/1), so
	// analysts can aggregate one column. Null for strings.
	ValueDouble *float64
	// ValueInt holds integers exactly (and bools as 0/1). Null for floats,
	// strings, and uint64 values above MaxInt64 (see ValueString).
	ValueInt *int64
	// ValueString holds strings, and the exact decimal text of uint64 values
	// above MaxInt64 (which ValueDouble can only approximate).
	ValueString *string
}

// Rows converts a metric into analytical rows, one per field, in field order.
// Fields of unsupported types are skipped.
func Rows(m *Metric) []Row {
	tags := make(map[string]string, len(m.Tags))
	for k, v := range m.Tags {
		if k != TagSource {
			tags[k] = v
		}
	}
	fields := m.SortedFieldKeys()
	out := make([]Row, 0, len(fields))
	for _, f := range fields {
		r := Row{Timestamp: m.Timestamp, Source: m.Source(), Name: m.Name, Field: f, Tags: tags,
			MetricType: m.KindOf(f).String()}
		switch v := m.Fields[f].(type) {
		case int64:
			r.ValueType = "int"
			r.ValueInt, r.ValueDouble = ptr(v), ptr(float64(v))
		case uint64:
			r.ValueType = "uint"
			r.ValueDouble = ptr(float64(v))
			if v <= math.MaxInt64 {
				r.ValueInt = ptr(int64(v))
			} else {
				r.ValueString = ptr(strconv.FormatUint(v, 10))
			}
		case float64:
			r.ValueType = "float"
			r.ValueDouble = ptr(v)
		case bool:
			r.ValueType = "bool"
			var i int64
			if v {
				i = 1
			}
			r.ValueInt, r.ValueDouble = ptr(i), ptr(float64(i))
		case string:
			r.ValueType = "string"
			r.ValueString = ptr(v)
		default:
			continue
		}
		out = append(out, r)
	}
	return out
}

// RowsOf converts a batch, sorted by (name, source, field, timestamp) so
// columnar files compress well and are cheap to scan.
func RowsOf(batch []*Metric) []Row {
	var out []Row
	for _, m := range batch {
		out = append(out, Rows(m)...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := &out[i], &out[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.Field != b.Field {
			return a.Field < b.Field
		}
		return a.Timestamp.Before(b.Timestamp)
	})
	return out
}

func ptr[T any](v T) *T { return &v }

// Fingerprint is a deterministic hash of a set of rows (as returned by
// RowsOf, which sorts them). Sinks use it as an idempotency key: content-
// addressed file names, insert-deduplication tokens. Don't hash EncodeBatch
// output for this: it follows Go's randomised map iteration order.
func Fingerprint(rows []Row) string {
	h := sha256.New()
	var buf []byte
	for i := range rows {
		r := &rows[i]
		buf = buf[:0]
		buf = binary.AppendVarint(buf, r.Timestamp.UnixNano())
		for _, s := range []string{r.Source, r.Name, r.Field, r.MetricType, r.ValueType} {
			buf = appendString(buf, s)
		}
		keys := make([]string, 0, len(r.Tags))
		for k := range r.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf = binary.AppendUvarint(buf, uint64(len(keys)))
		for _, k := range keys {
			buf = appendString(appendString(buf, k), r.Tags[k])
		}
		if r.ValueDouble != nil {
			buf = binary.LittleEndian.AppendUint64(append(buf, 1), math.Float64bits(*r.ValueDouble))
		} else {
			buf = append(buf, 0)
		}
		if r.ValueInt != nil {
			buf = binary.AppendVarint(append(buf, 1), *r.ValueInt)
		} else {
			buf = append(buf, 0)
		}
		if r.ValueString != nil {
			buf = appendString(append(buf, 1), *r.ValueString)
		} else {
			buf = append(buf, 0)
		}
		h.Write(buf)
	}
	return hex.EncodeToString(h.Sum(nil))
}
