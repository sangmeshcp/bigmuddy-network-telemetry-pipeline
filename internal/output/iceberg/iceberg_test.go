package iceberg

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/table"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/uptrace/bun/driver/sqliteshim"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/outputtest"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// sqlCatalogYAML points at a throwaway SQL catalog (SQLite) and a local
// warehouse. CI also runs against a REST catalog with MinIO.
func sqlCatalogYAML(dir string) string {
	return `{type: iceberg, namespace: net.raw, catalog: {type: sql, uri: "file:` + filepath.Join(dir, "cat.db") +
		`", warehouse: "file://` + dir + `/wh", properties: {sql.driver: ` + sqliteshim.ShimName + `, sql.dialect: sqlite}}}`
}

func newOut(t *testing.T, yaml string) *Output {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	return o.(*Output)
}

// ScanRows reads the whole table through an Iceberg scan.
func scanRows(t *testing.T, o *Output) []telemetry.Row {
	t.Helper()
	ctx := context.Background()
	tbl, err := o.cat.LoadTable(ctx, o.ident)
	if err != nil {
		t.Fatal(err)
	}
	at, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer at.Release()
	return arrowRows(t, at)
}

func arrowRows(t *testing.T, at arrow.Table) []telemetry.Row {
	t.Helper()
	tr := array.NewTableReader(at, 1024)
	defer tr.Release()
	idx := map[string]int{}
	for i, f := range at.Schema().Fields() {
		idx[f.Name] = i
	}
	var out []telemetry.Row
	for tr.Next() {
		rec := tr.RecordBatch()
		str := func(name string, i int) string { return rec.Column(idx[name]).(*array.String).Value(i) }
		ts := rec.Column(idx["timestamp"]).(*array.Timestamp)
		tags := rec.Column(idx["tags"]).(*array.Map)
		keys, items := tags.Keys().(*array.String), tags.Items().(*array.String)
		vd := rec.Column(idx["value_double"]).(*array.Float64)
		vi := rec.Column(idx["value_int"]).(*array.Int64)
		vs := rec.Column(idx["value_string"]).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			r := telemetry.Row{Timestamp: time.UnixMicro(int64(ts.Value(i))), Source: str("source", i), Name: str("name", i),
				Field: str("field", i), MetricType: str("metric_type", i), ValueType: str("value_type", i), Tags: map[string]string{}}
			start, end := tags.ValueOffsets(i)
			for j := start; j < end; j++ {
				r.Tags[keys.Value(int(j))] = items.Value(int(j))
			}
			if vd.IsValid(i) {
				v := vd.Value(i)
				r.ValueDouble = &v
			}
			if vi.IsValid(i) {
				v := vi.Value(i)
				r.ValueInt = &v
			}
			if vs.IsValid(i) {
				v := vs.Value(i)
				r.ValueString = &v
			}
			out = append(out, r)
		}
	}
	return out
}

func TestConformanceSQLCatalog(t *testing.T) {
	var cur *Output
	outputtest.Run(t, outputtest.Options{
		New: func(t *testing.T) pipeline.Output {
			cur = newOut(t, sqlCatalogYAML(t.TempDir()))
			return cur
		},
		ReadBack:      func(t *testing.T) []telemetry.Row { return scanRows(t, cur) },
		Idempotent:    true,
		TimePrecision: time.Microsecond,
	})
}

func TestTableLayoutAndDedup(t *testing.T) {
	o := newOut(t, sqlCatalogYAML(t.TempDir()))
	ctx := context.Background()
	golden := outputtest.GoldenBatch()
	for i := 0; i < 3; i++ {
		if err := o.Write(ctx, golden); err != nil {
			t.Fatal(err)
		}
	}
	if c, s := testutil.ToFloat64(o.commits), testutil.ToFloat64(o.skipped); c != 1 || s != 2 {
		t.Errorf("commits=%v skipped=%v, want 1 and 2", c, s)
	}
	tbl, err := o.cat.LoadTable(ctx, o.ident)
	if err != nil {
		t.Fatal(err)
	}
	md := tbl.Metadata()
	if spec := md.PartitionSpec(); spec.NumFields() != 1 || spec.Field(0).Name != "timestamp_day" {
		t.Errorf("partition spec: %v", spec)
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil || snap.Summary.Operation != table.OpAppend || snap.Summary.Properties[BatchIDProperty] == "" {
		t.Fatalf("snapshot summary: %+v", snap)
	}
	if md.Properties()["telemetry.schema-version"] != "1" {
		t.Errorf("table properties: %v", md.Properties())
	}
	if got := md.CurrentSchema().NumFields(); got != 10 {
		t.Errorf("schema fields: %d", got)
	}
	// A second, different batch is a second commit.
	other := outputtest.GoldenBatch()
	other[0].Fields["state/mtu"] = int64(1500)
	if err := o.Write(ctx, other); err != nil {
		t.Fatal(err)
	}
	if c := testutil.ToFloat64(o.commits); c != 2 {
		t.Errorf("commits after new batch: %v", c)
	}
}

func TestNoCreate(t *testing.T) {
	dir := t.TempDir()
	o := newOut(t, `{type: iceberg, namespace: x, create_table: false, catalog: {type: sql, uri: "file:`+
		filepath.Join(dir, "c.db")+`", warehouse: "file://`+dir+`", properties: {sql.driver: `+sqliteshim.ShimName+`, sql.dialect: sqlite}}}`)
	err := o.Write(context.Background(), outputtest.GoldenBatch())
	if err == nil || !pipeline.IsPermanent(err) {
		t.Fatalf("missing table with create_table=false should be permanent, got %v", err)
	}
}

func TestConfigErrors(t *testing.T) {
	for _, y := range []string{
		`{type: iceberg, catalog: {uri: "http://x"}}`,
		`{type: iceberg, namespace: n}`,
		`{type: iceberg, namespace: n, catalog: {type: hive, uri: "thrift://x"}}`,
		`{type: iceberg, namespace: n, partition: month, catalog: {uri: "http://x"}}`,
	} {
		cfg, _ := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + y))
		if _, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default()}); err == nil {
			t.Errorf("expected error for %s", y)
		}
	}
	o := newOut(t, `{type: iceberg, namespace: net, catalog: {uri: "http://cat", credential: "id:secret", warehouse: wh, properties: {s3.region: eu-west-1}}}`)
	for k, v := range map[string]string{"type": "rest", "uri": "http://cat", "credential": "id:secret", "warehouse": "wh", "s3.region": "eu-west-1"} {
		if o.props[k] != v {
			t.Errorf("prop %s=%q want %q", k, o.props[k], v)
		}
	}
}

// TestInteropExport writes the golden batch to a SQL (SQLite) catalog in
// $ICEBERG_INTEROP_DIR so another Iceberg implementation (PyIceberg in CI)
// can read it back: proof the table is valid Iceberg, not just readable by
// the library that wrote it.
func TestInteropExport(t *testing.T) {
	dir := os.Getenv("ICEBERG_INTEROP_DIR")
	if dir == "" {
		t.Skip("ICEBERG_INTEROP_DIR not set")
	}
	o := newOut(t, sqlCatalogYAML(dir))
	if err := o.Write(context.Background(), outputtest.GoldenBatch()); err != nil {
		t.Fatal(err)
	}
}
