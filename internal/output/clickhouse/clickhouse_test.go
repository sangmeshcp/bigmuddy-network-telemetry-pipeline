package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/outputtest"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// Tests against a real server run when CLICKHOUSE_ADDR is set (CI starts
// one; locally: clickhouse server, then CLICKHOUSE_ADDR=127.0.0.1:9000).
func addr(t *testing.T) string {
	t.Helper()
	a := os.Getenv("CLICKHOUSE_ADDR")
	if a == "" {
		t.Skip("CLICKHOUSE_ADDR not set")
	}
	return a
}

func build(t *testing.T, yaml string) (*Output, error) {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
	if err != nil {
		return nil, err
	}
	return o.(*Output), nil
}

func TestConformance(t *testing.T) {
	a := addr(t)
	var cur *Output
	outputtest.Run(t, outputtest.Options{
		New: func(t *testing.T) pipeline.Output {
			table := fmt.Sprintf("t_%d", time.Now().UnixNano())
			o, err := build(t, `{type: clickhouse, addr: ["`+a+`"], table: `+table+`}`)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { o.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+o.target) })
			cur = o
			return o
		},
		ReadBack: func(t *testing.T) []telemetry.Row {
			ctx := context.Background()
			rs, err := cur.conn.Query(ctx, "SELECT timestamp, source, name, field, tags, metric_type, value_type, "+
				"value_double, value_int, value_string FROM "+cur.target)
			if err != nil {
				t.Fatal(err)
			}
			defer rs.Close()
			var out []telemetry.Row
			for rs.Next() {
				var r telemetry.Row
				if err := rs.Scan(&r.Timestamp, &r.Source, &r.Name, &r.Field, &r.Tags, &r.MetricType, &r.ValueType,
					&r.ValueDouble, &r.ValueInt, &r.ValueString); err != nil {
					t.Fatal(err)
				}
				if len(r.Tags) == 0 {
					r.Tags = map[string]string{}
				}
				out = append(out, r)
			}
			return out
		},
		Idempotent: true,
	})
}

func TestDDLAndTTL(t *testing.T) {
	a := addr(t)
	table := fmt.Sprintf("ttl_%d", time.Now().UnixNano())
	o, err := build(t, `{type: clickhouse, addr: ["`+a+`"], table: `+table+`, ttl: 720h}`)
	if err != nil {
		t.Fatal(err)
	}
	defer o.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+o.target)
	if err := o.Write(context.Background(), outputtest.GoldenBatch()); err != nil {
		t.Fatal(err)
	}
	var ddl string
	row := o.conn.QueryRow(context.Background(), "SELECT create_table_query FROM system.tables WHERE database = 'default' AND name = ?", table)
	if err := row.Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"PARTITION BY toDate(timestamp)", "ORDER BY (name, source, field, timestamp)",
		"TTL toDateTime(timestamp) + toIntervalSecond(2592000)", "non_replicated_deduplication_window = 1000",
		"LowCardinality(String)", "Map(LowCardinality(String), String)"} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q:\n%s", want, ddl)
		}
	}
}

func TestPermanentErrors(t *testing.T) {
	a := addr(t)
	o, err := build(t, `{type: clickhouse, addr: ["`+a+`"], username: nobody, password: wrong}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Write(context.Background(), outputtest.GoldenBatch()); err == nil || !pipeline.IsPermanent(err) {
		t.Errorf("bad credentials should be permanent, got %v", err)
	}
	o2, _ := build(t, `{type: clickhouse, addr: ["`+a+`"], database: no_such_db, create_table: false}`)
	if err := o2.Write(context.Background(), outputtest.GoldenBatch()); err == nil || !pipeline.IsPermanent(err) {
		t.Errorf("unknown database should be permanent, got %v", err)
	}
}

func TestConfigErrors(t *testing.T) {
	for _, y := range []string{
		`{type: clickhouse}`,
		`{type: clickhouse, addr: [x:9000], table: "bad name"}`,
		`{type: clickhouse, addr: [x:9000], database: "a;drop"}`,
		`{type: clickhouse, addr: [x:9000], cluster: "c-1"}`,
	} {
		if _, err := build(t, y); err == nil {
			t.Errorf("expected error for %s", y)
		}
	}
	o, err := build(t, `{type: clickhouse, addr: [x:9000], cluster: prod, ttl: 24h}`)
	if err != nil {
		t.Fatal(err)
	}
	if ddl := o.DDL(); !strings.Contains(ddl, "ON CLUSTER `prod`") || !strings.Contains(ddl, "ReplicatedMergeTree") ||
		!strings.Contains(ddl, "INTERVAL 86400 SECOND") {
		t.Errorf("cluster DDL:\n%s", ddl)
	}
}
