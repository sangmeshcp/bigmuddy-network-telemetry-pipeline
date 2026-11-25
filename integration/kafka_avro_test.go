//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/kafka"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/outputtest"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// TestKafkaAvroReadByClickHouse produces the golden batch with format: avro,
// then has ClickHouse decode the records with its own AvroConfluent reader,
// which looks each schema ID up in the registry. This is the path the
// ClickHouse Kafka engine, the Snowflake/Databricks/BigQuery connectors and
// Kafka Connect sinks take.
//
// With KAFKA_BROKERS and SCHEMA_REGISTRY_URL (CI: Redpanda) it uses the real
// broker and registry; otherwise in-process fakes.
func TestKafkaAvroReadByClickHouse(t *testing.T) {
	e := env(t, "CLICKHOUSE_HTTP")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	brokers, registry := os.Getenv("KAFKA_BROKERS"), os.Getenv("SCHEMA_REGISTRY_URL")
	if brokers == "" || registry == "" {
		cl, err := kfake.NewCluster(kfake.NumBrokers(1))
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		reg := srfake.New()
		defer reg.Close()
		brokers, registry = strings.Join(cl.ListenAddrs(), ","), reg.URL()
	}
	topic := fmt.Sprintf("avro-%d", time.Now().UnixNano())
	admin, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(ctx, 3, 1, nil, topic); err != nil {
		t.Fatal(err)
	}

	out, err := kafka.New(plugin(t, "output", `{type: kafka, brokers: [`+brokers+`], topic: "`+topic+
		`", format: avro, schema_registry: {url: "`+registry+`"}}`), testEnv())
	if err != nil {
		t.Fatal(err)
	}
	golden := outputtest.GoldenBatch()
	if err := out.Write(ctx, golden); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Consume every record and hand the raw values to ClickHouse.
	want := telemetry.RowsOf(golden)
	cons, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	var body bytes.Buffer
	for n := 0; n < len(want); {
		f := cons.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("consumed %d of %d records: %v", n, len(want), err)
		}
		for _, r := range f.Records() {
			body.Write(r.Value)
			n++
		}
	}

	table := "avro_" + fmt.Sprint(time.Now().UnixNano())
	chQuery(t, e["CLICKHOUSE_HTTP"], `CREATE TABLE `+table+` (
	    timestamp DateTime64(6, 'UTC'), source String, name String, field String,
	    tags Map(String, String), metric_type String, value_type String,
	    value_double Nullable(Float64), value_int Nullable(Int64), value_string Nullable(String)
	) ENGINE = Memory`)
	defer chQuery(t, e["CLICKHOUSE_HTTP"], "DROP TABLE IF EXISTS "+table)
	q := url.Values{
		"query":                           {"INSERT INTO " + table + " FORMAT AvroConfluent"},
		"format_avro_schema_registry_url": {registry},
	}
	resp, err := http.Post(e["CLICKHOUSE_HTTP"]+"/?"+q.Encode(), "application/octet-stream", &body)
	if err != nil {
		t.Fatal(err)
	}
	msg := new(bytes.Buffer)
	_, _ = msg.ReadFrom(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("ClickHouse AvroConfluent insert: %s", msg)
	}

	var wantSum float64
	for _, r := range want {
		if r.ValueDouble != nil && r.ValueType != "uint" {
			wantSum += *r.ValueDouble
		}
	}
	got := chQuery(t, e["CLICKHOUSE_HTTP"], "SELECT count(), countIf(metric_type = 'counter'), "+
		"sumIf(value_double, value_type != 'uint'), "+
		"anyIf(value_string, field = 'state/counters/huge'), "+
		"anyIf(tags['site'], source = 'core1'), "+
		"countIf(value_int IS NULL), "+
		"toString(min(timestamp)) FROM "+table)
	nullInts := 0
	for _, r := range want {
		if r.ValueInt == nil {
			nullInts++
		}
	}
	exp := fmt.Sprintf("%d\t%d\t%g\t18446744073709551615\tdfw-1 \"east\", rack=4 ✓\t%d\t2026-11-24 09:59:59.123456",
		len(want), countKind(want, "counter"), wantSum, nullInts)
	t.Logf("clickhouse: %q", got)
	if got != exp {
		t.Errorf("ClickHouse decode of the Avro records:\n got  %q\n want %q", got, exp)
	}
}
