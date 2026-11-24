package kafka

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/outputtest"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func newKafka(t *testing.T, yaml string) (pipeline.Output, error) {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	return New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
}

func consumeAll(t *testing.T, brokers []string, topic string) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var out []*kgo.Record
	for {
		f := c.PollFetches(ctx)
		if ctx.Err() != nil {
			return out
		}
		out = append(out, f.Records()...)
	}
}

func TestAvroConformance(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	var cluster *kfake.Cluster
	outputtest.Run(t, outputtest.Options{
		New: func(t *testing.T) pipeline.Output {
			if cluster != nil {
				cluster.Close()
			}
			var err error
			cluster, err = kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "rows"))
			if err != nil {
				t.Fatal(err)
			}
			o, err := newKafka(t, `{type: kafka, brokers: [`+strings.Join(cluster.ListenAddrs(), ",")+
				`], topic: rows, format: avro, schema_registry: {url: "`+reg.URL()+`"}}`)
			if err != nil {
				t.Fatal(err)
			}
			return o
		},
		ReadBack: func(t *testing.T) []telemetry.Row {
			var rows []telemetry.Row
			for _, rec := range consumeAll(t, cluster.ListenAddrs(), "rows") {
				id, r, err := DecodeAvro(rec.Value)
				if err != nil {
					t.Fatal(err)
				}
				if ss, ok := reg.GetSchema("rows-value", 1); !ok || ss.ID != id {
					t.Fatalf("record schema id %d not registered as rows-value v1 (%+v)", id, ss)
				}
				if string(rec.Key) != r.Source {
					t.Errorf("key %q, want source %q", rec.Key, r.Source)
				}
				rows = append(rows, r)
			}
			return rows
		},
		TimePrecision: time.Microsecond,
	})
	if cluster != nil {
		cluster.Close()
	}
}

func TestAvroSchemaRegisteredOnceAndCompatible(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	calls := 0
	reg.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost {
			calls++
		}
		return false
	})
	o, err := newKafka(t, `{type: kafka, brokers: [`+strings.Join(cluster.ListenAddrs(), ",")+
		`], topic: t, format: avro, schema_registry: {url: "`+reg.URL()+`", subject: net-telemetry}}`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := o.Write(ctx, outputtest.GoldenBatch()); err != nil {
			t.Fatal(err)
		}
	}
	_ = o.Close(ctx)
	if calls != 1 {
		t.Errorf("schema registered %d times, want once", calls)
	}
	if !reg.SubjectExists("net-telemetry") {
		t.Fatal("custom subject not used")
	}
	if n := len(consumeAll(t, cluster.ListenAddrs(), "t")); n != 3*19 {
		t.Errorf("got %d records, want %d", n, 3*19)
	}
}

func TestAvroIncompatibleSubjectIsPermanent(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	// What Confluent/Redpanda answer when the subject holds an incompatible
	// schema (srfake doesn't check compatibility itself).
	reg.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPost {
			return false
		}
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error_code":409,"message":"Schema being registered is incompatible with an earlier schema"}`))
		return true
	})
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	o, err := newKafka(t, `{type: kafka, brokers: [`+strings.Join(cluster.ListenAddrs(), ",")+
		`], topic: t, format: avro, schema_registry: {url: "`+reg.URL()+`"}}`)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close(context.Background())
	err = o.Write(context.Background(), outputtest.GoldenBatch())
	if err == nil || !pipeline.IsPermanent(err) {
		t.Fatalf("want permanent error for incompatible schema, got %v", err)
	}
}

func TestAvroConfig(t *testing.T) {
	for _, tc := range []struct{ yaml, want string }{
		{`{type: kafka, brokers: [b:9092], topic: t, format: avro}`, "schema_registry.url"},
		{`{type: kafka, brokers: [b:9092], topic: t, format: xml}`, "format must be"},
		{`{type: kafka, brokers: [b:9092], topic: t, schema_registry: {url: "http://r"}}`, "only used with format: avro"},
		{`{type: kafka, brokers: [b:9092], topic: t, format: avro, schema_registry: {url: "http://r", username: u, token: x}}`, "not both"},
	} {
		_, err := newKafka(t, tc.yaml)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.yaml, err, tc.want)
		}
	}
	if _, err := newKafka(t, `{type: kafka, brokers: [b:9092], topic: t, format: avro, schema_registry: {url: "http://r", username: u, password: p}}`); err != nil {
		t.Fatal(err)
	}
}
