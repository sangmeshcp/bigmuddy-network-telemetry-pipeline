package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"math"
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
				id, r, err := decodeAvro(rec.Value)
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

// decodeAvro is a strict reader for one Confluent-framed AvroSchema value.
func decodeAvro(v []byte) (schemaID int, r telemetry.Row, err error) {
	if len(v) < 5 || v[0] != 0 {
		return 0, r, errors.New("not a Confluent-framed value")
	}
	schemaID = int(binary.BigEndian.Uint32(v[1:5]))
	d := &avroReader{b: v[5:]}
	r.Timestamp = time.UnixMicro(d.long()).UTC()
	r.Source, r.Name, r.Field = d.str(), d.str(), d.str()
	r.Tags = map[string]string{}
	for n := d.long(); n != 0 && d.err == nil; n = d.long() {
		if n < 0 {
			n = -n
			d.long() // block byte size
		}
		for ; n > 0 && d.err == nil; n-- {
			k := d.str()
			r.Tags[k] = d.str()
		}
	}
	r.MetricType, r.ValueType = d.str(), d.str()
	if d.branch() {
		if len(d.b) < 8 {
			return schemaID, r, errors.New("short double")
		}
		f := math.Float64frombits(binary.LittleEndian.Uint64(d.b))
		d.b = d.b[8:]
		r.ValueDouble = &f
	}
	if d.branch() {
		i := d.long()
		r.ValueInt = &i
	}
	if d.branch() {
		s := d.str()
		r.ValueString = &s
	}
	if d.err == nil && len(d.b) != 0 {
		d.err = errors.New("trailing bytes")
	}
	return schemaID, r, d.err
}

type avroReader struct {
	b   []byte
	err error
}

func (d *avroReader) long() int64 {
	u, n := binary.Uvarint(d.b)
	if n <= 0 {
		if d.err == nil {
			d.err = errors.New("bad varint")
		}
		return 0
	}
	d.b = d.b[n:]
	return int64(u>>1) ^ -int64(u&1)
}

func (d *avroReader) str() string {
	n := d.long()
	if n < 0 || int64(len(d.b)) < n {
		if d.err == nil {
			d.err = errors.New("bad string length")
		}
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}

func (d *avroReader) branch() bool {
	switch d.long() {
	case 0:
		return false
	case 1:
		return true
	}
	if d.err == nil {
		d.err = errors.New("bad union branch")
	}
	return false
}

// TestAvroEncodingVector pins the byte encoding against values worked out
// by hand from the Avro specification.
func TestAvroEncodingVector(t *testing.T) {
	f, i := 1.5, int64(-3)
	r := telemetry.Row{Timestamp: time.UnixMicro(1).UTC(), Source: "r1", Name: "n", Field: "f",
		Tags: map[string]string{"b": "2", "a": "1"}, MetricType: "counter", ValueType: "int",
		ValueDouble: &f, ValueInt: &i}
	want := []byte{
		0x02,           // timestamp 1us (zig-zag 2)
		0x04, 'r', '1', // source
		0x02, 'n', // name
		0x02, 'f', // field
		0x04, 0x02, 'a', 0x02, '1', 0x02, 'b', 0x02, '2', 0x00, // map: 2 entries, sorted, end
		0x0e, 'c', 'o', 'u', 'n', 't', 'e', 'r', // metric_type
		0x06, 'i', 'n', 't', // value_type
		0x02, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f, // double branch, 1.5 little-endian
		0x02, 0x05, // long branch, -3
		0x00, // null string
	}
	if got := appendAvroRow(nil, r); !bytes.Equal(got, want) {
		t.Fatalf("encoding\n got  % x\n want % x", got, want)
	}
	// Empty tags: just the end-of-map marker. Large and negative longs round-trip.
	for _, v := range []int64{0, -1, 1, math.MaxInt64, math.MinInt64} {
		v := v
		r := telemetry.Row{Timestamp: time.Unix(0, 0).UTC(), ValueInt: &v}
		_, back, err := decodeAvro(append([]byte{0, 0, 0, 0, 1}, appendAvroRow(nil, r)...))
		if err != nil || *back.ValueInt != v || len(back.Tags) != 0 {
			t.Fatalf("%d: %v %+v", v, err, back)
		}
	}
}
