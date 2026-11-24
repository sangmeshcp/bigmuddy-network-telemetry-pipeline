package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

// SchemaRegistry configures the Confluent-compatible Schema Registry used by
// format: avro (Confluent, Redpanda, Apicurio in ccompat mode, Karapace).
type SchemaRegistry struct {
	URL          string         `yaml:"url"`
	Subject      string         `yaml:"subject"` // default <topic>-value (TopicNameStrategy)
	Username     string         `yaml:"username"`
	Password     string         `yaml:"password"`
	PasswordFile string         `yaml:"password_file"`
	Token        string         `yaml:"token"`
	TokenFile    string         `yaml:"token_file"`
	TLS          tlsutil.Config `yaml:"tls"`
}

// AvroSchema is the Avro schema of the analytical row (telemetry.Row),
// registered under the configured subject. Adding fields with defaults keeps
// it BACKWARD compatible.
const AvroSchema = `{
  "type": "record",
  "name": "TelemetryRow",
  "namespace": "io.github.sangmeshcp.telemetry",
  "doc": "telemetry-pipeline analytical row, schema version 1",
  "fields": [
    {"name": "timestamp",    "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "source",       "type": "string"},
    {"name": "name",         "type": "string"},
    {"name": "field",        "type": "string"},
    {"name": "tags",         "type": {"type": "map", "values": "string"}},
    {"name": "metric_type",  "type": "string"},
    {"name": "value_type",   "type": "string"},
    {"name": "value_double", "type": ["null", "double"], "default": null},
    {"name": "value_int",    "type": ["null", "long"],   "default": null},
    {"name": "value_string", "type": ["null", "string"], "default": null}
  ]
}`

// avroEncoder registers the schema once (lazily, so the pipeline starts while
// the registry is down) and frames rows in the Confluent wire format:
// magic byte 0, 4-byte big-endian schema ID, Avro binary.
type avroEncoder struct {
	cl      *sr.Client
	subject string

	mu sync.Mutex
	id int
}

func newAvroEncoder(c *SchemaRegistry, topic string) (*avroEncoder, error) {
	if c == nil || c.URL == "" {
		return nil, errors.New("format avro needs schema_registry.url")
	}
	opts := []sr.ClientOpt{sr.URLs(c.URL), sr.UserAgent("telemetry-pipeline")}
	tc, err := c.TLS.Client()
	if err != nil {
		return nil, fmt.Errorf("schema_registry: %w", err)
	}
	if tc != nil {
		opts = append(opts, sr.HTTPClient(&http.Client{Timeout: 10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tc, Proxy: http.ProxyFromEnvironment}}))
	} else {
		opts = append(opts, sr.HTTPClient(&http.Client{Timeout: 10 * time.Second}))
	}
	pw, err := config.Secret(c.Password, c.PasswordFile)
	if err != nil {
		return nil, fmt.Errorf("schema_registry password: %w", err)
	}
	tok, err := config.Secret(c.Token, c.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("schema_registry token: %w", err)
	}
	switch {
	case c.Username != "" && tok != "":
		return nil, errors.New("schema_registry: set username/password or token, not both")
	case c.Username != "":
		opts = append(opts, sr.BasicAuth(c.Username, pw))
	case tok != "":
		opts = append(opts, sr.BearerToken(tok))
	}
	cl, err := sr.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	subject := c.Subject
	if subject == "" {
		subject = topic + "-value"
	}
	return &avroEncoder{cl: cl, subject: subject}, nil
}

func (e *avroEncoder) schemaID(ctx context.Context) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.id != 0 {
		return e.id, nil
	}
	id, err := e.cl.RegisterSchema(ctx, e.subject, sr.Schema{Schema: AvroSchema, Type: sr.TypeAvro}, -1, -1)
	if err != nil {
		var re *sr.ResponseError
		if errors.As(err, &re) {
			switch re.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusConflict, http.StatusUnprocessableEntity:
				// bad credentials, or an incompatible schema already on the subject
				return 0, pipeline.Permanent(fmt.Errorf("register schema under %q: %w", e.subject, err))
			}
		}
		return 0, fmt.Errorf("register schema under %q: %w", e.subject, err)
	}
	e.id = id
	return id, nil
}

// encode returns one framed Avro value per row.
func (e *avroEncoder) encode(ctx context.Context, rows []telemetry.Row) ([][]byte, error) {
	id, err := e.schemaID(ctx)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(rows))
	for i, r := range rows {
		b := []byte{0, byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id)}
		out[i] = appendAvroRow(b, r)
	}
	return out, nil
}

// appendAvroRow appends r in Avro binary encoding of AvroSchema. The schema
// is fixed and small, so it is encoded directly rather than through a
// general-purpose (reflection, schema-parsing) Avro library: no decoder, no
// third-party code on the hot path. ClickHouse's AvroConfluent reader checks
// this encoding in the integration tests.
func appendAvroRow(b []byte, r telemetry.Row) []byte {
	b = appendLong(b, r.Timestamp.UnixMicro()) // timestamp-micros
	b = appendString(b, r.Source)
	b = appendString(b, r.Name)
	b = appendString(b, r.Field)
	if len(r.Tags) > 0 { // map: one block of len(tags) entries, then 0
		keys := make([]string, 0, len(r.Tags))
		for k := range r.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b = appendLong(b, int64(len(keys)))
		for _, k := range keys {
			b = appendString(appendString(b, k), r.Tags[k])
		}
	}
	b = appendLong(b, 0)
	b = appendString(b, r.MetricType)
	b = appendString(b, r.ValueType)
	// Unions ["null", T]: branch index 0 = null, 1 = value.
	if r.ValueDouble == nil {
		b = appendLong(b, 0)
	} else {
		b = binary.LittleEndian.AppendUint64(appendLong(b, 1), math.Float64bits(*r.ValueDouble))
	}
	if r.ValueInt == nil {
		b = appendLong(b, 0)
	} else {
		b = appendLong(appendLong(b, 1), *r.ValueInt)
	}
	if r.ValueString == nil {
		b = appendLong(b, 0)
	} else {
		b = appendString(appendLong(b, 1), *r.ValueString)
	}
	return b
}

// appendLong writes an Avro int/long: zig-zag, then base-128 varint.
func appendLong(b []byte, v int64) []byte {
	return binary.AppendUvarint(b, uint64(v<<1)^uint64(v>>63))
}

func appendString(b []byte, s string) []byte {
	return append(appendLong(b, int64(len(s))), s...)
}
