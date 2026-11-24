package kafka

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
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

// avroRow mirrors AvroSchema.
type avroRow struct {
	Timestamp   time.Time         `avro:"timestamp"`
	Source      string            `avro:"source"`
	Name        string            `avro:"name"`
	Field       string            `avro:"field"`
	Tags        map[string]string `avro:"tags"`
	MetricType  string            `avro:"metric_type"`
	ValueType   string            `avro:"value_type"`
	ValueDouble *float64          `avro:"value_double"`
	ValueInt    *int64            `avro:"value_int"`
	ValueString *string           `avro:"value_string"`
}

var parsedAvro = avro.MustParse(AvroSchema)

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
		tags := r.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		b, err := avro.Marshal(parsedAvro, avroRow{
			Timestamp: r.Timestamp, Source: r.Source, Name: r.Name, Field: r.Field, Tags: tags,
			MetricType: r.MetricType, ValueType: r.ValueType,
			ValueDouble: r.ValueDouble, ValueInt: r.ValueInt, ValueString: r.ValueString,
		})
		if err != nil {
			return nil, pipeline.Permanent(fmt.Errorf("avro encode: %w", err))
		}
		framed := make([]byte, 5, 5+len(b))
		framed[1], framed[2], framed[3], framed[4] = byte(id>>24), byte(id>>16), byte(id>>8), byte(id)
		out[i] = append(framed, b...)
	}
	return out, nil
}

// DecodeAvro decodes one Confluent-framed value written by format: avro. It
// is used by tests and is handy for debugging.
func DecodeAvro(v []byte) (schemaID int, r telemetry.Row, err error) {
	if len(v) < 5 || v[0] != 0 {
		return 0, r, errors.New("not a Confluent-framed value")
	}
	schemaID = int(v[1])<<24 | int(v[2])<<16 | int(v[3])<<8 | int(v[4])
	var a avroRow
	if err := avro.Unmarshal(parsedAvro, v[5:], &a); err != nil {
		return schemaID, r, err
	}
	return schemaID, telemetry.Row{Timestamp: a.Timestamp.UTC(), Source: a.Source, Name: a.Name, Field: a.Field,
		Tags: a.Tags, MetricType: a.MetricType, ValueType: a.ValueType,
		ValueDouble: a.ValueDouble, ValueInt: a.ValueInt, ValueString: a.ValueString}, nil
}
