// Package parquet writes telemetry as Apache Parquet files to object
// storage or a local directory, in the canonical analytical row layout
// (see telemetry.Row).
//
// Files are Hive-partitioned by UTC date and hour:
//
//	{prefix}/{table}/dt=2026-11-24/hour=09/part-<fingerprint>.parquet
//
// so every engine can prune on time: Snowflake (external tables, Snowpipe,
// COPY INTO), Databricks (Auto Loader, COPY INTO), BigQuery (external tables,
// LOAD DATA), Athena, Trino, Spark, ClickHouse s3(), DuckDB. File names are
// derived from the content, so a retried batch overwrites the same object
// rather than duplicating data.
package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func init() { pipeline.RegisterOutput("parquet", New) }

// Version is recorded in each file's created_by (set from main).
var Version = "dev"

// S3 settings (any S3-compatible store: AWS, MinIO, R2, GCS interop, Ceph).
type S3 struct {
	Endpoint        string         `yaml:"endpoint"` // default s3.amazonaws.com
	Region          string         `yaml:"region"`
	AccessKey       string         `yaml:"access_key"` // empty: AWS credential chain (env, IRSA, instance role)
	SecretKey       string         `yaml:"secret_key"`
	SecretKeyFile   string         `yaml:"secret_key_file"`
	SessionToken    string         `yaml:"session_token"`
	PathStyle       bool           `yaml:"path_style"` // required by MinIO and most self-hosted stores
	Insecure        bool           `yaml:"insecure"`   // plain HTTP (local MinIO)
	TLS             tlsutil.Config `yaml:"tls"`
	StorageClass    string         `yaml:"storage_class"`
	ServerSideCrypt string         `yaml:"sse"` // "" or "AES256"
}

// Config is the parquet output's YAML.
type Config struct {
	URL           string `yaml:"url"`            // s3://bucket/prefix or file:///path
	Table         string `yaml:"table"`          // directory under the prefix (default "telemetry")
	Compression   string `yaml:"compression"`    // zstd (default), snappy, none
	TimestampUnit string `yaml:"timestamp_unit"` // us (default; widest compatibility), ns, ms
	S3            S3     `yaml:"s3"`
}

// Output is the Parquet writer.
type Output struct {
	cfg    Config
	store  objectStore
	prefix string
	codec  compress.Codec
	log    *slog.Logger
	files  prometheus.Counter
	bytes  prometheus.Counter
}

// New builds the output.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Output, error) {
	c := Config{Table: "telemetry", Compression: "zstd", TimestampUnit: "us"}
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if c.URL == "" {
		return nil, errors.New("url is required (s3://bucket/prefix or file:///path)")
	}
	u, err := url.Parse(c.URL)
	if err != nil {
		return nil, fmt.Errorf("url: %w", err)
	}
	o := &Output{cfg: c, log: env.Logger}
	switch c.Compression {
	case "zstd":
		o.codec = &zstd.Codec{}
	case "snappy":
		o.codec = &snappy.Codec{}
	case "none":
		o.codec = nil
	default:
		return nil, fmt.Errorf("compression must be zstd, snappy or none")
	}
	switch c.TimestampUnit {
	case "us", "ns", "ms":
	default:
		return nil, fmt.Errorf("timestamp_unit must be us, ns or ms")
	}
	switch u.Scheme {
	case "file":
		if u.Path == "" {
			return nil, errors.New("file:// url needs a path")
		}
		o.store = &fileStore{root: u.Path}
	case "s3":
		if u.Host == "" {
			return nil, errors.New("s3:// url needs a bucket")
		}
		if o.store, err = newS3Store(u.Host, c.S3); err != nil {
			return nil, err
		}
		o.prefix = strings.Trim(u.Path, "/")
	default:
		return nil, fmt.Errorf("unsupported url scheme %q (s3, file)", u.Scheme)
	}
	o.files = prometheus.NewCounter(prometheus.CounterOpts{Name: "parquet_files_written_total", Help: "Parquet files written."})
	o.bytes = prometheus.NewCounter(prometheus.CounterOpts{Name: "parquet_bytes_written_total", Help: "Bytes of Parquet written."})
	if env.Registry != nil {
		env.Registry.MustRegister(o.files, o.bytes)
	}
	return o, nil
}

// Write writes one file per (date, hour) partition present in the batch.
func (o *Output) Write(ctx context.Context, batch []*telemetry.Metric) error {
	rows := telemetry.RowsOf(batch)
	if len(rows) == 0 {
		return nil
	}
	parts := map[string][]telemetry.Row{}
	var order []string
	for _, r := range rows {
		t := r.Timestamp.UTC()
		key := fmt.Sprintf("dt=%s/hour=%02d", t.Format("2006-01-02"), t.Hour())
		if _, ok := parts[key]; !ok {
			order = append(order, key)
		}
		parts[key] = append(parts[key], r)
	}
	for _, part := range order {
		prs := parts[part]
		data, err := o.encode(prs)
		if err != nil {
			return pipeline.Permanent(fmt.Errorf("encode parquet: %w", err))
		}
		key := joinKey(o.prefix, o.cfg.Table, part, "part-"+telemetry.Fingerprint(prs)[:32]+".parquet")
		if err := o.store.Put(ctx, key, data); err != nil {
			return err
		}
		o.files.Inc()
		o.bytes.Add(float64(len(data)))
		o.log.Debug("wrote parquet file", "key", key, "rows", len(prs), "bytes", len(data))
	}
	return nil
}

func joinKey(parts ...string) string {
	var nonEmpty []string
	for _, p := range parts {
		if p != "" {
			nonEmpty = append(nonEmpty, p)
		}
	}
	return strings.Join(nonEmpty, "/")
}

// Close releases nothing; files are complete once written.
func (o *Output) Close(context.Context) error { return nil }

// --- encoding ------------------------------------------------------------

// The physical schema. Strings that repeat heavily are dictionary-encoded.
type rowUS struct {
	Timestamp time.Time `parquet:"timestamp,timestamp(microsecond)"`
	rowCommon
}
type rowNS struct {
	Timestamp time.Time `parquet:"timestamp,timestamp(nanosecond)"`
	rowCommon
}
type rowMS struct {
	Timestamp time.Time `parquet:"timestamp,timestamp(millisecond)"`
	rowCommon
}

type rowCommon struct {
	Source      string            `parquet:"source,dict"`
	Name        string            `parquet:"name,dict"`
	Field       string            `parquet:"field,dict"`
	Tags        map[string]string `parquet:"tags"`
	MetricType  string            `parquet:"metric_type,dict"`
	ValueType   string            `parquet:"value_type,dict"`
	ValueDouble *float64          `parquet:"value_double,optional"`
	ValueInt    *int64            `parquet:"value_int,optional"`
	ValueString *string           `parquet:"value_string,optional"`
}

func common(r telemetry.Row) rowCommon {
	return rowCommon{Source: r.Source, Name: r.Name, Field: r.Field, Tags: r.Tags, MetricType: r.MetricType,
		ValueType: r.ValueType, ValueDouble: r.ValueDouble, ValueInt: r.ValueInt, ValueString: r.ValueString}
}

func (o *Output) encode(rows []telemetry.Row) ([]byte, error) {
	var buf bytes.Buffer
	opts := []pq.WriterOption{pq.KeyValueMetadata("telemetry.schema_version", fmt.Sprint(telemetry.SchemaVersion)),
		pq.CreatedBy("telemetry-pipeline", Version, "")}
	if o.codec != nil {
		opts = append(opts, pq.Compression(o.codec))
	}
	switch o.cfg.TimestampUnit {
	case "ns":
		return writeRows(&buf, rows, func(r telemetry.Row) rowNS { return rowNS{r.Timestamp, common(r)} }, opts)
	case "ms":
		return writeRows(&buf, rows, func(r telemetry.Row) rowMS { return rowMS{r.Timestamp, common(r)} }, opts)
	default:
		return writeRows(&buf, rows, func(r telemetry.Row) rowUS { return rowUS{r.Timestamp, common(r)} }, opts)
	}
}

func writeRows[T any](buf *bytes.Buffer, rows []telemetry.Row, conv func(telemetry.Row) T, opts []pq.WriterOption) ([]byte, error) {
	w := pq.NewGenericWriter[T](buf, opts...)
	out := make([]T, len(rows))
	for i, r := range rows {
		out[i] = conv(r)
	}
	if _, err := w.Write(out); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ReadRows decodes a file written by this output (used by tests and tools).
func ReadRows(data []byte) ([]telemetry.Row, error) {
	f, err := pq.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	type anyRow struct {
		Timestamp time.Time `parquet:"timestamp"`
		rowCommon
	}
	r := pq.NewGenericReader[anyRow](f)
	defer r.Close()
	out := make([]telemetry.Row, 0, f.NumRows())
	buf := make([]anyRow, 1024)
	for {
		n, err := r.Read(buf)
		for _, x := range buf[:n] {
			out = append(out, telemetry.Row{Timestamp: x.Timestamp, Source: x.Source, Name: x.Name, Field: x.Field,
				Tags: x.Tags, MetricType: x.MetricType, ValueType: x.ValueType, ValueDouble: x.ValueDouble,
				ValueInt: x.ValueInt, ValueString: x.ValueString})
		}
		if err != nil {
			break
		}
	}
	return out, nil
}
