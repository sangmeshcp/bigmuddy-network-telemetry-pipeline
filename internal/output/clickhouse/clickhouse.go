// Package clickhouse writes telemetry into ClickHouse over the native
// protocol, in the canonical analytical row layout (see telemetry.Row), for
// real-time SQL dashboards and analytics.
//
// Each batch is one INSERT carrying insert_deduplication_token = the batch
// fingerprint, and the auto-created table enables deduplication, so a
// retried batch is not stored twice.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func init() { pipeline.RegisterOutput("clickhouse", New) }

// Config is the clickhouse output's YAML.
type Config struct {
	Addr         []string       `yaml:"addr"` // native protocol host:port (9000, or 9440 with TLS)
	Database     string         `yaml:"database"`
	Table        string         `yaml:"table"`
	Username     string         `yaml:"username"`
	Password     string         `yaml:"password"`
	PasswordFile string         `yaml:"password_file"`
	TLS          tlsutil.Config `yaml:"tls"`
	CreateTable  *bool          `yaml:"create_table"` // default true
	TTL          time.Duration  `yaml:"ttl"`          // optional retention for the created table
	// Cluster runs the CREATE TABLE ON CLUSTER with a Replicated engine.
	Cluster     string        `yaml:"cluster"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

var ident = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Output is the ClickHouse writer.
type Output struct {
	cfg    Config
	conn   driver.Conn
	target string // `db`.`table`
	log    *slog.Logger
	ready  bool
}

// New builds the output. The connection is opened lazily on first write so
// the pipeline starts even if ClickHouse is briefly unavailable.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Output, error) {
	c := Config{Database: "default", Table: "telemetry", DialTimeout: 10 * time.Second}
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if len(c.Addr) == 0 {
		return nil, errors.New("addr is required, e.g. [clickhouse:9000]")
	}
	for _, n := range []string{c.Database, c.Table} {
		if !ident.MatchString(n) {
			return nil, fmt.Errorf("invalid identifier %q", n)
		}
	}
	if c.Cluster != "" && !ident.MatchString(c.Cluster) {
		return nil, fmt.Errorf("invalid cluster name %q", c.Cluster)
	}
	pw, err := config.Secret(c.Password, c.PasswordFile)
	if err != nil {
		return nil, fmt.Errorf("password: %w", err)
	}
	tc, err := c.TLS.Client()
	if err != nil {
		return nil, err
	}
	conn, err := ch.Open(&ch.Options{
		Addr:        c.Addr,
		Auth:        ch.Auth{Database: c.Database, Username: c.Username, Password: pw},
		TLS:         tc,
		DialTimeout: c.DialTimeout,
		Compression: &ch.Compression{Method: ch.CompressionLZ4},
		ClientInfo:  ch.ClientInfo{Products: []struct{ Name, Version string }{{Name: "telemetry-pipeline", Version: "1"}}},
	})
	if err != nil {
		return nil, err
	}
	return &Output{cfg: c, conn: conn, target: fmt.Sprintf("`%s`.`%s`", c.Database, c.Table), log: env.Logger}, nil
}

// DDL returns the CREATE TABLE statement for the configured table.
func (o *Output) DDL() string {
	engine := "MergeTree"
	onCluster := ""
	if o.cfg.Cluster != "" {
		engine = "ReplicatedMergeTree"
		onCluster = " ON CLUSTER `" + o.cfg.Cluster + "`"
	}
	ttl := ""
	if o.cfg.TTL > 0 {
		ttl = fmt.Sprintf("\nTTL toDateTime(timestamp) + INTERVAL %d SECOND", int64(o.cfg.TTL.Seconds()))
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s%s (
    timestamp    DateTime64(9, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    source       LowCardinality(String),
    name         LowCardinality(String),
    field        LowCardinality(String),
    tags         Map(LowCardinality(String), String),
    metric_type  LowCardinality(String),
    value_type   LowCardinality(String),
    value_double Nullable(Float64) CODEC(ZSTD(1)),
    value_int    Nullable(Int64) CODEC(ZSTD(1)),
    value_string Nullable(String) CODEC(ZSTD(1))
)
ENGINE = %s
PARTITION BY toDate(timestamp)
ORDER BY (name, source, field, timestamp)%s
SETTINGS non_replicated_deduplication_window = 1000
COMMENT 'telemetry-pipeline analytical rows, schema version %d'`,
		o.target, onCluster, engine, ttl, telemetry.SchemaVersion)
}

func (o *Output) ensureTable(ctx context.Context) error {
	if o.ready {
		return nil
	}
	if o.cfg.CreateTable == nil || *o.cfg.CreateTable {
		ddl := o.DDL()
		if o.cfg.Cluster != "" {
			// non_replicated_deduplication_window doesn't apply to Replicated tables
			// (they deduplicate by default).
			ddl = strings.Replace(ddl, "\nSETTINGS non_replicated_deduplication_window = 1000", "", 1)
		}
		if err := o.conn.Exec(ctx, ddl); err != nil {
			return classify(fmt.Errorf("create table: %w", err))
		}
	}
	o.ready = true
	return nil
}

// Write inserts a batch as one block with a deduplication token.
func (o *Output) Write(ctx context.Context, batch []*telemetry.Metric) error {
	rows := telemetry.RowsOf(batch)
	if len(rows) == 0 {
		return nil
	}
	if err := o.ensureTable(ctx); err != nil {
		return err
	}
	ictx := ch.Context(ctx, ch.WithSettings(ch.Settings{
		"insert_deduplication_token": telemetry.Fingerprint(rows),
	}))
	b, err := o.conn.PrepareBatch(ictx, "INSERT INTO "+o.target+
		" (timestamp, source, name, field, tags, metric_type, value_type, value_double, value_int, value_string)")
	if err != nil {
		return classify(err)
	}
	for _, r := range rows {
		tags := r.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		if err := b.Append(r.Timestamp, r.Source, r.Name, r.Field, tags, r.MetricType, r.ValueType,
			r.ValueDouble, r.ValueInt, r.ValueString); err != nil {
			b.Abort()
			return pipeline.Permanent(fmt.Errorf("append row: %w", err))
		}
	}
	if err := b.Send(); err != nil {
		return classify(err)
	}
	return nil
}

// classify marks errors that retrying can't fix as permanent.
func classify(err error) error {
	var ex *ch.Exception
	if errors.As(err, &ex) {
		switch ex.Code {
		case 516, // AUTHENTICATION_FAILED
			497, // ACCESS_DENIED
			81,  // UNKNOWN_DATABASE
			60,  // UNKNOWN_TABLE
			16,  // NO_SUCH_COLUMN_IN_TABLE
			53,  // TYPE_MISMATCH
			62:  // SYNTAX_ERROR
			return pipeline.Permanent(err)
		}
	}
	return err
}

// Close closes the connection pool.
func (o *Output) Close(context.Context) error { return o.conn.Close() }
