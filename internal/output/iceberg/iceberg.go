// Package iceberg appends telemetry to an Apache Iceberg table through a
// catalog (REST, AWS Glue, SQL), in the canonical analytical row layout.
//
// Iceberg tables are read natively by Snowflake, Databricks, BigQuery
// (BigLake), Athena, Trino/Starburst, Spark, Flink, Dremio, DuckDB and
// ClickHouse, so one output serves every one of them as a real table:
// schema, partitions and snapshots are managed by the catalog, not by
// file-naming conventions.
//
// Each batch is one append commit whose snapshot summary carries the batch
// fingerprint ("telemetry.batch-id"); before appending, recent snapshots are
// checked for it, so a retried batch is not committed twice.
package iceberg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/rest"  // REST catalog (always available)
	_ "github.com/apache/iceberg-go/io/gocloud/s3" // s3:// data files (AWS S3, MinIO, R2, ...)
	"github.com/apache/iceberg-go/table"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func init() { pipeline.RegisterOutput("iceberg", New) }

// catalogTypes lists the catalog implementations compiled in. REST is always
// present and covers most deployments (Polaris / Snowflake Open Catalog,
// Unity Catalog, Nessie, Lakekeeper, Gravitino, AWS S3 Tables and Glue's
// Iceberg REST endpoint). Glue's native API and SQL catalogs add heavy
// dependencies, so they are opt-in build tags: iceberg_glue, iceberg_sql.
var catalogTypes = map[string]bool{"rest": true}

func availableCatalogs() []string {
	var out []string
	for k := range catalogTypes {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// BatchIDProperty is the snapshot-summary key holding the batch fingerprint.
const BatchIDProperty = "telemetry.batch-id"

// dedupLookback is how many recent snapshots are checked for a retried batch.
const dedupLookback = 64

// Catalog settings.
type Catalog struct {
	Type           string            `yaml:"type"` // rest (default), glue, sql
	URI            string            `yaml:"uri"`  // REST endpoint, or sql driver DSN
	Warehouse      string            `yaml:"warehouse"`
	Credential     string            `yaml:"credential"` // REST OAuth2 client_id:client_secret
	CredentialFile string            `yaml:"credential_file"`
	Token          string            `yaml:"token"` // REST bearer token
	TokenFile      string            `yaml:"token_file"`
	Properties     map[string]string `yaml:"properties"` // any other iceberg-go catalog/IO property (s3.endpoint, s3.region, scope, ...)
}

// Config is the iceberg output's YAML.
type Config struct {
	Catalog     Catalog `yaml:"catalog"`
	Namespace   string  `yaml:"namespace"`
	Table       string  `yaml:"table"`
	CreateTable *bool   `yaml:"create_table"` // default true
	Partition   string  `yaml:"partition"`    // day (default) or hour transform on timestamp
	Location    string  `yaml:"location"`     // optional table location for created tables
}

// Output is the Iceberg writer.
type Output struct {
	cfg     Config
	props   ice.Properties
	ident   table.Identifier
	cat     catalog.Catalog
	log     *slog.Logger
	commits prometheus.Counter
	skipped prometheus.Counter
}

// New validates configuration. The catalog is contacted lazily on first
// write, so the pipeline starts while the catalog is unavailable.
func New(p *config.Plugin, env pipeline.Env) (pipeline.Output, error) {
	c := Config{Table: "telemetry", Partition: "day"}
	c.Catalog.Type = "rest"
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if c.Namespace == "" {
		return nil, errors.New("namespace is required")
	}
	if !catalogTypes[c.Catalog.Type] {
		return nil, fmt.Errorf("catalog.type %q is not available in this build (available: %s)",
			c.Catalog.Type, strings.Join(availableCatalogs(), ", "))
	}
	if c.Catalog.Type != "glue" && c.Catalog.URI == "" {
		return nil, errors.New("catalog.uri is required")
	}
	if c.Partition != "day" && c.Partition != "hour" {
		return nil, errors.New("partition must be day or hour")
	}
	props := ice.Properties{"type": c.Catalog.Type}
	for k, v := range c.Catalog.Properties {
		props[k] = v
	}
	if c.Catalog.URI != "" {
		props["uri"] = c.Catalog.URI
	}
	if c.Catalog.Warehouse != "" {
		props["warehouse"] = c.Catalog.Warehouse
	}
	cred, err := config.Secret(c.Catalog.Credential, c.Catalog.CredentialFile)
	if err != nil {
		return nil, fmt.Errorf("catalog.credential: %w", err)
	}
	if cred != "" {
		props["credential"] = cred
	}
	tok, err := config.Secret(c.Catalog.Token, c.Catalog.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("catalog.token: %w", err)
	}
	if tok != "" {
		props["token"] = tok
	}
	o := &Output{cfg: c, props: props, log: env.Logger,
		ident: append(strings.Split(c.Namespace, "."), c.Table),
		commits: prometheus.NewCounter(prometheus.CounterOpts{Name: "iceberg_commits_total",
			Help: "Append commits to the Iceberg table."}),
		skipped: prometheus.NewCounter(prometheus.CounterOpts{Name: "iceberg_duplicate_batches_skipped_total",
			Help: "Retried batches found already committed and skipped."}),
	}
	if env.Registry != nil {
		env.Registry.MustRegister(o.commits, o.skipped)
	}
	return o, nil
}

// Schema is the Iceberg schema of the analytical rows. All columns are
// optional so engines can evolve the table without rewriting it.
func Schema() *ice.Schema {
	return ice.NewSchema(0,
		ice.NestedField{ID: 1, Name: "timestamp", Type: ice.PrimitiveTypes.TimestampTz},
		ice.NestedField{ID: 2, Name: "source", Type: ice.PrimitiveTypes.String},
		ice.NestedField{ID: 3, Name: "name", Type: ice.PrimitiveTypes.String},
		ice.NestedField{ID: 4, Name: "field", Type: ice.PrimitiveTypes.String},
		ice.NestedField{ID: 5, Name: "tags", Type: &ice.MapType{KeyID: 11, KeyType: ice.PrimitiveTypes.String,
			ValueID: 12, ValueType: ice.PrimitiveTypes.String}},
		ice.NestedField{ID: 6, Name: "metric_type", Type: ice.PrimitiveTypes.String},
		ice.NestedField{ID: 7, Name: "value_type", Type: ice.PrimitiveTypes.String},
		ice.NestedField{ID: 8, Name: "value_double", Type: ice.PrimitiveTypes.Float64},
		ice.NestedField{ID: 9, Name: "value_int", Type: ice.PrimitiveTypes.Int64},
		ice.NestedField{ID: 10, Name: "value_string", Type: ice.PrimitiveTypes.String},
	)
}

func (o *Output) connect(ctx context.Context) error {
	if o.cat != nil {
		return nil
	}
	cat, err := catalog.Load(ctx, "telemetry", o.props)
	if err != nil {
		return fmt.Errorf("load %s catalog: %w", o.cfg.Catalog.Type, err)
	}
	o.cat = cat
	return nil
}

// loadOrCreate returns the table, creating namespace and table if allowed.
func (o *Output) loadOrCreate(ctx context.Context) (*table.Table, error) {
	tbl, err := o.cat.LoadTable(ctx, o.ident)
	if err == nil {
		return tbl, nil
	}
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, fmt.Errorf("load table %s: %w", strings.Join(o.ident, "."), err)
	}
	if o.cfg.CreateTable != nil && !*o.cfg.CreateTable {
		return nil, pipeline.Permanent(fmt.Errorf("table %s does not exist and create_table is false", strings.Join(o.ident, ".")))
	}
	ns := o.ident[:len(o.ident)-1]
	if exists, err := o.cat.CheckNamespaceExists(ctx, ns); err != nil {
		return nil, err
	} else if !exists {
		if err := o.cat.CreateNamespace(ctx, ns, nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			return nil, fmt.Errorf("create namespace: %w", err)
		}
	}
	schema := Schema()
	var transform ice.Transform = ice.DayTransform{}
	if o.cfg.Partition == "hour" {
		transform = ice.HourTransform{}
	}
	spec, err := ice.NewPartitionSpecOpts(ice.AddPartitionFieldBySourceID(1, "timestamp_"+o.cfg.Partition, transform, schema, nil))
	if err != nil {
		return nil, err
	}
	opts := []catalog.CreateTableOpt{catalog.WithPartitionSpec(&spec), catalog.WithProperties(ice.Properties{
		"write.format.default":            "parquet",
		"write.parquet.compression-codec": "zstd",
		"telemetry.schema-version":        fmt.Sprint(telemetry.SchemaVersion),
	})}
	if o.cfg.Location != "" {
		opts = append(opts, catalog.WithLocation(o.cfg.Location))
	}
	tbl, err = o.cat.CreateTable(ctx, o.ident, schema, opts...)
	if errors.Is(err, catalog.ErrTableAlreadyExists) { // another replica won the race
		return o.cat.LoadTable(ctx, o.ident)
	}
	if err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}
	o.log.Info("created iceberg table", "table", strings.Join(o.ident, "."), "partition", o.cfg.Partition)
	return tbl, nil
}

// Write appends a batch as one commit, unless it was already committed.
func (o *Output) Write(ctx context.Context, batch []*telemetry.Metric) error {
	rows := telemetry.RowsOf(batch)
	if len(rows) == 0 {
		return nil
	}
	if err := o.connect(ctx); err != nil {
		return err
	}
	tbl, err := o.loadOrCreate(ctx) // fresh metadata each batch: other writers may have committed
	if err != nil {
		return err
	}
	id := telemetry.Fingerprint(rows)
	if committed(tbl, id) {
		o.skipped.Inc()
		o.log.Debug("batch already committed; skipping retry", "batch_id", id)
		return nil
	}
	arrTbl, err := toArrow(rows)
	if err != nil {
		return pipeline.Permanent(err)
	}
	defer arrTbl.Release()
	if _, err := tbl.AppendTable(ctx, arrTbl, int64(len(rows)), ice.Properties{BatchIDProperty: id}); err != nil {
		// Commit conflicts with concurrent writers are retried by the engine
		// (the next attempt reloads the table and rechecks the batch id).
		return fmt.Errorf("append: %w", err)
	}
	o.commits.Inc()
	return nil
}

// committed reports whether a recent snapshot carries this batch id.
func committed(tbl *table.Table, id string) bool {
	snaps := tbl.Metadata().Snapshots()
	for i := len(snaps) - 1; i >= 0 && i >= len(snaps)-dedupLookback; i-- {
		if s := snaps[i].Summary; s != nil && s.Properties[BatchIDProperty] == id {
			return true
		}
	}
	return false
}

// Close is a no-op; every batch is a complete commit.
func (o *Output) Close(context.Context) error { return nil }

// --- Arrow conversion -------------------------------------------------------

// ArrowSchema matches Schema() (all fields nullable = Iceberg optional).
var ArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	{Name: "source", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "field", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
	{Name: "metric_type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "value_type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "value_double", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "value_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "value_string", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func toArrow(rows []telemetry.Row) (arrow.Table, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, ArrowSchema)
	defer b.Release()
	ts := b.Field(0).(*array.TimestampBuilder)
	strs := func(i int) *array.StringBuilder { return b.Field(i).(*array.StringBuilder) }
	tags := b.Field(4).(*array.MapBuilder)
	tk, tv := tags.KeyBuilder().(*array.StringBuilder), tags.ItemBuilder().(*array.StringBuilder)
	vd := b.Field(7).(*array.Float64Builder)
	vi := b.Field(8).(*array.Int64Builder)
	vs := strs(9)
	for _, r := range rows {
		ts.Append(arrow.Timestamp(r.Timestamp.UTC().Truncate(time.Microsecond).UnixMicro()))
		strs(1).Append(r.Source)
		strs(2).Append(r.Name)
		strs(3).Append(r.Field)
		tags.Append(true)
		for _, k := range sortedKeys(r.Tags) {
			tk.Append(k)
			tv.Append(r.Tags[k])
		}
		strs(5).Append(r.MetricType)
		strs(6).Append(r.ValueType)
		if r.ValueDouble != nil {
			vd.Append(*r.ValueDouble)
		} else {
			vd.AppendNull()
		}
		if r.ValueInt != nil {
			vi.Append(*r.ValueInt)
		} else {
			vi.AppendNull()
		}
		if r.ValueString != nil {
			vs.Append(*r.ValueString)
		} else {
			vs.AppendNull()
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	return array.NewTableFromRecords(ArrowSchema, []arrow.RecordBatch{rec}), nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
