# Outputs

Every output also accepts the [delivery settings](overview.md#output-delivery-settings)
(`queue_size`, `batch_size`, `flush_interval`, `max_retries`, `write_timeout`).
How each output represents a metric is shown in
[Data model](../concepts/data-model.md#how-each-output-represents-a-metric).

## `prometheus`: scrape endpoint

Serves the latest value of every series for Prometheus (or any
OpenMetrics-compatible scraper) to pull.

```yaml
- name: prom
  type: prometheus
  listen: ":9273"
  expiry: 5m
  string_fields: info
```

| Key | Default | Meaning |
|---|---|---|
| `listen` | `:9273` | Bind address |
| `path` | `/metrics` | HTTP path |
| `namespace` | | Prefix added to every metric name |
| `expiry` | `5m` | Series not updated for this long disappear (e.g. a removed interface). Set it to a few times your longest `sample_interval`. |
| `string_fields` | `drop` | `info` exports strings as `<name>_info{…,value="UP"} 1` |
| `export_timestamps` | `false` | Attach device timestamps. Leave this off unless you know you need it: it disables Prometheus staleness handling. |
| `tls` | disabled | Serve HTTPS |

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: network-telemetry
    scrape_interval: 10s
    static_configs: [{targets: ["pipeline:9273"]}]
```

## `otlp`: OpenTelemetry

Pushes metrics using OTLP, accepted by the OpenTelemetry Collector,
**Prometheus 3** (native receiver), Grafana Mimir / Cloud, Datadog, Elastic,
Splunk, New Relic, Honeycomb and more.

```yaml
- name: otel
  type: otlp
  protocol: grpc
  endpoint: otel-collector:4317
  resource_attributes: {deployment.environment: prod}
```

| Key | Default | Meaning |
|---|---|---|
| `protocol` | `grpc` | `grpc` (endpoint = `host:port`) or `http` (endpoint = full URL) |
| `endpoint` | *required* | Where to send |
| `headers` | | Extra headers / gRPC metadata, e.g. `{Authorization: "Bearer ${TOKEN}"}` |
| `compression` | `gzip` | `gzip` or `none` |
| `tls` | disabled | Plaintext when disabled |
| `resource_attributes` | | Added to every device's Resource |

Pushing straight into Prometheus 3 (start Prometheus with
`--web.enable-otlp-receiver`):

```yaml
- {name: prom-push, type: otlp, protocol: http, endpoint: "http://prometheus:9090/api/v1/otlp/v1/metrics"}
```

Series arrive with `job="network-telemetry"` and `instance="<source>"`.

## `influxdb`: line protocol

```yaml
- name: influx
  type: influxdb
  url: http://influxdb:8086
  org: netops
  bucket: telemetry
  token: ${INFLUX_TOKEN}
  batch_size: 5000
```

| Key | Meaning |
|---|---|
| `url` | *Required.* Base URL. |
| `org`, `bucket`, `token` / `token_file` | **InfluxDB 2.x / 3.x** (and line-protocol-compatible stores such as VictoriaMetrics, QuestDB and GreptimeDB). Writes to `/api/v2/write`. |
| `database`, `retention_policy`, `username`, `password` / `password_file` | **InfluxDB 1.x.** Writes to `/write`. Unsigned integers are sent as signed. |
| `gzip` | Default `true` |
| `tls` | HTTPS client settings |

Set either `bucket` or `database`. A 400/401/403/404 response is treated as
permanent (the batch is dropped and the error logged); 429 and 5xx are
retried.

## `kafka`: Apache Kafka / Redpanda

One JSON record per metric (format on [Data model](../concepts/data-model.md#how-each-output-represents-a-metric)).

```yaml
- name: bus
  type: kafka
  brokers: [kafka-1:9092, kafka-2:9092]
  topic: network.telemetry
  sasl: {mechanism: SCRAM-SHA-512, username: pipeline, password_file: /run/secrets/kafka}
  tls: {enabled: true}
```

| Key | Default | Meaning |
|---|---|---|
| `brokers`, `topic` | *required* | |
| `key` | `source` | Record key: `source`, `name` or `none`. Keying by source keeps each device's data ordered. |
| `compression` | `zstd` | `zstd`, `lz4`, `snappy`, `gzip`, `none` |
| `acks` | `all` | `all` (idempotent producer), `leader`, `none` |
| `client_id` | `telemetry-pipeline` | |
| `sasl` | | `mechanism`: `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`; plus `username` and `password` / `password_file` |
| `tls` | disabled | |

!!! warning "Create the topic first"
    The producer does not ask the brokers to auto-create topics. Create it
    beforehand, e.g.
    `kafka-topics.sh --create --topic network.telemetry --partitions 12 --replication-factor 3`.
    Producing to a missing topic fails and is retried, then dropped.

## `parquet`: data lake (object storage)

Writes [Apache Parquet](https://parquet.apache.org/) files in the
[analytical row layout](../concepts/data-model.md#analytical-row-layout) to
any S3-compatible store or a local directory. These files can be read by
Snowflake, Databricks, BigQuery, Athena, Trino, Spark, ClickHouse and
DuckDB; see [Data platforms](../operations/data-platforms.md) for recipes.

```yaml
- name: lake
  type: parquet
  url: s3://my-bucket/network          # or file:///var/lib/pipeline/lake
  s3: {region: us-east-1}               # credentials from the AWS chain (IRSA, env, instance role)
  batch_size: 200000                    # bigger files are cheaper to query
  flush_interval: 1m
  queue: {type: disk, dir: /var/lib/pipeline/queue/lake}   # recommended
```

| Key | Default | Meaning |
|---|---|---|
| `url` | *required* | `s3://bucket/prefix` or `file:///path` |
| `table` | `telemetry` | Directory under the prefix |
| `compression` | `zstd` | `zstd`, `snappy`, `none` |
| `timestamp_unit` | `us` | `us` (widest compatibility, e.g. BigQuery), `ns`, `ms` |
| `s3.endpoint` | `s3.amazonaws.com` | Any S3-compatible endpoint: MinIO, Cloudflare R2, `storage.googleapis.com` (GCS with HMAC keys), Ceph |
| `s3.region` | | |
| `s3.access_key`, `s3.secret_key` / `secret_key_file`, `s3.session_token` | | Static credentials. Leave empty to use the AWS chain: environment, web identity (EKS IRSA), shared files, instance/task role. |
| `s3.path_style` | `false` | Required by MinIO and most self-hosted stores |
| `s3.insecure` | `false` | Plain HTTP (local MinIO only) |
| `s3.sse` | | `AES256` for server-side encryption |
| `s3.storage_class` | | e.g. `STANDARD_IA` |
| `s3.tls` | | Custom CA / mTLS for self-hosted endpoints |

**Layout.** Files are Hive-partitioned by UTC date and hour of the device
timestamp:

```text
s3://my-bucket/network/telemetry/dt=2026-11-24/hour=09/part-3f9c…e1.parquet
```

Each write produces one file per hour present in the batch. The file name is
a hash of its content, so **a retried write replaces the same object instead
of duplicating data**. Files are written atomically (S3 PUT, or temp file +
rename locally), so readers never see a partial file.

!!! tip "File size"
    Query engines prefer files of tens to hundreds of MB. Size them with
    `batch_size` and `flush_interval`, and use a disk queue so buffered data
    survives restarts and outages.

A missing bucket or denied access is a permanent error (the batch is
dropped and logged); network errors and 5xx responses are retried.
Self-metrics: `parquet_files_written_total`, `parquet_bytes_written_total`.

## `iceberg`: Apache Iceberg tables

Appends the [analytical rows](../concepts/data-model.md#analytical-row-layout)
to an [Apache Iceberg](https://iceberg.apache.org/) table through a catalog.
Snowflake, Databricks, BigQuery (BigLake), Athena, Trino/Starburst, Spark,
Flink, Dremio, DuckDB and ClickHouse all query Iceberg tables **natively**:
schema, partitions and snapshots come from the catalog, with no load jobs.

```yaml
- name: lakehouse
  type: iceberg
  namespace: network            # dotted for nested namespaces: network.raw
  table: telemetry
  partition: day                # day (default) or hour, on the timestamp
  catalog:
    type: rest
    uri: https://catalog.example.com/api/catalog
    warehouse: network_wh
    credential_file: /run/secrets/pipeline/iceberg   # OAuth2 client_id:client_secret
    properties:                 # passed to iceberg-go: catalog and file-IO settings
      scope: PRINCIPAL_ROLE:ALL
      s3.region: us-east-1
  batch_size: 200000
  flush_interval: 5m
  queue: {type: disk, dir: /var/lib/pipeline/queue/lakehouse}
```

| Key | Default | Meaning |
|---|---|---|
| `namespace` | *required* | Iceberg namespace (`a.b` for nested) |
| `table` | `telemetry` | |
| `partition` | `day` | `day` or `hour` transform on `timestamp` |
| `create_table` | `true` | Create the namespace and table if missing |
| `location` | | Explicit location for a created table |
| `catalog.type` | `rest` | `rest`; `glue` and `sql` need a build with `-tags iceberg_glue` / `iceberg_sql` |
| `catalog.uri` | *required for rest* | Catalog endpoint |
| `catalog.warehouse` | | Warehouse name or location, as your catalog expects |
| `catalog.credential` / `credential_file` | | REST OAuth2 client credentials (`client_id:client_secret`) |
| `catalog.token` / `token_file` | | REST bearer token |
| `catalog.properties` | | Extra [iceberg-go properties](https://github.com/apache/iceberg-go), e.g. `s3.endpoint`, `s3.region`, `s3.access-key-id`, `s3.secret-access-key`, `s3.force-virtual-addressing`, `scope`, `rest.signing-name` |

The REST catalog covers most deployments:

| Catalog | Notes |
|---|---|
| Apache Polaris / Snowflake Open Catalog | OAuth2 `credential`, `scope: PRINCIPAL_ROLE:ALL` |
| Databricks Unity Catalog | Iceberg REST endpoint of your workspace, OAuth token |
| Project Nessie, Lakekeeper, Apache Gravitino | REST endpoint |
| AWS S3 Tables, AWS Glue | Their Iceberg REST endpoints (SigV4 via `catalog.properties`); or Glue's native API with `-tags iceberg_glue` |

**Created table.** Format version 2, Parquet data files with zstd, the
analytical schema (`timestamp` as `timestamptz`, `tags` as
`map<string,string>`), partitioned by `day(timestamp)` or
`hour(timestamp)`.

**Retries don't duplicate.** Each batch is one append commit whose snapshot
summary records `telemetry.batch-id` (the batch's content fingerprint).
Before appending, the last 64 snapshots are checked for it, so a batch
retried after a commit that actually succeeded is skipped
(`iceberg_duplicate_batches_skipped_total`). Commit conflicts with other
writers are retried with fresh table metadata.

!!! note "Binary size"
    Iceberg support brings in Apache Arrow and the AWS SDK, roughly doubling
    the binary (about 76 MiB). If you don't need it, build with
    `-tags noiceberg` (or `make build-slim`) for a ~32 MiB binary.

## `clickhouse`

Inserts the [analytical rows](../concepts/data-model.md#analytical-row-layout)
into [ClickHouse](https://clickhouse.com/) over the native protocol, for
real-time SQL dashboards (Grafana's ClickHouse data source, Superset, …).

```yaml
- name: ch
  type: clickhouse
  addr: ["clickhouse:9000"]           # 9440 with TLS (ClickHouse Cloud)
  database: network
  username: pipeline
  password_file: /run/secrets/pipeline/clickhouse
  ttl: 2160h                          # keep 90 days (table created with this TTL)
  batch_size: 50000
  flush_interval: 5s
```

| Key | Default | Meaning |
|---|---|---|
| `addr` | *required* | Native-protocol `host:port` list (load-balanced) |
| `database`, `table` | `default`, `telemetry` | |
| `username`, `password` / `password_file` | | |
| `tls` | disabled | Enable for ClickHouse Cloud / port 9440 |
| `create_table` | `true` | Create the table if missing (see DDL below) |
| `ttl` | none | Retention applied when the table is created |
| `cluster` | | `CREATE TABLE … ON CLUSTER` with `ReplicatedMergeTree` |
| `dial_timeout` | `10s` | |

The created table:

```sql
CREATE TABLE IF NOT EXISTS network.telemetry (
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
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (name, source, field, timestamp)
SETTINGS non_replicated_deduplication_window = 1000
```

**Retries don't duplicate.** Each batch is one INSERT with
`insert_deduplication_token` set to the batch's content fingerprint, so
ClickHouse discards a retried batch. Keep `batch_size` below ClickHouse's
`max_insert_block_size` (default about 1M rows) so a batch stays one block.

Authentication failures, access denied, and unknown database, table or
column are permanent errors; others are retried.

```sql
-- Interface throughput (bits/s) per minute
SELECT toStartOfMinute(timestamp) AS t, tags['interface_name'] AS interface,
       (max(value_double) - min(value_double)) * 8 / 60 AS bps
FROM network.telemetry
WHERE field = 'state/counters/in-octets' AND source = 'core1'
  AND timestamp > now() - INTERVAL 1 HOUR
GROUP BY t, interface ORDER BY t;
```

## `file`: JSON lines

For debugging, or as a simple archive.

```yaml
- {name: debug, type: file, path: "-"}                # stdout
- {name: archive, type: file, path: /data/telemetry.jsonl}
```

The file is opened in append mode. Rotate it with `logrotate` using
`copytruncate`.
