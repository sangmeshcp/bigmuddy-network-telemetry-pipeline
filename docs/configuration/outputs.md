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

## `file`: JSON lines

For debugging, or as a simple archive.

```yaml
- {name: debug, type: file, path: "-"}                # stdout
- {name: archive, type: file, path: /data/telemetry.jsonl}
```

The file is opened in append mode. Rotate it with `logrotate` using
`copytruncate`.
