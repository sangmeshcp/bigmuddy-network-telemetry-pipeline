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

## `file`: JSON lines

For debugging, or as a simple archive.

```yaml
- {name: debug, type: file, path: "-"}                # stdout
- {name: archive, type: file, path: /data/telemetry.jsonl}
```

The file is opened in append mode. Rotate it with `logrotate` using
`copytruncate`.
