# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/) for its configuration file
and exported metric names.

## [Unreleased]

### Added
- **Data platforms**: `parquet` output (Hive-partitioned Parquet lake on
  S3-compatible or local storage, readable by Snowflake, Databricks, BigQuery,
  Athena, Trino, ClickHouse and DuckDB) and `clickhouse` output (native
  protocol, auto-created MergeTree table). Both use one fixed analytical row
  layout, and retries never duplicate data.
- `iceberg` output: appends to Apache Iceberg tables through a REST catalog
  (Glue and SQL catalogs with build tags), with partitioning by day or hour
  and idempotent commits. Verified against an Iceberg REST catalog and with
  PyIceberg. Build with `-tags noiceberg` for a smaller binary.
- `kafka` output `format: avro`: analytical rows as Avro in the Confluent
  wire format, schema registered in a Schema Registry, ready for the
  Snowflake, Databricks, BigQuery and ClickHouse Kafka connectors. Verified
  against Redpanda, with ClickHouse decoding the records.
- Durable disk-backed output queues (`queue: {type: disk}`): survive restarts
  and destination outages.
- `metric_types` processor: marks counters (OTLP monotonic Sums,
  Prometheus counter type); `downsample` processor for cost control.

- Signed release archives for linux/darwin × amd64/arm64 with checksums and
  SBOMs; multi-arch, cosign-signed container image on GHCR; release
  rehearsal in CI.
- Helm chart (`deploy/helm/telemetry-pipeline`): config validation in an init
  container, rollouts on config change, hardened defaults, ServiceMonitor,
  PrometheusRule and Grafana dashboard options; tested on a kind cluster in CI.
- Grafana dashboards (pipeline health, interfaces for OpenConfig and IOS XR,
  BGP) and 12 Prometheus alert rules with unit tests; every dashboard query is
  checked against a live pipeline in CI.
- Community files: issue forms, PR template, code of conduct, security
  policy, governance, this changelog, DCO check and Dependabot.

### Changed
- Builds use Go 1.27.3 everywhere (CI, release binaries, container image)
  via `toolchain go1.27.3` in `go.mod`; building from source still needs
  only Go 1.26.11+.
- The Prometheus exporter serves the classic text format (not OpenMetrics),
  so series names stay stable when counters are typed.

## 2026 rebuild

The 2016 Cisco implementation was replaced by a vendor-neutral collector:
gNMI dial-in, Cisco MDT dial-out (gRPC/TCP/UDP), filter/tags/rename
processors, and Prometheus, OTLP, InfluxDB, Kafka and file outputs, with
a full test suite, CI and documentation. See
[Modernization](docs/about/modernization.md) for details and the migration
guide.

[Unreleased]: https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/commits/master
