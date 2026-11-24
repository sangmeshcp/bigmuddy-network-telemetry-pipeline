# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/) for its configuration file
and exported metric names.

## [Unreleased]

### Added
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

## 2026 rebuild

The 2016 Cisco implementation was replaced by a vendor-neutral collector:
gNMI dial-in, Cisco MDT dial-out (gRPC/TCP/UDP), filter/tags/rename
processors, and Prometheus, OTLP, InfluxDB, Kafka and file outputs, with
a full test suite, CI and documentation. See
[Modernization](docs/about/modernization.md) for details and the migration
guide.

[Unreleased]: https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/commits/master
