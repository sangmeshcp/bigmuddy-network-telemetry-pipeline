# Developer onboarding

Welcome! This page takes you from a fresh machine to a merged first change.
Plan on about an hour.

## 1. Background you'll want

You don't need to be a networking expert, but these ideas come up constantly:

| Concept | One-line version | Read more |
|---|---|---|
| YANG | Schema language for network device data (like JSON Schema for routers) | [RFC 7950](https://www.rfc-editor.org/rfc/rfc7950) |
| OpenConfig | Vendor-neutral YANG models (`/interfaces/interface/state/counters`) | [openconfig.net](https://www.openconfig.net) |
| gNMI | gRPC protocol to read and subscribe to YANG data on any device | [gNMI spec](https://github.com/openconfig/reference/blob/master/rpc/gnmi/gnmi-specification.md) |
| Cisco MDT | Cisco's earlier telemetry transports (dial-out gRPC/TCP/UDP) | [IOS XR telemetry guide](https://www.cisco.com/c/en/us/td/docs/iosxr/ncs5500/telemetry/b-telemetry-cg-ncs5500.html) |
| OTLP | OpenTelemetry's wire protocol for metrics, traces and logs | [opentelemetry.io](https://opentelemetry.io/docs/specs/otlp/) |

Then read [Architecture](../concepts/architecture.md) and
[Data model](../concepts/data-model.md). They describe the whole system in
about ten minutes.

## 2. Set up your machine

**Required**

- **Go 1.26.11+**: `go version`. `go.mod` sets the minimum (`go 1.26.11`)
  and the toolchain CI, releases and the container image build with
  (`toolchain go1.27.3`); with the default `GOTOOLCHAIN=auto`, Go downloads
  that toolchain automatically.
- `git`, `make`, `curl`.

**Optional, for specific tasks**

| Tool | Needed for |
|---|---|
| Docker | `make image`, `make smoke`, the Compose lab, backend integration tests |
| `openssl` | `scripts/gnmi-reference-target.sh` (local gNMI target) |
| `buf`, `protoc-gen-go`, `protoc-gen-go-grpc` | Only when changing `.proto` files; see [Maintenance](maintenance.md#protobufs) |
| Python 3 | Previewing these docs |

Editor: any editor with `gopls` (VS Code with the Go extension, GoLand,
Neovim). Format on save with `gofmt`.

## 3. Clone, build, test

```bash
git clone https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline.git
cd bigmuddy-network-telemetry-pipeline

make build        # ./bin/pipeline
make test         # all unit tests, about 20s; no Docker or network needed
make lint         # gofmt, go vet, staticcheck (what CI enforces)
```

If all three pass, your environment is good. Then do the
[Quickstart](../getting-started/quickstart.md) once to see the system work
end to end.

## 4. Tour of the repository

```text
cmd/
  pipeline/              main(): flags, config, admin server, signal handling; end-to-end test
  mdt-replay/            dev tool: replay recorded MDT messages (acts like a router)
internal/
  telemetry/             Metric type, name sanitising, JSON flattening    ← start reading here
  config/                YAML loading, ${ENV} expansion, strict plugin decoding, secrets
  pipeline/              plugin interfaces + registry, router, output workers, self-metrics
  input/gnmi/            gNMI subscribe client (gnmi.go) and Notification→Metric (convert.go)
  input/ciscomdt/        MDT transports (ciscomdt.go) and KV-GPB/JSON decoding (decode.go)
    testdata/            real IOS XR captures used by tests
  processor/             filter, tags, rename
  output/<name>/         prometheus, otlp, influxdb, kafka, file
  admin/                 /metrics /healthz /readyz /debug/pprof
  tlsutil/               the shared `tls:` block → crypto/tls
  testutil/              test helpers (throwaway PKI)
  gen/                   generated protobuf code: never edit by hand
proto/                   Cisco MDT .proto sources (input to `make generate`)
integration/             tests against real InfluxDB/Kafka/Prometheus/gNMI (-tags integration)
scripts/                 smoke test, local gNMI reference target
examples/                reference and minimal configs (validated in CI)
deploy/                  Compose lab, Kubernetes manifest
docs/                    this site (MkDocs)
```

**Suggested reading order:** `internal/telemetry/metric.go` →
`internal/pipeline/plugin.go` → `internal/pipeline/engine.go` →
`internal/output/file/file.go` (the simplest plugin) →
`internal/input/gnmi/convert.go`.

## 5. How a change flows

```mermaid
flowchart LR
  A[branch from master] --> B[code + tests] --> C[make lint test] --> D[push, open PR]
  D --> E[CI] --> F[review] --> G[merge] --> H[docs auto-publish]
```

1. **Branch**: `git switch -c feat/short-description`, and **sign off**
   every commit with `git commit -s` ([DCO](https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/blob/master/CONTRIBUTING.md#developer-certificate-of-origin-dco)).
2. **Write the test first** when fixing a bug: reproduce it in the nearest
   `_test.go` file.
3. **Keep the docs in step**: a new config key belongs in
   `docs/configuration/*.md` *and* `examples/pipeline.yaml` in the same PR.
4. **Run locally**: `make lint test`. Add `make race` if you touched
   concurrency.
5. **Open a PR** against `master`. CI must be green; see
   [Testing & CI](testing.md) for what each job checks and how to reproduce
   it locally.
6. **Commit messages**: imperative subject under 72 characters ("Add statsd
   output"), with a body explaining *why*. Add user-visible changes to
   `CHANGELOG.md` under **Unreleased**.

Everyone taking part follows the
[Code of Conduct](https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/blob/master/CODE_OF_CONDUCT.md);
report security issues privately as described in
[SECURITY.md](https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/blob/master/SECURITY.md).

## 6. Conventions

- **Errors**: wrap with context (`fmt.Errorf("load CA: %w", err)`). Outputs
  return `pipeline.Permanent(err)` for errors retrying can't fix.
- **Logging**: `log/slog`, structured key/value pairs. Use the logger in
  `pipeline.Env` (it already carries the plugin name). Use `Debug` for
  per-message events and `Info` for lifecycle events.
- **Metrics**: register on `env.Registry` (already labelled with the plugin
  name). Counters end in `_total`.
- **Config**: every plugin config is a struct with `yaml` tags, decoded via
  `p.Decode(&cfg)` (strict). Validate in the constructor and fail fast. Secrets
  get a `*_file` variant via `config.Secret`.
- **Concurrency**: never modify a `*telemetry.Metric` inside an output; it is
  shared with other outputs.
- **Tests**: no sleeps for synchronisation where you can poll for a condition.
  Bind to `127.0.0.1:0`, never fixed ports. Use the race detector.
- **Dependencies**: think twice. Every dependency must be maintained, widely
  used, and justified in the PR description.

## 7. Good first issues

Small, self-contained ideas that touch one package:

- [ ] `filter`: add `include_sources` / `exclude_sources` shorthands.
- [ ] `prometheus` output: optional `const_labels` added to every series.
- [ ] `file` output: built-in size-based rotation.
- [ ] `mdt-replay`: support JSON-encoded capture files with multiple messages.
- [ ] Docs: a Grafana dashboard JSON for interface counters in `deploy/compose`.

For bigger items see the roadmap in
[Modernization](../about/modernization.md#3-plan-as-executed-and-roadmap).
