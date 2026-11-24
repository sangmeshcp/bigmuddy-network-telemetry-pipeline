# Testing & CI

## Test layers

| Layer | Where | Needs | Run with |
|---|---|---|---|
| **Unit** | `*_test.go` beside the code | nothing | `make test` / `make race` |
| **gNMI conformance** | `internal/input/gnmi/conformance_test.go` | nothing (in-process OpenConfig reference target) | part of `make test` |
| **End-to-end (in-process)** | `cmd/pipeline/main_test.go` | nothing | part of `make test` |
| **Integration** | `integration/` (build tag `integration`) | real services (env vars) | `make integration` |
| **Smoke** | `scripts/smoke.sh` | a running binary or container | `make smoke` |

### Unit tests

Every package has tests; fakes stand in for everything external:

| External system | Stand-in |
|---|---|
| gNMI device | small fake in `gnmi_test.go`, plus the OpenConfig reference target |
| IOS XR router | real captured messages in `internal/input/ciscomdt/testdata/` |
| Kafka | `kfake`, an in-memory Kafka cluster with SASL and TLS support |
| InfluxDB, OTLP/HTTP | `httptest.Server` |
| OTLP/gRPC | in-process gRPC collector |
| TLS PKI | `internal/testutil.NewCerts` (fresh CA, server and client certs per test) |

```bash
make test                                        # everything
go test ./internal/input/gnmi/ -run Conformance -v
make race                                        # with the race detector (CI does this)
make cover                                       # coverage summary
```

### gNMI conformance (vendor-neutral)

gNMI is a standard, so we test against the **standard's reference
implementation** rather than one vendor's device: the fake target published
by the OpenConfig project (`github.com/openconfig/gnmi/testing/fake`).

- **In-process** (every `make test`): scripted responses covering every
  `TypedValue` encoding, origin/target prefixes, JSON_IETF, legacy string
  paths, deletes and `sync_response`; plus a live, paced, changing stream
  over TLS.
- **Out of process** (`integration-gnmi` CI job), using the reference
  `fake_server` binary over **mutual TLS**:

    ```bash
    eval "$(scripts/gnmi-reference-target.sh start)"
    go test -tags integration -run TestGNMITarget -v ./integration/
    scripts/gnmi-reference-target.sh stop
    ```

- **Any real device**: the same `TestGNMITarget` works against any vendor:

    ```bash
    GNMI_TARGET=router:57400 GNMI_USERNAME=u GNMI_PASSWORD=p \
    GNMI_PATH=/interfaces go test -tags integration -run TestGNMITarget -v ./integration/
    ```

    Without `GNMI_TLS_CA`, certificate verification is skipped (lab
    devices). The optional `integration-device` CI job runs it against Nokia
    SR Linux.

### Integration tests

Each test **skips** unless its environment variables are set, so
`make integration` is always safe to run.

| Test | Variables | Local setup |
|---|---|---|
| `TestInfluxDB2` | `INFLUX_URL INFLUX_TOKEN INFLUX_ORG INFLUX_BUCKET` | `docker run -d -p 8086:8086 -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=ci -e DOCKER_INFLUXDB_INIT_PASSWORD=ci-password -e DOCKER_INFLUXDB_INIT_ORG=ci -e DOCKER_INFLUXDB_INIT_BUCKET=telemetry -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=ci-token influxdb:2.7` |
| `TestKafka` | `KAFKA_BROKERS` | `docker run -d -p 9092:9092 apache/kafka:3.9.0` |
| `TestPrometheusOTLP` | `PROMETHEUS_URL` | `docker run -d -p 9090:9090 prom/prometheus:v3.4.0 --config.file=/etc/prometheus/prometheus.yml --web.enable-otlp-receiver` |
| `TestGNMITarget` | `GNMI_TARGET GNMI_USERNAME GNMI_PASSWORD` (+ optional `GNMI_PATH GNMI_ENCODING GNMI_TLS_CA GNMI_TLS_CERT GNMI_TLS_KEY`) | see above |

Each one writes through the real plugin and **reads the data back** from the
system (Flux query, Kafka consumer, PromQL).

### Smoke test

`scripts/smoke.sh` checks a *running* pipeline using `examples/minimal.yaml`:
it waits for `/readyz`, replays the captured IOS XR data over gRPC, and
verifies the exported series and self-metrics.

```bash
make smoke     # builds the image, runs it, smoke-tests it, removes it
```

## CI

`.github/workflows/ci.yml` runs on every pull request and on pushes to
`master`:

| Job | Checks | Reproduce locally |
|---|---|---|
| `lint` | gofmt, `go mod tidy` is clean, `go vet`, staticcheck | `make lint && go mod tidy && git diff --exit-code` |
| `unit` (Linux + macOS) | all unit tests with `-race`; coverage summary on the job page | `make race` |
| `proto` | `buf lint`; regenerated code matches the committed code | `make generate && git diff --exit-code internal/gen` |
| `integration-backends` | InfluxDB 2.7, Kafka 3.9, Prometheus 3 | see table above |
| `integration-gnmi` | OpenConfig reference target over mTLS | `scripts/gnmi-reference-target.sh` |
| `integration-device` | Nokia SR Linux (weekly + manual dispatch only) | `integration/srlinux.clab.yml` with containerlab |
| `docker` | image builds, runs as non-root, smoke test, clean exit on SIGTERM | `make smoke` |
| `config-examples` | every shipped config passes `-check` | `make check-config` |
| `govulncheck` | known vulnerabilities in reachable code | `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` |

To run the real-device job on demand: **Actions → ci → Run workflow →**
tick *"Also test gNMI against a real NOS"*.

The docs site has its own workflow, `.github/workflows/docs.yml`; see
[Maintenance](maintenance.md#documentation).

## Writing good tests here

- Bind to `127.0.0.1:0` and read back the address; never hard-code ports.
- Poll for conditions with a deadline instead of `time.Sleep`.
- Use `t.TempDir()`, `t.Setenv()` and `t.Cleanup()`.
- To exercise TLS, use `testutil.NewCerts(t)`; it includes an `OtherCA` for
  negative tests.
- To test a new input, prefer real captured data in `testdata/`. Keep
  fixtures small (a few KB).
