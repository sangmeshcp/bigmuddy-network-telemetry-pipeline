# Roadmap

This is the public roadmap: what we plan to build, in what order, and why.
Each item ("ask") is written so it can be picked up directly: the problem,
the proposal, and acceptance criteria that define "done".

**Want to help?** Items marked :material-hand-heart: **help wanted** or
:material-sprout: **good first issue** are good places to start. Read
[Developer onboarding](../development/onboarding.md), then open a draft PR
that references the ask ID (e.g. `RM-06`).

## Who we're building for

| Persona | Needs | Biggest blocker today |
|---|---|---|
| **Network engineer** | Device metrics in Grafana without writing code | Must build from source; no dashboards; can't alert on `oper-status` |
| **Platform / SRE team** | Run it reliably at scale on Kubernetes | No Helm chart, hot reload, target discovery or sharding |
| **Contributor** | Add an input or output quickly and safely | No issue/PR templates, DCO or release process |

## Where we stand

| | **pipeline** | Telegraf | gNMIc | OTel Collector |
|---|---|---|---|---|
| gNMI dial-in | ✅ | ✅ | ✅ | ⚠️ community |
| Cisco MDT dial-out | ✅ | ✅ | ❌ | ❌ |
| Vendor-neutral conformance tests | ✅ | ❌ | ⚠️ | ❌ |
| Per-output isolation + backpressure | ✅ | ⚠️ | ⚠️ | ✅ |
| Prebuilt releases / Helm | ❌ → RM-01, RM-02 | ✅ | ✅ | ✅ |
| Target discovery | ❌ → RM-09 | ⚠️ | ✅ | ✅ |
| Clustering / sharding | ❌ → RM-11 | ❌ | ✅ | ⚠️ |
| State → numeric mapping | ❌ → RM-05 | ✅ | ✅ | ⚠️ |

**Strategy:** close the adoption gaps first (Now), then the scale gaps
(Next). Our differentiators are vendor-neutral conformance, strict and
safe configuration, and per-output isolation; we lean into them rather than
chasing breadth.

## How we measure success

- **Time to first graph:** under 10 minutes from `helm install` or a
  downloaded binary to a Grafana panel.
- **Contributors:** 5+ merged external PRs per quarter.
- **Performance:** published capacity numbers, guarded by CI benchmark
  regression checks.
- **Security:** zero known reachable vulnerabilities at release.

## Not planned

SNMP polling, syslog/trap ingestion, NETCONF configuration management, and a
built-in web UI. Other tools do these well; we'll integrate rather than
duplicate.

---

## Now: v0.1 "usable by anyone"

### RM-01 · Publish release binaries and signed multi-arch images
`P0` · size **M** · `area:release` · :material-hand-heart: help wanted

**Problem.** Today every user must install Go and build from source. That's
the single biggest adoption blocker.

**Proposal.** GoReleaser, triggered on `v*` tags: binaries for linux/darwin ×
amd64/arm64; multi-arch images on GHCR; checksums; an SBOM (SPDX); keyless
cosign signatures; a generated changelog.

**Done when**

- [ ] Pushing tag `vX.Y.Z` publishes a GitHub Release with binaries, checksums and SBOM
- [ ] `ghcr.io/<owner>/telemetry-pipeline:vX.Y.Z` and `:latest` exist for amd64 and arm64
- [ ] `cosign verify` succeeds against the image; the verification command is documented
- [ ] `pipeline -version` reports the tag
- [ ] Installation docs lead with "download a release"

### RM-02 · Helm chart
`P0` · size **M** · `area:deploy` · :material-hand-heart: help wanted

**Problem.** Kubernetes is the main deployment target, but users must adapt a
raw manifest by hand.

**Proposal.** `deploy/helm/telemetry-pipeline` with config passed as values,
existing-secret support, Services for dial-out and scraping, a
ServiceMonitor/PodMonitor toggle, a checksum annotation to restart pods on
config change, and resource and security defaults. Published to GHCR as an
OCI chart.

**Done when**

- [ ] `helm install` with only a targets list produces a working pipeline
- [ ] `helm lint` and a kind-based install test run in CI
- [ ] A config change triggers a rolling restart
- [ ] Values are documented (table generated with helm-docs)

### RM-03 · Ship Grafana dashboards and Prometheus alert rules
`P0` · size **M** · `area:ecosystem` · :material-sprout: good first issue · :material-hand-heart: help wanted

**Problem.** Users get metrics but no picture. Every team re-builds the same
interface and BGP dashboards.

**Proposal.** `deploy/grafana/`: **Interfaces** (throughput, errors, drops,
status), **BGP** (session state, prefixes), **System** (CPU, memory), and
**Pipeline health** (ingest, drops, queue, target up). Turn the alerts in the
[monitoring guide](../operations/monitoring.md) into a rules file. Provision
both in the Compose lab.

**Done when**

- [ ] Dashboards render with replayed data from `mdt-replay` and the gNMI reference target
- [ ] They work for both OpenConfig and Cisco-native metric names (variables or alias panels)
- [ ] The rules file passes `promtool check rules` in CI
- [ ] `docker compose up` shows populated dashboards

### RM-04 · Community health files and DCO
`P0` · size **S** · `area:community` · :material-sprout: good first issue

**Problem.** There are no issue or PR templates, code of conduct, security
policy or changelog, and no clear statement on contribution licensing.

**Proposal.** Issue forms (bug, feature, device-compatibility report), a PR
template with checklist, `CODE_OF_CONDUCT.md` (Contributor Covenant),
`SECURITY.md` (private vulnerability reporting), `CHANGELOG.md` (Keep a
Changelog), a **DCO** sign-off check (`Signed-off-by`) rather than a CLA, and
a `GOVERNANCE.md` with maintainers and how decisions are made.

**Done when**

- [ ] The GitHub "Community Standards" checklist is complete
- [ ] PRs without a sign-off fail a required check, with instructions in CONTRIBUTING
- [ ] The issue forms collect device platform, software version and pipeline version

### RM-05 · Value-mapping processor (enums and states to numbers)
`P0` · size **S** · `area:processors` · :material-sprout: good first issue

**Problem.** The most important signals are strings: `oper-status: UP`,
BGP `session-state: ESTABLISHED`. TSDBs can't graph or alert on strings.
Prometheus `_info` series help, but awkwardly.

**Proposal.** A `map` processor: per-field regex → value table, written as a
new numeric field (or replacing the original), with built-in presets for
common OpenConfig enums.

```yaml
- type: map
  fields:
    "oper-status$": {UP: 1, DOWN: 0, TESTING: 2, "*": -1}
  presets: [openconfig-interfaces, openconfig-bgp]
  keep_original: true
```

**Done when**

- [ ] Mapped numeric fields appear in every output
- [ ] An unmapped value uses `"*"` when given, otherwise it is left alone and counted
- [ ] Presets for interface oper/admin status and BGP session state are documented
- [ ] Unit tests and docs are included

### RM-06 · Counter typing (monotonic sums)
`P1` · size **S** · `area:processors`

**Problem.** Every number is exported as a gauge. OTLP backends can't apply
counter semantics; Prometheus shows `untyped`; Grafana's "rate" hints don't
work.

**Proposal.** A `metric_types` processor that attaches type hints (counter or
gauge) by field regex, with an OpenConfig/Cisco preset (e.g. `-octets$`,
`-pkts$`, `-errors$`, `-discards$` are counters). Outputs honour the hint:
OTLP `Sum{monotonic, cumulative}`, Prometheus `counter` type with `_total`
naming (optional).

**Done when**

- [ ] The `Metric` model carries per-field type hints without breaking existing outputs
- [ ] OTLP emits Sum for counters, Gauge otherwise
- [ ] The Prometheus exporter emits `# TYPE … counter`
- [ ] The preset covers interface counters on OpenConfig and IOS XR

### RM-07 · Fuzz the network-facing decoders
`P1` · size **S** · `area:security` · :material-hand-heart: help wanted

**Problem.** We parse untrusted bytes from the network: ST frames, KV-GPB,
JSON and gNMI notifications. A panic is a remote denial of service.

**Proposal.** Native Go fuzz targets for `ciscomdt.Decode`, ST header
parsing, `gnmi.Convert`, `gnmi.ParsePath` and `telemetry.FlattenJSON`,
seeded from the existing fixtures. Run in CI on a short budget, and nightly
on a long budget. Consider applying to OSS-Fuzz.

**Done when**

- [ ] Five or more fuzz targets with seed corpora are committed
- [ ] CI runs each for at least 30 s per PR; a nightly job runs 10 min each
- [ ] Any crashers found are fixed and kept as regression inputs

### RM-08 · Hot configuration reload
`P1` · size **M** · `area:core`

**Problem.** Adding one device means restarting the process: every gNMI
session re-syncs and dial-out devices reconnect.

**Proposal.** On `SIGHUP` (and optionally on file change), validate the new
config, then diff it: start and stop only the inputs, processors and outputs
whose config changed, and within `gnmi` add or remove individual targets.
An invalid config is rejected and logged, and the old one keeps running.

**Done when**

- [ ] Adding or removing a gNMI target doesn't interrupt other targets' sessions
- [ ] A bad config on reload is rejected, the old config keeps running, and the error is logged
- [ ] The metric `pipeline_config_reloads_total{result}` exists
- [ ] The Helm chart (RM-02) can use reload instead of a restart

---

## Next: v0.2 "production at scale"

### RM-09 · Target discovery (file, HTTP, NetBox)
`P1` · size **M** · `area:inputs`

**Problem.** Fleets of hundreds of devices change daily; hand-editing
`targets:` doesn't scale, and inventory already lives in NetBox or a CMDB.

**Proposal.** `targets_from:` on the gnmi input, with providers:
`file` (YAML/JSON, watched; Prometheus `file_sd`-style), `http` (polled),
and `netbox` (filter by site, role or tag; maps custom fields to tags). Each
discovered target may carry labels that become tags, replacing manual
`tags.sources` entries.

**Done when**

- [ ] Targets are added and removed live (builds on RM-08's per-target lifecycle)
- [ ] NetBox discovery works against the official NetBox container in an integration test
- [ ] Discovery failures keep the last known target set and are exposed as a metric

### RM-10 · gNMI dial-out (devices behind NAT and firewalls)
`P1` · size **L** · `area:inputs`

**Problem.** Dial-in needs the collector to reach every device; many networks
only allow devices to connect outward.

**Proposal.** Support device-initiated connections using
[openconfig/grpctunnel](https://github.com/openconfig/grpctunnel): the
device (or a local agent) opens a tunnel to the pipeline, and the gnmi input
subscribes through it, with the same subscription config.

**Done when**

- [ ] An end-to-end test runs through a tunnel using the OpenConfig reference target's tunnel support
- [ ] Targets are identified by tunnel target ID and tagged as usual
- [ ] Docs cover when to use dial-in, gNMI dial-out and MDT dial-out

### RM-11 · Horizontal scaling: shard targets across replicas
`P1` · size **L** · `area:core`

**Problem.** Today you scale gNMI collection by hand-partitioning target
lists across configs.

**Proposal.** Replicas agree on membership (Kubernetes StatefulSet ordinal
and replica count first, optionally a hashicorp/memberlist cluster) and use
rendezvous hashing so each target is owned by exactly one replica. Ownership
moves when replicas come or go.

**Done when**

- [ ] With N replicas, each target is subscribed exactly once; this is asserted in an integration test
- [ ] Killing a replica moves its targets within a configurable time
- [ ] `pipeline_shard_targets` and ownership-change metrics exist
- [ ] Helm values enable sharding

### RM-12 · Durable output queues (disk-backed WAL)
`P1` · size **L** · `area:core`

**Problem.** Output queues are in memory: a long backend outage or a pod
restart loses buffered data.

**Proposal.** An optional per-output `queue: {type: disk, path, max_size}`:
an append-only segment log with ack-on-write. It replays after a restart and
is bounded (oldest segments are dropped and counted when full).

**Done when**

- [ ] Data survives a process restart during a backend outage (integration test)
- [ ] Throughput overhead is under 20% versus memory, per the RM-18 benchmarks
- [ ] Disk usage is bounded, and drops are counted as `reason="disk_full"`

### RM-13 · Prometheus remote-write output
`P1` · size **M** · `area:outputs` · :material-hand-heart: help wanted

**Problem.** Mimir, Thanos Receive, Cortex, VictoriaMetrics and managed
Prometheus services all accept remote write. OTLP covers some of them;
remote write covers them all.

**Proposal.** A `prometheus_remote_write` output: snappy-compressed
protobuf, remote-write v1 (v2 behind a flag), basic/bearer/SigV4 auth,
retries honouring `Retry-After`, and the same naming as the exporter.

**Done when**

- [ ] Integration test against Prometheus with `--web.enable-remote-write-receiver`
- [ ] Integration test against VictoriaMetrics
- [ ] 4xx responses are permanent and 5xx/429 are retried

### RM-14 · Kafka input (pipeline chaining)
`P1` · size **M** · `area:inputs`

**Problem.** The 2016 version could consume from Kafka, letting edge
collectors feed central processors. The migration guide lists this as a gap.

**Proposal.** A `kafka` input consuming the JSON records the Kafka output
produces: consumer groups, SASL/TLS, configurable start offset, and commits
after the router accepts the batch, so delivery is at-least-once.

**Done when**

- [ ] Round trip kafka output → kafka input with identical metrics (kfake test + integration test)
- [ ] Rebalances don't lose or duplicate beyond at-least-once semantics
- [ ] The migration table in the modernization doc is updated

### RM-15 · Certificate and secret rotation without restarts
`P1` · size **S** · `area:security`

**Problem.** Certificates rotate (cert-manager, short-lived PKI). Today the
process must restart to pick up new files.

**Proposal.** `tlsutil` reloads cert, key and CA files on change (fsnotify),
using `GetCertificate`/`GetClientCertificate` and verify callbacks. `*_file`
secrets are re-read on reconnect.

**Done when**

- [ ] Rotating server and client certs takes effect for new connections without a restart (test with `testutil.NewCerts`)
- [ ] Invalid replacement files are rejected and the old ones kept, with the error logged and counted

### RM-16 · Live tap endpoint for debugging
`P1` · size **S** · `area:operability` · :material-sprout: good first issue

**Problem.** "Is the data arriving, and what does it look like?" is the most
common question. Today that means editing config to add a `file` output and
restarting.

**Proposal.** An admin endpoint
`GET /debug/tap?source=r1&name=interfaces&limit=100` that streams matching
metrics as NDJSON or SSE from the router. It's off unless enabled, and rate
limited.

**Done when**

- [ ] `curl -N` shows live metrics filtered by source, name regex or tag
- [ ] There's no measurable overhead when nobody is tapping
- [ ] It's documented in the troubleshooting guide

### RM-17 · Capabilities discovery at session start
`P2` · size **S** · `area:inputs` · :material-sprout: good first issue

**Problem.** When a subscription returns nothing, users can't tell whether
the device supports the model or encoding.

**Proposal.** Call gNMI `Capabilities` on connect: log supported encodings
and models, expose a `gnmi_target_info{gnmi_version,…}` metric, and warn if
the configured `encoding` isn't supported. Optionally `encoding: auto`.

**Done when**

- [ ] Capabilities appear in logs and as an info metric
- [ ] `encoding: auto` picks proto, then json_ietf, then json
- [ ] It's tested against the OpenConfig reference target

### RM-18 · Benchmarks and published capacity numbers
`P1` · size **M** · `area:performance` · :material-hand-heart: help wanted

**Problem.** Users ask "how many devices per core?" and we can't answer with
data.

**Proposal.** Go benchmarks for the decode, convert, router and output hot
paths; a load generator built on the reference gNMI target and `mdt-replay`;
and a CI job that compares against `master` and comments on regressions
over 10% (benchstat).

**Done when**

- [ ] The [sizing guidance](../operations/deployment.md#sizing-guidance) uses measured metrics/s per core and memory per series
- [ ] CI flags performance regressions on PRs

---

## Later: v0.3+ "ecosystem"

### RM-19 · ClickHouse output
`P2` · size **M** · `area:outputs` · :material-hand-heart: help wanted

Columnar long-term storage is popular for flow and telemetry analytics.
Native protocol, batch inserts, and a documented schema with a key/value
table and per-measurement materialised views.
**Done when:** integration test against ClickHouse, with a schema guide.

### RM-20 · OpenSearch / Elasticsearch output
`P2` · size **M** · `area:outputs` · :material-hand-heart: help wanted

The bulk API, index templates, data streams and ILM guidance. Many NOC teams
search state history in OpenSearch.
**Done when:** integration test against OpenSearch, plus index template docs.

### RM-21 · State-change events
`P2` · size **M** · `area:processors`

Turn `on_change` updates (oper-status, BGP state) into **events**
(previous → new value) emitted as OTLP logs or Kafka records, so you can
alert on "flapped 5 times in 10 minutes" without TSDB queries.
**Done when:** a `changes` processor emits events, and an OTLP logs output
path exists.

### RM-22 · Rate and delta processor
`P2` · size **S** · `area:processors` · :material-sprout: good first issue

For sinks without a rate function (Kafka consumers, some SQL stores):
compute per-second rates from counters, handling counter resets and 64-bit
wrap, keyed by series.
**Done when:** resets and wraps are tested, and memory is bounded by series
expiry.

### RM-23 · Path aliases and a model catalog
`P2` · size **M** · `area:inputs`

Long schema paths make ugly metric names, and the same concept has
different paths per vendor. Define aliases (`if_counters:
/interfaces/interface/state/counters`) for subscriptions and names, and ship
a small curated catalog for common OpenConfig and native paths across
vendors.
**Done when:** aliases work in subscriptions and the rename stage, and the
catalog is documented.

### RM-24 · Self-tracing with OpenTelemetry
`P2` · size **S** · `area:operability`

Trace a batch from input through processors to each output write (sampled),
exported via OTLP, to diagnose latency in complex pipelines.
**Done when:** spans exist for decode, route and write, and tracing is off
by default.

---

## Proposing something new

Open a feature request (or a Discussion for bigger ideas) describing the
**problem**, who has it, and what you've tried. Proposals that come with
acceptance criteria and a willingness to help build them move fastest.
