# Configuration overview

The pipeline reads one YAML file, given with `-config` (default
`pipeline.yaml`). An annotated example of every option lives in
[`examples/pipeline.yaml`](https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/blob/master/examples/pipeline.yaml).

## Structure

```yaml
log:        { level: info, format: json }   # optional
admin:      { listen: ":9180", pprof: false } # optional
router:     { queue_size: 1024 }             # optional
inputs:     [ ... ]                          # required, at least one
processors: [ ... ]                          # optional, run in order
outputs:    [ ... ]                          # required, at least one
```

Every entry in `inputs`, `processors` and `outputs` has:

| Key | Required | Meaning |
|---|---|---|
| `type` | yes | Plugin type, e.g. `gnmi`, `filter`, `prometheus` |
| `name` | no | Unique within its section. Defaults to `<type>-<index>`. Appears in logs and as the `input` / `output` label on self-metrics, so set it. |
| *other keys* | | Plugin-specific; see [Inputs](inputs.md), [Processors](processors.md), [Outputs](outputs.md) |

!!! important "Strict parsing"
    Unknown or misspelt keys are **errors**, not silently ignored. Run
    `pipeline -config file.yaml -check` after every change; it validates the
    file and constructs every plugin (loading certificates, compiling
    regexes) without opening any network connections.

## Top-level sections

### `log`

| Key | Default | Values |
|---|---|---|
| `level` | `info` | `debug`, `info`, `warn`, `error` |
| `format` | `json` | `json` (for log collectors), `text` (for humans) |

Logs go to stderr.

### `admin`

| Key | Default | Meaning |
|---|---|---|
| `listen` | *(disabled)* | Address for `/metrics`, `/healthz`, `/readyz`, e.g. `":9180"`. Always set this in production. |
| `pprof` | `false` | Also serve `/debug/pprof/` for CPU and memory profiling |

### `router`

| Key | Default | Meaning |
|---|---|---|
| `queue_size` | `1024` | Batches buffered between inputs and processors. When full, inputs block, which applies backpressure to devices. See [Architecture](../concepts/architecture.md#flow-control-and-failure-isolation). |

## Output delivery settings

Every output also accepts these keys, which control its queue, batching and
retry worker:

| Key | Default | Meaning |
|---|---|---|
| `queue_size` | `100000` | Metrics buffered for this output. Overflow is dropped and counted. |
| `batch_size` | `1000` | Maximum metrics per write |
| `flush_interval` | `1s` | Maximum time a partial batch waits |
| `max_retries` | `3` | Retries per batch on transient errors. `-1` disables retries. |
| `write_timeout` | `10s` | Timeout for a single write |

| `queue` | `{type: memory}` | Where to buffer: `memory`, or a durable `disk` queue (below) |

Durations use Go syntax: `500ms`, `10s`, `5m`, `1h`.

### Durable disk queues

By default an output buffers in memory: fast, but whatever is queued is lost
if the process restarts, and a destination outage longer than the retries
loses data. For outputs where that matters (warehouses, object storage,
Kafka) use a disk queue:

```yaml
outputs:
  - name: lake
    type: parquet
    # ...
    batch_size: 200000
    flush_interval: 1m
    queue:
      type: disk
      dir: /var/lib/pipeline/queue/lake   # one directory per output
      max_bytes: 10737418240              # 10 GiB (default 1 GiB)
```

| Behaviour | Memory queue | Disk queue |
|---|---|---|
| Survives a restart | ❌ | ✅ queued data is delivered after restart |
| Destination down | retried `max_retries` times, then dropped | **retried until it succeeds**; data waits on disk |
| Queue full | new data dropped (`queue_full`) | **oldest** data dropped (`disk_full`) |
| Delivery | at-most-once | at-least-once (a batch written just before a crash may be re-sent) |

Records are checksummed. Writes reach the OS immediately (safe against a
process crash) and are fsynced every second (bounds loss on power failure).
`pipeline_output_disk_queue_bytes{output}` shows the backlog.

!!! tip "Kubernetes"
    The Helm chart's root filesystem is read-only. Mount a volume for the
    queue with `extraVolumes` / `extraVolumeMounts`. Use a PersistentVolume
    if data must survive the pod being rescheduled; an `emptyDir` survives
    only container restarts.

## Environment variables and secrets

Never put secrets in the file itself.

**Environment expansion.** Any value may contain `${VAR}` or
`${VAR:-default}`:

```yaml
password: ${GNMI_PASSWORD}
url: ${INFLUX_URL:-http://localhost:8086}
```

- Expansion applies to **values only**. `${…}` inside comments is ignored, and
  a variable's content is never parsed as YAML, so a password containing
  `: ` or newlines stays a plain string.
- A variable that is **unset and has no default is a start-up error** that
  lists every missing name. A missing secret is caught immediately rather
  than sent as an empty string.
- Write `$$` for a literal `$`.
- Unquoted `${PORT}` can become a number; quoted `"${PORT}"` stays a string.
- Inside flow-style YAML (`{ a: ${X} }`) quote the reference: `{ a: "${X}" }`.

**Secret files.** Options that hold credentials have a `*_file` variant that
reads the value from a file, the native format for Kubernetes and Docker
secrets. Trailing newlines are trimmed. Set either the value or the file,
never both.

| Plugin | Inline | File |
|---|---|---|
| gnmi | `password` | `password_file` |
| influxdb | `token`, `password` | `token_file`, `password_file` |
| kafka | `sasl.password` | `sasl.password_file` |

## The `tls` block

Every network-facing plugin accepts the same `tls` block:

```yaml
tls:
  enabled: true
  ca_file: /etc/pipeline/ca.pem        # see below
  cert_file: /etc/pipeline/client.pem  # see below
  key_file: /etc/pipeline/client.key
  server_name: core1.example.net       # client only: override SNI / verified name
  insecure_skip_verify: false          # client only: LAB USE ONLY
```

The meaning of the files depends on which side of the connection the plugin
is on:

| | Client plugins (gnmi, otlp, influxdb, kafka) | Server plugins (cisco_mdt grpc/tcp, prometheus) |
|---|---|---|
| `ca_file` | CAs that may sign the **server's** certificate (default: system roots) | CAs that may sign **client** certificates. Setting it **requires** clients to present one (mTLS). |
| `cert_file` / `key_file` | Client certificate for mTLS (optional) | The server certificate (**required** when `enabled`) |

The minimum protocol version is TLS 1.2.

## Validating configuration in CI

```bash
GNMI_PASSWORD=dummy INFLUX_TOKEN=dummy pipeline -config pipeline.yaml -check
```

This is cheap and catches typos, bad regexes, unreadable certificates and
missing environment variables. The project's own CI runs it on every shipped
example.
