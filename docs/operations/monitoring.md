# Monitoring the pipeline

The admin endpoint (`admin.listen`, e.g. `:9180`) exposes the pipeline's own
health. Scrape it with Prometheus alongside the telemetry it exports.

| Endpoint | Use |
|---|---|
| `/healthz` | Liveness: the process is serving |
| `/readyz` | Readiness: 200 while running, 503 while starting or draining |
| `/metrics` | Self-metrics (below), plus Go runtime and process metrics |
| `/debug/pprof/` | Profiling, only when `admin.pprof: true` |

## Self-metrics

Plugin metrics carry an `input="<name>"` or `output="<name>"` label naming the
configured plugin.

### Flow

| Metric | Type | Meaning |
|---|---|---|
| `pipeline_input_metrics_total{input}` | counter | Metrics received from each input |
| `pipeline_router_queue_batches` | gauge | Router backlog. Sustained > 0 means processors or outputs are the bottleneck. |
| `pipeline_router_metrics_total` | counter | Metrics that passed the processors |
| `pipeline_router_filtered_total` | counter | Metrics removed by processors |
| `pipeline_output_enqueued_total{output}` | counter | Accepted into an output queue |
| `pipeline_output_written_total{output}` | counter | Successfully delivered |
| `pipeline_output_dropped_total{output,reason}` | counter | `queue_full` (output too slow) or `write_failed` (permanent error or retries exhausted) |
| `pipeline_output_write_errors_total{output}` | counter | Failed write attempts, including ones that later succeeded |
| `pipeline_output_write_duration_seconds{output}` | histogram | Write latency |
| `pipeline_output_batch_size{output}` | histogram | Metrics per write |

### Inputs

| Metric | Meaning |
|---|---|
| `gnmi_target_up{input,target}` | 1 while the subscription is established |
| `gnmi_session_failures_total{input,target}` | Sessions that ended with an error (each is followed by a redial) |
| `gnmi_sync_responses_total{input,target}` | Initial-state syncs completed |
| `gnmi_notifications_total{input,target}` | Notifications received |
| `gnmi_unsupported_values_total{input,target}` | Values that can't be decoded generically (`any`, `proto_bytes`) |
| `mdt_connections{input}` | Open dial-out sessions |
| `mdt_messages_total{input}` | Messages received |
| `mdt_decode_errors_total{input,reason}` | `malformed`, `compact_gpb`, `framing` |

### Outputs

| Metric | Meaning |
|---|---|
| `prometheus_exporter_series{output}` | Series currently exposed |

## Recommended alerts

```yaml
groups:
  - name: telemetry-pipeline
    rules:
      - alert: TelemetryPipelineDown
        expr: up{job="pipeline-self"} == 0
        for: 2m

      - alert: GNMITargetDown
        expr: gnmi_target_up == 0
        for: 5m
        annotations:
          summary: "gNMI subscription to {{ $labels.target }} is down"

      - alert: GNMITargetFlapping
        expr: increase(gnmi_session_failures_total[15m]) > 3

      - alert: TelemetryOutputDropping
        expr: sum by (output, reason) (rate(pipeline_output_dropped_total[5m])) > 0
        for: 10m
        annotations:
          summary: "Output {{ $labels.output }} is dropping data ({{ $labels.reason }})"

      - alert: TelemetryRouterBacklog
        expr: pipeline_router_queue_batches > 100
        for: 10m

      - alert: MDTCompactGPB
        expr: increase(mdt_decode_errors_total{reason="compact_gpb"}[10m]) > 0
        annotations:
          summary: "A device is sending compact GPB; change its encoding to self-describing-gpb"
```

## Useful queries

```promql
# Ingest rate per input
sum by (input) (rate(pipeline_input_metrics_total[1m]))

# Delivery ratio per output (should be 1)
sum by (output) (rate(pipeline_output_written_total[5m]))
  / sum by (output) (rate(pipeline_output_enqueued_total[5m]))

# p99 write latency per output
histogram_quantile(0.99, sum by (output, le) (rate(pipeline_output_write_duration_seconds_bucket[5m])))

# Targets currently down
gnmi_target_up == 0
```
