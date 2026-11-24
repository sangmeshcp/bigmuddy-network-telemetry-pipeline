# Deployment

## Topologies

=== "Single collector"

    One pipeline per site or per few hundred devices, exporting to
    Prometheus (scrape) and/or OTLP. The simplest option, and enough for most
    networks.

    ```mermaid
    flowchart LR
      D[devices] --> P[pipeline] --> PR[Prometheus] --> G[Grafana]
    ```

=== "Collectors + bus"

    Collectors close to the devices publish to Kafka; any number of
    consumers (TSDB writers, analytics, archival) read from it
    independently.

    ```mermaid
    flowchart LR
      D1[site A devices] --> P1[pipeline A]
      D2[site B devices] --> P2[pipeline B]
      P1 --> K[(Kafka)]
      P2 --> K
      K --> C1[TSDB writer]
      K --> C2[stream analytics]
      K --> C3[data lake]
    ```

=== "Collectors + OTel Collector"

    Pipelines push OTLP to an OpenTelemetry Collector, which handles
    routing, sampling and multi-backend export centrally.

    ```mermaid
    flowchart LR
      D[devices] --> P[pipeline] -- OTLP --> OC[OTel Collector]
      OC --> M[Mimir / Prometheus]
      OC --> V[vendor SaaS]
    ```

## Scaling

| Input | How to scale out |
|---|---|
| `gnmi` (dial-in) | **Partition targets** across replicas: each replica gets its own target list (one config per shard, or one Deployment per site). Two replicas must not subscribe to the same target, or you get duplicate data. |
| `cisco_mdt` (dial-out) | Stateless per connection. Put N replicas behind a TCP load balancer. On Kubernetes use `externalTrafficPolicy: Local` to keep device source IPs. |

### Sizing guidance

Start with **0.25 CPU / 256 MiB** and scale on observed usage. The main
drivers are:

- **Metrics per second** (`rate(pipeline_input_metrics_total[1m])`). The
  router and decoders are CPU-bound; one core handles on the order of tens of
  thousands of metrics/s, depending on how many fields each metric has.
- **Prometheus exporter series** (`prometheus_exporter_series`), which hold
  memory: roughly a few hundred bytes per series.
- **Output queues**: up to `queue_size` × outputs metrics held while a
  destination is slow. Lower `queue_size` if memory is tight; raise it to ride
  out longer outages.

Profile with `admin.pprof: true` and
`go tool pprof http://host:9180/debug/pprof/heap`.

## Kubernetes

`deploy/kubernetes/pipeline.yaml` is a production-shaped starting point:

- Config in a ConfigMap; credentials in a Secret mounted as files, read with
  `password_file`.
- `readinessProbe` on `/readyz` (503 while starting or draining) and
  `livenessProbe` on `/healthz`.
- `terminationGracePeriodSeconds: 45`, longer than the 30 s drain timeout.
- Non-root, read-only root filesystem, all capabilities dropped,
  `RuntimeDefault` seccomp.
- A `LoadBalancer` Service for dial-out and a `ClusterIP` Service with
  Prometheus scrape annotations.

Config changes need a pod restart; use a checksum annotation or a tool such
as Reloader to roll pods when the ConfigMap changes.

## Security checklist

- [ ] TLS on every gNMI target (`tls.enabled: true` with `ca_file`); no
      `insecure_skip_verify` outside labs.
- [ ] mTLS for dial-out (`cisco_mdt` with `tls.ca_file`) if devices support
      client certificates. Otherwise restrict the listener with network
      policy or firewall rules to device management subnets.
- [ ] Read-only telemetry user on devices.
- [ ] Credentials via `*_file` or environment, never in the config file.
- [ ] Admin port (`:9180`) and exporter (`:9273`) reachable only from your
      monitoring network, or served over TLS.
- [ ] `pprof` disabled in production unless actively debugging.
- [ ] Keep the image current; CI runs `govulncheck` on every change.

## Upgrades

The pipeline holds no state on disk. To upgrade, roll out the new
binary/image. On SIGTERM the old process drains its queues, gNMI targets are
re-subscribed by the new process, and dial-out devices reconnect on their own.
Run `pipeline -check` against your config with the new version before
rolling out.
