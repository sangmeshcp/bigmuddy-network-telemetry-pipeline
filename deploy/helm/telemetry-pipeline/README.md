# telemetry-pipeline Helm chart

Deploys the [network telemetry pipeline](https://sangameshpatil.com/bigmuddy-network-telemetry-pipeline/)
on Kubernetes.

## Install

```bash
helm install telemetry oci://ghcr.io/sangmeshcp/charts/telemetry-pipeline \
  --version 0.1.0 -n telemetry --create-namespace -f my-values.yaml
helm test telemetry -n telemetry
```

From a checkout: `helm install telemetry deploy/helm/telemetry-pipeline -f my-values.yaml`.

## Minimal values for gNMI

```yaml
config:
  admin: {listen: ":9180"}
  inputs:
    - name: core
      type: gnmi
      targets: ["core1.example.net:57400", "core2.example.net:57400"]
      username: telemetry
      password_file: /run/secrets/pipeline/gnmi-password
      tls: {enabled: true}
      subscriptions:
        - {name: if, path: /interfaces/interface/state/counters, sample_interval: 10s}
  outputs:
    - {name: prom, type: prometheus, listen: ":9273", string_fields: info}

secrets:
  existingSecret: telemetry-pipeline   # kubectl create secret generic telemetry-pipeline --from-literal=gnmi-password=...

inputPorts: []                         # gNMI is dial-in: no inbound ports needed
serviceMonitor: {enabled: true}        # if you run the Prometheus Operator
prometheusRule: {enabled: true}        # recommended alerts
grafanaDashboards: {enabled: true}     # dashboards via the Grafana sidecar
```

## How it behaves

- **Config validation.** An init container runs `pipeline -check`, so a bad
  config fails the pod with a readable error instead of crash-looping. With
  `helm upgrade --atomic` a bad config is rolled back automatically.
- **Config changes roll the pods** (checksum annotation). Pods drain their
  queues on shutdown; `terminationGracePeriodSeconds` (45 s) exceeds the drain
  timeout.
- **Secrets** are mounted at `/run/secrets/pipeline`. Use the config's
  `*_file` options, or `env`/`envFrom` with `${VAR}` references.
- **Hardened by default.** Non-root, read-only root filesystem, all
  capabilities dropped, RuntimeDefault seccomp, no service-account token.
- **Scaling.** Each replica runs the full config. For gNMI dial-in, split
  targets across releases (e.g. one per site) rather than raising
  `replicaCount`. MDT dial-out scales horizontally behind the dial-out
  Service.

## Values

| Key | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/sangmeshcp/telemetry-pipeline` | Image |
| `image.tag` | chart `appVersion` | Image tag |
| `image.digest` | `""` | Pin by digest (overrides tag) |
| `config` | MDT gRPC dial-out → Prometheus | Pipeline configuration, rendered to `pipeline.yaml` |
| `existingConfigMap` | `""` | Use your own ConfigMap (key `pipeline.yaml`) |
| `validateConfig` | `true` | Run `pipeline -check` in an init container |
| `secrets.existingSecret` | `""` | Secret mounted at `/run/secrets/pipeline` |
| `env`, `envFrom` | `[]` | Environment for `${VAR}` expansion |
| `adminPort` / `metricsPort` | `9180` / `9273` | Must match `admin.listen` and the prometheus output's `listen` |
| `inputPorts` | `[{name: mdt-grpc, port: 57500}]` | Ports inputs listen on (container ports and dial-out Service) |
| `service.type` | `ClusterIP` | Service for admin and metrics |
| `dialoutService.enabled` | `true` | Expose `inputPorts` to devices |
| `dialoutService.type` | `LoadBalancer` | |
| `dialoutService.externalTrafficPolicy` | `Local` | Keeps device source IPs |
| `dialoutService.loadBalancerSourceRanges` | `[]` | Restrict to device management subnets |
| `serviceMonitor.enabled` | `false` | Create a Prometheus Operator ServiceMonitor |
| `serviceMonitor.scrapeTelemetry` | `true` | Also scrape device telemetry (not only self-metrics) |
| `prometheusRule.enabled` | `false` | Install the recommended alert rules as a PrometheusRule |
| `prometheusRule.labels` | `{}` | e.g. `release: kube-prometheus-stack` so your Prometheus selects it |
| `grafanaDashboards.enabled` | `false` | Install the four dashboards as ConfigMaps for the Grafana sidecar |
| `grafanaDashboards.labels` | `{grafana_dashboard: "1"}` | Label the sidecar watches |
| `podDisruptionBudget.enabled` | `false` | |
| `replicaCount` | `1` | See *Scaling* |
| `resources` | 250m / 256Mi request, 1Gi limit | |
| `terminationGracePeriodSeconds` | `45` | |
| `podSecurityContext`, `securityContext` | hardened | |
| `nodeSelector`, `tolerations`, `affinity`, `topologySpreadConstraints` | empty | |
| `extraVolumes`, `extraVolumeMounts` | `[]` | e.g. TLS certificates from a Secret |
