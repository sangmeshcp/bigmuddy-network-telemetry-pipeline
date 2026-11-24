# Inputs

## `gnmi`: gNMI dial-in

The pipeline connects to each target and holds a long-lived `Subscribe`
stream in `STREAM` mode. When the stream breaks (device reboot, network
blip) it redials with jittered exponential backoff, between `redial_min` and
`redial_max`.

```yaml
- name: core
  type: gnmi
  targets:
    - address: core1.example.net:57400
      name: core1
    - core2.example.net:57400            # string shorthand; name = host part
  username: telemetry
  password: ${GNMI_PASSWORD}
  tls: {enabled: true, ca_file: /etc/pipeline/ca.pem}
  encoding: proto
  subscriptions:
    - {name: if-counters, path: /interfaces/interface/state/counters, mode: sample, sample_interval: 10s}
    - {name: if-status,   path: /interfaces/interface/state/oper-status, mode: on_change}
```

| Key | Default | Meaning |
|---|---|---|
| `targets` | *required* | List of `{address: host:port, name: …}` or plain `"host:port"` strings. `name` becomes the `source` tag. |
| `username` | | Sent as gRPC metadata `username` (the gNMI convention) |
| `password` / `password_file` | | Sent as metadata `password`. Without TLS a warning is logged, because the password would cross the network in clear text. |
| `tls` | disabled | See [the tls block](overview.md#the-tls-block) |
| `encoding` | `proto` | `proto`, `json_ietf`, `json` or `ascii`. Use what the device supports; `proto` is the most efficient. |
| `target` | | gNMI `prefix.target`, needed when connecting through a gNMI gateway or proxy |
| `updates_only` | `false` | Skip the initial full-state dump and send only changes |
| `compression` | | `gzip` to compress the gRPC stream |
| `redial_min` / `redial_max` | `1s` / `1m` | Reconnect backoff bounds |
| `max_message_size` | `67108864` (64 MiB) | Largest accepted gRPC message |
| `subscriptions` | *required* | See below |

Each subscription:

| Key | Default | Meaning |
|---|---|---|
| `name` | | Value of the `subscription` tag on matching metrics |
| `path` | *required* | gNMI path, e.g. `/interfaces/interface[name=Ethernet1]/state`. Keys may contain `/`; escape `]` as `\]`. Prefix with `origin:` (e.g. `openconfig:/…`) or set `origin`. |
| `origin` | | gNMI origin (`openconfig`, a vendor origin, …) |
| `mode` | `sample` | `sample`, `on_change` or `target_defined` |
| `sample_interval` | *required for sample* | How often the device sends |
| `heartbeat_interval` | | Re-send unchanged on-change values at least this often |
| `suppress_redundant` | `false` | In sample mode, skip unchanged values |

**Self-metrics** (all carry `input="<name>"`): `gnmi_target_up{target}`,
`gnmi_notifications_total{target}`, `gnmi_sync_responses_total{target}`,
`gnmi_session_failures_total{target}`, `gnmi_unsupported_values_total{target}`.

!!! tip "Choosing modes"
    Use `sample` for counters (interfaces, CPU, memory) at 10–60 s, and
    `on_change` for state (oper-status, BGP session state, LLDP neighbours).
    `target_defined` lets the device choose per leaf.

## `cisco_mdt`: Cisco Model-Driven Telemetry dial-out

Cisco devices connect to the pipeline and push data. Run one input per
transport and port.

```yaml
- name: xr-grpc
  type: cisco_mdt
  transport: grpc
  listen: ":57500"
  tls: {enabled: true, cert_file: /etc/pipeline/server.pem, key_file: /etc/pipeline/server.key}
```

| Key | Default | Meaning |
|---|---|---|
| `transport` | `grpc` | `grpc` (gRPCMdtDialout service), `tcp` or `udp` (both with the 12-byte ST header framing) |
| `listen` | *required* | Address to bind, e.g. `":57500"` |
| `tls` | disabled | Server-side TLS for `grpc` and `tcp`; add `ca_file` to require device certificates. Not available for `udp`. |
| `max_message_size` | `16777216` (16 MiB) | Larger messages are rejected; on TCP the session is closed |
| `idle_timeout` | none | `tcp` only: close connections silent for this long |

Supported payload encodings are **self-describing GPB (KV-GPB)** and
**JSON**. Compact GPB is rejected; see
[Connect your first device](../getting-started/first-device.md#option-b-cisco-mdt-dial-out).
A malformed message is counted and skipped and does not end the session. A
corrupt TCP frame header does close the connection, because framing can't be
recovered once it's lost.

**Self-metrics** (with `input="<name>"`): `mdt_connections`,
`mdt_messages_total`, `mdt_decode_errors_total{reason}` where reason is
`malformed`, `compact_gpb` or `framing`.
