# Troubleshooting

First steps for almost any problem:

1. `pipeline -config pipeline.yaml -check` to rule out configuration errors.
2. Restart with `log: {level: debug, format: text}`.
3. Add a `file` output with `path: "-"` to see exactly what is produced.
4. Look at the [self-metrics](monitoring.md) on the admin endpoint.

## Start-up errors

| Message | Cause / fix |
|---|---|
| `config references unset environment variables: X, Y` | Export them, or give a default: `${X:-value}` |
| `field foo not found in type …` | Unknown or misspelt key. Check the reference for that plugin. |
| `unknown input type "x" (available: [...])` | Typo in `type` |
| `delivery settings (queue_size, batch_size, ...) are only valid on outputs` | Move those keys to an output |
| `open …: no such file or directory` | A `tls` or `*_file` path doesn't exist or isn't readable |
| `address already in use` | Another process (or another input) owns that `listen` port |

## gNMI

| Symptom | Likely cause |
|---|---|
| `gnmi_target_up` stays 0; log `connection refused` | Wrong port, or gNMI not enabled on the device |
| `Unauthenticated` | Wrong username or password, or the user lacks gRPC/gNMI permission on the device |
| `certificate signed by unknown authority` | Set `tls.ca_file` to the CA that signed the device certificate (lab: `insecure_skip_verify: true`) |
| `certificate is valid for X, not Y` | Set `tls.server_name` to a name in the device certificate |
| `transport: authentication handshake failed` / `first record does not look like a TLS handshake` | TLS mismatch: device expects TLS and `tls.enabled` is false, or the reverse |
| `InvalidArgument` / `Unimplemented` from the device | Path or encoding not supported by the device. Try `encoding: json_ietf`, or check the path with the vendor's CLI. |
| Connected (`up` = 1) but no metrics | Path matches nothing on this device, or `updates_only: true` with `on_change` and nothing has changed. Remove filters, add a `file` output and try a broader path such as `/interfaces`. |
| `gnmi_session_failures_total` rising steadily | Device closing the stream: check its gNMI session limits and logs. Several pipelines subscribing to the same target can hit the limit. |
| `gnmi_unsupported_values_total` > 0 | Device sends `any`/`proto_bytes` values; switch `encoding` to `json_ietf` or `proto` with typed values |

## Cisco MDT dial-out

| Symptom | Likely cause |
|---|---|
| `mdt_connections` = 0 | Router can't reach the listener (firewall, wrong IP or port), or `protocol grpc tls` on the router vs no `tls` in the pipeline (or the reverse) |
| `mdt_decode_errors_total{reason="compact_gpb"}` | Router uses `encoding gpb`. Change it to `self-describing-gpb`. |
| `mdt_decode_errors_total{reason="framing"}` | TCP/UDP: wrong transport (e.g. router sends gRPC to a `tcp` input) or a message larger than `max_message_size` |
| `mdt_decode_errors_total{reason="malformed"}` | Payload is neither valid KV-GPB nor JSON; capture it with the debug log |

## Outputs

| Symptom | Likely cause |
|---|---|
| `dropped_total{reason="queue_full"}` | Destination slower than ingest. Raise `batch_size`, add capacity to the destination, or filter more. |
| `dropped_total{reason="write_failed"}` with log `permanent=true` | Destination rejects the data: bad token (401/403), missing bucket (404), schema conflict (400). The log line includes the response body. |
| Prometheus series vanish | Not updated within `expiry`; raise it above your longest sample interval |
| Prometheus scrape shows nothing for strings | Set `string_fields: info` |
| InfluxDB `field type conflict` | The same field was written earlier as a different type (e.g. int vs float). Write to a new bucket, or use `rename` to separate them. |
| Kafka `UNKNOWN_TOPIC_OR_PARTITION` | Create the topic first |
| OTLP to Prometheus: 404 | Start Prometheus with `--web.enable-otlp-receiver` and use path `/api/v1/otlp/v1/metrics` |

## Reproducing without a device

- MDT: `go run ./cmd/mdt-replay -addr host:57500 capture.kvgpb` replays
  recorded messages.
- gNMI: `scripts/gnmi-reference-target.sh start` runs the OpenConfig reference
  gNMI target locally; point a `gnmi` input at the address it prints.
