# Quickstart (5 minutes)

This walk-through runs the pipeline on your laptop, feeds it **real telemetry
captured from Cisco IOS XR routers**, and shows the result as Prometheus
metrics. You don't need a router.

## 1. Prerequisites

- **Go 1.26.11+** ([install](https://go.dev/doc/install)). Check with `go version`.
- `git`, `make`, `curl`.

## 2. Build

```bash
git clone https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline.git
cd bigmuddy-network-telemetry-pipeline
make build            # produces ./bin/pipeline
./bin/pipeline -version
```

## 3. Write a configuration

Create `quickstart.yaml`:

```yaml
log: {level: info, format: text}
admin: {listen: "127.0.0.1:9180"}       # the pipeline's own health & metrics

inputs:
  - name: dialout                        # act like a collector routers dial out to
    type: cisco_mdt
    transport: grpc
    listen: "127.0.0.1:57500"

outputs:
  - name: prom                           # expose everything for Prometheus to scrape
    type: prometheus
    listen: "127.0.0.1:9273"
    string_fields: info
  - name: debug                          # and print every metric as JSON
    type: file
    path: "-"
```

Check it before running. `-check` validates the file and builds every plugin
without starting anything:

```bash
./bin/pipeline -config quickstart.yaml -check
# configuration OK
```

## 4. Run it

```bash
./bin/pipeline -config quickstart.yaml
```

Leave it running and open a second terminal.

## 5. Send it data

`mdt-replay` pretends to be a router. It sends recorded IOS XR messages
(interface counters, memory, BGP) over the same gRPC dial-out protocol a
real router uses:

```bash
go run ./cmd/mdt-replay -addr 127.0.0.1:57500 internal/input/ciscomdt/testdata/*.kvgpb
# sent 3 messages
```

The first terminal now prints JSON lines like:

```json
{"name":"Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters",
 "tags":{"interface-name":"HundredGigE0/0/1/0","source":"dr02","subscription":"..."},
 "fields":{"bytes-received":8270089146,"packets-received":66694385,"crc-errors":0, ...},
 "timestamp":"2016-..."}
```

## 6. Look at the results

Outputs flush once per second, so give it a moment after the replay.

```bash
# Telemetry, ready for Prometheus
curl -s 127.0.0.1:9273/metrics | grep generic_counters_bytes_received | head -3

# The pipeline's own health
curl -s 127.0.0.1:9180/readyz
curl -s 127.0.0.1:9180/metrics | grep pipeline_input_metrics_total
```

You should see one series per interface, labelled with `source="dr02"` and
`interface_name="..."`, and `pipeline_input_metrics_total{input="dialout"} 13`.

## 7. Stop it

Press ++ctrl+c++ in the first terminal. The pipeline drains its queues,
flushes outputs and exits cleanly.

## Next steps

- Point a real device at it: [Connect your first device](first-device.md).
- Run the full stack (pipeline + Prometheus + Grafana) with Docker Compose:
  [Installation → Docker Compose lab](installation.md#docker-compose-lab).
- Learn how the data is shaped: [Data model](../concepts/data-model.md).
