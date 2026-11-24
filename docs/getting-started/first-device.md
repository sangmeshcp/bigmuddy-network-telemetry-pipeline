# Connect your first device

There are two ways to get telemetry out of a device. Choose one per device.

| | **gNMI dial-in** (recommended) | **Cisco MDT dial-out** |
|---|---|---|
| Who connects | pipeline → device | device → pipeline |
| Vendors | any: Cisco, Juniper, Arista, Nokia, SONiC, … | Cisco IOS XR, NX-OS, IOS XE |
| What to subscribe to | defined in the **pipeline** config | defined on the **device** |
| Models | OpenConfig or vendor-native YANG | vendor-native YANG |
| Good when | you want one config for a multi-vendor fleet | devices are behind NAT/firewalls, or the fleet already uses MDT |

## Option A: gNMI (any vendor)

### 1. Enable gNMI on the device

=== "Cisco IOS XR"

    ```
    grpc
     port 57400
     tls
    !
    ```
    Use a local or AAA user with read access to the models you subscribe to.

=== "Juniper Junos / Evolved"

    ```
    set system services extension-service request-response grpc ssl port 57400
    set system services extension-service request-response grpc ssl local-certificate <cert-name>
    ```

=== "Arista EOS"

    ```
    management api gnmi
       transport grpc default
          ssl profile <profile>
       provider eos-native
    ```

=== "Nokia SR Linux"

    gNMI is enabled by default on port 57400 in the `mgmt` network instance.
    Native paths look like `/interface[name=ethernet-1/1]/statistics`.

=== "Cisco NX-OS"

    ```
    feature grpc
    grpc port 50051
    grpc certificate <trustpoint>
    ```

!!! note
    Exact commands vary by software release; check your platform's gNMI guide.
    Whatever the vendor, you end up with a **host:port**, a **username and
    password**, and usually a **TLS certificate** (often self-signed in labs).

### 2. Describe the device in the pipeline config

```yaml
inputs:
  - name: lab
    type: gnmi
    targets:
      - address: 192.0.2.1:57400
        name: lab-r1                    # becomes the `source` tag
    username: telemetry
    password: ${GNMI_PASSWORD}          # read from the environment
    tls:
      enabled: true
      insecure_skip_verify: true        # LAB ONLY: use ca_file in production
    subscriptions:
      - name: if-counters
        path: /interfaces/interface/state/counters
        mode: sample
        sample_interval: 10s
      - name: if-status
        path: /interfaces/interface/state/oper-status
        mode: on_change

outputs:
  - {name: prom, type: prometheus, listen: ":9273", string_fields: info}
  - {name: debug, type: file, path: "-"}
```

!!! tip "Which paths?"
    `/interfaces/interface/state/counters` is OpenConfig and works on almost
    every platform. On SR Linux use the native `/interface/statistics`. If
    unsure, start with `/interfaces` and look at what arrives in the `debug`
    output, then narrow it down.

### 3. Run and verify

```bash
GNMI_PASSWORD='...' ./bin/pipeline -config pipeline.yaml
```

Within a few seconds you should see:

1. A log line `gNMI subscription sent` for the target.
2. JSON metrics on stdout from the `debug` output.
3. `gnmi_target_up{input="lab",target="lab-r1"} 1` at `http://localhost:9180/metrics`.

If the target stays at `0`, see [Troubleshooting → gNMI](../operations/troubleshooting.md#gnmi).

## Option B: Cisco MDT dial-out

### 1. Start a dial-out listener

```yaml
inputs:
  - name: xr
    type: cisco_mdt
    transport: grpc        # or tcp / udp
    listen: ":57500"
outputs:
  - {name: prom, type: prometheus, listen: ":9273"}
```

### 2. Configure the router to send to it

```
telemetry model-driven
 destination-group PIPELINE
  address-family ipv4 192.0.2.10 port 57500
   encoding self-describing-gpb
   protocol grpc no-tls
  !
 !
 sensor-group IF
  sensor-path Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters
 !
 subscription S1
  sensor-group-id IF sample-interval 10000
  destination-id PIPELINE
 !
!
```

!!! warning "Use `self-describing-gpb`"
    The router's default compact `gpb` encoding needs model-specific schemas
    compiled into the collector and is **not supported**. Such messages are
    counted in `mdt_decode_errors_total{reason="compact_gpb"}`. Use
    `self-describing-gpb` (KV-GPB) or `json`.

### 3. Verify

- Log line `dial-out session established` with the router's address.
- `mdt_connections{input="xr"} 1` and a rising `mdt_messages_total` on the
  admin endpoint.

## Next

- Narrow and enrich the data: [Processors](../configuration/processors.md).
- Send it somewhere durable: [Outputs](../configuration/outputs.md).
