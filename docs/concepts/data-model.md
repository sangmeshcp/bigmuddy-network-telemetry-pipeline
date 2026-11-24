# Data model

Everything inside the pipeline is a **Metric**:

| Field | Meaning | Example |
|---|---|---|
| `Name` | Schema path of the *instance*, with list keys removed | `openconfig:/interfaces/interface` |
| `Tags` | What identifies the instance: list keys, `source`, `subscription`, plus anything processors add | `source=core1`, `interface_name=Ethernet1` |
| `Fields` | Leaf values, keyed by path relative to `Name` | `state/counters/in-octets = 1234` |
| `Timestamp` | When the device sampled the data | `2026-11-24T09:00:00.123Z` |

Field values are always one of `int64`, `uint64`, `float64`, `bool` or `string`.

## How gNMI data becomes a Metric

A device sends updates like:

```text
prefix: origin=openconfig /interfaces
update: /interface[name=Ethernet1]/state/counters/in-octets = 1234
update: /interface[name=Ethernet1]/state/counters/out-octets = 5678
update: /interface[name=Ethernet1]/state/oper-status = "UP"
update: /interface[name=Ethernet2]/state/counters/in-octets = 99
```

The pipeline splits each path at its **last element that has keys**
(`interface[name=…]`). Everything up to that element is the instance; the rest
is the field name. All updates for the same instance in one notification are
grouped into one Metric:

```text
Metric 1
  name:   openconfig:/interfaces/interface
  tags:   source=core1  subscription=if  interface_name=Ethernet1
  fields: state/counters/in-octets=1234  state/counters/out-octets=5678
          state/oper-status="UP"
Metric 2
  name:   openconfig:/interfaces/interface
  tags:   source=core1  subscription=if  interface_name=Ethernet2
  fields: state/counters/in-octets=99
```

The rules in detail:

- **Tag names** are `<list>_<key>` (`interface_name`,
  `subinterface_index`), so keys from nested lists never collide.
- **YANG module prefixes** are removed from path elements and JSON members:
  `openconfig-interfaces:interfaces` becomes `interfaces`.
- **Paths without any keys** split before the last element:
  `/system/state/hostname` becomes name `/system/state`, field `hostname`.
- **JSON / JSON_IETF values** are flattened into `/`-joined field names.
  JSON arrays contribute their index (`servers/0`). RFC 7951 encodes 64-bit
  integers as strings (`"18446744073709551615"`); these are converted back to
  numbers.
- **Typed values**: int → `int64`, uint → `uint64`, double/float/decimal64 →
  `float64`, bool → `bool`, string/ascii → `string`, bytes → base64 `string`,
  leaf-list → comma-joined `string`. `any` and `proto_bytes` values cannot be
  decoded without a schema and are counted in `gnmi_unsupported_values_total`.
- **Legacy paths**: devices implementing gNMI earlier than 0.4 send string
  path elements (`"interface[name=Eth1]"`). These are parsed the same way.
- **Deletes** are counted as notifications but produce no metrics.
- **`source`** is the target's configured `name` (default: its host);
  **`subscription`** is the name of the configured subscription whose path
  best matches.

## How Cisco MDT data becomes a Metric

IOS XR sends one *row* per instance with separate `keys` and `content`:

```text
encoding_path: Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters
node_id: dr02   subscription: S1
row: keys    { interface-name: "HundredGigE0/0/1/0" }
     content { bytes-received: 8270089146, packets-received: 66694385, ... }
```

which becomes

```text
name:   Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters
tags:   source=dr02  subscription=S1  interface-name=HundredGigE0/0/1/0
fields: bytes-received=8270089146  packets-received=66694385 ...
```

- The **encoding path** is the name.
- Key leaves become tags with their YANG names (`interface-name`).
- Nested containers in `content` are flattened with `/`. Repeated siblings
  (lists inside the content) are disambiguated by index:
  `class-stats/0/...`, `class-stats/1/...`.
- `source` is the router's `node_id`, falling back to its IP address.

!!! tip "Aligning Cisco-native and OpenConfig tag names"
    Cisco tags keep YANG spelling (`interface-name`) while gNMI tags use
    `interface_name`. Prometheus sanitises both to `interface_name`. For other
    outputs, use the [`tags` processor](../configuration/processors.md#tags)
    with `rename: {interface-name: interface_name}`.

## How each output represents a Metric

Using Metric 1 from above:

=== "Prometheus"

    One series per numeric field. The name is the sanitised `name/field`
    path; tags become labels:

    ```text
    interfaces_interface_state_counters_in_octets{interface_name="Ethernet1",source="core1",subscription="if"} 1234
    interfaces_interface_state_counters_out_octets{interface_name="Ethernet1",source="core1",subscription="if"} 5678
    interfaces_interface_state_oper_status_info{interface_name="Ethernet1",source="core1",subscription="if",value="UP"} 1
    ```

    The `_info` series appears only with `string_fields: info`. Sanitising
    drops the `openconfig:` origin and module prefixes, and replaces every
    character outside `[a-zA-Z0-9_]` with `_`.

=== "OTLP"

    One OpenTelemetry *Resource* per device (`service.name=network-telemetry`,
    `service.instance.id=core1`, `host.name=core1`). Each numeric field is a
    **Gauge** data point with the same name as in Prometheus. The tags,
    except `source`, are data-point attributes. Strings are skipped.

=== "InfluxDB"

    One point per Metric. Measurement = name, tags = tags, fields = fields:

    ```text
    openconfig:/interfaces/interface,interface_name=Ethernet1,source=core1,subscription=if state/counters/in-octets=1234u,state/counters/out-octets=5678u,state/oper-status="UP" 1764000000000000000
    ```

    Integer suffixes: `i` = signed, `u` = unsigned. With `database:` (InfluxDB
    1.x) unsigned values are written as signed.

=== "Kafka / file"

    One JSON document per Metric:

    ```json
    {"name":"openconfig:/interfaces/interface",
     "tags":{"interface_name":"Ethernet1","source":"core1","subscription":"if"},
     "fields":{"state/counters/in-octets":1234,"state/counters/out-octets":5678,"state/oper-status":"UP"},
     "timestamp":"2026-11-24T09:00:00.123Z"}
    ```

    Kafka records are keyed by `source` by default, so each device's data
    stays ordered within a partition.

## Analytical row layout

Warehouse and lakehouse sinks (`parquet`, `iceberg`, `clickhouse`, and
`kafka` with `format: avro`) store **one row per field** in a fixed schema
(schema version 1):

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | timestamp (UTC) | Device sample time |
| `source` | string | Device |
| `name` | string | Metric name (schema path of the instance) |
| `field` | string | Leaf path relative to `name` |
| `tags` | map<string,string> | List keys, subscription, enrichment tags (everything except `source`) |
| `metric_type` | string | `counter`, `gauge` or empty (see [`metric_types`](../configuration/processors.md#metric_types)) |
| `value_type` | string | Original type: `int`, `uint`, `float`, `bool`, `string` |
| `value_double` | double, nullable | Every numeric and bool value (bool as 0/1). Aggregate this. |
| `value_int` | int64, nullable | Exact integers (and bools). Null for uint64 above 2⁶³−1. |
| `value_string` | string, nullable | Strings, and the exact decimal text of uint64 values above 2⁶³−1 |

Why long instead of one column per leaf? A new YANG model, a new leaf or a
new device software release **never changes the table schema**, so there are
no migrations and no failed loads. Columnar formats compress the repeated
`name`/`field` values to almost nothing. Build wide views in the warehouse
where you need them:

```sql
SELECT timestamp, source, tags['interface_name'] AS interface,
       max(value_double) FILTER (WHERE field = 'state/counters/in-octets')  AS in_octets,
       max(value_double) FILTER (WHERE field = 'state/counters/out-octets') AS out_octets
FROM telemetry
WHERE name = 'openconfig:/interfaces/interface'
GROUP BY ALL;
```

## Counters vs gauges

Streaming telemetry doesn't say whether a leaf is a counter or a gauge, so by
default every numeric value is exported as a gauge (untyped in Prometheus).
Add the [`metric_types`](../configuration/processors.md#metric_types)
processor to mark counters: OTLP then exports monotonic Sums and Prometheus
`# TYPE counter`. Either way, use `rate()` or `increase()` on counters at
query time as usual:

```promql
rate(interfaces_interface_state_counters_in_octets{source="core1"}[1m]) * 8   # bits/s
```
