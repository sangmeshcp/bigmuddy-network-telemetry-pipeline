# Processors

Processors run **in the order listed**, on every metric, before metrics are
handed to outputs. All matching uses Go
[regular expressions](https://pkg.go.dev/regexp/syntax), unanchored unless you
add `^` / `$`.

## `filter`

Keep or drop whole metrics, or individual fields.

```yaml
- type: filter
  include_names: ["interfaces", "bgp"]
  exclude_names: ["/system/processes"]
  include_tags: {source: ["^core", "^spine"]}
  exclude_tags: {interface_name: ["^Null", "^Loopback"]}
  keep_fields: ["counters/"]
  drop_fields: ["description$"]
  drop_string_fields: false
```

| Key | Effect |
|---|---|
| `include_names` | If set, keep only metrics whose name matches any regex |
| `exclude_names` | Drop metrics whose name matches any regex |
| `include_tags` | Map of tag → regexes. Every listed tag must be **present** and match. |
| `exclude_tags` | Map of tag → regexes. Drop the metric if any listed tag matches. |
| `keep_fields` | If set, keep only fields whose name matches |
| `drop_fields` | Remove fields whose name matches |
| `drop_string_fields` | Remove all string-valued fields |

A metric left with no fields is dropped. Everything removed is counted in
`pipeline_router_filtered_total`.

!!! tip
    Filtering early is the cheapest way to cut TSDB cost. A typical
    interface-counters subscription carries 30+ leaves; dashboards usually
    need 6.

## `tags`

Add, rename and remove tags. The typical use is **inventory enrichment**:
devices don't know their site or role, but dashboards need to group by them.

```yaml
- type: tags
  rename: {interface-name: interface_name}   # align Cisco-native with OpenConfig
  drop: [subscription]
  sources:                                   # per-device tags, keyed by `source`
    core1: {site: dfw1, role: spine}
    core2: {site: iad2, role: spine}
  add: {env: prod}                           # set only if not already present
```

Steps are applied in this order: `rename`, `drop`, `sources`, then `add`.
`sources` overwrites existing tags with the same name; `add` never does.

!!! tip "Generating `sources` from an inventory"
    Keep `sources` in sync with NetBox or a CMDB by templating the config at
    deploy time, e.g. a small script that renders `pipeline.yaml` from your
    inventory export.

## `rename`

Rewrite metric names and field names with regex replacements. Rules apply in
order; `$1`-style capture references are supported.

```yaml
- type: rename
  names:
    - {match: "^Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters$", replace: "xr_if"}
    - {match: "^openconfig:", replace: ""}
  fields:
    - {match: "^state/counters/", replace: ""}
```

With these rules, Prometheus series names become `xr_if_bytes_received`
instead of
`Cisco_IOS_XR_infra_statsd_oper_infra_statistics_interfaces_interface_latest_generic_counters_bytes_received`.

## `metric_types`

Marks numeric fields as **counters** or **gauges**. Telemetry doesn't say
which is which, so by default everything is treated as a gauge. Typing
counters makes outputs more accurate:

| Output | Counter fields become |
|---|---|
| `otlp` | a monotonic, cumulative **Sum** (instead of a Gauge) |
| `prometheus` | `# TYPE … counter` (series names are unchanged) |
| `kafka`, `file` | listed in the record's `kinds` map |
| analytical sinks (`parquet`, `clickhouse`) | `metric_type = 'counter'` |

```yaml
- type: metric_types
  presets: [network]                 # OpenConfig .../counters/..., *-octets, *-pkts, *-errors, *-drops, ...
  counters: ["^my-vendor-hits$"]     # extra counter regexes (on field names)
  gauges: ["^state/counters/rate$"]  # gauges win over counters
  # names: ["interfaces"]            # only metrics whose name matches
```

The `network` preset covers OpenConfig counters (any field under
`counters/`), Cisco IOS XR generic-counters (`bytes-received`,
`packets-sent`, `crc-errors`, `input-drops`, `carrier-transitions`, …) and
BGP message and update counters. Strings and booleans are never typed.
Fields keep their kind through the `rename` processor.

!!! note
    Because Prometheus' OpenMetrics format would rename counters to
    `<name>_total`, the exporter serves the classic text format so series
    names stay stable whether or not you enable typing.

## `downsample`

Keeps at most **one sample per series and field per interval**, dropping the
rest. Use it to cut storage and warehouse cost when devices send more often
than you need to keep, for example 10 s samples kept at 1 minute resolution
in a data lake.

```yaml
- type: downsample
  interval: 1m
  # names: ["interfaces"]   # only these metrics; others pass through untouched
```

- Windows are aligned to the device timestamp, so every replica makes the
  same choice.
- Each field is handled separately, so leaves a device splits across
  several notifications are each kept once per window.
- Counters stay correct for `rate()` because they're cumulative. Gauges keep
  the first sample of each window.

To keep full resolution in one place and downsample for another (e.g.
Prometheus at 10 s, the lake at 1 min), run two pipelines, or downsample in
the warehouse.

## Ordering example

```yaml
processors:
  - type: filter            # 1. throw away what we never want (cheap)
    exclude_tags: {interface_name: ["^Null"]}
  - type: rename            # 2. normalise names
    fields: [{match: "^state/counters/", replace: ""}]
  - type: tags              # 3. enrich survivors
    sources: {core1: {site: dfw1}}
```
