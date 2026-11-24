#!/usr/bin/env python3
"""Generate the Grafana dashboards in deploy/grafana/dashboards/.

Dashboards are code: edit this file, then run

    python3 deploy/grafana/generate.py

CI regenerates them and fails if the committed JSON differs, then runs every
panel query against a Prometheus fed with real telemetry
(scripts/dashboard-check.sh). Standard library only.
"""
import json
import os

DS = {"type": "prometheus", "uid": "${datasource}"}
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "dashboards")
# The Helm chart ships the same dashboards (charts can't read files outside
# their directory), so they're written to both places.
CHART_OUT = os.path.join(HERE, "..", "helm", "telemetry-pipeline", "files", "dashboards")

# Metric names as produced by the pipeline's prometheus output (see the
# data-model docs). OpenConfig paths via gNMI; Cisco-native via MDT.
OC_IF = "interfaces_interface_state"
XR_IF = "Cisco_IOS_XR_infra_statsd_oper_infra_statistics_interfaces_interface_latest_generic_counters"
OC_BGP = "network_instances_network_instance_protocols_protocol_bgp_neighbors_neighbor"


# --- building blocks -------------------------------------------------------

def datasource_var():
    return {"type": "datasource", "name": "datasource", "label": "Data source",
            "query": "prometheus", "current": {}, "hide": 0}


def query_var(name, label, query, multi=True):
    return {"type": "query", "name": name, "label": label, "datasource": DS,
            "query": {"query": query, "refId": f"{name}-var"}, "definition": query,
            "refresh": 2, "sort": 1, "multi": multi, "includeAll": multi,
            "allValue": ".*" if multi else None, "current": {}, "hide": 0, "regex": ""}


class Layout:
    """Places panels left to right on Grafana's 24-column grid."""

    def __init__(self):
        self.x, self.y, self.row_h, self.next_id = 0, 0, 0, 1

    def place(self, w, h):
        if self.x + w > 24:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        pid = self.next_id
        self.next_id += 1
        return pid, pos


def target(expr, legend="", ref="A", instant=False, fmt=None):
    t = {"refId": ref, "datasource": DS, "expr": expr, "legendFormat": legend}
    if instant:
        t["instant"], t["range"] = True, False
    if fmt:
        t["format"] = fmt
    return t


def timeseries(lay, title, targets, unit="short", w=12, h=8, desc=""):
    pid, pos = lay.place(w, h)
    return {"id": pid, "type": "timeseries", "title": title, "description": desc,
            "datasource": DS, "gridPos": pos, "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "custom": {"fillOpacity": 10, "lineWidth": 1}},
                            "overrides": []},
            "options": {"legend": {"displayMode": "table", "placement": "bottom",
                                   "calcs": ["mean", "max", "lastNotNull"]},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def stat(lay, title, expr, unit="short", w=6, h=4, thresholds=None, desc=""):
    pid, pos = lay.place(w, h)
    steps = thresholds or [{"color": "green", "value": None}]
    return {"id": pid, "type": "stat", "title": title, "description": desc,
            "datasource": DS, "gridPos": pos, "targets": [target(expr)],
            "fieldConfig": {"defaults": {"unit": unit,
                                         "thresholds": {"mode": "absolute", "steps": steps}},
                            "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background",
                        "graphMode": "area", "textMode": "auto"}}


def table(lay, title, expr, columns, w=24, h=8, desc="", value_map=None):
    """Instant query rendered as a table; `columns` are the labels to keep."""
    pid, pos = lay.place(w, h)
    exclude = {"Time": True, "Value": True, "__name__": True}
    overrides = []
    if value_map:
        overrides.append({"matcher": {"id": "byName", "options": "value"},
                          "properties": [{"id": "mappings", "value": value_map},
                                         {"id": "custom.cellOptions", "value": {"type": "color-background"}}]})
    return {"id": pid, "type": "table", "title": title, "description": desc,
            "datasource": DS, "gridPos": pos,
            "targets": [target(expr, instant=True, fmt="table")],
            "transformations": [{"id": "organize", "options": {
                "excludeByName": exclude, "indexByName": {c: i for i, c in enumerate(columns)}}},
                {"id": "filterFieldsByName", "options": {"include": {"names": columns}}}],
            "fieldConfig": {"defaults": {}, "overrides": overrides},
            "options": {"showHeader": True, "cellHeight": "sm"}}


def status_map(good, bad):
    m = {v: {"color": "green", "index": i} for i, v in enumerate(good)}
    m.update({v: {"color": "red", "index": len(good) + i} for i, v in enumerate(bad)})
    return [{"type": "value", "options": m}]


def dashboard(uid, title, tags, variables, panels, desc):
    return {"uid": uid, "title": title, "tags": ["network-telemetry"] + tags,
            "description": desc, "editable": True, "graphTooltip": 1,
            "schemaVersion": 39, "version": 1, "refresh": "30s",
            "time": {"from": "now-1h", "to": "now"}, "timezone": "",
            "templating": {"list": [datasource_var()] + variables},
            "annotations": {"list": []}, "links": [{
                "title": "Documentation", "type": "link", "icon": "doc", "targetBlank": True,
                "url": "https://sangameshpatil.com/bigmuddy-network-telemetry-pipeline/"}],
            "panels": panels}


R = "$__rate_interval"
GREEN, AMBER, RED = "green", "orange", "red"


# --- dashboards ------------------------------------------------------------

def pipeline_health():
    lay = Layout()
    sel = 'instance=~"$instance"'
    panels = [
        stat(lay, "Ingest (metrics/s)", f'sum(rate(pipeline_input_metrics_total{{{sel}}}[{R}])) or on() vector(0)', unit="ops"),
        stat(lay, "gNMI targets up", f'sum(gnmi_target_up{{{sel}}}) or on() vector(0)'),
        stat(lay, "gNMI targets down", f'count(gnmi_target_up{{{sel}}} == 0) or on() vector(0)',
             thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 1}]),
        stat(lay, "Dropped (metrics/s)", f'sum(rate(pipeline_output_dropped_total{{{sel}}}[{R}])) or on() vector(0)',
             unit="ops", thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 0.001}]),
        timeseries(lay, "Ingest by input", [target(f'sum by (input) (rate(pipeline_input_metrics_total{{{sel}}}[{R}]))', "{{input}}")], unit="ops"),
        timeseries(lay, "Written by output", [target(f'sum by (output) (rate(pipeline_output_written_total{{{sel}}}[{R}]))', "{{output}}")], unit="ops"),
        timeseries(lay, "Dropped by output and reason",
                   [target(f'sum by (output, reason) (rate(pipeline_output_dropped_total{{{sel}}}[{R}])) or on() vector(0)', "{{output}} {{reason}}")],
                   unit="ops", desc="queue_full: destination too slow. write_failed: destination rejecting data."),
        timeseries(lay, "Output write latency (p99)",
                   [target(f'histogram_quantile(0.99, sum by (output, le) (rate(pipeline_output_write_duration_seconds_bucket{{{sel}}}[{R}])))', "{{output}}")],
                   unit="s"),
        timeseries(lay, "Router backlog (batches)", [target(f'pipeline_router_queue_batches{{{sel}}}', "{{instance}}")],
                   desc="Sustained > 0 means processors/outputs can't keep up and inputs are being throttled."),
        timeseries(lay, "Output queue fill",
                   [target(f'sum by (output) (rate(pipeline_output_enqueued_total{{{sel}}}[{R}]))', "enqueued {{output}}")], unit="ops"),
        timeseries(lay, "gNMI target status", [target(f'gnmi_target_up{{{sel}}}', "{{target}}")],
                   desc="1 = subscription established."),
        timeseries(lay, "gNMI session failures", [target(f'sum by (target) (increase(gnmi_session_failures_total{{{sel}}}[{R}])) or on() vector(0)', "{{target}}")]),
        timeseries(lay, "MDT dial-out sessions", [target(f'sum by (input) (mdt_connections{{{sel}}}) or on() vector(0)', "{{input}}")]),
        timeseries(lay, "MDT decode errors", [target(f'sum by (input, reason) (rate(mdt_decode_errors_total{{{sel}}}[{R}])) or on() vector(0)', "{{input}} {{reason}}")],
                   unit="ops", desc="compact_gpb: set the device to self-describing-gpb."),
        timeseries(lay, "Memory (RSS)", [target(f'process_resident_memory_bytes{{{sel}}}', "{{instance}}")], unit="bytes"),
        timeseries(lay, "CPU", [target(f'rate(process_cpu_seconds_total{{{sel}}}[{R}])', "{{instance}}")], unit="percentunit"),
    ]
    return dashboard("telemetry-pipeline-health", "Telemetry pipeline: health", ["pipeline"],
                     [query_var("instance", "Instance", "label_values(pipeline_router_queue_batches, instance)")],
                     panels, "Self-monitoring of the telemetry pipeline: ingest, delivery, drops, sessions and resources.")


def interfaces_openconfig():
    lay = Layout()
    sel = 'source=~"$source", interface_name=~"$interface"'
    panels = [
        stat(lay, "Interfaces oper UP", f'count({OC_IF}_oper_status_info{{{sel}, value="UP"}}) or on() vector(0)', w=8),
        stat(lay, "Interfaces oper DOWN (admin UP)",
             f'count(({OC_IF}_oper_status_info{{{sel}, value="DOWN"}} == 1) and on (source, interface_name) '
             f'({OC_IF}_admin_status_info{{{sel}, value="UP"}} == 1)) or on() vector(0)', w=8,
             thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 1}]),
        stat(lay, "Total throughput (in)", f'sum(rate({OC_IF}_counters_in_octets{{{sel}}}[{R}])) * 8 or on() vector(0)', unit="bps", w=8),
        timeseries(lay, "Inbound", [target(f'rate({OC_IF}_counters_in_octets{{{sel}}}[{R}]) * 8', "{{source}} {{interface_name}}")], unit="bps"),
        timeseries(lay, "Outbound", [target(f'rate({OC_IF}_counters_out_octets{{{sel}}}[{R}]) * 8', "{{source}} {{interface_name}}")], unit="bps"),
        timeseries(lay, "Errors", [target(f'rate({OC_IF}_counters_in_errors{{{sel}}}[{R}])', "in {{source}} {{interface_name}}"),
                                   target(f'rate({OC_IF}_counters_out_errors{{{sel}}}[{R}])', "out {{source}} {{interface_name}}", ref="B")], unit="pps"),
        timeseries(lay, "Discards", [target(f'rate({OC_IF}_counters_in_discards{{{sel}}}[{R}])', "in {{source}} {{interface_name}}"),
                                     target(f'rate({OC_IF}_counters_out_discards{{{sel}}}[{R}])', "out {{source}} {{interface_name}}", ref="B")], unit="pps"),
        timeseries(lay, "Top 10 interfaces by inbound", [target(f'topk(10, rate({OC_IF}_counters_in_octets{{{sel}}}[{R}]) * 8)', "{{source}} {{interface_name}}")],
                   unit="bps", w=24),
        table(lay, "Operational status", f'{OC_IF}_oper_status_info{{{sel}}} == 1', ["source", "interface_name", "value"],
              value_map=status_map(["UP"], ["DOWN", "LOWER_LAYER_DOWN", "NOT_PRESENT"]),
              desc="Requires a subscription to .../state/oper-status and string_fields: info on the prometheus output."),
    ]
    return dashboard("telemetry-interfaces-openconfig", "Interfaces (OpenConfig / gNMI)", ["interfaces", "openconfig", "gnmi"],
                     [query_var("source", "Device", f"label_values({OC_IF}_counters_in_octets, source)"),
                      query_var("interface", "Interface", f'label_values({OC_IF}_counters_in_octets{{source=~"$source"}}, interface_name)')],
                     panels, "Multi-vendor interface throughput, errors and status from OpenConfig /interfaces via gNMI.")


def interfaces_iosxr():
    lay = Layout()
    sel = 'source=~"$source", interface_name=~"$interface"'
    m = XR_IF
    panels = [
        stat(lay, "Interfaces reporting", f'count({m}_bytes_received{{{sel}}}) or on() vector(0)', w=8),
        stat(lay, "Total throughput (in)", f'sum(rate({m}_bytes_received{{{sel}}}[{R}])) * 8 or on() vector(0)', unit="bps", w=8),
        stat(lay, "Interfaces with CRC errors (rate)", f'count(rate({m}_crc_errors{{{sel}}}[{R}]) > 0) or on() vector(0)', w=8,
             thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 1}]),
        timeseries(lay, "Inbound", [target(f'rate({m}_bytes_received{{{sel}}}[{R}]) * 8', "{{source}} {{interface_name}}")], unit="bps"),
        timeseries(lay, "Outbound", [target(f'rate({m}_bytes_sent{{{sel}}}[{R}]) * 8', "{{source}} {{interface_name}}")], unit="bps"),
        timeseries(lay, "Packets", [target(f'rate({m}_packets_received{{{sel}}}[{R}])', "in {{source}} {{interface_name}}"),
                                    target(f'rate({m}_packets_sent{{{sel}}}[{R}])', "out {{source}} {{interface_name}}", ref="B")], unit="pps"),
        timeseries(lay, "Errors and drops", [target(f'rate({m}_input_errors{{{sel}}}[{R}])', "in-err {{source}} {{interface_name}}"),
                                             target(f'rate({m}_crc_errors{{{sel}}}[{R}])', "crc {{source}} {{interface_name}}", ref="B"),
                                             target(f'rate({m}_input_drops{{{sel}}}[{R}])', "in-drop {{source}} {{interface_name}}", ref="C"),
                                             target(f'rate({m}_output_drops{{{sel}}}[{R}])', "out-drop {{source}} {{interface_name}}", ref="D")], unit="pps", w=24),
    ]
    return dashboard("telemetry-interfaces-iosxr", "Interfaces (Cisco IOS XR / MDT)", ["interfaces", "cisco", "mdt"],
                     [query_var("source", "Device", f"label_values({m}_bytes_received, source)"),
                      query_var("interface", "Interface", f'label_values({m}_bytes_received{{source=~"$source"}}, interface_name)')],
                     panels, "IOS XR native generic-counters (Cisco-IOS-XR-infra-statsd-oper) received over MDT dial-out.")


def bgp_openconfig():
    lay = Layout()
    sel = 'source=~"$source", neighbor_neighbor_address=~"$neighbor"'
    st = f"{OC_BGP}_state_session_state_info"
    pfx = f"{OC_BGP}_afi_safis_afi_safi_state_prefixes"
    panels = [
        stat(lay, "Sessions established", f'count({st}{{{sel}, value="ESTABLISHED"}} == 1) or on() vector(0)', w=12),
        stat(lay, "Sessions NOT established", f'count({st}{{{sel}, value!="ESTABLISHED"}} == 1) or on() vector(0)', w=12,
             thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 1}]),
        table(lay, "Session state", f'{st}{{{sel}}} == 1', ["source", "network_instance_name", "neighbor_neighbor_address", "value"],
              value_map=status_map(["ESTABLISHED"], ["IDLE", "CONNECT", "ACTIVE", "OPENSENT", "OPENCONFIRM"])),
        timeseries(lay, "Prefixes received", [target(f'{pfx}_received{{{sel}}}', "{{source}} {{neighbor_neighbor_address}} {{afi_safi_afi_safi_name}}")]),
        timeseries(lay, "Prefixes installed", [target(f'{pfx}_installed{{{sel}}}', "{{source}} {{neighbor_neighbor_address}} {{afi_safi_afi_safi_name}}")]),
    ]
    return dashboard("telemetry-bgp-openconfig", "BGP (OpenConfig / gNMI)", ["bgp", "openconfig", "gnmi"],
                     [query_var("source", "Device", f"label_values({st}, source)"),
                      query_var("neighbor", "Neighbor", f'label_values({st}{{source=~"$source"}}, neighbor_neighbor_address)')],
                     panels, "BGP neighbor session state and prefix counts from OpenConfig network-instances via gNMI.")


def main():
    for d in (OUT, CHART_OUT):
        os.makedirs(d, exist_ok=True)
    for name, fn in [("pipeline-health", pipeline_health), ("interfaces-openconfig", interfaces_openconfig),
                     ("interfaces-iosxr", interfaces_iosxr), ("bgp-openconfig", bgp_openconfig)]:
        text = json.dumps(fn(), indent=2, sort_keys=True) + "\n"
        for d in (OUT, CHART_OUT):
            path = os.path.join(d, f"{name}.json")
            with open(path, "w") as f:
                f.write(text)
            print("wrote", os.path.relpath(path))


if __name__ == "__main__":
    main()
