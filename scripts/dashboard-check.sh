#!/usr/bin/env bash
# End-to-end check of the Grafana dashboards and the pipeline's metric naming.
#
# Starts the OpenConfig reference gNMI target (interfaces + BGP), the
# pipeline (gNMI + MDT dial-out inputs, Prometheus exporter), replays
# captured IOS XR telemetry, has a real Prometheus scrape it all, and then
# runs EVERY panel query and template variable from deploy/grafana/dashboards
# against Prometheus. Any query returning no data fails the check.
#
#   PROMETHEUS=/path/to/prometheus scripts/dashboard-check.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PROMETHEUS=${PROMETHEUS:-prometheus}
WORK=${WORK:-$(mktemp -d)}
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  GNMI_REF_DIR="$WORK/gnmi" scripts/gnmi-reference-target.sh stop || true
}
trap cleanup EXIT

echo "--- building pipeline and replay tool"
go build -o "$WORK/pipeline" ./cmd/pipeline
go build -o "$WORK/mdt-replay" ./cmd/mdt-replay

echo "--- starting OpenConfig reference gNMI target"
eval "$(GNMI_REF_DIR="$WORK/gnmi" GNMI_REF_PORT=57411 scripts/gnmi-reference-target.sh start)"

cat > "$WORK/pipeline.yaml" <<CFG
log: {level: warn, format: text}
admin: {listen: "127.0.0.1:19180"}
inputs:
  - name: oc
    type: gnmi
    targets: [{address: "$GNMI_TARGET", name: dut}]
    tls: {enabled: true, ca_file: "$GNMI_TLS_CA", cert_file: "$GNMI_TLS_CERT", key_file: "$GNMI_TLS_KEY", server_name: localhost}
    encoding: proto
    subscriptions:
      - {name: interfaces, path: /interfaces, mode: sample, sample_interval: 1s}
      - {name: bgp, path: /network-instances, mode: sample, sample_interval: 1s}
  - name: dialout
    type: cisco_mdt
    transport: grpc
    listen: "127.0.0.1:57510"
outputs:
  - {name: prom, type: prometheus, listen: "127.0.0.1:19273", string_fields: info, flush_interval: 200ms}
CFG
"$WORK/pipeline" -config "$WORK/pipeline.yaml" -check
"$WORK/pipeline" -config "$WORK/pipeline.yaml" 2>"$WORK/pipeline.log" &
PIDS+=($!)

cat > "$WORK/prometheus.yml" <<CFG
global: {scrape_interval: 2s, evaluation_interval: 2s}
scrape_configs:
  - job_name: pipeline-self
    static_configs: [{targets: ["127.0.0.1:19180"]}]
  - job_name: network-telemetry
    honor_labels: true
    static_configs: [{targets: ["127.0.0.1:19273"]}]
CFG
"$PROMETHEUS" --config.file="$WORK/prometheus.yml" --storage.tsdb.path="$WORK/tsdb" \
  --web.listen-address=127.0.0.1:19090 --log.level=warn 2>"$WORK/prometheus.log" &
PIDS+=($!)

for _ in $(seq 1 60); do curl -fsS 127.0.0.1:19180/readyz >/dev/null 2>&1 && curl -fsS 127.0.0.1:19090/-/ready >/dev/null 2>&1 && break; sleep 0.5; done

echo "--- replaying IOS XR MDT telemetry (several rounds, so rate() has samples)"
"$WORK/mdt-replay" -addr 127.0.0.1:57510 -repeat 12 -interval 2s internal/input/ciscomdt/testdata/*.kvgpb >/dev/null &
PIDS+=($!)

echo "--- collecting for 30s"
sleep 30

echo "--- running every dashboard query and variable against Prometheus"
python3 - <<'PY'
import glob, json, re, sys, urllib.parse, urllib.request

API = "http://127.0.0.1:19090/api/v1"
VARS = {"$__rate_interval": "20s", "$__interval": "2s"}

def get(path, **params):
    url = f"{API}/{path}?" + urllib.parse.urlencode(params, doseq=True)
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)

def subst(expr):
    for k, v in VARS.items():
        expr = expr.replace(k, v)
    return re.sub(r'"\$[a-z_]+"', '".*"', expr)   # template variables -> match all

failures, checked = [], 0
for f in sorted(glob.glob("deploy/grafana/dashboards/*.json")):
    d = json.load(open(f))
    for v in d["templating"]["list"]:
        if v["type"] != "query":
            continue
        m = re.match(r"label_values\((.+),\s*(\w+)\)$", subst(v["definition"]))
        metric, label = m.group(1), m.group(2)
        vals = get(f"label/{label}/values", **{"match[]": metric})["data"]
        checked += 1
        status = "ok " if vals else "EMPTY"
        print(f"  [{status}] {d['uid']}: variable ${v['name']} -> {vals[:3]}")
        if not vals:
            failures.append(f"{d['uid']} variable {v['name']}")
    for p in d["panels"]:
        for t in p["targets"]:
            expr = subst(t["expr"])
            res = get("query", query=expr)
            checked += 1
            if res["status"] != "success":
                failures.append(f"{d['uid']} / {p['title']}: {res}")
                print(f"  [ERROR] {d['uid']} / {p['title']}")
                continue
            n = len(res["data"]["result"])
            ok = n > 0
            print(f"  [{'ok ' if ok else 'EMPTY'}] {d['uid']} / {p['title']} ({t['refId']}): {n} series")
            if not ok:
                failures.append(f"{d['uid']} / {p['title']} ({t['refId']}): {expr}")
print(f"\n{checked} queries checked, {len(failures)} failed")
for f in failures:
    print("  FAILED:", f)
sys.exit(1 if failures else 0)
PY
