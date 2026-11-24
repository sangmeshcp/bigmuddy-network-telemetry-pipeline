#!/usr/bin/env bash
# Smoke test a running pipeline (binary or container) that uses
# examples/minimal.yaml: MDT gRPC dial-out on :57500, Prometheus on :9273,
# admin on :9180.
#
#   HOST=127.0.0.1 scripts/smoke.sh
set -euo pipefail
HOST=${HOST:-127.0.0.1}
REPLAY=${REPLAY:-go run ./cmd/mdt-replay}

wait_for() { # url
  for _ in $(seq 1 60); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for $1" >&2
  return 1
}

echo "--- waiting for readiness"
wait_for "http://$HOST:9180/healthz"
wait_for "http://$HOST:9180/readyz"

echo "--- replaying captured IOS XR telemetry over gRPC dial-out"
$REPLAY -addr "$HOST:57500" internal/input/ciscomdt/testdata/*.kvgpb

echo "--- checking Prometheus exporter"
for _ in $(seq 1 20); do
  body=$(curl -fsS "http://$HOST:9273/metrics")
  if grep -q '^Cisco_IOS_XR_infra_statsd_oper_infra_statistics_interfaces_interface_latest_generic_counters_bytes_received{' <<<"$body"; then
    break
  fi
  sleep 0.5
done
grep -q 'source="dr02"' <<<"$body" || { echo "expected series missing"; echo "$body" | head -50; exit 1; }
echo "exported series: $(grep -c '^Cisco' <<<"$body")"

echo "--- checking self-metrics"
self=$(curl -fsS "http://$HOST:9180/metrics")
grep -E '^pipeline_input_metrics_total\{input="dialout"\} 13$' <<<"$self" \
  || { echo "unexpected input count:"; grep pipeline_input <<<"$self"; exit 1; }
grep -q '^mdt_messages_total{input="dialout"} 3$' <<<"$self" || { grep mdt_ <<<"$self"; exit 1; }
echo "smoke test passed"
