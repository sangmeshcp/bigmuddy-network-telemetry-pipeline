#!/usr/bin/env bash
# Start the OpenConfig reference gNMI target (fake_server from
# github.com/openconfig/gnmi) as a standalone process with mutual TLS.
# Vendor-neutral: no network OS image or licence required.
#
#   eval "$(scripts/gnmi-reference-target.sh start)"   # exports GNMI_* for the integration test
#   go test -tags integration -run TestGNMITarget -v ./integration/
#   scripts/gnmi-reference-target.sh stop
set -euo pipefail

DIR=${GNMI_REF_DIR:-${TMPDIR:-/tmp}/gnmi-reference-target}
PORT=${GNMI_REF_PORT:-57401}

case "${1:-start}" in
stop)
  [ -f "$DIR/pid" ] && kill "$(cat "$DIR/pid")" 2>/dev/null || true
  exit 0 ;;
start) ;;
*) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac

mkdir -p "$DIR"
cd "$(dirname "$0")/.."
GNMI_VERSION=$(go list -m -f '{{.Version}}' github.com/openconfig/gnmi)
GOBIN="$DIR" go install "github.com/openconfig/gnmi/testing/fake/gnmi/cmd/fake_server@${GNMI_VERSION}" >&2

# Throwaway PKI: CA, server cert for localhost, client cert for mTLS.
(
  cd "$DIR"
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
    -subj /CN=gnmi-test-ca -keyout ca.key -out ca.pem 2>/dev/null
  for who in server client; do
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$who" \
      -keyout "$who.key" -out "$who.csr" 2>/dev/null
    ext="extendedKeyUsage=clientAuth"
    [ "$who" = server ] && ext="subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
    openssl x509 -req -in "$who.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
      -extfile <(printf '%s\n' "$ext") -out "$who.pem" 2>/dev/null
  done
)

# Interface counters, errors, discards and status for two interfaces, plus a
# BGP neighbor (session state and prefix counts). Values change every 500ms.
gen_values() {
  local ts='timestamp: { timestamp: 1 delta_min: 500000000 delta_max: 500000000 }'
  for ifname in eth0 eth1; do
    local p='"interfaces", "interface[name='"$ifname"']", "state"'
    for c in in-octets out-octets; do
      echo "values: { path: [$p, \"counters\", \"$c\"] $ts int_value: { value: 1000 range: { minimum: 0 maximum: 1000000000000 delta_min: 1 delta_max: 50000 } } }"
    done
    for c in in-errors out-errors in-discards out-discards; do
      echo "values: { path: [$p, \"counters\", \"$c\"] $ts int_value: { value: 0 range: { minimum: 0 maximum: 1000000 delta_min: 0 delta_max: 2 } } }"
    done
    echo "values: { path: [$p, \"oper-status\"] $ts string_value: { value: \"UP\" list: { options: [\"UP\", \"DOWN\"] random: true } } }"
    echo "values: { path: [$p, \"admin-status\"] $ts string_value: { value: \"UP\" } }"
  done
  local n='"network-instances", "network-instance[name=default]", "protocols", "protocol[identifier=BGP][name=BGP]", "bgp", "neighbors", "neighbor[neighbor-address=192.0.2.2]"'
  echo "values: { path: [$n, \"state\", \"session-state\"] $ts string_value: { value: \"ESTABLISHED\" } }"
  for c in received installed; do
    echo "values: { path: [$n, \"afi-safis\", \"afi-safi[afi-safi-name=IPV4_UNICAST]\", \"state\", \"prefixes\", \"$c\"] $ts int_value: { value: 900 range: { minimum: 0 maximum: 2000 delta_min: 0 delta_max: 10 } } }"
  done
}

cat > "$DIR/config.textproto" <<CFG
target: "dut"
client_type: GRPC_GNMI
disable_eof: true
enable_delay: true
seed: 1
$(gen_values)
CFG

"$DIR/fake_server" -text -config "$DIR/config.textproto" -port "$PORT" \
  -server_crt "$DIR/server.pem" -server_key "$DIR/server.key" -ca_crt "$DIR/ca.pem" \
  -logtostderr >"$DIR/server.log" 2>&1 &
echo $! >"$DIR/pid"

for _ in $(seq 1 50); do
  (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break
  sleep 0.2
done
echo "reference gNMI target listening on 127.0.0.1:$PORT (log: $DIR/server.log)" >&2

cat <<VARS
export GNMI_TARGET=127.0.0.1:$PORT
export GNMI_USERNAME=admin
export GNMI_PASSWORD=unused-by-reference-target
export GNMI_PATH=/interfaces
export GNMI_TLS_CA=$DIR/ca.pem
export GNMI_TLS_CERT=$DIR/client.pem
export GNMI_TLS_KEY=$DIR/client.key
VARS
