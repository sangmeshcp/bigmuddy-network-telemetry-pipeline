//go:build integration

package integration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/input/gnmi"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

type chanSink chan *telemetry.Metric

func (c chanSink) Emit(ctx context.Context, b []*telemetry.Metric) error {
	for _, m := range b {
		select {
		case c <- m:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// TestGNMITarget subscribes to any gNMI target over TLS and checks that
// interface counters stream in, correctly shaped, sample after sample.
//
// In CI the target is the OpenConfig reference implementation
// (scripts/gnmi-reference-target.sh, mutual TLS) — vendor-neutral and
// always run. The same test can be pointed at real hardware or a NOS
// container from any vendor by setting GNMI_TARGET/GNMI_PATH; the optional
// "device" CI job does that with Nokia SR Linux.
//
// GNMI_TLS_CA verifies the target (otherwise verification is skipped, as
// for lab devices with self-signed certificates); GNMI_TLS_CERT/KEY enable
// mTLS.
func TestGNMITarget(t *testing.T) {
	e := env(t, "GNMI_TARGET", "GNMI_USERNAME", "GNMI_PASSWORD")
	path := envOr("GNMI_PATH", "/interface/statistics")
	enc := envOr("GNMI_ENCODING", "json_ietf")
	t.Setenv("ITEST_GNMI_PASSWORD", e["GNMI_PASSWORD"])

	in, err := gnmi.New(plugin(t, "input", `type: gnmi
    targets: [{address: "`+e["GNMI_TARGET"]+`", name: dut}]
    username: "`+e["GNMI_USERNAME"]+`"
    password: "${ITEST_GNMI_PASSWORD}"
    tls: `+tlsYAML()+`
    encoding: `+enc+`
    redial_min: 2s
    redial_max: 5s
    subscriptions:
      - {name: stats, path: "`+path+`", mode: sample, sample_interval: 5s}`), testEnv())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	sink := make(chanSink, 1000)
	done := make(chan struct{})
	go func() { in.Run(ctx, sink); close(done) }()
	defer func() { cancel(); <-done }()

	samples := map[string]int{} // interface -> samples seen
	for {
		select {
		case m := <-sink:
			if m.Source() != "dut" || m.Tags["subscription"] != "stats" {
				t.Fatalf("bad tags %v", m.Tags)
			}
			ifname := m.Tags["interface_name"]
			if ifname == "" {
				t.Logf("metric without interface_name: %s %v", m.Name, m.Tags)
				continue
			}
			// Devices may split leaves across notifications; only the
			// counter-bearing metrics are checked for numeric decoding.
			var numeric, hasOctets bool
			for k, v := range m.Fields {
				if strings.HasSuffix(k, "in-octets") {
					hasOctets = true
					_, numeric = telemetry.ToFloat(v)
				}
			}
			if !hasOctets {
				continue
			}
			if !numeric {
				t.Fatalf("in-octets not numeric in %s: %v", m.Name, m.Fields)
			}
			samples[ifname]++
			// Two samples of the same interface prove the stream is live
			// (sample_interval honoured), not just the initial sync.
			if samples[ifname] >= 2 {
				t.Logf("received %s: %d fields, e.g. tags=%v", m.Name, len(m.Fields), m.Tags)
				return
			}
		case <-ctx.Done():
			t.Fatalf("no repeated samples from device within timeout; got %v", samples)
		}
	}
}

var getenv = os.Getenv

func tlsYAML() string {
	ca := getenv("GNMI_TLS_CA")
	if ca == "" {
		return "{enabled: true, insecure_skip_verify: true}"
	}
	y := "{enabled: true, ca_file: \"" + ca + "\", server_name: localhost"
	if c, k := getenv("GNMI_TLS_CERT"), getenv("GNMI_TLS_KEY"); c != "" && k != "" {
		y += ", cert_file: \"" + c + "\", key_file: \"" + k + "\""
	}
	return y + "}"
}

func envOr(k, d string) string {
	if v := getenv(k); v != "" {
		return v
	}
	return d
}
