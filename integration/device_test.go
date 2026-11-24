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

// TestGNMIRealDevice subscribes to a real network OS (Nokia SR Linux in CI,
// started with containerlab) and checks that interface counters arrive and
// are shaped correctly. Works against any gNMI device given a path that
// exists on it.
func TestGNMIRealDevice(t *testing.T) {
	e := env(t, "GNMI_TARGET", "GNMI_USERNAME", "GNMI_PASSWORD")
	path := envOr("GNMI_PATH", "/interface/statistics")
	enc := envOr("GNMI_ENCODING", "json_ietf")
	t.Setenv("ITEST_GNMI_PASSWORD", e["GNMI_PASSWORD"])

	in, err := gnmi.New(plugin(t, "input", `type: gnmi
    targets: [{address: "`+e["GNMI_TARGET"]+`", name: dut}]
    username: "`+e["GNMI_USERNAME"]+`"
    password: "${ITEST_GNMI_PASSWORD}"
    tls: {enabled: true, insecure_skip_verify: true}
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
			var numeric bool
			for k, v := range m.Fields {
				if _, ok := telemetry.ToFloat(v); ok && strings.HasSuffix(k, "in-octets") {
					numeric = true
				}
			}
			if !numeric {
				t.Fatalf("no numeric in-octets field in %s: %v", m.Name, m.Fields)
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

func envOr(k, d string) string {
	if v := getenv(k); v != "" {
		return v
	}
	return d
}
