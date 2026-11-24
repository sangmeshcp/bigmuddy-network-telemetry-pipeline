package gnmi

// Vendor-neutral conformance tests. The target here is the OpenConfig
// project's reference fake gNMI agent (github.com/openconfig/gnmi/testing/
// fake), published by the authors of the gNMI specification. No vendor
// image or licence is involved, so these run on every `go test`.

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
	fakegnmi "github.com/openconfig/gnmi/testing/fake/gnmi"
	fpb "github.com/openconfig/gnmi/testing/fake/proto"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/testutil"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

// startAgent runs the reference target and returns its loopback address.
func startAgent(t *testing.T, cfg *fpb.Config, opts ...grpc.ServerOption) (*fakegnmi.Agent, string) {
	t.Helper()
	cfg.Port = 0
	cfg.ClientType = fpb.Config_GRPC_GNMI
	cfg.DisableEof = true // hold the stream open like a real device
	a, err := fakegnmi.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	_, port, _ := strings.Cut(a.Address(), "]:")
	if port == "" {
		port = a.Address()[strings.LastIndex(a.Address(), ":")+1:]
	}
	return a, "127.0.0.1:" + port
}

func conformanceInput(t *testing.T, addr, extra string) (*Input, *prometheus.Registry) {
	t.Helper()
	cfg, err := config.Parse([]byte(`
inputs:
  - type: gnmi
    targets: [{address: "` + addr + `", name: dut}]
    redial_min: 50ms
    redial_max: 100ms
    subscriptions:
      - {name: ifaces, path: /interfaces, mode: sample, sample_interval: 1s}
      - {name: system, path: /system, mode: on_change}
` + extra + `
outputs: [{type: file}]`))
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	in, err := New(&cfg.Inputs[0], pipeline.Env{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	return in.(*Input), reg
}

// collect runs the input until n metrics have arrived.
func collect(t *testing.T, in *Input, n int) []*telemetry.Metric {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	sink := chanSink{make(chan *telemetry.Metric, n+100)}
	done := make(chan struct{})
	go func() { in.Run(ctx, sink); close(done) }()
	defer func() { cancel(); <-done }()
	var out []*telemetry.Metric
	for len(out) < n {
		select {
		case m := <-sink.ch:
			out = append(out, m)
		case <-ctx.Done():
			t.Fatalf("got %d/%d metrics from reference target", len(out), n)
		}
	}
	return out
}

func p(t *testing.T, s string) *gpb.Path { return mustPath(t, s) }

func upd(path *gpb.Path, v *gpb.TypedValue) *gpb.Update { return &gpb.Update{Path: path, Val: v} }

// TestConformanceTypedValues replays a scripted stream covering every
// TypedValue encoding, origin/target prefixes, the legacy path form,
// JSON_IETF containers, deletes and sync_response.
func TestConformanceTypedValues(t *testing.T) {
	const ts = int64(1_700_000_000_000_000_000)
	ocPrefix := &gpb.Path{Origin: "openconfig", Target: "dut", Elem: []*gpb.PathElem{{Name: "interfaces"}}}
	responses := []*gpb.SubscribeResponse{
		// 1. Every scalar TypedValue on one interface instance.
		{Response: &gpb.SubscribeResponse_Update{Update: &gpb.Notification{Timestamp: ts, Prefix: ocPrefix, Update: []*gpb.Update{
			upd(p(t, "/interface[name=Ethernet1]/state/counters/in-octets"), &gpb.TypedValue{Value: &gpb.TypedValue_UintVal{UintVal: 18446744073709551615}}),
			upd(p(t, "/interface[name=Ethernet1]/state/counters/in-errors"), &gpb.TypedValue{Value: &gpb.TypedValue_IntVal{IntVal: -1}}),
			upd(p(t, "/interface[name=Ethernet1]/state/load"), &gpb.TypedValue{Value: &gpb.TypedValue_DoubleVal{DoubleVal: 0.25}}),
			upd(p(t, "/interface[name=Ethernet1]/state/enabled"), &gpb.TypedValue{Value: &gpb.TypedValue_BoolVal{BoolVal: true}}),
			upd(p(t, "/interface[name=Ethernet1]/state/oper-status"), &gpb.TypedValue{Value: &gpb.TypedValue_StringVal{StringVal: "UP"}}),
			upd(p(t, "/interface[name=Ethernet1]/state/description"), &gpb.TypedValue{Value: &gpb.TypedValue_AsciiVal{AsciiVal: "uplink"}}),
			upd(p(t, "/interface[name=Ethernet1]/state/hw-id"), &gpb.TypedValue{Value: &gpb.TypedValue_BytesVal{BytesVal: []byte{1, 2}}}),
			//lint:ignore SA1019 deprecated encodings must still decode
			upd(p(t, "/interface[name=Ethernet1]/state/temp"), &gpb.TypedValue{Value: &gpb.TypedValue_FloatVal{FloatVal: 1.5}}),
			//lint:ignore SA1019 deprecated encodings must still decode
			upd(p(t, "/interface[name=Ethernet1]/state/util"), &gpb.TypedValue{Value: &gpb.TypedValue_DecimalVal{DecimalVal: &gpb.Decimal64{Digits: 995, Precision: 1}}}),
			upd(p(t, "/interface[name=Ethernet1]/state/vlans"), &gpb.TypedValue{Value: &gpb.TypedValue_LeaflistVal{LeaflistVal: &gpb.ScalarArray{Element: []*gpb.TypedValue{
				{Value: &gpb.TypedValue_UintVal{UintVal: 10}}, {Value: &gpb.TypedValue_UintVal{UintVal: 20}}}}}}),
		}}}},
		// 2. JSON_IETF container for a nested list instance.
		{Response: &gpb.SubscribeResponse_Update{Update: &gpb.Notification{Timestamp: ts + 1, Prefix: ocPrefix, Update: []*gpb.Update{
			upd(p(t, "/interface[name=Ethernet1]/subinterfaces/subinterface[index=0]/state"), &gpb.TypedValue{Value: &gpb.TypedValue_JsonIetfVal{JsonIetfVal: []byte(
				`{"openconfig-interfaces:counters":{"in-pkts":"42","out-pkts":"7"},"ifindex":5}`)}}),
		}}}},
		// 3. Initial sync complete.
		{Response: &gpb.SubscribeResponse_SyncResponse{SyncResponse: true}},
		// 4. Delete-only notification: must not produce a metric or error.
		{Response: &gpb.SubscribeResponse_Update{Update: &gpb.Notification{Timestamp: ts + 2, Prefix: ocPrefix,
			Delete: []*gpb.Path{p(t, "/interface[name=Ethernet9]")}}}},
		// 5. Legacy (gNMI < 0.4) string path elements.
		{Response: &gpb.SubscribeResponse_Update{Update: &gpb.Notification{Timestamp: ts + 3, Update: []*gpb.Update{{
			//lint:ignore SA1019 legacy path form sent by older devices
			Path: &gpb.Path{Element: []string{"interfaces", "interface[name=Ethernet2]", "state", "counters", "in-octets"}},
			Val:  &gpb.TypedValue{Value: &gpb.TypedValue_UintVal{UintVal: 99}},
		}}}}},
		// 6. Keyless container (system) as a separate subscription.
		{Response: &gpb.SubscribeResponse_Update{Update: &gpb.Notification{Timestamp: ts + 4, Update: []*gpb.Update{
			upd(p(t, "/system/state/hostname"), &gpb.TypedValue{Value: &gpb.TypedValue_StringVal{StringVal: "dut1"}}),
		}}}},
	}
	// DisableSync: the reference target otherwise appends its own
	// sync_response; we script ours explicitly.
	agent, addr := startAgent(t, &fpb.Config{Target: "dut", DisableSync: true,
		Generator: &fpb.Config_Fixed{Fixed: &fpb.FixedGenerator{Responses: responses}}})
	in, reg := conformanceInput(t, addr, "")
	ms := collect(t, in, 4)

	// 1. scalar values
	eth1 := ms[0]
	if eth1.Name != "openconfig:/interfaces/interface" || eth1.Tags["interface_name"] != "Ethernet1" ||
		eth1.Source() != "dut" || eth1.Tags[telemetry.TagSubscription] != "ifaces" || eth1.Timestamp.UnixNano() != ts {
		t.Errorf("identity: %s %v %v", eth1.Name, eth1.Tags, eth1.Timestamp)
	}
	want := map[string]any{
		"state/counters/in-octets": uint64(18446744073709551615),
		"state/counters/in-errors": int64(-1),
		"state/load":               0.25,
		"state/enabled":            true,
		"state/oper-status":        "UP",
		"state/description":        "uplink",
		"state/hw-id":              "AQI=",
		"state/temp":               1.5,
		"state/util":               99.5,
		"state/vlans":              "10,20",
	}
	if len(eth1.Fields) != len(want) {
		t.Errorf("fields: got %v", eth1.Fields)
	}
	for k, v := range want {
		if eth1.Fields[k] != v {
			t.Errorf("%s = %#v, want %#v", k, eth1.Fields[k], v)
		}
	}
	// 2. JSON_IETF nested list
	sub := ms[1]
	if sub.Name != "openconfig:/interfaces/interface/subinterfaces/subinterface" || sub.Tags["subinterface_index"] != "0" ||
		sub.Fields["state/counters/in-pkts"] != int64(42) || sub.Fields["state/ifindex"] != int64(5) {
		t.Errorf("json_ietf: %s %v %v", sub.Name, sub.Tags, sub.Fields)
	}
	// 5. legacy path
	if l := ms[2]; l.Name != "/interfaces/interface" || l.Tags["interface_name"] != "Ethernet2" ||
		l.Fields["state/counters/in-octets"] != uint64(99) || l.Tags[telemetry.TagSubscription] != "ifaces" {
		t.Errorf("legacy path: %s %v %v", l.Name, l.Tags, l.Fields)
	}
	// 6. keyless container, matched to the second subscription
	if s := ms[3]; s.Name != "/system/state" || s.Fields["hostname"] != "dut1" || s.Tags[telemetry.TagSubscription] != "system" {
		t.Errorf("system: %s %v %v", s.Name, s.Tags, s.Fields)
	}
	if v := promtest.ToFloat64(in.syncs.WithLabelValues("dut")); v != 1 {
		t.Errorf("sync_response count = %v", v)
	}
	if v := promtest.ToFloat64(in.notifications.WithLabelValues("dut")); v != 5 {
		t.Errorf("notifications = %v, want 5 (incl. delete-only)", v)
	}
	if n, _ := promtest.GatherAndCount(reg, "gnmi_unsupported_values_total"); n != 0 {
		t.Errorf("no value should be unsupported")
	}

	// The target recorded a well-formed STREAM subscription.
	reqs := agent.Requests()
	if len(reqs) == 0 {
		t.Fatal("reference target recorded no SubscribeRequest")
	}
	sl := reqs[0].GetSubscribe()
	if sl.GetMode() != gpb.SubscriptionList_STREAM || len(sl.GetSubscription()) != 2 ||
		sl.GetSubscription()[0].GetMode() != gpb.SubscriptionMode_SAMPLE ||
		sl.GetSubscription()[0].GetSampleInterval() != uint64(time.Second) ||
		sl.GetSubscription()[1].GetMode() != gpb.SubscriptionMode_ON_CHANGE {
		t.Errorf("subscribe request: %v", reqs[0])
	}
}

// TestConformanceLiveStreamTLS uses the reference target's random generator
// (the same engine its standalone fake_server uses) over TLS: a continuous,
// paced stream of changing counters, as a device in SAMPLE mode produces.
func TestConformanceLiveStreamTLS(t *testing.T) {
	certs := testutil.NewCerts(t)
	stc, err := tlsutil.Config{Enabled: true, CertFile: certs.ServerCert, KeyFile: certs.ServerKey}.Server()
	if err != nil {
		t.Fatal(err)
	}
	const step = int64(10 * time.Millisecond)
	ts := &fpb.Timestamp{Timestamp: time.Now().UnixNano(), DeltaMin: step, DeltaMax: step}
	counter := func(ifname string) *fpb.Value {
		return &fpb.Value{
			Path:      []string{"interfaces", "interface[name=" + ifname + "]", "state", "counters", "in-octets"},
			Timestamp: ts,
			Value: &fpb.Value_IntValue{IntValue: &fpb.IntValue{Value: 1000,
				Distribution: &fpb.IntValue_Range{Range: &fpb.IntRange{Minimum: 0, Maximum: 1 << 40, DeltaMin: 1, DeltaMax: 5000}}}},
		}
	}
	// The agent's random-value engine is driven by Config.values/seed (the
	// same fields its standalone fake_server reads); the newer "random"
	// generator oneof is not consulted by this agent version.
	values := []*fpb.Value{
		counter("eth0"), counter("eth1"),
		{Path: []string{"interfaces", "interface[name=eth0]", "state", "oper-status"}, Timestamp: ts,
			Value: &fpb.Value_StringValue{StringValue: &fpb.StringValue{Distribution: &fpb.StringValue_List{
				List: &fpb.StringList{Options: []string{"UP", "DOWN"}, Random: true}}}}},
	}
	cfg := &fpb.Config{Target: "dut", EnableDelay: true}
	//lint:ignore SA1019 the reference agent reads these deprecated fields
	cfg.Seed, cfg.Values = 1, values
	_, addr := startAgent(t, cfg,
		grpc.Creds(credentials.NewTLS(stc)))

	in, _ := conformanceInput(t, addr, "    tls: {enabled: true, ca_file: "+certs.CA+", server_name: localhost}")
	ms := collect(t, in, 30)

	perIf := map[string][]*telemetry.Metric{}
	for _, m := range ms {
		if m.Name != "/interfaces/interface" || m.Source() != "dut" {
			t.Fatalf("unexpected metric %s %v", m.Name, m.Tags)
		}
		perIf[m.Tags["interface_name"]] = append(perIf[m.Tags["interface_name"]], m)
	}
	if len(perIf["eth0"]) < 3 || len(perIf["eth1"]) < 3 {
		t.Fatalf("expected repeated samples for both interfaces, got eth0=%d eth1=%d", len(perIf["eth0"]), len(perIf["eth1"]))
	}
	var sawStatus, changed bool
	var last int64 = -1
	var lastTS time.Time
	for _, m := range perIf["eth1"] {
		v, ok := m.Fields["state/counters/in-octets"].(int64)
		if !ok {
			t.Fatalf("in-octets not int64: %#v", m.Fields)
		}
		if last >= 0 && v != last {
			changed = true
		}
		if !lastTS.IsZero() && !m.Timestamp.After(lastTS) {
			t.Errorf("timestamps not increasing: %v then %v", lastTS, m.Timestamp)
		}
		last, lastTS = v, m.Timestamp
	}
	for _, m := range perIf["eth0"] {
		if s, ok := m.Fields["state/oper-status"].(string); ok && (s == "UP" || s == "DOWN") {
			sawStatus = true
		}
	}
	if !changed {
		t.Error("counter values never changed across samples")
	}
	if !sawStatus {
		t.Error("never saw oper-status string updates")
	}
}
