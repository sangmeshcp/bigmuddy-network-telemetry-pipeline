package gnmi

import (
	"context"
	"net"
	"testing"
	"time"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/testutil"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func startTLSFake(t *testing.T, f *fakeTarget, srvTLS tlsutil.Config) string {
	t.Helper()
	tc, err := srvTLS.Server()
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tc)))
	gpb.RegisterGNMIServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// runFor runs the input briefly and reports whether a metric arrived.
func runFor(t *testing.T, in *Input, wait time.Duration) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	sink := chanSink{make(chan *telemetry.Metric, 10)}
	done := make(chan struct{})
	go func() { in.Run(ctx, sink); close(done) }()
	defer func() { cancel(); <-done }()
	select {
	case <-sink.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func TestTLSAndMTLS(t *testing.T) {
	certs := testutil.NewCerts(t)
	sub := `
    username: admin
    password: pw
    redial_min: 20ms
    redial_max: 40ms
    subscriptions: [{path: /interfaces, sample_interval: 1s}]
outputs: [{type: file}]`

	// Server-auth TLS: trusting the right CA works, the wrong CA does not.
	addr := startTLSFake(t, &fakeTarget{}, tlsutil.Config{Enabled: true, CertFile: certs.ServerCert, KeyFile: certs.ServerKey})
	in, _ := newInput(t, "inputs:\n  - type: gnmi\n    targets: [\""+addr+"\"]\n    tls: {enabled: true, ca_file: "+certs.CA+", server_name: localhost}"+sub)
	if !runFor(t, in, 3*time.Second) {
		t.Error("TLS session with trusted CA produced no data")
	}
	in, _ = newInput(t, "inputs:\n  - type: gnmi\n    targets: [\""+addr+"\"]\n    tls: {enabled: true, ca_file: "+certs.OtherCA+"}"+sub)
	if runFor(t, in, 500*time.Millisecond) {
		t.Error("TLS session with untrusted CA should fail")
	}
	// Plaintext client against TLS server must fail.
	in, _ = newInput(t, "inputs:\n  - type: gnmi\n    targets: [\""+addr+"\"]"+sub)
	if runFor(t, in, 500*time.Millisecond) {
		t.Error("plaintext client should not talk to TLS server")
	}

	// mTLS: target requires a client certificate.
	maddr := startTLSFake(t, &fakeTarget{}, tlsutil.Config{Enabled: true, CertFile: certs.ServerCert, KeyFile: certs.ServerKey, CAFile: certs.CA})
	in, _ = newInput(t, "inputs:\n  - type: gnmi\n    targets: [\""+maddr+"\"]\n    tls: {enabled: true, ca_file: "+certs.CA+"}"+sub)
	if runFor(t, in, 500*time.Millisecond) {
		t.Error("mTLS target accepted client without certificate")
	}
	in, _ = newInput(t, "inputs:\n  - type: gnmi\n    targets: [\""+maddr+"\"]\n    tls: {enabled: true, ca_file: "+certs.CA+", cert_file: "+certs.ClientCert+", key_file: "+certs.ClientKey+"}"+sub)
	if !runFor(t, in, 3*time.Second) {
		t.Error("mTLS with client certificate produced no data")
	}
}

func TestPasswordFileAndOptions(t *testing.T) {
	f := &fakeTarget{}
	addr := startFake(t, f)
	pw := t.TempDir() + "/pw"
	writeFile(t, pw, "pw\n")
	in, _ := newInput(t, `
inputs:
  - type: gnmi
    targets: ["`+addr+`"]
    username: admin
    password_file: `+pw+`
    encoding: json_ietf
    compression: gzip
    target: gw-target
    updates_only: true
    subscriptions: [{path: "/a/b[k=v]", mode: target_defined, heartbeat_interval: 1m, suppress_redundant: true}]
outputs: [{type: file}]`)
	if !runFor(t, in, 3*time.Second) {
		t.Fatal("no data with password_file")
	}
	req := f.lastReq.Load().GetSubscribe()
	if req.Encoding != gpb.Encoding_JSON_IETF || !req.UpdatesOnly || req.Prefix.GetTarget() != "gw-target" ||
		req.Subscription[0].Mode != gpb.SubscriptionMode_TARGET_DEFINED || !req.Subscription[0].SuppressRedundant ||
		req.Subscription[0].HeartbeatInterval != uint64(time.Minute) || req.Subscription[0].Path.Elem[1].Key["k"] != "v" {
		t.Errorf("request not built from options: %v", req)
	}
}

func TestSubscriptionTagging(t *testing.T) {
	in, _ := newInput(t, `
inputs:
  - type: gnmi
    targets: ["127.0.0.1:1"]
    subscriptions:
      - {name: all-if, path: /interfaces, sample_interval: 1s}
      - {name: counters, path: /interfaces/interface/state/counters, sample_interval: 1s}
      - {name: sys, path: /system, mode: on_change}
outputs: [{type: file}]`)
	n := func(prefix, path string) *gpb.Notification {
		return &gpb.Notification{Prefix: mustPath(t, prefix), Update: []*gpb.Update{{Path: mustPath(t, path)}}}
	}
	cases := map[*gpb.Notification]string{
		n("/interfaces", "/interface[name=x]/state/counters/in-octets"): "counters", // longest match wins
		n("/", "/interfaces/interface[name=x]/state/mtu"):               "all-if",
		n("/openconfig-system:system", "/state/hostname"):               "sys",
		n("/", "/bgp"): "",
	}
	for notif, want := range cases {
		if got := in.subscriptionFor(notif); got != want {
			t.Errorf("subscriptionFor(%v) = %q, want %q", notif, got, want)
		}
	}
}
