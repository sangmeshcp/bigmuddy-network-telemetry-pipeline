package ciscomdt

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	mdpb "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/gen/cisco/mdtdialout"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/testutil"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func startCfg(t *testing.T, body string) (net.Addr, chanSink) {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs:\n  - type: cisco_mdt\n    listen: \"127.0.0.1:0\"\n" + body + "\noutputs: [{type: file}]"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(&cfg.Inputs[0], pipeline.Env{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	in := p.(*Input)
	ctx, cancel := context.WithCancel(context.Background())
	sink := make(chanSink, 100)
	done := make(chan struct{})
	go func() { in.Run(ctx, sink); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return <-in.addr, sink
}

func expectNone(t *testing.T, s chanSink) {
	t.Helper()
	select {
	case m := <-s:
		t.Errorf("unexpected metric %v", m.Name)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestGRPCDialoutMTLS(t *testing.T) {
	certs := testutil.NewCerts(t)
	addr, sink := startCfg(t, "    transport: grpc\n    tls: {enabled: true, cert_file: "+certs.ServerCert+
		", key_file: "+certs.ServerKey+", ca_file: "+certs.CA+"}")

	send := func(clientTLS tlsutil.Config) error {
		tc, err := clientTLS.Client()
		if err != nil {
			t.Fatal(err)
		}
		conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(credentials.NewTLS(tc)))
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stream, err := mdpb.NewGRPCMdtDialoutClient(conn).MdtDialout(ctx)
		if err != nil {
			return err
		}
		if err := stream.Send(&mdpb.MdtDialoutArgs{Data: fixture(t, "memory_summary")}); err != nil {
			return err
		}
		stream.CloseSend()
		_, err = stream.Recv() // io.EOF on success, handshake error otherwise
		if err == io.EOF {
			return nil
		}
		return err
	}

	if err := send(tlsutil.Config{Enabled: true, CAFile: certs.CA}); err == nil {
		t.Error("device without client certificate was accepted")
	}
	expectNone(t, sink)
	if err := send(tlsutil.Config{Enabled: true, CAFile: certs.CA, CertFile: certs.ClientCert, KeyFile: certs.ClientKey}); err != nil {
		t.Fatalf("mTLS device rejected: %v", err)
	}
	if ms := recv(t, sink, 4); ms[0].Source() != "dr02" {
		t.Errorf("unexpected %v", ms[0].Tags)
	}
}

func TestTCPDialoutTLS(t *testing.T) {
	certs := testutil.NewCerts(t)
	addr, sink := startCfg(t, "    transport: tcp\n    idle_timeout: 5s\n    tls: {enabled: true, cert_file: "+certs.ServerCert+", key_file: "+certs.ServerKey+"}")
	tc, _ := tlsutil.Config{Enabled: true, CAFile: certs.CA}.Client()
	conn, err := tls.Dial("tcp", addr.String(), tc)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write(EncodeST(fixture(t, "memory_summary"), false))
	recv(t, sink, 4)

	// Plaintext framing against a TLS listener yields nothing.
	raw, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	raw.Write(EncodeST(fixture(t, "memory_summary"), false))
	raw.Close()
	expectNone(t, sink)
}

func TestSTHeaderHandling(t *testing.T) {
	addr, sink := startCfg(t, "    transport: tcp")
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	hb := EncodeST(nil, false)
	hb[1] = stTypeHeartbeat
	conn.Write(hb) // heartbeat is skipped, session continues
	conn.Write(EncodeST(fixture(t, "memory_summary"), false))
	recv(t, sink, 4)

	for name, mutate := range map[string]func([]byte){
		"version": func(b []byte) { b[5] = 2 },
		"flags":   func(b []byte) { b[7] = 1 },
	} {
		c, _ := net.Dial("tcp", addr.String())
		b := EncodeST([]byte("x"), false)
		mutate(b)
		c.Write(b)
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.Read(make([]byte, 1)); err == nil || isTimeout(err) {
			t.Errorf("bad %s: connection not closed (%v)", name, err)
		}
		c.Close()
	}
	if (stHeader{encap: 99}).encoding() != EncodingUnknown || (stHeader{encap: stEncapGPBKV}).encoding() != EncodingGPB {
		t.Error("encap mapping")
	}
}

func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}

func TestConfigErrors(t *testing.T) {
	for _, body := range []string{
		"inputs: [{type: cisco_mdt}]",
		"inputs: [{type: cisco_mdt, listen: ':0', transport: sctp}]",
		"inputs: [{type: cisco_mdt, listen: ':0', transport: udp, tls: {enabled: true, cert_file: a, key_file: b}}]",
		"inputs: [{type: cisco_mdt, listen: ':0', tls: {enabled: true}}]",
	} {
		cfg, err := config.Parse([]byte(body + "\noutputs: [{type: file}]"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := New(&cfg.Inputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()}); err == nil {
			t.Errorf("expected error for %s", body)
		}
	}
	// Listen failure is a fatal Run error (port in use).
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	cfg, _ := config.Parse([]byte("inputs: [{type: cisco_mdt, transport: tcp, listen: '" + l.Addr().String() + "'}]\noutputs: [{type: file}]"))
	p, _ := New(&cfg.Inputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
	if err := p.Run(context.Background(), make(chanSink)); err == nil {
		t.Error("expected listen error")
	}
}
