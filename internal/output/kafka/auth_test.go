package kafka

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/testutil"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/tlsutil"
)

func build(t *testing.T, yaml string) (pipeline.Output, error) {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	return New(&cfg.Outputs[0], pipeline.Env{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Registry: prometheus.NewRegistry()})
}

func one() []*telemetry.Metric {
	m := telemetry.New("/n", time.Now())
	m.Tags["source"] = "r1"
	m.Fields["v"] = int64(1)
	return []*telemetry.Metric{m}
}

func TestAllOptionVariantsBuild(t *testing.T) {
	for _, comp := range []string{"zstd", "lz4", "snappy", "gzip", "none"} {
		for _, acks := range []string{"all", "leader", "none"} {
			for _, key := range []string{"source", "name", "none"} {
				o, err := build(t, fmt.Sprintf(`{type: kafka, brokers: ["127.0.0.1:1"], topic: t, compression: %s, acks: %s, key: %s}`, comp, acks, key))
				if err != nil {
					t.Fatalf("%s/%s/%s: %v", comp, acks, key, err)
				}
				o.Close(context.Background())
			}
		}
	}
	if _, err := build(t, `{type: kafka, brokers: [a:1], topic: t, key: partition}`); err == nil {
		t.Error("bad key accepted")
	}
}

func TestSASLMechanisms(t *testing.T) {
	for _, mech := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		t.Run(mech, func(t *testing.T) {
			cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "t"),
				kfake.EnableSASL(), kfake.Superuser(mech, "pipeline", "s3cret"))
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			brokers := strings.Join(cluster.ListenAddrs(), ",")
			pwFile := filepath.Join(t.TempDir(), "pw")
			os.WriteFile(pwFile, []byte("s3cret\n"), 0o600)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			good, err := build(t, `{type: kafka, brokers: [`+brokers+`], topic: t, sasl: {mechanism: `+mech+`, username: pipeline, password_file: `+pwFile+`}}`)
			if err != nil {
				t.Fatal(err)
			}
			defer good.Close(context.Background())
			if err := good.Write(ctx, one()); err != nil {
				t.Fatalf("authenticated produce failed: %v", err)
			}

			bad, _ := build(t, `{type: kafka, brokers: [`+brokers+`], topic: t, sasl: {mechanism: `+mech+`, username: pipeline, password: wrong}}`)
			defer bad.Close(context.Background())
			sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer scancel()
			if err := bad.Write(sctx, one()); err == nil {
				t.Error("wrong password was accepted")
			}
		})
	}
}

func TestTLS(t *testing.T) {
	certs := testutil.NewCerts(t)
	srvTLS, err := tlsutil.Config{Enabled: true, CertFile: certs.ServerCert, KeyFile: certs.ServerKey}.Server()
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "t"), kfake.TLS(srvTLS))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := strings.Join(cluster.ListenAddrs(), ",")
	o, err := build(t, `{type: kafka, brokers: [`+brokers+`], topic: t, key: name, tls: {enabled: true, ca_file: `+certs.CA+`, server_name: localhost}}`)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := o.Write(ctx, one()); err != nil {
		t.Fatalf("TLS produce failed: %v", err)
	}
	o.Close(ctx)

	// Verify key=name was applied.
	tc, _ := tlsutil.Config{Enabled: true, CAFile: certs.CA, ServerName: "localhost"}.Client()
	cons, _ := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DialTLSConfig(tc),
		kgo.ConsumeTopics("t"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cons.Close()
	recs := cons.PollFetches(ctx).Records()
	if len(recs) != 1 || string(recs[0].Key) != "/n" {
		t.Errorf("records %v", recs)
	}
}
