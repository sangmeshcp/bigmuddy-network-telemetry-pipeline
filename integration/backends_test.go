//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/influxdb"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/kafka"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/otlp"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// InfluxDB 2.x: write line protocol, read it back with Flux.
func TestInfluxDB2(t *testing.T) {
	e := env(t, "INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET")
	t.Setenv("ITEST_INFLUX_TOKEN", e["INFLUX_TOKEN"])
	out, err := influxdb.New(plugin(t, "output", `{type: influxdb, url: "`+e["INFLUX_URL"]+`", org: "`+e["INFLUX_ORG"]+
		`", bucket: "`+e["INFLUX_BUCKET"]+`", token: "${ITEST_INFLUX_TOKEN}"}`), testEnv())
	if err != nil {
		t.Fatal(err)
	}
	id := runID()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := out.Write(ctx, []*telemetry.Metric{sampleMetric(id, 18446744073709551000)}); err != nil {
		t.Fatalf("write: %v", err)
	}

	flux := `from(bucket: "` + e["INFLUX_BUCKET"] + `") |> range(start: -1h)
	  |> filter(fn: (r) => r.interface_name == "` + id + `")`
	eventually(t, 30*time.Second, "points in InfluxDB", func() (bool, string) {
		req, _ := http.NewRequest(http.MethodPost, e["INFLUX_URL"]+"/api/v2/query?org="+url.QueryEscape(e["INFLUX_ORG"]),
			strings.NewReader(flux))
		req.Header.Set("Authorization", "Token "+e["INFLUX_TOKEN"])
		req.Header.Set("Content-Type", "application/vnd.flux")
		req.Header.Set("Accept", "application/csv")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false, err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		s := string(b)
		ok := resp.StatusCode == 200 && strings.Contains(s, "state/counters/in-octets") &&
			strings.Contains(s, "18446744073709551000") && strings.Contains(s, "state/oper-status") &&
			strings.Contains(s, "openconfig:/interfaces/interface")
		return ok, s
	})
}

// Apache Kafka: produce through the output, consume the JSON back.
func TestKafka(t *testing.T) {
	e := env(t, "KAFKA_BROKERS")
	brokers := strings.Split(e["KAFKA_BROKERS"], ",")
	topic := runID()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admCl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer admCl.Close()
	if _, err := kadm.NewClient(admCl).CreateTopic(ctx, 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	out, err := kafka.New(plugin(t, "output", `{type: kafka, brokers: [`+e["KAFKA_BROKERS"]+`], topic: "`+topic+`", compression: zstd}`), testEnv())
	if err != nil {
		t.Fatal(err)
	}
	batch := []*telemetry.Metric{sampleMetric("Eth1", 1), sampleMetric("Eth2", 2), sampleMetric("Eth3", 3)}
	if err := out.Write(ctx, batch); err != nil {
		t.Fatalf("produce: %v", err)
	}
	out.Close(ctx)

	cons, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	seen := map[string]bool{}
	for len(seen) < 3 {
		fetches := cons.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("only consumed %v", seen)
		}
		for _, r := range fetches.Records() {
			var rec kafka.Record
			if err := json.Unmarshal(r.Value, &rec); err != nil {
				t.Fatal(err)
			}
			if string(r.Key) != "r1" || rec.Name != "openconfig:/interfaces/interface" {
				t.Errorf("record key=%s name=%s", r.Key, rec.Name)
			}
			seen[rec.Tags["interface_name"]] = true
		}
	}
}

// Prometheus 3.x native OTLP receiver: push, then query PromQL.
func TestPrometheusOTLP(t *testing.T) {
	e := env(t, "PROMETHEUS_URL")
	out, err := otlp.New(plugin(t, "output", `{type: otlp, protocol: http, endpoint: "`+e["PROMETHEUS_URL"]+`/api/v1/otlp/v1/metrics"}`), testEnv())
	if err != nil {
		t.Fatal(err)
	}
	id := runID()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := out.Write(ctx, []*telemetry.Metric{sampleMetric(id, 4242)}); err != nil {
		t.Fatalf("export: %v", err)
	}
	q := `interfaces_interface_state_counters_in_octets{interface_name="` + id + `"}`
	eventually(t, 30*time.Second, "series in Prometheus", func() (bool, string) {
		resp, err := http.Get(e["PROMETHEUS_URL"] + "/api/v1/query?query=" + url.QueryEscape(q))
		if err != nil {
			return false, err.Error()
		}
		defer resp.Body.Close()
		var r struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Value  []any             `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		b, _ := io.ReadAll(resp.Body)
		json.Unmarshal(b, &r)
		if len(r.Data.Result) != 1 {
			return false, string(b)
		}
		res := r.Data.Result[0]
		return res.Value[1] == "4242" && res.Metric["instance"] == "r1" && res.Metric["job"] == "network-telemetry", string(b)
	})
}
