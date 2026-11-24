//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/outputtest"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/parquet"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// TestParquetOnMinIOReadByClickHouse writes the golden batch to a real
// S3 implementation (MinIO) with the parquet output, then has ClickHouse
// read the files through s3() and checks the data matches exactly: proof
// that a warehouse engine reads the lake, not just our own reader.
func TestParquetOnMinIOReadByClickHouse(t *testing.T) {
	e := env(t, "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "CLICKHOUSE_HTTP")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	mc, err := minio.New(e["MINIO_ENDPOINT"], &minio.Options{
		Creds: credentials.NewStaticV4(e["MINIO_ACCESS_KEY"], e["MINIO_SECRET_KEY"], ""), BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	bucket := fmt.Sprintf("lake-%d", time.Now().UnixNano())
	if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ITEST_MINIO_SECRET", e["MINIO_SECRET_KEY"])
	out, err := parquet.New(plugin(t, "output", `{type: parquet, url: "s3://`+bucket+`/net",
    s3: {endpoint: "`+e["MINIO_ENDPOINT"]+`", access_key: "`+e["MINIO_ACCESS_KEY"]+`", secret_key: "${ITEST_MINIO_SECRET}", path_style: true, insecure: true}}`), testEnv())
	if err != nil {
		t.Fatal(err)
	}
	golden := outputtest.GoldenBatch()
	for i := 0; i < 2; i++ { // second write is a retry: must not duplicate
		if err := out.Write(ctx, golden); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	src := fmt.Sprintf("s3('http://%s/%s/net/telemetry/*/*/*.parquet', '%s', '%s', 'Parquet')",
		e["MINIO_ENDPOINT"], bucket, e["MINIO_ACCESS_KEY"], e["MINIO_SECRET_KEY"])
	want := telemetry.RowsOf(golden)
	var wantSum float64
	for _, r := range want {
		if r.ValueDouble != nil && r.ValueType != "uint" {
			wantSum += *r.ValueDouble
		}
	}
	got := chQuery(t, e["CLICKHOUSE_HTTP"], fmt.Sprintf(
		"SELECT count(), uniqExact(_path), countIf(metric_type = 'counter'), "+
			"sumIf(value_double, value_type != 'uint'), "+
			"anyIf(value_string, field = 'state/counters/huge'), "+
			"anyIf(tags['site'], source = 'core1'), "+
			"toString(min(timestamp)) FROM %s", src))
	exp := fmt.Sprintf("%d\t2\t%d\t%g\t18446744073709551615\tdfw-1 \"east\", rack=4 ✓\t2026-11-24 09:59:59.123456",
		len(want), countKind(want, "counter"), wantSum)
	if got != exp {
		t.Errorf("ClickHouse read of the lake:\n got  %q\n want %q", got, exp)
	}
	// Hive partitioning: ClickHouse derives dt/hour from the path.
	if parts := chQuery(t, e["CLICKHOUSE_HTTP"], "SELECT groupArray(DISTINCT h) FROM (SELECT toUInt8(hour) AS h FROM "+src+" ORDER BY h)"); parts != "[9,10]" {
		t.Errorf("hive partitions: %s", parts)
	}
}

func countKind(rows []telemetry.Row, kind string) int {
	n := 0
	for _, r := range rows {
		if r.MetricType == kind {
			n++
		}
	}
	return n
}

func chQuery(t *testing.T, base, q string) string {
	t.Helper()
	resp, err := http.Post(base+"/?"+url.Values{"use_hive_partitioning": {"1"}}.Encode(), "text/plain", strings.NewReader(q))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("clickhouse: %s", b)
	}
	return strings.TrimSpace(string(b))
}
