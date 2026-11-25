//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/iceberg"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/outputtest"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// TestIcebergRESTOnMinIO writes through a real Iceberg REST catalog with
// data files on MinIO, retries the batch, and scans the table back through
// the catalog.
func TestIcebergRESTOnMinIO(t *testing.T) {
	e := env(t, "ICEBERG_REST_URI", "ICEBERG_WAREHOUSE_BUCKET", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mc, err := minio.New(e["MINIO_ENDPOINT"], &minio.Options{
		Creds: credentials.NewStaticV4(e["MINIO_ACCESS_KEY"], e["MINIO_SECRET_KEY"], ""), BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := mc.BucketExists(ctx, e["ICEBERG_WAREHOUSE_BUCKET"]); !ok {
		if err := mc.MakeBucket(ctx, e["ICEBERG_WAREHOUSE_BUCKET"], minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	ns := fmt.Sprintf("itest_%d", time.Now().UnixNano())
	props := fmt.Sprintf(`{s3.endpoint: "http://%s", s3.access-key-id: "%s", s3.secret-access-key: "%s", s3.region: us-east-1, s3.force-virtual-addressing: "false"}`,
		e["MINIO_ENDPOINT"], e["MINIO_ACCESS_KEY"], e["MINIO_SECRET_KEY"])
	out, err := iceberg.New(plugin(t, "output", `{type: iceberg, namespace: `+ns+`, partition: hour,
    catalog: {type: rest, uri: "`+e["ICEBERG_REST_URI"]+`", properties: `+props+`}}`), testEnv())
	if err != nil {
		t.Fatal(err)
	}
	golden := outputtest.GoldenBatch()
	for i := 0; i < 2; i++ { // second write is a retry
		if err := out.Write(ctx, golden); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	cat, err := catalog.Load(ctx, "itest", map[string]string{"type": "rest", "uri": e["ICEBERG_REST_URI"],
		"s3.endpoint": "http://" + e["MINIO_ENDPOINT"], "s3.access-key-id": e["MINIO_ACCESS_KEY"],
		"s3.secret-access-key": e["MINIO_SECRET_KEY"], "s3.region": "us-east-1", "s3.force-virtual-addressing": "false"})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.LoadTable(ctx, []string{ns, "telemetry"})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(tbl.Metadata().Snapshots()); n != 1 {
		t.Errorf("retry produced %d snapshots, want 1", n)
	}
	spec := tbl.Metadata().PartitionSpec()
	if f := spec.Field(0).Name; f != "timestamp_hour" {
		t.Errorf("partition field %q", f)
	}
	at, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer at.Release()
	if int(at.NumRows()) != len(telemetry.RowsOf(golden)) {
		t.Errorf("scanned %d rows, want %d", at.NumRows(), len(telemetry.RowsOf(golden)))
	}
	// Spot-check a value through the catalog read path.
	tr := array.NewTableReader(at, 1024)
	defer tr.Release()
	found := false
	for tr.Next() {
		rec := tr.RecordBatch()
		var fi, si int
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "field":
				fi = i
			case "value_string":
				si = i
			}
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			if rec.Column(fi).(*array.String).Value(i) == "state/counters/huge" &&
				rec.Column(si).(*array.String).Value(i) == "18446744073709551615" {
				found = true
			}
		}
	}
	if !found {
		t.Error("uint64 > MaxInt64 value not read back exactly")
	}
}
