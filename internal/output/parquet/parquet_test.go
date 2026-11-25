package parquet

import (
	"context"
	"io"
	"io/fs"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/outputtest"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func newOut(t *testing.T, yaml string) pipeline.Output {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + yaml))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func localFiles(t *testing.T, root string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			b, _ := os.ReadFile(p)
			rel, _ := filepath.Rel(root, p)
			out[filepath.ToSlash(rel)] = b
		}
		return nil
	})
	return out
}

func rowsOf(t *testing.T, files map[string][]byte) []telemetry.Row {
	t.Helper()
	var all []telemetry.Row
	for name, b := range files {
		rows, err := ReadRows(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		all = append(all, rows...)
	}
	return all
}

var keyRE = regexp.MustCompile(`^(lake/)?telemetry/dt=\d{4}-\d{2}-\d{2}/hour=\d{2}/part-[0-9a-f]{32}\.parquet$`)

func TestConformanceLocal(t *testing.T) {
	var dir string
	outputtest.Run(t, outputtest.Options{
		New: func(t *testing.T) pipeline.Output {
			dir = t.TempDir()
			return newOut(t, `{type: parquet, url: "file://`+dir+`"}`)
		},
		ReadBack: func(t *testing.T) []telemetry.Row {
			files := localFiles(t, dir)
			for name := range files {
				if !keyRE.MatchString(name) {
					t.Errorf("unexpected object key %q", name)
				}
			}
			// The golden batch spans an hour boundary: two partitions.
			if len(files) != 2 {
				t.Errorf("want 2 partition files, got %v", keys(files))
			}
			return rowsOf(t, files)
		},
		Idempotent:    true,
		TimePrecision: time.Microsecond,
	})
}

func keys(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func startFakeS3(t *testing.T) (endpoint string, client *minio.Client) {
	t.Helper()
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)
	endpoint = strings.TrimPrefix(srv.URL, "http://")
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4("k", "s", ""), Secure: false,
		BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	return endpoint, client
}

func TestConformanceS3(t *testing.T) {
	endpoint, client := startFakeS3(t)
	ctx := context.Background()
	var bucket string
	n := 0
	outputtest.Run(t, outputtest.Options{
		New: func(t *testing.T) pipeline.Output {
			n++
			bucket = "telemetry-" + string(rune('a'+n))
			if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
				t.Fatal(err)
			}
			return newOut(t, `{type: parquet, url: "s3://`+bucket+`/lake", compression: snappy,
    s3: {endpoint: "`+endpoint+`", access_key: k, secret_key: s, path_style: true, insecure: true}}`)
		},
		ReadBack: func(t *testing.T) []telemetry.Row {
			files := map[string][]byte{}
			for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
				if !keyRE.MatchString(obj.Key) {
					t.Errorf("unexpected key %q", obj.Key)
				}
				r, err := client.GetObject(ctx, bucket, obj.Key, minio.GetObjectOptions{})
				if err != nil {
					t.Fatal(err)
				}
				b, _ := io.ReadAll(r)
				files[obj.Key] = b
			}
			return rowsOf(t, files)
		},
		Idempotent:    true,
		TimePrecision: time.Microsecond,
	})
}

func TestS3MissingBucketIsPermanent(t *testing.T) {
	endpoint, _ := startFakeS3(t)
	o := newOut(t, `{type: parquet, url: "s3://does-not-exist/x",
    s3: {endpoint: "`+endpoint+`", access_key: k, secret_key: s, path_style: true, insecure: true}}`)
	err := o.Write(context.Background(), outputtest.GoldenBatch())
	if err == nil || !pipeline.IsPermanent(err) {
		t.Fatalf("missing bucket should be a permanent error, got %v", err)
	}
}

func TestNanosecondTimestamps(t *testing.T) {
	dir := t.TempDir()
	o := newOut(t, `{type: parquet, url: "file://`+dir+`", timestamp_unit: ns, compression: none}`)
	if err := o.Write(context.Background(), outputtest.GoldenBatch()); err != nil {
		t.Fatal(err)
	}
	outputtest.Compare(t, telemetry.RowsOf(outputtest.GoldenBatch()), rowsOf(t, localFiles(t, dir)), time.Nanosecond)
}

func TestConfigErrors(t *testing.T) {
	for _, y := range []string{
		`{type: parquet}`,
		`{type: parquet, url: "ftp://x/y"}`,
		`{type: parquet, url: "s3:///nobucket"}`,
		`{type: parquet, url: "file:///tmp/x", compression: lz77}`,
		`{type: parquet, url: "file:///tmp/x", timestamp_unit: s}`,
		`{type: parquet, url: "s3://b", s3: {sse: KMS}}`,
	} {
		cfg, _ := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - " + y))
		if _, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default()}); err == nil {
			t.Errorf("expected error for %s", y)
		}
	}
}
