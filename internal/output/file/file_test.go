package file

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func open(t *testing.T, path string) pipeline.Output {
	t.Helper()
	cfg, err := config.Parse([]byte("inputs: [{type: x}]\noutputs:\n  - {type: file, path: \"" + path + "\"}"))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(&cfg.Outputs[0], pipeline.Env{})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestWritesJSONLinesAndAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	m := telemetry.New("/x", time.Unix(1700000000, 0).UTC())
	m.Tags["source"] = "r1"
	m.Fields["v"] = int64(5)
	m.Fields["s"] = "up"

	for i := 0; i < 2; i++ { // second open must append, not truncate
		o := open(t, path)
		if err := o.Write(context.Background(), []*telemetry.Metric{m, m}); err != nil {
			t.Fatal(err)
		}
		if err := o.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	f, _ := os.Open(path)
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var rec struct {
			Name      string            `json:"name"`
			Tags      map[string]string `json:"tags"`
			Fields    map[string]any    `json:"fields"`
			Timestamp time.Time         `json:"timestamp"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Name != "/x" || rec.Tags["source"] != "r1" || rec.Fields["v"] != 5.0 || !rec.Timestamp.Equal(m.Timestamp) {
			t.Errorf("record %+v", rec)
		}
		n++
	}
	if n != 4 {
		t.Errorf("got %d lines, want 4", n)
	}
}

func TestStdoutAndErrors(t *testing.T) {
	o := open(t, "-")
	if err := o.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Parse([]byte("inputs: [{type: x}]\noutputs: [{type: file, path: /nonexistent-dir/x}]"))
	if _, err := New(&cfg.Outputs[0], pipeline.Env{}); err == nil {
		t.Error("expected error opening file in missing directory")
	}
	cfg, _ = config.Parse([]byte("inputs: [{type: x}]\noutputs: [{type: file, bogus: 1}]"))
	if _, err := New(&cfg.Outputs[0], pipeline.Env{}); err == nil {
		t.Error("expected unknown-field error")
	}
}
