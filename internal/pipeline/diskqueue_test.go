package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func batchOf(start, n int) []*telemetry.Metric {
	out := make([]*telemetry.Metric, n)
	for i := range out {
		m := telemetry.New("m", time.Unix(int64(start+i), 0))
		m.Fields["i"] = int64(start + i)
		out[i] = m
	}
	return out
}

func readN(t *testing.T, q *diskQueue, n int) ([]int64, queuePos) {
	t.Helper()
	var got []int64
	var pos queuePos
	for len(got) < n {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ms, p, err := q.Read(ctx)
		cancel()
		if err != nil {
			t.Fatalf("read after %d: %v", len(got), err)
		}
		for _, m := range ms {
			got = append(got, m.Fields["i"].(int64))
		}
		pos = p
	}
	return got, pos
}

func TestDiskQueueFIFOAndRestart(t *testing.T) {
	dir := t.TempDir()
	q, err := openDiskQueue(dir, 1<<20, 1024, nil) // small segments to force rotation
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := q.Append(batchOf(i*2, 2)); err != nil {
			t.Fatal(err)
		}
	}
	got, pos := readN(t, q, 40)
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("out of order at %d: %d", i, v)
		}
	}
	if err := q.Ack(pos); err != nil {
		t.Fatal(err)
	}
	// Read 20 more but DON'T ack: they must be redelivered after a restart.
	readN(t, q, 20)
	q.Close()
	segsBefore, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))

	q2, err := openDiskQueue(dir, 1<<20, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	got, _ = readN(t, q2, 60)
	if got[0] != 40 || got[59] != 99 {
		t.Fatalf("restart resumed at %d..%d, want 40..99", got[0], got[59])
	}
	if q2.Bytes() != 0 {
		t.Errorf("pending bytes after reading everything: %d", q2.Bytes())
	}
	if len(segsBefore) == 0 {
		t.Fatal("no segments written")
	}
}

func TestDiskQueueAckDeletesSegments(t *testing.T) {
	dir := t.TempDir()
	q, _ := openDiskQueue(dir, 1<<20, 512, nil)
	defer q.Close()
	for i := 0; i < 120; i++ {
		q.Append(batchOf(i, 1))
	}
	before, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	_, pos := readN(t, q, 120)
	q.Ack(pos)
	after, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	if len(before) < 3 || len(after) != 1 {
		t.Errorf("segments before=%d after=%d (want >=3 then 1)", len(before), len(after))
	}
}

func TestDiskQueueBoundDropsOldest(t *testing.T) {
	var dropped atomic.Int64
	q, _ := openDiskQueue(t.TempDir(), 4096, 1024, func(n int) { dropped.Add(int64(n)) })
	defer q.Close()
	for i := 0; i < 200; i++ {
		if err := q.Append(batchOf(i, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if dropped.Load() == 0 {
		t.Fatal("bound not enforced")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ms, _, err := q.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := ms[0].Fields["i"].(int64)
	if first == 0 || first != dropped.Load() {
		t.Errorf("oldest kept=%d, dropped=%d: should match", first, dropped.Load())
	}
}

func TestDiskQueueSkipsCorruptSegment(t *testing.T) {
	dir := t.TempDir()
	q, _ := openDiskQueue(dir, 1<<20, 256, nil)
	for i := 0; i < 30; i++ {
		q.Append(batchOf(i, 1))
	}
	q.Close()
	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	// Flip a byte in the middle of the first segment.
	b, _ := os.ReadFile(segs[0])
	b[len(b)/2] ^= 0xff
	os.WriteFile(segs[0], b, 0o640)

	q2, _ := openDiskQueue(dir, 1<<20, 256, nil)
	defer q2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var got []int64
	for {
		ms, _, err := q2.Read(ctx)
		if err != nil {
			break
		}
		got = append(got, ms[0].Fields["i"].(int64))
	}
	if len(got) == 0 || got[len(got)-1] != 29 {
		t.Fatalf("corruption should only lose part of one segment; got %v", got)
	}
}

func TestDiskQueueBlockingAndClose(t *testing.T) {
	q, _ := openDiskQueue(t.TempDir(), 1<<20, 0, nil)
	res := make(chan error, 1)
	go func() {
		_, _, err := q.Read(context.Background())
		res <- err
	}()
	time.Sleep(50 * time.Millisecond)
	q.Append(batchOf(0, 1))
	if err := <-res; err != nil {
		t.Fatalf("blocked read not woken by append: %v", err)
	}
	go func() {
		_, _, err := q.Read(context.Background())
		res <- err
	}()
	time.Sleep(50 * time.Millisecond)
	q.Close()
	if err := <-res; !errors.Is(err, errQueueClosed) {
		t.Fatalf("close should unblock readers, got %v", err)
	}
	if err := q.Append(batchOf(0, 1)); !errors.Is(err, errQueueClosed) {
		t.Fatalf("append after close: %v", err)
	}
}
