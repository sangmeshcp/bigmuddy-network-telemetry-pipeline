package pipeline

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

// diskQueue is a durable FIFO of metric batches for one output.
//
// Records are appended to numbered segment files:
//
//	seg-00000000000000000001.log: [len uint32][crc32 uint32][payload]...
//
// A cursor file records the position up to which the output has written
// successfully. Segments wholly before the cursor are deleted. On restart,
// reading resumes from the cursor, so data that was queued but not yet
// written is delivered after the restart (at-least-once).
//
// Writes are flushed to the OS immediately (safe against a process crash)
// and fsynced every syncInterval and on Close (bounds loss on power failure).
// When the queue exceeds maxBytes, the oldest unread segment is dropped and
// its metrics are reported via onDrop.
type diskQueue struct {
	dir          string
	maxBytes     int64
	segmentBytes int64
	onDrop       func(metrics int)

	mu        sync.Mutex
	notify    chan struct{}
	closed    chan struct{}
	closeOnce sync.Once

	segs     []*segment // ascending; last is the write segment
	w        *os.File
	wbuf     *bufio.Writer
	readSeg  int   // index into segs
	readOff  int64 // offset within segs[readSeg]
	rfile    *os.File
	rfileSeg uint64
	total    int64 // bytes in all segments
	pending  int64 // bytes not yet read
	dirty    bool
	stopSync chan struct{}
}

type segment struct {
	id      uint64
	size    int64
	metrics int // metrics in the segment (for drop accounting)
}

// queuePos identifies the end of a record; acking it releases everything before.
type queuePos struct {
	seg uint64
	off int64
}

const (
	recordHeader     = 8
	maxRecordBytes   = 256 << 20
	syncInterval     = time.Second
	cursorFile       = "cursor"
	defaultSegBytes  = 64 << 20
	defaultQueueSize = 1 << 30
)

var errQueueClosed = errors.New("queue closed")

func segName(id uint64) string { return fmt.Sprintf("seg-%020d.log", id) }

func openDiskQueue(dir string, maxBytes, segmentBytes int64, onDrop func(int)) (*diskQueue, error) {
	if maxBytes <= 0 {
		maxBytes = defaultQueueSize
	}
	if segmentBytes <= 0 {
		segmentBytes = min(defaultSegBytes, maxBytes/8+1)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	q := &diskQueue{dir: dir, maxBytes: maxBytes, segmentBytes: segmentBytes, onDrop: onDrop,
		notify: make(chan struct{}, 1), closed: make(chan struct{}), stopSync: make(chan struct{})}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), ".log"), 10, 64)
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		q.segs = append(q.segs, &segment{id: id, size: info.Size()})
	}
	sort.Slice(q.segs, func(i, j int) bool { return q.segs[i].id < q.segs[j].id })

	// Resume from the cursor.
	cur := q.readCursor()
	for len(q.segs) > 0 && q.segs[0].id < cur.seg {
		os.Remove(filepath.Join(dir, segName(q.segs[0].id)))
		q.segs = q.segs[1:]
	}
	if len(q.segs) > 0 && q.segs[0].id == cur.seg {
		q.readOff = min(cur.off, q.segs[0].size)
	}
	for _, s := range q.segs {
		s.metrics = q.countMetrics(s)
		q.total += s.size
	}
	q.pending = q.total - q.readOff

	next := uint64(1)
	if len(q.segs) > 0 {
		next = q.segs[len(q.segs)-1].id + 1
	}
	// Always start a fresh write segment so a torn tail from a crash in the
	// previous segment can never be appended to.
	if err := q.newWriteSegment(next); err != nil {
		return nil, err
	}
	go q.syncLoop()
	return q, nil
}

func (q *diskQueue) readCursor() queuePos {
	b, err := os.ReadFile(filepath.Join(q.dir, cursorFile))
	if err != nil {
		return queuePos{}
	}
	parts := strings.Fields(string(b))
	if len(parts) != 2 {
		return queuePos{}
	}
	seg, err1 := strconv.ParseUint(parts[0], 10, 64)
	off, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return queuePos{}
	}
	return queuePos{seg, off}
}

// countMetrics scans a segment (used on start-up for drop accounting).
func (q *diskQueue) countMetrics(s *segment) int {
	f, err := os.Open(filepath.Join(q.dir, segName(s.id)))
	if err != nil {
		return 0
	}
	defer f.Close()
	r := bufio.NewReader(f)
	n := 0
	for {
		payload, err := readRecord(r)
		if err != nil {
			return n
		}
		if ms, err := telemetry.DecodeBatch(payload); err == nil {
			n += len(ms)
		}
	}
}

func (q *diskQueue) newWriteSegment(id uint64) error {
	if q.w != nil {
		q.wbuf.Flush()
		q.w.Close()
	}
	f, err := os.OpenFile(filepath.Join(q.dir, segName(id)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	q.w, q.wbuf = f, bufio.NewWriterSize(f, 64<<10)
	q.segs = append(q.segs, &segment{id: id})
	return nil
}

// Append durably queues a batch. It never blocks on the reader.
func (q *diskQueue) Append(batch []*telemetry.Metric) error {
	payload := telemetry.EncodeBatch(nil, batch)
	if len(payload) > maxRecordBytes {
		return fmt.Errorf("batch too large for disk queue (%d bytes)", len(payload))
	}
	var hdr [recordHeader]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
	size := int64(recordHeader + len(payload))

	q.mu.Lock()
	defer q.mu.Unlock()
	select {
	case <-q.closed:
		return errQueueClosed
	default:
	}
	ws := q.segs[len(q.segs)-1]
	if ws.size > 0 && ws.size+size > q.segmentBytes {
		if err := q.newWriteSegment(ws.id + 1); err != nil {
			return err
		}
		ws = q.segs[len(q.segs)-1]
	}
	if _, err := q.wbuf.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := q.wbuf.Write(payload); err != nil {
		return err
	}
	if err := q.wbuf.Flush(); err != nil { // hand to the OS: survives a process crash
		return err
	}
	ws.size += size
	ws.metrics += len(batch)
	q.total += size
	q.pending += size
	q.dirty = true

	// Enforce the size bound by dropping the oldest data (never the segment
	// being written).
	for q.total > q.maxBytes && len(q.segs) > 1 {
		old := q.segs[0]
		unread := q.readSeg == 0 // already-read data is in flight and will still be written
		q.closeReader()
		os.Remove(filepath.Join(q.dir, segName(old.id)))
		q.total -= old.size
		if unread {
			q.pending -= old.size - q.readOff
			q.readOff = 0
		} else {
			q.readSeg--
		}
		q.segs = q.segs[1:]
		if unread && q.onDrop != nil && old.metrics > 0 {
			q.onDrop(old.metrics)
		}
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return nil
}

// Read returns the next queued batch and its position, blocking until one is
// available, ctx is done (ctx.Err()) or the queue is closed (errQueueClosed).
func (q *diskQueue) Read(ctx context.Context) ([]*telemetry.Metric, queuePos, error) {
	for {
		q.mu.Lock()
		ms, pos, ok, err := q.tryRead()
		q.mu.Unlock()
		if err != nil {
			return nil, queuePos{}, err
		}
		if ok {
			return ms, pos, nil
		}
		select {
		case <-q.notify:
		case <-ctx.Done():
			return nil, queuePos{}, ctx.Err()
		case <-q.closed:
			return nil, queuePos{}, errQueueClosed
		}
	}
}

// tryRead reads one record without blocking. Corrupt or torn records skip
// to the next segment (counted as dropped).
func (q *diskQueue) tryRead() ([]*telemetry.Metric, queuePos, bool, error) {
	for q.readSeg < len(q.segs) {
		s := q.segs[q.readSeg]
		last := q.readSeg == len(q.segs)-1
		if q.readOff >= s.size {
			if last {
				return nil, queuePos{}, false, nil
			}
			q.readSeg++
			q.readOff = 0
			continue
		}
		if q.rfile == nil || q.rfileSeg != s.id {
			q.closeReader()
			f, err := os.Open(filepath.Join(q.dir, segName(s.id)))
			if err != nil {
				return nil, queuePos{}, false, err
			}
			q.rfile, q.rfileSeg = f, s.id
		}
		var hdr [recordHeader]byte
		if _, err := q.rfile.ReadAt(hdr[:], q.readOff); err != nil {
			q.skipSegment(s, last)
			continue
		}
		n := int64(binary.LittleEndian.Uint32(hdr[0:]))
		if n > maxRecordBytes || q.readOff+recordHeader+n > s.size {
			q.skipSegment(s, last)
			continue
		}
		payload := make([]byte, n)
		if _, err := q.rfile.ReadAt(payload, q.readOff+recordHeader); err != nil ||
			crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(hdr[4:]) {
			q.skipSegment(s, last)
			continue
		}
		ms, err := telemetry.DecodeBatch(payload)
		q.readOff += recordHeader + n
		q.pending -= recordHeader + n
		if err != nil {
			continue
		}
		return ms, queuePos{s.id, q.readOff}, true, nil
	}
	return nil, queuePos{}, false, nil
}

func (q *diskQueue) skipSegment(s *segment, last bool) {
	if last {
		// A torn record at the tail of the write segment can't happen
		// (writes are whole records), so treat it as end of data.
		q.pending -= s.size - q.readOff
		q.readOff = s.size
		return
	}
	q.pending -= s.size - q.readOff
	q.readSeg++
	q.readOff = 0
}

func (q *diskQueue) closeReader() {
	if q.rfile != nil {
		q.rfile.Close()
		q.rfile = nil
	}
}

// Ack records that everything up to pos has been written by the output, and
// deletes fully consumed segments.
func (q *diskQueue) Ack(pos queuePos) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	tmp := filepath.Join(q.dir, cursorFile+".tmp")
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d %d\n", pos.seg, pos.off)), 0o640); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(q.dir, cursorFile)); err != nil {
		return err
	}
	for len(q.segs) > 1 && q.segs[0].id < pos.seg {
		old := q.segs[0]
		if q.rfileSeg == old.id {
			q.closeReader()
		}
		os.Remove(filepath.Join(q.dir, segName(old.id)))
		q.total -= old.size
		q.segs = q.segs[1:]
		q.readSeg--
	}
	return nil
}

// Bytes reports queued-but-unread bytes.
func (q *diskQueue) Bytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending
}

func (q *diskQueue) syncLoop() {
	t := time.NewTicker(syncInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			q.mu.Lock()
			if q.dirty {
				q.w.Sync()
				q.dirty = false
			}
			q.mu.Unlock()
		case <-q.stopSync:
			return
		}
	}
}

// Close syncs and closes the queue. Unread data stays on disk.
func (q *diskQueue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		close(q.closed)
		close(q.stopSync)
		q.mu.Lock()
		defer q.mu.Unlock()
		q.wbuf.Flush()
		err = q.w.Sync()
		q.w.Close()
		q.closeReader()
	})
	return err
}

func readRecord(r io.Reader) ([]byte, error) {
	var hdr [recordHeader]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[0:])
	if n > maxRecordBytes {
		return nil, errors.New("record too large")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(hdr[4:]) {
		return nil, errors.New("checksum mismatch")
	}
	return payload, nil
}
