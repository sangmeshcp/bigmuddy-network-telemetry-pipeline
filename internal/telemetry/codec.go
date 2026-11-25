package telemetry

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Binary codec for batches of metrics, used by the disk-backed output queue.
// It is compact, preserves every field type exactly (int64 vs uint64 vs
// float64), and carries kinds. Layout (all integers varint-encoded):
//
//	batch  := version(1 byte) count metric*
//	metric := name ts_unix_nano ntags (key val)* nfields (key type value)* nkinds (key kind)*
//
// It is an internal format: versioned so queues written by an older release
// can still be read, but not meant for exchange with other systems.

const codecVersion = 1

const (
	tInt64 byte = iota + 1
	tUint64
	tFloat64
	tBoolFalse
	tBoolTrue
	tString
)

var errCodec = errors.New("telemetry codec: malformed data")

// EncodeBatch appends the encoding of batch to dst.
func EncodeBatch(dst []byte, batch []*Metric) []byte {
	dst = append(dst, codecVersion)
	dst = binary.AppendUvarint(dst, uint64(len(batch)))
	for _, m := range batch {
		dst = appendString(dst, m.Name)
		dst = binary.AppendVarint(dst, m.Timestamp.UnixNano())
		dst = binary.AppendUvarint(dst, uint64(len(m.Tags)))
		for k, v := range m.Tags {
			dst = appendString(appendString(dst, k), v)
		}
		n := 0
		for _, v := range m.Fields {
			if fieldType(v) != 0 {
				n++
			}
		}
		dst = binary.AppendUvarint(dst, uint64(n))
		for k, v := range m.Fields {
			t := fieldType(v)
			if t == 0 {
				continue
			}
			dst = appendString(dst, k)
			dst = append(dst, t)
			switch x := v.(type) {
			case int64:
				dst = binary.AppendVarint(dst, x)
			case uint64:
				dst = binary.AppendUvarint(dst, x)
			case float64:
				dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(x))
			case string:
				dst = appendString(dst, x)
			}
		}
		dst = binary.AppendUvarint(dst, uint64(len(m.Kinds)))
		for k, kind := range m.Kinds {
			dst = append(appendString(dst, k), byte(kind))
		}
	}
	return dst
}

func fieldType(v any) byte {
	switch x := v.(type) {
	case int64:
		return tInt64
	case uint64:
		return tUint64
	case float64:
		return tFloat64
	case bool:
		if x {
			return tBoolTrue
		}
		return tBoolFalse
	case string:
		return tString
	}
	return 0
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// DecodeBatch decodes the output of EncodeBatch.
func DecodeBatch(b []byte) ([]*Metric, error) {
	d := decoder{b: b}
	if v := d.byte(); v != codecVersion {
		return nil, fmt.Errorf("telemetry codec: unsupported version %d", v)
	}
	n := d.uvarint()
	if d.err != nil || n > uint64(len(b)) {
		return nil, errCodec
	}
	out := make([]*Metric, 0, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		m := &Metric{Name: d.string()}
		m.Timestamp = time.Unix(0, d.varint())
		nt := d.count()
		m.Tags = make(map[string]string, nt)
		for j := 0; j < nt && d.err == nil; j++ {
			k := d.string()
			m.Tags[k] = d.string()
		}
		nf := d.count()
		m.Fields = make(map[string]any, nf)
		for j := 0; j < nf && d.err == nil; j++ {
			k := d.string()
			switch d.byte() {
			case tInt64:
				m.Fields[k] = d.varint()
			case tUint64:
				m.Fields[k] = d.uvarint()
			case tFloat64:
				m.Fields[k] = d.float64()
			case tBoolFalse:
				m.Fields[k] = false
			case tBoolTrue:
				m.Fields[k] = true
			case tString:
				m.Fields[k] = d.string()
			default:
				d.err = errCodec
			}
		}
		if nk := d.count(); nk > 0 {
			m.Kinds = make(map[string]Kind, nk)
			for j := 0; j < nk && d.err == nil; j++ {
				k := d.string()
				m.Kinds[k] = Kind(d.byte())
			}
		}
		out = append(out, m)
	}
	if d.err != nil {
		return nil, d.err
	}
	if d.off != len(b) {
		return nil, errCodec
	}
	return out, nil
}

type decoder struct {
	b   []byte
	off int
	err error
}

func (d *decoder) byte() byte {
	if d.err != nil || d.off >= len(d.b) {
		d.err = errCodec
		return 0
	}
	c := d.b[d.off]
	d.off++
	return c
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b[d.off:])
	if n <= 0 {
		d.err = errCodec
		return 0
	}
	d.off += n
	return v
}

func (d *decoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b[d.off:])
	if n <= 0 {
		d.err = errCodec
		return 0
	}
	d.off += n
	return v
}

// count reads a length that must fit in the remaining input.
func (d *decoder) count() int {
	n := d.uvarint()
	if n > uint64(len(d.b)-d.off) {
		d.err = errCodec
		return 0
	}
	return int(n)
}

func (d *decoder) string() string {
	n := d.count()
	if d.err != nil {
		return ""
	}
	s := string(d.b[d.off : d.off+n])
	d.off += n
	return s
}

func (d *decoder) float64() float64 {
	if d.err != nil || d.off+8 > len(d.b) {
		d.err = errCodec
		return 0
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(d.b[d.off:]))
	d.off += 8
	return v
}
