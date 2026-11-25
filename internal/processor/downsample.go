package processor

import (
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func init() { pipeline.RegisterProcessor("downsample", newDownsample) }

type downsampleConfig struct {
	Interval time.Duration `yaml:"interval"` // at most one sample per series and field per interval
	Names    []string      `yaml:"names"`    // optional: only metrics whose name matches
}

// downsample keeps the first sample of each series+field in every
// interval-aligned window (by device timestamp, so replicas agree) and drops
// the rest. Counters stay correct for rate() because they are cumulative.
type downsample struct {
	interval int64
	names    []*regexp.Regexp

	mu      sync.Mutex
	last    map[string]int64 // series\x00field -> last emitted window
	calls   int
	highWin int64
}

func newDownsample(p *config.Plugin, _ pipeline.Env) (pipeline.Processor, error) {
	var c downsampleConfig
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	if c.Interval <= 0 {
		return nil, errors.New("interval is required, e.g. 1m")
	}
	names, err := compileAll(c.Names)
	if err != nil {
		return nil, err
	}
	return &downsample{interval: int64(c.Interval), names: names, last: map[string]int64{}}, nil
}

func (d *downsample) Process(batch []*telemetry.Metric) []*telemetry.Metric {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := batch[:0]
	for _, m := range batch {
		if len(d.names) > 0 && !anyMatch(d.names, m.Name) {
			out = append(out, m)
			continue
		}
		win := m.Timestamp.UnixNano() / d.interval
		if win > d.highWin {
			d.highWin = win
		}
		series := m.SeriesKey() + "\x00"
		for f := range m.Fields {
			key := series + f
			if prev, ok := d.last[key]; ok && win <= prev {
				delete(m.Fields, f)
				delete(m.Kinds, f)
				continue
			}
			d.last[key] = win
		}
		if len(m.Fields) > 0 {
			out = append(out, m)
		}
	}
	// Forget series that stopped reporting, so memory stays bounded.
	if d.calls++; d.calls%1024 == 0 {
		for k, w := range d.last {
			if d.highWin-w > 10 {
				delete(d.last, k)
			}
		}
	}
	return out
}
