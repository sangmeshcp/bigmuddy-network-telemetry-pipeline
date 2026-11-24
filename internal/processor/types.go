package processor

import (
	"fmt"
	"regexp"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func init() { pipeline.RegisterProcessor("metric_types", newMetricTypes) }

// presets are field-name regexes for well-known counter leaves. Telemetry
// doesn't say which leaves are counters, so these encode the conventions of
// OpenConfig (".../counters/...") and Cisco IOS XR native models.
var presets = map[string][]string{
	"network": {
		`(^|/)counters/`, // OpenConfig: state/counters/in-octets ...
		`(^|/|-)(octets|pkts|packets|bytes)(-|$)`, // *-octets, packets-*, bytes-received ...
		`(^|/|-)(errors|discards|drops)$`,         // in-errors, input-drops, crc-errors ...
		`carrier-transitions$`,
		`(^|/)(prefixes-)?(received|sent)-(updates|messages|notifications)$`,
	},
}

type metricTypesConfig struct {
	Presets  []string `yaml:"presets"`  // e.g. [network]
	Counters []string `yaml:"counters"` // field regexes marked as counters
	Gauges   []string `yaml:"gauges"`   // field regexes marked as gauges; wins over counters
	// Names optionally restricts the processor to metrics whose name matches.
	Names []string `yaml:"names"`
}

type metricTypes struct {
	counters, gauges, names []*regexp.Regexp
}

func newMetricTypes(p *config.Plugin, _ pipeline.Env) (pipeline.Processor, error) {
	var c metricTypesConfig
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	exprs := append([]string{}, c.Counters...)
	for _, name := range c.Presets {
		ps, ok := presets[name]
		if !ok {
			return nil, fmt.Errorf("unknown preset %q (available: network)", name)
		}
		exprs = append(exprs, ps...)
	}
	mt := &metricTypes{}
	var err error
	if mt.counters, err = compileAll(exprs); err != nil {
		return nil, err
	}
	if mt.gauges, err = compileAll(c.Gauges); err != nil {
		return nil, err
	}
	if mt.names, err = compileAll(c.Names); err != nil {
		return nil, err
	}
	if len(mt.counters) == 0 && len(mt.gauges) == 0 {
		return nil, fmt.Errorf("set presets, counters or gauges")
	}
	return mt, nil
}

func (mt *metricTypes) Process(batch []*telemetry.Metric) []*telemetry.Metric {
	for _, m := range batch {
		if len(mt.names) > 0 && !anyMatch(mt.names, m.Name) {
			continue
		}
		for field, v := range m.Fields {
			if _, numeric := telemetry.ToFloat(v); !numeric {
				continue
			}
			if _, isBool := v.(bool); isBool {
				continue
			}
			switch {
			case anyMatch(mt.gauges, field):
				m.SetKind(field, telemetry.KindGauge)
			case anyMatch(mt.counters, field):
				m.SetKind(field, telemetry.KindCounter)
			}
		}
	}
	return batch
}
