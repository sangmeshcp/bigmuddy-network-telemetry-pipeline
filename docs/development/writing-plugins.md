# Writing a plugin

Inputs, processors and outputs are plugins: a Go type implementing one
interface, plus a factory registered under a type name. This page walks
through adding a complete **output** that sends metrics to a StatsD server
over UDP. Inputs and processors follow the same pattern.

## The interfaces

From `internal/pipeline/plugin.go`:

```go
type Input interface {
    // Run blocks until ctx is cancelled. Handle transient failures yourself;
    // return an error only for unrecoverable problems (e.g. listen failed).
    Run(ctx context.Context, sink Sink) error
}

type Processor interface {
    // May modify, drop or add metrics.
    Process(batch []*telemetry.Metric) []*telemetry.Metric
}

type Output interface {
    // Called from one goroutine. Never modify the metrics.
    // Return pipeline.Permanent(err) for errors retrying cannot fix.
    Write(ctx context.Context, batch []*telemetry.Metric) error
    Close(ctx context.Context) error
}

// Optional, for outputs that run a server:
type Starter interface { Start(ctx context.Context) error }
```

Each factory receives the plugin's config section and an `Env` with a
`Logger` and a Prometheus `Registry`, both already labelled with the plugin's
name.

## Step 1: package and config

```go title="internal/output/statsd/statsd.go"
// Package statsd sends numeric fields as StatsD gauges over UDP.
package statsd

import (
    "context"
    "errors"
    "fmt"
    "net"
    "strings"

    "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
    "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
    "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func init() { pipeline.RegisterOutput("statsd", New) }

// Config is the YAML for this output. Unknown keys are rejected automatically.
type Config struct {
    Address string `yaml:"address"` // host:port
    Prefix  string `yaml:"prefix"`
}

type Output struct {
    cfg  Config
    conn net.Conn
}
```

## Step 2: factory (validate early)

```go
func New(p *config.Plugin, env pipeline.Env) (pipeline.Output, error) {
    c := Config{Prefix: "net"}            // defaults first, then decode over them
    if err := p.Decode(&c); err != nil {  // strict: typos are errors
        return nil, err
    }
    if c.Address == "" {
        return nil, errors.New("address is required")
    }
    conn, err := net.Dial("udp", c.Address)
    if err != nil {
        return nil, err
    }
    env.Logger.Info("sending StatsD", "address", c.Address)
    return &Output{cfg: c, conn: conn}, nil
}
```

Anything wrong in the config should fail here, so `pipeline -check` catches
it before deployment.

## Step 3: Write and Close

```go
func (o *Output) Write(ctx context.Context, batch []*telemetry.Metric) error {
    var b strings.Builder
    for _, m := range batch {
        for field, v := range m.Fields {
            f, ok := telemetry.ToFloat(v)   // skips strings
            if !ok {
                continue
            }
            name := telemetry.SanitizeName(o.cfg.Prefix + "/" + m.Name + "/" + field)
            fmt.Fprintf(&b, "%s.%s:%g|g\n", name, telemetry.SanitizeName(m.Source()), f)
        }
    }
    if _, err := o.conn.Write([]byte(b.String())); err != nil {
        return err // transient: the worker retries with backoff
    }
    return nil
}

func (o *Output) Close(context.Context) error { return o.conn.Close() }
```

You get batching, retries, queueing, drop accounting and self-metrics for
free from the worker in `internal/pipeline/engine.go`.

## Step 4: register in the binary

```go title="cmd/pipeline/main.go"
import (
    _ "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/output/statsd"
)
```

## Step 5: test it

```go title="internal/output/statsd/statsd_test.go"
func TestWrite(t *testing.T) {
    pc, _ := net.ListenPacket("udp", "127.0.0.1:0")
    defer pc.Close()
    cfg, _ := config.Parse([]byte(`inputs: [{type: x}]
outputs: [{type: statsd, address: "` + pc.LocalAddr().String() + `"}]`))
    o, err := New(&cfg.Outputs[0], pipeline.Env{Logger: slog.Default(), Registry: prometheus.NewRegistry()})
    if err != nil { t.Fatal(err) }

    m := telemetry.New("/system/cpu", time.Now())
    m.Tags["source"] = "r1"
    m.Fields["load"] = 0.5
    if err := o.Write(context.Background(), []*telemetry.Metric{m}); err != nil { t.Fatal(err) }

    buf := make([]byte, 512)
    pc.SetReadDeadline(time.Now().Add(2 * time.Second))
    n, _, _ := pc.ReadFrom(buf)
    if got := string(buf[:n]); got != "net_system_cpu_load.r1:0.5|g\n" {
        t.Fatalf("got %q", got)
    }
}
```

Also test that bad config is rejected.

## Step 6: document it

- Add a section to `docs/configuration/outputs.md` with a table of every key
  and its default.
- Add a commented example to `examples/pipeline.yaml`.
- If it has a real backend, add an env-gated test in `integration/` and a CI
  service in `.github/workflows/ci.yml`.

## Checklist

- [ ] Config struct with `yaml` tags; defaults set before `Decode`
- [ ] Validation in the factory; secrets via `config.Secret(inline, file)`
- [ ] TLS via `tlsutil.Config` if it talks over the network
- [ ] Permanent vs transient errors classified (`pipeline.Permanent`)
- [ ] Metrics never modified in outputs
- [ ] Plugin-specific self-metrics registered on `env.Registry`
- [ ] Unit tests including config errors; `make race` passes
- [ ] Docs and example config updated
