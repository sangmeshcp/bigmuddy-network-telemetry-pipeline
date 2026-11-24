VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
IMAGE   ?= telemetry-pipeline

.PHONY: all build build-slim test race cover lint generate image check-config integration smoke helm-lint dashboards chart-assets alerts-test dashboard-check clean

all: lint test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/pipeline ./cmd/pipeline

# Without the Iceberg output (Apache Arrow + AWS SDK): ~32 MiB instead of ~76 MiB.
build-slim:
	CGO_ENABLED=0 go build -trimpath -tags noiceberg -ldflags "$(LDFLAGS)" -o bin/pipeline ./cmd/pipeline

test:
	go test ./...

race:
	go test -race -count=1 ./...

cover:
	go test -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

lint:
	gofmt -l . | (! grep .) || (echo "run gofmt -w ."; exit 1)
	go vet -tags integration ./...
	go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 -tags integration ./...

# Integration tests against real backends/devices. Each test skips unless its
# env vars are set (INFLUX_*, KAFKA_BROKERS, PROMETHEUS_URL, GNMI_*); see
# .github/workflows/ci.yml for how CI provides them.
integration:
	go test -tags integration -count=1 -v ./integration/...

# Smoke-test the image: build, run, replay captured telemetry, check output.
smoke: image
	docker rm -f pipeline-smoke 2>/dev/null || true
	docker run -d --name pipeline-smoke -p 9180:9180 -p 9273:9273 -p 57500:57500 $(IMAGE):$(VERSION)
	scripts/smoke.sh; rc=$$?; docker rm -f pipeline-smoke >/dev/null; exit $$rc

# Lint the Helm chart and validate rendered manifests (needs helm, kubeconform).
helm-lint:
	helm lint deploy/helm/telemetry-pipeline --strict
	helm template t deploy/helm/telemetry-pipeline | kubeconform -strict -summary
	helm template t deploy/helm/telemetry-pipeline -f deploy/helm/telemetry-pipeline/ci/all-features-values.yaml \
		| kubeconform -strict -summary -skip ServiceMonitor,PrometheusRule

# Regenerate Grafana dashboards (deploy/grafana/generate.py) and the chart's
# copies of the dashboards and alert rules.
dashboards chart-assets:
	python3 deploy/grafana/generate.py
	cp deploy/prometheus/alerts.yaml deploy/helm/telemetry-pipeline/files/alerts.yaml

# Validate and unit-test the alert rules (needs promtool).
alerts-test:
	promtool check rules deploy/prometheus/alerts.yaml
	promtool test rules deploy/prometheus/alerts_test.yaml

# Run every dashboard query against a live pipeline + Prometheus.
dashboard-check:
	scripts/dashboard-check.sh

# Regenerate Go code from proto/ (requires buf, protoc-gen-go, protoc-gen-go-grpc).
generate:
	buf generate

image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

check-config: build
	GNMI_PASSWORD=x INFLUX_TOKEN=x ./bin/pipeline -config examples/pipeline.yaml -check

clean:
	rm -rf bin coverage.out
