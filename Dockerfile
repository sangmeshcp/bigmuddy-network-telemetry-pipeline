# syntax=docker/dockerfile:1
#
# Build:  docker build -t telemetry-pipeline .
#         docker buildx build --platform linux/amd64,linux/arm64 -t telemetry-pipeline .
# Run:    docker run --rm -v $PWD/pipeline.yaml:/etc/pipeline/pipeline.yaml \
#           -p 9180:9180 -p 9273:9273 -p 57500:57500 telemetry-pipeline

# The build stage always runs on the build machine's architecture and
# cross-compiles, so multi-arch builds don't need CPU emulation.
FROM --platform=$BUILDPLATFORM golang:1.27.4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/pipeline ./cmd/pipeline

# Static, shell-less, non-root runtime image (~20 MB).
FROM gcr.io/distroless/static-debian12:nonroot
ARG VERSION=dev
LABEL org.opencontainers.image.title="telemetry-pipeline" \
      org.opencontainers.image.description="Network streaming telemetry collector: gNMI and Cisco MDT to Prometheus, OTLP, InfluxDB and Kafka" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=build /out/pipeline /usr/local/bin/pipeline
COPY examples/minimal.yaml /etc/pipeline/pipeline.yaml
# Numeric UID/GID (distroless "nonroot") so Kubernetes can enforce runAsNonRoot.
USER 65532:65532
# 9180 admin/self-metrics, 9273 Prometheus exporter, 57500 MDT gRPC dial-out
EXPOSE 9180 9273 57500
ENTRYPOINT ["/usr/local/bin/pipeline"]
CMD ["-config", "/etc/pipeline/pipeline.yaml"]
