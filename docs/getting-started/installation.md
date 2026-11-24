# Installation

The pipeline is a single static binary with no runtime dependencies. Pick
whichever packaging suits your environment.

## Download a release (recommended)

Every [release](https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/releases)
publishes archives for Linux and macOS on amd64 and arm64. Each archive
contains `pipeline`, the `mdt-replay` test tool, the license and example
configs.

```bash
VERSION=0.1.0                                    # pick from the releases page
OS=linux ARCH=amd64                              # or darwin / arm64
BASE=https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/releases/download/v$VERSION
curl -fLO "$BASE/telemetry-pipeline_${VERSION}_${OS}_${ARCH}.tar.gz"
curl -fLO "$BASE/telemetry-pipeline_${VERSION}_checksums.txt"
sha256sum --ignore-missing -c telemetry-pipeline_${VERSION}_checksums.txt
tar xzf telemetry-pipeline_${VERSION}_${OS}_${ARCH}.tar.gz
sudo install pipeline /usr/local/bin/
pipeline -version
```

??? tip "Verify the signature (supply-chain security)"
    Releases are signed keylessly with [Sigstore cosign](https://docs.sigstore.dev/)
    from the release workflow. Each release also ships an SPDX SBOM per archive.

    ```bash
    curl -fLO "$BASE/telemetry-pipeline_${VERSION}_checksums.txt.pem"
    curl -fLO "$BASE/telemetry-pipeline_${VERSION}_checksums.txt.sig"
    cosign verify-blob \
      --certificate telemetry-pipeline_${VERSION}_checksums.txt.pem \
      --signature   telemetry-pipeline_${VERSION}_checksums.txt.sig \
      --certificate-identity-regexp 'https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/' \
      --certificate-oidc-issuer https://token.actions.githubusercontent.com \
      telemetry-pipeline_${VERSION}_checksums.txt
    ```

    A valid signature over the checksum file, plus a passing `sha256sum -c`,
    proves the archive was built by this repository's release workflow.

## From source

Requires Go 1.26.11 or newer.

```bash
git clone https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline.git
cd bigmuddy-network-telemetry-pipeline
make build                    # ./bin/pipeline (CGO disabled, fully static)
sudo install bin/pipeline /usr/local/bin/
```

Cross-compiling works the usual Go way, for example
`GOOS=linux GOARCH=arm64 make build`.

### Running as a systemd service

```ini title="/etc/systemd/system/pipeline.service"
[Unit]
Description=Network telemetry pipeline
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/pipeline -config /etc/pipeline/pipeline.yaml
# Secrets referenced as ${GNMI_PASSWORD} etc. in the config:
EnvironmentFile=-/etc/pipeline/secrets.env
DynamicUser=yes
Restart=on-failure
# The pipeline drains queues for up to 30s on SIGTERM
TimeoutStopSec=45
AmbientCapabilities=
NoNewPrivileges=yes

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now pipeline
journalctl -u pipeline -f
```

!!! tip "Ports below 1024"
    The defaults (57500, 9180, 9273) are unprivileged. If you must listen on a
    port below 1024, add `AmbientCapabilities=CAP_NET_BIND_SERVICE` rather than
    running as root.

## Container image

Published images are multi-arch (amd64, arm64): a static binary on
`gcr.io/distroless/static`, running as a non-root user, about 20 MB. Each one
is signed with cosign and carries SBOM and build-provenance attestations.

```bash
docker run --rm \
  -v "$PWD/pipeline.yaml:/etc/pipeline/pipeline.yaml:ro" \
  -e GNMI_PASSWORD \
  -p 9180:9180 -p 9273:9273 -p 57500:57500 \
  ghcr.io/sangmeshcp/telemetry-pipeline:v0.1.0
```

| Tag | Meaning |
|---|---|
| `vX.Y.Z` | An exact release. Pin this in production. |
| `vX.Y` | Latest patch of a minor release |
| `latest` | Latest stable release (pre-releases excluded) |

Verify an image before deploying:

```bash
cosign verify ghcr.io/sangmeshcp/telemetry-pipeline:v0.1.0 \
  --certificate-identity-regexp 'https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

To build your own image instead: `make image`, or
`docker buildx build --platform linux/amd64,linux/arm64 .` for both
architectures.

Without a mounted config the image runs `examples/minimal.yaml`, which accepts
MDT gRPC dial-out on `:57500` and serves Prometheus metrics on `:9273`.

The image has no shell. Inspect it through its endpoints: `/healthz`,
`/readyz` and `/metrics` on the admin port.

## Docker Compose lab

`deploy/compose/` starts the pipeline, Prometheus 3 and Grafana together, wired
up and ready for devices:

```bash
cd deploy/compose
docker compose up --build
```

| Service | URL | Notes |
|---|---|---|
| Pipeline admin | <http://localhost:9180/metrics> | self-monitoring |
| Pipeline exporter | <http://localhost:9273/metrics> | device telemetry |
| Prometheus | <http://localhost:9090> | scrapes both, OTLP receiver enabled |
| Grafana | <http://localhost:3000> | Prometheus datasource pre-provisioned |

Point IOS XR dial-out at `<your-host>:57500` (gRPC) or `:5432` (TCP), or add
gNMI targets to `deploy/compose/pipeline.yaml`. To try it without a router:

```bash
go run ./cmd/mdt-replay -addr localhost:57500 internal/input/ciscomdt/testdata/*.kvgpb
```

## Kubernetes

`deploy/kubernetes/pipeline.yaml` contains a ConfigMap, a Deployment with
liveness and readiness probes and a hardened security context, a ClusterIP
Service for scraping, and a LoadBalancer Service for device dial-out.

```bash
kubectl create secret generic telemetry-pipeline --from-literal=gnmi-password='...'
kubectl apply -f deploy/kubernetes/pipeline.yaml
kubectl port-forward deploy/telemetry-pipeline 9180 &
curl localhost:9180/readyz
```

See [Deployment](../operations/deployment.md) for scaling, sizing and
security guidance.

## Verify any installation

```bash
pipeline -version
pipeline -config /etc/pipeline/pipeline.yaml -check   # exits non-zero on any config error
curl -fsS http://<host>:9180/readyz                    # 200 "ready" once running
```
