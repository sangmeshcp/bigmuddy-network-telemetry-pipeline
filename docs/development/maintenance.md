# Protobufs, releases & docs

## Protobufs

Only the Cisco MDT messages are compiled from source; gNMI and OTLP come
from their upstream Go modules.

| Source | Generated | Wire identity (don't change) |
|---|---|---|
| `proto/telemetry/telemetry.proto` | `internal/gen/cisco/telemetry` | package `telemetry` |
| `proto/mdt_dialout/mdt_dialout.proto` | `internal/gen/cisco/mdtdialout` | `/mdt_dialout.gRPCMdtDialout/MdtDialout` (the method routers call) |

!!! danger "Proto package names are part of the wire protocol"
    Renaming a proto `package` or `service` changes the gRPC method path, and
    routers will no longer be able to connect. The directories are laid out
    to satisfy `buf lint` without renaming anything.

To regenerate, install the exact generator versions CI uses (so the drift
check passes):

```bash
go install github.com/bufbuild/buf/cmd/buf@v1.47.2
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
make generate
git diff internal/gen     # commit the result together with the .proto change
```

## Dependencies

```bash
go get -u ./... && go mod tidy && make lint race   # update everything
go get github.com/twmb/franz-go@latest             # or just one module
```

- The `go` directive in `go.mod` is the minimum toolchain **and** what CI
  installs. Bump it to the latest patch release whenever `govulncheck`
  reports standard-library vulnerabilities, and keep the `golang:` tag in the
  `Dockerfile` in step.
- `kfake` (test-only, from franz-go) has no tagged releases, so `go get @latest`
  picks an untested HEAD commit. Pin it to the commit of a franz-go *release*
  instead:
  `go get github.com/twmb/franz-go/pkg/kfake@$(go list -m -json github.com/twmb/franz-go@vX.Y.Z | jq -r .Origin.Hash)`.

## Releases

Releases are fully automated by `.github/workflows/release.yml` and
`.goreleaser.yaml`. Pushing a `v*` tag:

1. **verify**: vet, race tests and `govulncheck` on the tagged commit.
2. **binaries**: GoReleaser builds linux/darwin × amd64/arm64 archives
   (`pipeline` + `mdt-replay`), checksums, an SPDX SBOM per archive, a
   keyless cosign signature over the checksums, and a GitHub Release with a
   generated changelog and verification instructions.
3. **image**: a multi-arch image is pushed to
   `ghcr.io/<owner>/telemetry-pipeline` with tags `vX.Y.Z`, `vX.Y` and
   `latest` (stable releases only), SBOM and provenance attestations, then
   signed with cosign and the signature verified.

### Cutting a release

```bash
git switch master && git pull
# Update CHANGELOG.md: move "Unreleased" items under the new version
git tag -a v0.2.0 -m "v0.2.0"
git push origin v0.2.0
```

Tags with a suffix (`v0.2.0-rc.1`) are published as **pre-releases** and don't
move `latest`.

### Rehearsal

Every pull request runs the `release-dry-run` CI job: a GoReleaser snapshot
(with SBOMs) and a multi-arch image build without pushing. A broken release
config therefore fails the PR, not the tag. To rehearse locally:

```bash
go install github.com/goreleaser/goreleaser/v2@latest
IMAGE_OWNER=me GITHUB_REPOSITORY=me/repo goreleaser release --snapshot --clean --skip=sign,sbom
ls dist/
```

### Versioning

SemVer, applied to the **configuration file and exported metric names**.
Renaming or removing a config key, or changing how metrics are named, is a
breaking change and needs a major version bump (or a minor bump before 1.0,
called out in the changelog).

### One-time setup

- The GHCR package is created on the first release. Make it public under
  **Packages → telemetry-pipeline → Package settings → Change visibility**
  so users can pull without logging in.
- No secrets are needed: signing uses GitHub's OIDC token (keyless), and
  publishing uses the workflow's `GITHUB_TOKEN`.

## Documentation

This site is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
from `docs/` and `mkdocs.yml`.

### Preview locally

```bash
python3 -m venv .venv
.venv/bin/pip install -r docs/requirements.txt
.venv/bin/mkdocs serve          # http://127.0.0.1:8000, live reload
.venv/bin/mkdocs build --strict # what CI runs: broken links fail
```

### Publishing (GitHub Pages)

`.github/workflows/docs.yml`:

- **Pull requests** that touch docs: `mkdocs build --strict`, so broken links
  or nav entries fail the PR.
- **Push to `master`**: builds and deploys to GitHub Pages.

**One-time repository setup** (needs admin): **Settings → Pages → Build and
deployment → Source: GitHub Actions**. Don't use the suggested Jekyll or
Static HTML workflows; `docs.yml` is the publishing workflow.

The site is live at <https://sangameshpatil.com/bigmuddy-network-telemetry-pipeline/>.
The owner's account has a custom Pages domain, so the `github.io` address
redirects there. `site_url` in `mkdocs.yml` must match the live address,
because canonical links and the sitemap are built from it.

To publish without a docs change, run **Actions → docs → Run workflow** on
`master`.

### Writing docs

- One topic per page; add new pages to `nav:` in `mkdocs.yml`.
- Every config key goes in a table with its default.
- Prefer runnable examples, and test them.
- Diagrams: fenced ` ```mermaid ` blocks render on both GitHub and this site.
