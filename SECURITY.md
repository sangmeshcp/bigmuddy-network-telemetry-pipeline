# Security policy

The pipeline parses data from the network (gNMI streams, Cisco MDT
frames and payloads) and often runs with credentials for network devices,
so we take security reports seriously.

## Supported versions

| Version | Supported |
|---|---|
| Latest release (`vX.Y.Z`) | ✅ security fixes |
| `master` | ✅ |
| Older releases | ❌ upgrade to the latest release |

## Reporting a vulnerability

**Don't open a public issue.** Report privately through GitHub:
**[Security → Report a vulnerability](https://github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/security/advisories/new)**.

Please include:

- the affected version (`pipeline -version`) and component (e.g. `cisco_mdt` TCP input);
- steps or input to reproduce (a crashing payload is ideal);
- the impact you believe it has (crash/DoS, credential exposure, …).

## What to expect

| Step | Target |
|---|---|
| Acknowledgement | within 3 business days |
| Initial assessment | within 7 days |
| Fix and release, for confirmed issues | depends on severity; critical issues are prioritised over all other work |

We coordinate disclosure with you: a GitHub Security Advisory (with a CVE
where appropriate) is published when the fix is released, crediting you
unless you prefer otherwise.

## Scope

In scope: anything in this repository, including malformed input that
crashes or hangs the pipeline (it is network-facing), credential handling,
TLS configuration, and the release artifacts and container image.

Out of scope: vulnerabilities in network devices themselves, and issues
that need an attacker to already control the pipeline's configuration file.

## Verifying what you run

Releases and images are signed with Sigstore cosign and ship SBOMs; see
[Installation → verify the signature](https://sangameshpatil.com/bigmuddy-network-telemetry-pipeline/getting-started/installation/).
