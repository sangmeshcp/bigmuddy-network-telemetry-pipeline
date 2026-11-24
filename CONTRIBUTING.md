# Contributing

Thanks for helping! The full guide, including environment setup, a tour of
the code, conventions and how CI works, is in the docs:

- **[Developer onboarding](docs/development/onboarding.md)**: start here
- [Testing & CI](docs/development/testing.md)
- [Writing a plugin](docs/development/writing-plugins.md)
- [Protobufs, releases & docs](docs/development/maintenance.md)

Published version: <https://sangameshpatil.com/bigmuddy-network-telemetry-pipeline/>

## In short

```bash
make build lint test        # must pass before you open a PR
make race                   # if you touched concurrency
```

1. Branch from `master`; one logical change per PR; sign off your commits (`git commit -s`).
2. Include tests. Bug fixes start with a failing test.
3. Update docs (`docs/configuration/*.md`, `examples/pipeline.yaml`) in the
   same PR as any config or behaviour change.
4. Open a PR against `master` and get CI green.

## Developer Certificate of Origin (DCO)

Contributions are accepted under the [Apache 2.0 license](LICENSE). Instead
of a Contributor License Agreement, we use the
[Developer Certificate of Origin](https://developercertificate.org/): by
signing off a commit, you certify that you wrote it or otherwise have the
right to submit it under the project's license.

Sign off every commit with `-s`:

```bash
git commit -s -m "Add statsd output"
# adds:  Signed-off-by: Your Name <you@example.com>   (must match the commit author)
```

Forgot? Fix the whole branch and update your PR:

```bash
git rebase --signoff origin/master
git push --force-with-lease
```

A CI check (`dco`) verifies this on every pull request. Maintainers'
and bots' PRs are exempt, as with the DCO GitHub App's default.

## Code of conduct and security

Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).
Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
See [GOVERNANCE.md](GOVERNANCE.md) for how decisions are made, and record
user-visible changes in [CHANGELOG.md](CHANGELOG.md) under **Unreleased**.

## Reporting bugs and proposing features

Use the issue forms (bug report, feature request, device-compatibility
report). They ask for what we need: pipeline version, config with secrets
removed, device platform and version, and debug logs.
