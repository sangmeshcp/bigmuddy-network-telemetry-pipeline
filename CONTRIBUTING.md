# Contributing

Thanks for helping! The full guide, including environment setup, a tour of
the code, conventions and how CI works, is in the docs:

- **[Developer onboarding](docs/development/onboarding.md)**: start here
- [Testing & CI](docs/development/testing.md)
- [Writing a plugin](docs/development/writing-plugins.md)
- [Protobufs, releases & docs](docs/development/maintenance.md)

Published version: <https://sangmeshcp.github.io/bigmuddy-network-telemetry-pipeline/>

## In short

```bash
make build lint test        # must pass before you open a PR
make race                   # if you touched concurrency
```

1. Branch from `master`; one logical change per PR.
2. Include tests. Bug fixes start with a failing test.
3. Update docs (`docs/configuration/*.md`, `examples/pipeline.yaml`) in the
   same PR as any config or behaviour change.
4. Open a PR against `master` and get CI green.

## Reporting bugs

Please include the pipeline version (`pipeline -version`), your config with
secrets removed, the device platform and software version, and relevant logs
from a run with `log: {level: debug}`.

## Security issues

Please don't open public issues for vulnerabilities. Use GitHub's private
vulnerability reporting (**Security → Report a vulnerability**) on this
repository.
