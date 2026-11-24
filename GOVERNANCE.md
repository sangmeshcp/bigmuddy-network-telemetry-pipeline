# Governance

## Roles

**Users and contributors.** Anyone can use the project, report issues,
suggest features, improve docs and send pull requests. Contributions are
accepted under the Apache 2.0 license with a
[DCO sign-off](CONTRIBUTING.md#developer-certificate-of-origin-dco).

**Maintainers** review and merge pull requests, triage issues, cut releases
and steward the project's direction.

| Maintainer | GitHub |
|---|---|
| Sangamesh Patil | [sangmeshcp](https://github.com/sangmeshcp) |

## Decisions

- Day-to-day changes are decided in pull requests: one maintainer approval
  and green CI are needed to merge.
- Larger changes (new plugin types, config format or metric-naming changes,
  new dependencies with wide impact) start as an issue or discussion
  describing the problem and the options, open for comment for at least a
  week.
- Maintainers seek consensus; if it can't be reached, the maintainers decide
  by majority.

## Becoming a maintainer

Contributors with a track record of high-quality contributions and reviews
over several months may be invited by the existing maintainers. Maintainers
who are inactive for six months may be moved to emeritus status.

## Releases

Releases are cut by maintainers by pushing a `vX.Y.Z` tag. The release
workflow builds, signs and publishes everything; see
[Maintenance → Releases](docs/development/maintenance.md#releases).
