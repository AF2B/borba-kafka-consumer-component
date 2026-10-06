# Contributing

## Set up

```bash
git clone https://github.com/AF2B/borba-kafka-consumer-component.git && cd borba-kafka-consumer-component
make deps       # resolves every dependency
make check      # what a change must pass before a push
make ci         # everything the pipelines enforce
```

`make help` lists every target. You need the Clojure CLI, Babashka and Docker (the vulnerability and secret scans run in
pinned containers); the versions the pipelines use are in `.github/workflows` of [borba-tooling](https://github.com/AF2B/borba-tooling).

## Branches and commits

- **Branches** are `<type>/<short-description>`: `feat/`, `fix/`, `refactor/`, `perf/`, `test/`, `docs/`, `build/`, `ci/` or
  `chore/`, for example `fix/body-limit`. One branch carries one intent.
- **Commits** follow [Conventional Commits](https://www.conventionalcommits.org/): a type, an optional scope, and an imperative
  title in lowercase without a final period, then a body of bullets that say what changed in the library and what that
  does. They are written in English, and they describe the code, never the process that produced it: no ticket numbers, no
  names, no "as discussed". A breaking change is marked with `!` after the type or a `BREAKING CHANGE:` footer.
- **Commit what is finished**, in pieces that each build and pass: a fix with its test, a refactor on its own.

## What a change needs

- **Tests.** Behaviour has tests; a bug fix has a test that failed before the fix. Integration tests carry `^:integration`
  metadata and run against the real service, never a mock of it.
- **Failure as data.** Expected failures are values with a stable `:error` keyword and context, not strings and not exceptions
  thrown from deep inside the logic. Exceptions are for broken invariants and for the edge (I/O, network, database).
- **Named constants** instead of unexplained numbers and strings; calls and definitions with three or more parameters, one per
  line.
- **Documentation** on every public var, parameter by parameter (`- name: what it is`), with a first line that stands alone.
  `make conventions` enforces it, together with the 80-column limit.
- **No reflection**: type-hint the interop that `make reflection` reports.
- **No dead code**, no commented-out code, no `TODO` left behind.
- **The changelog.** A change a consumer can notice adds a line under `[Unreleased]` in `CHANGELOG.md`.

## Releasing

1. Move the `[Unreleased]` entries under a new `## [x.y.z] - YYYY-MM-DD` heading, add the link at the bottom of the file and
   commit it as `docs(changelog): release x.y.z`.
2. Check it: `make release-check TAG=vx.y.z`.
3. Tag and push: `git tag -a vx.y.z -m "Release x.y.z" && git push origin vx.y.z`.

The Release workflow runs the whole gate again, builds the jar and the bill of materials, attests them and creates the GitHub
release with the notes from the changelog and the `deps.edn` coordinate to copy.

## Security

Report vulnerabilities privately, as [`SECURITY.md`](SECURITY.md) describes.
