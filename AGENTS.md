# Working on TorrentSeek

Guidance for agents (and humans) making changes here.

## The spec is load-bearing

`docs/spec/` is the project's contract, not documentation written after the
fact. The rule:

> **Any change to externally observable behavior lands in the same PR as a
> spec update.** If the spec and the code disagree, that is a bug in one of
> them — decide which, and fix that one.

Externally observable means:

- HTTP API surface: endpoints, request/response fields, status codes, error
  codes, auth behavior (`02-http-api.md`).
- Streaming semantics: Range handling, blocking/timeout behavior, scheduling
  policy, window/bootstrap behavior (`03-streaming.md`).
- The backend interface contract and its error semantics (`01-backends.md`).
- Backend-specific mechanisms and their failure handling
  (`04-deluge-backend.md`).
- Daemon flags, defaults, and tuning knobs (the knob table in
  `03-streaming.md`; scope/process model in `00-overview.md`).

Below spec level (change freely, no spec edit needed):

- Log output — messages, levels, fields. Logging is for humans debugging,
  not a contract. (The *existence* of a `-debug` flag is spec-worthy; what
  it prints is not.)
- Internal refactors, test infrastructure, CI mechanics, install scripts.
- Performance work that doesn't change contracts or defaults.

When unsure, err toward updating the spec: a one-line spec edit is cheap,
and a reader discovering unspecced behavior is expensive.

## Spec style

- Specs contain **decisions, not open questions**. When a choice comes up,
  make the call (or ask the maintainer), write down the outcome and the
  one-sentence why. No "TBD" sections.
- Keep each doc's scope: overview = boundaries and principles; numbered
  specs = their layer. New surface area gets a new numbered doc.
- Specs and the README are **standalone**: written for someone encountering
  the repo with no prior context, not someone who sat in on the discussion,
  investigation, or PR that produced a decision. State the decision and its
  reasoning on their own merits. Cut anything whose relevance depends on
  having been there — narrating how a conclusion was reached ("turned out
  to matter in practice", "after further digging"), or comparing against an
  earlier state as if the reader already knows it existed. A link to
  another doc for supporting evidence (a real investigation, a spec) is
  fine; a reference that only resolves for someone already familiar with
  the backstory is not.

## Development conventions

- Milestone-sized PRs into `main`; every PR must pass `make test`
  (vet + race-enabled tests) and the cross-compile CI job.
- The streaming core is developed against the fake backend
  (`internal/backend/fake`), the Deluge backend against a real `deluged`
  process (no simulator or container — `deluged` is a plain process and
  Docker is unavailable in this project's CI environment); end-to-end
  truth comes from the live harness (`test/live/run.sh`, manual "Live
  Deluge test" workflow). A change to scheduling or backend behavior
  should extend whichever of those layers can prove it.
- Reproduce field bugs as automated tests (usually in the live harness)
  before fixing them, so the fix is demonstrated rather than asserted.

## Releases

Every merge to `main` rebuilds the rolling **latest** prerelease
(`release.yml`). Versioned releases are semver: cut one by running the
**Release** workflow from the Actions tab with the version as input
(e.g. `v1.2.0` — the bump is a judgment call, so the workflow never
derives it), or by pushing a `v*` tag by hand.
