# Documentation Index

A map of the Kiwi documentation tree and a suggested reading order. If you are not
sure where to start, follow the **Newcomer** path below.

## Top-level entry points

| Document | Audience | What it covers |
|----------|----------|----------------|
| [`README.md`](../README.md) / [`README_CN.md`](../README_CN.md) | Everyone | Project intro, features, quickstart, toolchain, and documentation entry points |
| [`CLAUDE.md`](../CLAUDE.md) (a.k.a. `AGENTS.md`) | Contributors / AI agents | Dev commands, crate layout, code style, behavioral guidelines |
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | Contributors | How to contribute |
| [`CHANGELOG.md`](../CHANGELOG.md) | Everyone | Notable changes (history) |

## Planning and requirements

The sole authoritative project entry is `.planning/SDD.md`. It owns the current
architecture, `M0`–`M10` roadmap, `M0`–`M6` current executable milestone scope,
`WP0`–`WP8` work packages,
current state, GitHub traceability, and verification gates.

Its subordinate registries are:

- `.planning/PROJECT.md` — constitution / north star
- `.planning/REQUIREMENTS.md` — acceptance requirement registry (`REQ-*`)
- `.planning/DECISIONS.md` — approved decision registry
- `.planning/OPEN_QUESTIONS.md` — unresolved high-impact questions
- `.planning/REFERENCES.md` — exact upstream and license references

`.planning/STATE.md`, `.planning/KANBAN.md`, and `.planning/ROADMAP.md` are
compatibility pointers and must not maintain independent project state.

## Developer docs

| Document | What it covers |
|----------|----------------|
| [`docs/development.md`](development.md) | Dev environment, build optimization, sccache, lint rules, testing (incl. Python integration) |
| [`docs/cluster.md`](cluster.md) | Raft cluster quickstart and write-path verification |
| [`docs/key-encoding.md`](key-encoding.md) | Key/value encoding internals (Chinese, with diagrams) |

## Product & compatibility

| Document | What it covers |
|----------|----------------|
| [`docs/prd.md`](prd.md) | Product requirements; goals, scope, Redis 8.8.1 compatibility target |
| [`docs/personas-and-user-stories.md`](personas-and-user-stories.md) | Target users and usage scenarios |
| [`docs/compatibility/redis-8.8.1.md`](compatibility/redis-8.8.1.md) | Exact Oracle, raw RESP, TCL, and client-test boundaries |
| [`docs/compatibility/redisraft-public-profile.md`](compatibility/redisraft-public-profile.md) | RedisRaft public compatibility profile |

## Architecture

| Document | What it covers |
|----------|----------------|
| [`docs/architecture/redis-8.8.1-system-boundaries.md`](architecture/redis-8.8.1-system-boundaries.md) | Cache OFF request, storage, and consensus boundaries |
| [`docs/architecture/redis-hot-tier-native-abi.md`](architecture/redis-hot-tier-native-abi.md) | Deferred native ABI contract (future design, not an implementation authorization) |
| [`docs/architecture/combined-distribution-licensing.md`](architecture/combined-distribution-licensing.md) | Future Redis-derived library and source-distribution licensing obligations |

## Quality & stability

| Document | What it covers |
|----------|----------------|
| [`docs/quality/system-stability-gate.md`](quality/system-stability-gate.md) | Required evidence (G1–G7) before deferred hot-tier work is reconsidered |
| [`docs/quality/quality-gates.md`](quality/quality-gates.md) | Code, test, and release quality gates |
| [`docs/performance/storage-runtime-baseline.md`](performance/storage-runtime-baseline.md) | Storage-runtime performance baseline |

## Design history (`docs/superpowers/`)

`docs/superpowers/plans/` and `docs/superpowers/specs/` contain **dated design
records** (filename prefix `YYYY-MM-DD-`). They are historical decision logs, not
a second roadmap. Follow related specs and plans by topic and by their explicit
cross-references; dates do not define a one-to-one pairing. For the current
authoritative architecture, state, and plan, use `.planning/SDD.md`.

## Suggested reading order

- **Newcomer**: `README.md` → `docs/development.md` → `docs/cluster.md` → `docs/key-encoding.md`
- **Contributor**: `CLAUDE.md` → `.planning/SDD.md` → `docs/development.md` → `docs/quality/quality-gates.md`
- **Architecture / compatibility**: `docs/prd.md` → `docs/compatibility/redis-8.8.1.md` → `docs/architecture/redis-8.8.1-system-boundaries.md`
- **Stability & quality**: `docs/quality/system-stability-gate.md` → `docs/quality/quality-gates.md` → `docs/performance/storage-runtime-baseline.md`
- **Design history**: `docs/superpowers/plans/` + `docs/superpowers/specs/` (follow by topic and explicit cross-references)
