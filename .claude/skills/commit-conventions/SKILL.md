---
name: commit-conventions
description: Commit message conventions for Kiwi. Load before git commit.
---

# Commit Conventions

Kiwi follows Conventional Commits format, enforced by PR title checker.

## Format

```text
<type>(<scope>): <subject>

<body>

[Optional sections]
```

## Types

| Type | When to Use |
|------|-------------|
| `feat` | New feature or capability |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `test` | Adding or fixing tests |
| `refactor` | Code change without feature/fix |
| `perf` | Performance improvement |
| `style` | Formatting/style-only changes |
| `build` | Build system or dependencies |
| `ci` | CI pipeline or workflow changes |
| `chore` | Build, deps, config changes |
| `revert` | Revert a previous commit |
| `upgrade` | Dependency upgrade |
| `bump` | Version bump |

## Scope

Infer from changed files:

- `src/storage/` -> `storage`
- `src/raft/` -> `raft`
- `src/cmd/` -> `cmd`
- `src/net/` -> `net`
- `src/resp/` -> `resp`
- `src/executor/` -> `executor`
- `src/engine/` -> `engine`
- `src/common/runtime/` -> `runtime`
- `src/conf/` -> `conf`
- `docs/` -> `docs`
- Multiple areas -> omit scope or use broader term

## Rules

- Subject: imperative mood, ~50-72 chars, no period
- Body: explain "why" not "what", wrap at 72 chars
- Key changes: bullet list for complex commits

## PR Title Regex

```regex
^(feat|fix|test|refactor|chore|upgrade|bump|style|docs|perf|build|ci|revert)(\(.*\))?:.*
```

## Examples

```text
feat(cmd): add MGET command for batch key retrieval

fix(storage): handle empty key in HSET

refactor(raft): extract log store trait for testability

docs: update README with cluster setup instructions

test(storage): add property-based tests for zset encoding
```
