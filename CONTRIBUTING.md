# Contributing to Kiwi

Thank you for your interest in contributing to Kiwi!

For development environment setup, build commands, and architecture overview, see [docs/development.md](docs/development.md).

## Development Baseline

Normal development, CI, and release builds use Rust 1.97.1 stable. The repository
root contains `rust-toolchain.toml`, so rustup selects the exact toolchain
automatically. Verify it with:

```bash
rustup show active-toolchain
rustc --version --verbose
```

The source currently remains on Rust 2021 Edition. Migrating to Rust 2024 Edition
is a separate follow-up change and is not part of the toolchain baseline update.
Dated nightly toolchains are limited to specialized checks such as Sanitizers.

`protoc` is required. Windows builds use the Rust MSVC target and the Visual
Studio C++ build tools; Linux and macOS builds require the native C/C++ tooling
documented in [docs/development.md](docs/development.md).

## Pull Request Workflow

1. Fork the repository and clone your fork
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Make your changes
4. Run `make test` and `make lint` to verify
5. Commit following the [convention](#commit-convention) below
6. Push and create a Pull Request

Keep PRs focused on a single feature or fix. Add tests for new functionality and update documentation as needed.

## Commit Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/). PR titles are checked against this format by CI.

| Type | Usage |
|------|-------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation changes |
| `refactor` | Code refactoring |
| `perf` | Performance improvement |
| `test` | Adding/updating tests |
| `chore` | Maintenance tasks |
| `ci` | CI/CD changes |
| `build` | Build system changes |
| `style` | Formatting, whitespace |
| `revert` | Revert a previous commit |

Examples:
```
feat: add support for Redis streams
fix: resolve memory leak in connection pool
docs: update installation instructions
```

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

## Compatibility and Third-Party Source Rules

- Redis compatibility and public interface changes target exact Redis `8.8.1` commit `77b6c308396c9700672390a210143a8496fb4b10`.
- Update the machine-readable compatibility manifest and raw differential tests when changing public command behavior.
- A skipped Redis test must include an owner, Issue, exact reason, introduction date, and removal condition.
- Kiwi-authored source remains Apache-2.0. Future Redis-derived native source is maintained in the separately governed AGPL-3.0-only fork and must retain upstream copyright, license, exact source identity, patch history, and reproducible build records.
- The Embedded Redis Hot Tier is design-only. Until the system stability gate passes and the user explicitly authorizes a separate implementation task, do not add Redis-derived production dependencies, dynamic-library loaders, hot-tier data paths, default settings, or release packaging.
- Do not vendor or copy RedisRaft source or tests into Kiwi. RedisRaft is used as a clean-room public behavior reference.
- redis-rs is permitted only in compatibility tooling or development/test dependencies. Production server crates must not depend on it.

Every implementation PR must reference the relevant `REQ-*` entries in `.planning/REQUIREMENTS.md`. When a work item is completed, update `.planning/STATE.md` and `.planning/KANBAN.md` with the exact validation command and result.
