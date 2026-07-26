# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kiwi is a Rust database targeting Redis 8.8.1 compatibility. It persists the complete authoritative data set with RocksDB, replicates with OpenRaft, and separates network I/O and storage into two distinct Tokio runtimes that communicate via async message channels.

The exact Redis compatibility and interface-design baseline is tag `8.8.1`, commit `77b6c308396c9700672390a210143a8496fb4b10`. Current required work runs Cache OFF and focuses on compatibility, authoritative RocksDB recovery, OpenRaft correctness, and system stability. The Embedded Redis Hot Tier remains design-only until the stability gate passes and the user explicitly authorizes a separate implementation task.

## Common Development Commands

Use `make` for day-to-day tasks. Build and test targets delegate to `scripts/dev.sh`, which automatically uses `sccache` when installed.

| Command | Purpose |
|---------|---------|
| `make check` | Fast syntax check (`cargo check`); preferred during iterative development. |
| `make build` | Debug build. |
| `make release` | Release build. |
| `make standalone` | Build and run a single-node server on the default port (`127.0.0.1:7379`). |
| `make cluster` | Start a local multi-node Raft cluster (`make cluster NODES=5`). |
| `make test` | Run all Rust unit tests. Sets `RUST_TEST_THREADS=1` and raises the fd limit. |
| `make fmt` / `make fmt-check` | Format code / check formatting (CI). |
| `make lint` | Run clippy with project lints (`-D warnings -D clippy::unwrap_used`). |
| `./scripts/dev.sh test --release` | Run tests in release mode. |
| `./scripts/dev.sh build --debug` | Build with full debug symbols; disables sccache. |

### Running a Single Test

```bash
# Run one test inside a specific crate
cargo test --package storage test_redis_mset

# Run by test name across the workspace
cargo test test_redis_mset
```

### Python Integration Tests

Requires a running server:

```bash
# Terminal 1
make standalone

# Terminal 2
make -C tests install-deps
make -C tests test-python
```

## Toolchain & Build Notes

- Normal development, CI, and release builds use Rust 1.97.1 stable. The root
  `rust-toolchain.toml` selects the exact toolchain automatically; verify it with
  `rustup show active-toolchain` and `rustc --version --verbose`.
- The source remains on Rust 2021 Edition. Rust 2024 Edition migration is a
  separate follow-up change; do not treat it as part of the baseline update.
- Dated nightly toolchains are reserved for specialized checks such as
  Sanitizers and do not define the normal development baseline.
- The first build compiles `librocksdb-sys` from source and can take ~18 minutes. Incremental builds with `sccache` are typically 30 seconds–2 minutes.
- The project depends on a forked RocksDB crate (`arana-db/rust-rocksdb`) because upstream does not yet expose the `TablePropertiesCollector` FFI functions required by the Raft module. Do not switch to the official `rust-rocksdb` crate.
- `protoc` (protobuf compiler) is required. Windows builds use the Rust MSVC
  target and Visual Studio C++ build tools; Linux and macOS builds need the
  project's native C/C++ build dependencies.

## Architecture

### Crate Layout

Workspace members under `src/`:

- `server/` — Binary entry point (`kiwi`), `RuntimeManager` setup, and Raft wiring.
- `net/` — TCP/Unix server, connection handling, pipeline, storage client, and executor integration.
- `resp/` — RESP protocol parser, encoder, `RespData` types, and command negotiation.
- `cmd/` — Redis command implementations. Each command implements the `Cmd` trait.
- `executor/` — Async command executor / task pool.
- `client/` — Per-connection client state (`argv`, `cmd_name`, `key`, reply buffer, authentication).
- `storage/` — Multi-instance concrete RocksDB ownership, column families, TTL, key encoding, and log index for Raft.
- `raft/` — OpenRaft integration, concrete RocksDB log-store ownership, state machine, snapshot archive, and gRPC services.
- `conf/` — Configuration loading, validation, and sample-config generation.
- `kstd/` — Utilities, including `LockMgr` for sharded key-level locking.
- `common/runtime/` — Dual-runtime manager, async message channel between network and storage runtimes, and `StorageServer`.
- `common/macro/` — Proc macros, including `#[stack_trace_debug]`.

### Request Flow

```text
Client → TCP accept [network runtime] → RESP parse → command lookup
  → CmdExecutor [network runtime] → Cmd.execute() → StorageClient
    → async message channel →
  → StorageServer [storage runtime] → RocksDB
    ← oneshot response ←
  → RESP encode [network runtime] → write back to client
```

### Adding a Redis Command

Commands implement the `Cmd` trait in `src/cmd/src/lib.rs`:

- `meta()` → `CmdMeta` (name, arity, flags such as `WRITE`, `READONLY`, `RAFT`).
- `do_initial(&self, client)` → validate arguments and set the client key.
- `do_cmd(&self, client, storage)` → business logic; call `Storage` methods and set the reply.

Steps to add a command:

1. Create `src/cmd/src/<command>.rs`.
2. Implement `Cmd` using `impl_cmd_meta!()` and `impl_cmd_clone_box!()`.
3. Add `pub mod <command>;` in `src/cmd/src/lib.rs`.
4. Register it in `src/cmd/src/table.rs` via `register_cmd!(cmd_table, YourCmd)`.
5. Add unit tests in the appropriate crate test directory (e.g., `src/storage/tests/`).

### Storage Model

- `Storage` holds multiple `Redis` instances (default 3), distributed by a `SlotIndexer` hash.
- Each `Redis` instance is a RocksDB database with column families: `MetaCF` (metadata & strings), `HashesDataCF`, `SetsDataCF`, `ListsDataCF`, `ZsetsDataCF`, and `ZsetsScoreCF`.
- `LockMgr` provides sharded key-level locking.
- TTL and expiration are managed by `ExpirationManager`.

### Raft Cluster Mode

When `config.raft` is present, `src/server/src/main.rs`:

1. Creates a Raft node via `raft::node::create_raft_node`.
2. Bridges storage-runtime binlogs to Raft `client_write` through an async channel.
3. Starts gRPC services (core, admin, client, metrics) on `raft_addr`.
4. Exposes a leader gate so non-leader nodes can reject or redirect writes.

## Code Style & Lint

- `clippy::unwrap_used` is denied project-wide. Use `expect("descriptive message")` or propagate errors with `?`/`Result`.
- In tests, add `#![allow(clippy::unwrap_used)]` at the top of the test module.
- All Kiwi-authored `.rs` files must include the Apache 2.0 license header (enforced by CI). Copy the header from an existing Kiwi source file. Future Redis-derived source belongs to the separately governed AGPL-3.0-only fork and must not be relabeled as Apache-2.0.
- PR titles follow Conventional Commits (checked by CI).
- Run `make fmt && make lint && make test` before opening a PR.

## Behavioral Guidelines

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

### 1. Think Before Coding

- State assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop, name what's confusing, and ask.

### 2. Simplicity First

- Minimum code that solves the problem. Nothing speculative.
- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- If you write 200 lines and it could be 50, rewrite it.

### 3. Surgical Changes

- Touch only what you must. Clean up only your own mess.
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If your changes create unused imports/variables/functions, remove them.
- Don't remove pre-existing dead code unless asked.

The test: every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass."
- "Fix the bug" → "Write a test that reproduces it, then make it pass."
- "Refactor X" → "Ensure tests pass before and after."

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Project Continuity and Crash Recovery

Before modifying files in every new session:

1. Read this file, `CONTRIBUTING.md`, `.planning/PROJECT.md`, `.planning/STATE.md`, and `.planning/KANBAN.md` completely.
2. Read the current plan linked by `.planning/STATE.md`.
3. Read `.codex/recovery/ACTIVE.md` if it exists.
4. Run `git status --porcelain=v2 --branch --untracked-files=all`.
5. Compare branch, HEAD, and dirty ownership with the recovery record.

If branch, HEAD, or dirty ownership differs, report the drift and stop before modifying files. Never automatically run checkout, restore, reset, stash, clean, whole-tree formatting, or deletion to make the state match.

`.planning/` is the versioned project truth. `.codex/recovery/` is ignored local runtime state containing the active task, append-only checkpoints, authority boundaries, and Git snapshots. Save checkpoints with `scripts/codex-workstate.ps1` before long-running work, authority changes, verified milestones, blockers, and session handoff.

Architecture terminology and boundaries:

- Use only **Embedded Redis Hot Tier** or **内嵌 Redis 8.8.1 原生内存热数据层** as the canonical terminology.
- The hot tier is design-only. Do not add Redis-derived production dependencies, loaders, dynamic-library build paths, runtime integration, or release packaging until the system stability gate passes and the user explicitly approves a separate implementation task.
- RocksDB is the only complete authoritative storage; the future hot tier is disposable and rebuildable.
- Cache hits cannot bypass OpenRaft consistency gates.
- RedisRaft defines the public behavioral profile; OpenRaft remains the implementation.
- redis-rs is test-only and must not enter production server dependencies.

## Subagent Policy

This project permits and requires the use of subagents when a task contains two or more independent, bounded workstreams that can be executed in parallel. The lead agent should delegate those workstreams without requesting separate approval for each delegation. Tasks that cannot be safely or usefully decomposed do not require a subagent.

Using subagents does not expand the scope of the user's request or authorize additional state-changing actions. The lead agent remains responsible for defining non-overlapping ownership, coordinating the work, independently verifying material conclusions, resolving duplicate or conflicting findings, and producing the final result.

For pull request reviews, all subagents must remain read-only unless the user explicitly requests fixes. They may inspect the diff and source code, trace callers and implementations, assess tests, and run non-modifying static checks or tests. Subagents must not post GitHub comments or independently issue the final severity or merge decision; the lead agent must verify and submit all inline comments and the final conclusion.

Authorization to implement fixes does not authorize commit, push, merge, rebase, closing the pull request, or resolving review threads. Those actions require the authorization specified by the applicable review protocol.
