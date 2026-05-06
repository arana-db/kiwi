<!-- Go-to brief for AI coding agents working on Kiwi. -->

# AGENTS.md -- Kiwi Agent Operations Guide

## Quick reference

**Tech stack**: Rust (nightly) | Tokio (dual runtime) | RocksDB (custom fork) | OpenRaft 0.9 | RESP (nom)

```bash
# Build & Check
cargo check                        # Fast syntax check
cargo build                        # Debug build
cargo build --release              # Release build (LTO)
cargo run --release                # Run server (127.0.0.1:7379)

# Quality
cargo fmt --all                    # Format (rustfmt, edition 2024)
cargo fmt --all -- --check         # Format check
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used  # Lint

# Test
cargo test                         # All unit tests
cargo test --package <crate>       # Test specific crate
pytest tests/python/ -v            # Integration tests (needs running server)
```

**Hard rules** -- never violate:

- No `.unwrap()` in non-test code (CI: `-D clippy::unwrap_used`).
- No `dbg!()` macro (clippy warns).
- No `once_cell::sync::Lazy` or `lazy_static!` -- use `std::sync::LazyLock`.
- No hardcoded paths, secrets, or endpoints.
- No skipping license headers (Apache 2.0, enforced by SkyWalking Eyes).
- Integration tests require running server -- explain skips explicitly.

**Always do**:

- Read relevant files before modifying code.
- Run `cargo fmt --all` and `cargo clippy` before committing.
- Follow existing code patterns in the same crate.
- Add tests for new functionality.
- Use `Result<T, E>` with descriptive error types (`thiserror`/`snafu`).

**Ask first** before:

- Modifying `conf` crate configuration structures.
- Adding new dependencies to workspace `Cargo.toml`.
- Changing Raft consensus logic (`raft` crate).
- Changing dual runtime architecture (`common/runtime` crate).
- Deleting or renaming public APIs.
- Modifying RocksDB column family layout.

When unsure, leave a `// TODO(agent)` comment and note the constraint in your response.

---

## Repository map

```text
src/
  server/           Binary entry point (kiwi), CLI args (clap), bootstrap
  net/              TCP server, RESP pipeline, async parser, storage client
  storage/          RocksDB storage: Redis data structures, TTL, compaction, checkpoint
  engine/           Low-level RocksDB abstraction
  cmd/              Redis command implementations (90+ files)
  executor/         Command execution engine, cluster executor
  resp/             RESP protocol parser/encoder (nom)
  client/           Client abstraction
  conf/             Configuration parsing (INI-style), Raft type defs
  raft/             OpenRaft: log store, state machine, snapshot, network
  kstd/             Standard lib: env utils, lock manager, slice helpers
  common/macro/     Proc macros
  common/runtime/   Dual runtime: RuntimeManager, channels, metrics, circuit breaker

tests/              Integration tests (Rust + Python)
benches/            Benchmarks (dual runtime, list, general, raft)
scripts/            Dev tooling, cluster management
```

---

## Code style & patterns

- **Composition over inheritance** -- Rust traits + delegation, no deep trait hierarchies.
- **Error handling**: `Result<T, E>` everywhere. Use `thiserror` for library errors, `snafu` for structured context. Never `.unwrap()` outside tests.
- **Concurrency**: `tokio` async/await for I/O, `parking_lot` for sync primitives, `arc-swap` for hot-swappable storage.

| Pattern | Example |
|---------|---------|
| Command implementation | `src/cmd/src/get.rs`, `src/cmd/src/set.rs` |
| Storage data structure | `src/storage/src/redis_strings.rs`, `redis_hashes.rs` |
| Column family | `src/storage/src/storage.rs` (CF definitions) |
| Raft adaptor | `src/raft/src/log_store.rs`, `state_machine.rs` |
| Dual runtime | `src/common/runtime/` (RuntimeManager, StorageServer) |

**Naming conventions**:

| Type | Pattern | Example |
|------|---------|---------|
| Crate | `kebab-case` | `common-runtime`, `storage` |
| Module | `snake_case` | `redis_strings.rs`, `log_store.rs` |
| Struct | `PascalCase` | `StorageClient`, `RuntimeManager` |
| Function | `snake_case` | `handle_get`, `create_cf` |
| Constant | `SCREAMING_SNAKE_CASE` | `DEFAULT_PORT`, `MAX_BATCH_SIZE` |
| Error type | `PascalCase` + `Error` suffix | `StorageError`, `RaftError` |

**Performance**:

- No blocking I/O in async contexts (use `spawn_blocking` for RocksDB ops if needed).
- Prefer `bytes::Bytes` over `Vec<u8>` for data passing.
- Use `batch` operations for RocksDB (WriteBatch) instead of individual puts.
- Dual runtime: network runtime is I/O-optimized, storage runtime is CPU-optimized.

**Imports**: Group as stdlib, third-party, crate-internal (rustfmt handles order via `reorder_imports`).

---

## Domain experts & skills

Fire the appropriate **expert subagent** or **load a skill** based on what you're working on.

| Working on... | Fire subagent | Load skill |
|---------------|---------------|------------|
| RocksDB storage layer | `storage-expert` | -- |
| New Redis command | `cmd-expert` | `add-command` |
| Raft consensus | `raft-expert` | -- |
| Network/RESP protocol | `net-expert` | -- |
| Dual runtime | `runtime-expert` | `debug-runtime` |
| New column family | `storage-expert` | `add-column-family` |
| Tests | -- | `add-test` |
| Architecture planning | `planner` | -- |

---

## Core concepts

**Dual Runtime Architecture**: Network and storage run on separate tokio runtimes.
- Network runtime: I/O-optimized, handles RESP parsing and client connections.
- Storage runtime: CPU-optimized, handles RocksDB operations.
- Communication via `StorageClient`/`StorageServer` async channels.
- Features: circuit breaker, backpressure, request queuing, retry with exponential backoff.

**Storage Engine** (`src/storage/`):
- 6 column families: `default` (meta), `hash_data_cf`, `set_data_cf`, `list_data_cf`, `zset_data_cf`, `zset_score_cf`.
- Custom key/value formats per data type.
- Custom compaction filters for data and meta cleanup.
- TTL/expiration via `ExpirationManager`.
- Checkpoint/snapshot support for Raft.

**Raft Consensus** (`src/raft/`):
- OpenRaft 0.9 with `storage-v2` feature.
- RocksDB-backed log store (`log_store_rocksdb.rs`).
- State machine applies commands to storage.
- `ArcSwap<GlobalStorage>` for atomic hot-swapping during snapshot install.
- HTTP-based Raft networking via `actix-web`.

**Command Pattern** (`src/cmd/`):
- Each Redis command is a separate file (90+ commands).
- Registered via command table (`table.rs`).
- Commands implement execution logic, dispatched by `executor`.

**RESP Protocol** (`src/resp/`):
- Parser built with `nom` 8.0.
- Supports RESP2/RESP3 wire format.
- Inline command parsing support.

---

## Testing rules

| Marker | When |
|--------|------|
| `#[cfg(test)]` | Standard Rust unit tests |
| `tempfile::TempDir` | Isolated test databases (always use `unique_test_db_path()`) |
| `proptest` | Property-based testing for data structures |

- Naming: `test_<what>_<condition>_<expected>()` with Arrange/Act/Assert.
- Cleanup: always use `safe_cleanup_test_db()` helper.
- Integration tests: Python-based with `redis-py`, require running server.

| Suite | Command | Requires |
|-------|---------|----------|
| Unit | `cargo test` | Nothing |
| Specific crate | `cargo test --package storage` | Nothing |
| Integration (Python) | `pytest tests/python/ -v` | Running server |
| Benchmarks | `cargo bench` | Nothing |

---

## Collaboration & review

- **Branches**: `feat/*`, `fix/*`, `chore/*`, `docs/*`, `test/*` (kebab-case).
- **Commits**: Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`), ~72 char subject, imperative voice.
- **PR title**: Must match `^(feat|fix|test|refactor|chore|upgrade|bump|style|docs|perf|build|ci|revert)(\(.*\))?:.*`
- **Pre-merge**: CI runs format, clippy, build+test (macOS/Ubuntu/Windows), license headers.

---

## Reference material

- **README**: `README.md` / `README_CN.md`
- **Config example**: `config.example.toml`
- **Cluster config**: `cluster.conf`
- **Changelog**: `CHANGELOG.md`
- **CI**: `.github/workflows/ci.yml`
