# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kiwi is a Redis-compatible key-value database built in Rust. It uses RocksDB as the persistent storage backend and integrates OpenRaft for distributed consensus and high availability. The server defaults to `127.0.0.1:7379`.

## Prerequisites

- Rust toolchain (stable)
- `protoc` (protobuf compiler) — installed by CI on all platforms. Install via `brew install protobuf` (macOS) or `apt install protobuf-compiler` (Linux).

## Build & Development Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build

# Run
cargo run --bin kiwi           # Run server (debug)
cargo run --release            # Run server (release)

# Test
cargo test                     # All unit tests
cargo test --package storage   # Tests for a specific crate
cargo test test_redis_mset     # Run a single test by name

# Lint & Format
make lint                      # clippy with all warnings as errors + unwrap_used denied
make fmt                       # Format all code
make fmt-check                 # Check formatting without modifying
```

The lint command enforces: `cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used`

## Lint Rules

- **`clippy::unwrap_used` is denied project-wide.** Use `expect()` with a descriptive message, or propagate errors with `?`/`Result`. Never use `.unwrap()`.
  - In test code, add `#![allow(clippy::unwrap_used)]` at the top of the test module or `#[allow(clippy::unwrap_used)]` on individual test functions.
- `clippy::dbg_macro` and `clippy::implicit_clone` are warnings (see `[workspace.lints.clippy]` in root Cargo.toml).
- All new `.rs` files must include the Apache 2.0 license header (enforced by CI via `skywalking-eyes`). Copy the header from any existing source file.

## PR Title Convention

PR titles must follow conventional commits format (enforced by CI):
```
type(scope): description
```
Allowed types: `feat`, `fix`, `test`, `refactor`, `chore`, `upgrade`, `bump`, `style`, `docs`, `perf`, `build`, `ci`, `revert`

## Architecture

### Workspace Crates

```
src/server/    → Entry point (main.rs): CLI args, runtime init, server startup
src/net/       → Network layer: TCP server, connection handling, cluster routing
src/cmd/       → Command definitions: Cmd trait, CmdMeta, command table
src/executor/  → Command executor: tokio async task pool via async_channel
src/storage/   → Storage layer: multi-instance RocksDB, column families, TTL
src/engine/    → Engine trait abstraction over RocksDB
src/resp/      → RESP protocol: parser, encoder, RespData types
src/raft/      → Raft consensus: OpenRaft integration, state machine, router
src/conf/      → Configuration: TOML loading, validation, ClusterConfig
src/client/    → Client context: connection state, argv, reply buffer
src/common/runtime/ → Runtime management: async channel between net & storage
src/common/macro/   → Proc macros: #[stack_trace_debug] for error types
src/kstd/      → Utilities: LockMgr (sharded key-level locking), slice, status
```

### Runtime Architecture

Network I/O and storage operations communicate via an **async message channel**. The `RuntimeManager` (in `src/common/runtime/`) manages the lifecycle. The network side uses a `StorageClient` to send requests; the `StorageServer` receives them, executes against RocksDB, and responds via oneshot channels.

### Request Flow

```
Client → TCP accept (net) → RESP parse (resp) → Command lookup (cmd table)
  → CmdExecutor async tasks (executor) → Cmd.execute() → Storage ops (storage/engine)
  → RESP encode response → write back to client
```

In cluster mode, write commands route through `RequestRouter → RaftNode.propose()` for consensus before applying to the state machine.

### Command System

Commands implement the `Cmd` trait (`src/cmd/src/lib.rs`):
- `meta()` → CmdMeta (name, arity, flags like WRITE/READONLY/RAFT)
- `clone_box()` → Box<dyn Cmd> (required for cloning trait objects)
- `do_initial(&self, client)` → validate args, set client key
- `do_cmd(&self, client, storage)` → business logic

To add a new command:
1. Create `src/cmd/src/yourcommand.rs` — define a struct with `CmdMeta`, implement `Cmd` using `impl_cmd_meta!()` and `impl_cmd_clone_box!()` macros
2. Add `pub mod yourcommand;` in `src/cmd/src/lib.rs`
3. Register it in `src/cmd/src/table.rs` via `register_cmd!(cmd_table, YourCmd)`

### Storage Model

`Storage` holds multiple `Redis` instances (default 3), each backed by a RocksDB database with 6 column families:
- `MetaCF`: metadata & strings
- `HashesDataCF`, `SetsDataCF`, `ListsDataCF`, `ZsetsDataCF`, `ZsetsScoreCF`

A `SlotIndexer` hashes keys to distribute across instances. `LockMgr` provides sharded key-level locking for consistency.

### Raft Integration

`src/raft/` bridges Kiwi with OpenRaft via an adaptor pattern:
- `RaftNode` wraps the OpenRaft instance
- `KiwiStateMachine` applies committed entries to storage
- `RequestRouter` routes commands based on cluster mode and consistency level (Eventual, Strong, Linearizable)
- `RaftStorage` persists Raft logs to RocksDB

## CI

CI runs on Linux, macOS, and Windows:
1. License header check (skywalking-eyes)
2. `cargo fmt --check`
3. `make lint` (clippy)
4. `make build` + `make test`
5. Python integration tests (Ubuntu only, requires running server)

## Testing

- **Unit tests**: In each crate, run with `cargo test --package <crate>` (e.g. storage, cmd, executor, raft)
- **Integration tests**: `tests/` directory contains Rust integration tests and Python tests (`tests/python/`)
- **Python integration tests** require a running Kiwi server and `pip install redis pytest`:
  ```bash
  # Terminal 1: start server
  cargo run --bin kiwi
  # Terminal 2: run tests
  pytest tests/python/ -v
  ```
- Storage tests use `tempfile::tempdir()` for isolated RocksDB instances — see `src/storage/src/util.rs` for `unique_test_db_path()` and `safe_cleanup_test_db()`

## Gotchas

- **RocksDB fork**: The project uses `arana-db/rust-rocksdb` (pinned to a specific rev), not the official `rust-rocksdb` crate. This fork adds TablePropertiesCollector FFI functions. See comment in root `Cargo.toml` near the `rocksdb` dependency.
- **Binary name is `kiwi`**, not `server` — defined in `src/server/Cargo.toml` as `[[bin]] name = "kiwi"`.
