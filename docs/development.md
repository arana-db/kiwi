# Development Guide

## Prerequisites

```bash
# Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# protobuf compiler (macOS)
brew install protobuf

# protobuf compiler (Linux)
apt install protobuf-compiler
```

## Quick Start

```bash
make check         # fast syntax check
make build         # debug build
make release       # release build
make standalone    # build (release) and run a single node
make test          # run all tests
make fmt           # format code
make lint          # run clippy
```

All build-related targets (`build`, `release`, `standalone`, `test`, `clean`) delegate to `scripts/dev.sh` and automatically use sccache if installed.

## Dev Script

`scripts/dev.sh` wraps common cargo commands with sccache integration and extra utilities that Make doesn't cover:

```bash
./scripts/dev.sh check       # cargo check
./scripts/dev.sh build       # cargo build (auto-uses sccache)
./scripts/dev.sh run         # cargo run
./scripts/dev.sh test        # cargo test
./scripts/dev.sh watch       # auto-check on file save (requires cargo-watch)
./scripts/dev.sh clean       # cargo clean
./scripts/dev.sh stats       # show build stats and sccache cache info
```

Options: `--release`, `-v` (verbose), `--debug` (disable sccache, enable debug symbols).

Windows equivalents: `scripts\dev.cmd` and `scripts\dev.ps1`.

## Speeding Up Builds

### sccache

```bash
./scripts/quick_setup.sh     # installs sccache + cargo-watch, configures Cargo
```

After the first build, incremental builds are 50-90% faster. The dev script and Makefile automatically use sccache if installed.

### Why is compilation slow?

RocksDB (`librocksdb-sys`) is a large C++ dependency compiled from source. The first build takes ~18 minutes.

| Operation | First build | Incremental (sccache) |
|-----------|------------|---------------------|
| `cargo build` | ~18 min | ~30 sec - 2 min |
| `cargo check` | ~5 min | ~5-10 sec |

### Tips

- Use `make check` during development — it skips codegen and is 5-10x faster.
- Use `cargo build -p <crate>` to build only the crate you're working on.
- `scripts/dev.sh watch` auto-checks on every file save for instant feedback.

## Runtime Architecture

Network I/O and storage operations communicate via an **async message channel**. `RuntimeManager` (`src/common/runtime/`) manages the lifecycle. The network side uses a `StorageClient` to send requests; `StorageServer` receives them, executes against RocksDB, and responds via oneshot channels.

See [README.md](../README.md) for the crate layout and request flow diagram.

### Adding a Command

Commands implement the `Cmd` trait (`src/cmd/src/lib.rs`):
- `meta()` → CmdMeta (name, arity, flags like WRITE/READONLY/RAFT)
- `do_initial(&self, client)` → validate args, set client key
- `do_cmd(&self, client, storage)` → business logic

To add a new command:
1. Create `src/cmd/src/yourcommand.rs` — define a struct with `CmdMeta`, implement `Cmd` using `impl_cmd_meta!()` and `impl_cmd_clone_box!()` macros
2. Add `pub mod yourcommand;` in `src/cmd/src/lib.rs`
3. Register it in `src/cmd/src/table.rs` via `register_cmd!(cmd_table, YourCmd)`

### Storage Model

`Storage` holds multiple `Redis` instances (default 3), each backed by a RocksDB database with 6 column families: `MetaCF` (metadata & strings), `HashesDataCF`, `SetsDataCF`, `ListsDataCF`, `ZsetsDataCF`, `ZsetsScoreCF`.

A `SlotIndexer` hashes keys to distribute across instances. `LockMgr` provides sharded key-level locking for consistency.

## Lint Rules

- **`clippy::unwrap_used` is denied project-wide.** Use `expect()` with a descriptive message, or propagate errors with `?`/`Result`.
  - In tests, add `#![allow(clippy::unwrap_used)]` at the top of the test module.
- `clippy::dbg_macro` and `clippy::implicit_clone` are warnings.
- All new `.rs` files must include the Apache 2.0 license header (enforced by CI). Copy the header from any existing source file.

## Testing

```bash
make test                          # all unit tests
cargo test --package storage      # tests for a specific crate
cargo test test_redis_mset        # run a single test by name
```

Python integration tests require a running Kiwi server:

```bash
# Terminal 1
cargo run --bin kiwi

# Terminal 2
pip install redis pytest
pytest tests/python/ -v
```

Storage tests use `tempfile::tempdir()` for isolated RocksDB instances.

## Gotchas

- **RocksDB fork**: The project uses the `v0.51.0-arana.1` maintenance release tag from `arana-db/rust-rocksdb`, not the official `rust-rocksdb` crate. Published release tags must not be moved. This fork adds the TableProperties Collector/Factory FFI required by Storage LogIndex, and `Cargo.lock` records the exact resolved commit.
- **Binary name is `kiwi`**, not `server` — defined in `src/server/Cargo.toml` as `[[bin]] name = "kiwi"`.
