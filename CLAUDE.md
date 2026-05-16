# CLAUDE.md - Kiwi

## WHAT: Project Overview

Kiwi is a Redis-compatible key-value database built in Rust, providing high capacity,
high performance, and strong consistency through RocksDB and Raft consensus.

**Tech Stack**: Rust (nightly) | Tokio (dual runtime) | RocksDB | OpenRaft 0.9 | RESP (nom)

**Core Crates** (`src/`):

| Crate | Purpose |
|-------|---------|
| `server` | Binary entry point, CLI args (clap), starts RuntimeManager + Raft HTTP |
| `net` | TCP server, RESP pipeline, async parser, storage client |
| `storage` | RocksDB engine: Redis data structures (string/hash/list/set/zset), TTL, compaction |
| `engine` | Low-level RocksDB abstraction |
| `cmd` | Redis command implementations (90+ files: get.rs, set.rs, hset.rs, zadd.rs, ...) |
| `executor` | Command execution engine with builder pattern, cluster executor |
| `resp` | RESP protocol parser/encoder (nom) |
| `client` | Client abstraction layer |
| `conf` | Configuration parsing (INI-style), Raft type definitions |
| `raft` | OpenRaft integration: log store, state machine, snapshot, network |
| `kstd` | Standard library: env utils, lock manager, slice helpers |
| `common/macro` | Proc macros |
| `common/runtime` | Dual runtime architecture: RuntimeManager, channels, metrics, circuit breaker |

## WHY: Purpose

- Redis-compatible KV store with persistent storage (RocksDB)
- Strong consistency via Raft consensus for cluster mode
- Dual runtime architecture: separate network/storage tokio runtimes for fault isolation
- Modular crate design: each concern is an independent workspace member

## HOW: Core Commands

```bash
# Build
cargo check                    # Fast syntax check
cargo build                    # Debug build
cargo build --release          # Release build (LTO, size-optimized)
cargo run --release            # Run server (default: 127.0.0.1:7379)

# Quality
cargo fmt --all                # Format code (rustfmt, edition 2024)
cargo fmt --all -- --check     # Format check only
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used  # Lint

# Test
cargo test                     # All unit tests
cargo test --package storage   # Test specific crate
cargo test test_redis_mset     # Test specific function
pytest tests/python/ -v        # Python integration tests (requires running server)

# Convenience (Makefile)
make build / make release / make run / make test / make fmt / make lint / make clean

# Cluster
cargo run --release -- --config cluster.conf --init-cluster
./scripts/start_cluster.sh / ./scripts/stop_cluster.sh / ./scripts/clean_cluster.sh
```

## Boundaries

### Constraints

- Rust nightly toolchain (pinned via `rust-toolchain.toml`)
- Custom RocksDB fork (`arana-db/rust-rocksdb`) for TablePropertiesCollector FFI
- Integration tests require running server instance
- Cluster tests require multi-node setup

### Always Do

- Read relevant files before modifying code
- Run `cargo fmt --all` and `cargo clippy` before committing
- Follow existing code patterns in the same crate
- Add tests for new functionality
- Use `Result<T, E>` for error handling -- never `.unwrap()` in production code (clippy enforced)
- Use `std::sync::LazyLock` -- never `once_cell::sync::Lazy` or `lazy_static` (clippy enforced)

### Ask First

- Modifying `conf` crate configuration structures
- Adding new dependencies to workspace `Cargo.toml`
- Changing Raft consensus logic (`raft` crate)
- Changing dual runtime architecture (`common/runtime` crate)
- Deleting or renaming public APIs
- Modifying RocksDB column family layout (`storage` crate)

### Never Do

- Use `.unwrap()` in non-test code (CI enforces `-D clippy::unwrap_used`)
- Use `dbg!()` macro (clippy warns)
- Use `once_cell::sync::Lazy` or `lazy_static!` (use `std::sync::LazyLock`)
- Hardcode paths or endpoints
- Skip license headers (Apache 2.0, enforced by SkyWalking Eyes)

## Progressive Disclosure: Detailed Guides

| Task | Reference |
|------|-----------|
| Add Redis command | `src/cmd/src/get.rs` (simple), `src/cmd/src/hset.rs` (complex) |
| Add data structure | `src/storage/src/redis_strings.rs`, `redis_hashes.rs` |
| Add Column Family | `src/storage/src/storage.rs` (CF definitions), `src/engine/` |
| Raft integration | `src/raft/src/log_store.rs`, `state_machine.rs`, `network.rs` |
| Dual Runtime | `src/common/runtime/src/` (RuntimeManager, StorageServer) |
| RESP protocol | `src/resp/src/` (nom parser) |
| Configuration | `config.example.toml`, `src/conf/` |

## Git Workflow

- **Commits**: Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`)
- **Branches**: `feat/*`, `fix/*`, `chore/*`, `docs/*`, `test/*`
- **PR title**: Must match `^(feat|fix|test|refactor|chore|upgrade|bump|style|docs|perf|build|ci|revert)(\(.*\))?:.*`
- **Pre-merge**: CI runs format check, clippy (3 platforms), build+test (3 platforms), license headers

## Extended Configuration

See `.claude/agents/`, `.claude/skills/`, `.claude/commands/`, and `.claude/rules/` for
specialized instructions.

### Agents

| Agent | Purpose | Activation Trigger |
|-------|---------|-------------------|
| `planner` | Implementation planning | Before multi-file changes, new features, architectural decisions |
| `code-verifier` | Formatting/linting/tests | After code changes, before committing |
| `code-reviewer` | Quick code quality checks | After code changes, before committing |
| `storage-expert` | RocksDB storage layer | Storage crate changes or questions |
| `raft-expert` | Raft consensus | Raft crate changes or questions |
| `net-expert` | Network/protocol layer | Net/resp/executor crate changes |
| `runtime-expert` | Dual runtime architecture | Common/runtime crate changes |
| `cmd-expert` | Redis command implementation | Cmd crate changes or new commands |

### Skills (Guided Development Workflows)

- `/add-command` - New Redis command implementation guide
- `/add-column-family` - New RocksDB column family guide
- `/add-test` - Test development guide
- `/commit-conventions` - Commit message conventions
- `/debug-runtime` - Dual runtime debugging guide

### Commands (User-invoked Actions)

- `/create-pr` - Rebase, squash commits, and create/update PR
- `/gen-commit-msg` - Generate commit messages from staged changes
- `/review-pr` - Intelligent PR code review

### Rules (Code Quality Standards)

- `code-style.md` - Coding conventions beyond clippy/rustfmt
- `rust-patterns.md` - Rust-specific patterns and idioms
- `testing.md` - Testing strategy and coverage requirements
- `distributed.md` - Raft and dual runtime patterns
