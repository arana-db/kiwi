# Kiwi

English | [简体中文](README_CN.md)

## Introduction

Kiwi is a Rust database targeting the observable behavior of Redis 8.8.1. RocksDB is the complete authoritative storage layer, and OpenRaft provides strong consistency and high availability.

The compatibility and interface-design baseline is pinned to Redis tag `8.8.1`, commit `77b6c308396c9700672390a210143a8496fb4b10`. Current required work runs Cache OFF and focuses on compatibility, authoritative recovery, Raft correctness, and system stability. The Embedded Redis Hot Tier is a future design boundary and is not authorized for implementation until the stability gate passes and a separate approval is given.

## Features

- **Dual Runtime Architecture**: Network and storage runtimes are separated for performance isolation
- **RocksDB Authority**: RocksDB stores the complete, durable, recoverable data set
- **Redis 8.8.1 Compatibility Target**: Protocol, commands, errors, TTL, transactions, and client behavior must be verified against an exact upstream baseline
- **Stability-First Delivery**: Compatibility, RocksDB close/reopen recovery, and single-group OpenRaft correctness must pass the system stability gate before deferred acceleration work is reconsidered
- **Raft Consensus**: Integrates OpenRaft for strong consistency and high availability
- **Adaptor Pattern**: Custom adapter layer bridging storage with OpenRaft
- **High Performance**: Optimized request processing with dedicated thread pools
- **Asynchronous Communication**: Message channel-based asynchronous communication
- **Fault Isolation**: Network and storage operations run in isolated runtimes

## Architecture

```text
src/server/    → Entry point (main.rs)
src/net/       → TCP server, connection handling, cluster routing
src/cmd/       → Command definitions: Cmd trait, CmdMeta, command table
src/executor/  → Command executor: tokio async task pool
src/storage/   → Multi-instance RocksDB ownership, column families, TTL
src/resp/      → RESP protocol: parser, encoder, RespData types
src/raft/      → Raft consensus: OpenRaft integration, RocksDB log store, state machine, router
src/conf/      → Configuration: loading, validation, RaftClusterConfig
src/client/    → Client context: connection state, argv, reply buffer
src/common/runtime/ → Runtime management: async channel between net & storage
src/common/macro/   → Proc macros: #[stack_trace_debug]
src/kstd/      → Utilities: LockMgr (sharded key-level locking)
```

### Request Flow

```text
Client → TCP accept [network runtime] → RESP parse → Command lookup
  → CmdExecutor [network runtime] → Cmd.execute() calls StorageClient
    → MessageChannel →
  → StorageServer [storage runtime] → RocksDB
    ← oneshot response ←
  → RESP encode [network runtime] → write back to client
```

## Roadmap

The authoritative north star, requirements, milestones, current state, and Kanban are maintained in:

- [Project constitution](.planning/PROJECT.md)
- [Requirements](.planning/REQUIREMENTS.md)
- [Roadmap](.planning/ROADMAP.md)
- [Current state](.planning/STATE.md)
- [Kanban](.planning/KANBAN.md)

README checklists are intentionally not used as a second roadmap.

## Getting Started

### Prerequisites

Kiwi's normal development, CI, and release baseline is Rust 1.97.1 stable. After
you clone the repository, `rust-toolchain.toml` makes rustup select that exact
toolchain automatically. All Kiwi workspace crates use Rust 2024 Edition.

`protoc` and the native C/C++ tools needed to build RocksDB are also required.
On Windows, use the Rust MSVC target and install the Visual Studio C++ build
tools. On Linux and macOS, install the platform C/C++ build dependencies used by
the project in addition to `protoc`. See the platform-specific commands in the
[development guide](docs/development.md#prerequisites).

```bash
# Install rustup; rust-toolchain.toml selects Rust 1.97.1 stable in this repo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# macOS: Xcode CLT + protobuf compiler + CMake (required to build RocksDB from source)
xcode-select --install
brew install protobuf cmake

# Debian/Ubuntu Linux: native C/C++ toolchain + protobuf compiler
sudo apt install clang cmake libclang-dev llvm-dev pkg-config protobuf-compiler
```

On Windows, use the same official Protobuf 27.1 release as CI. Download and
extract `protoc-27.1-win64.zip`, add its `bin` directory to `PATH`, and run
`protoc --version`. See the [development guide](docs/development.md#prerequisites)
for the PowerShell commands.

Dated nightly toolchains are reserved for specialized checks such as
Sanitizers. They do not define the normal development or release baseline.

### Get the Code

```bash
git clone https://github.com/arana-db/kiwi.git
cd kiwi

# Verify the compiler selected by this checkout
rustup show active-toolchain
rustc --version --verbose
```

### Standalone Mode

```bash
make standalone                    # builds (release) and starts kiwi on 127.0.0.1:7379

# In another terminal:
redis-cli -p 7379 set foo bar     # OK
redis-cli -p 7379 get foo         # "bar"
```

### Cluster Mode

```bash
make cluster                       # start a 3-node Raft cluster (default)
make cluster NODES=5               # start a 5-node cluster
```

See [docs/cluster.md](docs/cluster.md) for the manual step-by-step procedure and Raft architecture details.

## Documentation

| Document | Description |
|----------|-------------|
| [docs/development.md](docs/development.md) | Dev environment, build optimization, sccache |
| [docs/cluster.md](docs/cluster.md) | Raft cluster quickstart and write-path verification |
| [docs/key-encoding.md](docs/key-encoding.md) | Key encoding internals (Chinese) |
| [Redis 8.8.1 compatibility contract](docs/compatibility/redis-8.8.1.md) | Exact Oracle, raw RESP, TCL, and client-test boundaries |
| [Redis 8.8.1 system boundaries](docs/architecture/redis-8.8.1-system-boundaries.md) | Cache OFF request, storage, and consensus boundaries |
| [System stability gate](docs/quality/system-stability-gate.md) | Required evidence before deferred hot-tier work can be reconsidered |
| [Deferred native ABI contract](docs/architecture/redis-hot-tier-native-abi.md) | Future interface design; not an implementation authorization |
| [Combined distribution licensing](docs/architecture/combined-distribution-licensing.md) | Future Redis-derived library and source-distribution obligations |
| [Product requirements (PRD)](docs/prd.md) | Goals, scope, and Redis 8.8.1 compatibility target |
| [Engineering quality gates](docs/quality/quality-gates.md) | Code, test, and release quality gates |
| [Personas and user stories](docs/personas-and-user-stories.md) | Target users and usage scenarios |
| [Documentation index](docs/INDEX.md) | Map of the whole `docs/` tree and suggested reading order |
| [Design plans & specs](docs/superpowers/) | Dated design records (plans + specs) |
| `kiwi --sample-config > kiwi.conf` | Write the default configuration to `kiwi.conf` |
| `kiwi --full-sample-config > kiwi.conf` | Write all available configuration keys to `kiwi.conf` |

## Dependencies

### RocksDB (Arana-maintained Fork)

Kiwi uses the [Arana-maintained rust-rocksdb fork](https://github.com/arana-db/rust-rocksdb) to provide the TableProperties Collector/Factory FFI required by Storage LogIndex. The dependency uses the maintenance release tag [`v0.51.0-arana.2`](https://github.com/arana-db/rust-rocksdb/tree/v0.51.0-arana.2). Published release tags must not be moved, and `Cargo.lock` records the exact resolved commit for auditable and reproducible builds.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Kiwi-authored source is licensed under the Apache License 2.0. See [LICENSE](LICENSE). Future official distributions that include the Redis-derived native library must satisfy the applicable AGPL-3.0-only obligations described in the architecture and [third-party notices](THIRD_PARTY_NOTICES.md).
