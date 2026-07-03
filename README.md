# Kiwi

English | [简体中文](README_CN.md)

## Introduction

Kiwi is a Redis-compatible key-value database built in Rust, providing high capacity and strong consistency through RocksDB and the Raft protocol.

## Features

- **Dual Runtime Architecture**: Network and storage runtimes are separated for performance isolation
- **RocksDB Backend**: Uses RocksDB as the persistent storage backend
- **Redis Protocol Compatibility**: Highly compatible with Redis protocol
- **Raft Consensus**: Integrates OpenRaft for strong consistency and high availability
- **Adaptor Pattern**: Custom adapter layer bridging storage with OpenRaft
- **High Performance**: Optimized request processing with dedicated thread pools
- **Asynchronous Communication**: Message channel-based asynchronous communication
- **Fault Isolation**: Network and storage operations run in isolated runtimes

## Architecture

```
src/server/    → Entry point (main.rs)
src/net/       → TCP server, connection handling, cluster routing
src/cmd/       → Command definitions: Cmd trait, CmdMeta, command table
src/executor/  → Command executor: tokio async task pool
src/storage/   → Multi-instance RocksDB, column families, TTL
src/engine/    → Engine trait abstraction over RocksDB
src/resp/      → RESP protocol: parser, encoder, RespData types
src/raft/      → Raft consensus: OpenRaft integration, state machine, router
src/conf/      → Configuration: loading, validation, RaftClusterConfig
src/client/    → Client context: connection state, argv, reply buffer
src/common/runtime/ → Runtime management: async channel between net & storage
src/common/macro/   → Proc macros: #[stack_trace_debug]
src/kstd/      → Utilities: LockMgr (sharded key-level locking)
```

### Request Flow

```
Client → TCP accept [network runtime] → RESP parse → Command lookup
  → CmdExecutor [network runtime] → Cmd.execute() calls StorageClient
    → MessageChannel →
  → StorageServer [storage runtime] → RocksDB
    ← oneshot response ←
  → RESP encode [network runtime] → write back to client
```

## Roadmap

- ✅ Dual-runtime architecture for performance isolation
- ✅ Message channel-based async communication
- ✅ Basic Redis commands (GET, SET, DEL, etc.)
- ✅ OpenRaft integration via adaptor pattern
- 🚧 Most Redis commands
- 🚧 Complete cluster mode
- 🚧 Extended command support and execution optimization
- 🚧 Module extension capabilities
- 🚧 Comprehensive metrics and monitoring

## Getting Started

### Prerequisites

```bash
# Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# protobuf compiler (macOS)
brew install protobuf

# protobuf compiler (Linux)
apt install protobuf-compiler
```

### Get the Code

```bash
git clone https://github.com/arana-db/kiwi.git
cd kiwi
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
| [CLAUDE.md](CLAUDE.md) | Build commands and architecture overview |
| `kiwi --sample-config` | Generate a default config file |
| `kiwi --full-sample-config` | Generate a config with all available keys |

## Dependencies

### RocksDB (Temporary Fork)

This project uses a [customized fork of rust-rocksdb](https://github.com/arana-db/rust-rocksdb/tree/addtableproperties) because the official crate does not yet support the TablePropertiesCollector FFI functions required by the Raft module. Once the needed functionality lands in upstream [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb), we will switch back.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0. See [LICENSE](LICENSE).
