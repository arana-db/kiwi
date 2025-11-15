# Kiwi

## Introduction

Kiwi is a Redis-compatible key-value database built in Rust, providing high capacity, high performance, and strong consistency through RocksDB and the Raft protocol.

## Features

- **Dual Runtime Architecture**: Network and storage runtimes are separated for performance isolation
- **RocksDB Backend**: Uses RocksDB as the persistent storage backend
- **Redis Protocol Compatibility**: Highly compatible with Redis protocol
- **Raft Consensus Algorithm**: Integrates OpenRaft for strong consistency and high availability
- **Adaptor Pattern**: Custom adapter layer bridging storage with OpenRaft
- **High Performance**: Optimized request processing with dedicated thread pools
- **Asynchronous Communication**: Message channel-based asynchronous communication
- **Fault Isolation**: Network and storage operations run in isolated runtimes

## System Requirements

- Operating System: Linux, macOS, FreeBSD, or Windows
- Rust toolchain

## Installation

Make sure you have the Rust toolchain installed. You can install it using [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Quick Start

- Run `kiwi-server` (defaults to `127.0.0.1:7379`)
- For cluster mode, refer to `config.example.toml` or `cluster.conf` in the repository root, and use the `--init-cluster` flag when starting the first node

## Key Components

- **Raft Network Handler**: `src/net/src/raft_network_handle.rs`
- **Router**: `src/raft/src/router.rs`
- **Raft Node**: `src/raft/src/node.rs`
- **Storage Backend**: `src/storage/src` and `src/raft/src/storage_engine/redis_storage_engine.rs`

For more details, see `docs/CONSISTENCY_README.md`.

## Raft Consensus Integration

Kiwi integrates the **OpenRaft** library to provide distributed consensus and high availability:

- **Adaptor Pattern**: Custom adapter layer bridging Kiwi storage with OpenRaft's sealed traits
- **RocksDB-based Raft Log**: Uses RocksDB to persist Raft logs
- **State Machine Replication**: Consistent state replication across cluster nodes
- **Snapshot Support**: Efficient state transfer for new or lagging nodes

For detailed integration documentation, see [docs/raft/OPENRAFT_INTEGRATION.md](docs/raft/OPENRAFT_INTEGRATION.md).

## Development Roadmap

- ✅ Dual runtime architecture for performance isolation
- ✅ Message channel-based asynchronous communication
- ✅ Basic Redis command support (GET, SET, DEL, etc.)
- ✅ OpenRaft integration using Adaptor pattern
- 🚧 Support for most Redis commands
- 🚧 Complete cluster mode implementation
- 🚧 Extended command support and command execution optimization
- 🚧 Enhanced modular extension capabilities with examples
- 🚧 Comprehensive development documentation and user guides
- 🚧 Comprehensive metrics and monitoring

## Contributing

Contributions to the Kiwi project are welcome! If you have any suggestions or find issues, please submit an Issue or create a Pull Request.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
