# Kiwi

English | [简体中文](README_CN.md)

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

### Building from Source

```bash
# Clone the repository
git clone https://github.com/arana-db/kiwi.git
cd kiwi

# Quick check (fast, recommended for development)
cargo check

# Build the project
cargo build --release

# Run the server (defaults to 127.0.0.1:7379)
cargo run --release
```

### Using Development Scripts

For faster development workflow, use the provided scripts:

**Linux/macOS:**
```bash
# Make scripts executable (first time only)
chmod +x scripts/*.sh

# Quick check (fastest)
./scripts/dev.sh check

# Build and run
./scripts/dev.sh run

# Auto-watch mode (checks on file save)
./scripts/dev.sh watch
```

**Windows:**
```cmd
# Quick check (fastest)
scripts\dev check

# Build and run
scripts\dev run

# Auto-watch mode
scripts\dev watch
```

### Cluster Mode

For cluster mode, refer to `config.example.toml` or `cluster.conf` in the repository root, and use the `--init-cluster` flag when starting the first node:

```bash
cargo run --release -- --config cluster.conf --init-cluster
```

### Recommended Development Workflow

#### 🚀 First-time Setup (Automatic Prompt)

When you first run `build` or `run`, the script will automatically prompt you to install sccache and cargo-watch:

```bash
⚠️  First-time setup recommended for optimal performance!
Run quick setup now? (y/n)
```

**Press 'y'** to install automatically, or run manually:

```bash
# Linux/macOS:
chmod +x scripts/quick_setup.sh
./scripts/quick_setup.sh

# Windows:
scripts\quick_setup.cmd
```

After setup, builds will be **50-90% faster** on subsequent runs!

#### 📝 Daily Development

The development scripts **automatically use sccache** if installed:

```bash
# Linux/macOS:
./scripts/dev.sh check   # Quick check (5-10x faster than build)
./scripts/dev.sh build   # Build (automatically uses sccache)
./scripts/dev.sh run     # Run (automatically uses sccache)
./scripts/dev.sh watch   # Auto-check on file save

# Windows:
scripts\dev check        # Quick check
scripts\dev build        # Build (automatically uses sccache)
scripts\dev run          # Run (automatically uses sccache)
scripts\dev watch        # Auto-check on file save
```

**Performance Tips:**
- Use `check` for fast syntax checking during development (5-10x faster)
- Use `watch` for automatic checking on file save (instant feedback)
- Use `build` or `run` only when you need to execute the program
- Scripts automatically use sccache if installed (no manual configuration needed)

**Why is compilation slow?** See [Why Recompiling?](docs/WHY_RECOMPILING.md) for diagnosis and solutions.

For detailed build optimization guide, see [docs/BUILD_OPTIMIZATION.md](docs/BUILD_OPTIMIZATION.md).

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

## Documentation

- [Quick Start Guide](docs/QUICK_START.md) - Detailed getting started guide
- [Build Optimization](docs/BUILD_OPTIMIZATION.md) - How to speed up compilation
- [Scripts Usage](scripts/README.md) - Development scripts documentation
- [中文文档](docs/编译加速方案总结.md) - Chinese documentation

## Contributing

Contributions to the Kiwi project are welcome! If you have any suggestions or find issues, please submit an Issue or create a Pull Request.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
