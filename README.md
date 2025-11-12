# Kiwi

## Introduction

Kiwi is a Redis-compatible key-value database built in Rust, leveraging RocksDB for storage and the Raft consensus algorithm to achieve strong consistency, high performance, and scalability.

## Features

- **Dual Runtime Architecture**: Separate network and storage runtimes for optimal performance isolation
- **RocksDB Backend**: Uses RocksDB as the backend persistent storage
- **Redis Protocol Compatible**: Highly compatible with the Redis protocol
- **High Performance**: Optimized request handling with dedicated thread pools
- **Async Communication**: Message channel-based communication between network and storage layers
- **Fault Isolation**: Network and storage operations run in isolated runtimes
- **Performance Benchmarking**: Supports performance benchmarking using Redis tools
- **Modular Design**: Planned modular support for custom extensions

## System Requirements

- Operating System: Linux, macOS, FreeBSD, windows
- Rust toolchain

## Installation Guide

Please ensure that the Rust toolchain is installed, which can be done using [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Architecture

Kiwi uses a **dual runtime architecture** to separate concerns and optimize performance:

### Network Runtime
- Handles all network I/O operations
- Processes Redis protocol parsing
- Manages client connections
- Optimized for I/O-bound operations

### Storage Runtime
- Executes all storage operations (RocksDB interactions)
- Processes storage commands from the network runtime
- Optimized for CPU-intensive operations
- Isolated from network latency

### Communication
- **MessageChannel**: Async message passing between runtimes
- **StorageClient**: Network runtime sends requests to storage runtime
- **StorageServer**: Storage runtime processes requests and returns responses
- **Backpressure Handling**: Automatic flow control to prevent overload
- **Circuit Breaker**: Fault tolerance for storage failures

This architecture provides:
- **Performance Isolation**: Network issues don't block storage operations
- **Fault Isolation**: Failures in one runtime don't crash the other
- **Scalability**: Independent thread pool sizing for network and storage
- **Observability**: Separate health monitoring and metrics per runtime

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Configuration

Kiwi uses a Redis-style configuration file located at `src/conf/kiwi.conf`. Key configuration options:

- **Port**: Default is `7379` (configurable via `port` setting, Redis-compatible variant of `6379`)
- **Memory**: Maximum memory usage (supports units: B, KB, MB, GB, TB)
- **Runtime Settings**: Thread pool sizes for network and storage runtimes
- **RocksDB settings**: Various tuning parameters for the storage engine

Example configuration:
```conf
# Server port
port 7379

# Maximum memory
memory 1GB

# Runtime configuration (auto-detected based on CPU cores)
# network-threads 4
# storage-threads 4

# RocksDB configuration
rocksdb-max-background-jobs 4
rocksdb-write-buffer-size 67108864
```

For a complete list of configuration options, see `src/conf/kiwi.conf`.

### Migration from Previous Versions

**Important**: If you are upgrading from a version that used port `9221`, note that the default port has changed to `7379`. Please update:
- Client connection strings
- Firewall rules
- Container port mappings

Port `7379` was chosen to indicate Redis compatibility while avoiding conflicts with the standard Redis port (`6379`).

See [CHANGELOG.md](CHANGELOG.md) for detailed migration instructions.

## Building and Running

### Build from Source

```bash
# Clone the repository
git clone https://github.com/arana-db/kiwi.git
cd kiwi

# Build the project
cargo build --release

# Run the server
./target/release/kiwi
```

### Command Line Options

```bash
# Use custom configuration file
kiwi --config /path/to/kiwi.conf

# Force single-node mode (disable cluster)
kiwi --single-node

# Initialize a new cluster (first node only)
kiwi --init-cluster
```

## Raft Consensus Integration

Kiwi integrates the **Openraft** library to provide distributed consensus and high availability:

- **Adaptor Pattern**: Custom adaptor layer to bridge Kiwi's storage with Openraft's sealed traits
- **RocksDB-backed Raft Log**: Persistent Raft log storage using RocksDB
- **State Machine Replication**: Consistent state replication across cluster nodes
- **Snapshot Support**: Efficient state transfer for new or lagging nodes

For detailed integration documentation, see [src/raft/docs/OPENRAFT_INTEGRATION.md](src/raft/docs/OPENRAFT_INTEGRATION.md).

## Development Plan

- ✅ Dual runtime architecture for performance isolation
- ✅ Message channel-based async communication
- ✅ Basic Redis command support (GET, SET, DEL, etc.)
- ✅ Openraft integration with Adaptor pattern
- 🚧 Support most Redis commands
- 🚧 Complete cluster mode implementation
- 🚧 Extend command support and optimize command execution efficiency
- 🚧 Enhance modular extension features and provide examples
- 🚧 Improve development documentation and user guides
- 🚧 Add comprehensive metrics and monitoring

## Contribution

Contributions to the Kiwi project are welcome! If you have any suggestions or have found any issues, please submit an Issue or create a Pull Request.

## Contact Us
