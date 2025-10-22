# Kiwi

## Introduction

Kiwi is a Redis-compatible key-value database built in Rust, leveraging RocksDB for storage and the Raft consensus algorithm to achieve strong consistency, high performance, and scalability.

## Features

- Uses RocksDB as the backend persistent storage.
- Highly compatible with the Redis protocol.
- Supports performance benchmarking using Redis.
- Planned modular support, allowing developers to customize extensions.
- Provides high-performance request handling capabilities.

## System Requirements

- Operating System: Linux, macOS, FreeBSD, windows
- Rust toolchain

## Installation Guide

Please ensure that the Rust toolchain is installed, which can be done using [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Configuration

Kiwi uses a Redis-style configuration file located at `src/conf/kiwi.conf`. Key configuration options:

- **Port**: Default is `7379` (configurable via `port` setting, Redis-compatible variant of `6379`)
- **Memory**: Maximum memory usage (supports units: B, KB, MB, GB, TB)
- **RocksDB settings**: Various tuning parameters for the storage engine

Example configuration:
```conf
# Server port
port 7379

# Maximum memory
memory 1GB

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

## Development Plan

- Support most Redis commands
- Add support for cluster mode
- Extend command support and optimize command execution efficiency
- Enhance modular extension features and provide examples
- Improve development documentation and user guides

## Contribution

Contributions to the Kiwi project are welcome! If you have any suggestions or have found any issues, please submit an Issue or create a Pull Request.

## Contact Us
