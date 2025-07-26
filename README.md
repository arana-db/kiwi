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

- Operating System: Linux, macOS, or FreeBSD
- Rust toolchain

## Installation Guide

Please ensure that the Rust toolchain is installed, which can be done using [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Development Plan

- Support most Redis commands
- Add support for cluster mode
- Extend command support and optimize command execution efficiency
- Enhance modular extension features and provide examples
- Improve development documentation and user guides

## Contribution

Contributions to the Kiwi project are welcome! If you have any suggestions or have found any issues, please submit an Issue or create a Pull Request.

## Contact Us
