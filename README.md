# Kiwi-rs

## Introduction

Kiwi-rs is an enhanced Redis server implemented in Rust, aiming to provide high-performance and persistent key-value storage. The current project is still in the early stages, with features continually being expanded and improved.

## Features

- Uses RocksDB as the backend persistent storage.
- Highly compatible with the Redis protocol, supporting most Redis commands.
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

- Add support for cluster mode
- Extend command support and optimize command execution efficiency
- Enhance modular extension features and provide examples
- Improve development documentation and user guides

## Contribution

Contributions to the Kiwi-RS project are welcome! If you have any suggestions or have found any issues, please submit an Issue or create a Pull Request.

## Contact Us
