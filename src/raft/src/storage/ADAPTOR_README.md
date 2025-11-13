# RaftStorageAdaptor Implementation

## Overview

This module implements the `RaftStorageAdaptor` which bridges our custom `RaftStorage` implementation with OpenRaft's sealed traits using the Adaptor pattern.

## Components

### RaftStorageAdaptor

The main adaptor struct that wraps:
- `RaftStorage` - Our custom storage implementation using RocksDB
- `KiwiStateMachine` - The state machine for applying log entries

### Implemented Traits

1. **RaftStorage** - Core storage operations
   - `save_vote` / `read_vote` - Persist and retrieve voting information
   - `get_log_state` - Get the current log state
   - `append_to_log` - Append new log entries
   - `delete_conflict_logs_since` - Remove conflicting logs
   - `purge_logs_upto` - Clean up old logs
   - `last_applied_state` - Get the last applied state
   - `apply_to_state_machine` - Apply log entries to the state machine
   - `begin_receiving_snapshot` / `install_snapshot` / `get_current_snapshot` - Snapshot operations

2. **RaftLogReader** - Read log entries
   - `try_get_log_entries` - Read log entries in a range

3. **RaftSnapshotBuilder** - Build snapshots
   - `build_snapshot` - Create a snapshot of the current state

## Usage

```rust
use std::sync::Arc;
use crate::storage::{RaftStorage, create_raft_storage_adaptor};
use crate::state_machine::KiwiStateMachine;

// Create storage and state machine
let storage = Arc::new(RaftStorage::new("./data")?);
let state_machine = Arc::new(KiwiStateMachine::new(1));

// Create the adaptor
let (log_storage, state_machine) = create_raft_storage_adaptor(storage, state_machine);

// Use with OpenRaft
let raft = Raft::new(node_id, config, network, log_storage, state_machine).await?;
```

## Key Features

- **Persistent Storage**: Uses RocksDB for durable log and state storage
- **Snapshot Support**: Full snapshot creation and restoration
- **Efficient Log Operations**: Batch operations for better performance
- **Type Safety**: Leverages Rust's type system for correctness

## Implementation Notes

- Log entries are serialized using bincode
- The adaptor handles conversion between our internal types and OpenRaft types
- Snapshot metadata is cached for performance
- All async operations use tokio runtime

## Testing

Tests are located in `adaptor_tests.rs` and cover:
- Basic adaptor creation
- Vote persistence and retrieval
- Log operations
- Snapshot operations

Run tests with:
```bash
cargo test -p raft --lib storage::adaptor_tests
```
