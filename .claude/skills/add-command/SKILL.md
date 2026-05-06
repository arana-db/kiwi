---
name: add-command
description: Step-by-step guide for implementing new Redis commands in Kiwi.
---

# Add Redis Command

Guide for adding a new Redis command to Kiwi's `cmd` crate.

## Prerequisites

- Understand the command's Redis specification (https://redis.io/commands/)
- Know which data structure it operates on (string, hash, list, set, zset)

## Step 1: Create Command File

Create `src/cmd/src/<command_name>.rs` following the existing pattern:

```rust
// Reference: src/cmd/src/get.rs (simple) or src/cmd/src/set.rs (complex)

use std::sync::Arc;
use client::Client;
use resp::RespData;
use storage::storage::Storage;
use crate::{Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

pub struct <CommandName>Cmd {
    meta: CmdMeta,
}

impl <CommandName>Cmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "<command_name>".to_string(),
                arity: <arity>,
                flags: CmdFlags::WRITE, // or READONLY
                ..Default::default()
            },
        }
    }
}

impl Cmd for <CommandName>Cmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        // Validate args from client.argv(), set key on client
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        // Call storage method, set reply via client.set_reply()
    }
}
```

## Step 2: Add Module Declaration

In `src/cmd/src/lib.rs`, add:

```rust
pub mod <command_name>;
```

## Step 3: Register in Command Table

In `src/cmd/src/table.rs`, locate `create_command_table()` and register your command via `register_cmd!`:

```rust
// Add your <CommandName>Cmd to the appropriate section (strings/hashes/lists/etc.)
register_cmd!("<command_name>", <CommandName>Cmd);
```

## Step 4: Implement Argument Validation (do_initial)

- Implement `do_initial(&self, client: &Client) -> bool`
- Validate argument count from `client.argv()`
- Set key/fields on client via `client.set_key()`
- Return `false` to abort if arguments are invalid

## Step 5: Implement Execution (do_cmd)

- Implement `do_cmd(&self, client: &Client, storage: Arc<Storage>)`
- Call the appropriate storage method
- Handle all error cases (wrong type, key not found, etc.)
- Set reply on client via `client.set_reply(RespData::...)`

## Step 6: Add Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid() {
        // Test valid argument parsing
    }

    #[test]
    fn test_parse_invalid() {
        // Test invalid argument handling
    }

    #[test]
    fn test_parse_wrong_arity() {
        // Test wrong number of arguments
    }
}
```

## Step 7: Verify

```bash
cargo fmt --all
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
cargo test --package cmd
```

## Reference Commands

| Complexity | Command | File |
|------------|---------|------|
| Simple | GET | `src/cmd/src/get.rs` |
| With options | SET | `src/cmd/src/set.rs` |
| Multi-key | DEL | `src/cmd/src/del.rs` |
| Hash | HSET | `src/cmd/src/hset.rs` |
| List | LPUSH | `src/cmd/src/lpush.rs` |
| Set | SADD | `src/cmd/src/sadd.rs` |
| Sorted set | ZADD | `src/cmd/src/zadd.rs` |
