---
name: add-column-family
description: Step-by-step guide for adding new RocksDB column families to Kiwi.
---

# Add Column Family

Guide for adding a new RocksDB column family to Kiwi's storage engine.

## Prerequisites

- Understand why a new CF is needed (new data type, separate index, etc.)
- Know the key format and value format for the new data

## Step 1: Define CF Name

In `src/storage/src/storage.rs`, add the CF name constant:

```rust
pub const NEW_DATA_CF: &str = "new_data_cf";
```

## Step 2: Add CF to Open List

In the same file, add the new CF to the list of column families opened when the database
starts. Follow the existing pattern for `hash_data_cf`, `set_data_cf`, etc.

## Step 3: Define Key Format

Create `src/storage/src/<new>_key_format.rs`:

```rust
// Reference: src/storage/src/base_key_format.rs

// Define key encoding/decoding
// Keys must be:
// - Deterministic (same input -> same bytes)
// - Comparable (for iteration/range scans)
// - Efficient (minimal overhead)
```

## Step 4: Define Value Format

Create `src/storage/src/<new>_value_format.rs` if needed:

```rust
// Reference: src/storage/src/base_value_format.rs
// or src/storage/src/base_meta_value_format.rs for metadata
```

## Step 5: Implement Operations

Create `src/storage/src/redis_<new>.rs`:

```rust
// Reference: src/storage/src/redis_strings.rs

// Implement CRUD operations for the new data type
// Use WriteBatch for multi-key atomic operations
// Always use CF handles, never default CF
```

## Step 6: Add Compaction Filter (if needed)

If the new CF needs custom compaction (TTL cleanup, etc.):

- Create or modify `src/storage/src/data_compaction_filter.rs`
- Register the filter for the new CF

## Step 7: Register Module

In `src/storage/src/lib.rs`, add:

```rust
pub mod <new>_key_format;
pub mod <new>_value_format;
pub mod redis_<new>;
```

## Step 8: Add Tests

```rust
#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    #[test]
    fn test_cf_basic_operations() {
        // Create temp DB with new CF
        // Test put/get/delete
        // Test iteration
    }
}
```

## Step 9: Verify

```bash
cargo fmt --all
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
cargo test --package storage
```

## Important Notes

- Adding a CF is a **storage format change** -- may affect Raft snapshots
- Update `src/raft/src/cf_tracker.rs` if snapshot needs to include the new CF
- Test with existing data to ensure backward compatibility
