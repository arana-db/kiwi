// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Batch abstraction for atomic write operations.
//!
//! This module provides a unified interface for batch operations that can work
//! in both standalone and cluster (Raft) modes.
//!
//! # Design
//!
//! The batch system is designed with two implementations:
//! - `RocksBatch`: For standalone mode, directly writes to RocksDB
//! - `BinlogBatch`: For cluster mode, writes through Raft consensus (TODO)
//!
//! # Usage
//!
//! ```ignore
//! let mut batch = redis.create_batch()?;
//! batch.put(ColumnFamilyIndex::MetaCF, key, value)?;
//! batch.delete(ColumnFamilyIndex::HashesDataCF, key)?;
//! batch.commit()?;
//! ```

use std::sync::Arc;

use rocksdb::{BoundColumnFamily, WriteBatch, WriteOptions};
use snafu::ResultExt;

use crate::ColumnFamilyIndex;
use crate::error::{BatchSnafu, Result, RocksSnafu};
use engine::Engine;

/// Trait for batch write operations.
///
/// This trait abstracts the batch write mechanism to support both standalone
/// (RocksDB direct write) and cluster (Raft consensus) modes.
///
/// # Error Handling
///
/// All operations return `Result<()>` to properly propagate errors instead of
/// panicking. This is important for production stability in storage systems.
pub trait Batch: Send {
    /// Add a put operation to the batch.
    ///
    /// # Arguments
    /// * `cf_idx` - The column family index to write to
    /// * `key` - The key to write
    /// * `value` - The value to write
    ///
    /// # Errors
    /// Returns an error if the column family index is invalid.
    fn put(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8], value: &[u8]) -> Result<()>;

    /// Add a delete operation to the batch.
    ///
    /// # Arguments
    /// * `cf_idx` - The column family index to delete from
    /// * `key` - The key to delete
    ///
    /// # Errors
    /// Returns an error if the column family index is invalid.
    fn delete(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8]) -> Result<()>;

    /// Commit all operations in the batch atomically.
    ///
    /// # Returns
    /// * `Ok(())` - if all operations were committed successfully
    /// * `Err(_)` - if the commit failed
    fn commit(self: Box<Self>) -> Result<()>;

    /// Get the number of operations in the batch.
    fn count(&self) -> u32;

    /// Clear all operations from the batch.
    fn clear(&mut self);
}

/// Type alias for column family handles used in batch operations.
pub type CfHandles<'a> = Vec<Option<Arc<BoundColumnFamily<'a>>>>;

/// Expected number of column families.
/// This must match the number of variants in ColumnFamilyIndex enum.
/// Update this constant when adding new column families.
pub const EXPECTED_CF_COUNT: usize = 6;

/// RocksDB batch implementation for standalone mode.
///
/// This implementation directly uses RocksDB's WriteBatch for atomic writes.
pub struct RocksBatch<'a> {
    inner: WriteBatch,
    db: &'a dyn Engine,
    write_options: &'a WriteOptions,
    cf_handles: CfHandles<'a>,
    count: u32,
}

impl<'a> RocksBatch<'a> {
    /// Create a new RocksBatch.
    ///
    /// # Arguments
    /// * `db` - Reference to the database engine
    /// * `write_options` - Write options for the batch commit
    /// * `cf_handles` - Column family handles for all column families
    ///
    /// # Panics
    /// Panics if cf_handles length doesn't match EXPECTED_CF_COUNT.
    /// This is a programming error that should be caught during development.
    pub fn new(
        db: &'a dyn Engine,
        write_options: &'a WriteOptions,
        cf_handles: CfHandles<'a>,
    ) -> Self {
        // Validate cf_handles length matches expected column family count.
        // This catches mismatches between ColumnFamilyIndex enum and cf_handles vec
        // at batch creation time rather than during put/delete operations.
        assert_eq!(
            cf_handles.len(),
            EXPECTED_CF_COUNT,
            "cf_handles length ({}) must match EXPECTED_CF_COUNT ({}). \
             Update EXPECTED_CF_COUNT when adding new column families.",
            cf_handles.len(),
            EXPECTED_CF_COUNT
        );

        Self {
            inner: WriteBatch::default(),
            db,
            write_options,
            cf_handles,
            count: 0,
        }
    }
}

/// Convert ColumnFamilyIndex to its corresponding array index.
///
/// This function uses an explicit match to ensure compile-time safety.
/// When a new ColumnFamilyIndex variant is added, the compiler will
/// require this match to be updated.
#[inline]
fn cf_index_to_usize(cf_idx: ColumnFamilyIndex) -> usize {
    match cf_idx {
        ColumnFamilyIndex::MetaCF => 0,
        ColumnFamilyIndex::HashesDataCF => 1,
        ColumnFamilyIndex::SetsDataCF => 2,
        ColumnFamilyIndex::ListsDataCF => 3,
        ColumnFamilyIndex::ZsetsDataCF => 4,
        ColumnFamilyIndex::ZsetsScoreCF => 5,
    }
}

/// Get the column family handle from the handles vector.
///
/// This function provides validated access to column family handles,
/// ensuring the handle exists at the given index.
///
/// # Arguments
/// * `cf_handles` - The vector of column family handles
/// * `cf_idx` - The column family index to look up
///
/// # Returns
/// A reference to the column family handle, or an error if invalid.
fn get_cf_handle<'a>(
    cf_handles: &'a CfHandles<'a>,
    cf_idx: ColumnFamilyIndex,
) -> Result<&'a Arc<BoundColumnFamily<'a>>> {
    let idx = cf_index_to_usize(cf_idx);

    cf_handles
        .get(idx)
        .and_then(|opt| opt.as_ref())
        .ok_or_else(|| crate::error::Error::Batch {
            message: format!(
                "Column family handle is None for {:?} (index {}) - \
                 this indicates a bug in initialization",
                cf_idx, idx
            ),
            location: snafu::Location::new(file!(), line!(), column!()),
        })
}

impl<'a> Batch for RocksBatch<'a> {
    fn put(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = get_cf_handle(&self.cf_handles, cf_idx)?;
        self.inner.put_cf(cf, key, value);
        self.count += 1;
        Ok(())
    }

    fn delete(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8]) -> Result<()> {
        let cf = get_cf_handle(&self.cf_handles, cf_idx)?;
        self.inner.delete_cf(cf, key);
        self.count += 1;
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<()> {
        self.db
            .write_opt(self.inner, self.write_options)
            .context(RocksSnafu)
    }

    fn count(&self) -> u32 {
        self.count
    }

    fn clear(&mut self) {
        self.inner.clear();
        self.count = 0;
    }
}

/// Binlog batch implementation for cluster (Raft) mode.
///
/// This implementation serializes operations to a binlog format and commits
/// through the Raft consensus layer.
///
/// TODO: Implement when Raft integration is ready.
#[allow(dead_code)]
pub struct BinlogBatch {
    // TODO: Add binlog entries
    // entries: Vec<BinlogEntry>,
    // append_log_fn: AppendLogFunction,
    count: u32,
}

#[allow(dead_code)]
impl BinlogBatch {
    /// Create a new BinlogBatch.
    ///
    /// # Arguments
    /// * `append_log_fn` - Function to append log entries to Raft
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Default for BinlogBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Batch for BinlogBatch {
    fn put(&mut self, _cf_idx: ColumnFamilyIndex, _key: &[u8], _value: &[u8]) -> Result<()> {
        // TODO: Implement when Raft integration is ready
        // Create binlog entry and add to entries
        self.count += 1;
        Ok(())
    }

    fn delete(&mut self, _cf_idx: ColumnFamilyIndex, _key: &[u8]) -> Result<()> {
        // TODO: Implement when Raft integration is ready
        // Create binlog entry and add to entries
        self.count += 1;
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<()> {
        // BinlogBatch commit is not yet implemented.
        // Return an error to prevent silent data loss.
        BatchSnafu {
            message: "BinlogBatch commit is not implemented - Raft integration pending".to_string(),
        }
        .fail()
    }

    fn count(&self) -> u32 {
        self.count
    }

    fn clear(&mut self) {
        // TODO: Clear entries
        self.count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binlog_batch_default() {
        let batch = BinlogBatch::default();
        assert_eq!(batch.count(), 0);
    }

    #[test]
    fn test_binlog_batch_commit_returns_error() {
        let batch = BinlogBatch::default();
        let result = Box::new(batch).commit();
        assert!(result.is_err());
    }
}
