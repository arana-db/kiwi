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
//! - `BinlogBatch`: For cluster mode, writes through Raft consensus
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

#[cfg(test)]
use std::{future::Future, pin::Pin};

use rocksdb::{BoundColumnFamily, WriteBatch, WriteOptions};
use snafu::ResultExt;

use crate::ColumnFamilyIndex;
use crate::error::{Result, RocksSnafu};
use crate::options::AppendLogFunction;
use conf::raft_type::{Binlog, BinlogEntry, OperateType};
use engine::Engine;

/// Trait for batch write operations.
pub trait Batch: Send {
    fn put(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8]) -> Result<()>;
    fn commit(self: Box<Self>) -> Result<()>;
    fn count(&self) -> u32;
    fn clear(&mut self);
}

pub type CfHandles<'a> = Vec<Option<Arc<BoundColumnFamily<'a>>>>;

/// RocksDB batch implementation for standalone mode.
pub struct RocksBatch<'a> {
    inner: WriteBatch,
    db: &'a dyn Engine,
    write_options: &'a WriteOptions,
    cf_handles: CfHandles<'a>,
    count: u32,
}

impl<'a> RocksBatch<'a> {
    pub fn new(
        db: &'a dyn Engine,
        write_options: &'a WriteOptions,
        cf_handles: CfHandles<'a>,
    ) -> Self {
        assert_eq!(
            cf_handles.len(),
            ColumnFamilyIndex::COUNT,
            "cf_handles length ({}) must match ColumnFamilyIndex::COUNT ({})",
            cf_handles.len(),
            ColumnFamilyIndex::COUNT
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
                "Column family handle is None for {:?} (index {})",
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
/// Serializes operations to binlog format and commits through Raft consensus.
pub struct BinlogBatch {
    entries: Vec<BinlogEntry>,
    db_id: u32,
    slot_idx: u32,
    append_log_fn: AppendLogFunction,
    count: u32,
}

impl BinlogBatch {
    pub fn new(db_id: u32, append_log_fn: AppendLogFunction) -> Self {
        Self {
            entries: Vec::new(),
            db_id,
            slot_idx: 0,
            append_log_fn,
            count: 0,
        }
    }

    fn calculate_slot_from_key(key: &[u8]) -> u32 {
        crate::slot_indexer::key_to_slot_id(key) as u32
    }
}

impl Batch for BinlogBatch {
    fn put(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8], value: &[u8]) -> Result<()> {
        if self.entries.is_empty() {
            self.slot_idx = Self::calculate_slot_from_key(key);
        }
        self.entries.push(BinlogEntry {
            cf_idx: cf_idx as u32,
            op_type: OperateType::Put,
            key: key.to_vec(),
            value: Some(value.to_vec()),
        });
        self.count += 1;
        Ok(())
    }

    fn delete(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8]) -> Result<()> {
        if self.entries.is_empty() {
            self.slot_idx = Self::calculate_slot_from_key(key);
        }
        self.entries.push(BinlogEntry {
            cf_idx: cf_idx as u32,
            op_type: OperateType::Delete,
            key: key.to_vec(),
            value: None,
        });
        self.count += 1;
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        let binlog = Binlog {
            db_id: self.db_id,
            slot_idx: self.slot_idx,
            entries: self.entries,
        };

        let future = (self.append_log_fn)(binlog);

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| crate::error::Error::Batch {
                        message: format!("Failed to create runtime for Raft commit: {e}"),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?;
                rt.block_on(future)
            }
        }
    }

    fn count(&self) -> u32 {
        self.count
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.count = 0;
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn create_mock_append_fn() -> AppendLogFunction {
        let called = Arc::new(AtomicUsize::new(0));
        let called_clone = called.clone();

        Arc::new(move |_binlog: Binlog| {
            called_clone.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) }) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
        })
    }

    #[test]
    fn test_binlog_batch_put_and_delete() {
        let append_fn = create_mock_append_fn();

        let mut batch = BinlogBatch::new(0, append_fn);
        batch
            .put(ColumnFamilyIndex::MetaCF, b"key1", b"value1")
            .unwrap();
        batch
            .delete(ColumnFamilyIndex::HashesDataCF, b"key2")
            .unwrap();

        assert_eq!(batch.count(), 2);
    }

    #[test]
    fn test_binlog_batch_clear() {
        let append_fn = create_mock_append_fn();

        let mut batch = BinlogBatch::new(0, append_fn);
        batch
            .put(ColumnFamilyIndex::MetaCF, b"key1", b"value1")
            .unwrap();
        assert_eq!(batch.count(), 1);

        batch.clear();
        assert_eq!(batch.count(), 0);
    }
}
