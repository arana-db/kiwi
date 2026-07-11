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
//! - `BinlogBatch`: For cluster mode, accumulates operations as `BinlogEntry`
//!   values and on `commit` hands the assembled `Binlog` to an `AppendLogFn`
//!   callback that proposes it through Raft consensus.
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

use conf::raft_type::{Binlog, BinlogEntry, BinlogResponse, OperateType};
use rocksdb::{BoundColumnFamily, WriteBatch, WriteOptions};
use snafu::ResultExt;

use crate::ColumnFamilyIndex;
use crate::error::{BatchSnafu, InvalidFormatSnafu, Result, RocksSnafu};
use crate::slot_indexer::key_to_slot_id;
use crate::storage_define::{PREFIX_RESERVE_LENGTH, decode_user_key, seek_userkey_delim};
use bytes::BytesMut;
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
    /// Panics if cf_handles length doesn't match ColumnFamilyIndex::COUNT.
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
            ColumnFamilyIndex::COUNT,
            "cf_handles length ({}) must match ColumnFamilyIndex::COUNT ({}). \
             Update ColumnFamilyIndex::COUNT when adding new column families.",
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

/// Callback that proposes an assembled binlog through Raft and blocks for the result.
///
/// Returns `Err(message)` if the propose failed (not leader, timeout, etc.).
pub type AppendLogFn =
    Arc<dyn Fn(Binlog) -> std::result::Result<BinlogResponse, String> + Send + Sync>;

/// Binlog batch implementation for cluster (Raft) mode.
///
/// Accumulates put/delete operations as `BinlogEntry` values, then on `commit`
/// hands the assembled `Binlog` to `append_log_fn` (which proposes via Raft).
pub struct BinlogBatch {
    append_log_fn: AppendLogFn,
    entries: Vec<BinlogEntry>,
}

impl BinlogBatch {
    /// Create a new BinlogBatch.
    ///
    /// # Arguments
    /// * `append_log_fn` - Closure that proposes the binlog through Raft.
    pub fn new(append_log_fn: AppendLogFn) -> Self {
        Self {
            append_log_fn,
            entries: Vec::new(),
        }
    }

    pub(crate) fn infer_user_key(cf_idx: u32, encoded_key: &[u8]) -> Result<Vec<u8>> {
        if cf_idx as usize >= ColumnFamilyIndex::COUNT {
            return InvalidFormatSnafu {
                message: format!("Invalid column family index: {cf_idx}"),
            }
            .fail();
        }

        if encoded_key.len() <= PREFIX_RESERVE_LENGTH {
            return InvalidFormatSnafu {
                message: format!(
                    "Encoded key too short to infer binlog slot: len={}",
                    encoded_key.len()
                ),
            }
            .fail();
        }

        let key_part_start = PREFIX_RESERVE_LENGTH;
        let key_part = &encoded_key[key_part_start..];
        let key_part_end = seek_userkey_delim(key_part);
        if key_part_end > key_part.len() {
            return InvalidFormatSnafu {
                message: "Encoded key delimiter not found while inferring binlog slot".to_string(),
            }
            .fail();
        }

        let mut user_key = BytesMut::new();
        decode_user_key(&key_part[..key_part_end], &mut user_key)?;
        Ok(user_key.to_vec())
    }

    fn infer_slot_idx(entries: &[BinlogEntry]) -> Result<u32> {
        let mut inferred_slot = None;

        for entry in entries {
            let user_key = Self::infer_user_key(entry.cf_idx, &entry.key)?;
            let slot = key_to_slot_id(&user_key) as u32;

            match inferred_slot {
                Some(existing) if existing != slot => {
                    return BatchSnafu {
                        message: format!(
                            "CROSSSLOT binlog batch contains multiple slots: {existing} and {slot}"
                        ),
                    }
                    .fail();
                }
                Some(_) => {}
                None => inferred_slot = Some(slot),
            }
        }

        inferred_slot.ok_or_else(|| crate::error::Error::Batch {
            message: "Cannot infer slot for empty binlog batch".to_string(),
            location: snafu::Location::new(file!(), line!(), column!()),
        })
    }
}

impl Batch for BinlogBatch {
    fn put(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8], value: &[u8]) -> Result<()> {
        self.entries.push(BinlogEntry {
            cf_idx: cf_idx as u32,
            op_type: OperateType::Put,
            key: key.to_vec(),
            value: Some(value.to_vec()),
        });
        Ok(())
    }

    fn delete(&mut self, cf_idx: ColumnFamilyIndex, key: &[u8]) -> Result<()> {
        self.entries.push(BinlogEntry {
            cf_idx: cf_idx as u32,
            op_type: OperateType::Delete,
            key: key.to_vec(),
            value: None,
        });
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<()> {
        let slot_idx = Self::infer_slot_idx(&self.entries)?;
        let binlog = Binlog {
            db_id: 0, // TODO: thread real db_id from Redis/Storage in a later task
            slot_idx,
            entries: self.entries,
        };
        let resp = (self.append_log_fn)(binlog).map_err(|message| crate::error::Error::Batch {
            message,
            location: snafu::Location::new(file!(), line!(), column!()),
        })?;
        if resp.success {
            Ok(())
        } else {
            BatchSnafu {
                message: resp
                    .message
                    .unwrap_or_else(|| "raft propose failed".to_string()),
            }
            .fail()
        }
    }

    fn count(&self) -> u32 {
        self.entries.len() as u32
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::format_base_key::BaseKey;
    use crate::format_lists_data_key::ListsDataKey;
    use crate::format_member_data_key::MemberDataKey;
    use crate::format_zset_score_key::ZSetsScoreKey;
    use crate::slot_indexer::key_to_slot_id;
    use conf::raft_type::{BinlogResponse, OperateType};
    use std::sync::{Arc, Mutex};

    fn capture_append_log_fn(captured: Arc<Mutex<Option<conf::raft_type::Binlog>>>) -> AppendLogFn {
        Arc::new(move |binlog| {
            *captured.lock().unwrap() = Some(binlog);
            Ok(BinlogResponse::ok())
        })
    }

    #[test]
    fn test_binlog_batch_accumulates_entries() {
        let captured: Arc<Mutex<Option<conf::raft_type::Binlog>>> = Arc::new(Mutex::new(None));
        let append_log_fn = capture_append_log_fn(captured.clone());
        let user_key = b"k1";
        let encoded_key = BaseKey::new(user_key).encode().unwrap();

        let mut batch = BinlogBatch::new(append_log_fn);
        batch
            .put(ColumnFamilyIndex::MetaCF, &encoded_key, b"v1")
            .unwrap();
        batch
            .delete(ColumnFamilyIndex::MetaCF, &encoded_key)
            .unwrap();
        assert_eq!(batch.count(), 2);

        Box::new(batch).commit().unwrap();

        let binlog = captured.lock().unwrap().take().expect("binlog captured");
        assert_eq!(binlog.slot_idx, key_to_slot_id(user_key) as u32);
        assert_eq!(binlog.entries.len(), 2);
        assert_eq!(binlog.entries[0].cf_idx, ColumnFamilyIndex::MetaCF as u32);
        assert_eq!(binlog.entries[0].op_type, OperateType::Put);
        assert_eq!(binlog.entries[0].value, Some(b"v1".to_vec()));
        assert_eq!(binlog.entries[1].op_type, OperateType::Delete);
        assert_eq!(binlog.entries[1].value, None);
    }

    #[test]
    fn test_binlog_batch_commit_propagates_error() {
        let append_log_fn: AppendLogFn = Arc::new(|_binlog| Err("raft unavailable".to_string()));
        let mut batch = BinlogBatch::new(append_log_fn);
        let encoded_key = BaseKey::new(b"k").encode().unwrap();
        batch
            .put(ColumnFamilyIndex::MetaCF, &encoded_key, b"v")
            .unwrap();
        let result = Box::new(batch).commit();
        assert!(result.is_err());
    }

    #[test]
    fn test_binlog_batch_infers_slot_from_member_data_key() {
        let captured: Arc<Mutex<Option<conf::raft_type::Binlog>>> = Arc::new(Mutex::new(None));
        let append_log_fn = capture_append_log_fn(captured.clone());
        let user_key = b"hash_key";
        let encoded_key = MemberDataKey::new(user_key, 1, b"field").encode().unwrap();

        let mut batch = BinlogBatch::new(append_log_fn);
        batch
            .put(ColumnFamilyIndex::HashesDataCF, &encoded_key, b"value")
            .unwrap();
        Box::new(batch).commit().unwrap();

        let binlog = captured.lock().unwrap().take().expect("binlog captured");
        assert_eq!(binlog.slot_idx, key_to_slot_id(user_key) as u32);
    }

    #[test]
    fn test_binlog_batch_infers_slot_from_list_data_key() {
        let captured: Arc<Mutex<Option<conf::raft_type::Binlog>>> = Arc::new(Mutex::new(None));
        let append_log_fn = capture_append_log_fn(captured.clone());
        let user_key = b"list_key";
        let encoded_key = ListsDataKey::new(user_key, 1, 42).encode().unwrap();

        let mut batch = BinlogBatch::new(append_log_fn);
        batch
            .put(ColumnFamilyIndex::ListsDataCF, &encoded_key, b"value")
            .unwrap();
        Box::new(batch).commit().unwrap();

        let binlog = captured.lock().unwrap().take().expect("binlog captured");
        assert_eq!(binlog.slot_idx, key_to_slot_id(user_key) as u32);
    }

    #[test]
    fn test_binlog_batch_infers_slot_from_zset_score_key() {
        let captured: Arc<Mutex<Option<conf::raft_type::Binlog>>> = Arc::new(Mutex::new(None));
        let append_log_fn = capture_append_log_fn(captured.clone());
        let user_key = b"zset_key";
        let encoded_key = ZSetsScoreKey::new(user_key, 1, 2.5, b"member")
            .encode()
            .unwrap();

        let mut batch = BinlogBatch::new(append_log_fn);
        batch
            .put(ColumnFamilyIndex::ZsetsScoreCF, &encoded_key, b"")
            .unwrap();
        Box::new(batch).commit().unwrap();

        let binlog = captured.lock().unwrap().take().expect("binlog captured");
        assert_eq!(binlog.slot_idx, key_to_slot_id(user_key) as u32);
    }

    #[test]
    fn test_binlog_batch_rejects_cross_slot_entries() {
        let append_log_fn: AppendLogFn = Arc::new(|_binlog| Ok(BinlogResponse::ok()));
        let first_user_key = b"key_one";
        let first_slot = key_to_slot_id(first_user_key);
        let second_user_key = (0..1000)
            .map(|index| format!("key_two_{index}"))
            .find(|key| key_to_slot_id(key.as_bytes()) != first_slot)
            .expect("test keys should include a different slot");

        let first_key = BaseKey::new(first_user_key).encode().unwrap();
        let second_key = BaseKey::new(second_user_key.as_bytes()).encode().unwrap();

        let mut batch = BinlogBatch::new(append_log_fn);
        batch
            .put(ColumnFamilyIndex::MetaCF, &first_key, b"v1")
            .unwrap();
        batch
            .put(ColumnFamilyIndex::MetaCF, &second_key, b"v2")
            .unwrap();

        let result = Box::new(batch).commit();
        assert!(result.is_err());
        assert!(format!("{:?}", result.unwrap_err()).contains("CROSSSLOT"));
    }
}
