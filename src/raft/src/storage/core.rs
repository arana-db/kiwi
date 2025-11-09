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

//! Raft storage layer implementation using RocksDB

use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::{ColumnFamilyDescriptor, DB, Options, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::error::{RaftError, StorageError};
use crate::types::{LogIndex, NodeId, Term};

/// Column family names for Raft storage
const CF_LOG: &str = "raft_log";
const CF_STATE: &str = "raft_state";
const CF_SNAPSHOT: &str = "raft_snapshot";

/// Keys for storing Raft state
const KEY_CURRENT_TERM: &str = "current_term";
const KEY_VOTED_FOR: &str = "voted_for";
const KEY_LAST_APPLIED: &str = "last_applied";
const KEY_SNAPSHOT_META: &str = "snapshot_meta";

/// Raft storage implementation using RocksDB
/// 
/// # Locking Strategy
/// 
/// This implementation uses `parking_lot::RwLock` for the cached Raft state to allow:
/// - Concurrent reads of state (term, voted_for, last_applied) without blocking
/// - Exclusive writes when state needs to be updated
/// 
/// RocksDB itself is thread-safe and handles its own internal locking, so we don't
/// need additional locks for database operations. The RwLock is only for the in-memory
/// cache to avoid frequent database reads.
/// 
/// # Caching Strategy
/// 
/// This implementation caches frequently accessed data in memory:
/// - Raft state (term, voted_for, last_applied) - cached in `state` field
/// - Snapshot metadata - cached in `snapshot_meta_cache` field
/// 
/// This reduces RocksDB reads and improves performance for hot paths.
pub struct RaftStorage {
    /// RocksDB instance (thread-safe, handles its own locking)
    pub db: Arc<DB>,
    /// Current Raft state (cached for performance, protected by RwLock for concurrent reads)
    state: Arc<RwLock<RaftState>>,
    /// Cached snapshot metadata (reduces RocksDB reads)
    snapshot_meta_cache: Arc<RwLock<Option<StoredSnapshotMeta>>>,
}

/// Raft persistent state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftState {
    /// Current term
    current_term: Term,
    /// Node voted for in current term
    voted_for: Option<NodeId>,
    /// Last applied log index
    last_applied: LogIndex,
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            last_applied: 0,
        }
    }
}

/// Log entry for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredLogEntry {
    /// Log index
    pub index: LogIndex,
    /// Term when entry was created
    pub term: Term,
    /// Entry payload (serialized Redis command)
    pub payload: Vec<u8>,
}

/// Snapshot metadata for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshotMeta {
    /// Last log index included in snapshot
    pub last_log_index: LogIndex,
    /// Last log term included in snapshot
    pub last_log_term: Term,
    /// Snapshot ID
    pub snapshot_id: String,
    /// Timestamp when snapshot was created
    pub timestamp: i64,
}

impl RaftStorage {
    /// Helper function to get column family handle
    pub fn get_cf_handle(
        &self,
        cf_name: &str,
    ) -> Result<std::sync::Arc<rocksdb::BoundColumnFamily<'_>>, RaftError> {
        self.db.cf_handle(cf_name).ok_or_else(|| {
            log::error!("Column family {} not found in RocksDB", cf_name);
            RaftError::Storage(StorageError::DataInconsistency { 
                message: format!("Column family {} not found", cf_name),
                context: format!("get_cf_handle({})", cf_name),
            })
        })
    }

    /// Create a new Raft storage instance
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self, RaftError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Define column families
        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_LOG, Options::default()),
            ColumnFamilyDescriptor::new(CF_STATE, Options::default()),
            ColumnFamilyDescriptor::new(CF_SNAPSHOT, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, db_path, cfs)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?;

        let db = Arc::new(db);
        let state = Arc::new(RwLock::new(RaftState::default()));
        let snapshot_meta_cache = Arc::new(RwLock::new(None));

        let storage = Self {
            db,
            state,
            snapshot_meta_cache,
        };

        // Load existing state and snapshot metadata into cache
        storage.load_state()?;
        storage.load_snapshot_meta_cache()?;

        Ok(storage)
    }

    /// Load snapshot metadata into cache
    fn load_snapshot_meta_cache(&self) -> Result<(), RaftError> {
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        if let Some(meta_data) = self
            .db
            .get_cf(&cf_snapshot, KEY_SNAPSHOT_META)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?
        {
            let meta: StoredSnapshotMeta = bincode::deserialize(&meta_data).map_err(|e| {
                RaftError::Storage(StorageError::SnapshotRestorationFailed { message: format!("Failed to deserialize snapshot metadata: {}", e), snapshot_id: String::from("unknown"), context: String::from("load_snapshot_meta_cache"),
                })
            })?;

            let mut cache = self.snapshot_meta_cache.write();
            *cache = Some(meta);
        }

        Ok(())
    }

    /// Load Raft state from persistent storage
    fn load_state(&self) -> Result<(), RaftError> {
        let cf_state = self.get_cf_handle(CF_STATE)?;

        // Load current term
        let current_term = if let Some(data) = self
            .db
            .get_cf(&cf_state, KEY_CURRENT_TERM)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?
        {
            bincode::deserialize(&data).map_err(|e| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to deserialize current term: {}", e), context: String::new(),
                })
            })?
        } else {
            0
        };

        // Load voted for
        let voted_for = if let Some(data) = self
            .db
            .get_cf(&cf_state, KEY_VOTED_FOR)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?
        {
            bincode::deserialize(&data).map_err(|e| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to deserialize voted_for: {}", e), context: String::new(),
                })
            })?
        } else {
            None
        };

        // Load last applied
        let last_applied = if let Some(data) = self
            .db
            .get_cf(&cf_state, KEY_LAST_APPLIED)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?
        {
            bincode::deserialize(&data).map_err(|e| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to deserialize last_applied: {}", e), context: String::new(),
                })
            })?
        } else {
            0
        };

        // Update cached state
        let mut state = self.state.write();
        state.current_term = current_term;
        state.voted_for = voted_for;
        state.last_applied = last_applied;

        Ok(())
    }

    /// Async wrapper for get_log_entry to avoid blocking
    async fn get_log_entry_async(&self, index: LogIndex) -> Result<Option<StoredLogEntry>, RaftError> {
        let start = std::time::Instant::now();
        log::trace!("get_log_entry_async: index={}", index);
        
        let db = self.db.clone();
        let cf_name = CF_LOG.to_string();
        
        let result = tokio::task::spawn_blocking(move || {
            let block_start = std::time::Instant::now();
            let cf_log = db.cf_handle(&cf_name).ok_or_else(|| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Column family {} not found", cf_name), context: String::new(),
                })
            })?;

            let key = RaftStorage::log_key(index);
            let db_read_start = std::time::Instant::now();
            if let Some(value) = db
                .get_cf(&cf_log, key)
                .map_err(|e| RaftError::Storage(StorageError::from(e)))?
            {
                let db_read_elapsed = db_read_start.elapsed();
                log::trace!("RocksDB read for index {}: {:?}", index, db_read_elapsed);
                
                let deserialize_start = std::time::Instant::now();
                let entry: StoredLogEntry = bincode::deserialize(&value).map_err(|e| {
                    RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to deserialize log entry: {}", e), context: String::new(),
                    })
                })?;
                let deserialize_elapsed = deserialize_start.elapsed();
                log::trace!("Deserialization for index {}: {:?}, entry_size={} bytes", 
                    index, deserialize_elapsed, value.len());
                
                let block_elapsed = block_start.elapsed();
                log::trace!("Blocking task for index {}: {:?}", index, block_elapsed);
                Ok(Some(entry))
            } else {
                let db_read_elapsed = db_read_start.elapsed();
                log::trace!("RocksDB read for index {} (not found): {:?}", index, db_read_elapsed);
                Ok(None)
            }
        })
        .await
        .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { message: format!("Task join error: {}", e), context: String::new(),
        }))?;
        
        let elapsed = start.elapsed();
        log::trace!("get_log_entry_async: index={}, found={}, total_duration={:?}", 
            index, result.is_ok() && result.as_ref().unwrap().is_some(), elapsed);
        
        result
    }

    /// Async wrapper for get_last_log_entry to avoid blocking
    async fn get_last_log_entry_async(&self) -> Result<Option<StoredLogEntry>, RaftError> {
        let db = self.db.clone();
        let cf_name = CF_LOG.to_string();
        
        tokio::task::spawn_blocking(move || {
            let cf_log = db.cf_handle(&cf_name).ok_or_else(|| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Column family {} not found", cf_name), context: String::new(),
                })
            })?;

            let mut iter = db.iterator_cf(&cf_log, rocksdb::IteratorMode::End);

            if let Some(result) = iter.next() {
                let (_key, value) = result.map_err(|e| RaftError::Storage(StorageError::from(e)))?;
                let entry: StoredLogEntry = bincode::deserialize(&value).map_err(|e| {
                    RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to deserialize log entry: {}", e), context: String::new(),
                    })
                })?;
                Ok(Some(entry))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { message: format!("Task join error: {}", e), context: String::new(),
        }))?
    }

    /// Async wrapper for store_snapshot_meta to avoid blocking
    /// Updates both persistent storage and in-memory cache
    async fn store_snapshot_meta_async(&self, meta: &StoredSnapshotMeta) -> Result<(), RaftError> {
        let start = std::time::Instant::now();
        log::trace!("store_snapshot_meta_async: snapshot_id={}, index={}", 
            meta.snapshot_id, meta.last_log_index);
        
        let db = self.db.clone();
        let cf_name = CF_SNAPSHOT.to_string();
        
        let serialize_start = std::time::Instant::now();
        let meta_data = bincode::serialize(meta).map_err(|e| {
            RaftError::Storage(StorageError::SnapshotCreationFailed { message: format!("Failed to serialize snapshot metadata: {}", e), snapshot_id: String::from("unknown"), context: String::from("store_snapshot_meta_async"),
            })
        })?;
        let serialize_elapsed = serialize_start.elapsed();
        log::trace!("Snapshot metadata serialization: {:?}, size={} bytes", 
            serialize_elapsed, meta_data.len());

        let result: Result<(), RaftError> = tokio::task::spawn_blocking(move || {
            let block_start = std::time::Instant::now();
            let cf_snapshot = db.cf_handle(&cf_name).ok_or_else(|| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Column family {} not found", cf_name), context: String::new(),
                })
            })?;

            let write_start = std::time::Instant::now();
            db.put_cf(&cf_snapshot, KEY_SNAPSHOT_META, meta_data)
                .map_err(|e| RaftError::Storage(StorageError::from(e)))?;
            let write_elapsed = write_start.elapsed();
            log::trace!("RocksDB snapshot metadata write: {:?}", write_elapsed);
            
            let block_elapsed = block_start.elapsed();
            log::trace!("Blocking task for snapshot metadata: {:?}", block_elapsed);
            Ok(())
        })
        .await
        .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { message: format!("Task join error: {}", e), context: String::new(),
        }))?;

        result?;

        // Update cache after successful write
        let cache_start = std::time::Instant::now();
        let mut cache = self.snapshot_meta_cache.write();
        *cache = Some(meta.clone());
        let cache_elapsed = cache_start.elapsed();
        log::trace!("Snapshot metadata cache update: {:?}", cache_elapsed);

        let elapsed = start.elapsed();
        log::trace!("store_snapshot_meta_async: total_duration={:?}", elapsed);
        Ok(())
    }

    /// Async wrapper for store_snapshot_data to avoid blocking
    async fn store_snapshot_data_async(&self, snapshot_id: &str, data: &[u8]) -> Result<(), RaftError> {
        let start = std::time::Instant::now();
        log::trace!("store_snapshot_data_async: snapshot_id={}, data_size={} bytes", 
            snapshot_id, data.len());
        
        let db = self.db.clone();
        let cf_name = CF_SNAPSHOT.to_string();
        let snapshot_id = snapshot_id.to_string();
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            let block_start = std::time::Instant::now();
            let cf_snapshot = db.cf_handle(&cf_name).ok_or_else(|| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Column family {} not found", cf_name), context: String::new(),
                })
            })?;

            let write_start = std::time::Instant::now();
            db.put_cf(&cf_snapshot, snapshot_id.clone(), data.clone())
                .map_err(|e| RaftError::Storage(StorageError::from(e)))?;
            let write_elapsed = write_start.elapsed();
            log::trace!("RocksDB snapshot data write for {}: {:?}, size={} bytes", 
                snapshot_id, write_elapsed, data.len());
            
            let block_elapsed = block_start.elapsed();
            log::trace!("Blocking task for snapshot data: {:?}", block_elapsed);
            Ok::<(), RaftError>(())
        })
        .await
        .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { message: format!("Task join error: {}", e), context: String::from("store_snapshot_data_async"),
        }))?;
        
        let elapsed = start.elapsed();
        log::trace!("store_snapshot_data_async: total_duration={:?}", elapsed);
        Ok(())
    }

    /// Save Raft state to persistent storage
    pub fn save_state(&self, state: &RaftState) -> Result<(), RaftError> {
        let cf_state = self.get_cf_handle(CF_STATE)?;

        let mut batch = WriteBatch::default();

        // Serialize and save current term
        let term_data = bincode::serialize(&state.current_term).map_err(|e| {
            RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to serialize current term: {}", e), context: String::new(),
            })
        })?;
        batch.put_cf(&cf_state, KEY_CURRENT_TERM, term_data);

        // Serialize and save voted for
        let voted_for_data = bincode::serialize(&state.voted_for).map_err(|e| {
            RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to serialize voted_for: {}", e), context: String::new(),
            })
        })?;
        batch.put_cf(&cf_state, KEY_VOTED_FOR, voted_for_data);

        // Serialize and save last applied
        let last_applied_data = bincode::serialize(&state.last_applied).map_err(|e| {
            RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to serialize last_applied: {}", e), context: String::new(),
            })
        })?;
        batch.put_cf(&cf_state, KEY_LAST_APPLIED, last_applied_data);

        // Write batch atomically
        self.db
            .write(batch)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?;

        Ok(())
    }

    /// Get current term
    pub fn get_current_term(&self) -> Term {
        self.state.read().current_term
    }

    /// Set current term
    pub fn set_current_term(&self, term: Term) -> Result<(), RaftError> {
        let mut state = self.state.write();
        state.current_term = term;
        self.save_state(&state)
    }

    /// Get voted for
    pub fn get_voted_for(&self) -> Option<NodeId> {
        self.state.read().voted_for
    }

    /// Set voted for
    pub fn set_voted_for(&self, node_id: Option<NodeId>) -> Result<(), RaftError> {
        let mut state = self.state.write();
        state.voted_for = node_id;
        self.save_state(&state)
    }

    /// Get last applied index
    pub fn get_last_applied(&self) -> LogIndex {
        self.state.read().last_applied
    }

    /// Set last applied index
    pub fn set_last_applied(&self, index: LogIndex) -> Result<(), RaftError> {
        let mut state = self.state.write();
        state.last_applied = index;
        self.save_state(&state)
    }

    /// Create log entry key for RocksDB
    pub fn log_key(index: LogIndex) -> Vec<u8> {
        // Use big-endian encoding for proper ordering
        index.to_be_bytes().to_vec()
    }

    /// Parse log entry key from RocksDB
    pub fn parse_log_key(key: &[u8]) -> Result<LogIndex, RaftError> {
        if key.len() != 8 {
            return Err(RaftError::Storage(StorageError::DataInconsistency { message: format!("Invalid log key length: {}", key.len()),
            }));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(key);
        Ok(LogIndex::from_be_bytes(bytes))
    }

    /// Append log entry
    pub fn append_log_entry(&self, entry: &StoredLogEntry) -> Result<(), RaftError> {
        let cf_log = self.get_cf_handle(CF_LOG)?;

        let key = Self::log_key(entry.index);
        let value = bincode::serialize(entry).map_err(|e| {
            RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to serialize log entry: {}", e), context: String::new(),
            })
        })?;

        self.db
            .put_cf(&cf_log, key, value)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?;

        Ok(())
    }

    /// Get log entry by index
    pub fn get_log_entry(&self, index: LogIndex) -> Result<Option<StoredLogEntry>, RaftError> {
        let cf_log = self.get_cf_handle(CF_LOG)?;

        let key = Self::log_key(index);
        if let Some(value) = self
            .db
            .get_cf(&cf_log, key)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?
        {
            let entry: StoredLogEntry = bincode::deserialize(&value).map_err(|e| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to deserialize log entry: {}", e), context: String::new(),
                })
            })?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    /// Get the last log entry
    pub fn get_last_log_entry(&self) -> Result<Option<StoredLogEntry>, RaftError> {
        let cf_log = self.get_cf_handle(CF_LOG)?;

        let mut iter = self.db.iterator_cf(&cf_log, rocksdb::IteratorMode::End);

        if let Some(result) = iter.next() {
            let (_key, value) = result.map_err(|e| RaftError::Storage(StorageError::from(e)))?;
            let entry: StoredLogEntry = bincode::deserialize(&value).map_err(|e| {
                RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to deserialize log entry: {}", e), context: String::new(),
                })
            })?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    /// Delete log entries from index onwards
    pub fn delete_log_entries_from(&self, from_index: LogIndex) -> Result<(), RaftError> {
        let cf_log = self.get_cf_handle(CF_LOG)?;

        let mut batch = WriteBatch::default();
        let start_key = Self::log_key(from_index);
        let iter = self.db.iterator_cf(
            &cf_log,
            rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        for result in iter {
            let (key, _) = result.map_err(|e| RaftError::Storage(StorageError::from(e)))?;
            batch.delete_cf(&cf_log, key);
        }

        self.db
            .write(batch)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?;

        Ok(())
    }

    /// Store snapshot metadata
    /// Updates both persistent storage and in-memory cache
    pub fn store_snapshot_meta(&self, meta: &StoredSnapshotMeta) -> Result<(), RaftError> {
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        let meta_data = bincode::serialize(meta).map_err(|e| {
            RaftError::Storage(StorageError::SnapshotCreationFailed { message: format!("Failed to serialize snapshot metadata: {}", e), snapshot_id: String::from("unknown"), context: String::from("store_snapshot_meta"),
            })
        })?;

        self.db
            .put_cf(&cf_snapshot, KEY_SNAPSHOT_META, meta_data)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?;

        // Update cache after successful write
        let mut cache = self.snapshot_meta_cache.write();
        *cache = Some(meta.clone());

        Ok(())
    }

    /// Get snapshot metadata
    /// Returns cached value if available, otherwise reads from storage
    pub fn get_snapshot_meta(&self) -> Result<Option<StoredSnapshotMeta>, RaftError> {
        // Try cache first (fast path)
        {
            let cache = self.snapshot_meta_cache.read();
            if cache.is_some() {
                return Ok(cache.clone());
            }
        }

        // Cache miss - read from storage (slow path)
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        if let Some(meta_data) = self
            .db
            .get_cf(&cf_snapshot, KEY_SNAPSHOT_META)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?
        {
            let meta: StoredSnapshotMeta = bincode::deserialize(&meta_data).map_err(|e| {
                RaftError::Storage(StorageError::SnapshotRestorationFailed { message: format!("Failed to deserialize snapshot metadata: {}", e), snapshot_id: String::from("unknown"), context: String::from("get_snapshot_meta"),
                })
            })?;

            // Update cache for future reads
            let mut cache = self.snapshot_meta_cache.write();
            *cache = Some(meta.clone());

            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    /// Store snapshot data
    pub fn store_snapshot_data(&self, snapshot_id: &str, data: &[u8]) -> Result<(), RaftError> {
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        self.db
            .put_cf(&cf_snapshot, snapshot_id, data)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?;

        Ok(())
    }

    /// Get snapshot data
    pub fn get_snapshot_data(&self, snapshot_id: &str) -> Result<Option<Vec<u8>>, RaftError> {
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        let data = self
            .db
            .get_cf(&cf_snapshot, snapshot_id)
            .map_err(|e| RaftError::Storage(StorageError::from(e)))?;

        Ok(data)
    }
}

// ============================================================================
// Openraft RaftLogReader Implementation
// ============================================================================

use openraft::storage::{RaftLogReader, RaftSnapshotBuilder};
use openraft::{Entry, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError as OpenraftStorageError, StoredMembership};
use std::io::Cursor;
use std::ops::RangeBounds;

use crate::types::{ClientRequest, TypeConfig};

/// Convert RaftError to Openraft StorageError
pub(crate) fn to_storage_error(err: RaftError) -> OpenraftStorageError<crate::types::NodeId> {
    use openraft::{ErrorSubject, ErrorVerb, StorageIOError};
    
    OpenraftStorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::Store,
            ErrorVerb::Read,
            openraft::AnyError::new(&err),
        ),
    }
}

// Implement RaftLogReader for Arc<RaftStorage> to allow shared ownership
impl RaftLogReader<TypeConfig> for Arc<RaftStorage> {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, OpenraftStorageError<crate::types::NodeId>> 
    where
        RB: RangeBounds<u64> + Clone + Send,
    {
        // Delegate to the inner RaftStorage implementation
        let start_index = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end_index = match range.end_bound() {
            std::ops::Bound::Included(&n) => n + 1,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        log::debug!(
            "Reading log entries from {} to {}",
            start_index,
            end_index
        );

        let mut entries = Vec::new();

        // Use async wrapper to avoid blocking the async runtime
        for index in start_index..end_index {
            match self.get_log_entry_async(index).await.map_err(to_storage_error)? {
                Some(stored_entry) => {
                    let entry = convert_stored_entry_to_openraft_entry(stored_entry)
                        .map_err(to_storage_error)?;
                    entries.push(entry);
                }
                None => {
                    break;
                }
            }
        }

        log::debug!("Read {} log entries", entries.len());
        Ok(entries)
    }
}

impl RaftLogReader<TypeConfig> for RaftStorage {
    /// Read log entries from storage within the specified range
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, OpenraftStorageError<crate::types::NodeId>> 
    where
        RB: RangeBounds<u64> + Clone + Send,
    {
        let start = std::time::Instant::now();
        
        // Determine the start and end indices from the range bounds
        let start_index = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end_index = match range.end_bound() {
            std::ops::Bound::Included(&n) => n + 1,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        log::debug!(
            "Reading log entries from {} to {}",
            start_index,
            end_index
        );
        log::trace!("try_get_log_entries: range=[{}, {})", start_index, end_index);

        let mut entries = Vec::new();
        let mut total_bytes = 0usize;

        // Iterate through the range and collect entries using async wrapper
        for index in start_index..end_index {
            match self.get_log_entry_async(index).await.map_err(to_storage_error)? {
                Some(stored_entry) => {
                    total_bytes += stored_entry.payload.len();
                    // Convert StoredLogEntry to Openraft Entry
                    let entry = convert_stored_entry_to_openraft_entry(stored_entry)
                        .map_err(to_storage_error)?;
                    entries.push(entry);
                }
                None => {
                    // No more entries, stop iteration
                    log::trace!("No more entries at index {}, stopping iteration", index);
                    break;
                }
            }
        }

        let elapsed = start.elapsed();
        log::debug!("Read {} log entries in {:?}", entries.len(), elapsed);
        log::trace!("try_get_log_entries: count={}, total_bytes={}, duration={:?}, throughput={:.2} MB/s", 
            entries.len(), total_bytes, elapsed,
            if elapsed.as_secs_f64() > 0.0 {
                (total_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64()
            } else {
                0.0
            });
        Ok(entries)
    }
}

/// Convert StoredLogEntry to Openraft Entry
fn convert_stored_entry_to_openraft_entry(
    stored: StoredLogEntry,
) -> Result<Entry<TypeConfig>, RaftError> {
    // Deserialize the payload to ClientRequest
    let client_request: ClientRequest = bincode::deserialize(&stored.payload).map_err(|e| {
        RaftError::Storage(StorageError::DataInconsistency { message: format!("Failed to deserialize log entry payload: {}", e), context: String::new(),
        })
    })?;

    // Create LogId
    let log_id = LogId::new(
        openraft::CommittedLeaderId::new(stored.term, 0), // TODO: Get actual leader_id
        stored.index,
    );

    // Create Entry with Normal payload
    let entry = Entry {
        log_id,
        payload: EntryPayload::Normal(client_request),
    };

    Ok(entry)
}

// ============================================================================
// Openraft RaftSnapshotBuilder Implementation
// ============================================================================

// Implement RaftSnapshotBuilder for Arc<RaftStorage>
impl RaftSnapshotBuilder<TypeConfig> for Arc<RaftStorage> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, OpenraftStorageError<crate::types::NodeId>> {
        log::info!("Building snapshot");

        // Use async wrapper to avoid blocking
        let last_log_entry = self
            .get_last_log_entry_async()
            .await
            .map_err(to_storage_error)?;

        let (last_log_index, last_log_term) = if let Some(entry) = last_log_entry {
            (entry.index, entry.term)
        } else {
            (0, 0)
        };

        log::debug!(
            "Snapshot will include up to log index {} (term {})",
            last_log_index,
            last_log_term
        );

        let snapshot_id = format!("snapshot-{}-{}", last_log_term, last_log_index);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let stored_meta = StoredSnapshotMeta {
            last_log_index,
            last_log_term,
            snapshot_id: snapshot_id.clone(),
            timestamp,
        };

        let snapshot_data = Vec::new();

        // Use async wrappers to avoid blocking
        self.store_snapshot_meta_async(&stored_meta)
            .await
            .map_err(to_storage_error)?;

        self.store_snapshot_data_async(&snapshot_id, &snapshot_data)
            .await
            .map_err(to_storage_error)?;

        log::info!(
            "Successfully built snapshot {} at index {} (term {})",
            snapshot_id,
            last_log_index,
            last_log_term
        );

        let last_log_id = if last_log_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(last_log_term, 0),
                last_log_index,
            ))
        } else {
            None
        };

        let snapshot_meta = SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::default(),
            snapshot_id: snapshot_id.clone(),
        };

        let snapshot = Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        };

        Ok(snapshot)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RaftStorage {
    /// Build a snapshot of the current state
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, OpenraftStorageError<crate::types::NodeId>> {
        log::info!("Building snapshot");

        // Get the last log entry to determine snapshot metadata using async wrapper
        let last_log_entry = self
            .get_last_log_entry_async()
            .await
            .map_err(to_storage_error)?;

        let (last_log_index, last_log_term) = if let Some(entry) = last_log_entry {
            (entry.index, entry.term)
        } else {
            // No log entries yet, use defaults
            (0, 0)
        };

        log::debug!(
            "Snapshot will include up to log index {} (term {})",
            last_log_index,
            last_log_term
        );

        // Create snapshot metadata
        let snapshot_id = format!("snapshot-{}-{}", last_log_term, last_log_index);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let stored_meta = StoredSnapshotMeta {
            last_log_index,
            last_log_term,
            snapshot_id: snapshot_id.clone(),
            timestamp,
        };

        // Create snapshot data by iterating through all log entries
        // In a real implementation, this would snapshot the state machine data
        // For now, we'll create an empty snapshot as a placeholder
        let snapshot_data = Vec::new();

        // Store the snapshot metadata using async wrapper
        self.store_snapshot_meta_async(&stored_meta)
            .await
            .map_err(to_storage_error)?;

        // Store the snapshot data using async wrapper
        self.store_snapshot_data_async(&snapshot_id, &snapshot_data)
            .await
            .map_err(to_storage_error)?;

        log::info!(
            "Successfully built snapshot {} at index {} (term {})",
            snapshot_id,
            last_log_index,
            last_log_term
        );

        // Create the LogId for the snapshot
        let last_log_id = if last_log_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(last_log_term, 0), // TODO: Get actual leader_id
                last_log_index,
            ))
        } else {
            None
        };

        // Create snapshot metadata for Openraft
        let snapshot_meta = SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::default(),
            snapshot_id: snapshot_id.clone(),
        };

        // Create the snapshot with data as Cursor<Vec<u8>>
        let snapshot = Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        };

        Ok(snapshot)
    }
}

#[cfg(test)]
mod raft_log_reader_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_try_get_log_entries_empty() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = RaftStorage::new(temp_dir.path()).unwrap();

        let entries = storage.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_try_get_log_entries_with_data() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Add some log entries
        for i in 0..5 {
            let request = ClientRequest {
                id: crate::types::RequestId::new(),
                command: crate::types::RedisCommand::new(
                    "SET".to_string(),
                    vec![
                        bytes::Bytes::from(format!("key{}", i)),
                        bytes::Bytes::from(format!("value{}", i)),
                    ],
                ),
                consistency_level: crate::types::ConsistencyLevel::Linearizable,
            };

            let payload = bincode::serialize(&request).unwrap();
            let entry = StoredLogEntry {
                index: i,
                term: 1,
                payload,
            };

            storage.append_log_entry(&entry).unwrap();
        }

        // Read all entries
        let entries = storage.try_get_log_entries(0..5).await.unwrap();
        assert_eq!(entries.len(), 5);

        // Read partial range
        let entries = storage.try_get_log_entries(2..4).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id.index, 2);
        assert_eq!(entries[1].log_id.index, 3);
    }

    #[tokio::test]
    async fn test_try_get_log_entries_unbounded() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Add some log entries
        for i in 0..3 {
            let request = ClientRequest {
                id: crate::types::RequestId::new(),
                command: crate::types::RedisCommand::new(
                    "GET".to_string(),
                    vec![bytes::Bytes::from(format!("key{}", i))],
                ),
                consistency_level: crate::types::ConsistencyLevel::Linearizable,
            };

            let payload = bincode::serialize(&request).unwrap();
            let entry = StoredLogEntry {
                index: i,
                term: 1,
                payload,
            };

            storage.append_log_entry(&entry).unwrap();
        }

        // Read from start
        let entries = storage.try_get_log_entries(1..).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id.index, 1);
    }
}

#[cfg(test)]
mod raft_snapshot_builder_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_build_snapshot_empty() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Build snapshot with no log entries
        let snapshot = storage.build_snapshot().await.unwrap();

        // Verify snapshot metadata
        assert!(snapshot.meta.last_log_id.is_none());
        assert_eq!(snapshot.meta.snapshot_id, "snapshot-0-0");

        // Verify snapshot data is empty
        let snapshot_data = snapshot.snapshot.into_inner();
        assert_eq!(snapshot_data.len(), 0);
    }

    #[tokio::test]
    async fn test_build_snapshot_with_logs() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Add some log entries
        for i in 0..5 {
            let request = ClientRequest {
                id: crate::types::RequestId::new(),
                command: crate::types::RedisCommand::new(
                    "SET".to_string(),
                    vec![
                        bytes::Bytes::from(format!("key{}", i)),
                        bytes::Bytes::from(format!("value{}", i)),
                    ],
                ),
                consistency_level: crate::types::ConsistencyLevel::Linearizable,
            };

            let payload = bincode::serialize(&request).unwrap();
            let entry = StoredLogEntry {
                index: i,
                term: 2,
                payload,
            };

            storage.append_log_entry(&entry).unwrap();
        }

        // Build snapshot
        let snapshot = storage.build_snapshot().await.unwrap();

        // Verify snapshot metadata
        assert!(snapshot.meta.last_log_id.is_some());
        let last_log_id = snapshot.meta.last_log_id.unwrap();
        assert_eq!(last_log_id.index, 4); // Last index is 4 (0-4)
        assert_eq!(last_log_id.leader_id.term, 2);
        assert_eq!(snapshot.meta.snapshot_id, "snapshot-2-4");
    }

    #[tokio::test]
    async fn test_build_snapshot_metadata_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Add a log entry
        let request = ClientRequest {
            id: crate::types::RequestId::new(),
            command: crate::types::RedisCommand::new(
                "SET".to_string(),
                vec![
                    bytes::Bytes::from("test_key"),
                    bytes::Bytes::from("test_value"),
                ],
            ),
            consistency_level: crate::types::ConsistencyLevel::Linearizable,
        };

        let payload = bincode::serialize(&request).unwrap();
        let entry = StoredLogEntry {
            index: 10,
            term: 3,
            payload,
        };

        storage.append_log_entry(&entry).unwrap();

        // Build snapshot
        let snapshot = storage.build_snapshot().await.unwrap();
        let snapshot_id = snapshot.meta.snapshot_id.clone();

        // Verify metadata was persisted
        let stored_meta = storage.get_snapshot_meta().unwrap();
        assert!(stored_meta.is_some());
        let stored_meta = stored_meta.unwrap();
        assert_eq!(stored_meta.snapshot_id, snapshot_id);
        assert_eq!(stored_meta.last_log_index, 10);
        assert_eq!(stored_meta.last_log_term, 3);

        // Verify snapshot data was persisted
        let stored_data = storage.get_snapshot_data(&snapshot_id).unwrap();
        assert!(stored_data.is_some());
    }

    #[tokio::test]
    async fn test_build_snapshot_serialization() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Add log entries
        for i in 0..3 {
            let request = ClientRequest {
                id: crate::types::RequestId::new(),
                command: crate::types::RedisCommand::new(
                    "DEL".to_string(),
                    vec![bytes::Bytes::from(format!("key{}", i))],
                ),
                consistency_level: crate::types::ConsistencyLevel::Linearizable,
            };

            let payload = bincode::serialize(&request).unwrap();
            let entry = StoredLogEntry {
                index: i,
                term: 1,
                payload,
            };

            storage.append_log_entry(&entry).unwrap();
        }

        // Build snapshot
        let snapshot = storage.build_snapshot().await.unwrap();

        // Verify we can read the snapshot data
        let snapshot_data = snapshot.snapshot.into_inner();
        
        // For now, snapshot data is empty (placeholder implementation)
        // In a full implementation, this would contain serialized state machine data
        assert_eq!(snapshot_data.len(), 0);
    }

    #[tokio::test]
    async fn test_multiple_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Build first snapshot
        let snapshot1 = storage.build_snapshot().await.unwrap();
        let snapshot1_id = snapshot1.meta.snapshot_id.clone();

        // Add more log entries
        for i in 0..2 {
            let request = ClientRequest {
                id: crate::types::RequestId::new(),
                command: crate::types::RedisCommand::new(
                    "SET".to_string(),
                    vec![
                        bytes::Bytes::from(format!("key{}", i)),
                        bytes::Bytes::from(format!("value{}", i)),
                    ],
                ),
                consistency_level: crate::types::ConsistencyLevel::Linearizable,
            };

            let payload = bincode::serialize(&request).unwrap();
            let entry = StoredLogEntry {
                index: i,
                term: 1,
                payload,
            };

            storage.append_log_entry(&entry).unwrap();
        }

        // Build second snapshot
        let snapshot2 = storage.build_snapshot().await.unwrap();
        let snapshot2_id = snapshot2.meta.snapshot_id.clone();

        // Verify snapshots have different IDs
        assert_ne!(snapshot1_id, snapshot2_id);

        // Verify the latest snapshot metadata is stored
        let stored_meta = storage.get_snapshot_meta().unwrap();
        assert!(stored_meta.is_some());
        let stored_meta = stored_meta.unwrap();
        assert_eq!(stored_meta.snapshot_id, snapshot2_id);
        assert_eq!(stored_meta.last_log_index, 1); // Last index is 1 (0-1)
    }
}








