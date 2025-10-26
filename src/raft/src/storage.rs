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
//! 
//! This is a basic implementation that provides the foundation for Raft storage
//! using RocksDB as the underlying persistence layer.

use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch, DB};
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
pub struct RaftStorage {
    /// RocksDB instance
    db: Arc<DB>,
    /// Current Raft state (cached for performance)
    state: Arc<RwLock<RaftState>>,
}

/// Raft persistent state
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RaftState {
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
    fn get_cf_handle(&self, cf_name: &str) -> Result<std::sync::Arc<rocksdb::BoundColumnFamily>, RaftError> {
        self.db.cf_handle(cf_name)
            .ok_or_else(|| RaftError::Storage(StorageError::DataInconsistency { 
                message: format!("Column family {} not found", cf_name) 
            }))
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
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;

        let db = Arc::new(db);
        let state = Arc::new(RwLock::new(RaftState::default()));

        let storage = Self { db, state };

        // Load existing state
        storage.load_state()?;

        Ok(storage)
    }

    /// Load Raft state from persistent storage
    fn load_state(&self) -> Result<(), RaftError> {
        let cf_state = self.get_cf_handle(CF_STATE)?;

        // Load current term
        let current_term = if let Some(data) = self.db.get_cf(&cf_state, KEY_CURRENT_TERM)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?
        {
            bincode::deserialize(&data)
                .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                    message: format!("Failed to deserialize current term: {}", e) 
                }))?
        } else {
            0
        };

        // Load voted for
        let voted_for = if let Some(data) = self.db.get_cf(&cf_state, KEY_VOTED_FOR)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?
        {
            bincode::deserialize(&data)
                .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                    message: format!("Failed to deserialize voted_for: {}", e) 
                }))?
        } else {
            None
        };

        // Load last applied
        let last_applied = if let Some(data) = self.db.get_cf(&cf_state, KEY_LAST_APPLIED)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?
        {
            bincode::deserialize(&data)
                .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                    message: format!("Failed to deserialize last_applied: {}", e) 
                }))?
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

    /// Save Raft state to persistent storage
    pub fn save_state(&self, state: &RaftState) -> Result<(), RaftError> {
        let cf_state = self.get_cf_handle(CF_STATE)?;
        
        let mut batch = WriteBatch::default();

        // Serialize and save current term
        let term_data = bincode::serialize(&state.current_term)
            .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                message: format!("Failed to serialize current term: {}", e) 
            }))?;
        batch.put_cf(&cf_state, KEY_CURRENT_TERM, term_data);

        // Serialize and save voted for
        let voted_for_data = bincode::serialize(&state.voted_for)
            .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                message: format!("Failed to serialize voted_for: {}", e) 
            }))?;
        batch.put_cf(&cf_state, KEY_VOTED_FOR, voted_for_data);

        // Serialize and save last applied
        let last_applied_data = bincode::serialize(&state.last_applied)
            .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                message: format!("Failed to serialize last_applied: {}", e) 
            }))?;
        batch.put_cf(&cf_state, KEY_LAST_APPLIED, last_applied_data);

        // Write batch atomically
        self.db.write(batch)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;

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
    fn log_key(index: LogIndex) -> Vec<u8> {
        // Use big-endian encoding for proper ordering
        index.to_be_bytes().to_vec()
    }

    /// Parse log entry key from RocksDB
    fn parse_log_key(key: &[u8]) -> Result<LogIndex, RaftError> {
        if key.len() != 8 {
            return Err(RaftError::Storage(StorageError::DataInconsistency { 
                message: "Invalid log key length".to_string() 
            }));
        }

        let index = LogIndex::from_be_bytes(
            key.try_into()
                .map_err(|_| RaftError::Storage(StorageError::DataInconsistency { 
                    message: "Failed to parse log index".to_string() 
                }))?
        );

        Ok(index)
    }

    /// Append log entry
    pub fn append_log_entry(&self, entry: &StoredLogEntry) -> Result<(), RaftError> {
        let cf_log = self.get_cf_handle(CF_LOG)?;

        let key = Self::log_key(entry.index);
        let value = bincode::serialize(entry)
            .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                message: format!("Failed to serialize log entry: {}", e) 
            }))?;

        self.db.put_cf(&cf_log, key, value)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;

        Ok(())
    }

    /// Get log entry by index
    pub fn get_log_entry(&self, index: LogIndex) -> Result<Option<StoredLogEntry>, RaftError> {
        let cf_log = self.get_cf_handle(CF_LOG)?;

        let key = Self::log_key(index);
        if let Some(value) = self.db.get_cf(&cf_log, key)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?
        {
            let entry: StoredLogEntry = bincode::deserialize(&value)
                .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                    message: format!("Failed to deserialize log entry: {}", e) 
                }))?;
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
            let (_key, value) = result
                .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;
            let entry: StoredLogEntry = bincode::deserialize(&value)
                .map_err(|e| RaftError::Storage(StorageError::DataInconsistency { 
                    message: format!("Failed to deserialize log entry: {}", e) 
                }))?;
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
        let iter = self.db.iterator_cf(&cf_log, rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward));

        for result in iter {
            let (key, _) = result
                .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;
            batch.delete_cf(&cf_log, key);
        }

        self.db.write(batch)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;

        Ok(())
    }

    /// Store snapshot metadata
    pub fn store_snapshot_meta(&self, meta: &StoredSnapshotMeta) -> Result<(), RaftError> {
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        let meta_data = bincode::serialize(meta)
            .map_err(|e| RaftError::Storage(StorageError::SnapshotCreationFailed { 
                message: format!("Failed to serialize snapshot metadata: {}", e) 
            }))?;

        self.db.put_cf(&cf_snapshot, KEY_SNAPSHOT_META, meta_data)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;

        Ok(())
    }

    /// Get snapshot metadata
    pub fn get_snapshot_meta(&self) -> Result<Option<StoredSnapshotMeta>, RaftError> {
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        if let Some(meta_data) = self.db.get_cf(&cf_snapshot, KEY_SNAPSHOT_META)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?
        {
            let meta: StoredSnapshotMeta = bincode::deserialize(&meta_data)
                .map_err(|e| RaftError::Storage(StorageError::SnapshotRestorationFailed { 
                    message: format!("Failed to deserialize snapshot metadata: {}", e) 
                }))?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    /// Store snapshot data
    pub fn store_snapshot_data(&self, snapshot_id: &str, data: &[u8]) -> Result<(), RaftError> {
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        self.db.put_cf(&cf_snapshot, snapshot_id, data)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;

        Ok(())
    }

    /// Get snapshot data
    pub fn get_snapshot_data(&self, snapshot_id: &str) -> Result<Option<Vec<u8>>, RaftError> {
        let cf_snapshot = self.get_cf_handle(CF_SNAPSHOT)?;

        let data = self.db.get_cf(&cf_snapshot, snapshot_id)
            .map_err(|e| RaftError::Storage(StorageError::RocksDb(e)))?;

        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_raft_storage_creation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RaftStorage::new(temp_dir.path()).unwrap();
        
        // Verify initial state
        assert_eq!(storage.get_current_term(), 0);
        assert_eq!(storage.get_voted_for(), None);
        assert_eq!(storage.get_last_applied(), 0);
    }

    #[test]
    fn test_state_persistence() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create storage and modify state
        {
            let storage = RaftStorage::new(temp_dir.path()).unwrap();
            storage.set_current_term(5).unwrap();
            storage.set_voted_for(Some(42)).unwrap();
            storage.set_last_applied(100).unwrap();
        }
        
        // Recreate storage and verify state is loaded
        {
            let storage = RaftStorage::new(temp_dir.path()).unwrap();
            assert_eq!(storage.get_current_term(), 5);
            assert_eq!(storage.get_voted_for(), Some(42));
            assert_eq!(storage.get_last_applied(), 100);
        }
    }

    #[test]
    fn test_log_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Test appending log entry
        let entry = StoredLogEntry {
            index: 1,
            term: 1,
            payload: b"test command".to_vec(),
        };
        storage.append_log_entry(&entry).unwrap();

        // Test retrieving log entry
        let retrieved = storage.get_log_entry(1).unwrap().unwrap();
        assert_eq!(retrieved.index, entry.index);
        assert_eq!(retrieved.term, entry.term);
        assert_eq!(retrieved.payload, entry.payload);

        // Test getting last log entry
        let last = storage.get_last_log_entry().unwrap().unwrap();
        assert_eq!(last.index, entry.index);
    }

    #[test]
    fn test_snapshot_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Test storing snapshot metadata
        let meta = StoredSnapshotMeta {
            last_log_index: 100,
            last_log_term: 5,
            snapshot_id: "snapshot_1".to_string(),
            timestamp: 1234567890,
        };
        storage.store_snapshot_meta(&meta).unwrap();

        // Test retrieving snapshot metadata
        let retrieved_meta = storage.get_snapshot_meta().unwrap().unwrap();
        assert_eq!(retrieved_meta.last_log_index, meta.last_log_index);
        assert_eq!(retrieved_meta.snapshot_id, meta.snapshot_id);

        // Test storing and retrieving snapshot data
        let data = b"snapshot data";
        storage.store_snapshot_data(&meta.snapshot_id, data).unwrap();
        let retrieved_data = storage.get_snapshot_data(&meta.snapshot_id).unwrap().unwrap();
        assert_eq!(retrieved_data, data);
    }
}