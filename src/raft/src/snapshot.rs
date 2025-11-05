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

//! Snapshot/full backup implementation
//!
//! This module provides three methods for creating snapshots:
//! 1. RocksDB checkpoint (hard link, no extra space)
//! 2. SST file transmission (copy flushed SST files)
//! 3. Iterator-based backup (real-time read from RocksDB)

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocksdb::{DB, IteratorMode, Options, checkpoint::Checkpoint};

use crate::error::RaftError;
use crate::types::LogIndex;

/// Snapshot creation method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMethod {
    /// Use RocksDB checkpoint (hard link, no extra space)
    Checkpoint,
    /// Copy SST files and metadata
    SstFiles,
    /// Use iterator to read all data in real-time
    Iterator,
}

/// Snapshot metadata
#[derive(Debug, Clone)]
pub struct SnapshotMeta {
    /// Snapshot ID
    pub snapshot_id: String,
    /// Last log index included in snapshot
    pub last_log_index: LogIndex,
    /// Snapshot directory path
    pub snapshot_dir: PathBuf,
    /// Creation method
    pub method: SnapshotMethod,
    /// Total size in bytes
    pub size_bytes: u64,
}

/// Snapshot manager
pub struct SnapshotManager {
    /// RocksDB instance
    db: Arc<DB>,
    /// Base directory for snapshots
    snapshot_base_dir: PathBuf,
}

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new<P: AsRef<Path>>(db: Arc<DB>, snapshot_base_dir: P) -> Result<Self, RaftError> {
        let snapshot_base_dir = snapshot_base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&snapshot_base_dir).map_err(|e| {
            RaftError::state_machine(format!("Failed to create snapshot directory: {}", e))
        })?;

        Ok(Self {
            db,
            snapshot_base_dir,
        })
    }

    /// Create a snapshot using checkpoint (hard link)
    pub fn create_checkpoint_snapshot(
        &self,
        snapshot_id: &str,
        last_log_index: LogIndex,
    ) -> Result<SnapshotMeta, RaftError> {
        let snapshot_dir = self.snapshot_base_dir.join(snapshot_id);

        // Create checkpoint directory
        std::fs::create_dir_all(&snapshot_dir).map_err(|e| {
            RaftError::state_machine(format!("Failed to create snapshot dir: {}", e))
        })?;

        // Create RocksDB checkpoint
        let checkpoint = Checkpoint::new(&*self.db)
            .map_err(|e| RaftError::state_machine(format!("Failed to create checkpoint: {}", e)))?;

        checkpoint
            .create_checkpoint(&snapshot_dir)
            .map_err(|e| RaftError::state_machine(format!("Failed to create checkpoint: {}", e)))?;

        // Calculate snapshot size (approximate, checkpoints use hard links)
        let size_bytes = Self::calculate_directory_size(&snapshot_dir)?;

        Ok(SnapshotMeta {
            snapshot_id: snapshot_id.to_string(),
            last_log_index,
            snapshot_dir,
            method: SnapshotMethod::Checkpoint,
            size_bytes,
        })
    }

    /// Create a snapshot by copying SST files
    pub fn create_sst_snapshot(
        &self,
        snapshot_id: &str,
        last_log_index: LogIndex,
    ) -> Result<SnapshotMeta, RaftError> {
        let snapshot_dir = self.snapshot_base_dir.join(snapshot_id);

        // For SST file method, we would:
        // 1. Get the current database directory
        // 2. Copy all SST files that are already flushed
        // 3. Copy MANIFEST and other metadata files

        // This is a placeholder implementation
        // In a real implementation, we'd need to:
        // - Get list of SST files from RocksDB
        // - Filter only flushed (immutable) SST files
        // - Copy them to snapshot directory

        std::fs::create_dir_all(&snapshot_dir).map_err(|e| {
            RaftError::state_machine(format!("Failed to create snapshot dir: {}", e))
        })?;

        // TODO: Implement actual SST file copying
        let size_bytes = 0; // Placeholder

        Ok(SnapshotMeta {
            snapshot_id: snapshot_id.to_string(),
            last_log_index,
            snapshot_dir,
            method: SnapshotMethod::SstFiles,
            size_bytes,
        })
    }

    /// Create a snapshot using iterator (real-time read)
    pub fn create_iterator_snapshot(
        &self,
        snapshot_id: &str,
        last_log_index: LogIndex,
    ) -> Result<SnapshotMeta, RaftError> {
        let snapshot_dir = self.snapshot_base_dir.join(snapshot_id);

        std::fs::create_dir_all(&snapshot_dir).map_err(|e| {
            RaftError::state_machine(format!("Failed to create snapshot dir: {}", e))
        })?;

        // Create a new RocksDB instance for snapshot
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let snapshot_db_path = snapshot_dir.join("data");
        let snapshot_db = DB::open(&opts, &snapshot_db_path).map_err(|e| {
            RaftError::state_machine(format!("Failed to create snapshot DB: {}", e))
        })?;

        // Iterate through all keys in the original database and copy them
        let mut batch = rocksdb::WriteBatch::default();
        let mut batch_size = 0;
        const BATCH_SIZE_LIMIT: usize = 1024 * 1024; // 1MB batches

        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            let (key, value) =
                item.map_err(|e| RaftError::state_machine(format!("Iterator error: {}", e)))?;

            batch.put(&key, &value);
            batch_size += key.len() + value.len();

            // Write batch when it gets too large
            if batch_size >= BATCH_SIZE_LIMIT {
                snapshot_db.write(batch).map_err(|e| {
                    RaftError::state_machine(format!("Failed to write batch: {}", e))
                })?;
                batch = rocksdb::WriteBatch::default();
                batch_size = 0;
            }
        }

        // Write remaining batch
        if batch_size > 0 {
            snapshot_db.write(batch).map_err(|e| {
                RaftError::state_machine(format!("Failed to write final batch: {}", e))
            })?;
        }

        let size_bytes = Self::calculate_directory_size(&snapshot_db_path)?;

        Ok(SnapshotMeta {
            snapshot_id: snapshot_id.to_string(),
            last_log_index,
            snapshot_dir,
            method: SnapshotMethod::Iterator,
            size_bytes,
        })
    }

    /// Install a snapshot (restore from snapshot)
    pub fn install_snapshot(
        &self,
        snapshot_meta: &SnapshotMeta,
        target_db_path: &Path,
    ) -> Result<(), RaftError> {
        match snapshot_meta.method {
            SnapshotMethod::Checkpoint => {
                // For checkpoint, we can use it directly or copy
                // In a real implementation, we'd restore to a temporary location first
                Self::copy_directory(&snapshot_meta.snapshot_dir, target_db_path)
            }
            SnapshotMethod::SstFiles => {
                // Copy SST files to target location
                Self::copy_directory(&snapshot_meta.snapshot_dir, target_db_path)
            }
            SnapshotMethod::Iterator => {
                // Copy the snapshot database
                Self::copy_directory(&snapshot_meta.snapshot_dir.join("data"), target_db_path)
            }
        }
    }

    /// Calculate directory size recursively
    fn calculate_directory_size(dir: &Path) -> Result<u64, RaftError> {
        let mut total_size = 0u64;

        if dir.is_dir() {
            for entry in std::fs::read_dir(dir)
                .map_err(|e| RaftError::state_machine(format!("Failed to read dir: {}", e)))?
            {
                let entry = entry.map_err(|e| {
                    RaftError::state_machine(format!("Failed to read entry: {}", e))
                })?;
                let path = entry.path();

                if path.is_dir() {
                    total_size += Self::calculate_directory_size(&path)?;
                } else {
                    total_size += entry
                        .metadata()
                        .map_err(|e| {
                            RaftError::state_machine(format!("Failed to get metadata: {}", e))
                        })?
                        .len();
                }
            }
        }

        Ok(total_size)
    }

    /// Copy directory recursively
    fn copy_directory(src: &Path, dst: &Path) -> Result<(), RaftError> {
        if src.is_file() {
            std::fs::copy(src, dst)
                .map_err(|e| RaftError::state_machine(format!("Failed to copy file: {}", e)))?;
            return Ok(());
        }

        std::fs::create_dir_all(dst)
            .map_err(|e| RaftError::state_machine(format!("Failed to create dst dir: {}", e)))?;

        for entry in std::fs::read_dir(src)
            .map_err(|e| RaftError::state_machine(format!("Failed to read src dir: {}", e)))?
        {
            let entry = entry
                .map_err(|e| RaftError::state_machine(format!("Failed to read entry: {}", e)))?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());

            if src_path.is_dir() {
                Self::copy_directory(&src_path, &dst_path)?;
            } else {
                std::fs::copy(&src_path, &dst_path)
                    .map_err(|e| RaftError::state_machine(format!("Failed to copy file: {}", e)))?;
            }
        }

        Ok(())
    }

    /// List all available snapshots
    pub fn list_snapshots(&self) -> Result<Vec<String>, RaftError> {
        let mut snapshots = Vec::new();

        for entry in std::fs::read_dir(&self.snapshot_base_dir)
            .map_err(|e| RaftError::state_machine(format!("Failed to read snapshot dir: {}", e)))?
        {
            let entry = entry
                .map_err(|e| RaftError::state_machine(format!("Failed to read entry: {}", e)))?;

            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    snapshots.push(name.to_string());
                }
            }
        }

        snapshots.sort();
        Ok(snapshots)
    }

    /// Delete a snapshot
    pub fn delete_snapshot(&self, snapshot_id: &str) -> Result<(), RaftError> {
        let snapshot_dir = self.snapshot_base_dir.join(snapshot_id);

        if snapshot_dir.exists() {
            std::fs::remove_dir_all(&snapshot_dir).map_err(|e| {
                RaftError::state_machine(format!("Failed to delete snapshot: {}", e))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::DB;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_methods() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, &db_path).unwrap());

        // Insert some test data
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();

        let snapshot_dir = temp_dir.path().join("snapshots");
        let manager = SnapshotManager::new(db.clone(), &snapshot_dir).unwrap();

        // Test checkpoint snapshot
        let meta = manager.create_checkpoint_snapshot("test1", 100).unwrap();
        assert_eq!(meta.snapshot_id, "test1");
        assert_eq!(meta.last_log_index, 100);
        assert_eq!(meta.method, SnapshotMethod::Checkpoint);

        // Test iterator snapshot
        let meta2 = manager.create_iterator_snapshot("test2", 200).unwrap();
        assert_eq!(meta2.snapshot_id, "test2");
        assert_eq!(meta2.method, SnapshotMethod::Iterator);
    }
}
