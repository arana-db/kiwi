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

//! Raft snapshot checkpoint layout: one RocksDB checkpoint per DB instance plus `__raft_snapshot_meta`.

use crate::logindex::LogIndexAndSequenceCollector;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// File name for JSON metadata at the checkpoint root (not OpenRaft's `SnapshotMeta`).
pub const RAFT_SNAPSHOT_META_FILE: &str = "__raft_snapshot_meta";

const ROCKSDB_LOCK_FILE: &str = "LOCK";

/// Current snapshot format version
pub const CURRENT_SNAPSHOT_VERSION: u32 = 1;

/// Metadata persisted next to per-instance checkpoint directories.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotMeta {
    /// Snapshot format version
    pub version: u32,
    /// Last log index included in the snapshot
    pub last_included_index: u64,
    /// Last log term included in the snapshot
    pub last_included_term: u64,
    /// LogIndex collector state.
    /// Format: Vec<"log_index:seqno">
    #[serde(default)]
    pub logindex_collector_state: Vec<String>,
}

impl RaftSnapshotMeta {
    /// Create a new snapshot meta with current version
    pub fn new(last_included_index: u64, last_included_term: u64) -> Self {
        Self {
            version: CURRENT_SNAPSHOT_VERSION,
            last_included_index,
            last_included_term,
            logindex_collector_state: Vec::new(),
        }
    }

    /// Create snapshot meta with collector state
    pub fn with_collector_state(
        last_included_index: u64,
        last_included_term: u64,
        collector: &Arc<LogIndexAndSequenceCollector>,
    ) -> Self {
        Self {
            version: CURRENT_SNAPSHOT_VERSION,
            last_included_index,
            last_included_term,
            logindex_collector_state: collector.export_state(),
        }
    }

    /// Restore collector state from snapshot metadata.
    ///
    /// Parses "log_index:seqno" entries exported by `LogIndexAndSequenceCollector::export_state()`.
    /// Entries that fail to parse are logged and skipped.
    pub fn restore_collector_state(&self, collector: &Arc<LogIndexAndSequenceCollector>) {
        for entry in &self.logindex_collector_state {
            if let Some((log_index_str, seqno_str)) = entry.split_once(':') {
                if let (Ok(log_index), Ok(seqno)) =
                    (log_index_str.parse::<i64>(), seqno_str.parse::<u64>())
                {
                    collector.update(log_index, seqno);
                } else {
                    log::warn!("Failed to parse collector state entry: {:?}", entry);
                }
            } else {
                log::warn!("Invalid collector state format (missing ':'): {:?}", entry);
            }
        }
    }

    pub fn write_to_dir(&self, dir: &Path) -> io::Result<()> {
        let path = dir.join(RAFT_SNAPSHOT_META_FILE);
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(path, json)
    }

    /// Write metadata atomically using temp file + rename pattern.
    /// This ensures that the file is either completely written or not present at all.
    ///
    /// Atomic rename (on POSIX systems, rename is atomic if on same filesystem).
    pub fn write_to_dir_atomically(&self, dir: &Path) -> io::Result<()> {
        let path = dir.join(RAFT_SNAPSHOT_META_FILE);
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let temp_path = dir.join(format!(".{}.tmp", RAFT_SNAPSHOT_META_FILE));
        fs::write(&temp_path, &json)?;
        fs::rename(&temp_path, &path)?;

        Ok(())
    }

    pub fn read_from_dir(dir: &Path) -> io::Result<Self> {
        let path = dir.join(RAFT_SNAPSHOT_META_FILE);
        let bytes = fs::read(path)?;
        let meta: Self = serde_json::from_slice(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Validate version: reject old unsupported versions for safety,
        // but allow higher versions for forward compatibility during rolling upgrades.
        if meta.version < CURRENT_SNAPSHOT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported snapshot version: {}, expected >= {}",
                    meta.version, CURRENT_SNAPSHOT_VERSION
                ),
            ));
        }

        Ok(meta)
    }
}

pub fn copy_dir_all(src: &Path, dst: &Path) -> io::Result<()> {
    if !src.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("source is not a directory: {}", src.display()),
        ));
    }
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if let Some(file_name) = src_path.file_name().and_then(|n| n.to_str()) {
            // Skip RocksDB LOCK file - it's runtime state, not persistent data.
            // Copying it causes "lock held by current process" errors when opening.
            if file_name == ROCKSDB_LOCK_FILE {
                continue;
            }
        }

        if ty.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Copy checkpoint layout from `checkpoint_root` into `target_db_path` (`0/`, `1/`, …).
///
/// Uses atomic replacement pattern to avoid data loss:
/// 1. Validate all source directories exist
/// 2. Copy to a temporary sibling directory
/// 3. Atomically swap by removing old and renaming new
pub fn restore_checkpoint_layout(
    checkpoint_root: &Path,
    target_db_path: &Path,
    db_instance_num: usize,
) -> io::Result<()> {
    for i in 0..db_instance_num {
        let from = checkpoint_root.join(i.to_string());
        if !from.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing checkpoint instance directory: {}", from.display()),
            ));
        }
    }

    // Create temp directory as sibling of target (same filesystem for atomic rename)
    let temp_dir = target_db_path.with_file_name(format!(".restore_temp_{}", std::process::id()));

    // Clean up any stale temp directory from previous failed attempts
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir)?;
    }

    fs::create_dir_all(&temp_dir)?;

    // Copy checkpoint data to temp directory
    let copy_result = (|| -> io::Result<()> {
        for i in 0..db_instance_num {
            let from = checkpoint_root.join(i.to_string());
            let to = temp_dir.join(i.to_string());
            copy_dir_all(&from, &to)?;
        }
        Ok(())
    })();

    if let Err(e) = copy_result {
        let _ = fs::remove_dir_all(&temp_dir);
        return Err(e);
    }

    // Remove old target directory if it exists
    // On Windows, this may require retries due to file locking
    if target_db_path.exists() {
        for attempt in 0..5 {
            match fs::remove_dir_all(target_db_path) {
                Ok(()) => break,
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied && attempt < 4 => {
                    std::thread::sleep(std::time::Duration::from_millis(100 * (attempt + 1)));
                }
                Err(e) => return Err(e),
            }
        }
    }

    // Rename temp to target (this is the atomic switch)
    let switch_result = fs::rename(&temp_dir, target_db_path);

    if let Err(e) = switch_result {
        // Switch failed: clean up partial state
        let _ = fs::remove_dir_all(target_db_path);
        let _ = fs::remove_dir_all(&temp_dir);
        return Err(e);
    }

    Ok(())
}
