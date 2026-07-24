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

use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[cfg(any(test, feature = "test-fault-injection"))]
use std::collections::HashSet;

#[cfg(any(test, feature = "test-fault-injection"))]
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

#[cfg(any(test, feature = "test-fault-injection"))]
use parking_lot::Mutex;

use crate::logindex::LogIndexAndSequenceCollector;
use crate::{sync_directory, sync_parent_directory};

/// File name for JSON metadata at the checkpoint root (not OpenRaft's `SnapshotMeta`).
pub const RAFT_SNAPSHOT_META_FILE: &str = "__raft_snapshot_meta";

const ROCKSDB_LOCK_FILE: &str = "LOCK";
const TARGET_REMOVE_ATTEMPTS: usize = 5;
const TARGET_REMOVE_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

static RESTORE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[cfg(any(test, feature = "test-fault-injection"))]
static RESTORE_PARENT_SYNC_FAILURES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Removes an unconsumed restore sync failpoint when a test exits early.
#[cfg(any(test, feature = "test-fault-injection"))]
#[doc(hidden)]
pub struct RestoreParentSyncFailureGuard {
    target_db_path: PathBuf,
}

#[cfg(any(test, feature = "test-fault-injection"))]
impl Drop for RestoreParentSyncFailureGuard {
    fn drop(&mut self) {
        RESTORE_PARENT_SYNC_FAILURES
            .lock()
            .remove(&self.target_db_path);
    }
}

/// Inject one failure after the staged checkpoint is renamed to `target_db_path`.
#[cfg(any(test, feature = "test-fault-injection"))]
#[doc(hidden)]
#[must_use]
pub fn fail_next_restore_parent_sync_after_rename(
    target_db_path: &Path,
) -> RestoreParentSyncFailureGuard {
    let target_db_path = target_db_path.to_path_buf();
    assert!(
        RESTORE_PARENT_SYNC_FAILURES
            .lock()
            .insert(target_db_path.clone()),
        "restore parent sync failure already registered for {}",
        target_db_path.display()
    );
    RestoreParentSyncFailureGuard { target_db_path }
}

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
    /// LogIndex collector states, one entry per Storage instance.
    /// Outer index is the instance id; inner Vec holds `"log_index:seqno"` pairs.
    #[serde(default)]
    pub logindex_collector_states: Vec<Vec<String>>,
}

impl RaftSnapshotMeta {
    /// Create a new snapshot meta with current version
    pub fn new(last_included_index: u64, last_included_term: u64) -> Self {
        Self {
            version: CURRENT_SNAPSHOT_VERSION,
            last_included_index,
            last_included_term,
            logindex_collector_states: Vec::new(),
        }
    }

    /// Create snapshot meta with collector states for every Storage instance.
    pub fn with_collector_states(
        last_included_index: u64,
        last_included_term: u64,
        collectors: &[Arc<LogIndexAndSequenceCollector>],
    ) -> Self {
        Self {
            version: CURRENT_SNAPSHOT_VERSION,
            last_included_index,
            last_included_term,
            logindex_collector_states: collectors.iter().map(|c| c.export_state()).collect(),
        }
    }

    /// Restore collector states for each Storage instance from snapshot metadata.
    ///
    /// `collectors[i]` receives the entries originally exported from instance `i`. Extra
    /// entries (i.e. snapshot has more instances than the target) are logged and ignored.
    /// Entries that fail to parse are logged and skipped.
    pub fn restore_collector_states(&self, collectors: &[Arc<LogIndexAndSequenceCollector>]) {
        for (idx, entries) in self.logindex_collector_states.iter().enumerate() {
            let Some(collector) = collectors.get(idx) else {
                log::warn!(
                    "Snapshot has collector state for instance {idx} but target only has {} instances; ignoring",
                    collectors.len()
                );
                continue;
            };
            for entry in entries {
                if let Some((log_index_str, seqno_str)) = entry.split_once(':') {
                    if let (Ok(log_index), Ok(seqno)) =
                        (log_index_str.parse::<i64>(), seqno_str.parse::<u64>())
                    {
                        collector.update(log_index, seqno);
                    } else {
                        log::warn!(
                            "Failed to parse collector state entry for instance {idx}: {:?}",
                            entry
                        );
                    }
                } else {
                    log::warn!(
                        "Invalid collector state format (missing ':') for instance {idx}: {:?}",
                        entry
                    );
                }
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
            OpenOptions::new().write(true).open(&dst_path)?.sync_all()?;
        }
    }
    sync_directory(dst)
}

/// A checkpoint layout that has been validated and copied next to its target.
///
/// Dropping this value before [`Self::commit`] removes the staged directory without changing the
/// target database directory.
#[derive(Debug)]
pub struct PreparedCheckpointRestore {
    temp_dir: PathBuf,
    target_db_path: PathBuf,
    committed: bool,
}

impl PreparedCheckpointRestore {
    /// Replace the target database directory with the staged checkpoint layout.
    ///
    /// This is the destructive phase of checkpoint restoration. On Windows, removing the old
    /// target is retried to tolerate transient filesystem locks.
    pub fn commit(mut self) -> io::Result<()> {
        if self.target_db_path.exists() {
            remove_target_with_retry(&self.target_db_path)?;
            sync_parent_directory(&self.target_db_path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "failed to sync target parent after removing {}: {error}",
                        self.target_db_path.display()
                    ),
                )
            })?;
        }

        fs::rename(&self.temp_dir, &self.target_db_path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to switch staged checkpoint {} to target {}: {error}",
                    self.temp_dir.display(),
                    self.target_db_path.display()
                ),
            )
        })?;
        sync_directory(&self.target_db_path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to sync restored target directory {}: {error}",
                    self.target_db_path.display()
                ),
            )
        })?;
        sync_restore_parent_after_rename(&self.target_db_path)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PreparedCheckpointRestore {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_dir_all(&self.temp_dir);
        }
    }
}

/// Validate and stage a checkpoint layout without changing `target_db_path`.
pub fn prepare_checkpoint_restore(
    checkpoint_root: &Path,
    target_db_path: &Path,
    db_instance_num: usize,
) -> io::Result<PreparedCheckpointRestore> {
    for i in 0..db_instance_num {
        let from = checkpoint_root.join(i.to_string());
        if !from.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing checkpoint instance directory: {}", from.display()),
            ));
        }
    }

    let temp_dir = create_restore_temp_dir(target_db_path)?;
    let prepared = PreparedCheckpointRestore {
        temp_dir,
        target_db_path: target_db_path.to_path_buf(),
        committed: false,
    };

    for i in 0..db_instance_num {
        let from = checkpoint_root.join(i.to_string());
        let to = prepared.temp_dir.join(i.to_string());
        copy_dir_all(&from, &to).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to stage checkpoint instance {} from {} to {}: {error}",
                    i,
                    from.display(),
                    to.display()
                ),
            )
        })?;
    }

    sync_directory(&prepared.temp_dir).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to sync staged checkpoint root {}: {error}",
                prepared.temp_dir.display()
            ),
        )
    })?;
    sync_parent_directory(&prepared.temp_dir).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to sync checkpoint staging parent for {}: {error}",
                prepared.temp_dir.display()
            ),
        )
    })?;

    Ok(prepared)
}

fn create_restore_temp_dir(target_db_path: &Path) -> io::Result<PathBuf> {
    let parent = target_db_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "target database path has no parent directory: {}",
                target_db_path.display()
            ),
        )
    })?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    fs::create_dir_all(parent).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to create target parent directory {} for checkpoint staging: {error}",
                parent.display()
            ),
        )
    })?;

    loop {
        let sequence = RESTORE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp_dir = parent.join(format!(".restore_temp_{}_{}", std::process::id(), sequence));
        match fs::create_dir(&temp_dir) {
            Ok(()) => {
                sync_directory(parent).map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!(
                            "failed to sync checkpoint staging parent {}: {error}",
                            parent.display()
                        ),
                    )
                })?;
                return Ok(temp_dir);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "failed to create checkpoint staging directory {}: {error}",
                        temp_dir.display()
                    ),
                ));
            }
        }
    }
}

fn sync_restore_parent_after_rename(target_db_path: &Path) -> io::Result<()> {
    #[cfg(any(test, feature = "test-fault-injection"))]
    if RESTORE_PARENT_SYNC_FAILURES.lock().remove(target_db_path) {
        return Err(io::Error::other(format!(
            "injected restore parent sync failure after rename for {}",
            target_db_path.display()
        )));
    }

    sync_parent_directory(target_db_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to sync target parent after renaming staged checkpoint to {}: {error}",
                target_db_path.display()
            ),
        )
    })
}

fn remove_target_with_retry(target_db_path: &Path) -> io::Result<()> {
    for attempt in 0..TARGET_REMOVE_ATTEMPTS {
        match fs::remove_dir_all(target_db_path) {
            Ok(()) => return Ok(()),
            Err(error)
                if error.kind() == io::ErrorKind::PermissionDenied
                    && attempt + 1 < TARGET_REMOVE_ATTEMPTS =>
            {
                std::thread::sleep(TARGET_REMOVE_RETRY_BASE_DELAY * (attempt as u32 + 1));
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "failed to remove target database directory {}: {error}",
                        target_db_path.display()
                    ),
                ));
            }
        }
    }

    unreachable!("target removal loop always returns")
}

/// Copy checkpoint layout from `checkpoint_root` into `target_db_path` (`0/`, `1/`, …).
///
/// This compatibility helper stages the complete checkpoint before entering the destructive
/// target replacement phase.
pub fn restore_checkpoint_layout(
    checkpoint_root: &Path,
    target_db_path: &Path,
    db_instance_num: usize,
) -> io::Result<()> {
    prepare_checkpoint_restore(checkpoint_root, target_db_path, db_instance_num)?.commit()
}
