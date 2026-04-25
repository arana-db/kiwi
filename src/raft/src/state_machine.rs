// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Raft snapshot: checkpoint → tar in `build_snapshot`, unpack → restore target layout in
//! `install_snapshot`.
//!
//! Snapshot metadata uses `last_applied` to ensure (index, term) pair comes from the same log entry,
//! maintaining Raft invariant that snapshot must refer to a single, valid log entry.
//!
//! This module supports hot-swapping Storage during install_snapshot using ArcSwap.
//! When a snapshot is installed:
//! 1. StorageServer is paused (wait for pending requests to complete)
//! 2. Old Storage is closed and replaced with restored data
//! 3. ArcSwap atomically switches to new Storage
//! 4. StorageServer resumes, using the new Storage

use std::io::{self, Cursor};
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use openraft::{
    CommittedLeaderId, EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, storage::RaftStateMachine,
};
use storage::logindex::{LogIndexAndSequenceCollector, LogIndexOfColumnFamilies};
use storage::storage::Storage;
use storage::{RaftSnapshotMeta, StorageOptions, restore_checkpoint_layout};

use conf::raft_type::{Binlog, BinlogResponse, KiwiNode, KiwiTypeConfig};

use crate::snapshot_archive::{pack_dir_to_vec, unpack_tar_to_dir, unpacked_checkpoint_root};

fn storage_err_to_raft(e: storage::error::Error) -> StorageError<u64> {
    StorageError::from_io_error(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        io::Error::other(e.to_string()),
    )
}

fn io_err_to_raft(e: std::io::Error) -> StorageError<u64> {
    StorageError::from_io_error(ErrorSubject::StateMachine, ErrorVerb::Write, e)
}

/// OpenRaft expects a single on-disk "current" snapshot; `build_snapshot` / `install_snapshot`
/// must persist here so [`RaftStateMachine::get_current_snapshot`] can return it.
const CURRENT_SNAPSHOT_DATA: &str = "current_snapshot.tar";
const CURRENT_SNAPSHOT_META: &str = "current_snapshot_meta.json";

#[allow(clippy::result_large_err)]
fn persist_current_snapshot(
    work_dir: &std::path::Path,
    meta: &SnapshotMeta<u64, KiwiNode>,
    bytes: &[u8],
) -> Result<(), StorageError<u64>> {
    std::fs::create_dir_all(work_dir).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
    })?;

    let data_path = work_dir.join(CURRENT_SNAPSHOT_DATA);
    let meta_path = work_dir.join(CURRENT_SNAPSHOT_META);

    // Use temporary files + atomic rename to prevent TOCTOU race conditions.
    let data_tmp = work_dir.join(format!(".{}.tmp", CURRENT_SNAPSHOT_DATA));
    let meta_tmp = work_dir.join(format!(".{}.tmp", CURRENT_SNAPSHOT_META));
    std::fs::write(&data_tmp, bytes).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
    })?;

    let json = serde_json::to_string_pretty(meta).map_err(|e| {
        StorageError::from_io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            io::Error::other(e.to_string()),
        )
    })?;
    std::fs::write(&meta_tmp, json).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
    })?;

    // Atomic rename (on POSIX systems, rename within same filesystem is atomic)
    // For Windows, see atomic_replace functions below
    atomic_replace_file(&data_tmp, &data_path)?;
    atomic_replace_file(&meta_tmp, &meta_path)?;

    Ok(())
}

#[allow(clippy::result_large_err)]
fn load_current_snapshot(
    work_dir: &std::path::Path,
) -> Result<Option<Snapshot<KiwiTypeConfig>>, StorageError<u64>> {
    let data_path = work_dir.join(CURRENT_SNAPSHOT_DATA);
    let meta_path = work_dir.join(CURRENT_SNAPSHOT_META);
    if !data_path.is_file() || !meta_path.is_file() {
        return Ok(None);
    }
    let bytes = std::fs::read(&data_path).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e)
    })?;
    let json = std::fs::read_to_string(&meta_path).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e)
    })?;
    let meta: SnapshotMeta<u64, KiwiNode> = serde_json::from_str(&json).map_err(|e| {
        StorageError::from_io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Read,
            io::Error::other(e.to_string()),
        )
    })?;
    Ok(Some(Snapshot {
        meta,
        snapshot: Box::new(Cursor::new(bytes)),
    }))
}

/// Windows-safe atomic file replace.
/// On Unix: rename is atomic.
/// On Windows: must delete target first, then rename.
#[allow(clippy::result_large_err)]
fn atomic_replace_file(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> Result<(), StorageError<u64>> {
    #[cfg(unix)]
    {
        std::fs::rename(src, dst).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
        })?;
    }

    #[cfg(windows)]
    {
        if dst.exists() {
            std::fs::remove_file(dst).map_err(|e| {
                StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
            })?;
        }
        std::fs::rename(src, dst).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
        })?;
    }

    Ok(())
}

/// Pause controller for coordinating with StorageServer during snapshot installation.
pub trait PauseController: Send + Sync {
    /// Request pause: wait for all pending requests to complete.
    fn request_pause(&self)
    -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;

    /// Resume: allow new requests to proceed.
    fn resume(&self);
}

/// Kiwi state machine with hot-swapping Storage support.
pub struct KiwiStateMachine {
    _node_id: u64,
    /// ArcSwap for hot-swapping Storage during install_snapshot.
    storage_swap: Arc<ArcSwap<Storage>>,
    /// RocksDB data directory (`<db_path>/0`, …)
    db_path: PathBuf,
    /// Working directory for snapshot export/import.
    snapshot_work_dir: PathBuf,
    /// Last applied log ID.
    last_applied: Option<LogId<u64>>,
    /// Last membership configuration.
    last_membership: StoredMembership<u64, KiwiNode>,
    /// Snapshot counter for generating unique snapshot IDs.
    snapshot_idx: u64,
    /// Pause controller for coordinating with StorageServer.
    pause_controller: Option<Arc<dyn PauseController>>,
    /// LogIndex collector for Raft snapshot integration
    collector: Arc<LogIndexAndSequenceCollector>,
    /// LogIndex CF tracker for Raft snapshot integration
    cf_tracker: Arc<LogIndexOfColumnFamilies>,
}

impl KiwiStateMachine {
    /// Create a new state machine.
    pub fn new(
        node_id: u64,
        storage_swap: Arc<ArcSwap<Storage>>,
        db_path: PathBuf,
        snapshot_work_dir: PathBuf,
        collector: Arc<LogIndexAndSequenceCollector>,
        cf_tracker: Arc<LogIndexOfColumnFamilies>,
    ) -> Self {
        Self {
            _node_id: node_id,
            storage_swap,
            db_path,
            snapshot_work_dir,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
            pause_controller: None,
            collector,
            cf_tracker,
        }
    }

    /// Initialize cf_tracker from restored SST properties after snapshot install
    /// or from existing DB on startup
    pub fn init_cf_tracker(&self) -> Result<(), io::Error> {
        let storage = self.storage_swap.load_full();
        storage.init_cf_trackers().map_err(io::Error::other)
    }

    /// Set pause controller for coordinating with StorageServer.
    pub fn set_pause_controller(&mut self, controller: Arc<dyn PauseController>) {
        self.pause_controller = Some(controller);
    }

    /// Apply binlog to storage.
    async fn apply_binlog(&self, binlog: &Binlog, _log_idx: u64) -> Result<(), io::Error> {
        let storage = self.storage_swap.load_full();
        storage
            .on_binlog_write(binlog, _log_idx)
            .map_err(|e| io::Error::other(format!("Failed to apply binlog: {}", e)))
    }
}

impl RaftStateMachine<KiwiTypeConfig> for KiwiStateMachine {
    type SnapshotBuilder = KiwiSnapshotBuilder;

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<BinlogResponse>, openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<KiwiTypeConfig>> + Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            self.last_applied = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => BinlogResponse::ok(),
                EntryPayload::Normal(binlog) => {
                    match self.apply_binlog(&binlog, entry.log_id.index).await {
                        Ok(_) => BinlogResponse::ok(),
                        Err(e) => BinlogResponse::error(e.to_string()),
                    }
                }
                EntryPayload::Membership(mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    BinlogResponse::ok()
                }
            };

            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx = self.snapshot_idx.saturating_add(1);
        KiwiSnapshotBuilder {
            _storage: Arc::clone(&self.storage_swap),
            _idx: self.snapshot_idx,
            snapshot_work_dir: self.snapshot_work_dir.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            _node_id: self._node_id,
            collector: self.collector.clone(),
            cf_tracker: self.cf_tracker.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, openraft::StorageError<u64>> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, KiwiNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<u64>> {
        log::info!("Installing snapshot: meta {:?}", meta.last_log_id);

        // ========== Phase 1: Request pause from StorageServer ==========
        if let Some(ctrl) = &self.pause_controller {
            ctrl.request_pause().await;
        }

        // Define cleanup on error: resume service
        let cleanup_on_error = || {
            if let Some(ctrl) = &self.pause_controller {
                ctrl.resume();
            }
        };

        // ========== Phase 2: Unpack and validate ==========
        let bytes = snapshot.into_inner();

        let unpack_root = tempfile::tempdir().map_err(|e| {
            cleanup_on_error();
            io_err_to_raft(e)
        })?;

        unpack_tar_to_dir(&bytes, unpack_root.path()).map_err(|e| {
            cleanup_on_error();
            io_err_to_raft(e)
        })?;

        let checkpoint_root = unpacked_checkpoint_root(unpack_root.path());

        // P0: Validate metadata consistency
        let file_meta = RaftSnapshotMeta::read_from_dir(&checkpoint_root).map_err(|e| {
            cleanup_on_error();
            io_err_to_raft(e)
        })?;

        let expected_index = meta.last_log_id.map(|l| l.index).unwrap_or(0);
        let expected_term = meta.last_log_id.map(|l| l.leader_id.term).unwrap_or(0);

        if file_meta.last_included_index != expected_index
            || file_meta.last_included_term != expected_term
        {
            cleanup_on_error();
            return Err(StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                io::Error::other(format!(
                    "Snapshot metadata mismatch: file=(index={}, term={}), expected=(index={}, term={})",
                    file_meta.last_included_index,
                    file_meta.last_included_term,
                    expected_index,
                    expected_term
                )),
            ));
        }

        log::info!(
            "Snapshot metadata validated: index={}, term={}",
            file_meta.last_included_index,
            file_meta.last_included_term
        );

        // ========== Phase 3: Close old Storage (release RocksDB lock) ==========
        let current_storage = self.storage_swap.load_full();

        // Save needed values before closing
        let db_instance_num = current_storage.db_instance_num;
        let db_id = current_storage.db_id;

        // Close old Storage to release RocksDB lock before restoring checkpoint.
        // ArcSwap guarantees we have exclusive access during pause.
        {
            // Note: During normal operation, there may be other Arc references from
            // pending requests or Raft apply operations. We need to close via ArcSwap.
            // Use a different approach: create a temporary "closed" placeholder.
            //
            // Actually, we can call close() on the Storage directly since RocksDB
            // close doesn't require exclusive access - it's idempotent.
            // But to ensure proper cleanup, we create a new empty Storage as placeholder,
            // swap it in, then drop the old one.

            // Create placeholder Storage (not opened)
            let placeholder = Arc::new(Storage::new(db_instance_num, db_id));

            // Swap placeholder in - this releases ArcSwap's reference to old Storage
            self.storage_swap.swap(placeholder);

            // Now current_storage (the old Arc) is the only reference left
            // It will be dropped at the end of this block, releasing RocksDB lock
        }

        log::info!("Old Storage dropped, RocksDB lock released");

        // ========== Phase 4: Restore checkpoint (atomic operation) ==========
        restore_checkpoint_layout(&checkpoint_root, &self.db_path, db_instance_num).map_err(
            |e| {
                cleanup_on_error();
                io_err_to_raft(e)
            },
        )?;

        // ========== Phase 5: Create new Storage and open ==========
        let mut new_storage = Storage::new(db_instance_num, db_id);

        let options = Arc::new(StorageOptions::default());
        new_storage.open(options, &self.db_path).map_err(|e| {
            cleanup_on_error();
            storage_err_to_raft(e)
        })?;

        // ========== Phase 6: Swap ArcSwap (atomic switch) ==========
        self.storage_swap.swap(Arc::new(new_storage));
        log::info!("Storage swapped to new instance after snapshot installation");

        // Refresh collector and cf_tracker from the new storage.
        // The new storage creates its own collector/tracker instances during open().
        // After swap, we must use the new ones instead of the orphaned old references.
        let storage_after_swap = self.storage_swap.load();
        if let Some(new_collector) = storage_after_swap.get_logindex_collector(0) {
            self.collector = new_collector;
        }
        if let Some(new_cf_tracker) = storage_after_swap.get_logindex_cf_tracker(0) {
            self.cf_tracker = new_cf_tracker;
        }
        drop(storage_after_swap);

        // ========== Phase 7: Update state and persist ==========
        // Initialize cf_tracker from restored SST properties
        self.init_cf_tracker().map_err(|e| {
            cleanup_on_error();
            StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
        })?;

        // Restore collector state from snapshot metadata
        // This replays the (log_index, seqno) mappings captured during snapshot creation.
        // The restore_collector_state() method parses "log_index:seqno" pairs exported
        // by LogIndexAndSequenceCollector::export_state() and replays them.
        file_meta.restore_collector_state(&self.collector);

        // Purge collector entries for indices compacted into the snapshot.
        // This immediately compacts restored pairs to a single boundary entry at
        // last_included_index, which is acceptable since the follower will receive
        // new entries via replication after this snapshot is installed.
        self.collector
            .purge(file_meta.last_included_index as storage::logindex::LogIndex);

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        persist_current_snapshot(&self.snapshot_work_dir, meta, &bytes).inspect_err(|_| {
            cleanup_on_error();
        })?;

        // ========== Phase 8: Resume StorageServer ==========
        // Resume only after all state updates are complete, so applied_state()
        // returns correct values if queries arrive immediately after resume.
        if let Some(ctrl) = &self.pause_controller {
            ctrl.resume();
        }

        drop(unpack_root);
        log::info!("Snapshot installation complete");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<KiwiTypeConfig>>, openraft::StorageError<u64>> {
        load_current_snapshot(&self.snapshot_work_dir)
    }

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, KiwiNode>), openraft::StorageError<u64>>
    {
        // On first access, lazily load from persisted snapshot to recover last_applied
        // after restart (otherwise openraft would scan from index 0 and fail if logs were purged).
        if self.last_applied.is_none() {
            if let Ok(Some(snap)) = load_current_snapshot(&self.snapshot_work_dir) {
                self.last_applied = snap.meta.last_log_id;
                self.last_membership = snap.meta.last_membership.clone();
                log::info!(
                    "Recovered last_applied={:?} from persisted snapshot",
                    self.last_applied
                );
            }
        }
        Ok((self.last_applied, self.last_membership.clone()))
    }
}

pub struct KiwiSnapshotBuilder {
    _storage: Arc<ArcSwap<Storage>>,
    _idx: u64,
    snapshot_work_dir: PathBuf,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, KiwiNode>,
    _node_id: u64,
    // Reserved for future LogIndex synchronization during snapshot creation
    #[allow(dead_code)]
    collector: Arc<LogIndexAndSequenceCollector>,
    #[allow(dead_code)]
    cf_tracker: Arc<LogIndexOfColumnFamilies>,
}

impl RaftSnapshotBuilder<KiwiTypeConfig> for KiwiSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<KiwiTypeConfig>, StorageError<u64>> {
        // Use tempfile to ensure automatic cleanup on error.
        let temp_dir = tempfile::tempdir().map_err(io_err_to_raft)?;
        let dir = temp_dir.path().join(format!("build-{}", self._idx));
        std::fs::create_dir_all(&dir).map_err(io_err_to_raft)?;

        // Use last_applied to ensure (index, term) pair comes from the same log entry.
        let (last_idx, last_term) = if let Some(last_log_id) = self.last_applied {
            (last_log_id.index, last_log_id.leader_id.term)
        } else {
            (0, 0)
        };

        // Create snapshot meta with collector state
        let storage = self._storage.load_full();
        let collector = storage
            .get_logindex_collector(0)
            .expect("logindex collector for instance 0");
        let raft_meta = RaftSnapshotMeta::with_collector_state(last_idx, last_term, &collector);
        storage
            .create_checkpoint(&dir, &raft_meta)
            .map_err(storage_err_to_raft)?;

        let bytes = pack_dir_to_vec(&dir).map_err(io_err_to_raft)?;

        let leader_id = self
            .last_applied
            .map(|l| l.leader_id)
            .unwrap_or(CommittedLeaderId::new(0, self._node_id));

        let last_log_id = Some(LogId::new(leader_id, raft_meta.last_included_index));

        let meta = SnapshotMeta {
            last_log_id,
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snapshot-{}", self._idx),
        };

        persist_current_snapshot(&self.snapshot_work_dir, &meta, &bytes)?;

        // Purge collector entries that are now covered by the snapshot.
        // This prevents unbounded memory growth as the leader continues accepting writes.
        collector.purge(raft_meta.last_included_index as storage::logindex::LogIndex);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}
