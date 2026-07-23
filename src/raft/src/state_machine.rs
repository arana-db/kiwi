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
//! 1. The archive is unpacked, validated and staged without touching live data.
//! 2. StorageServer is paused and all hot-swappable Storage owners are drained.
//! 3. A durable recovery marker is written before the old Storage is detached.
//! 4. The staged checkpoint is committed and ArcSwap switches to the restored Storage.
//! 5. Snapshot state is durably persisted, the marker is removed, and requests resume.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;

use arc_swap::ArcSwap;
use openraft::{
    CommittedLeaderId, EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, storage::RaftStateMachine,
};
use storage::storage::Storage;
use storage::{
    RaftSnapshotMeta, StorageOptions, prepare_checkpoint_restore, sync_parent_directory,
};

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
const SNAPSHOT_INSTALL_MARKER_VERSION: u32 = 1;
const SNAPSHOT_INSTALL_MARKER_SUFFIX: &str = ".snapshot-install-in-progress.json";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct SnapshotInstallMarker {
    version: u32,
    id: String,
    index: u64,
    term: u64,
    db: PathBuf,
    workdir: PathBuf,
    instances: usize,
}

/// Return the stable sibling marker used to reject startup after an incomplete install.
pub fn snapshot_install_marker_path(db_path: &Path) -> io::Result<PathBuf> {
    let file_name = db_path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("database path has no file name: {}", db_path.display()),
        )
    })?;
    let parent = db_path.parent().filter(|path| !path.as_os_str().is_empty());
    let mut marker_name = OsString::from(".");
    marker_name.push(file_name);
    marker_name.push(SNAPSHOT_INSTALL_MARKER_SUFFIX);
    Ok(parent.unwrap_or_else(|| Path::new(".")).join(marker_name))
}

fn incomplete_install_error(marker_path: &Path, detail: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "snapshot install recovery marker {} blocks startup: {detail}; the node must remain stopped and rejoin from a healthy leader with a new node ID and clean DB and Raft data directories",
            marker_path.display()
        ),
    )
}

/// Fail closed whenever an install marker exists, including malformed and unknown-version files.
pub fn preflight_snapshot_install(db_path: &Path) -> io::Result<()> {
    let marker_path = snapshot_install_marker_path(db_path)?;
    if !marker_path.try_exists()? {
        return Ok(());
    }

    let bytes = std::fs::read(&marker_path).map_err(|error| {
        incomplete_install_error(&marker_path, format!("failed to read marker: {error}"))
    })?;
    let marker: SnapshotInstallMarker = serde_json::from_slice(&bytes).map_err(|error| {
        incomplete_install_error(&marker_path, format!("marker is malformed: {error}"))
    })?;
    if marker.version != SNAPSHOT_INSTALL_MARKER_VERSION {
        return Err(incomplete_install_error(
            &marker_path,
            format!(
                "unsupported marker version {}, expected {}",
                marker.version, SNAPSHOT_INSTALL_MARKER_VERSION
            ),
        ));
    }
    Err(incomplete_install_error(
        &marker_path,
        format!(
            "snapshot {} at index {} term {} did not complete",
            marker.id, marker.index, marker.term
        ),
    ))
}

fn write_and_sync(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn persist_install_marker(path: &Path, marker: &SnapshotInstallMarker) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_parent_directory(path)?;
    Ok(bytes)
}

fn remove_install_marker(path: &Path, marker_bytes: &[u8]) -> io::Result<()> {
    std::fs::remove_file(path)?;
    if let Err(sync_error) = sync_parent_directory(path) {
        let restore_result = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .and_then(|mut file| {
                file.write_all(marker_bytes)?;
                file.sync_all()?;
                sync_parent_directory(path)
            });
        return Err(io::Error::other(format!(
            "failed to durably remove install marker {}: {sync_error}; marker restore result: {restore_result:?}",
            path.display()
        )));
    }
    Ok(())
}

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
    write_and_sync(&data_tmp, bytes).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
    })?;

    let json = serde_json::to_string_pretty(meta).map_err(|e| {
        StorageError::from_io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            io::Error::other(e.to_string()),
        )
    })?;
    write_and_sync(&meta_tmp, json.as_bytes()).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
    })?;

    // Atomic rename (on POSIX systems, rename within same filesystem is atomic)
    // For Windows, see atomic_replace functions below
    atomic_replace_file(&data_tmp, &data_path)?;
    atomic_replace_file(&meta_tmp, &meta_path)?;
    sync_parent_directory(&meta_path).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
    })?;

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

/// Atomically replace a snapshot file within one directory.
#[allow(clippy::result_large_err)]
fn atomic_replace_file(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> Result<(), StorageError<u64>> {
    std::fs::rename(src, dst).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
    })?;

    Ok(())
}

/// Pause controller for coordinating with StorageServer during snapshot installation.
pub trait StorageAccessPermit: Send {}

pub trait PauseController: Send + Sync {
    /// Request pause: wait for all pending requests to complete.
    fn request_pause(&self)
    -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;

    /// Enter the shared Storage access gate and hold the returned permit for as
    /// long as any owner loaded from the hot-swappable Storage remains alive.
    fn enter(
        self: Arc<Self>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>,
    >;

    /// Resume: allow new requests to proceed.
    fn resume(&self);
}

struct ResumeBeforeMarkerGuard {
    controller: Arc<dyn PauseController>,
    armed: bool,
}

impl ResumeBeforeMarkerGuard {
    fn new(controller: Arc<dyn PauseController>) -> Self {
        Self {
            controller,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ResumeBeforeMarkerGuard {
    fn drop(&mut self) {
        if self.armed {
            self.controller.resume();
        }
    }
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
    pause_controller: Arc<dyn PauseController>,
    /// Serializes snapshot-visible state with apply, build and install operations.
    snapshot_operation_gate: Arc<tokio::sync::Mutex<()>>,
    /// Shared Raft append-log callback used to re-arm restored Storage after snapshot install.
    append_log_fn: Option<Arc<OnceLock<storage::AppendLogFn>>>,
}

impl KiwiStateMachine {
    /// Create a new state machine.
    ///
    /// Per-instance LogIndex collectors and cf_trackers are owned by the underlying
    /// Storage; snapshot build/install paths look them up through `storage_swap` so
    /// they remain valid after a hot swap.
    pub fn new(
        node_id: u64,
        storage_swap: Arc<ArcSwap<Storage>>,
        db_path: PathBuf,
        snapshot_work_dir: PathBuf,
        pause_controller: Arc<dyn PauseController>,
        append_log_fn: Option<Arc<OnceLock<storage::AppendLogFn>>>,
    ) -> Self {
        Self {
            _node_id: node_id,
            storage_swap,
            db_path,
            snapshot_work_dir,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
            pause_controller,
            snapshot_operation_gate: Arc::new(tokio::sync::Mutex::new(())),
            append_log_fn,
        }
    }

    /// Initialize cf_tracker from restored SST properties after snapshot install
    /// or from existing DB on startup
    pub fn init_cf_tracker(&self) -> Result<(), io::Error> {
        let storage = self.storage_swap.load_full();
        storage.init_cf_trackers().map_err(io::Error::other)
    }

    fn rearm_append_log_fn(&self, storage: &Storage) {
        if let Some(append_log_fn) = self.append_log_fn.as_ref().and_then(|holder| holder.get()) {
            storage.set_append_log_fn(append_log_fn.clone());
        }
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
        let _snapshot_operation = Arc::clone(&self.snapshot_operation_gate).lock_owned().await;
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
        let snapshot_operation = Arc::clone(&self.snapshot_operation_gate).lock_owned().await;
        self.snapshot_idx = self.snapshot_idx.saturating_add(1);
        KiwiSnapshotBuilder {
            storage: self.storage_swap.load_full(),
            _snapshot_operation: snapshot_operation,
            _idx: self.snapshot_idx,
            snapshot_work_dir: self.snapshot_work_dir.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            _node_id: self._node_id,
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
        let bytes = snapshot.into_inner();

        // Stage and validate the complete checkpoint before pausing request
        // processing. Failures in this phase cannot affect the live Storage.
        let unpack_root = tempfile::tempdir().map_err(io_err_to_raft)?;
        unpack_tar_to_dir(&bytes, unpack_root.path()).map_err(io_err_to_raft)?;

        let checkpoint_root = unpacked_checkpoint_root(unpack_root.path());
        let file_meta =
            RaftSnapshotMeta::read_from_dir(&checkpoint_root).map_err(io_err_to_raft)?;

        let expected_index = meta.last_log_id.map(|l| l.index).unwrap_or(0);
        let expected_term = meta.last_log_id.map(|l| l.leader_id.term).unwrap_or(0);

        if file_meta.last_included_index != expected_index
            || file_meta.last_included_term != expected_term
        {
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

        // Loading the current Storage here is safe: prepare only reads and copies
        // checkpoint input, and the owner remains live until the durable marker
        // has been written after pause/drain.
        let current_storage = self.storage_swap.load_full();
        let db_instance_num = current_storage.db_instance_num;
        let db_id = current_storage.db_id;
        let prepared = prepare_checkpoint_restore(&checkpoint_root, &self.db_path, db_instance_num)
            .map_err(io_err_to_raft)?;

        self.pause_controller.request_pause().await;
        let mut resume_before_marker =
            ResumeBeforeMarkerGuard::new(Arc::clone(&self.pause_controller));
        let _snapshot_operation = Arc::clone(&self.snapshot_operation_gate).lock_owned().await;

        let marker_path = snapshot_install_marker_path(&self.db_path).map_err(io_err_to_raft)?;
        let marker = SnapshotInstallMarker {
            version: SNAPSHOT_INSTALL_MARKER_VERSION,
            id: meta.snapshot_id.clone(),
            index: expected_index,
            term: expected_term,
            db: self.db_path.clone(),
            workdir: self.snapshot_work_dir.clone(),
            instances: db_instance_num,
        };
        let marker_bytes = persist_install_marker(&marker_path, &marker).map_err(|error| {
            StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                io::Error::new(
                    error.kind(),
                    format!(
                        "failed to durably create snapshot install marker {} before changing live storage: {error}",
                        marker_path.display()
                    ),
                ),
            )
        })?;
        resume_before_marker.disarm();

        let post_marker_error = |context: &str, error: &dyn std::fmt::Display| {
            StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                io::Error::other(format!(
                    "snapshot install failed after durable recovery marker {} was written while {context}: {error}; storage access remains paused and the marker must remain in place; rejoin from a healthy leader with a new node ID and clean DB and Raft data directories",
                    marker_path.display()
                )),
            )
        };

        let placeholder = Arc::new(Storage::new(db_instance_num, db_id));
        self.storage_swap.swap(placeholder);
        drop(current_storage);
        log::info!("Old Storage dropped, RocksDB lock released");

        prepared
            .commit()
            .map_err(|error| post_marker_error("committing the staged checkpoint", &error))?;

        let mut new_storage = Storage::new(db_instance_num, db_id);
        let options = Arc::new(StorageOptions::default());
        new_storage
            .open(options, &self.db_path)
            .map_err(|error| post_marker_error("opening the restored storage", &error))?;
        self.rearm_append_log_fn(&new_storage);

        self.storage_swap.swap(Arc::new(new_storage));
        log::info!("Storage swapped to new instance after snapshot installation");

        self.init_cf_tracker()
            .map_err(|error| post_marker_error("initializing restored CF trackers", &error))?;

        // Restore each instance's collector state from snapshot metadata. The new
        // storage created its own collector/tracker instances during open(), so we
        // must look them up through storage_swap rather than reusing pre-swap refs.
        let storage_after_swap = self.storage_swap.load();
        let collectors: Vec<_> = (0..db_instance_num)
            .filter_map(|i| storage_after_swap.get_logindex_collector(i))
            .collect();
        file_meta.restore_collector_states(&collectors);

        // Purge collector entries for indices compacted into the snapshot.
        // This immediately compacts restored pairs to a single boundary entry at
        // last_included_index, which is acceptable since the follower will receive
        // new entries via replication after this snapshot is installed.
        let purge_idx = file_meta.last_included_index as storage::logindex::LogIndex;
        for c in &collectors {
            c.purge(purge_idx);
        }
        drop(storage_after_swap);

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        persist_current_snapshot(&self.snapshot_work_dir, meta, &bytes).map_err(|error| {
            let context = format!(
                "persisting the installed current snapshot under {}",
                self.snapshot_work_dir.display()
            );
            post_marker_error(&context, &error)
        })?;

        remove_install_marker(&marker_path, &marker_bytes)
            .map_err(|error| post_marker_error("removing the durable install marker", &error))?;

        self.pause_controller.resume();

        log::info!("Snapshot installation complete");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<KiwiTypeConfig>>, openraft::StorageError<u64>> {
        let _snapshot_operation = Arc::clone(&self.snapshot_operation_gate).lock_owned().await;
        load_current_snapshot(&self.snapshot_work_dir)
    }

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, KiwiNode>), openraft::StorageError<u64>>
    {
        let _snapshot_operation = Arc::clone(&self.snapshot_operation_gate).lock_owned().await;
        // On first access, lazily load from persisted snapshot to recover last_applied
        // after restart (otherwise openraft would scan from index 0 and fail if logs were purged).
        if self.last_applied.is_none() {
            if let Some(snap) = load_current_snapshot(&self.snapshot_work_dir)? {
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
    storage: Arc<Storage>,
    _snapshot_operation: tokio::sync::OwnedMutexGuard<()>,
    _idx: u64,
    snapshot_work_dir: PathBuf,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, KiwiNode>,
    _node_id: u64,
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

        // Snapshot meta carries each instance's collector state so the receiver can
        // rebuild every (log_index, seqno) mapping, not just instance 0's.
        let collectors: Vec<_> = (0..self.storage.db_instance_num)
            .filter_map(|i| self.storage.get_logindex_collector(i))
            .collect();
        let raft_meta = RaftSnapshotMeta::with_collector_states(last_idx, last_term, &collectors);
        self.storage
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
        let purge_idx = raft_meta.last_included_index as storage::logindex::LogIndex;
        for c in &collectors {
            c.purge(purge_idx);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}
