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

#[cfg(test)]
use std::collections::HashSet;
#[cfg(test)]
use std::sync::LazyLock;

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

/// Number of vector metas / member entries decoded per instance when
/// validating restored snapshot data (sampling, not a full scan).
const RESTORED_VECTOR_SAMPLE_SIZE: usize = 64;
const SNAPSHOT_INSTALL_CLEANUP_SUFFIX: &str = ".cleanup-pending";

#[cfg(test)]
static MARKER_PRIMARY_REMOVAL_SYNC_FAILURES: LazyLock<parking_lot::Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashSet::new()));

#[cfg(test)]
struct MarkerPrimaryRemovalSyncFailureGuard {
    marker_path: PathBuf,
}

#[cfg(test)]
impl Drop for MarkerPrimaryRemovalSyncFailureGuard {
    fn drop(&mut self) {
        MARKER_PRIMARY_REMOVAL_SYNC_FAILURES
            .lock()
            .remove(&self.marker_path);
    }
}

#[cfg(test)]
fn fail_next_marker_primary_removal_sync(
    marker_path: &Path,
) -> MarkerPrimaryRemovalSyncFailureGuard {
    let marker_path = marker_path.to_path_buf();
    assert!(
        MARKER_PRIMARY_REMOVAL_SYNC_FAILURES
            .lock()
            .insert(marker_path.clone()),
        "marker removal sync failure already registered for {}",
        marker_path.display()
    );
    MarkerPrimaryRemovalSyncFailureGuard { marker_path }
}

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

fn snapshot_install_cleanup_marker_path(marker_path: &Path) -> PathBuf {
    let mut cleanup_name = marker_path.as_os_str().to_os_string();
    cleanup_name.push(SNAPSHOT_INSTALL_CLEANUP_SUFFIX);
    PathBuf::from(cleanup_name)
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
    if marker_path.try_exists()? {
        return reject_install_marker(&marker_path, "snapshot install did not complete");
    }

    let cleanup_path = snapshot_install_cleanup_marker_path(&marker_path);
    if cleanup_path.try_exists()? {
        return reject_install_marker(&cleanup_path, "snapshot marker cleanup did not complete");
    }

    Ok(())
}

fn reject_install_marker(marker_path: &Path, state: &str) -> io::Result<()> {
    let bytes = std::fs::read(marker_path).map_err(|error| {
        incomplete_install_error(marker_path, format!("failed to read marker: {error}"))
    })?;
    let marker: SnapshotInstallMarker = serde_json::from_slice(&bytes).map_err(|error| {
        incomplete_install_error(marker_path, format!("marker is malformed: {error}"))
    })?;
    if marker.version != SNAPSHOT_INSTALL_MARKER_VERSION {
        return Err(incomplete_install_error(
            marker_path,
            format!(
                "unsupported marker version {}, expected {}",
                marker.version, SNAPSHOT_INSTALL_MARKER_VERSION
            ),
        ));
    }
    Err(incomplete_install_error(
        marker_path,
        format!(
            "{state}: snapshot {} at index {} term {}",
            marker.id, marker.index, marker.term,
        ),
    ))
}

fn write_and_sync(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn persist_install_marker(path: &Path, marker: &SnapshotInstallMarker) -> io::Result<Vec<u8>> {
    let cleanup_path = snapshot_install_cleanup_marker_path(path);
    if cleanup_path.try_exists()? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "snapshot install cleanup marker already exists: {}",
                cleanup_path.display()
            ),
        ));
    }

    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_parent_directory(path)?;
    Ok(bytes)
}

fn remove_install_marker(path: &Path, marker_bytes: &[u8]) -> io::Result<()> {
    let cleanup_path = snapshot_install_cleanup_marker_path(path);
    let mut cleanup_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&cleanup_path)?;
    cleanup_file.write_all(marker_bytes)?;
    cleanup_file.sync_all()?;
    sync_parent_directory(&cleanup_path)?;

    std::fs::remove_file(path)?;
    sync_marker_parent_after_primary_removal(path)?;

    std::fs::remove_file(&cleanup_path)?;
    if let Err(error) = sync_parent_directory(&cleanup_path) {
        // The cleanup marker was durably created before the primary marker was
        // removed. If this final unlink is not durable, a restart observes
        // either the cleanup marker (and blocks conservatively) or no marker
        // after all restored state was already synchronized.
        log::warn!(
            "Failed to durably remove snapshot cleanup marker {}: {error}",
            cleanup_path.display()
        );
    }
    Ok(())
}

fn sync_marker_parent_after_primary_removal(marker_path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if MARKER_PRIMARY_REMOVAL_SYNC_FAILURES
        .lock()
        .remove(marker_path)
    {
        return Err(io::Error::other(format!(
            "injected marker primary removal sync failure for {}",
            marker_path.display()
        )));
    }

    sync_parent_directory(marker_path)
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
    /// Serializes snapshot-visible state with apply, checkpoint and install operations.
    snapshot_state_gate: Arc<tokio::sync::Mutex<()>>,
    /// Serializes current snapshot publication with build, install and snapshot readers.
    snapshot_publication_gate: Arc<tokio::sync::Mutex<()>>,
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
            snapshot_state_gate: Arc::new(tokio::sync::Mutex::new(())),
            snapshot_publication_gate: Arc::new(tokio::sync::Mutex::new(())),
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
        let _snapshot_state = Arc::clone(&self.snapshot_state_gate).lock_owned().await;
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            let response = match entry.payload {
                EntryPayload::Blank => BinlogResponse::ok(),
                EntryPayload::Normal(binlog) => {
                    // Persist the mutation BEFORE advancing applied state. A binlog
                    // carries physical put/delete ops only; deterministic business
                    // rejections (e.g. WRONGTYPE) are decided upstream at command
                    // execution and never reach apply. Any failure here (I/O,
                    // corruption, invalid CF/slot) is therefore a fatal storage
                    // error and must abort apply rather than be reported as a normal
                    // response. Advancing `last_applied` past an entry that did not
                    // durably commit would let this replica claim progress it does
                    // not hold, diverging from the rest of the group.
                    if let Err(e) = self.apply_binlog(&binlog, log_id.index).await {
                        // Diagnostics only: log id, slot and mutation shape. Keys and
                        // values are never logged.
                        log::error!(
                            "fatal storage error applying binlog: log_id={:?}, slot={}, ops={}",
                            log_id,
                            binlog.slot_idx,
                            binlog.entries.len()
                        );
                        return Err(io_err_to_raft(e));
                    }
                    BinlogResponse::ok()
                }
                EntryPayload::Membership(mem) => {
                    self.last_membership = StoredMembership::new(Some(log_id), mem);
                    BinlogResponse::ok()
                }
            };

            // Reached only after the entry has been durably applied.
            self.last_applied = Some(log_id);
            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let snapshot_publication = Arc::clone(&self.snapshot_publication_gate)
            .lock_owned()
            .await;
        let snapshot_state = Arc::clone(&self.snapshot_state_gate).lock_owned().await;
        self.snapshot_idx = self.snapshot_idx.saturating_add(1);
        KiwiSnapshotBuilder {
            storage: self.storage_swap.load_full(),
            snapshot_state_guard: Some(snapshot_state),
            snapshot_publication_guard: Some(snapshot_publication),
            #[cfg(test)]
            checkpoint_completed_hook: None,
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

        // Loading the current Storage here is safe: schema validation and
        // prepare only read and copy checkpoint input, and the owner remains
        // live until the durable marker has been written after pause/drain.
        let current_storage = self.storage_swap.load_full();
        let db_instance_num = current_storage.db_instance_num;
        let db_id = current_storage.db_id;

        // Deterministically reject snapshots whose storage schema this binary
        // cannot consume (version, instance count, column families, vector
        // value format), before touching live storage.
        file_meta
            .validate_for_restore(db_instance_num)
            .map_err(io_err_to_raft)?;

        log::info!(
            "Snapshot metadata validated: index={}, term={}",
            file_meta.last_included_index,
            file_meta.last_included_term
        );

        let options = current_storage.storage_options().unwrap_or_else(|| {
            log::warn!(
                "snapshot install found unopened Storage without configured options; using defaults"
            );
            Arc::new(StorageOptions::default())
        });
        let prepared = prepare_checkpoint_restore(&checkpoint_root, &self.db_path, db_instance_num)
            .map_err(io_err_to_raft)?;
        prepared
            .validate_storage_incarnations(&file_meta.storage_incarnations)
            .map_err(io_err_to_raft)?;

        // Open and sample the disposable staged copy before pausing live
        // storage. All RocksDB handles must be released before rename.
        let mut staged_storage = Storage::new(db_instance_num, db_id);
        let staged_rx = staged_storage
            .open(Arc::clone(&options), prepared.staged_path())
            .map_err(storage_err_to_raft)?;
        staged_storage
            .validate_vector_data_sample(RESTORED_VECTOR_SAMPLE_SIZE)
            .map_err(storage_err_to_raft)?;
        staged_storage.shutdown().await;
        staged_storage.close();
        drop(staged_rx);
        drop(staged_storage);

        let _snapshot_publication = Arc::clone(&self.snapshot_publication_gate)
            .lock_owned()
            .await;
        self.pause_controller.request_pause().await;
        let mut resume_before_marker =
            ResumeBeforeMarkerGuard::new(Arc::clone(&self.pause_controller));
        let _snapshot_state = Arc::clone(&self.snapshot_state_gate).lock_owned().await;

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
        let _snapshot_publication = Arc::clone(&self.snapshot_publication_gate)
            .lock_owned()
            .await;
        load_current_snapshot(&self.snapshot_work_dir)
    }

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, KiwiNode>), openraft::StorageError<u64>>
    {
        let _snapshot_publication = Arc::clone(&self.snapshot_publication_gate)
            .lock_owned()
            .await;
        let _snapshot_state = Arc::clone(&self.snapshot_state_gate).lock_owned().await;
        // On first access, lazily load from persisted snapshot to recover last_applied
        // after restart (otherwise openraft would scan from index 0 and fail if logs were purged).
        if self.last_applied.is_none()
            && let Some(snap) = load_current_snapshot(&self.snapshot_work_dir)?
        {
            self.last_applied = snap.meta.last_log_id;
            self.last_membership = snap.meta.last_membership.clone();
            log::info!(
                "Recovered last_applied={:?} from persisted snapshot",
                self.last_applied
            );
        }
        Ok((self.last_applied, self.last_membership.clone()))
    }
}

#[cfg(test)]
#[derive(Default)]
struct SnapshotCheckpointTestHook {
    checkpoint_completed: tokio::sync::Notify,
    continue_build: tokio::sync::Notify,
}

pub struct KiwiSnapshotBuilder {
    storage: Arc<Storage>,
    snapshot_state_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    snapshot_publication_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    #[cfg(test)]
    checkpoint_completed_hook: Option<Arc<SnapshotCheckpointTestHook>>,
    _idx: u64,
    snapshot_work_dir: PathBuf,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, KiwiNode>,
    _node_id: u64,
}

impl KiwiSnapshotBuilder {
    fn create_checkpoint(
        &self,
        dir: &Path,
        snapshot_state_guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> storage::error::Result<RaftSnapshotMeta> {
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
        let raft_meta =
            RaftSnapshotMeta::for_storage(last_idx, last_term, &collectors, &self.storage)?;
        self.storage.create_checkpoint(dir, &raft_meta)?;

        drop(snapshot_state_guard);
        Ok(raft_meta)
    }
}

impl RaftSnapshotBuilder<KiwiTypeConfig> for KiwiSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<KiwiTypeConfig>, StorageError<u64>> {
        let snapshot_publication_guard = self.snapshot_publication_guard.take();
        let snapshot_state_guard = self.snapshot_state_guard.take();
        let _snapshot_publication_guard = snapshot_publication_guard.ok_or_else(|| {
            io_err_to_raft(io::Error::other(
                "snapshot publication guard is missing before building the snapshot",
            ))
        })?;
        let snapshot_state_guard = snapshot_state_guard.ok_or_else(|| {
            io_err_to_raft(io::Error::other(
                "snapshot state guard is missing before building the snapshot",
            ))
        })?;

        // Use tempfile to ensure automatic cleanup on error.
        let temp_dir = tempfile::tempdir().map_err(io_err_to_raft)?;
        let dir = temp_dir.path().join(format!("build-{}", self._idx));
        std::fs::create_dir_all(&dir).map_err(io_err_to_raft)?;

        let raft_meta = self
            .create_checkpoint(&dir, snapshot_state_guard)
            .map_err(storage_err_to_raft)?;

        #[cfg(test)]
        if let Some(hook) = &self.checkpoint_completed_hook {
            hook.checkpoint_completed.notify_one();
            hook.continue_build.notified().await;
        }

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
        let collectors: Vec<_> = (0..self.storage.db_instance_num)
            .filter_map(|i| self.storage.get_logindex_collector(i))
            .collect();
        for c in &collectors {
            c.purge(purge_idx);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

#[cfg(test)]
mod snapshot_gate_tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use openraft::{Entry, LeaderId};
    use storage::{safe_cleanup_test_db, unique_test_db_path};

    use super::*;

    const SNAPSHOT_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    #[derive(Default)]
    struct CountingPauseController {
        paused: AtomicBool,
        pause_count: AtomicUsize,
        resume_count: AtomicUsize,
        state_changed: tokio::sync::Notify,
    }

    struct TestStorageAccessPermit;

    impl StorageAccessPermit for TestStorageAccessPermit {}

    impl PauseController for CountingPauseController {
        fn request_pause(
            &self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(async {
                self.pause_count.fetch_add(1, Ordering::SeqCst);
                self.paused.store(true, Ordering::SeqCst);
            })
        }

        fn enter(
            self: Arc<Self>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>,
        > {
            Box::pin(async move {
                loop {
                    let resumed = self.state_changed.notified();
                    tokio::pin!(resumed);
                    resumed.as_mut().enable();

                    if !self.paused.load(Ordering::SeqCst) {
                        return Box::new(TestStorageAccessPermit) as Box<dyn StorageAccessPermit>;
                    }
                    resumed.await;
                }
            })
        }

        fn resume(&self) {
            self.resume_count.fetch_add(1, Ordering::SeqCst);
            self.paused.store(false, Ordering::SeqCst);
            self.state_changed.notify_waiters();
        }
    }

    async fn close_storage(storage: Arc<Storage>) {
        let mut storage = match Arc::try_unwrap(storage) {
            Ok(storage) => storage,
            Err(_) => panic!("test storage should not retain Arc references during cleanup"),
        };
        storage.shutdown().await;
        storage.close();
    }

    #[tokio::test]
    async fn snapshot_build_releases_apply_gate_after_checkpoint() {
        let db_path = unique_test_db_path();
        let snapshot_work_dir = unique_test_db_path();
        std::fs::create_dir_all(&snapshot_work_dir)
            .expect("snapshot work directory should be created");

        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _storage_rx = storage
            .open(options, &db_path)
            .expect("test storage should open");
        let storage_swap = Arc::new(ArcSwap::from_pointee(storage));
        let controller = Arc::new(CountingPauseController::default());
        let mut state_machine = KiwiStateMachine::new(
            1,
            Arc::clone(&storage_swap),
            db_path.clone(),
            snapshot_work_dir.clone(),
            controller,
            None,
        );

        let mut builder = state_machine.get_snapshot_builder().await;
        let hook = Arc::new(SnapshotCheckpointTestHook::default());
        builder.checkpoint_completed_hook = Some(Arc::clone(&hook));
        let build = tokio::spawn(async move {
            let result = builder.build_snapshot().await;
            (builder, result)
        });
        tokio::time::timeout(SNAPSHOT_TEST_TIMEOUT, hook.checkpoint_completed.notified())
            .await
            .expect("snapshot build should reach the post-checkpoint barrier");
        assert!(
            !build.is_finished(),
            "snapshot build should remain blocked before archive and persistence"
        );

        let blank = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: EntryPayload::Blank,
        };
        tokio::time::timeout(SNAPSHOT_TEST_TIMEOUT, state_machine.apply([blank]))
            .await
            .expect("apply should proceed after checkpoint while builder remains alive")
            .expect("blank entry should apply");

        hook.continue_build.notify_one();
        let (builder, snapshot) = tokio::time::timeout(SNAPSHOT_TEST_TIMEOUT, build)
            .await
            .expect("snapshot build should resume after the test barrier")
            .expect("snapshot build task should not panic");
        snapshot.expect("snapshot build should complete");
        drop(builder);
        drop(state_machine);
        let storage = storage_swap.load_full();
        drop(storage_swap);
        close_storage(storage).await;
        drop(_storage_rx);
        safe_cleanup_test_db(&db_path);
        safe_cleanup_test_db(&snapshot_work_dir);
    }

    #[tokio::test]
    async fn snapshot_checkpoint_error_releases_apply_gate_while_builder_remains_alive() {
        let db_path = unique_test_db_path();
        let snapshot_work_dir = unique_test_db_path();
        std::fs::create_dir_all(&snapshot_work_dir)
            .expect("snapshot work directory should be created");

        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _storage_rx = storage
            .open(options, &db_path)
            .expect("test storage should open");
        let storage_swap = Arc::new(ArcSwap::from_pointee(storage));
        let controller = Arc::new(CountingPauseController::default());
        let mut state_machine = KiwiStateMachine::new(
            1,
            Arc::clone(&storage_swap),
            db_path.clone(),
            snapshot_work_dir.clone(),
            controller,
            None,
        );

        let mut builder = state_machine.get_snapshot_builder().await;
        let snapshot_state_guard = builder
            .snapshot_state_guard
            .take()
            .expect("builder should own the snapshot state guard");
        let checkpoint_root = tempfile::tempdir().expect("checkpoint root should be created");
        let checkpoint_file = checkpoint_root.path().join("not-a-directory");
        std::fs::write(
            &checkpoint_file,
            b"file blocks checkpoint directory creation",
        )
        .expect("checkpoint blocker file should be written");
        builder
            .create_checkpoint(&checkpoint_file, snapshot_state_guard)
            .expect_err("checkpoint creation should reject a regular file path");

        let blank = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: EntryPayload::Blank,
        };
        tokio::time::timeout(SNAPSHOT_TEST_TIMEOUT, state_machine.apply([blank]))
            .await
            .expect("apply should proceed after checkpoint failure while builder remains alive")
            .expect("blank entry should apply");

        drop(builder);
        drop(state_machine);
        let storage = storage_swap.load_full();
        drop(storage_swap);
        close_storage(storage).await;
        drop(_storage_rx);
        safe_cleanup_test_db(&db_path);
        safe_cleanup_test_db(&snapshot_work_dir);
    }

    #[tokio::test]
    async fn aborted_install_after_pause_while_waiting_for_state_gate_resumes() {
        let db_path = unique_test_db_path();
        let snapshot_work_dir = unique_test_db_path();
        std::fs::create_dir_all(&snapshot_work_dir)
            .expect("snapshot work directory should be created");

        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _storage_rx = storage
            .open(options, &db_path)
            .expect("test storage should open");
        storage
            .set(b"live-key", b"live-value")
            .expect("live data should be written");
        let storage_swap = Arc::new(ArcSwap::from_pointee(storage));
        let controller = Arc::new(CountingPauseController::default());
        let mut state_machine = KiwiStateMachine::new(
            1,
            Arc::clone(&storage_swap),
            db_path.clone(),
            snapshot_work_dir.clone(),
            controller.clone(),
            None,
        );

        let mut builder = state_machine.get_snapshot_builder().await;
        let snapshot = builder
            .build_snapshot()
            .await
            .expect("test snapshot should build");
        drop(builder);
        let snapshot_meta = snapshot.meta;
        let snapshot_bytes = snapshot.snapshot.into_inner();
        let old_storage = storage_swap.load_full();
        let marker_path =
            snapshot_install_marker_path(&db_path).expect("snapshot marker path should be derived");
        let state_guard = Arc::clone(&state_machine.snapshot_state_gate)
            .lock_owned()
            .await;

        let install = tokio::spawn(async move {
            state_machine
                .install_snapshot(
                    &snapshot_meta,
                    Box::new(std::io::Cursor::new(snapshot_bytes)),
                )
                .await
        });
        tokio::time::timeout(SNAPSHOT_TEST_TIMEOUT, async {
            while controller.pause_count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("install should pause before waiting for the state gate");
        assert!(!marker_path.exists());
        assert!(Arc::ptr_eq(&storage_swap.load_full(), &old_storage));

        install.abort();
        let cancelled = install.await;
        assert!(cancelled.is_err(), "install task should be cancelled");
        drop(state_guard);

        assert_eq!(controller.pause_count.load(Ordering::SeqCst), 1);
        assert_eq!(controller.resume_count.load(Ordering::SeqCst), 1);
        assert!(!controller.paused.load(Ordering::SeqCst));
        assert!(!marker_path.exists());
        assert!(Arc::ptr_eq(&storage_swap.load_full(), &old_storage));
        assert_eq!(
            old_storage
                .get(b"live-key")
                .expect("live storage should remain readable"),
            "live-value"
        );
        let permit = tokio::time::timeout(SNAPSHOT_TEST_TIMEOUT, Arc::clone(&controller).enter())
            .await
            .expect("storage access should resume after cancelling install");
        drop(permit);

        drop(storage_swap);
        close_storage(old_storage).await;
        drop(_storage_rx);
        safe_cleanup_test_db(&db_path);
        safe_cleanup_test_db(&snapshot_work_dir);
    }
}

#[cfg(test)]
mod marker_cleanup_tests {
    use super::*;

    #[test]
    fn primary_marker_removal_sync_failure_leaves_restart_blocker() {
        let root = tempfile::tempdir().expect("temporary directory should be created");
        let db_path = root.path().join("db");
        let marker_path = snapshot_install_marker_path(&db_path)
            .expect("marker path should be derived from database path");
        let cleanup_path = snapshot_install_cleanup_marker_path(&marker_path);
        let marker = SnapshotInstallMarker {
            version: SNAPSHOT_INSTALL_MARKER_VERSION,
            id: "snapshot-test".to_string(),
            index: 42,
            term: 7,
            db: db_path.clone(),
            workdir: root.path().join("snapshots"),
            instances: 1,
        };
        let marker_bytes =
            persist_install_marker(&marker_path, &marker).expect("marker should be persisted");
        let _failure = fail_next_marker_primary_removal_sync(&marker_path);

        let error = remove_install_marker(&marker_path, &marker_bytes)
            .expect_err("primary marker removal sync must fail at the injected point");
        assert!(
            error
                .to_string()
                .contains("injected marker primary removal sync failure"),
            "unexpected error: {error}"
        );
        assert!(
            cleanup_path.is_file(),
            "a durable cleanup marker must remain at {}",
            cleanup_path.display()
        );

        let restart_error = preflight_snapshot_install(&db_path)
            .expect_err("restart must reject a pending marker cleanup");
        assert!(
            restart_error
                .to_string()
                .contains(&cleanup_path.display().to_string()),
            "restart refusal must identify cleanup marker: {restart_error}"
        );
    }
}

#[cfg(test)]
mod apply_ordering_tests {
    //! Covers issue #333: applied state must only advance after a committed entry
    //! is durably applied, and a storage failure during apply is a fatal error
    //! that aborts apply instead of being reported as a normal response.
    #![allow(clippy::unwrap_used)]

    use std::pin::Pin;

    use conf::raft_type::{BinlogEntry, OperateType};
    use openraft::{Entry, LeaderId};
    use storage::BaseMetaKey;
    use storage::format_strings_value::StringValue;
    use storage::slot_indexer::key_to_slot_id;
    use storage::{
        ColumnFamilyIndex, fail_next_rocks_batch_commit, safe_cleanup_test_db, unique_test_db_path,
    };

    use super::*;

    struct NoopPermit;
    impl StorageAccessPermit for NoopPermit {}

    /// The apply path only takes the snapshot-state gate; it never drives the
    /// pause controller, so a no-op implementation is sufficient here.
    struct NoopPauseController;
    impl PauseController for NoopPauseController {
        fn request_pause(&self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }

        fn enter(
            self: Arc<Self>,
        ) -> Pin<Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>>
        {
            Box::pin(async { Box::new(NoopPermit) as Box<dyn StorageAccessPermit> })
        }

        fn resume(&self) {}
    }

    struct Fixture {
        state_machine: KiwiStateMachine,
        storage_swap: Arc<ArcSwap<Storage>>,
        db_path: PathBuf,
        snapshot_work_dir: PathBuf,
        storage_rx: Box<dyn std::any::Any + Send>,
    }

    struct ClosedFixture {
        db_path: PathBuf,
        snapshot_work_dir: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let db_path = unique_test_db_path();
            let snapshot_work_dir = unique_test_db_path();
            std::fs::create_dir_all(&snapshot_work_dir)
                .expect("snapshot work directory should be created");

            let mut storage = Storage::new(1, 0);
            let options = Arc::new(StorageOptions::default());
            let storage_rx = storage
                .open(options, &db_path)
                .expect("test storage should open");
            let storage_swap = Arc::new(ArcSwap::from_pointee(storage));
            let state_machine = KiwiStateMachine::new(
                1,
                Arc::clone(&storage_swap),
                db_path.clone(),
                snapshot_work_dir.clone(),
                Arc::new(NoopPauseController),
                None,
            );

            Self {
                state_machine,
                storage_swap,
                db_path,
                snapshot_work_dir,
                storage_rx: Box::new(storage_rx),
            }
        }

        async fn close(self) -> ClosedFixture {
            let Fixture {
                state_machine,
                storage_swap,
                db_path,
                snapshot_work_dir,
                storage_rx,
            } = self;
            drop(state_machine);
            let storage = storage_swap.load_full();
            drop(storage_swap);
            let mut storage = match Arc::try_unwrap(storage) {
                Ok(storage) => storage,
                Err(_) => panic!("test storage should not retain Arc references during cleanup"),
            };
            storage.shutdown().await;
            storage.close();
            drop(storage_rx);
            drop(storage);

            ClosedFixture {
                db_path,
                snapshot_work_dir,
            }
        }

        fn rocksdb_path(&self) -> PathBuf {
            self.storage_swap
                .load_full()
                .insts
                .first()
                .and_then(|instance| instance.db())
                .expect("test storage should expose its RocksDB instance")
                .path()
                .to_path_buf()
        }
    }

    impl ClosedFixture {
        async fn assert_reopened_values(self, expected: &[(&[u8], Option<&str>)]) {
            let mut reopened = Storage::new(1, 0);
            let reopened_rx = reopened
                .open(Arc::new(StorageOptions::default()), &self.db_path)
                .expect("test storage should reopen from the original path");

            for (key, value) in expected {
                match value {
                    Some(value) => assert_eq!(
                        reopened
                            .get(key)
                            .expect("reopened storage should read the applied value"),
                        *value
                    ),
                    None => assert!(
                        reopened.get(key).is_err(),
                        "reopened storage must not contain an unapplied key"
                    ),
                }
            }

            reopened.shutdown().await;
            reopened.close();
            drop(reopened_rx);
            drop(reopened);
            safe_cleanup_test_db(&self.db_path);
            safe_cleanup_test_db(&self.snapshot_work_dir);
        }
    }

    fn entry_at(index: u64, payload: EntryPayload<KiwiTypeConfig>) -> Entry<KiwiTypeConfig> {
        Entry {
            log_id: LogId::new(LeaderId::new(1, 1), index),
            payload,
        }
    }

    fn string_put_binlog(key: &[u8], value: &[u8]) -> Binlog {
        Binlog {
            db_id: 0,
            slot_idx: key_to_slot_id(key) as u32,
            entries: vec![BinlogEntry {
                cf_idx: ColumnFamilyIndex::MetaCF as u32,
                op_type: OperateType::Put,
                key: BaseMetaKey::new(key)
                    .encode()
                    .expect("test string key should encode")
                    .to_vec(),
                value: Some(StringValue::new(value.to_vec()).encode().to_vec()),
            }],
        }
    }

    #[tokio::test]
    async fn apply_advances_applied_only_on_success() {
        let mut fx = Fixture::new();
        let key = b"durable-success";
        assert!(fx.state_machine.last_applied.is_none());

        let responses = fx
            .state_machine
            .apply([entry_at(
                5,
                EntryPayload::Normal(string_put_binlog(key, b"value")),
            )])
            .await
            .expect("a valid binlog should apply cleanly");

        assert_eq!(responses.len(), 1);
        assert!(responses[0].success, "successful apply reports success");
        assert_eq!(
            fx.state_machine.last_applied.map(|l| l.index),
            Some(5),
            "applied state advances to the committed entry"
        );
        assert_eq!(
            fx.storage_swap
                .load_full()
                .get(key)
                .expect("applied value should be readable before close"),
            "value"
        );

        fx.close()
            .await
            .assert_reopened_values(&[(key, Some("value"))])
            .await;
    }

    #[tokio::test]
    async fn apply_does_not_advance_applied_on_fatal_storage_error() {
        let mut fx = Fixture::new();
        let key = b"fatal-write";
        let _failure = fail_next_rocks_batch_commit(&fx.rocksdb_path());

        let err = fx
            .state_machine
            .apply([entry_at(
                7,
                EntryPayload::Normal(string_put_binlog(key, b"value")),
            )])
            .await
            .expect_err("a storage failure during apply must surface as a fatal error");
        // A non-empty error is enough; the point is that it is Err, not Ok.
        let _ = err;

        assert!(
            fx.state_machine.last_applied.is_none(),
            "applied state must not advance past an entry that failed to commit"
        );
        assert!(
            fx.storage_swap.load_full().get(key).is_err(),
            "a failed RocksDB commit must not expose the mutation"
        );

        fx.close()
            .await
            .assert_reopened_values(&[(key, None)])
            .await;
    }

    #[tokio::test]
    async fn apply_stops_at_first_fatal_error() {
        let mut fx = Fixture::new();

        fx.state_machine
            .apply([entry_at(
                2,
                EntryPayload::Normal(string_put_binlog(b"before-failure", b"before")),
            )])
            .await
            .expect("the entry before the injected failure should apply");

        let _failure = fail_next_rocks_batch_commit(&fx.rocksdb_path());

        let err = fx
            .state_machine
            .apply([
                entry_at(
                    3,
                    EntryPayload::Normal(string_put_binlog(b"failed-write", b"failed")),
                ),
                entry_at(
                    4,
                    EntryPayload::Normal(string_put_binlog(b"after-failure", b"after")),
                ),
            ])
            .await
            .expect_err("apply must abort at the first fatal error");
        let _ = err;

        assert_eq!(
            fx.state_machine.last_applied.map(|l| l.index),
            Some(2),
            "entries after a fatal error must not be applied"
        );
        assert_eq!(
            fx.storage_swap
                .load_full()
                .get(b"before-failure")
                .expect("the mutation before the failure should remain applied"),
            "before"
        );
        assert!(
            fx.storage_swap.load_full().get(b"failed-write").is_err(),
            "the mutation whose RocksDB commit failed must remain absent"
        );
        assert!(
            fx.storage_swap.load_full().get(b"after-failure").is_err(),
            "the mutation after the failure must not execute"
        );

        fx.close()
            .await
            .assert_reopened_values(&[
                (b"before-failure", Some("before")),
                (b"failed-write", None),
                (b"after-failure", None),
            ])
            .await;
    }
}
