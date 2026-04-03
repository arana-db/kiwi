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
//! TODO: streaming snapshot I/O instead of in-memory buffer (`snapshot_archive` module).
//! TODO: live-install concurrency safety during `install_snapshot` (dual-runtime coordination).
//! TODO: support smallest flushed log index with proper LogId lookup from log store.

use std::io::{self, Cursor};
use std::path::PathBuf;
use std::sync::Arc;

use openraft::{
    CommittedLeaderId, EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, storage::RaftStateMachine,
};

use conf::raft_type::{Binlog, BinlogResponse, KiwiNode, KiwiTypeConfig};
use storage::storage::Storage;
use storage::{RaftSnapshotMeta, restore_checkpoint_layout};

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

#[allow(clippy::result_large_err)] // OpenRaft `StorageError` is large; matches other state-machine helpers.
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
    // This ensures that snapshot files are either completely written or not present at all.
    let data_tmp = work_dir.join(format!(".{}.tmp", CURRENT_SNAPSHOT_DATA));
    let meta_tmp = work_dir.join(format!(".{}.tmp", CURRENT_SNAPSHOT_META));

    // Write to temporary files first
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
    std::fs::rename(&data_tmp, &data_path).map_err(|e| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
    })?;
    std::fs::rename(&meta_tmp, &meta_path).map_err(|e| {
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

pub struct KiwiStateMachine {
    _node_id: u64,
    storage: Arc<Storage>,
    /// Live RocksDB directory (`db_path/0`, `db_path/1`, …).
    db_path: PathBuf,
    /// Working directory for snapshot export/import (checkpoints and unpack).
    snapshot_work_dir: PathBuf,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, KiwiNode>,
    snapshot_idx: u64,
}

impl KiwiStateMachine {
    pub fn new(
        node_id: u64,
        storage: Arc<Storage>,
        db_path: PathBuf,
        snapshot_work_dir: PathBuf,
    ) -> Self {
        Self {
            _node_id: node_id,
            storage,
            db_path,
            snapshot_work_dir,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
        }
    }
}

impl KiwiStateMachine {
    async fn apply_binlog(&self, binlog: &Binlog, _log_idx: u64) -> Result<(), io::Error> {
        self.storage
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
            _storage: Arc::clone(&self.storage),
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
        let bytes = snapshot.into_inner();

        // Unpack snapshot to temporary directory first
        let unpack_root = tempfile::tempdir().map_err(|e| {
            StorageError::from_io_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                io::Error::other(e.to_string()),
            )
        })?;
        unpack_tar_to_dir(&bytes, unpack_root.path()).map_err(io_err_to_raft)?;
        let checkpoint_root = unpacked_checkpoint_root(unpack_root.path());

        let _file_meta = RaftSnapshotMeta::read_from_dir(&checkpoint_root).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e)
        })?;

        // Restore checkpoint layout atomically - handles both empty and non-empty db_path.
        // restore_checkpoint_layout uses temp directory + atomic rename pattern,
        // so it safely replaces any existing data in db_path.
        restore_checkpoint_layout(
            &checkpoint_root,
            &self.db_path,
            self.storage.db_instance_num,
        )
        .map_err(io_err_to_raft)?;

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        persist_current_snapshot(&self.snapshot_work_dir, meta, &bytes)?;

        drop(unpack_root);
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
        Ok((self.last_applied, self.last_membership.clone()))
    }
}

pub struct KiwiSnapshotBuilder {
    _storage: Arc<Storage>,
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
        // This maintains Raft invariant that snapshot metadata must refer to a single log entry.
        let (last_idx, last_term) = if let Some(last_log_id) = self.last_applied {
            (last_log_id.index, last_log_id.leader_id.term)
        } else {
            // No log applied yet, use initial state
            (0, 0)
        };
        let raft_meta = RaftSnapshotMeta::new(last_idx, last_term);

        self._storage
            .create_checkpoint(&dir, &raft_meta)
            .map_err(storage_err_to_raft)?;

        let bytes = pack_dir_to_vec(&dir).map_err(io_err_to_raft)?;

        // temp_dir is automatically cleaned up when dropped

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

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}
