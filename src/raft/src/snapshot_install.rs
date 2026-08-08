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

//! Durable orchestration for crash-recoverable Raft snapshot installation.

use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::SnapshotMeta;
use serde::{Deserialize, Serialize};
use storage::storage::Storage;
use storage::{
    InstanceStorageManifestV2, ManifestDigest, RootStorageManifestV2, SnapshotInstanceManifest,
    StorageOptions, logical_snapshot_digests_from_root, sync_directory, sync_parent_directory,
};

use conf::raft_type::KiwiNode;

pub const CURRENT_SNAPSHOT_DATA: &str = "current_snapshot.tar";
pub const CURRENT_SNAPSHOT_META: &str = "current_snapshot_meta.json";
pub const SNAPSHOT_INSTALL_MARKER_VERSION: u32 = 2;
const SNAPSHOT_INSTALL_MARKER_SUFFIX: &str = ".snapshot-install-in-progress.json";
const STAGED_PREFIX: &str = ".restore_temp_";
const PENDING_PREFIX: &str = ".snapshot-install-";

static INSTALL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotInstallPhase {
    StagedValidated,
    StoragePaused,
    MarkerPersisted,
    OldRenamedToBackup,
    NewRenamedToTarget,
    NewStorageReopened,
    RaftMetadataPersisted,
    CleanupPending,
    RollbackPending,
    RollbackCleanupPending,
    Complete,
}

impl SnapshotInstallPhase {
    fn next(self) -> Option<Self> {
        match self {
            Self::StagedValidated => Some(Self::StoragePaused),
            Self::StoragePaused => Some(Self::MarkerPersisted),
            Self::MarkerPersisted => Some(Self::OldRenamedToBackup),
            Self::OldRenamedToBackup => Some(Self::NewRenamedToTarget),
            Self::NewRenamedToTarget => Some(Self::NewStorageReopened),
            Self::NewStorageReopened => Some(Self::RaftMetadataPersisted),
            Self::RaftMetadataPersisted => Some(Self::CleanupPending),
            Self::CleanupPending => Some(Self::Complete),
            Self::RollbackPending => Some(Self::RollbackCleanupPending),
            Self::RollbackCleanupPending | Self::Complete => None,
        }
    }

    fn allows_transition_to(self, next: Self) -> bool {
        self.next() == Some(next)
            || matches!(
                (self, next),
                (
                    Self::MarkerPersisted | Self::OldRenamedToBackup,
                    Self::RollbackPending
                )
            )
    }

    fn requires_old_identity(self) -> bool {
        !matches!(self, Self::StagedValidated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotInstallStorageIdentity {
    pub root_manifest_id: String,
    pub root_manifest_digest: ManifestDigest,
    pub instance_manifests: Vec<SnapshotInstanceManifest>,
    pub logical_instance_digests: Vec<ManifestDigest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotInstallMarkerV2 {
    pub version: u32,
    pub phase: SnapshotInstallPhase,
    pub snapshot_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub db_instance_num: u32,
    pub target_name: String,
    pub staged_name: String,
    pub backup_name: String,
    pub pending_snapshot_data_name: String,
    pub pending_raft_meta_name: String,
    pub pending_checkpoint_meta_name: String,
    pub snapshot_archive_digest: ManifestDigest,
    pub raft_metadata_digest: ManifestDigest,
    pub checkpoint_metadata_digest: ManifestDigest,
    pub old_storage: Option<SnapshotInstallStorageIdentity>,
    pub new_storage: SnapshotInstallStorageIdentity,
}

impl SnapshotInstallMarkerV2 {
    fn validate(&self) -> io::Result<()> {
        if self.version != SNAPSHOT_INSTALL_MARKER_VERSION {
            return Err(invalid_data(format!(
                "unsupported snapshot install marker version {}, expected {}",
                self.version, SNAPSHOT_INSTALL_MARKER_VERSION
            )));
        }
        if self.snapshot_id.is_empty() || self.db_instance_num == 0 {
            return Err(invalid_data(
                "snapshot install marker has an empty identity or zero instances",
            ));
        }
        for name in [
            &self.target_name,
            &self.staged_name,
            &self.backup_name,
            &self.pending_snapshot_data_name,
            &self.pending_raft_meta_name,
            &self.pending_checkpoint_meta_name,
        ] {
            validate_install_basename(Path::new(name))?;
        }
        if !self.staged_name.starts_with(STAGED_PREFIX) {
            return Err(invalid_data(format!(
                "snapshot install staged basename {} is outside the registered prefix",
                self.staged_name
            )));
        }
        let backup_prefix = format!(".{}.snapshot-install-backup-", self.target_name);
        if !self.backup_name.starts_with(&backup_prefix) {
            return Err(invalid_data(format!(
                "snapshot install backup basename {} is outside the registered prefix",
                self.backup_name
            )));
        }
        for pending in [
            &self.pending_snapshot_data_name,
            &self.pending_raft_meta_name,
            &self.pending_checkpoint_meta_name,
        ] {
            if !pending.starts_with(PENDING_PREFIX) {
                return Err(invalid_data(format!(
                    "snapshot install pending basename {pending} is outside the registered prefix"
                )));
            }
        }
        let mut names = std::collections::HashSet::new();
        for name in [
            &self.target_name,
            &self.staged_name,
            &self.backup_name,
            &self.pending_snapshot_data_name,
            &self.pending_raft_meta_name,
            &self.pending_checkpoint_meta_name,
        ] {
            if !names.insert(name) {
                return Err(invalid_data(format!(
                    "snapshot install marker reuses basename {name}"
                )));
            }
        }
        if self.phase.requires_old_identity() && self.old_storage.is_none() {
            return Err(invalid_data(format!(
                "snapshot install phase {:?} is missing the paused old-storage identity",
                self.phase
            )));
        }
        validate_storage_identity_shape(&self.new_storage, self.db_instance_num as usize)?;
        if let Some(old) = &self.old_storage {
            validate_storage_identity_shape(old, self.db_instance_num as usize)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotInstallLayout {
    marker_path: PathBuf,
    target_path: PathBuf,
    staged_path: PathBuf,
    backup_path: PathBuf,
    snapshot_work_dir: PathBuf,
    pending_snapshot_data_path: PathBuf,
    pending_raft_meta_path: PathBuf,
    pending_checkpoint_meta_path: PathBuf,
}

impl SnapshotInstallLayout {
    fn from_marker(
        db_path: &Path,
        snapshot_work_dir: &Path,
        marker: &SnapshotInstallMarkerV2,
    ) -> io::Result<Self> {
        marker.validate()?;
        let target_name = db_path.file_name().ok_or_else(|| {
            invalid_data(format!(
                "database path has no basename: {}",
                db_path.display()
            ))
        })?;
        if target_name != std::ffi::OsStr::new(&marker.target_name) {
            return Err(invalid_data(format!(
                "snapshot install marker target {} does not match configured database {}",
                marker.target_name,
                db_path.display()
            )));
        }
        let parent = db_path.parent().filter(|path| !path.as_os_str().is_empty());
        let parent = parent.unwrap_or_else(|| Path::new("."));
        Ok(Self {
            marker_path: snapshot_install_marker_path(db_path)?,
            target_path: db_path.to_path_buf(),
            staged_path: parent.join(&marker.staged_name),
            backup_path: parent.join(&marker.backup_name),
            snapshot_work_dir: snapshot_work_dir.to_path_buf(),
            pending_snapshot_data_path: snapshot_work_dir.join(&marker.pending_snapshot_data_name),
            pending_raft_meta_path: snapshot_work_dir.join(&marker.pending_raft_meta_name),
            pending_checkpoint_meta_path: snapshot_work_dir
                .join(&marker.pending_checkpoint_meta_name),
        })
    }

    pub fn marker_path(&self) -> &Path {
        &self.marker_path
    }

    pub fn target_path(&self) -> &Path {
        &self.target_path
    }

    pub fn staged_path(&self) -> &Path {
        &self.staged_path
    }

    pub fn backup_path(&self) -> &Path {
        &self.backup_path
    }

    pub fn snapshot_work_dir(&self) -> &Path {
        &self.snapshot_work_dir
    }

    pub fn pending_snapshot_data_path(&self) -> &Path {
        &self.pending_snapshot_data_path
    }

    pub fn pending_raft_meta_path(&self) -> &Path {
        &self.pending_raft_meta_path
    }

    pub fn pending_checkpoint_meta_path(&self) -> &Path {
        &self.pending_checkpoint_meta_path
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotInstallIntent {
    pub marker: SnapshotInstallMarkerV2,
    pub layout: SnapshotInstallLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotInstallRecoveryDecision {
    NoPendingInstall,
    Installed,
    RolledBack,
}

/// Accept exactly one relative normal path component.
pub fn validate_install_basename(path: &Path) -> io::Result<()> {
    let mut components = path.components();
    let valid = matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none()
        && !path.as_os_str().is_empty();
    if !valid {
        return Err(invalid_data(format!(
            "snapshot install path must be one relative basename: {}",
            path.display()
        )));
    }
    Ok(())
}

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

pub fn storage_identity_from_open(
    storage: &storage::storage::Storage,
) -> io::Result<SnapshotInstallStorageIdentity> {
    let db_path = storage.db_path().ok_or_else(|| {
        invalid_data("cannot capture snapshot install identity from closed Storage")
    })?;
    let logical = storage
        .logical_snapshot_digests()
        .map_err(|error| invalid_data(error.to_string()))?;
    storage_identity_from_manifests(db_path, storage.db_instance_num, logical)
}

pub fn storage_identity_from_root(
    root: &Path,
    db_instance_num: usize,
    options: &StorageOptions,
) -> io::Result<SnapshotInstallStorageIdentity> {
    ensure_real_directory(root, "snapshot install storage root")?;
    let logical = logical_snapshot_digests_from_root(root, db_instance_num, options)
        .map_err(|error| invalid_data(error.to_string()))?;
    storage_identity_from_manifests(root, db_instance_num, logical)
}

fn storage_identity_from_manifests(
    root: &Path,
    db_instance_num: usize,
    logical_instance_digests: Vec<ManifestDigest>,
) -> io::Result<SnapshotInstallStorageIdentity> {
    let root_manifest = RootStorageManifestV2::read_from_dir(root)
        .map_err(|error| invalid_data(format!("invalid Root manifest: {error}")))?;
    root_manifest
        .validate_runtime_topology(db_instance_num)
        .map_err(|error| invalid_data(error.to_string()))?;
    let instance_manifests = (0..db_instance_num)
        .map(|instance_id| {
            let instance =
                InstanceStorageManifestV2::read_from_dir(&root.join(instance_id.to_string()))
                    .map_err(|error| {
                        invalid_data(format!(
                            "invalid snapshot install instance {instance_id} manifest: {error}"
                        ))
                    })?;
            instance
                .validate_root_binding(instance_id as u32, &root_manifest)
                .map_err(|error| invalid_data(error.to_string()))?;
            Ok(SnapshotInstanceManifest {
                instance_id: instance_id as u32,
                manifest_digest: instance.manifest_digest().clone(),
                storage_incarnation: instance.storage_incarnation(),
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    let identity = SnapshotInstallStorageIdentity {
        root_manifest_id: root_manifest.manifest_id().to_string(),
        root_manifest_digest: root_manifest.manifest_digest().clone(),
        instance_manifests,
        logical_instance_digests,
    };
    validate_storage_identity_shape(&identity, db_instance_num)?;
    Ok(identity)
}

fn validate_storage_identity_shape(
    identity: &SnapshotInstallStorageIdentity,
    db_instance_num: usize,
) -> io::Result<()> {
    if identity.root_manifest_id.is_empty()
        || identity.instance_manifests.len() != db_instance_num
        || identity.logical_instance_digests.len() != db_instance_num
    {
        return Err(invalid_data(format!(
            "snapshot install storage identity does not describe {db_instance_num} instances"
        )));
    }
    for (instance_id, manifest) in identity.instance_manifests.iter().enumerate() {
        if manifest.instance_id != instance_id as u32 || manifest.storage_incarnation == 0 {
            return Err(invalid_data(format!(
                "snapshot install instance identity is invalid at index {instance_id}"
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn persist_staged_validated(
    db_path: &Path,
    staged_path: &Path,
    snapshot_work_dir: &Path,
    snapshot_meta: &SnapshotMeta<u64, KiwiNode>,
    snapshot_bytes: &[u8],
    checkpoint_meta_path: &Path,
    db_instance_num: usize,
    options: &StorageOptions,
) -> io::Result<SnapshotInstallIntent> {
    let parent = db_path.parent().filter(|path| !path.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    if staged_path.parent() != Some(parent) {
        return Err(invalid_data(format!(
            "staged snapshot {} is not a sibling of target {}",
            staged_path.display(),
            db_path.display()
        )));
    }
    let target_name = basename_string(db_path)?;
    let staged_name = basename_string(staged_path)?;
    validate_install_basename(Path::new(&target_name))?;
    validate_install_basename(Path::new(&staged_name))?;
    if !staged_name.starts_with(STAGED_PREFIX) {
        return Err(invalid_data(format!(
            "staged snapshot basename {staged_name} is outside the registered prefix"
        )));
    }
    ensure_real_directory(db_path, "snapshot install target")?;
    ensure_real_directory(staged_path, "staged snapshot")?;

    fs::create_dir_all(snapshot_work_dir)?;
    sync_directory(snapshot_work_dir)?;
    let token = format!(
        "{}-{}-{}-{}",
        snapshot_meta.last_log_id.map(|id| id.index).unwrap_or(0),
        snapshot_meta
            .last_log_id
            .map(|id| id.leader_id.term)
            .unwrap_or(0),
        std::process::id(),
        INSTALL_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let backup_name = format!(".{target_name}.snapshot-install-backup-{token}");
    let pending_snapshot_data_name = format!("{PENDING_PREFIX}{token}.tar");
    let pending_raft_meta_name = format!("{PENDING_PREFIX}{token}.raft-meta.json");
    let pending_checkpoint_meta_name = format!("{PENDING_PREFIX}{token}.checkpoint-meta.json");
    let raft_meta_bytes = serde_json::to_vec(snapshot_meta)
        .map_err(|error| invalid_data(format!("failed to serialize snapshot metadata: {error}")))?;
    let checkpoint_meta_bytes = fs::read(checkpoint_meta_path)?;
    let new_storage = storage_identity_from_root(staged_path, db_instance_num, options)?;
    let marker = SnapshotInstallMarkerV2 {
        version: SNAPSHOT_INSTALL_MARKER_VERSION,
        phase: SnapshotInstallPhase::StagedValidated,
        snapshot_id: snapshot_meta.snapshot_id.clone(),
        last_log_index: snapshot_meta.last_log_id.map(|id| id.index).unwrap_or(0),
        last_log_term: snapshot_meta
            .last_log_id
            .map(|id| id.leader_id.term)
            .unwrap_or(0),
        db_instance_num: db_instance_num as u32,
        target_name,
        staged_name,
        backup_name,
        pending_snapshot_data_name,
        pending_raft_meta_name,
        pending_checkpoint_meta_name,
        snapshot_archive_digest: ManifestDigest::compute(snapshot_bytes),
        raft_metadata_digest: ManifestDigest::compute(&raft_meta_bytes),
        checkpoint_metadata_digest: ManifestDigest::compute(&checkpoint_meta_bytes),
        old_storage: None,
        new_storage,
    };
    marker.validate()?;
    let layout = SnapshotInstallLayout::from_marker(db_path, snapshot_work_dir, &marker)?;
    if layout.marker_path.try_exists()? || layout.backup_path.try_exists()? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "snapshot install marker or backup already exists",
        ));
    }
    write_new_durable(&layout.pending_snapshot_data_path, snapshot_bytes)?;
    if let Err(error) = write_new_durable(&layout.pending_raft_meta_path, &raft_meta_bytes) {
        let _ = fs::remove_file(&layout.pending_snapshot_data_path);
        return Err(error);
    }
    if let Err(error) =
        write_new_durable(&layout.pending_checkpoint_meta_path, &checkpoint_meta_bytes)
    {
        let _ = fs::remove_file(&layout.pending_snapshot_data_path);
        let _ = fs::remove_file(&layout.pending_raft_meta_path);
        return Err(error);
    }
    if let Err(error) = persist_marker_new(&layout.marker_path, &marker) {
        let _ = fs::remove_file(&layout.pending_snapshot_data_path);
        let _ = fs::remove_file(&layout.pending_raft_meta_path);
        let _ = fs::remove_file(&layout.pending_checkpoint_meta_path);
        return Err(error);
    }
    Ok(SnapshotInstallIntent { marker, layout })
}

pub fn read_snapshot_install_intent(
    db_path: &Path,
    snapshot_work_dir: &Path,
) -> io::Result<Option<SnapshotInstallIntent>> {
    let marker_path = snapshot_install_marker_path(db_path)?;
    if !marker_path.try_exists()? {
        return Ok(None);
    }
    let metadata = fs::symlink_metadata(&marker_path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(invalid_data(format!(
            "snapshot install marker is not a real file: {}",
            marker_path.display()
        )));
    }
    let bytes = fs::read(&marker_path)?;
    let marker: SnapshotInstallMarkerV2 = serde_json::from_slice(&bytes)
        .map_err(|error| invalid_data(format!("snapshot install marker is malformed: {error}")))?;
    let layout = SnapshotInstallLayout::from_marker(db_path, snapshot_work_dir, &marker)?;
    Ok(Some(SnapshotInstallIntent { marker, layout }))
}

/// Validate a marker before any runtime or Storage is started. A valid v2
/// marker is recoverable later; malformed, unknown, or path-escaping markers
/// fail closed here.
pub fn validate_snapshot_install_marker(db_path: &Path) -> io::Result<Option<usize>> {
    let marker_path = snapshot_install_marker_path(db_path)?;
    if !marker_path.try_exists()? {
        return Ok(None);
    }
    let metadata = fs::symlink_metadata(&marker_path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(invalid_data(format!(
            "snapshot install marker is not a real file: {}",
            marker_path.display()
        )));
    }
    let marker: SnapshotInstallMarkerV2 = serde_json::from_slice(&fs::read(&marker_path)?)
        .map_err(|error| invalid_data(format!("snapshot install marker is malformed: {error}")))?;
    marker.validate()?;
    let target_name = basename_string(db_path)?;
    if marker.target_name != target_name {
        return Err(invalid_data(format!(
            "snapshot install marker target {} does not match configured target {target_name}",
            marker.target_name
        )));
    }
    Ok(Some(marker.db_instance_num as usize))
}

pub fn persist_paused_old_storage(
    intent: &mut SnapshotInstallIntent,
    old_storage: SnapshotInstallStorageIdentity,
) -> io::Result<()> {
    if intent.marker.phase != SnapshotInstallPhase::StagedValidated {
        return Err(invalid_data(format!(
            "cannot bind paused storage from phase {:?}",
            intent.marker.phase
        )));
    }
    validate_storage_identity_shape(&old_storage, intent.marker.db_instance_num as usize)?;
    intent.marker.old_storage = Some(old_storage);
    persist_phase(intent, SnapshotInstallPhase::StoragePaused)
}

pub fn persist_phase(
    intent: &mut SnapshotInstallIntent,
    next: SnapshotInstallPhase,
) -> io::Result<()> {
    if !intent.marker.phase.allows_transition_to(next) {
        return Err(invalid_data(format!(
            "invalid snapshot install phase transition {:?} -> {:?}",
            intent.marker.phase, next
        )));
    }
    let previous = intent.marker.phase;
    intent.marker.phase = next;
    if let Err(error) = replace_marker_atomically(&intent.layout.marker_path, &intent.marker) {
        intent.marker.phase = previous;
        return Err(error);
    }
    Ok(())
}

pub fn validate_install_layout_and_digests(intent: &SnapshotInstallIntent) -> io::Result<()> {
    validate_file_digest(
        &intent.layout.pending_snapshot_data_path,
        &intent.marker.snapshot_archive_digest,
        "pending snapshot archive",
    )?;
    validate_file_digest(
        &intent.layout.pending_raft_meta_path,
        &intent.marker.raft_metadata_digest,
        "pending Raft metadata",
    )?;
    validate_file_digest(
        &intent.layout.pending_checkpoint_meta_path,
        &intent.marker.checkpoint_metadata_digest,
        "pending checkpoint metadata",
    )?;
    let raft_meta_bytes = fs::read(&intent.layout.pending_raft_meta_path)?;
    let raft_meta: SnapshotMeta<u64, KiwiNode> = serde_json::from_slice(&raft_meta_bytes)
        .map_err(|error| invalid_data(format!("pending Raft metadata is malformed: {error}")))?;
    if raft_meta.snapshot_id != intent.marker.snapshot_id
        || raft_meta.last_log_id.map(|id| id.index).unwrap_or(0) != intent.marker.last_log_index
        || raft_meta
            .last_log_id
            .map(|id| id.leader_id.term)
            .unwrap_or(0)
            != intent.marker.last_log_term
    {
        return Err(invalid_data(
            "pending Raft metadata does not match the snapshot install marker",
        ));
    }
    Ok(())
}

pub fn validate_storage_copy(
    path: &Path,
    expected: &SnapshotInstallStorageIdentity,
    db_instance_num: usize,
    options: &StorageOptions,
) -> io::Result<()> {
    let actual = storage_identity_from_root(path, db_instance_num, options)?;
    if &actual != expected {
        return Err(invalid_data(format!(
            "snapshot install storage digest mismatch at {}",
            path.display()
        )));
    }
    Ok(())
}

pub fn rename_and_sync(from: &Path, to: &Path) -> io::Result<()> {
    if to.try_exists()? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "snapshot install rename target already exists: {}",
                to.display()
            ),
        ));
    }
    fs::rename(from, to)?;
    sync_parent_directory(to)
}

pub fn publish_pending_current_snapshot(intent: &SnapshotInstallIntent) -> io::Result<()> {
    validate_install_layout_and_digests(intent)?;
    fs::create_dir_all(&intent.layout.snapshot_work_dir)?;
    let data = fs::read(&intent.layout.pending_snapshot_data_path)?;
    let meta = fs::read(&intent.layout.pending_raft_meta_path)?;
    write_replace_durable(
        &intent.layout.snapshot_work_dir.join(CURRENT_SNAPSHOT_DATA),
        &data,
    )?;
    write_replace_durable(
        &intent.layout.snapshot_work_dir.join(CURRENT_SNAPSHOT_META),
        &meta,
    )?;
    sync_directory(&intent.layout.snapshot_work_dir)
}

pub fn current_snapshot_is_durable(intent: &SnapshotInstallIntent) -> io::Result<()> {
    validate_file_digest(
        &intent.layout.snapshot_work_dir.join(CURRENT_SNAPSHOT_DATA),
        &intent.marker.snapshot_archive_digest,
        "current snapshot archive",
    )?;
    validate_file_digest(
        &intent.layout.snapshot_work_dir.join(CURRENT_SNAPSHOT_META),
        &intent.marker.raft_metadata_digest,
        "current snapshot metadata",
    )
}

pub fn complete_pending_cleanup(intent: &SnapshotInstallIntent) -> io::Result<()> {
    current_snapshot_is_durable(intent)?;
    if intent.marker.phase != SnapshotInstallPhase::CleanupPending {
        return Err(invalid_data(format!(
            "cannot clean an installed snapshot from phase {:?}",
            intent.marker.phase
        )));
    }
    // The caller verifies the authoritative target before entering this phase:
    // live install uses its open handle, while startup recovery validates offline.
    // Reopening it here would conflict with the live RocksDB lock.
    if intent.layout.backup_path.try_exists()? {
        ensure_real_directory(&intent.layout.backup_path, "snapshot install backup")?;
        fs::remove_dir_all(&intent.layout.backup_path)?;
        sync_parent_directory(&intent.layout.backup_path)?;
    }
    if intent.layout.staged_path.try_exists()? {
        ensure_real_directory(&intent.layout.staged_path, "staged snapshot cleanup")?;
        fs::remove_dir_all(&intent.layout.staged_path)?;
        sync_parent_directory(&intent.layout.staged_path)?;
    }
    Ok(())
}

pub fn remove_completed_install(intent: &SnapshotInstallIntent) -> io::Result<()> {
    if intent.marker.phase != SnapshotInstallPhase::Complete {
        return Err(invalid_data(format!(
            "cannot remove snapshot install marker from phase {:?}",
            intent.marker.phase
        )));
    }
    current_snapshot_is_durable(intent)?;
    for pending in [
        &intent.layout.pending_snapshot_data_path,
        &intent.layout.pending_raft_meta_path,
        &intent.layout.pending_checkpoint_meta_path,
    ] {
        if pending.try_exists()? {
            fs::remove_file(pending)?;
        }
    }
    sync_directory(&intent.layout.snapshot_work_dir)?;
    fs::remove_file(&intent.layout.marker_path)?;
    sync_parent_directory(&intent.layout.marker_path)
}

pub fn abandon_staged_install(intent: &SnapshotInstallIntent) -> io::Result<()> {
    if intent.marker.phase != SnapshotInstallPhase::StagedValidated {
        return Err(invalid_data(format!(
            "cannot abandon snapshot install from phase {:?}",
            intent.marker.phase
        )));
    }
    fs::remove_file(&intent.layout.marker_path)?;
    sync_parent_directory(&intent.layout.marker_path)?;

    if intent.layout.staged_path.try_exists()? {
        ensure_real_directory(&intent.layout.staged_path, "abandoned staged snapshot")?;
        fs::remove_dir_all(&intent.layout.staged_path)?;
        sync_parent_directory(&intent.layout.staged_path)?;
    }
    for pending in [
        &intent.layout.pending_snapshot_data_path,
        &intent.layout.pending_raft_meta_path,
        &intent.layout.pending_checkpoint_meta_path,
    ] {
        if pending.try_exists()? {
            fs::remove_file(pending)?;
        }
    }
    sync_directory(&intent.layout.snapshot_work_dir)
}

pub async fn recover_snapshot_install(
    db_path: &Path,
    snapshot_work_dir: &Path,
    options: Arc<StorageOptions>,
) -> io::Result<SnapshotInstallRecoveryDecision> {
    let Some(mut intent) = read_snapshot_install_intent(db_path, snapshot_work_dir)? else {
        return Ok(SnapshotInstallRecoveryDecision::NoPendingInstall);
    };
    let db_instance_num = intent.marker.db_instance_num as usize;

    if intent.marker.phase == SnapshotInstallPhase::StagedValidated {
        validate_install_layout_and_digests(&intent)?;
        ensure_layout(&intent, true, true, false)?;
        validate_storage_copy(
            &intent.layout.staged_path,
            &intent.marker.new_storage,
            db_instance_num,
            &options,
        )?;
        let old =
            storage_identity_from_root(&intent.layout.target_path, db_instance_num, &options)?;
        persist_paused_old_storage(&mut intent, old)?;
    }

    if intent.marker.phase == SnapshotInstallPhase::StoragePaused {
        validate_install_layout_and_digests(&intent)?;
        ensure_original_and_stage(&intent, &options)?;
        persist_phase(&mut intent, SnapshotInstallPhase::MarkerPersisted)?;
    }

    if intent.marker.phase == SnapshotInstallPhase::RollbackPending {
        return resume_pending_rollback(&mut intent, &options);
    }

    if intent.marker.phase == SnapshotInstallPhase::RollbackCleanupPending {
        return finish_rolled_back_cleanup(&intent, &options);
    }

    if intent.marker.phase == SnapshotInstallPhase::MarkerPersisted {
        validate_install_layout_and_digests(&intent)?;
        if intent.layout.target_path.try_exists()?
            && intent.layout.staged_path.try_exists()?
            && !intent.layout.backup_path.try_exists()?
        {
            ensure_original_and_stage(&intent, &options)?;
            rename_and_sync(&intent.layout.target_path, &intent.layout.backup_path)?;
            persist_phase(&mut intent, SnapshotInstallPhase::OldRenamedToBackup)?;
            rename_and_sync(&intent.layout.staged_path, &intent.layout.target_path)?;
            validate_promoted_and_backup(&intent, &options)?;
            persist_phase(&mut intent, SnapshotInstallPhase::NewRenamedToTarget)?;
        } else if !intent.layout.target_path.try_exists()?
            && intent.layout.staged_path.try_exists()?
            && intent.layout.backup_path.try_exists()?
        {
            return rollback_before_promotion(&mut intent, &options);
        } else if intent.layout.target_path.try_exists()?
            && !intent.layout.staged_path.try_exists()?
            && intent.layout.backup_path.try_exists()?
        {
            validate_promoted_and_backup(&intent, &options)?;
            persist_phase(&mut intent, SnapshotInstallPhase::OldRenamedToBackup)?;
            persist_phase(&mut intent, SnapshotInstallPhase::NewRenamedToTarget)?;
        } else if intent.layout.target_path.try_exists()?
            && !intent.layout.staged_path.try_exists()?
            && !intent.layout.backup_path.try_exists()?
        {
            return rollback_before_promotion(&mut intent, &options);
        } else {
            return Err(layout_mismatch(&intent));
        }
    }

    if intent.marker.phase == SnapshotInstallPhase::OldRenamedToBackup {
        if !intent.layout.target_path.try_exists()?
            && intent.layout.staged_path.try_exists()?
            && intent.layout.backup_path.try_exists()?
        {
            return rollback_before_promotion(&mut intent, &options);
        }
        if intent.layout.target_path.try_exists()?
            && !intent.layout.staged_path.try_exists()?
            && intent.layout.backup_path.try_exists()?
        {
            validate_promoted_and_backup(&intent, &options)?;
            persist_phase(&mut intent, SnapshotInstallPhase::NewRenamedToTarget)?;
        } else if intent.layout.target_path.try_exists()?
            && !intent.layout.backup_path.try_exists()?
        {
            return rollback_before_promotion(&mut intent, &options);
        } else {
            return Err(layout_mismatch(&intent));
        }
    }

    if intent.marker.phase == SnapshotInstallPhase::NewRenamedToTarget {
        validate_install_layout_and_digests(&intent)?;
        validate_promoted_and_backup(&intent, &options)?;
        reopen_and_verify(&intent, Arc::clone(&options)).await?;
        persist_phase(&mut intent, SnapshotInstallPhase::NewStorageReopened)?;
    }

    if intent.marker.phase == SnapshotInstallPhase::NewStorageReopened {
        validate_install_layout_and_digests(&intent)?;
        validate_promoted_and_backup(&intent, &options)?;
        publish_pending_current_snapshot(&intent)?;
        persist_phase(&mut intent, SnapshotInstallPhase::RaftMetadataPersisted)?;
    }

    if intent.marker.phase == SnapshotInstallPhase::RaftMetadataPersisted {
        current_snapshot_is_durable(&intent)?;
        validate_promoted_and_backup(&intent, &options)?;
        persist_phase(&mut intent, SnapshotInstallPhase::CleanupPending)?;
    }

    if intent.marker.phase == SnapshotInstallPhase::CleanupPending {
        current_snapshot_is_durable(&intent)?;
        validate_storage_copy(
            &intent.layout.target_path,
            &intent.marker.new_storage,
            db_instance_num,
            &options,
        )?;
        complete_pending_cleanup(&intent)?;
        persist_phase(&mut intent, SnapshotInstallPhase::Complete)?;
    }

    if intent.marker.phase == SnapshotInstallPhase::Complete {
        current_snapshot_is_durable(&intent)?;
        if intent.layout.backup_path.try_exists()? || intent.layout.staged_path.try_exists()? {
            return Err(layout_mismatch(&intent));
        }
        validate_storage_copy(
            &intent.layout.target_path,
            &intent.marker.new_storage,
            db_instance_num,
            &options,
        )?;
        remove_completed_install(&intent)?;
        return Ok(SnapshotInstallRecoveryDecision::Installed);
    }

    Err(invalid_data(format!(
        "snapshot install recovery stopped in phase {:?}",
        intent.marker.phase
    )))
}

fn ensure_original_and_stage(
    intent: &SnapshotInstallIntent,
    options: &StorageOptions,
) -> io::Result<()> {
    ensure_layout(intent, true, true, false)?;
    let old =
        intent.marker.old_storage.as_ref().ok_or_else(|| {
            invalid_data("paused snapshot install is missing old storage identity")
        })?;
    validate_storage_copy(
        &intent.layout.target_path,
        old,
        intent.marker.db_instance_num as usize,
        options,
    )?;
    validate_storage_copy(
        &intent.layout.staged_path,
        &intent.marker.new_storage,
        intent.marker.db_instance_num as usize,
        options,
    )
}

fn validate_promoted_and_backup(
    intent: &SnapshotInstallIntent,
    options: &StorageOptions,
) -> io::Result<()> {
    ensure_layout(intent, true, false, true)?;
    validate_storage_copy(
        &intent.layout.target_path,
        &intent.marker.new_storage,
        intent.marker.db_instance_num as usize,
        options,
    )?;
    let old =
        intent.marker.old_storage.as_ref().ok_or_else(|| {
            invalid_data("promoted snapshot install is missing old storage identity")
        })?;
    validate_storage_copy(
        &intent.layout.backup_path,
        old,
        intent.marker.db_instance_num as usize,
        options,
    )
}

fn ensure_layout(
    intent: &SnapshotInstallIntent,
    target: bool,
    staged: bool,
    backup: bool,
) -> io::Result<()> {
    let actual = (
        intent.layout.target_path.try_exists()?,
        intent.layout.staged_path.try_exists()?,
        intent.layout.backup_path.try_exists()?,
    );
    if actual != (target, staged, backup) {
        return Err(layout_mismatch(intent));
    }
    Ok(())
}

fn rollback_before_promotion(
    intent: &mut SnapshotInstallIntent,
    options: &StorageOptions,
) -> io::Result<SnapshotInstallRecoveryDecision> {
    persist_phase(intent, SnapshotInstallPhase::RollbackPending)?;
    resume_pending_rollback(intent, options)
}

fn resume_pending_rollback(
    intent: &mut SnapshotInstallIntent,
    options: &StorageOptions,
) -> io::Result<SnapshotInstallRecoveryDecision> {
    if intent.marker.phase != SnapshotInstallPhase::RollbackPending {
        return Err(invalid_data(format!(
            "cannot resume rollback from snapshot install phase {:?}",
            intent.marker.phase
        )));
    }
    validate_install_layout_and_digests(intent)?;
    let old = intent
        .marker
        .old_storage
        .as_ref()
        .ok_or_else(|| invalid_data("rollback is missing old storage identity"))?;
    let target_exists = intent.layout.target_path.try_exists()?;
    let staged_exists = intent.layout.staged_path.try_exists()?;
    let backup_exists = intent.layout.backup_path.try_exists()?;
    if !target_exists && staged_exists && backup_exists {
        validate_storage_copy(
            &intent.layout.backup_path,
            old,
            intent.marker.db_instance_num as usize,
            options,
        )?;
        validate_storage_copy(
            &intent.layout.staged_path,
            &intent.marker.new_storage,
            intent.marker.db_instance_num as usize,
            options,
        )?;
        rename_and_sync(&intent.layout.backup_path, &intent.layout.target_path)?;
    } else if target_exists && !backup_exists {
        validate_storage_copy(
            &intent.layout.target_path,
            old,
            intent.marker.db_instance_num as usize,
            options,
        )?;
        if staged_exists {
            validate_storage_copy(
                &intent.layout.staged_path,
                &intent.marker.new_storage,
                intent.marker.db_instance_num as usize,
                options,
            )?;
        }
    } else {
        return Err(layout_mismatch(intent));
    }
    persist_phase(intent, SnapshotInstallPhase::RollbackCleanupPending)?;
    finish_rolled_back_cleanup(intent, options)
}

fn finish_rolled_back_cleanup(
    intent: &SnapshotInstallIntent,
    options: &StorageOptions,
) -> io::Result<SnapshotInstallRecoveryDecision> {
    if intent.marker.phase != SnapshotInstallPhase::RollbackCleanupPending {
        return Err(invalid_data(format!(
            "cannot clean a rolled-back install from phase {:?}",
            intent.marker.phase
        )));
    }
    if !intent.layout.target_path.try_exists()? || intent.layout.backup_path.try_exists()? {
        return Err(layout_mismatch(intent));
    }
    let old = intent
        .marker
        .old_storage
        .as_ref()
        .ok_or_else(|| invalid_data("rollback cleanup is missing old storage identity"))?;
    validate_storage_copy(
        &intent.layout.target_path,
        old,
        intent.marker.db_instance_num as usize,
        options,
    )?;
    if intent.layout.staged_path.try_exists()? {
        ensure_real_directory(&intent.layout.staged_path, "rolled-back staged snapshot")?;
        fs::remove_dir_all(&intent.layout.staged_path)?;
        sync_parent_directory(&intent.layout.staged_path)?;
    }
    for pending in [
        &intent.layout.pending_snapshot_data_path,
        &intent.layout.pending_raft_meta_path,
        &intent.layout.pending_checkpoint_meta_path,
    ] {
        if pending.try_exists()? {
            fs::remove_file(pending)?;
        }
    }
    sync_directory(&intent.layout.snapshot_work_dir)?;
    fs::remove_file(&intent.layout.marker_path)?;
    sync_parent_directory(&intent.layout.marker_path)?;
    Ok(SnapshotInstallRecoveryDecision::RolledBack)
}

async fn reopen_and_verify(
    intent: &SnapshotInstallIntent,
    options: Arc<StorageOptions>,
) -> io::Result<()> {
    let mut storage = Storage::new(intent.marker.db_instance_num as usize, 0);
    let receiver = storage
        .open(options, &intent.layout.target_path)
        .map_err(|error| invalid_data(format!("failed to reopen installed storage: {error}")))?;
    storage
        .validate_vector_consistency()
        .map_err(|error| invalid_data(format!("installed Vector data is invalid: {error}")))?;
    let actual = storage_identity_from_open(&storage)?;
    if actual != intent.marker.new_storage {
        return Err(invalid_data(
            "reopened installed storage does not match the marker digest",
        ));
    }
    storage.shutdown().await;
    storage.close();
    drop(receiver);
    Ok(())
}

fn layout_mismatch(intent: &SnapshotInstallIntent) -> io::Error {
    invalid_data(format!(
        "snapshot install layout does not match phase {:?}: target={}, staged={}, backup={}",
        intent.marker.phase,
        intent.layout.target_path.display(),
        intent.layout.staged_path.display(),
        intent.layout.backup_path.display()
    ))
}

fn basename_string(path: &Path) -> io::Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| invalid_data(format!("path has no UTF-8 basename: {}", path.display())))
}

fn ensure_real_directory(path: &Path, label: &str) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid_data(format!(
            "{label} is not a real directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_file_digest(path: &Path, expected: &ManifestDigest, label: &str) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to inspect {label} {}: {error}", path.display()),
        )
    })?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(invalid_data(format!(
            "{label} is not a real file: {}",
            path.display()
        )));
    }
    let actual = ManifestDigest::compute(&fs::read(path)?);
    if &actual != expected {
        return Err(invalid_data(format!(
            "{label} digest mismatch at {}",
            path.display()
        )));
    }
    Ok(())
}

fn write_new_durable(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    sync_parent_directory(path)
}

fn persist_marker_new(path: &Path, marker: &SnapshotInstallMarkerV2) -> io::Result<()> {
    let bytes = serde_json::to_vec(marker)
        .map_err(|error| invalid_data(format!("failed to serialize install marker: {error}")))?;
    write_new_durable(path, &bytes)
}

fn replace_marker_atomically(path: &Path, marker: &SnapshotInstallMarkerV2) -> io::Result<()> {
    marker.validate()?;
    let bytes = serde_json::to_vec(marker)
        .map_err(|error| invalid_data(format!("failed to serialize install marker: {error}")))?;
    write_replace_durable(path, &bytes)
}

fn write_replace_durable(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let sequence = INSTALL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .ok_or_else(|| invalid_data(format!("durable file has no basename: {}", path.display())))?;
    let mut temp_name = OsString::from(".");
    temp_name.push(file_name);
    temp_name.push(format!(".tmp-{}-{sequence}", std::process::id()));
    let temp_path = path.with_file_name(temp_name);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to create durable temporary file {} for {}: {error}",
                    temp_path.display(),
                    path.display()
                ),
            )
        })?;
    if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    if let Err(error) = replace_file_atomically(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(io::Error::new(
            error.kind(),
            format!(
                "failed to atomically replace {} with {}: {error}",
                path.display(),
                temp_path.display()
            ),
        ));
    }
    sync_parent_directory(path)
}

#[cfg(not(windows))]
fn replace_file_atomically(temp_path: &Path, target_path: &Path) -> io::Result<()> {
    fs::rename(temp_path, target_path)
}

#[cfg(windows)]
fn replace_file_atomically(temp_path: &Path, target_path: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let temp_wide: Vec<u16> = temp_path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let target_wide: Vec<u16> = target_path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: both buffers are owned by this stack frame, remain alive for the
    // duration of the call, and are explicitly terminated with one NUL code unit.
    let result = unsafe {
        MoveFileExW(
            temp_wide.as_ptr(),
            target_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
