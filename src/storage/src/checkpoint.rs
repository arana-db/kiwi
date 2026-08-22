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

#[cfg(any(test, feature = "test-fault-injection"))]
use std::collections::HashSet;

#[cfg(any(test, feature = "test-fault-injection"))]
use std::sync::LazyLock;

use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(any(test, feature = "test-fault-injection"))]
use parking_lot::Mutex;

use crate::format_vector::VECTOR_VALUE_FORMAT;
use crate::logindex::LogIndexAndSequenceCollector;
use crate::storage_manifest::StorageManifest;
use crate::storage_migration::{
    prepare_or_resume_migration, validate_base_v1_snapshot_instance,
    validate_vector_v1_snapshot_instance,
};
use crate::{
    CANONICAL_COLUMN_FAMILIES, InstanceStorageManifestV2, ManifestDigest,
    ROOT_STORAGE_MANIFEST_FILE, RootStorageManifestV2, STORAGE_SCHEMA_VERSION_V2, StorageOptions,
    canonical_column_family_names, sync_directory, sync_parent_directory,
};

/// File name for JSON metadata at the checkpoint root (not OpenRaft's `SnapshotMeta`).
pub const RAFT_SNAPSHOT_META_FILE: &str = "__raft_snapshot_meta";

const ROCKSDB_LOCK_FILE: &str = "LOCK";
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

/// Current snapshot format version.
///
/// Version 1 is the registered Base-era format and is accepted only through
/// classified staged migration. Version 2 carries a storage schema contract;
/// the merged Vector-v1 schema and current manifest-v2 schema are distinguished
/// by exact, fail-closed metadata shapes.
pub const CURRENT_SNAPSHOT_VERSION: u32 = 2;

/// Version of the storage on-disk schema understood by this binary.
pub const STORAGE_SCHEMA_VERSION: u32 = STORAGE_SCHEMA_VERSION_V2;

/// Storage schema written by the already-merged Vector Set implementation.
/// Its snapshot metadata is v2, but its per-instance manifest is the legacy
/// Vector-v1 shape and the checkpoint does not carry a Root manifest.
const MERGED_HEAD_STORAGE_SCHEMA_VERSION: u32 = 1;

/// Column families every instance of the checkpoint must contain, in
/// declaration order.
pub fn expected_column_families() -> Vec<String> {
    canonical_column_family_names()
        .into_iter()
        .map(str::to_string)
        .collect()
}

/// Metadata persisted next to per-instance checkpoint directories.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotInstanceManifest {
    pub instance_id: u32,
    pub manifest_digest: ManifestDigest,
    pub storage_incarnation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RaftSnapshotMeta {
    /// Snapshot format version
    pub version: u32,
    /// Last log index included in the snapshot
    pub last_included_index: u64,
    /// Last log term included in the snapshot
    pub last_included_term: u64,
    /// Storage on-disk schema version understood by the snapshot writer.
    #[serde(default)]
    pub storage_schema_version: u32,
    /// Storage incarnation of each RocksDB instance, ordered by instance id.
    /// Restore validates these values against the per-instance manifest files
    /// carried inside the checkpoint before replacing live storage.
    #[serde(default)]
    pub storage_incarnations: Vec<u64>,
    /// Root manifest identity carried by current snapshots.
    #[serde(default)]
    pub root_manifest_id: Option<Uuid>,
    /// Exact Root manifest digest carried inside the checkpoint.
    #[serde(default)]
    pub root_manifest_digest: Option<ManifestDigest>,
    /// Exact per-instance manifest digests and incarnations, sorted by instance id.
    #[serde(default)]
    pub instance_manifests: Vec<SnapshotInstanceManifest>,
    /// Number of RocksDB instances in the checkpoint.
    #[serde(default)]
    pub db_instance_num: u32,
    /// Column families each checkpoint instance must contain, in declaration order.
    #[serde(default)]
    pub column_families: Vec<String>,
    /// Highest vector value format byte the snapshot writer can emit.
    #[serde(default)]
    pub vector_value_format_max: u8,
    /// LogIndex collector states, one entry per Storage instance.
    /// Outer index is the instance id; inner Vec holds `"log_index:seqno"` pairs.
    #[serde(default)]
    pub logindex_collector_states: Vec<Vec<String>>,
}

/// Snapshot metadata classified before any checkpoint database is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedSnapshotMeta {
    /// Known Base-era metadata. The checkpoint is accepted only after its actual
    /// RocksDB layout is proven to be the registered six-CF profile.
    LegacyV1(RaftSnapshotMeta),
    /// Version-2 metadata carrying either the registered merged-Head storage
    /// contract or the current complete manifest-v2 identity contract.
    CurrentV2(RaftSnapshotMeta),
}

impl ParsedSnapshotMeta {
    pub fn read_from_dir(dir: &Path) -> io::Result<Self> {
        let path = dir.join(RAFT_SNAPSHOT_META_FILE);
        let bytes = fs::read(path)?;
        let meta: RaftSnapshotMeta = serde_json::from_slice(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        match meta.version {
            1 => Ok(Self::LegacyV1(meta)),
            CURRENT_SNAPSHOT_VERSION => Ok(Self::CurrentV2(meta)),
            version => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported snapshot version: {version}, expected legacy 1 or current {CURRENT_SNAPSHOT_VERSION}"
                ),
            )),
        }
    }

    pub fn metadata(&self) -> &RaftSnapshotMeta {
        match self {
            Self::LegacyV1(meta) | Self::CurrentV2(meta) => meta,
        }
    }

    pub fn validate_for_restore(&self, expected_db_instance_num: usize) -> io::Result<()> {
        match self {
            Self::LegacyV1(meta) => {
                if meta.storage_schema_version != 0
                    || meta.db_instance_num != 0
                    || !meta.storage_incarnations.is_empty()
                    || meta.root_manifest_id.is_some()
                    || meta.root_manifest_digest.is_some()
                    || !meta.instance_manifests.is_empty()
                    || !meta.column_families.is_empty()
                    || meta.vector_value_format_max != 0
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "legacy snapshot metadata contains an ambiguous current-schema contract",
                    ));
                }
                Ok(())
            }
            Self::CurrentV2(meta) if meta.is_merged_head_v2_contract() => {
                meta.validate_merged_head_v2_for_restore(expected_db_instance_num)
            }
            Self::CurrentV2(meta) => meta.validate_for_restore(expected_db_instance_num),
        }
    }
}

impl RaftSnapshotMeta {
    /// Create a new snapshot meta with current version
    pub fn new(last_included_index: u64, last_included_term: u64) -> Self {
        Self {
            version: CURRENT_SNAPSHOT_VERSION,
            last_included_index,
            last_included_term,
            storage_schema_version: STORAGE_SCHEMA_VERSION,
            storage_incarnations: Vec::new(),
            root_manifest_id: None,
            root_manifest_digest: None,
            instance_manifests: Vec::new(),
            db_instance_num: 0,
            column_families: expected_column_families(),
            vector_value_format_max: VECTOR_VALUE_FORMAT,
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
            db_instance_num: collectors.len() as u32,
            logindex_collector_states: collectors.iter().map(|c| c.export_state()).collect(),
            ..Self::new(last_included_index, last_included_term)
        }
    }

    /// Create snapshot meta describing the given live Storage: per-instance
    /// storage incarnations, the instance count, and the column-family list.
    pub fn for_storage(
        last_included_index: u64,
        last_included_term: u64,
        collectors: &[Arc<LogIndexAndSequenceCollector>],
        storage: &crate::storage::Storage,
    ) -> crate::error::Result<Self> {
        let db_path = storage
            .db_path()
            .ok_or_else(|| crate::error::Error::InvalidFormat {
                message: "cannot describe snapshot identity for unopened Storage".to_string(),
                location: snafu::location!(),
            })?;
        let root_manifest = RootStorageManifestV2::read_from_dir(db_path)?;
        let instance_manifests = (0..storage.db_instance_num)
            .map(|instance_id| {
                let manifest = InstanceStorageManifestV2::read_from_dir(
                    &db_path.join(instance_id.to_string()),
                )?;
                manifest.validate_root_binding(instance_id as u32, &root_manifest)?;
                Ok(SnapshotInstanceManifest {
                    instance_id: instance_id as u32,
                    manifest_digest: manifest.manifest_digest().clone(),
                    storage_incarnation: manifest.storage_incarnation(),
                })
            })
            .collect::<crate::error::Result<Vec<_>>>()?;
        let storage_incarnations = instance_manifests
            .iter()
            .map(|manifest| manifest.storage_incarnation)
            .collect();
        Ok(Self {
            storage_incarnations,
            root_manifest_id: Some(root_manifest.manifest_id()),
            root_manifest_digest: Some(root_manifest.manifest_digest().clone()),
            instance_manifests,
            db_instance_num: storage.db_instance_num as u32,
            ..Self::with_collector_states(last_included_index, last_included_term, collectors)
        })
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
        match ParsedSnapshotMeta::read_from_dir(dir)? {
            ParsedSnapshotMeta::CurrentV2(meta) => Ok(meta),
            ParsedSnapshotMeta::LegacyV1(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "legacy snapshot version 1 requires classified staged restore; expected current {}",
                    CURRENT_SNAPSHOT_VERSION
                ),
            )),
        }
    }

    /// Validate the storage schema description against this binary and the
    /// local configuration. Restore must deterministically reject snapshots it
    /// cannot consume. This validates incarnation count and shape; the staged
    /// checkpoint is paired with the exact manifest values before commit.
    pub fn validate_for_restore(&self, expected_db_instance_num: usize) -> io::Result<()> {
        let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, message);

        if self.version != CURRENT_SNAPSHOT_VERSION {
            return Err(invalid(format!(
                "unsupported snapshot version: {}, expected {}",
                self.version, CURRENT_SNAPSHOT_VERSION
            )));
        }
        if self.storage_schema_version != STORAGE_SCHEMA_VERSION {
            return Err(invalid(format!(
                "unsupported current storage schema version: {}, expected {}",
                self.storage_schema_version, STORAGE_SCHEMA_VERSION
            )));
        }
        if self.db_instance_num as usize != expected_db_instance_num {
            return Err(invalid(format!(
                "snapshot db_instance_num {} does not match local configuration {}",
                self.db_instance_num, expected_db_instance_num
            )));
        }
        if self.storage_incarnations.len() != self.db_instance_num as usize {
            return Err(invalid(format!(
                "snapshot carries {} storage incarnations for {} instances",
                self.storage_incarnations.len(),
                self.db_instance_num
            )));
        }
        if self.root_manifest_id.is_none() || self.root_manifest_digest.is_none() {
            return Err(invalid(
                "current snapshot is missing Root manifest identity".to_string(),
            ));
        }
        if self.instance_manifests.len() != self.db_instance_num as usize {
            return Err(invalid(format!(
                "snapshot carries {} instance manifest identities for {} instances",
                self.instance_manifests.len(),
                self.db_instance_num
            )));
        }
        for (instance_id, manifest) in self.instance_manifests.iter().enumerate() {
            if manifest.instance_id != instance_id as u32 {
                return Err(invalid(format!(
                    "snapshot instance manifest identities are not sorted: entry {instance_id} names instance {}",
                    manifest.instance_id
                )));
            }
            if manifest.storage_incarnation == 0
                || self.storage_incarnations[instance_id] != manifest.storage_incarnation
            {
                return Err(invalid(format!(
                    "snapshot instance {instance_id} incarnation metadata is inconsistent"
                )));
            }
        }
        let expected_column_families = expected_column_families();
        if self.column_families != expected_column_families {
            return Err(invalid(format!(
                "snapshot column families {:?} do not match expected {:?}",
                self.column_families, expected_column_families
            )));
        }
        if self.vector_value_format_max > VECTOR_VALUE_FORMAT {
            return Err(invalid(format!(
                "snapshot vector value format {} exceeds supported format {}",
                self.vector_value_format_max, VECTOR_VALUE_FORMAT
            )));
        }
        Ok(())
    }

    fn is_merged_head_v2_contract(&self) -> bool {
        self.version == CURRENT_SNAPSHOT_VERSION
            && self.storage_schema_version == MERGED_HEAD_STORAGE_SCHEMA_VERSION
            && self.root_manifest_id.is_none()
            && self.root_manifest_digest.is_none()
            && self.instance_manifests.is_empty()
    }

    fn validate_merged_head_v2_for_restore(
        &self,
        expected_db_instance_num: usize,
    ) -> io::Result<()> {
        let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, message);
        if !self.is_merged_head_v2_contract() {
            return Err(invalid(
                "merged-Head v2 snapshot metadata has an ambiguous storage identity contract"
                    .to_string(),
            ));
        }
        if self.db_instance_num as usize != expected_db_instance_num {
            return Err(invalid(format!(
                "snapshot db_instance_num {} does not match local configuration {}",
                self.db_instance_num, expected_db_instance_num
            )));
        }
        if self.storage_incarnations.len() != self.db_instance_num as usize
            || self.storage_incarnations.contains(&0)
        {
            return Err(invalid(format!(
                "merged-Head v2 snapshot carries invalid storage incarnations for {} instances",
                self.db_instance_num
            )));
        }
        let expected_column_families = expected_column_families();
        if self.column_families != expected_column_families {
            return Err(invalid(format!(
                "snapshot column families {:?} do not match expected {:?}",
                self.column_families, expected_column_families
            )));
        }
        if self.vector_value_format_max != VECTOR_VALUE_FORMAT {
            return Err(invalid(format!(
                "merged-Head v2 snapshot vector value format {} does not match registered format {}",
                self.vector_value_format_max, VECTOR_VALUE_FORMAT
            )));
        }
        Ok(())
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
    db_instance_num: usize,
    historical_migration: bool,
    committed: bool,
}

impl PreparedCheckpointRestore {
    /// Return the root of the fully copied, not-yet-committed checkpoint.
    pub fn staged_path(&self) -> &Path {
        &self.temp_dir
    }

    /// Whether this staged restore upgraded a registered historical snapshot
    /// and must close its rollback window before it can become live.
    pub fn has_historical_migration(&self) -> bool {
        self.historical_migration
    }

    /// Transfer cleanup ownership of the staged directory to the durable
    /// snapshot-install state machine.
    pub fn into_staged_path(mut self) -> PathBuf {
        self.committed = true;
        self.temp_dir.clone()
    }

    /// Pair snapshot metadata incarnations with the staged per-instance manifests.
    pub fn validate_storage_incarnations(&self, expected: &[u64]) -> io::Result<()> {
        if expected.len() != self.db_instance_num {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "snapshot carries {} storage incarnations for {} staged instances",
                    expected.len(),
                    self.db_instance_num
                ),
            ));
        }
        for (instance, expected_incarnation) in expected.iter().copied().enumerate() {
            let instance_dir = self.temp_dir.join(instance.to_string());
            let manifest_incarnation = StorageManifest::load_storage_incarnation(&instance_dir)
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "invalid storage manifest for snapshot instance {instance} at {}: {error}",
                            instance_dir.display()
                        ),
                    )
                })?;
            if manifest_incarnation != expected_incarnation {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "snapshot storage incarnation mismatch for instance {instance}: metadata {expected_incarnation}, manifest {manifest_incarnation}"
                    ),
                ));
            }
        }
        Ok(())
    }

    pub fn validate_snapshot_manifests(&self, parsed: &ParsedSnapshotMeta) -> io::Result<()> {
        let ParsedSnapshotMeta::CurrentV2(meta) = parsed else {
            return Ok(());
        };
        let root = RootStorageManifestV2::read_from_dir(&self.temp_dir).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid checkpoint Root manifest: {error}"),
            )
        })?;
        let expected_root_id = meta.root_manifest_id.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "current snapshot is missing Root manifest ID",
            )
        })?;
        let expected_root_digest = meta.root_manifest_digest.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "current snapshot is missing Root manifest digest",
            )
        })?;
        if root.manifest_id() != expected_root_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "root manifest ID mismatch: metadata {expected_root_id}, checkpoint {}",
                    root.manifest_id()
                ),
            ));
        }
        if root.manifest_digest() != expected_root_digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "root manifest digest mismatch: metadata {}, checkpoint {}",
                    expected_root_digest.as_str(),
                    root.manifest_digest().as_str()
                ),
            ));
        }
        root.validate_runtime_topology(self.db_instance_num)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        if meta.instance_manifests.len() != self.db_instance_num {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "snapshot carries {} instance manifest identities for {} staged instances",
                    meta.instance_manifests.len(),
                    self.db_instance_num
                ),
            ));
        }
        for (instance_id, expected) in meta.instance_manifests.iter().enumerate() {
            if expected.instance_id != instance_id as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "snapshot instance manifest identities are not sorted at entry {instance_id}"
                    ),
                ));
            }
            let instance = InstanceStorageManifestV2::read_from_dir(
                &self.temp_dir.join(instance_id.to_string()),
            )
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid checkpoint instance {instance_id} manifest: {error}"),
                )
            })?;
            instance
                .validate_root_binding(instance_id as u32, &root)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if instance.manifest_digest() != &expected.manifest_digest {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("instance {instance_id} manifest digest mismatch"),
                ));
            }
            if instance.storage_incarnation() != expected.storage_incarnation {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("instance {instance_id} storage incarnation mismatch"),
                ));
            }
        }
        Ok(())
    }

    /// Move the staged checkpoint into a target that does not already exist.
    /// Existing live storage is never deleted by this low-level helper; snapshot
    /// replacement is owned by the durable install state machine.
    pub fn commit(mut self) -> io::Result<()> {
        if self.target_db_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "refusing to delete existing checkpoint restore target {}",
                    self.target_db_path.display()
                ),
            ));
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
        db_instance_num,
        historical_migration: false,
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
    let root_manifest = checkpoint_root.join(ROOT_STORAGE_MANIFEST_FILE);
    if root_manifest.exists() {
        fs::copy(
            &root_manifest,
            prepared.temp_dir.join(ROOT_STORAGE_MANIFEST_FILE),
        )?;
        OpenOptions::new()
            .write(true)
            .open(prepared.temp_dir.join(ROOT_STORAGE_MANIFEST_FILE))?
            .sync_all()?;
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

/// Validate a classified snapshot from its actual files, then stage it next to the target.
/// Legacy v1 is migrated only inside the disposable staged directory.
pub fn prepare_classified_checkpoint_restore(
    checkpoint_root: &Path,
    target_db_path: &Path,
    db_instance_num: usize,
    parsed: &ParsedSnapshotMeta,
    options: &StorageOptions,
) -> io::Result<PreparedCheckpointRestore> {
    parsed.validate_for_restore(db_instance_num)?;
    validate_checkpoint_root_and_column_families(
        checkpoint_root,
        db_instance_num,
        parsed,
        options,
    )?;
    let mut prepared =
        prepare_checkpoint_restore(checkpoint_root, target_db_path, db_instance_num)?;
    let needs_staged_migration = matches!(parsed, ParsedSnapshotMeta::LegacyV1(_))
        || matches!(
            parsed,
            ParsedSnapshotMeta::CurrentV2(meta) if meta.is_merged_head_v2_contract()
        );
    if needs_staged_migration {
        prepare_or_resume_migration(prepared.staged_path(), db_instance_num, options).map_err(
            |error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to migrate staged historical snapshot: {error}"),
                )
            },
        )?;
        prepared.historical_migration = true;
    } else {
        prepared.validate_snapshot_manifests(parsed)?;
    }
    Ok(prepared)
}

fn validate_checkpoint_root_and_column_families(
    checkpoint_root: &Path,
    db_instance_num: usize,
    parsed: &ParsedSnapshotMeta,
    options: &StorageOptions,
) -> io::Result<()> {
    let mut instance_ids = std::collections::HashSet::new();
    let merged_head_v2 = matches!(
        parsed,
        ParsedSnapshotMeta::CurrentV2(meta) if meta.is_merged_head_v2_contract()
    );
    let expects_root_manifest =
        matches!(parsed, ParsedSnapshotMeta::CurrentV2(_)) && !merged_head_v2;
    let mut found_root_manifest = false;
    for entry in fs::read_dir(checkpoint_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot checkpoint root contains a non-UTF-8 entry",
            )
        })?;
        let file_type = entry.file_type()?;
        if name == RAFT_SNAPSHOT_META_FILE {
            if !file_type.is_file() || file_type.is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot metadata is not a real file",
                ));
            }
            continue;
        }
        if name == ROOT_STORAGE_MANIFEST_FILE {
            if !expects_root_manifest || !file_type.is_file() || file_type.is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot Root manifest is unexpected or not a real file",
                ));
            }
            found_root_manifest = true;
            continue;
        }
        let instance_id = name.parse::<usize>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot checkpoint root contains unknown entry {name}"),
            )
        })?;
        if name != instance_id.to_string()
            || instance_id >= db_instance_num
            || !file_type.is_dir()
            || file_type.is_symlink()
            || !instance_ids.insert(instance_id)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot checkpoint instance entry {name} is invalid"),
            ));
        }
    }
    if instance_ids.len() != db_instance_num {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "snapshot checkpoint contains {} instances, expected {db_instance_num}",
                instance_ids.len()
            ),
        ));
    }
    if expects_root_manifest != found_root_manifest {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "current snapshot is missing its Root manifest",
        ));
    }

    let expected_current: std::collections::HashSet<&str> = CANONICAL_COLUMN_FAMILIES
        .iter()
        .map(|spec| spec.name)
        .collect();
    for instance_id in 0..db_instance_num {
        let instance = checkpoint_root.join(instance_id.to_string());
        match parsed {
            ParsedSnapshotMeta::LegacyV1(_) => {
                validate_base_v1_snapshot_instance(&instance, options).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid Base-v1 snapshot instance {instance_id}: {error}"),
                    )
                })?;
            }
            ParsedSnapshotMeta::CurrentV2(meta) if merged_head_v2 => {
                validate_vector_v1_snapshot_instance(
                    &instance,
                    meta.storage_incarnations[instance_id],
                    options,
                )
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid merged-Head v2 snapshot instance {instance_id}: {error}"),
                    )
                })?;
            }
            ParsedSnapshotMeta::CurrentV2(_) => {
                let actual: std::collections::HashSet<String> =
                    DB::list_cf(&Options::default(), &instance)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                        .into_iter()
                        .collect();
                if actual.len() != expected_current.len()
                    || !actual
                        .iter()
                        .all(|name| expected_current.contains(name.as_str()))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "current snapshot instance {instance_id} has a non-canonical CF set: {actual:?}"
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
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

#[doc(hidden)]
pub fn sync_restore_parent_after_rename(target_db_path: &Path) -> io::Result<()> {
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
