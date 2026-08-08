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

//! Fail-closed staged migration from the two registered v1 storage layouts.

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

#[cfg(any(test, feature = "test-fault-injection"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "test-fault-injection"))]
use std::sync::LazyLock;

#[cfg(any(test, feature = "test-fault-injection"))]
use parking_lot::Mutex as ParkingMutex;
use rand::Rng;
use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode, Options, ReadOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};
use uuid::Uuid;

use crate::custom_comparator::{
    lists_data_key_comparator_name, lists_data_key_compare, zsets_score_key_comparator_name,
    zsets_score_key_compare,
};
use crate::durable_fs::{sync_directory, sync_parent_directory};
use crate::error::{InvalidFormatSnafu, IoSnafu, Result, RocksSnafu};
use crate::storage_manifest::{
    InstanceStorageManifestV2, ManifestDigest, MigrationPhase, MigrationSourceProfile,
    MigrationTransaction, ROOT_STORAGE_MANIFEST_FILE, RootStorageManifestV2, SLOT_MAPPING_VERSION,
    STORAGE_MANIFEST_FILE, slot_mapping_digest,
};
use crate::storage_schema::{CANONICAL_COLUMN_FAMILIES, ColumnFamilySpec, ComparatorId};
use crate::{DataType, StorageOptions};

const BASE_V1_CF_COUNT: usize = 6;
const LIVE_SOURCE_NAME: &str = "live";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationFaultPoint {
    AfterSourceDetected,
    AfterShadowPrepared,
    AfterInstanceCopied(u32),
    AfterVectorCfCreatedBeforeInstanceManifest(u32),
    AfterInstanceUpgraded(u32),
    AfterAllInstancesVerified,
    AfterSwitchPrepared,
    AfterOldMovedToBackup(u32),
    AfterShadowPromoted(u32),
    AfterNewStorageOpened,
    AfterCommitted,
    AfterRollbackV2MovedAside(u32),
    AfterRollbackLegacyRestored(u32),
    AfterRollbackShadowRemoved(u32),
    AfterRollbackShadowRootRemoved,
    AfterRollbackBackupRootRemoved,
    AfterRollbackRootManifestRemoved,
}

#[cfg(any(test, feature = "test-fault-injection"))]
static MIGRATION_FAILURES: LazyLock<ParkingMutex<HashMap<PathBuf, MigrationFaultPoint>>> =
    LazyLock::new(|| ParkingMutex::new(HashMap::new()));

#[cfg(any(test, feature = "test-fault-injection"))]
#[doc(hidden)]
pub struct StorageMigrationFailureGuard {
    root: PathBuf,
}

#[cfg(any(test, feature = "test-fault-injection"))]
impl Drop for StorageMigrationFailureGuard {
    fn drop(&mut self) {
        MIGRATION_FAILURES.lock().remove(&self.root);
    }
}

#[cfg(any(test, feature = "test-fault-injection"))]
#[doc(hidden)]
#[must_use]
pub fn fail_next_storage_migration(
    root: &Path,
    point: MigrationFaultPoint,
) -> StorageMigrationFailureGuard {
    let root = root.to_path_buf();
    assert!(
        MIGRATION_FAILURES
            .lock()
            .insert(root.clone(), point)
            .is_none(),
        "storage migration failure already registered for {}",
        root.display()
    );
    StorageMigrationFailureGuard { root }
}

fn maybe_fail(root: &Path, point: MigrationFaultPoint) -> Result<()> {
    #[cfg(any(test, feature = "test-fault-injection"))]
    {
        let mut failures = MIGRATION_FAILURES.lock();
        if failures.get(root) == Some(&point) {
            failures.remove(root);
            return Err(InvalidFormatSnafu {
                message: format!("injected storage migration failure at {point:?}"),
            }
            .build());
        }
    }
    let _ = (root, point);
    Ok(())
}

#[derive(Debug, Clone)]
pub struct MigrationLayout {
    root: PathBuf,
    shadow_name: String,
    backup_name: String,
}

impl MigrationLayout {
    fn new(root: &Path, transaction: &MigrationTransaction) -> Result<Self> {
        ensure!(
            transaction.source_name == LIVE_SOURCE_NAME,
            InvalidFormatSnafu {
                message: format!(
                    "unsupported migration source basename {:?}",
                    transaction.source_name
                )
            }
        );
        Ok(Self {
            root: root.to_path_buf(),
            shadow_name: transaction.shadow_name.clone(),
            backup_name: transaction.backup_name.clone(),
        })
    }

    fn source_instance(&self, instance_id: u32) -> PathBuf {
        self.root.join(instance_id.to_string())
    }

    fn shadow_root(&self) -> PathBuf {
        self.root.join(&self.shadow_name)
    }

    fn shadow_instance(&self, instance_id: u32) -> PathBuf {
        self.shadow_root().join(instance_id.to_string())
    }

    fn backup_root(&self) -> PathBuf {
        self.root.join(&self.backup_name)
    }

    fn backup_instance(&self, instance_id: u32) -> PathBuf {
        self.backup_root().join(instance_id.to_string())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct VectorV1Manifest {
    version: u32,
    storage_incarnation: u64,
    next_generation: u64,
}

impl VectorV1Manifest {
    fn read_from_dir(dir: &Path) -> Result<Self> {
        let path = dir.join(STORAGE_MANIFEST_FILE);
        let bytes = fs::read(&path).context(IoSnafu)?;
        let manifest: Self = serde_json::from_slice(&bytes).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("invalid Vector-v1 manifest {}: {error}", path.display()),
            }
            .build()
        })?;
        ensure!(
            manifest.version == 1
                && manifest.storage_incarnation != 0
                && manifest.next_generation >= 1,
            InvalidFormatSnafu {
                message: format!("invalid Vector-v1 identity in {}", path.display())
            }
        );
        ensure!(
            serde_json::to_vec(&manifest).map_err(|error| {
                InvalidFormatSnafu {
                    message: format!("failed to canonicalize Vector-v1 manifest: {error}"),
                }
                .build()
            })? == bytes,
            InvalidFormatSnafu {
                message: format!(
                    "Vector-v1 manifest {} is not compact fixed-order JSON",
                    path.display()
                )
            }
        );
        Ok(manifest)
    }
}

pub fn classify_storage_root(
    root: &Path,
    db_instance_num: usize,
    options: &StorageOptions,
) -> Result<Option<MigrationSourceProfile>> {
    ensure!(
        db_instance_num > 0,
        InvalidFormatSnafu {
            message: "migration db_instance_num must be greater than zero".to_string()
        }
    );
    if !root.exists() {
        return Ok(None);
    }
    if root.join(ROOT_STORAGE_MANIFEST_FILE).exists() {
        let manifest = RootStorageManifestV2::read_from_dir(root)?;
        manifest.validate_runtime_topology(db_instance_num)?;
        return Ok(manifest
            .migration()
            .map(|transaction| transaction.source_profile));
    }

    let entries: Vec<_> = fs::read_dir(root)
        .context(IoSnafu)?
        .collect::<std::io::Result<_>>()
        .context(IoSnafu)?;
    if entries.is_empty() {
        return Ok(None);
    }
    let root_manifest_temp = root.join(ROOT_STORAGE_MANIFEST_FILE).with_extension("tmp");
    let mut instance_ids = HashSet::new();
    for entry in &entries {
        let path = entry.path();
        let file_type = entry.file_type().context(IoSnafu)?;
        if path == root_manifest_temp {
            ensure!(
                file_type.is_file() && !file_type.is_symlink(),
                InvalidFormatSnafu {
                    message: format!(
                        "interrupted Root manifest temp {} must be a regular file",
                        path.display()
                    )
                }
            );
            continue;
        }
        let name = entry.file_name();
        let instance_id = name.to_string_lossy().parse::<usize>().map_err(|_| {
            InvalidFormatSnafu {
                message: format!(
                    "legacy storage root {} contains unexpected entry {}",
                    root.display(),
                    name.to_string_lossy()
                ),
            }
            .build()
        })?;
        ensure!(
            instance_id < db_instance_num
                && file_type.is_dir()
                && !file_type.is_symlink()
                && instance_ids.insert(instance_id),
            InvalidFormatSnafu {
                message: format!(
                    "legacy storage instance entry {} is invalid or outside topology",
                    path.display()
                )
            }
        );
    }
    ensure!(
        instance_ids.len() == db_instance_num,
        InvalidFormatSnafu {
            message: format!(
                "legacy storage root {} must contain exactly {} instance directories",
                root.display(),
                db_instance_num
            )
        }
    );

    let mut detected = None;
    for instance_id in 0..db_instance_num {
        let instance = root.join(instance_id.to_string());
        ensure!(
            instance.is_dir(),
            InvalidFormatSnafu {
                message: format!("legacy storage instance {} is missing", instance.display())
            }
        );
        let profile = classify_instance(&instance)?;
        if let Some(expected) = detected {
            ensure!(
                expected == profile,
                InvalidFormatSnafu {
                    message: format!(
                        "mixed legacy storage profiles: instance 0 is {expected:?}, instance {instance_id} is {profile:?}"
                    )
                }
            );
        } else {
            detected = Some(profile);
        }
        strict_open_legacy_instance(&instance, profile, options)?;
    }
    Ok(detected)
}

fn classify_instance(instance: &Path) -> Result<MigrationSourceProfile> {
    let mut actual = DB::list_cf(&Options::default(), instance).context(RocksSnafu)?;
    actual.sort();
    let mut base: Vec<String> = CANONICAL_COLUMN_FAMILIES[..BASE_V1_CF_COUNT]
        .iter()
        .map(|spec| spec.name.to_string())
        .collect();
    base.sort();
    let mut vector: Vec<String> = CANONICAL_COLUMN_FAMILIES
        .iter()
        .map(|spec| spec.name.to_string())
        .collect();
    vector.sort();

    let manifest_exists = instance.join(STORAGE_MANIFEST_FILE).exists();
    if actual == base {
        ensure!(
            !manifest_exists,
            InvalidFormatSnafu {
                message: format!(
                    "Base-v1 instance {} unexpectedly contains a storage manifest",
                    instance.display()
                )
            }
        );
        return Ok(MigrationSourceProfile::BaseV1SixCf);
    }
    if actual == vector {
        ensure!(
            manifest_exists,
            InvalidFormatSnafu {
                message: format!(
                    "Vector-v1 instance {} is missing its v1 manifest",
                    instance.display()
                )
            }
        );
        VectorV1Manifest::read_from_dir(instance)?;
        return Ok(MigrationSourceProfile::VectorSetV1SevenCf);
    }
    Err(InvalidFormatSnafu {
        message: format!(
            "unregistered legacy column-family layout in {}: {:?}",
            instance.display(),
            actual
        ),
    }
    .build())
}

pub fn prepare_or_resume_migration(
    root: &Path,
    db_instance_num: usize,
    options: &StorageOptions,
) -> Result<Option<MigrationSourceProfile>> {
    let profile = classify_storage_root(root, db_instance_num, options)?;
    let Some(profile) = profile else {
        return Ok(None);
    };

    let mut root_manifest = if root.join(ROOT_STORAGE_MANIFEST_FILE).exists() {
        RootStorageManifestV2::read_from_dir(root)?
    } else {
        let transaction_id = Uuid::new_v4();
        let transaction = MigrationTransaction::new(
            transaction_id,
            1,
            2,
            profile,
            MigrationPhase::SourceDetected,
            0,
            LIVE_SOURCE_NAME,
            format!(".kiwi-shadow-{transaction_id}"),
            format!(".kiwi-backup-{transaction_id}"),
        );
        let manifest = RootStorageManifestV2::new(
            Uuid::new_v4(),
            db_instance_num as u32,
            SLOT_MAPPING_VERSION,
            slot_mapping_digest(db_instance_num),
            Some(transaction),
        )?;
        manifest.write_to_dir_atomically(root)?;
        maybe_fail(root, MigrationFaultPoint::AfterSourceDetected)?;
        manifest
    };
    root_manifest.validate_runtime_topology(db_instance_num)?;
    let transaction = root_manifest.migration().cloned().ok_or_else(|| {
        InvalidFormatSnafu {
            message: "classified migration root has no migration transaction".to_string(),
        }
        .build()
    })?;
    ensure!(
        transaction.source_profile == profile,
        InvalidFormatSnafu {
            message: "persisted migration profile disagrees with classified source".to_string()
        }
    );
    let layout = MigrationLayout::new(root, &transaction)?;
    validate_resume_layout(&layout, &transaction, db_instance_num, options)?;

    if transaction.phase == MigrationPhase::RollbackWindowClosed {
        rebind_all_v2_instances(&layout, db_instance_num, &root_manifest)?;
        verify_live_v2_instances(root, db_instance_num, &root_manifest, options)?;
        return Ok(Some(profile));
    }

    if transaction.phase == MigrationPhase::Committed {
        rebind_all_v2_instances(&layout, db_instance_num, &root_manifest)?;
        verify_instances(
            &layout,
            db_instance_num,
            &root_manifest,
            options,
            InstanceLocation::Live,
        )?;
        return Ok(Some(profile));
    }

    if transaction.phase == MigrationPhase::SourceDetected {
        fs::create_dir_all(layout.shadow_root()).context(IoSnafu)?;
        sync_directory(&layout.shadow_root()).context(IoSnafu)?;
        sync_directory(root).context(IoSnafu)?;
        persist_transition(
            root,
            &mut root_manifest,
            MigrationPhase::ShadowPrepared,
            0,
            &layout,
            db_instance_num,
        )?;
        maybe_fail(root, MigrationFaultPoint::AfterShadowPrepared)?;
    }

    let phase = root_manifest
        .migration()
        .expect("migration exists after SourceDetected")
        .phase;
    if matches!(
        phase,
        MigrationPhase::ShadowPrepared
            | MigrationPhase::InstanceCopied
            | MigrationPhase::InstanceUpgraded
    ) {
        rebind_all_v2_instances(&layout, db_instance_num, &root_manifest)?;
        for instance_id in 0..db_instance_num as u32 {
            let shadow = layout.shadow_instance(instance_id);
            if !is_v2_manifest(&shadow)? {
                if root_manifest.migration().is_some_and(|transaction| {
                    transaction.phase != MigrationPhase::InstanceCopied
                        || transaction.current_instance != instance_id
                }) && shadow.exists()
                {
                    fs::remove_dir_all(&shadow).context(IoSnafu)?;
                    sync_parent_directory(&shadow).context(IoSnafu)?;
                }
                if !shadow.exists() {
                    copy_directory_durable(&layout.source_instance(instance_id), &shadow)?;
                    persist_transition(
                        root,
                        &mut root_manifest,
                        MigrationPhase::InstanceCopied,
                        instance_id,
                        &layout,
                        db_instance_num,
                    )?;
                    maybe_fail(root, MigrationFaultPoint::AfterInstanceCopied(instance_id))?;
                }
                upgrade_shadow_instance(&shadow, instance_id, profile, options, &root_manifest)?;
                persist_transition(
                    root,
                    &mut root_manifest,
                    MigrationPhase::InstanceUpgraded,
                    instance_id,
                    &layout,
                    db_instance_num,
                )?;
                maybe_fail(
                    root,
                    MigrationFaultPoint::AfterInstanceUpgraded(instance_id),
                )?;
            }
        }
        verify_instances(
            &layout,
            db_instance_num,
            &root_manifest,
            options,
            InstanceLocation::Shadow,
        )?;
        persist_transition(
            root,
            &mut root_manifest,
            MigrationPhase::AllInstancesVerified,
            db_instance_num.saturating_sub(1) as u32,
            &layout,
            db_instance_num,
        )?;
        maybe_fail(root, MigrationFaultPoint::AfterAllInstancesVerified)?;
    }

    if root_manifest
        .migration()
        .is_some_and(|transaction| transaction.phase == MigrationPhase::AllInstancesVerified)
    {
        persist_transition(
            root,
            &mut root_manifest,
            MigrationPhase::SwitchPrepared,
            0,
            &layout,
            db_instance_num,
        )?;
        maybe_fail(root, MigrationFaultPoint::AfterSwitchPrepared)?;
    }

    fs::create_dir_all(layout.backup_root()).context(IoSnafu)?;
    for instance_id in 0..db_instance_num as u32 {
        let source = layout.source_instance(instance_id);
        let shadow = layout.shadow_instance(instance_id);
        let backup = layout.backup_instance(instance_id);

        if !backup.exists() {
            ensure!(
                source.exists() && shadow.exists(),
                InvalidFormatSnafu {
                    message: format!(
                        "cannot move instance {instance_id} to backup: source/shadow layout is inconsistent"
                    )
                }
            );
            fs::rename(&source, &backup).context(IoSnafu)?;
            sync_directory(root).context(IoSnafu)?;
            sync_directory(&layout.backup_root()).context(IoSnafu)?;
            persist_transition(
                root,
                &mut root_manifest,
                MigrationPhase::OldMovedToBackup,
                instance_id,
                &layout,
                db_instance_num,
            )?;
            maybe_fail(
                root,
                MigrationFaultPoint::AfterOldMovedToBackup(instance_id),
            )?;
        }

        if !source.exists() {
            ensure!(
                backup.exists() && shadow.exists(),
                InvalidFormatSnafu {
                    message: format!(
                        "cannot promote instance {instance_id}: backup/shadow layout is inconsistent"
                    )
                }
            );
            fs::rename(&shadow, &source).context(IoSnafu)?;
            sync_directory(root).context(IoSnafu)?;
            sync_directory(&layout.shadow_root()).context(IoSnafu)?;
            persist_transition(
                root,
                &mut root_manifest,
                MigrationPhase::ShadowPromoted,
                instance_id,
                &layout,
                db_instance_num,
            )?;
            maybe_fail(root, MigrationFaultPoint::AfterShadowPromoted(instance_id))?;
        } else {
            ensure!(
                backup.exists() && !shadow.exists() && is_v2_manifest(&source)?,
                InvalidFormatSnafu {
                    message: format!(
                        "instance {instance_id} has an ambiguous source/backup/shadow layout"
                    )
                }
            );
        }
    }

    if layout.shadow_root().exists()
        && fs::read_dir(layout.shadow_root())
            .context(IoSnafu)?
            .next()
            .is_none()
    {
        fs::remove_dir(layout.shadow_root()).context(IoSnafu)?;
        sync_directory(root).context(IoSnafu)?;
    }

    rebind_all_v2_instances(&layout, db_instance_num, &root_manifest)?;
    verify_instances(
        &layout,
        db_instance_num,
        &root_manifest,
        options,
        InstanceLocation::Live,
    )?;

    Ok(Some(profile))
}

/// Persist the phases that are only truthful after the production `Storage`/`Redis`
/// entrypoint has successfully opened every promoted instance.
pub fn finalize_migration_after_storage_open(
    root: &Path,
    db_instance_num: usize,
    options: &StorageOptions,
) -> Result<bool> {
    let mut root_manifest = RootStorageManifestV2::read_from_dir(root)?;
    root_manifest.validate_runtime_topology(db_instance_num)?;
    let Some(transaction) = root_manifest.migration().cloned() else {
        return Ok(false);
    };
    if matches!(
        transaction.phase,
        MigrationPhase::Committed | MigrationPhase::RollbackWindowClosed
    ) {
        return Ok(false);
    }
    ensure!(
        matches!(
            transaction.phase,
            MigrationPhase::ShadowPromoted | MigrationPhase::NewStorageOpened
        ) && transaction.current_instance == db_instance_num.saturating_sub(1) as u32,
        InvalidFormatSnafu {
            message: format!(
                "production storage open cannot finalize migration phase {:?} at instance {}",
                transaction.phase, transaction.current_instance
            )
        }
    );
    let layout = MigrationLayout::new(root, &transaction)?;
    validate_resume_layout(&layout, &transaction, db_instance_num, options)?;
    rebind_all_v2_instances(&layout, db_instance_num, &root_manifest)?;
    verify_instances(
        &layout,
        db_instance_num,
        &root_manifest,
        options,
        InstanceLocation::Live,
    )?;

    if transaction.phase == MigrationPhase::ShadowPromoted {
        persist_transition(
            root,
            &mut root_manifest,
            MigrationPhase::NewStorageOpened,
            transaction.current_instance,
            &layout,
            db_instance_num,
        )?;
        maybe_fail(root, MigrationFaultPoint::AfterNewStorageOpened)?;
    }

    persist_transition(
        root,
        &mut root_manifest,
        MigrationPhase::Committed,
        transaction.current_instance,
        &layout,
        db_instance_num,
    )?;
    maybe_fail(root, MigrationFaultPoint::AfterCommitted)?;
    Ok(true)
}

pub fn close_rollback_window(root: &Path) -> Result<bool> {
    let mut manifest = RootStorageManifestV2::read_from_dir(root)?;
    let Some(transaction) = manifest.migration().cloned() else {
        return Ok(false);
    };
    if transaction.phase == MigrationPhase::RollbackWindowClosed {
        return Ok(false);
    }
    ensure!(
        transaction.phase == MigrationPhase::Committed,
        InvalidFormatSnafu {
            message: format!(
                "cannot close rollback window from migration phase {:?}",
                transaction.phase
            )
        }
    );
    let layout = MigrationLayout::new(root, &transaction)?;
    let instance_count = manifest.db_instance_num() as usize;
    persist_transition(
        root,
        &mut manifest,
        MigrationPhase::RollbackWindowClosed,
        transaction.current_instance,
        &layout,
        instance_count,
    )?;
    Ok(true)
}

pub fn recover_or_rollback_before_admission(
    root: &Path,
    db_instance_num: usize,
    options: &StorageOptions,
) -> Result<bool> {
    if !root.join(ROOT_STORAGE_MANIFEST_FILE).exists() {
        return Ok(false);
    }
    let manifest = RootStorageManifestV2::read_from_dir(root)?;
    let Some(transaction) = manifest.migration().cloned() else {
        return Ok(false);
    };
    ensure!(
        transaction.phase != MigrationPhase::RollbackWindowClosed,
        InvalidFormatSnafu {
            message: "automatic backup restore is forbidden after RollbackWindowClosed".to_string()
        }
    );
    let layout = MigrationLayout::new(root, &transaction)?;
    validate_rollback_resume_layout(&layout, &transaction, db_instance_num, options)?;
    fs::create_dir_all(layout.shadow_root()).context(IoSnafu)?;
    sync_directory(root).context(IoSnafu)?;
    for instance_id in 0..db_instance_num as u32 {
        let source = layout.source_instance(instance_id);
        let shadow = layout.shadow_instance(instance_id);
        let backup = layout.backup_instance(instance_id);
        if backup.exists() {
            if source.exists() {
                ensure!(
                    is_v2_manifest(&source)? && !shadow.exists(),
                    InvalidFormatSnafu {
                        message: format!(
                            "cannot preserve failed v2 instance {instance_id} during rollback"
                        )
                    }
                );
                fs::rename(&source, &shadow).context(IoSnafu)?;
                sync_directory(root).context(IoSnafu)?;
                sync_directory(&layout.shadow_root()).context(IoSnafu)?;
                maybe_fail(
                    root,
                    MigrationFaultPoint::AfterRollbackV2MovedAside(instance_id),
                )?;
            } else {
                ensure!(
                    shadow.exists() && is_v2_manifest(&shadow)?,
                    InvalidFormatSnafu {
                        message: format!(
                            "rollback instance {instance_id} lost both live and preserved v2 copies"
                        )
                    }
                );
            }
            fs::rename(&backup, &source).context(IoSnafu)?;
            sync_directory(root).context(IoSnafu)?;
            sync_directory(&layout.backup_root()).context(IoSnafu)?;
            maybe_fail(
                root,
                MigrationFaultPoint::AfterRollbackLegacyRestored(instance_id),
            )?;
        }
    }
    for instance_id in 0..db_instance_num as u32 {
        let source = layout.source_instance(instance_id);
        ensure!(
            classify_instance(&source)? == transaction.source_profile,
            InvalidFormatSnafu {
                message: format!("rollback source profile mismatch for instance {instance_id}")
            }
        );
        strict_open_legacy_instance(&source, transaction.source_profile, options)?;
    }
    for instance_id in 0..db_instance_num as u32 {
        let shadow = layout.shadow_instance(instance_id);
        if shadow.exists() {
            fs::remove_dir_all(&shadow).context(IoSnafu)?;
            sync_directory(&layout.shadow_root()).context(IoSnafu)?;
            maybe_fail(
                root,
                MigrationFaultPoint::AfterRollbackShadowRemoved(instance_id),
            )?;
        }
    }
    if layout.shadow_root().exists() {
        ensure!(
            fs::read_dir(layout.shadow_root())
                .context(IoSnafu)?
                .next()
                .is_none(),
            InvalidFormatSnafu {
                message: "rollback shadow root still contains unexpected entries".to_string()
            }
        );
        fs::remove_dir(layout.shadow_root()).context(IoSnafu)?;
        sync_directory(root).context(IoSnafu)?;
        maybe_fail(root, MigrationFaultPoint::AfterRollbackShadowRootRemoved)?;
    }
    if layout.backup_root().exists() {
        ensure!(
            fs::read_dir(layout.backup_root())
                .context(IoSnafu)?
                .next()
                .is_none(),
            InvalidFormatSnafu {
                message: "rollback backup root still contains unexpected entries".to_string()
            }
        );
        fs::remove_dir(layout.backup_root()).context(IoSnafu)?;
        sync_directory(root).context(IoSnafu)?;
        maybe_fail(root, MigrationFaultPoint::AfterRollbackBackupRootRemoved)?;
    }
    fs::remove_file(root.join(ROOT_STORAGE_MANIFEST_FILE)).context(IoSnafu)?;
    sync_directory(root).context(IoSnafu)?;
    maybe_fail(root, MigrationFaultPoint::AfterRollbackRootManifestRemoved)?;
    Ok(true)
}

fn persist_transition(
    root: &Path,
    manifest: &mut RootStorageManifestV2,
    phase: MigrationPhase,
    current_instance: u32,
    layout: &MigrationLayout,
    db_instance_num: usize,
) -> Result<()> {
    let mut transaction = manifest.migration().cloned().ok_or_else(|| {
        InvalidFormatSnafu {
            message: "migration transition has no transaction".to_string(),
        }
        .build()
    })?;
    transaction.phase = phase;
    transaction.current_instance = current_instance;
    manifest.set_migration(Some(transaction))?;
    manifest.write_to_dir_atomically(root)?;
    rebind_all_v2_instances(layout, db_instance_num, manifest)
}

fn rebind_all_v2_instances(
    layout: &MigrationLayout,
    db_instance_num: usize,
    root_manifest: &RootStorageManifestV2,
) -> Result<()> {
    for instance_id in 0..db_instance_num as u32 {
        for instance in [
            layout.source_instance(instance_id),
            layout.shadow_instance(instance_id),
        ] {
            if instance.join(STORAGE_MANIFEST_FILE).exists() && is_v2_manifest(&instance)? {
                let mut manifest = InstanceStorageManifestV2::read_from_dir(&instance)?;
                manifest.rebind_root(root_manifest)?;
                manifest.write_to_dir_atomically(&instance)?;
            }
        }
    }
    Ok(())
}

fn is_v2_manifest(instance: &Path) -> Result<bool> {
    let path = instance.join(STORAGE_MANIFEST_FILE);
    if !path.exists() {
        return Ok(false);
    }
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path).context(IoSnafu)?)
        .map_err(|error| {
            InvalidFormatSnafu {
                message: format!("invalid storage manifest JSON: {error}"),
            }
            .build()
        })?;
    Ok(value.get("manifest_version").is_some())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstanceDiskKind {
    Missing,
    Legacy,
    V2,
    PartialShadow,
}

fn validate_resume_layout(
    layout: &MigrationLayout,
    transaction: &MigrationTransaction,
    db_instance_num: usize,
    options: &StorageOptions,
) -> Result<()> {
    validate_root_entries(layout, db_instance_num)?;
    let last_instance = db_instance_num.saturating_sub(1) as u32;
    let expected_fixed_instance = match transaction.phase {
        MigrationPhase::SourceDetected
        | MigrationPhase::ShadowPrepared
        | MigrationPhase::SwitchPrepared => Some(0),
        MigrationPhase::AllInstancesVerified
        | MigrationPhase::NewStorageOpened
        | MigrationPhase::Committed
        | MigrationPhase::RollbackWindowClosed => Some(last_instance),
        MigrationPhase::InstanceCopied
        | MigrationPhase::InstanceUpgraded
        | MigrationPhase::OldMovedToBackup
        | MigrationPhase::ShadowPromoted => None,
    };
    if let Some(expected) = expected_fixed_instance {
        ensure!(
            transaction.current_instance == expected,
            InvalidFormatSnafu {
                message: format!(
                    "migration phase {:?} requires current_instance {expected}, got {}",
                    transaction.phase, transaction.current_instance
                )
            }
        );
    }
    let allow_partial_shadow = matches!(
        transaction.phase,
        MigrationPhase::SourceDetected
            | MigrationPhase::ShadowPrepared
            | MigrationPhase::InstanceCopied
            | MigrationPhase::InstanceUpgraded
    );

    for instance_id in 0..db_instance_num as u32 {
        let source = layout.source_instance(instance_id);
        let shadow = layout.shadow_instance(instance_id);
        let backup = layout.backup_instance(instance_id);
        let source_kind =
            inspect_instance(&source, transaction.source_profile, options, false, "live")?;
        let shadow_kind = inspect_instance(
            &shadow,
            transaction.source_profile,
            options,
            allow_partial_shadow,
            "shadow",
        )?;
        let backup_kind = inspect_instance(
            &backup,
            transaction.source_profile,
            options,
            false,
            "backup",
        )?;

        let triplet = (source_kind, shadow_kind, backup_kind);
        let legacy_with_shadow = (
            InstanceDiskKind::Legacy,
            InstanceDiskKind::V2,
            InstanceDiskKind::Missing,
        );
        let moved_to_backup = (
            InstanceDiskKind::Missing,
            InstanceDiskKind::V2,
            InstanceDiskKind::Legacy,
        );
        let promoted = (
            InstanceDiskKind::V2,
            InstanceDiskKind::Missing,
            InstanceDiskKind::Legacy,
        );
        let untouched = (
            InstanceDiskKind::Legacy,
            InstanceDiskKind::Missing,
            InstanceDiskKind::Missing,
        );
        let valid = match transaction.phase {
            MigrationPhase::SourceDetected => triplet == untouched,
            MigrationPhase::ShadowPrepared => {
                source_kind == InstanceDiskKind::Legacy
                    && backup_kind == InstanceDiskKind::Missing
                    && if instance_id == 0 {
                        matches!(
                            shadow_kind,
                            InstanceDiskKind::Missing
                                | InstanceDiskKind::Legacy
                                | InstanceDiskKind::PartialShadow
                        )
                    } else {
                        shadow_kind == InstanceDiskKind::Missing
                    }
            }
            MigrationPhase::InstanceCopied => {
                source_kind == InstanceDiskKind::Legacy
                    && backup_kind == InstanceDiskKind::Missing
                    && if instance_id < transaction.current_instance {
                        shadow_kind == InstanceDiskKind::V2
                    } else if instance_id == transaction.current_instance {
                        matches!(
                            shadow_kind,
                            InstanceDiskKind::Legacy
                                | InstanceDiskKind::V2
                                | InstanceDiskKind::PartialShadow
                        )
                    } else {
                        shadow_kind == InstanceDiskKind::Missing
                    }
            }
            MigrationPhase::InstanceUpgraded => {
                source_kind == InstanceDiskKind::Legacy
                    && backup_kind == InstanceDiskKind::Missing
                    && if instance_id <= transaction.current_instance {
                        shadow_kind == InstanceDiskKind::V2
                    } else if instance_id == transaction.current_instance.saturating_add(1) {
                        matches!(
                            shadow_kind,
                            InstanceDiskKind::Missing
                                | InstanceDiskKind::Legacy
                                | InstanceDiskKind::PartialShadow
                        )
                    } else {
                        shadow_kind == InstanceDiskKind::Missing
                    }
            }
            MigrationPhase::AllInstancesVerified => triplet == legacy_with_shadow,
            MigrationPhase::SwitchPrepared => {
                if instance_id == 0 {
                    triplet == legacy_with_shadow || triplet == moved_to_backup
                } else {
                    triplet == legacy_with_shadow
                }
            }
            MigrationPhase::OldMovedToBackup => {
                if instance_id < transaction.current_instance {
                    triplet == promoted
                } else if instance_id == transaction.current_instance {
                    triplet == moved_to_backup || triplet == promoted
                } else {
                    triplet == legacy_with_shadow
                }
            }
            MigrationPhase::ShadowPromoted => {
                if instance_id <= transaction.current_instance {
                    triplet == promoted
                } else if instance_id == transaction.current_instance.saturating_add(1) {
                    triplet == legacy_with_shadow || triplet == moved_to_backup
                } else {
                    triplet == legacy_with_shadow
                }
            }
            MigrationPhase::NewStorageOpened | MigrationPhase::Committed => triplet == promoted,
            MigrationPhase::RollbackWindowClosed => {
                source_kind == InstanceDiskKind::V2
                    && shadow_kind == InstanceDiskKind::Missing
                    && matches!(
                        backup_kind,
                        InstanceDiskKind::Legacy | InstanceDiskKind::Missing
                    )
            }
        };
        ensure!(
            valid,
            InvalidFormatSnafu {
                message: format!(
                    "migration phase {:?} has inconsistent instance {instance_id} layout: live={source_kind:?}, shadow={shadow_kind:?}, backup={backup_kind:?}",
                    transaction.phase
                )
            }
        );

        if transaction.phase == MigrationPhase::RollbackWindowClosed {
            continue;
        }
        match (source_kind, shadow_kind, backup_kind) {
            (InstanceDiskKind::Legacy, InstanceDiskKind::V2, _) => verify_logical_pair(
                &source,
                &shadow,
                transaction.source_profile,
                options,
                instance_id,
            )?,
            (InstanceDiskKind::Missing, InstanceDiskKind::V2, InstanceDiskKind::Legacy) => {
                verify_logical_pair(
                    &backup,
                    &shadow,
                    transaction.source_profile,
                    options,
                    instance_id,
                )?;
            }
            (InstanceDiskKind::V2, InstanceDiskKind::Missing, InstanceDiskKind::Legacy) => {
                verify_logical_pair(
                    &backup,
                    &source,
                    transaction.source_profile,
                    options,
                    instance_id,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_rollback_resume_layout(
    layout: &MigrationLayout,
    transaction: &MigrationTransaction,
    db_instance_num: usize,
    options: &StorageOptions,
) -> Result<()> {
    validate_root_entries(layout, db_instance_num)?;
    for instance_id in 0..db_instance_num as u32 {
        let source = layout.source_instance(instance_id);
        let shadow = layout.shadow_instance(instance_id);
        let backup = layout.backup_instance(instance_id);
        let source_kind =
            inspect_instance(&source, transaction.source_profile, options, false, "live")?;
        let shadow_kind = inspect_instance(
            &shadow,
            transaction.source_profile,
            options,
            true,
            "rollback shadow",
        )?;
        let backup_kind = inspect_instance(
            &backup,
            transaction.source_profile,
            options,
            false,
            "rollback backup",
        )?;
        let restored_or_not_switched = source_kind == InstanceDiskKind::Legacy
            && backup_kind == InstanceDiskKind::Missing
            && matches!(
                shadow_kind,
                InstanceDiskKind::Missing
                    | InstanceDiskKind::Legacy
                    | InstanceDiskKind::V2
                    | InstanceDiskKind::PartialShadow
            );
        let failed_v2_moved_aside = source_kind == InstanceDiskKind::Missing
            && shadow_kind == InstanceDiskKind::V2
            && backup_kind == InstanceDiskKind::Legacy;
        let promoted_v2 = source_kind == InstanceDiskKind::V2
            && shadow_kind == InstanceDiskKind::Missing
            && backup_kind == InstanceDiskKind::Legacy;
        ensure!(
            restored_or_not_switched || failed_v2_moved_aside || promoted_v2,
            InvalidFormatSnafu {
                message: format!(
                    "rollback has inconsistent instance {instance_id} layout: live={source_kind:?}, shadow={shadow_kind:?}, backup={backup_kind:?}"
                )
            }
        );
        match (source_kind, shadow_kind, backup_kind) {
            (InstanceDiskKind::Legacy, InstanceDiskKind::V2, InstanceDiskKind::Missing) => {
                verify_logical_pair(
                    &source,
                    &shadow,
                    transaction.source_profile,
                    options,
                    instance_id,
                )?;
            }
            (InstanceDiskKind::Missing, InstanceDiskKind::V2, InstanceDiskKind::Legacy) => {
                verify_logical_pair(
                    &backup,
                    &shadow,
                    transaction.source_profile,
                    options,
                    instance_id,
                )?;
            }
            (InstanceDiskKind::V2, InstanceDiskKind::Missing, InstanceDiskKind::Legacy) => {
                verify_logical_pair(
                    &backup,
                    &source,
                    transaction.source_profile,
                    options,
                    instance_id,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_root_entries(layout: &MigrationLayout, db_instance_num: usize) -> Result<()> {
    let root_manifest_temp = Path::new(ROOT_STORAGE_MANIFEST_FILE).with_extension("tmp");
    for entry in fs::read_dir(&layout.root).context(IoSnafu)? {
        let entry = entry.context(IoSnafu)?;
        let name = entry.file_name();
        let name_text = name.to_str().ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!(
                    "migration root {} contains a non-UTF-8 entry",
                    layout.root.display()
                ),
            }
            .build()
        })?;
        let file_type = entry.file_type().context(IoSnafu)?;
        if name_text == ROOT_STORAGE_MANIFEST_FILE || name == root_manifest_temp.as_os_str() {
            ensure!(
                file_type.is_file() && !file_type.is_symlink(),
                InvalidFormatSnafu {
                    message: format!(
                        "migration manifest artifact {} is not a real file",
                        entry.path().display()
                    )
                }
            );
            continue;
        }
        if name_text == layout.shadow_name || name_text == layout.backup_name {
            ensure!(
                file_type.is_dir() && !file_type.is_symlink(),
                InvalidFormatSnafu {
                    message: format!(
                        "migration artifact {} is not a real directory",
                        entry.path().display()
                    )
                }
            );
            continue;
        }
        let is_instance = name_text.parse::<usize>().is_ok_and(|instance_id| {
            name_text == instance_id.to_string() && instance_id < db_instance_num
        });
        ensure!(
            is_instance && file_type.is_dir() && !file_type.is_symlink(),
            InvalidFormatSnafu {
                message: format!(
                    "migration root {} contains unexpected entry {name_text}",
                    layout.root.display()
                )
            }
        );
    }
    validate_instance_root_entries(&layout.shadow_root(), db_instance_num, "shadow")?;
    validate_instance_root_entries(&layout.backup_root(), db_instance_num, "backup")
}

fn validate_instance_root_entries(root: &Path, db_instance_num: usize, label: &str) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    let root_file_type = fs::symlink_metadata(root).context(IoSnafu)?.file_type();
    ensure!(
        root_file_type.is_dir() && !root_file_type.is_symlink(),
        InvalidFormatSnafu {
            message: format!(
                "migration {label} root {} is not a real directory",
                root.display()
            )
        }
    );
    for entry in fs::read_dir(root).context(IoSnafu)? {
        let entry = entry.context(IoSnafu)?;
        let name = entry.file_name();
        let name_text = name.to_str().ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!(
                    "migration {label} root {} contains a non-UTF-8 entry",
                    root.display()
                ),
            }
            .build()
        })?;
        let instance_id = name_text.parse::<usize>().map_err(|_| {
            InvalidFormatSnafu {
                message: format!(
                    "migration {label} root {} contains non-instance entry {name_text}",
                    root.display()
                ),
            }
            .build()
        })?;
        let file_type = entry.file_type().context(IoSnafu)?;
        ensure!(
            name_text == instance_id.to_string()
                && instance_id < db_instance_num
                && file_type.is_dir()
                && !file_type.is_symlink(),
            InvalidFormatSnafu {
                message: format!(
                    "migration {label} entry {} is outside the configured instance topology",
                    entry.path().display()
                )
            }
        );
    }
    Ok(())
}

fn inspect_instance(
    instance: &Path,
    profile: MigrationSourceProfile,
    options: &StorageOptions,
    allow_partial_shadow: bool,
    label: &str,
) -> Result<InstanceDiskKind> {
    if !instance.exists() {
        return Ok(InstanceDiskKind::Missing);
    }
    ensure!(
        instance.is_dir(),
        InvalidFormatSnafu {
            message: format!(
                "migration {label} instance {} is not a directory",
                instance.display()
            )
        }
    );
    if is_v2_manifest(instance)? {
        validate_v2_instance_structure(instance, options)?;
        return Ok(InstanceDiskKind::V2);
    }
    match classify_instance(instance) {
        Ok(actual_profile) => {
            ensure!(
                actual_profile == profile,
                InvalidFormatSnafu {
                    message: format!(
                        "migration {label} source profile mismatch in {}: journal={profile:?}, disk={actual_profile:?}",
                        instance.display()
                    )
                }
            );
            strict_open_legacy_instance(instance, profile, options)?;
            Ok(InstanceDiskKind::Legacy)
        }
        Err(_) if allow_partial_shadow => Ok(InstanceDiskKind::PartialShadow),
        Err(error) => Err(error),
    }
}

fn validate_v2_instance_structure(instance: &Path, options: &StorageOptions) -> Result<()> {
    let manifest = InstanceStorageManifestV2::read_from_dir(instance)?;
    let names: Vec<&str> = CANONICAL_COLUMN_FAMILIES
        .iter()
        .map(|spec| spec.name)
        .collect();
    let actual: HashSet<String> = DB::list_cf(&Options::default(), instance)
        .context(RocksSnafu)?
        .into_iter()
        .collect();
    ensure!(
        actual.len() == names.len() && names.iter().all(|name| actual.contains(*name)),
        InvalidFormatSnafu {
            message: format!(
                "v2 instance {} has a non-canonical CF set",
                instance.display()
            )
        }
    );
    let db = open_instance_strict(instance, options, &names)?;
    validate_vector_identity(&db, manifest.storage_incarnation(), instance)
}

fn upgrade_shadow_instance(
    shadow: &Path,
    instance_id: u32,
    profile: MigrationSourceProfile,
    options: &StorageOptions,
    root_manifest: &RootStorageManifestV2,
) -> Result<()> {
    let (storage_incarnation, next_generation) = match profile {
        MigrationSourceProfile::BaseV1SixCf => {
            let listed = DB::list_cf(&Options::default(), shadow).context(RocksSnafu)?;
            if !listed
                .iter()
                .any(|name| name == CANONICAL_COLUMN_FAMILIES[6].name)
            {
                let names: Vec<&str> = CANONICAL_COLUMN_FAMILIES[..BASE_V1_CF_COUNT]
                    .iter()
                    .map(|spec| spec.name)
                    .collect();
                let mut db_options = options.options.clone();
                db_options.create_if_missing(false);
                db_options.create_missing_column_families(false);
                let db =
                    DB::open_cf_descriptors(&db_options, shadow, descriptors_for(options, &names))
                        .context(RocksSnafu)?;
                db.create_cf(
                    CANONICAL_COLUMN_FAMILIES[6].name,
                    &cf_options(options, &CANONICAL_COLUMN_FAMILIES[6]),
                )
                .context(RocksSnafu)?;
                maybe_fail(
                    shadow.parent().and_then(Path::parent).ok_or_else(|| {
                        InvalidFormatSnafu {
                            message: format!(
                                "shadow instance {} has no migration root",
                                shadow.display()
                            ),
                        }
                        .build()
                    })?,
                    MigrationFaultPoint::AfterVectorCfCreatedBeforeInstanceManifest(instance_id),
                )?;
            }
            (rand::thread_rng().r#gen::<u64>().max(1), 1)
        }
        MigrationSourceProfile::VectorSetV1SevenCf => {
            let legacy = VectorV1Manifest::read_from_dir(shadow)?;
            (legacy.storage_incarnation, legacy.next_generation)
        }
    };
    let manifest = InstanceStorageManifestV2::new(
        instance_id,
        Uuid::new_v4(),
        root_manifest,
        storage_incarnation,
        next_generation,
    )?;
    manifest.write_to_dir_atomically(shadow)
}

#[derive(Debug, Clone, Copy)]
enum InstanceLocation {
    Shadow,
    Live,
}

fn verify_instances(
    layout: &MigrationLayout,
    db_instance_num: usize,
    root_manifest: &RootStorageManifestV2,
    options: &StorageOptions,
    location: InstanceLocation,
) -> Result<()> {
    let expected: HashSet<&str> = CANONICAL_COLUMN_FAMILIES
        .iter()
        .map(|spec| spec.name)
        .collect();
    for instance_id in 0..db_instance_num as u32 {
        let instance = match location {
            InstanceLocation::Shadow => layout.shadow_instance(instance_id),
            InstanceLocation::Live => layout.source_instance(instance_id),
        };
        let legacy = match location {
            InstanceLocation::Shadow => layout.source_instance(instance_id),
            InstanceLocation::Live => layout.backup_instance(instance_id),
        };
        let actual: HashSet<String> = DB::list_cf(&Options::default(), &instance)
            .context(RocksSnafu)?
            .into_iter()
            .collect();
        ensure!(
            actual.len() == expected.len()
                && actual.iter().all(|name| expected.contains(name.as_str())),
            InvalidFormatSnafu {
                message: format!(
                    "migrated instance {} has a non-canonical CF set: {:?}",
                    instance.display(),
                    actual
                )
            }
        );
        let manifest = InstanceStorageManifestV2::read_from_dir(&instance)?;
        manifest.validate_root_binding(instance_id, root_manifest)?;
        let profile = root_manifest
            .migration()
            .expect("verified migration root has a transaction")
            .source_profile;
        verify_logical_pair(&legacy, &instance, profile, options, instance_id)?;
    }
    Ok(())
}

fn verify_live_v2_instances(
    root: &Path,
    db_instance_num: usize,
    root_manifest: &RootStorageManifestV2,
    options: &StorageOptions,
) -> Result<()> {
    for instance_id in 0..db_instance_num as u32 {
        let instance = root.join(instance_id.to_string());
        validate_v2_instance_structure(&instance, options)?;
        InstanceStorageManifestV2::read_from_dir(&instance)?
            .validate_root_binding(instance_id, root_manifest)?;
    }
    Ok(())
}

fn verify_logical_pair(
    legacy: &Path,
    migrated: &Path,
    profile: MigrationSourceProfile,
    options: &StorageOptions,
    instance_id: u32,
) -> Result<()> {
    let legacy_count = match profile {
        MigrationSourceProfile::BaseV1SixCf => BASE_V1_CF_COUNT,
        MigrationSourceProfile::VectorSetV1SevenCf => CANONICAL_COLUMN_FAMILIES.len(),
    };
    let legacy_names: Vec<&str> = CANONICAL_COLUMN_FAMILIES[..legacy_count]
        .iter()
        .map(|spec| spec.name)
        .collect();
    let migrated_names: Vec<&str> = CANONICAL_COLUMN_FAMILIES
        .iter()
        .map(|spec| spec.name)
        .collect();
    let legacy_db = open_instance_strict(legacy, options, &legacy_names)?;
    let migrated_db = open_instance_strict(migrated, options, &migrated_names)?;
    for cf_name in &legacy_names {
        ensure!(
            logical_cf_digest(&legacy_db, cf_name)? == logical_cf_digest(&migrated_db, cf_name)?,
            InvalidFormatSnafu {
                message: format!(
                    "migration changed logical data for instance {instance_id} CF {cf_name}"
                )
            }
        );
    }
    if legacy_count == BASE_V1_CF_COUNT {
        ensure!(
            logical_cf_digest(&migrated_db, CANONICAL_COLUMN_FAMILIES[6].name)?.0 == 0,
            InvalidFormatSnafu {
                message: format!(
                    "Base-v1 migration created non-empty Vector CF for instance {instance_id}"
                )
            }
        );
    }
    let manifest = InstanceStorageManifestV2::read_from_dir(migrated)?;
    validate_vector_identity(&migrated_db, manifest.storage_incarnation(), migrated)
}

fn strict_open_legacy_instance(
    instance: &Path,
    profile: MigrationSourceProfile,
    options: &StorageOptions,
) -> Result<()> {
    let count = match profile {
        MigrationSourceProfile::BaseV1SixCf => BASE_V1_CF_COUNT,
        MigrationSourceProfile::VectorSetV1SevenCf => CANONICAL_COLUMN_FAMILIES.len(),
    };
    let names: Vec<&str> = CANONICAL_COLUMN_FAMILIES[..count]
        .iter()
        .map(|spec| spec.name)
        .collect();
    let db = open_instance_strict(instance, options, &names)?;
    if profile == MigrationSourceProfile::VectorSetV1SevenCf {
        let manifest = VectorV1Manifest::read_from_dir(instance)?;
        validate_vector_identity(&db, manifest.storage_incarnation, instance)?;
    }
    Ok(())
}

pub(crate) fn validate_base_v1_snapshot_instance(
    instance: &Path,
    options: &StorageOptions,
) -> Result<()> {
    ensure!(
        classify_instance(instance)? == MigrationSourceProfile::BaseV1SixCf,
        InvalidFormatSnafu {
            message: format!(
                "legacy snapshot instance {} is not the registered Base-v1 six-CF profile",
                instance.display()
            )
        }
    );
    let names: Vec<&str> = CANONICAL_COLUMN_FAMILIES[..BASE_V1_CF_COUNT]
        .iter()
        .map(|spec| spec.name)
        .collect();
    let db = open_instance_strict(instance, options, &names)?;
    let meta_cf = db
        .cf_handle(CANONICAL_COLUMN_FAMILIES[0].name)
        .ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!("Base-v1 snapshot {} is missing MetaCF", instance.display()),
            }
            .build()
        })?;
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(true);
    for entry in db.iterator_cf_opt(&meta_cf, read_options, IteratorMode::Start) {
        let (_, value) = entry.context(RocksSnafu)?;
        ensure!(
            value.first() != Some(&(DataType::VectorSet as u8)),
            InvalidFormatSnafu {
                message: format!(
                    "Base-v1 snapshot {} contains Vector Set metadata without the registered Vector layout",
                    instance.display()
                )
            }
        );
    }
    Ok(())
}

pub(crate) fn validate_vector_v1_snapshot_instance(
    instance: &Path,
    expected_storage_incarnation: u64,
    options: &StorageOptions,
) -> Result<()> {
    ensure!(
        classify_instance(instance)? == MigrationSourceProfile::VectorSetV1SevenCf,
        InvalidFormatSnafu {
            message: format!(
                "historical snapshot instance {} is not the registered Vector-v1 seven-CF profile",
                instance.display()
            )
        }
    );
    let names: Vec<&str> = CANONICAL_COLUMN_FAMILIES
        .iter()
        .map(|spec| spec.name)
        .collect();
    let db = open_instance_strict(instance, options, &names)?;
    let manifest = VectorV1Manifest::read_from_dir(instance)?;
    ensure!(
        manifest.storage_incarnation == expected_storage_incarnation,
        InvalidFormatSnafu {
            message: format!(
                "Vector-v1 snapshot {} storage incarnation {} does not match metadata {}",
                instance.display(),
                manifest.storage_incarnation,
                expected_storage_incarnation
            )
        }
    );
    validate_vector_identity(&db, manifest.storage_incarnation, instance)
}

fn open_instance_strict(instance: &Path, options: &StorageOptions, names: &[&str]) -> Result<DB> {
    let mut db_options = options.options.clone();
    db_options.create_if_missing(false);
    db_options.create_missing_column_families(false);
    DB::open_cf_descriptors(&db_options, instance, descriptors_for(options, names))
        .context(RocksSnafu)
}

fn logical_cf_digest(db: &DB, cf_name: &str) -> Result<(u64, [u8; 32])> {
    let cf = db.cf_handle(cf_name).ok_or_else(|| {
        InvalidFormatSnafu {
            message: format!("column family {cf_name} is missing during migration verification"),
        }
        .build()
    })?;
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(true);
    let mut count = 0_u64;
    let mut digest = Sha256::new();
    for entry in db.iterator_cf_opt(&cf, read_options, IteratorMode::Start) {
        let (key, value) = entry.context(RocksSnafu)?;
        count = count.checked_add(1).ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!("column family {cf_name} entry count overflow"),
            }
            .build()
        })?;
        digest.update((key.len() as u64).to_le_bytes());
        digest.update(&key);
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(&value);
    }
    Ok((count, digest.finalize().into()))
}

pub(crate) fn logical_open_db_digest(db: &DB) -> Result<ManifestDigest> {
    let mut encoded = Vec::with_capacity(CANONICAL_COLUMN_FAMILIES.len() * 96);
    for spec in CANONICAL_COLUMN_FAMILIES {
        let (count, digest) = logical_cf_digest(db, spec.name)?;
        encoded.extend_from_slice(&(spec.name.len() as u64).to_le_bytes());
        encoded.extend_from_slice(spec.name.as_bytes());
        encoded.extend_from_slice(&count.to_le_bytes());
        encoded.extend_from_slice(&digest);
    }
    Ok(ManifestDigest::compute(&encoded))
}

/// Compute one checksum-verified logical digest per current-schema instance.
/// The result is stable across RocksDB physical compaction and file layout.
pub fn logical_snapshot_digests_from_root(
    root: &Path,
    db_instance_num: usize,
    options: &StorageOptions,
) -> Result<Vec<ManifestDigest>> {
    let root_manifest = RootStorageManifestV2::read_from_dir(root)?;
    root_manifest.validate_runtime_topology(db_instance_num)?;
    let names: Vec<&str> = CANONICAL_COLUMN_FAMILIES
        .iter()
        .map(|spec| spec.name)
        .collect();
    (0..db_instance_num)
        .map(|instance_id| {
            let instance = root.join(instance_id.to_string());
            validate_v2_instance_structure(&instance, options)?;
            InstanceStorageManifestV2::read_from_dir(&instance)?
                .validate_root_binding(instance_id as u32, &root_manifest)?;
            let db = open_instance_strict(&instance, options, &names)?;
            logical_open_db_digest(&db)
        })
        .collect()
}

fn validate_vector_identity(db: &DB, storage_incarnation: u64, instance: &Path) -> Result<()> {
    crate::vector_consistency::validate_vector_consistency_db(
        db,
        storage_incarnation,
        &instance.display().to_string(),
    )?;
    Ok(())
}

fn descriptors_for(options: &StorageOptions, names: &[&str]) -> Vec<ColumnFamilyDescriptor> {
    names
        .iter()
        .map(|name| {
            let spec = CANONICAL_COLUMN_FAMILIES
                .iter()
                .find(|spec| spec.name == *name)
                .expect("registered migration CF");
            ColumnFamilyDescriptor::new(*name, cf_options(options, spec))
        })
        .collect()
}

fn cf_options(options: &StorageOptions, spec: &ColumnFamilySpec) -> Options {
    let mut cf_options = options.options.clone();
    cf_options.create_if_missing(false);
    cf_options.create_missing_column_families(false);
    match spec.comparator_id {
        ComparatorId::Bytewise => {}
        ComparatorId::ListsDataKey => cf_options.set_comparator(
            lists_data_key_comparator_name(),
            Box::new(lists_data_key_compare),
        ),
        ComparatorId::ZsetsScoreKey => cf_options.set_comparator(
            zsets_score_key_comparator_name(),
            Box::new(zsets_score_key_compare),
        ),
    }
    cf_options
}

fn copy_directory_durable(source: &Path, target: &Path) -> Result<()> {
    ensure!(
        source.is_dir() && !target.exists(),
        InvalidFormatSnafu {
            message: format!(
                "migration copy requires existing source and absent target: {} -> {}",
                source.display(),
                target.display()
            )
        }
    );
    fs::create_dir(target).context(IoSnafu)?;
    for entry in fs::read_dir(source).context(IoSnafu)? {
        let entry = entry.context(IoSnafu)?;
        let file_type = entry.file_type().context(IoSnafu)?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        ensure!(
            !file_type.is_symlink(),
            InvalidFormatSnafu {
                message: format!("migration refuses symbolic link {}", source_path.display())
            }
        );
        if file_type.is_dir() {
            copy_directory_durable(&source_path, &target_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &target_path).context(IoSnafu)?;
            OpenOptions::new()
                .write(true)
                .open(&target_path)
                .context(IoSnafu)?
                .sync_all()
                .context(IoSnafu)?;
        } else {
            return Err(InvalidFormatSnafu {
                message: format!("migration refuses non-file entry {}", source_path.display()),
            }
            .build());
        }
    }
    sync_directory(target).context(IoSnafu)?;
    sync_parent_directory(target).context(IoSnafu)?;
    Ok(())
}
