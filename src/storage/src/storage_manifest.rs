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

//! Durable root and per-instance storage manifests.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[cfg(any(test, feature = "test-fault-injection"))]
use std::collections::HashSet;
#[cfg(any(test, feature = "test-fault-injection"))]
use std::sync::LazyLock;

#[cfg(any(test, feature = "test-fault-injection"))]
use parking_lot::Mutex as ParkingMutex;

use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::{ResultExt, ensure};
use uuid::Uuid;

use crate::durable_fs::sync_parent_directory;
use crate::error::{InvalidFormatSnafu, IoSnafu, Result};
use crate::storage_schema::{CANONICAL_COLUMN_FAMILIES, ColumnFamilySpec};

pub const ROOT_STORAGE_MANIFEST_FILE: &str = "__kiwi_root_storage_manifest";
pub const STORAGE_MANIFEST_FILE: &str = "__kiwi_storage_manifest";
pub const ROOT_STORAGE_MANIFEST_VERSION: u32 = 2;
pub const INSTANCE_STORAGE_MANIFEST_VERSION: u32 = 2;
pub const STORAGE_SCHEMA_VERSION_V2: u32 = 2;
pub const SLOT_MAPPING_VERSION: u32 = 1;
const FIRST_GENERATION: u64 = 1;
const ROLLBACK_FLOOR_MIN: u32 = 1;
const ROLLBACK_FLOOR_MAX: u32 = 1;
const PRODUCER_IDENTITY_PREFIX: &str = "kiwi-storage/v";
const CURRENT_PRODUCER_IDENTITY: &str = "kiwi-storage/v2";
const KNOWN_FEATURES: &[&str] = &["vector_set"];

#[cfg(any(test, feature = "test-fault-injection"))]
static STORAGE_MANIFEST_PERSIST_FAILURES: LazyLock<ParkingMutex<HashSet<PathBuf>>> =
    LazyLock::new(|| ParkingMutex::new(HashSet::new()));

#[cfg(any(test, feature = "test-fault-injection"))]
#[doc(hidden)]
pub struct StorageManifestPersistFailureGuard {
    db_path: PathBuf,
}

#[cfg(any(test, feature = "test-fault-injection"))]
impl Drop for StorageManifestPersistFailureGuard {
    fn drop(&mut self) {
        STORAGE_MANIFEST_PERSIST_FAILURES
            .lock()
            .remove(&self.db_path);
    }
}

#[cfg(any(test, feature = "test-fault-injection"))]
#[doc(hidden)]
#[must_use]
pub fn fail_next_storage_manifest_persist(db_path: &Path) -> StorageManifestPersistFailureGuard {
    let db_path = db_path.to_path_buf();
    assert!(
        STORAGE_MANIFEST_PERSIST_FAILURES
            .lock()
            .insert(db_path.clone()),
        "storage manifest persist failure already registered for {}",
        db_path.display()
    );
    StorageManifestPersistFailureGuard { db_path }
}

/// Lowercase SHA-256 digest persisted by both manifest formats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ManifestDigest(String);

impl ManifestDigest {
    pub fn compute(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        let mut encoded = String::with_capacity(digest.len() * 2);
        for byte in digest {
            encoded.push_str(&format!("{byte:02x}"));
        }
        Self(encoded)
    }

    /// Compute a manifest digest from the fixed-order JSON including its final digest field.
    pub fn compute_payload(encoded: &[u8]) -> Result<Self> {
        const MARKER: &[u8] = b",\"digest\":\"";
        let marker = encoded
            .windows(MARKER.len())
            .rposition(|window| window == MARKER)
            .ok_or_else(|| {
                InvalidFormatSnafu {
                    message: "manifest JSON is missing its final digest field".to_string(),
                }
                .build()
            })?;
        ensure!(
            encoded.ends_with(b"\"}"),
            InvalidFormatSnafu {
                message: "manifest digest field must be the final JSON field".to_string()
            }
        );
        let mut payload = encoded[..marker].to_vec();
        payload.push(b'}');
        Ok(Self::compute(&payload))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.0.len() == 64
                && self
                    .0
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            InvalidFormatSnafu {
                message: format!("invalid manifest digest {}", self.0)
            }
        );
        Ok(())
    }
}

/// Migration phases are declared here for durable DTO compatibility; Task 2 owns transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    SourceDetected,
    ShadowPrepared,
    InstanceCopied,
    InstanceUpgraded,
    AllInstancesVerified,
    SwitchPrepared,
    OldMovedToBackup,
    ShadowPromoted,
    NewStorageOpened,
    Committed,
    RollbackWindowClosed,
}

/// Persisted legacy layout classification used to select migration and rollback binaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationSourceProfile {
    BaseV1SixCf,
    VectorSetV1SevenCf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationTransaction {
    pub transaction_id: Uuid,
    pub from_schema: u32,
    pub to_schema: u32,
    pub source_profile: MigrationSourceProfile,
    pub phase: MigrationPhase,
    pub current_instance: u32,
    pub source_name: String,
    pub shadow_name: String,
    pub backup_name: String,
}

impl MigrationTransaction {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transaction_id: Uuid,
        from_schema: u32,
        to_schema: u32,
        source_profile: MigrationSourceProfile,
        phase: MigrationPhase,
        current_instance: u32,
        source_name: impl Into<String>,
        shadow_name: impl Into<String>,
        backup_name: impl Into<String>,
    ) -> Self {
        Self {
            transaction_id,
            from_schema,
            to_schema,
            source_profile,
            phase,
            current_instance,
            source_name: source_name.into(),
            shadow_name: shadow_name.into(),
            backup_name: backup_name.into(),
        }
    }

    fn validate(&self, db_instance_num: u32) -> Result<()> {
        ensure!(
            self.transaction_id != Uuid::nil(),
            InvalidFormatSnafu {
                message: "migration transaction UUID must not be nil".to_string()
            }
        );
        ensure!(
            self.from_schema < self.to_schema,
            InvalidFormatSnafu {
                message: format!(
                    "migration from_schema {} must be lower than to_schema {}",
                    self.from_schema, self.to_schema
                )
            }
        );
        ensure!(
            self.to_schema == STORAGE_SCHEMA_VERSION_V2,
            InvalidFormatSnafu {
                message: format!(
                    "migration to_schema {} must equal current schema {}",
                    self.to_schema, STORAGE_SCHEMA_VERSION_V2
                )
            }
        );
        ensure!(
            self.current_instance < db_instance_num,
            InvalidFormatSnafu {
                message: format!(
                    "migration current_instance {} is outside db_instance_num {}",
                    self.current_instance, db_instance_num
                )
            }
        );
        let expected_source_schema = match self.source_profile {
            MigrationSourceProfile::BaseV1SixCf | MigrationSourceProfile::VectorSetV1SevenCf => 1,
        };
        ensure!(
            self.from_schema == expected_source_schema,
            InvalidFormatSnafu {
                message: format!(
                    "migration source_profile {:?} requires from_schema {}, got {}",
                    self.source_profile, expected_source_schema, self.from_schema
                )
            }
        );
        for (field, value) in [
            ("source_name", self.source_name.as_str()),
            ("shadow_name", self.shadow_name.as_str()),
            ("backup_name", self.backup_name.as_str()),
        ] {
            ensure!(
                is_relative_basename(value),
                InvalidFormatSnafu {
                    message: format!(
                        "migration {field} must be a single relative basename: {value:?}"
                    )
                }
            );
        }
        ensure!(
            self.source_name == "live",
            InvalidFormatSnafu {
                message: format!(
                    "migration source_name must use the reserved live basename, got {:?}",
                    self.source_name
                )
            }
        );
        ensure!(
            self.shadow_name != self.source_name
                && self.backup_name != self.source_name
                && self.shadow_name != self.backup_name,
            InvalidFormatSnafu {
                message: "migration source/shadow/backup basenames must be distinct".to_string()
            }
        );
        let root_manifest_temp = Path::new(ROOT_STORAGE_MANIFEST_FILE).with_extension("tmp");
        for (field, value) in [
            ("shadow_name", self.shadow_name.as_str()),
            ("backup_name", self.backup_name.as_str()),
        ] {
            ensure!(
                value != ROOT_STORAGE_MANIFEST_FILE
                    && value != STORAGE_MANIFEST_FILE
                    && value != root_manifest_temp.to_string_lossy()
                    && value.parse::<u32>().is_err(),
                InvalidFormatSnafu {
                    message: format!(
                        "migration {field} conflicts with a reserved storage basename: {value:?}"
                    )
                }
            );
        }
        Ok(())
    }
}

fn is_relative_basename(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && !value.contains('/')
        && !value.contains('\\')
        && !Path::new(value).is_absolute()
}

fn is_versioned_producer_identity(value: &str) -> bool {
    value
        .strip_prefix(PRODUCER_IDENTITY_PREFIX)
        .is_some_and(|version| {
            !version.is_empty()
                && version.split('.').all(|component| {
                    !component.is_empty() && component.bytes().all(|b| b.is_ascii_digit())
                })
        })
}

#[derive(Serialize)]
struct RootPayload<'a> {
    manifest_version: u32,
    manifest_id: Uuid,
    storage_schema_version: u32,
    db_instance_num: u32,
    slot_mapping_version: u32,
    slot_mapping_digest: &'a ManifestDigest,
    column_families: &'a [ColumnFamilySpec],
    snapshot_read_min_version: u32,
    snapshot_read_max_version: u32,
    snapshot_write_version: u32,
    migration: &'a Option<MigrationTransaction>,
    rollback_floor: u32,
    features_used: &'a [String],
    created_by: &'a str,
    last_migrated_by: &'a str,
}

/// Root authority for storage topology, schema, snapshot compatibility, and migration intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootStorageManifestV2 {
    manifest_version: u32,
    manifest_id: Uuid,
    storage_schema_version: u32,
    db_instance_num: u32,
    slot_mapping_version: u32,
    slot_mapping_digest: ManifestDigest,
    column_families: Vec<ColumnFamilySpec>,
    snapshot_read_min_version: u32,
    snapshot_read_max_version: u32,
    snapshot_write_version: u32,
    migration: Option<MigrationTransaction>,
    rollback_floor: u32,
    features_used: Vec<String>,
    created_by: String,
    last_migrated_by: String,
    digest: ManifestDigest,
}

impl RootStorageManifestV2 {
    pub fn new(
        manifest_id: Uuid,
        db_instance_num: u32,
        slot_mapping_version: u32,
        slot_mapping_digest: ManifestDigest,
        migration: Option<MigrationTransaction>,
    ) -> Result<Self> {
        let mut manifest = Self {
            manifest_version: ROOT_STORAGE_MANIFEST_VERSION,
            manifest_id,
            storage_schema_version: STORAGE_SCHEMA_VERSION_V2,
            db_instance_num,
            slot_mapping_version,
            slot_mapping_digest,
            column_families: CANONICAL_COLUMN_FAMILIES.to_vec(),
            snapshot_read_min_version: 1,
            snapshot_read_max_version: 2,
            snapshot_write_version: 2,
            migration,
            rollback_floor: 1,
            features_used: vec!["vector_set".to_string()],
            created_by: CURRENT_PRODUCER_IDENTITY.to_string(),
            last_migrated_by: CURRENT_PRODUCER_IDENTITY.to_string(),
            digest: ManifestDigest(String::new()),
        };
        manifest.validate_payload()?;
        manifest.digest = ManifestDigest::compute(&manifest.payload_bytes()?);
        Ok(manifest)
    }

    fn payload(&self) -> RootPayload<'_> {
        RootPayload {
            manifest_version: self.manifest_version,
            manifest_id: self.manifest_id,
            storage_schema_version: self.storage_schema_version,
            db_instance_num: self.db_instance_num,
            slot_mapping_version: self.slot_mapping_version,
            slot_mapping_digest: &self.slot_mapping_digest,
            column_families: &self.column_families,
            snapshot_read_min_version: self.snapshot_read_min_version,
            snapshot_read_max_version: self.snapshot_read_max_version,
            snapshot_write_version: self.snapshot_write_version,
            migration: &self.migration,
            rollback_floor: self.rollback_floor,
            features_used: &self.features_used,
            created_by: &self.created_by,
            last_migrated_by: &self.last_migrated_by,
        }
    }

    fn payload_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.payload()).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("failed to serialize root storage manifest payload: {error}"),
            }
            .build()
        })
    }

    fn normalize_column_family_indices(&mut self) -> Result<()> {
        ensure!(
            self.column_families.len() == CANONICAL_COLUMN_FAMILIES.len(),
            InvalidFormatSnafu {
                message: "root manifest column-family count mismatch".to_string()
            }
        );
        for (actual, expected) in self
            .column_families
            .iter_mut()
            .zip(CANONICAL_COLUMN_FAMILIES.iter())
        {
            actual.index = expected.index;
            actual.use_bloom_filter = expected.use_bloom_filter;
            actual.block_size = expected.block_size;
        }
        Ok(())
    }

    fn validate_payload(&self) -> Result<()> {
        ensure!(
            self.manifest_version == ROOT_STORAGE_MANIFEST_VERSION,
            InvalidFormatSnafu {
                message: format!(
                    "unsupported root storage manifest version {}",
                    self.manifest_version
                )
            }
        );
        ensure!(
            self.manifest_id != Uuid::nil(),
            InvalidFormatSnafu {
                message: "root manifest UUID must not be nil".to_string()
            }
        );
        ensure!(
            self.storage_schema_version == STORAGE_SCHEMA_VERSION_V2,
            InvalidFormatSnafu {
                message: format!(
                    "unsupported storage schema version {}",
                    self.storage_schema_version
                )
            }
        );
        ensure!(
            self.db_instance_num > 0,
            InvalidFormatSnafu {
                message: "db_instance_num must be greater than zero".to_string()
            }
        );
        ensure!(
            self.slot_mapping_version == SLOT_MAPPING_VERSION,
            InvalidFormatSnafu {
                message: format!(
                    "unsupported slot mapping version {}",
                    self.slot_mapping_version
                )
            }
        );
        self.slot_mapping_digest.validate()?;
        ensure!(
            self.column_families.as_slice() == CANONICAL_COLUMN_FAMILIES,
            InvalidFormatSnafu {
                message: "root manifest canonical column-family contract mismatch".to_string()
            }
        );
        ensure!(
            self.snapshot_read_min_version == 1
                && self.snapshot_read_max_version == 2
                && self.snapshot_write_version == 2,
            InvalidFormatSnafu {
                message: "root manifest snapshot compatibility contract mismatch".to_string()
            }
        );
        if let Some(migration) = &self.migration {
            migration.validate(self.db_instance_num)?;
        }
        ensure!(
            (ROLLBACK_FLOOR_MIN..=ROLLBACK_FLOOR_MAX).contains(&self.rollback_floor),
            InvalidFormatSnafu {
                message: format!(
                    "unsupported rollback_floor {}; supported range is {}..={}",
                    self.rollback_floor, ROLLBACK_FLOOR_MIN, ROLLBACK_FLOOR_MAX
                )
            }
        );
        ensure!(
            !self.features_used.is_empty()
                && self
                    .features_used
                    .windows(2)
                    .all(|pair| pair[0].as_bytes() < pair[1].as_bytes()),
            InvalidFormatSnafu {
                message:
                    "root manifest features_used must be non-empty and byte-order sorted and unique"
                        .to_string()
            }
        );
        for feature in &self.features_used {
            ensure!(
                KNOWN_FEATURES.contains(&feature.as_str()),
                InvalidFormatSnafu {
                    message: format!("unknown root manifest feature {feature:?}")
                }
            );
        }
        for (field, value) in [
            ("created_by", self.created_by.as_str()),
            ("last_migrated_by", self.last_migrated_by.as_str()),
        ] {
            ensure!(
                is_versioned_producer_identity(value),
                InvalidFormatSnafu {
                    message: format!(
                        "root manifest {field} must use {PRODUCER_IDENTITY_PREFIX}<numeric-version> syntax"
                    )
                }
            );
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        self.digest.validate()?;
        let expected = ManifestDigest::compute(&self.payload_bytes()?);
        ensure!(
            self.digest == expected,
            InvalidFormatSnafu {
                message: format!(
                    "root manifest digest mismatch: expected {}, got {}",
                    expected.as_str(),
                    self.digest.as_str()
                )
            }
        );
        Ok(())
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("failed to serialize root storage manifest: {error}"),
            }
            .build()
        })
    }

    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        let mut manifest: Self = serde_json::from_slice(bytes).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("invalid root storage manifest JSON: {error}"),
            }
            .build()
        })?;
        manifest.normalize_column_family_indices()?;
        manifest.validate()?;
        ensure!(
            serde_json::to_vec(&manifest).map_err(|error| {
                InvalidFormatSnafu {
                    message: format!("failed to canonicalize root storage manifest: {error}"),
                }
                .build()
            })? == bytes,
            InvalidFormatSnafu {
                message: "root storage manifest is not compact fixed-order JSON".to_string()
            }
        );
        Ok(manifest)
    }

    pub fn write_to_dir_atomically(&self, dir: &Path) -> Result<()> {
        write_atomically(
            &dir.join(ROOT_STORAGE_MANIFEST_FILE),
            &self.to_json_bytes()?,
            false,
        )
    }

    pub fn read_from_dir(dir: &Path) -> Result<Self> {
        let path = dir.join(ROOT_STORAGE_MANIFEST_FILE);
        let bytes = fs::read(&path).context(IoSnafu)?;
        Self::from_json_bytes(&bytes).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("invalid root storage manifest {}: {error}", path.display()),
            }
            .build()
        })
    }

    pub fn manifest_id(&self) -> Uuid {
        self.manifest_id
    }

    pub fn manifest_digest(&self) -> &ManifestDigest {
        &self.digest
    }

    pub fn db_instance_num(&self) -> u32 {
        self.db_instance_num
    }

    pub fn slot_mapping_version(&self) -> u32 {
        self.slot_mapping_version
    }

    pub fn slot_mapping_digest(&self) -> &ManifestDigest {
        &self.slot_mapping_digest
    }

    pub fn column_families(&self) -> &[ColumnFamilySpec] {
        &self.column_families
    }

    pub fn migration(&self) -> Option<&MigrationTransaction> {
        self.migration.as_ref()
    }

    pub(crate) fn set_migration(&mut self, migration: Option<MigrationTransaction>) -> Result<()> {
        self.migration = migration;
        self.validate_payload()?;
        self.digest = ManifestDigest::compute(&self.payload_bytes()?);
        Ok(())
    }

    pub fn validate_runtime_topology(&self, db_instance_num: usize) -> Result<()> {
        ensure!(
            self.db_instance_num == db_instance_num as u32,
            InvalidFormatSnafu {
                message: format!(
                    "root manifest db_instance_num {} does not match configured {}",
                    self.db_instance_num, db_instance_num
                )
            }
        );
        let expected = slot_mapping_digest(db_instance_num);
        ensure!(
            self.slot_mapping_digest == expected,
            InvalidFormatSnafu {
                message: format!(
                    "root manifest slot mapping digest mismatch: expected {}, got {}",
                    expected.as_str(),
                    self.slot_mapping_digest.as_str()
                )
            }
        );
        Ok(())
    }
}

#[derive(Serialize)]
struct InstancePayload<'a> {
    manifest_version: u32,
    instance_id: u32,
    instance_uuid: Uuid,
    root_manifest_id: Uuid,
    root_manifest_digest: &'a ManifestDigest,
    storage_incarnation: u64,
    next_generation: u64,
}

/// Per-RocksDB-instance durable identity and generation allocator state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceStorageManifestV2 {
    manifest_version: u32,
    instance_id: u32,
    instance_uuid: Uuid,
    root_manifest_id: Uuid,
    root_manifest_digest: ManifestDigest,
    storage_incarnation: u64,
    next_generation: u64,
    digest: ManifestDigest,
}

impl InstanceStorageManifestV2 {
    pub fn new(
        instance_id: u32,
        instance_uuid: Uuid,
        root: &RootStorageManifestV2,
        storage_incarnation: u64,
        next_generation: u64,
    ) -> Result<Self> {
        let mut manifest = Self {
            manifest_version: INSTANCE_STORAGE_MANIFEST_VERSION,
            instance_id,
            instance_uuid,
            root_manifest_id: root.manifest_id,
            root_manifest_digest: root.digest.clone(),
            storage_incarnation,
            next_generation,
            digest: ManifestDigest(String::new()),
        };
        manifest.validate_payload()?;
        manifest.digest = ManifestDigest::compute(&manifest.payload_bytes()?);
        Ok(manifest)
    }

    fn payload(&self) -> InstancePayload<'_> {
        InstancePayload {
            manifest_version: self.manifest_version,
            instance_id: self.instance_id,
            instance_uuid: self.instance_uuid,
            root_manifest_id: self.root_manifest_id,
            root_manifest_digest: &self.root_manifest_digest,
            storage_incarnation: self.storage_incarnation,
            next_generation: self.next_generation,
        }
    }

    fn payload_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.payload()).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("failed to serialize instance storage manifest payload: {error}"),
            }
            .build()
        })
    }

    fn validate_payload(&self) -> Result<()> {
        ensure!(
            self.manifest_version == INSTANCE_STORAGE_MANIFEST_VERSION,
            InvalidFormatSnafu {
                message: format!(
                    "unsupported instance storage manifest version {}",
                    self.manifest_version
                )
            }
        );
        ensure!(
            self.instance_uuid != Uuid::nil(),
            InvalidFormatSnafu {
                message: "instance_uuid must not be nil".to_string()
            }
        );
        ensure!(
            self.root_manifest_id != Uuid::nil(),
            InvalidFormatSnafu {
                message: "root manifest UUID must not be nil".to_string()
            }
        );
        self.root_manifest_digest.validate()?;
        ensure!(
            self.storage_incarnation != 0 && self.next_generation >= FIRST_GENERATION,
            InvalidFormatSnafu {
                message: format!(
                    "invalid instance manifest incarnation {} or next generation {}",
                    self.storage_incarnation, self.next_generation
                )
            }
        );
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        self.validate_payload()?;
        self.digest.validate()?;
        let expected = ManifestDigest::compute(&self.payload_bytes()?);
        ensure!(
            self.digest == expected,
            InvalidFormatSnafu {
                message: format!(
                    "instance manifest digest mismatch: expected {}, got {}",
                    expected.as_str(),
                    self.digest.as_str()
                )
            }
        );
        Ok(())
    }

    pub fn validate_binding(
        &self,
        expected_instance_id: u32,
        expected_instance_uuid: Uuid,
        root: &RootStorageManifestV2,
    ) -> Result<()> {
        self.validate_root_binding(expected_instance_id, root)?;
        ensure!(
            self.instance_uuid == expected_instance_uuid,
            InvalidFormatSnafu {
                message: format!(
                    "instance_uuid {} does not match expected {}",
                    self.instance_uuid, expected_instance_uuid
                )
            }
        );
        Ok(())
    }

    pub fn validate_root_binding(
        &self,
        expected_instance_id: u32,
        root: &RootStorageManifestV2,
    ) -> Result<()> {
        self.validate()?;
        ensure!(
            self.instance_id == expected_instance_id,
            InvalidFormatSnafu {
                message: format!(
                    "instance_id {} does not match expected {}",
                    self.instance_id, expected_instance_id
                )
            }
        );
        ensure!(
            self.root_manifest_id == root.manifest_id && self.root_manifest_digest == root.digest,
            InvalidFormatSnafu {
                message: "instance root manifest identity or digest mismatch".to_string()
            }
        );
        Ok(())
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("failed to serialize instance storage manifest: {error}"),
            }
            .build()
        })
    }

    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("invalid instance storage manifest UUID/JSON: {error}"),
            }
            .build()
        })?;
        manifest.validate()?;
        ensure!(
            serde_json::to_vec(&manifest).map_err(|error| {
                InvalidFormatSnafu {
                    message: format!("failed to canonicalize instance storage manifest: {error}"),
                }
                .build()
            })? == bytes,
            InvalidFormatSnafu {
                message: "instance storage manifest is not compact fixed-order JSON".to_string()
            }
        );
        Ok(manifest)
    }

    pub fn write_to_dir_atomically(&self, dir: &Path) -> Result<()> {
        write_atomically(
            &dir.join(STORAGE_MANIFEST_FILE),
            &self.to_json_bytes()?,
            true,
        )
    }

    pub fn read_from_dir(dir: &Path) -> Result<Self> {
        let path = dir.join(STORAGE_MANIFEST_FILE);
        let bytes = fs::read(&path).context(IoSnafu)?;
        Self::from_json_bytes(&bytes).map_err(|error| {
            InvalidFormatSnafu {
                message: format!(
                    "invalid instance storage manifest {}: {error}",
                    path.display()
                ),
            }
            .build()
        })
    }

    pub fn instance_id(&self) -> u32 {
        self.instance_id
    }

    pub fn instance_uuid(&self) -> Uuid {
        self.instance_uuid
    }

    pub fn root_manifest_id(&self) -> Uuid {
        self.root_manifest_id
    }

    pub fn root_manifest_digest(&self) -> &ManifestDigest {
        &self.root_manifest_digest
    }

    pub fn storage_incarnation(&self) -> u64 {
        self.storage_incarnation
    }

    pub fn next_generation(&self) -> u64 {
        self.next_generation
    }

    pub fn manifest_digest(&self) -> &ManifestDigest {
        &self.digest
    }

    pub(crate) fn rebind_root(&mut self, root: &RootStorageManifestV2) -> Result<()> {
        self.root_manifest_id = root.manifest_id;
        self.root_manifest_digest = root.digest.clone();
        self.validate_payload()?;
        self.digest = ManifestDigest::compute(&self.payload_bytes()?);
        Ok(())
    }
}

/// Stable digest for the modulo slot-mapping algorithm and configured instance count.
pub fn slot_mapping_digest(db_instance_num: usize) -> ManifestDigest {
    ManifestDigest::compute(format!("kiwi-slot-mapping/modulo/v1/{db_instance_num}").as_bytes())
}

pub(crate) fn load_or_create_root_manifest(
    root: &Path,
    db_instance_num: usize,
) -> Result<RootManifestLoad> {
    let path = root.join(ROOT_STORAGE_MANIFEST_FILE);
    if path.exists() {
        let manifest = RootStorageManifestV2::read_from_dir(root)?;
        manifest.validate_runtime_topology(db_instance_num)?;
        return Ok(RootManifestLoad {
            manifest,
            created_this_call: false,
        });
    }

    if root.exists() {
        let root_temp_path = path.with_extension("tmp");
        let mut entries = fs::read_dir(root).context(IoSnafu)?;
        ensure!(
            entries.all(|entry| entry
                .map(|entry| entry.path() == root_temp_path)
                .unwrap_or(false)),
            InvalidFormatSnafu {
                message: format!(
                    "storage root {} is non-empty but has no root manifest; staged migration is required",
                    root.display()
                )
            }
        );
    } else {
        fs::create_dir_all(root).context(IoSnafu)?;
    }

    let manifest = RootStorageManifestV2::new(
        Uuid::new_v4(),
        db_instance_num as u32,
        SLOT_MAPPING_VERSION,
        slot_mapping_digest(db_instance_num),
        None,
    )?;
    manifest.write_to_dir_atomically(root)?;
    Ok(RootManifestLoad {
        manifest,
        created_this_call: true,
    })
}

pub(crate) struct RootManifestLoad {
    pub(crate) manifest: RootStorageManifestV2,
    pub(crate) created_this_call: bool,
}

pub(crate) fn validate_existing_instance_manifests(
    root_dir: &Path,
    root: &RootStorageManifestV2,
    root_created_this_call: bool,
) -> Result<()> {
    let root_manifest_path = root_dir.join(ROOT_STORAGE_MANIFEST_FILE);
    let root_temp_path = root_manifest_path.with_extension("tmp");
    for entry in fs::read_dir(root_dir).context(IoSnafu)? {
        let entry = entry.context(IoSnafu)?;
        let path = entry.path();
        if path == root_manifest_path || path == root_temp_path {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!(
                    "storage root contains a non-UTF-8 entry: {}",
                    path.display()
                ),
            }
            .build()
        })?;
        if root.migration().is_some_and(|transaction| {
            name == transaction.shadow_name || name == transaction.backup_name
        }) {
            ensure!(
                entry.file_type().context(IoSnafu)?.is_dir(),
                InvalidFormatSnafu {
                    message: format!(
                        "storage migration artifact {} is not a directory",
                        path.display()
                    )
                }
            );
            continue;
        }
        let instance_id = name.parse::<u32>().map_err(|_| {
            InvalidFormatSnafu {
                message: format!("storage root contains unknown entry {}", path.display()),
            }
            .build()
        })?;
        ensure!(
            name == instance_id.to_string()
                && instance_id < root.db_instance_num
                && entry.file_type().context(IoSnafu)?.is_dir(),
            InvalidFormatSnafu {
                message: format!(
                    "storage root contains out-of-range or non-directory instance entry {}",
                    path.display()
                )
            }
        );
    }

    for instance_id in 0..root.db_instance_num {
        let instance_dir = root_dir.join(instance_id.to_string());
        if !instance_dir.exists() {
            ensure!(
                root_created_this_call,
                InvalidFormatSnafu {
                    message: format!(
                        "storage instance directory {} is missing under an existing root manifest",
                        instance_dir.display()
                    )
                }
            );
            continue;
        }
        ensure!(
            instance_dir.is_dir(),
            InvalidFormatSnafu {
                message: format!(
                    "storage instance {} is not a directory",
                    instance_dir.display()
                )
            }
        );
        let mut entries = fs::read_dir(&instance_dir).context(IoSnafu)?;
        if entries.next().is_none() {
            ensure!(
                root_created_this_call,
                InvalidFormatSnafu {
                    message: format!(
                        "storage instance manifest {} is missing under an existing root manifest",
                        instance_dir.join(STORAGE_MANIFEST_FILE).display()
                    )
                }
            );
            continue;
        }
        ensure!(
            instance_dir.join(STORAGE_MANIFEST_FILE).exists(),
            InvalidFormatSnafu {
                message: format!(
                    "storage instance {} is non-empty but has no instance manifest; staged migration is required",
                    instance_dir.display()
                )
            }
        );
        InstanceStorageManifestV2::read_from_dir(&instance_dir)?
            .validate_root_binding(instance_id, root)?;
    }
    Ok(())
}

pub(crate) struct StorageManifest {
    path: PathBuf,
    manifest: Mutex<InstanceStorageManifestV2>,
}

impl StorageManifest {
    pub(crate) fn open_bound(
        db_dir: &Path,
        instance_id: u32,
        root: &RootStorageManifestV2,
        vector_data_has_entries: bool,
        allow_manifest_creation: bool,
    ) -> Result<Self> {
        let path = db_dir.join(STORAGE_MANIFEST_FILE);
        let manifest = if path.exists() {
            let manifest = InstanceStorageManifestV2::read_from_dir(db_dir)?;
            manifest.validate_root_binding(instance_id, root)?;
            manifest
        } else {
            ensure!(
                allow_manifest_creation,
                InvalidFormatSnafu {
                    message: format!(
                        "storage manifest {} is missing under an existing root manifest",
                        path.display()
                    )
                }
            );
            ensure!(
                !vector_data_has_entries,
                InvalidFormatSnafu {
                    message: format!(
                        "storage manifest {} is missing but vector data is present",
                        path.display()
                    )
                }
            );
            let manifest = InstanceStorageManifestV2::new(
                instance_id,
                Uuid::new_v4(),
                root,
                rand::thread_rng().r#gen::<u64>().max(1),
                FIRST_GENERATION,
            )?;
            manifest.write_to_dir_atomically(db_dir)?;
            manifest
        };
        Ok(Self {
            path,
            manifest: Mutex::new(manifest),
        })
    }

    pub(crate) fn storage_incarnation(&self) -> u64 {
        match self.manifest.lock() {
            Ok(manifest) => manifest.storage_incarnation,
            Err(poisoned) => poisoned.into_inner().storage_incarnation,
        }
    }

    pub(crate) fn load_storage_incarnation(db_dir: &Path) -> Result<u64> {
        Ok(InstanceStorageManifestV2::read_from_dir(db_dir)?.storage_incarnation)
    }

    pub(crate) fn allocate_generation(&self) -> Result<u64> {
        let mut manifest = self.manifest.lock().map_err(|_| {
            InvalidFormatSnafu {
                message: "storage manifest mutex is poisoned".to_string(),
            }
            .build()
        })?;
        let generation = manifest.next_generation;
        let successor = generation.checked_add(1).ok_or_else(|| {
            InvalidFormatSnafu {
                message: "generation sequence exhausted".to_string(),
            }
            .build()
        })?;
        let mut candidate = manifest.clone();
        candidate.next_generation = successor;
        candidate.digest = ManifestDigest::compute(&candidate.payload_bytes()?);
        write_atomically(&self.path, &candidate.to_json_bytes()?, true)?;
        *manifest = candidate;
        Ok(generation)
    }

    pub(crate) fn copy_to(&self, dir: &Path) -> Result<()> {
        let target = dir.join(STORAGE_MANIFEST_FILE);
        let mut last_error = None;
        for attempt in 0..5 {
            match (|| -> std::io::Result<()> {
                fs::copy(&self.path, &target)?;
                OpenOptions::new().write(true).open(&target)?.sync_all()?;
                Ok(())
            })() {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 4 {
                        let _ = fs::remove_file(&target);
                        std::thread::sleep(std::time::Duration::from_millis(100 * (attempt + 1)));
                    }
                }
            }
        }
        let error = last_error.ok_or_else(|| {
            InvalidFormatSnafu {
                message: "manifest copy exhausted retries without recording an I/O error"
                    .to_string(),
            }
            .build()
        })?;
        Err(error).context(IoSnafu)
    }
}

fn write_atomically(path: &Path, bytes: &[u8], use_instance_failpoint: bool) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        InvalidFormatSnafu {
            message: format!("manifest path {} has no parent", path.display()),
        }
        .build()
    })?;
    fs::create_dir_all(parent).context(IoSnafu)?;
    let temp_path = path.with_extension("tmp");
    {
        let mut temp = fs::File::create(&temp_path).context(IoSnafu)?;
        temp.write_all(bytes).context(IoSnafu)?;
        temp.sync_all().context(IoSnafu)?;
    }
    #[cfg(any(test, feature = "test-fault-injection"))]
    if use_instance_failpoint && STORAGE_MANIFEST_PERSIST_FAILURES.lock().remove(parent) {
        return Err(std::io::Error::other(
            "injected storage manifest persist failure",
        ))
        .context(IoSnafu);
    }
    #[cfg(not(any(test, feature = "test-fault-injection")))]
    let _ = use_instance_failpoint;
    replace_file_atomically(&temp_path, path).context(IoSnafu)?;
    sync_parent_directory(path).context(IoSnafu)?;
    Ok(())
}

#[cfg(not(windows))]
fn replace_file_atomically(temp_path: &Path, target_path: &Path) -> std::io::Result<()> {
    fs::rename(temp_path, target_path)
}

#[cfg(windows)]
fn replace_file_atomically(temp_path: &Path, target_path: &Path) -> std::io::Result<()> {
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
    // SAFETY: both paths are encoded as owned, NUL-terminated UTF-16 buffers that remain alive
    // for the call. Flags request an atomic same-volume replacement and durable write-through.
    let result = unsafe {
        MoveFileExW(
            temp_wide.as_ptr(),
            target_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn root(dir: &Path) -> RootStorageManifestV2 {
        load_or_create_root_manifest(dir, 1)
            .expect("root manifest")
            .manifest
    }

    fn refresh_root_digest(manifest: &mut RootStorageManifestV2) {
        manifest.digest = ManifestDigest::compute(&manifest.payload_bytes().expect("root payload"));
    }

    fn assert_root_contract_rejected(mut manifest: RootStorageManifestV2, expected: &str) {
        refresh_root_digest(&mut manifest);
        let error = manifest
            .validate()
            .expect_err("mutated root contract must fail");
        assert!(
            error.to_string().contains(expected),
            "unexpected error for {expected:?}: {error}"
        );
    }

    #[test]
    fn manifest_is_created_for_empty_db_and_survives_reopen() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = root(dir.path());
        let instance_dir = dir.path().join("0");
        let created = StorageManifest::open_bound(&instance_dir, 0, &root, false, true)
            .expect("create manifest");
        let incarnation = created.storage_incarnation();
        assert_ne!(incarnation, 0);
        assert!(instance_dir.join(STORAGE_MANIFEST_FILE).exists());

        let reopened = StorageManifest::open_bound(&instance_dir, 0, &root, false, false)
            .expect("reopen manifest");
        assert_eq!(reopened.storage_incarnation(), incarnation);
    }

    #[test]
    fn allocations_are_monotonic_and_persisted() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = root(dir.path());
        let instance_dir = dir.path().join("0");
        let manifest = StorageManifest::open_bound(&instance_dir, 0, &root, false, true)
            .expect("create manifest");
        let first = manifest.allocate_generation().expect("allocate first");
        let second = manifest.allocate_generation().expect("allocate second");
        assert_eq!(first, FIRST_GENERATION);
        assert_eq!(second, FIRST_GENERATION + 1);

        let reopened = StorageManifest::open_bound(&instance_dir, 0, &root, false, false)
            .expect("reopen manifest");
        let third = reopened
            .allocate_generation()
            .expect("allocate after reopen");
        assert!(third > second);
    }

    #[test]
    fn failed_generation_persist_does_not_advance_in_memory_state() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = root(dir.path());
        let instance_dir = dir.path().join("0");
        let manifest = StorageManifest::open_bound(&instance_dir, 0, &root, false, true)
            .expect("create manifest");
        let _guard = fail_next_storage_manifest_persist(&instance_dir);

        assert!(manifest.allocate_generation().is_err());
        assert_eq!(
            manifest.allocate_generation().expect("retry allocation"),
            FIRST_GENERATION
        );
    }

    #[test]
    fn root_creation_retries_when_only_its_temp_file_remains() {
        let dir = tempfile::tempdir().expect("temp dir");
        fs::write(
            dir.path()
                .join(ROOT_STORAGE_MANIFEST_FILE)
                .with_extension("tmp"),
            b"interrupted root manifest",
        )
        .expect("write interrupted temp file");

        let manifest = load_or_create_root_manifest(dir.path(), 1)
            .expect("retry root create")
            .manifest;
        manifest
            .validate_runtime_topology(1)
            .expect("valid topology");
        assert!(dir.path().join(ROOT_STORAGE_MANIFEST_FILE).is_file());
    }

    #[test]
    fn root_preflight_rejects_orphan_or_unknown_entries() {
        let orphan_root = tempfile::tempdir().expect("temp dir");
        let orphan_manifest = root(orphan_root.path());
        fs::create_dir(orphan_root.path().join("1")).expect("orphan instance dir");
        assert!(
            validate_existing_instance_manifests(orphan_root.path(), &orphan_manifest, true)
                .is_err()
        );

        let unknown_root = tempfile::tempdir().expect("temp dir");
        let unknown_manifest = root(unknown_root.path());
        fs::create_dir(unknown_root.path().join("00")).expect("non-canonical instance basename");
        assert!(
            validate_existing_instance_manifests(unknown_root.path(), &unknown_manifest, true)
                .is_err()
        );
    }

    #[test]
    fn missing_manifest_on_non_empty_vector_db_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = root(dir.path());
        assert!(StorageManifest::open_bound(&dir.path().join("0"), 0, &root, true, true).is_err());
    }

    #[test]
    fn corrupt_manifest_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = root(dir.path());
        let instance_dir = dir.path().join("0");
        fs::create_dir_all(&instance_dir).expect("instance dir");
        fs::write(instance_dir.join(STORAGE_MANIFEST_FILE), b"not json").expect("write");
        assert!(StorageManifest::open_bound(&instance_dir, 0, &root, false, false).is_err());
    }

    #[test]
    fn root_static_fields_reject_unsupported_or_noncanonical_values_with_fresh_digest() {
        let dir = tempfile::tempdir().expect("temp dir");
        let base = root(dir.path());

        let mut rollback = base.clone();
        rollback.rollback_floor = 0;
        assert_root_contract_rejected(rollback, "rollback_floor");

        let mut future_rollback = base.clone();
        future_rollback.rollback_floor = 2;
        assert_root_contract_rejected(future_rollback, "rollback_floor");

        let mut unknown_feature = base.clone();
        unknown_feature.features_used = vec!["unknown".to_string()];
        assert_root_contract_rejected(unknown_feature, "feature");

        let mut empty_features = base.clone();
        empty_features.features_used.clear();
        assert_root_contract_rejected(empty_features, "features_used");

        let mut duplicate_feature = base.clone();
        duplicate_feature.features_used = vec!["vector_set".to_string(), "vector_set".to_string()];
        assert_root_contract_rejected(duplicate_feature, "sorted and unique");

        let mut unsorted_feature = base.clone();
        unsorted_feature.features_used = vec!["vector_set".to_string(), "base".to_string()];
        assert_root_contract_rejected(unsorted_feature, "sorted and unique");

        let mut unversioned_creator = base.clone();
        unversioned_creator.created_by = "kiwi".to_string();
        assert_root_contract_rejected(unversioned_creator, "created_by");

        let mut empty_migrator = base;
        empty_migrator.last_migrated_by.clear();
        assert_root_contract_rejected(empty_migrator, "last_migrated_by");

        let mut compatible_patch_identity = root(dir.path());
        compatible_patch_identity.created_by = "kiwi-storage/v2.1.7".to_string();
        compatible_patch_identity.last_migrated_by = "kiwi-storage/v2.9".to_string();
        refresh_root_digest(&mut compatible_patch_identity);
        assert!(compatible_patch_identity.validate().is_ok());
    }

    #[test]
    fn root_rejects_reordered_or_duplicated_cf_registry_with_fresh_digest() {
        let dir = tempfile::tempdir().expect("temp dir");
        let base = root(dir.path());

        let mut reordered = base.clone();
        reordered.column_families.swap(0, 1);
        assert_root_contract_rejected(reordered, "canonical column-family");

        let mut duplicated = base;
        duplicated.column_families[1] = duplicated.column_families[0];
        assert_root_contract_rejected(duplicated, "canonical column-family");
    }

    #[test]
    fn migration_contract_rejects_invalid_versions_instance_or_source_profile_with_fresh_digest() {
        let dir = tempfile::tempdir().expect("temp dir");
        let base = root(dir.path());
        for source_profile in [
            MigrationSourceProfile::BaseV1SixCf,
            MigrationSourceProfile::VectorSetV1SevenCf,
        ] {
            let migration = MigrationTransaction::new(
                Uuid::new_v4(),
                1,
                STORAGE_SCHEMA_VERSION_V2,
                source_profile,
                MigrationPhase::SourceDetected,
                0,
                "live",
                ".0.shadow",
                ".0.backup",
            );
            let manifest = RootStorageManifestV2::new(
                Uuid::new_v4(),
                1,
                SLOT_MAPPING_VERSION,
                slot_mapping_digest(1),
                Some(migration),
            )
            .expect("valid migration contract");
            assert!(manifest.validate().is_ok());
        }

        let valid = MigrationTransaction::new(
            Uuid::new_v4(),
            1,
            STORAGE_SCHEMA_VERSION_V2,
            MigrationSourceProfile::BaseV1SixCf,
            MigrationPhase::SourceDetected,
            0,
            "live",
            ".0.shadow",
            ".0.backup",
        );
        let mut profile_mismatch = base.clone();
        let migration = profile_mismatch
            .migration
            .get_or_insert_with(|| valid.clone());
        migration.from_schema = 0;
        assert_root_contract_rejected(profile_mismatch, "source_profile");

        for (field, value) in [
            ("from_schema", 2),
            ("to_schema", 3),
            ("current_instance", 1),
        ] {
            let mut manifest = base.clone();
            let migration = manifest.migration.get_or_insert_with(|| valid.clone());
            match field {
                "from_schema" => migration.from_schema = value,
                "to_schema" => migration.to_schema = value,
                "current_instance" => migration.current_instance = value,
                _ => unreachable!(),
            }
            assert_root_contract_rejected(manifest, field);
        }
    }
}
