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

#![allow(clippy::unwrap_used)]

use std::fs;
use std::sync::Arc;

use storage::checkpoint::expected_column_families;
use storage::logindex::cf_metadata;
use storage::storage::Storage;
use storage::{
    CANONICAL_COLUMN_FAMILIES, ColumnFamilyIndex, ColumnFamilyRole, ComparatorId,
    InstanceStorageManifestV2, ManifestDigest, MigrationPhase, MigrationSourceProfile,
    MigrationTransaction, ROOT_STORAGE_MANIFEST_FILE, RootStorageManifestV2, STORAGE_MANIFEST_FILE,
    StorageOptions, canonical_column_family_names,
};
use tempfile::tempdir;

const ROOT_MANIFEST_ID: &str = "e2dcd3ab-e4d9-4d92-9713-c5f39b7ad651";
const OTHER_ROOT_MANIFEST_ID: &str = "6be0d4c3-5963-4b86-8da6-5e36d17b21a1";
const MIGRATION_TRANSACTION_ID: &str = "42348e18-3f8e-4a7a-b65f-815597141ee4";
const INSTANCE_UUIDS: [&str; 3] = [
    "18ffad3f-f3c1-48d9-a605-a42981f3e45b",
    "2d8ef7a6-006c-452d-a36e-9ceccb70f66c",
    "b39c0846-a569-4666-9a3d-d4c0fbd35c98",
];

fn root_manifest(db_instance_num: u32) -> RootStorageManifestV2 {
    RootStorageManifestV2::new(
        ROOT_MANIFEST_ID.parse().unwrap(),
        db_instance_num,
        1,
        ManifestDigest::compute(format!("slot-modulo-v1:{db_instance_num}").as_bytes()),
        None,
    )
    .unwrap()
}

fn instance_manifest(instance_id: u32, root: &RootStorageManifestV2) -> InstanceStorageManifestV2 {
    InstanceStorageManifestV2::new(
        instance_id,
        INSTANCE_UUIDS[instance_id as usize].parse().unwrap(),
        root,
        41 + u64::from(instance_id),
        1,
    )
    .unwrap()
}

fn replace_once(bytes: &[u8], from: &str, to: &str) -> Vec<u8> {
    let text = std::str::from_utf8(bytes).unwrap();
    assert_eq!(
        text.matches(from).count(),
        1,
        "fixture replacement must be unique"
    );
    text.replacen(from, to, 1).into_bytes()
}

#[test]
fn root_manifest_roundtrips_topology_and_canonical_cf_contract() {
    let temp = tempdir().unwrap();
    let expected = root_manifest(3);

    expected.write_to_dir_atomically(temp.path()).unwrap();

    let path = temp.path().join(ROOT_STORAGE_MANIFEST_FILE);
    let encoded = fs::read(&path).unwrap();
    assert!(
        !encoded.contains(&b'\n'),
        "manifest JSON must remain compact"
    );
    assert_eq!(encoded, expected.to_json_bytes().unwrap());

    let actual = RootStorageManifestV2::read_from_dir(temp.path()).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.manifest_id().to_string(), ROOT_MANIFEST_ID);
    assert_eq!(actual.db_instance_num(), 3);
    assert_eq!(actual.slot_mapping_version(), 1);
    assert_eq!(
        actual.column_families(),
        CANONICAL_COLUMN_FAMILIES,
        "the root manifest must persist the complete canonical CF contract"
    );
    assert_eq!(
        actual.manifest_digest(),
        &ManifestDigest::compute_payload(&encoded).unwrap()
    );
}

#[test]
fn instance_manifest_binds_instance_to_root_manifest_digest() {
    let temp = tempdir().unwrap();
    let root = root_manifest(2);
    let expected = instance_manifest(1, &root);

    expected.write_to_dir_atomically(temp.path()).unwrap();

    assert!(temp.path().join(STORAGE_MANIFEST_FILE).is_file());
    let actual = InstanceStorageManifestV2::read_from_dir(temp.path()).unwrap();
    actual
        .validate_binding(1, INSTANCE_UUIDS[1].parse().unwrap(), &root)
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.instance_id(), 1);
    assert_eq!(actual.instance_uuid().to_string(), INSTANCE_UUIDS[1]);
    assert_eq!(actual.root_manifest_id(), root.manifest_id());
    assert_eq!(actual.root_manifest_digest(), root.manifest_digest());
    let encoded = fs::read(temp.path().join(STORAGE_MANIFEST_FILE)).unwrap();
    assert_eq!(
        &ManifestDigest::compute_payload(&encoded).unwrap(),
        actual.manifest_digest(),
        "the instance digest must cover the exact canonical payload bytes"
    );
}

#[test]
fn root_manifest_rejects_unknown_version_corrupt_digest_and_noncanonical_encoding() {
    let root = root_manifest(1);
    let encoded = root.to_json_bytes().unwrap();

    let unknown_version = replace_once(
        &encoded,
        "\"manifest_version\":2",
        "\"manifest_version\":99",
    );
    let version_error = RootStorageManifestV2::from_json_bytes(&unknown_version).unwrap_err();
    assert!(
        version_error.to_string().contains("version 99"),
        "unexpected error: {version_error}"
    );

    let digest = root.manifest_digest().as_str();
    let replacement = match digest.strip_prefix('0') {
        Some(suffix) => format!("1{suffix}"),
        None => {
            let (_, suffix) = digest.split_at(1);
            format!("0{suffix}")
        }
    };
    let corrupt_digest = replace_once(&encoded, digest, &replacement);
    let digest_error = RootStorageManifestV2::from_json_bytes(&corrupt_digest).unwrap_err();
    assert!(
        digest_error.to_string().contains("digest"),
        "unexpected error: {digest_error}"
    );

    let mut non_canonical = encoded.clone();
    non_canonical.insert(1, b' ');
    let canonical_error = RootStorageManifestV2::from_json_bytes(&non_canonical).unwrap_err();
    assert!(
        canonical_error.to_string().contains("compact fixed-order"),
        "unexpected error: {canonical_error}"
    );

    let unknown_field = replace_once(&encoded, ",\"digest\":", ",\"unknown\":1,\"digest\":");
    let unknown_error = RootStorageManifestV2::from_json_bytes(&unknown_field).unwrap_err();
    assert!(
        unknown_error.to_string().contains("unknown field"),
        "unexpected error: {unknown_error}"
    );

    let canonical_prefix =
        format!("{{\"manifest_version\":2,\"manifest_id\":\"{ROOT_MANIFEST_ID}\"");
    let swapped_prefix = format!("{{\"manifest_id\":\"{ROOT_MANIFEST_ID}\",\"manifest_version\":2");
    let swapped = replace_once(&encoded, &canonical_prefix, &swapped_prefix);
    let swapped_error = RootStorageManifestV2::from_json_bytes(&swapped).unwrap_err();
    assert!(
        swapped_error.to_string().contains("compact fixed-order"),
        "unexpected error: {swapped_error}"
    );
}

#[test]
fn instance_manifest_rejects_wrong_instance_id_or_root_digest() {
    let root = root_manifest(2);
    let instance = instance_manifest(1, &root);

    let id_error = instance
        .validate_binding(0, INSTANCE_UUIDS[1].parse().unwrap(), &root)
        .unwrap_err();
    assert!(
        id_error.to_string().contains("instance_id"),
        "unexpected error: {id_error}"
    );

    let uuid_error = instance
        .validate_binding(1, INSTANCE_UUIDS[2].parse().unwrap(), &root)
        .unwrap_err();
    assert!(
        uuid_error.to_string().contains("instance_uuid"),
        "unexpected error: {uuid_error}"
    );

    let encoded = instance.to_json_bytes().unwrap();
    let malformed_uuid = replace_once(&encoded, INSTANCE_UUIDS[1], "not-a-uuid");
    let malformed_error = InstanceStorageManifestV2::from_json_bytes(&malformed_uuid).unwrap_err();
    assert!(
        malformed_error.to_string().contains("UUID"),
        "unexpected error: {malformed_error}"
    );

    let other_root = RootStorageManifestV2::new(
        OTHER_ROOT_MANIFEST_ID.parse().unwrap(),
        2,
        1,
        ManifestDigest::compute(b"slot-modulo-v1:2"),
        None,
    )
    .unwrap();
    let digest_error = instance
        .validate_binding(1, INSTANCE_UUIDS[1].parse().unwrap(), &other_root)
        .unwrap_err();
    assert!(
        digest_error.to_string().contains("root manifest"),
        "unexpected error: {digest_error}"
    );

    let mut non_canonical = encoded.clone();
    non_canonical.insert(1, b' ');
    let whitespace_error = InstanceStorageManifestV2::from_json_bytes(&non_canonical).unwrap_err();
    assert!(
        whitespace_error.to_string().contains("compact fixed-order"),
        "unexpected error: {whitespace_error}"
    );

    let unknown_field = replace_once(&encoded, ",\"digest\":", ",\"unknown\":1,\"digest\":");
    let unknown_error = InstanceStorageManifestV2::from_json_bytes(&unknown_field).unwrap_err();
    assert!(
        unknown_error.to_string().contains("unknown field"),
        "unexpected error: {unknown_error}"
    );

    let canonical_prefix = "{\"manifest_version\":2,\"instance_id\":1";
    let swapped_prefix = "{\"instance_id\":1,\"manifest_version\":2";
    let swapped = replace_once(&encoded, canonical_prefix, swapped_prefix);
    let swapped_error = InstanceStorageManifestV2::from_json_bytes(&swapped).unwrap_err();
    assert!(
        swapped_error.to_string().contains("compact fixed-order"),
        "unexpected error: {swapped_error}"
    );
}

#[tokio::test]
async fn existing_root_missing_instance_directory_fails_closed_without_recreation() {
    let temp = tempdir().unwrap();
    let options = Arc::new(StorageOptions::default());
    let mut created = Storage::new(2, 0);
    let _receiver = created.open(Arc::clone(&options), temp.path()).unwrap();
    created.shutdown().await;
    created.close();

    let missing = temp.path().join("1");
    fs::remove_dir_all(&missing).unwrap();
    assert!(!missing.exists());

    let mut reopened = Storage::new(2, 0);
    let error = reopened.open(options, temp.path()).unwrap_err();
    assert!(
        error.to_string().contains("instance") && error.to_string().contains("missing"),
        "unexpected error: {error}"
    );
    assert!(
        !missing.exists(),
        "an existing root must never authorize recreating a missing instance"
    );
    assert!(reopened.insts.is_empty());
}

#[tokio::test]
async fn existing_root_without_any_instance_directories_fails_closed() {
    let temp = tempdir().unwrap();
    RootStorageManifestV2::new(
        ROOT_MANIFEST_ID.parse().unwrap(),
        2,
        1,
        storage::slot_mapping_digest(2),
        None,
    )
    .unwrap()
    .write_to_dir_atomically(temp.path())
    .unwrap();

    let mut storage = Storage::new(2, 0);
    let error = storage
        .open(Arc::new(StorageOptions::default()), temp.path())
        .unwrap_err();
    assert!(
        error.to_string().contains("instance") && error.to_string().contains("missing"),
        "unexpected error: {error}"
    );
    assert!(!temp.path().join("0").exists());
    assert!(!temp.path().join("1").exists());
    assert!(storage.insts.is_empty());
}

#[test]
fn root_manifest_rejects_absolute_parent_or_nested_migration_paths() {
    let invalid_names = [
        "C:\\kiwi\\0",
        "/var/lib/kiwi/0",
        "../0",
        "shadow/0",
        "shadow\\0",
        ".",
        "..",
        "",
    ];

    for invalid_name in invalid_names {
        for field in ["source_name", "shadow_name", "backup_name"] {
            let (source_name, shadow_name, backup_name) = match field {
                "source_name" => (invalid_name, ".0.shadow", ".0.backup"),
                "shadow_name" => ("live", invalid_name, ".0.backup"),
                "backup_name" => ("live", ".0.shadow", invalid_name),
                _ => unreachable!(),
            };
            let migration = MigrationTransaction::new(
                MIGRATION_TRANSACTION_ID.parse().unwrap(),
                1,
                2,
                MigrationSourceProfile::BaseV1SixCf,
                MigrationPhase::ShadowPrepared,
                0,
                source_name,
                shadow_name,
                backup_name,
            );
            let result = RootStorageManifestV2::new(
                ROOT_MANIFEST_ID.parse().unwrap(),
                1,
                1,
                ManifestDigest::compute(b"slot-modulo-v1:1"),
                Some(migration),
            );
            assert!(
                result.is_err(),
                "migration {field} basename {invalid_name:?} must be rejected"
            );
        }
    }
}

#[test]
fn root_manifest_rejects_aliased_or_reserved_migration_basenames() {
    for (source, shadow, backup) in [
        ("0", ".shadow", ".backup"),
        ("live", ".same", ".same"),
        ("live", "0", ".backup"),
        ("live", ROOT_STORAGE_MANIFEST_FILE, ".backup"),
        ("live", ".shadow", STORAGE_MANIFEST_FILE),
        ("live", "__kiwi_root_storage_manifest.tmp", ".backup"),
    ] {
        let migration = MigrationTransaction::new(
            MIGRATION_TRANSACTION_ID.parse().unwrap(),
            1,
            2,
            MigrationSourceProfile::BaseV1SixCf,
            MigrationPhase::SourceDetected,
            0,
            source,
            shadow,
            backup,
        );
        assert!(
            RootStorageManifestV2::new(
                ROOT_MANIFEST_ID.parse().unwrap(),
                1,
                1,
                ManifestDigest::compute(b"slot-modulo-v1:1"),
                Some(migration),
            )
            .is_err(),
            "reserved or aliased migration layout must fail: {source}/{shadow}/{backup}"
        );
    }
}

#[test]
fn root_manifest_persists_source_profile_in_fixed_migration_order() {
    for source_profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        let migration = MigrationTransaction::new(
            MIGRATION_TRANSACTION_ID.parse().unwrap(),
            1,
            2,
            source_profile,
            MigrationPhase::SourceDetected,
            0,
            "live",
            ".0.shadow",
            ".0.backup",
        );
        let manifest = RootStorageManifestV2::new(
            ROOT_MANIFEST_ID.parse().unwrap(),
            1,
            1,
            ManifestDigest::compute(b"slot-modulo-v1:1"),
            Some(migration),
        )
        .unwrap();
        let encoded = String::from_utf8(manifest.to_json_bytes().unwrap()).unwrap();
        let from_pos = encoded.find("\"from_schema\":1").unwrap();
        let to_pos = encoded.find("\"to_schema\":2").unwrap();
        let profile_pos = encoded.find("\"source_profile\":").unwrap();
        let phase_pos = encoded.find("\"phase\":\"source_detected\"").unwrap();
        assert!(from_pos < to_pos && to_pos < profile_pos && profile_pos < phase_pos);
        assert_eq!(
            RootStorageManifestV2::from_json_bytes(encoded.as_bytes()).unwrap(),
            manifest
        );
    }
}

#[test]
fn storage_open_rejects_instance_count_or_slot_mapping_mismatch() {
    let options = Arc::new(StorageOptions::default());

    let count_temp = tempdir().unwrap();
    root_manifest(2)
        .write_to_dir_atomically(count_temp.path())
        .unwrap();
    let mut wrong_count = Storage::new(3, 0);
    let count_error = wrong_count
        .open(Arc::clone(&options), count_temp.path())
        .unwrap_err();
    assert!(
        count_error.to_string().contains("db_instance_num"),
        "unexpected error: {count_error}"
    );
    assert!(wrong_count.insts.is_empty());
    assert!(
        !wrong_count
            .is_opened
            .load(std::sync::atomic::Ordering::SeqCst)
    );

    let mapping_temp = tempdir().unwrap();
    let wrong_mapping = RootStorageManifestV2::new(
        ROOT_MANIFEST_ID.parse().unwrap(),
        2,
        1,
        ManifestDigest::compute(b"deliberately-wrong-slot-mapping"),
        None,
    )
    .unwrap();
    wrong_mapping
        .write_to_dir_atomically(mapping_temp.path())
        .unwrap();
    let mut storage = Storage::new(2, 0);
    let mapping_error = storage.open(options, mapping_temp.path()).unwrap_err();
    assert!(
        mapping_error.to_string().contains("slot mapping"),
        "unexpected error: {mapping_error}"
    );
    assert!(storage.insts.is_empty());
    assert!(!storage.is_opened.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn canonical_cf_registry_matches_every_column_family_index_variant() {
    let expected = [
        (
            ColumnFamilyIndex::MetaCF,
            0,
            "default",
            ColumnFamilyRole::Metadata,
            ComparatorId::Bytewise,
            1,
        ),
        (
            ColumnFamilyIndex::HashesDataCF,
            1,
            "hash_data_cf",
            ColumnFamilyRole::HashData,
            ComparatorId::Bytewise,
            1,
        ),
        (
            ColumnFamilyIndex::SetsDataCF,
            2,
            "set_data_cf",
            ColumnFamilyRole::SetData,
            ComparatorId::Bytewise,
            1,
        ),
        (
            ColumnFamilyIndex::ListsDataCF,
            3,
            "list_data_cf",
            ColumnFamilyRole::ListData,
            ComparatorId::ListsDataKey,
            1,
        ),
        (
            ColumnFamilyIndex::ZsetsDataCF,
            4,
            "zset_data_cf",
            ColumnFamilyRole::ZsetData,
            ComparatorId::Bytewise,
            1,
        ),
        (
            ColumnFamilyIndex::ZsetsScoreCF,
            5,
            "zset_score_cf",
            ColumnFamilyRole::ZsetScore,
            ComparatorId::ZsetsScoreKey,
            1,
        ),
        (
            ColumnFamilyIndex::VectorDataCF,
            6,
            "vector_data_cf",
            ColumnFamilyRole::VectorData,
            ComparatorId::Bytewise,
            2,
        ),
    ];

    assert_eq!(CANONICAL_COLUMN_FAMILIES.len(), expected.len());
    for (spec, (index, stable_id, name, role, comparator_id, snapshot_read_min_version)) in
        CANONICAL_COLUMN_FAMILIES.iter().zip(expected)
    {
        assert_eq!(spec.index, index);
        assert_eq!(spec.stable_id, stable_id);
        assert_eq!(spec.name, name);
        assert_eq!(spec.role, role);
        assert_eq!(spec.comparator_id, comparator_id);
        assert_eq!(index.stable_id(), stable_id);
        assert_eq!(ColumnFamilyIndex::try_from(stable_id).unwrap(), index);
        assert_eq!(index.name(), name);
        assert_eq!(spec.key_codec_version, 1);
        assert_eq!(spec.value_codec_version, 1);
        assert_eq!(spec.snapshot_read_min_version, snapshot_read_min_version);
        assert_eq!(spec.snapshot_write_version, 2);
    }
}

#[test]
fn raft_logindex_flush_and_checkpoint_consumers_match_canonical_registry() {
    let canonical_names = canonical_column_family_names();
    let index_names: Vec<&str> = ColumnFamilyIndex::ALL
        .iter()
        .map(ColumnFamilyIndex::name)
        .collect();

    assert_eq!(canonical_names, index_names);
    assert_eq!(
        cf_metadata::COLUMN_FAMILY_COUNT,
        CANONICAL_COLUMN_FAMILIES.len()
    );
    assert_eq!(cf_metadata::CF_NAMES_STR, canonical_names.as_slice());
    assert_eq!(expected_column_families(), canonical_names);

    // Raft exports `canonical_column_family_names()` from storage instead of owning a second
    // literal array. Redis open and FLUSHDB consume `ColumnFamilyIndex::ALL`, whose exact mapping
    // to the same registry is asserted above. The raft crate has its own targeted export test.
}
