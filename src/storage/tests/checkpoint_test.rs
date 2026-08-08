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

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use rocksdb::{DB, Options};
use storage::{
    DataType, ManifestDigest, MigrationSourceProfile, ParsedSnapshotMeta,
    ROOT_STORAGE_MANIFEST_FILE, RaftSnapshotMeta, StorageOptions, classify_storage_root,
    close_rollback_window, prepare_checkpoint_restore, prepare_classified_checkpoint_restore,
    restore_checkpoint_layout, storage::Storage, unique_test_db_path,
};

#[allow(dead_code)]
mod support;

fn write_v1_snapshot_meta(checkpoint_root: &std::path::Path) {
    std::fs::write(
        checkpoint_root.join(storage::RAFT_SNAPSHOT_META_FILE),
        br#"{"version":1,"last_included_index":42,"last_included_term":7,"logindex_collector_states":[]}"#,
    )
    .unwrap();
}

fn restore_temp_dirs(parent: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            let is_restore_temp = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".restore_temp_"));
            is_restore_temp.then_some(path)
        })
        .collect()
}

fn write_instance(checkpoint_root: &std::path::Path, instance: usize, value: &[u8]) {
    let instance_dir = checkpoint_root.join(instance.to_string());
    std::fs::create_dir_all(&instance_dir).unwrap();
    std::fs::write(instance_dir.join("CURRENT"), value).unwrap();
}

#[test]
fn missing_checkpoint_instance_leaves_target_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint_root = root.path().join("checkpoint");
    let target = root.path().join("db");
    std::fs::create_dir_all(&checkpoint_root).unwrap();
    write_instance(&checkpoint_root, 0, b"snapshot");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(target.join("sentinel"), b"live").unwrap();

    let error = prepare_checkpoint_restore(&checkpoint_root, &target, 2).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(std::fs::read(target.join("sentinel")).unwrap(), b"live");
    assert!(restore_temp_dirs(root.path()).is_empty());
}

#[test]
fn prepared_restore_does_not_replace_target_until_commit() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint_root = root.path().join("checkpoint");
    let target = root.path().join("db");
    write_instance(&checkpoint_root, 0, b"snapshot");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(target.join("sentinel"), b"live").unwrap();

    let prepared = prepare_checkpoint_restore(&checkpoint_root, &target, 1).unwrap();

    assert_eq!(std::fs::read(target.join("sentinel")).unwrap(), b"live");
    assert_eq!(restore_temp_dirs(root.path()).len(), 1);

    prepared.commit().unwrap();

    assert!(!target.join("sentinel").exists());
    assert_eq!(
        std::fs::read(target.join("0/CURRENT")).unwrap(),
        b"snapshot"
    );
    assert!(restore_temp_dirs(root.path()).is_empty());
}

#[test]
fn dropping_prepared_restore_removes_staged_directory() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint_root = root.path().join("checkpoint");
    let target = root.path().join("db");
    write_instance(&checkpoint_root, 0, b"snapshot");

    let prepared = prepare_checkpoint_restore(&checkpoint_root, &target, 1).unwrap();
    assert_eq!(restore_temp_dirs(root.path()).len(), 1);

    drop(prepared);

    assert!(restore_temp_dirs(root.path()).is_empty());
    assert!(!target.exists());
}

#[tokio::test]
async fn l1_checkpoint_roundtrip() {
    let db_path = unique_test_db_path();
    let cp_root = unique_test_db_path();

    let mut storage = Storage::new(1, 0);
    let options = Arc::new(StorageOptions::default());
    let _rx = storage.open(options.clone(), &db_path).unwrap();

    storage.set(b"k_l1", b"v1").unwrap();

    let meta = RaftSnapshotMeta::new(42, 7);
    storage.create_checkpoint(&cp_root, &meta).unwrap();
    let read_back = RaftSnapshotMeta::read_from_dir(&cp_root).unwrap();
    assert_eq!(read_back, meta);

    let restore_path = unique_test_db_path();
    restore_checkpoint_layout(&cp_root, &restore_path, 1).unwrap();

    let mut storage2 = Storage::new(1, 0);
    let _rx2 = storage2.open(options, &restore_path).unwrap();
    assert_eq!(storage2.get(b"k_l1").unwrap(), "v1");
}

/// Restoring from a checkpoint should materialize the captured state in a fresh path,
/// even if the source storage has changed afterwards.
#[tokio::test]
async fn restore_checkpoint_to_new_storage_after_source_mutation() {
    let db_path = unique_test_db_path();
    let cp_root = unique_test_db_path();
    let restore_path = unique_test_db_path();

    let mut storage = Storage::new(1, 0);
    let options = Arc::new(StorageOptions::default());
    let _rx = storage.open(options.clone(), &db_path).unwrap();

    storage.set(b"k_rep", b"from_cp").unwrap();

    let meta = RaftSnapshotMeta::new(1, 1);
    storage.create_checkpoint(&cp_root, &meta).unwrap();

    storage.set(b"k_rep", b"after_cp").unwrap();

    restore_checkpoint_layout(&cp_root, &restore_path, 1).unwrap();

    let mut restored = Storage::new(1, 0);
    let _rx2 = restored.open(options, &restore_path).unwrap();
    assert_eq!(restored.get(b"k_rep").unwrap(), "from_cp");
}

#[test]
fn test_snapshot_meta_version() {
    let meta = RaftSnapshotMeta::new(100, 5);

    let json = serde_json::to_string(&meta).unwrap();
    assert!(json.contains("\"version\":2"));

    let deserialized: RaftSnapshotMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.version, 2);
    assert_eq!(deserialized.last_included_index, 100);
    assert_eq!(deserialized.last_included_term, 5);
}

#[test]
fn test_snapshot_meta_rejects_unsupported_version() {
    use std::fs;

    let tmp_dir = tempfile::tempdir().unwrap();
    let meta_path = tmp_dir.path().join("__raft_snapshot_meta");

    // Write unsupported version 0 to file
    let invalid_json = r#"{
        "version": 0,
        "last_included_index": 42,
        "last_included_term": 7
    }"#;
    fs::write(&meta_path, invalid_json).unwrap();

    let result = RaftSnapshotMeta::read_from_dir(tmp_dir.path());
    assert!(result.is_err(), "Should reject unsupported version 0");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("unsupported snapshot version"),
        "Error should mention unsupported version"
    );
}

#[test]
fn snapshot_meta_rejects_unknown_future_version() {
    use std::fs;

    let tmp_dir = tempfile::tempdir().unwrap();
    let meta_path = tmp_dir.path().join("__raft_snapshot_meta");

    // A higher version comes from a newer binary whose schema this node
    // cannot safely consume; it must be rejected deterministically.
    let json = r#"{
        "version": 9999,
        "last_included_index": 42,
        "last_included_term": 7
    }"#;
    fs::write(&meta_path, json).unwrap();

    let result = ParsedSnapshotMeta::read_from_dir(tmp_dir.path());
    assert!(result.is_err(), "Higher versions must be rejected");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("unsupported snapshot version"),
        "Error should mention unsupported version"
    );
}

#[test]
fn snapshot_meta_rejects_unknown_fields_in_known_version() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let json = r#"{
        "version": 1,
        "last_included_index": 42,
        "last_included_term": 7,
        "logindex_collector_states": [],
        "future_storage_contract": true
    }"#;
    std::fs::write(tmp_dir.path().join(storage::RAFT_SNAPSHOT_META_FILE), json).unwrap();

    let error = ParsedSnapshotMeta::read_from_dir(tmp_dir.path()).unwrap_err();
    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn snapshot_meta_classifies_known_base_v1() {
    use std::fs;

    let tmp_dir = tempfile::tempdir().unwrap();
    let meta_path = tmp_dir.path().join("__raft_snapshot_meta");

    let json = r#"{
        "version": 1,
        "last_included_index": 42,
        "last_included_term": 7,
        "logindex_collector_states": []
    }"#;
    fs::write(&meta_path, json).unwrap();

    let parsed = ParsedSnapshotMeta::read_from_dir(tmp_dir.path()).unwrap();
    let ParsedSnapshotMeta::LegacyV1(meta) = parsed else {
        panic!("known snapshot version 1 must classify as LegacyV1");
    };
    assert_eq!(meta.last_included_index, 42);
    assert_eq!(meta.last_included_term, 7);
}

#[tokio::test]
async fn base_v1_snapshot_stages_and_migrates_every_registered_cf() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint_root = root.path().join("checkpoint");
    let target = root.path().join("target");
    support::legacy_storage::create_legacy_root(&checkpoint_root, 1, false);
    write_v1_snapshot_meta(&checkpoint_root);
    let parsed = ParsedSnapshotMeta::read_from_dir(&checkpoint_root).unwrap();
    let options = StorageOptions::default();

    let prepared =
        prepare_classified_checkpoint_restore(&checkpoint_root, &target, 1, &parsed, &options)
            .unwrap();
    assert_eq!(
        classify_storage_root(prepared.staged_path(), 1, &options).unwrap(),
        Some(MigrationSourceProfile::BaseV1SixCf)
    );
    assert!(
        prepared
            .staged_path()
            .join(ROOT_STORAGE_MANIFEST_FILE)
            .is_file()
    );

    let staged_path = prepared.staged_path().to_path_buf();
    let mut staged = Storage::new(1, 0);
    let staged_rx = staged
        .open(Arc::new(StorageOptions::default()), &staged_path)
        .unwrap();
    staged.close();
    drop(staged_rx);
    drop(staged);
    for (cf_name, key) in [
        ("default", b"string:alpha".as_slice()),
        ("hash_data_cf", b"hash:field".as_slice()),
        ("zset_data_cf", b"zset:member".as_slice()),
        ("default", b"ttl:alpha".as_slice()),
    ] {
        assert!(
            !support::legacy_storage::read_sentinel(&staged_path.join("0"), cf_name, key)
                .is_empty()
        );
    }
}

#[test]
fn base_v1_snapshot_with_vector_data_cf_fails_before_pause() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint_root = root.path().join("checkpoint");
    let target = root.path().join("target");
    support::legacy_storage::create_legacy_root(&checkpoint_root, 1, true);
    write_v1_snapshot_meta(&checkpoint_root);
    let parsed = ParsedSnapshotMeta::read_from_dir(&checkpoint_root).unwrap();

    let error = prepare_classified_checkpoint_restore(
        &checkpoint_root,
        &target,
        1,
        &parsed,
        &StorageOptions::default(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("invalid Base-v1 snapshot instance 0")
    );
    assert!(restore_temp_dirs(root.path()).is_empty());
    assert!(!target.exists());
}

#[test]
fn base_v1_snapshot_with_vector_meta_fails_before_pause() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint_root = root.path().join("checkpoint");
    let target = root.path().join("target");
    support::legacy_storage::create_legacy_root(&checkpoint_root, 1, false);
    write_v1_snapshot_meta(&checkpoint_root);
    {
        let instance = checkpoint_root.join("0");
        let db = DB::open_cf_descriptors(
            &Options::default(),
            &instance,
            support::legacy_storage::descriptors(&support::legacy_storage::BASE_CF_NAMES),
        )
        .unwrap();
        let meta_cf = db.cf_handle("default").unwrap();
        db.put_cf(
            &meta_cf,
            b"forbidden-vector-meta",
            [DataType::VectorSet as u8],
        )
        .unwrap();
    }
    let parsed = ParsedSnapshotMeta::read_from_dir(&checkpoint_root).unwrap();

    let error = prepare_classified_checkpoint_restore(
        &checkpoint_root,
        &target,
        1,
        &parsed,
        &StorageOptions::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("contains Vector Set metadata"));
    assert!(restore_temp_dirs(root.path()).is_empty());
}

#[test]
fn base_v1_snapshot_with_unknown_cf_fails_before_pause() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint_root = root.path().join("checkpoint");
    let target = root.path().join("target");
    support::legacy_storage::create_legacy_root(&checkpoint_root, 1, false);
    write_v1_snapshot_meta(&checkpoint_root);
    {
        let instance = checkpoint_root.join("0");
        let db = DB::open_cf_descriptors(
            &Options::default(),
            &instance,
            support::legacy_storage::descriptors(&support::legacy_storage::BASE_CF_NAMES),
        )
        .unwrap();
        db.create_cf("unknown_cf", &Options::default()).unwrap();
    }
    let parsed = ParsedSnapshotMeta::read_from_dir(&checkpoint_root).unwrap();

    let error = prepare_classified_checkpoint_restore(
        &checkpoint_root,
        &target,
        1,
        &parsed,
        &StorageOptions::default(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unregistered legacy column-family layout")
    );
    assert!(restore_temp_dirs(root.path()).is_empty());
}

#[tokio::test]
async fn restore_lists_actual_cfs_before_opening_staged_rocksdb() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let checkpoint = root.path().join("checkpoint");
    let target = root.path().join("target");
    let options = Arc::new(StorageOptions::default());
    let mut storage = Storage::new(1, 0);
    let storage_rx = storage.open(Arc::clone(&options), &source).unwrap();
    let meta = RaftSnapshotMeta::for_storage(42, 7, &[], &storage).unwrap();
    storage.create_checkpoint(&checkpoint, &meta).unwrap();
    storage.shutdown().await;
    storage.close();
    drop(storage_rx);
    drop(storage);

    {
        let db = DB::open_cf_descriptors(
            &Options::default(),
            checkpoint.join("0"),
            support::legacy_storage::descriptors(&support::legacy_storage::VECTOR_CF_NAMES),
        )
        .unwrap();
        db.create_cf("unknown_cf", &Options::default()).unwrap();
    }

    let parsed = ParsedSnapshotMeta::read_from_dir(&checkpoint).unwrap();
    let error = prepare_classified_checkpoint_restore(
        &checkpoint,
        &target,
        1,
        &parsed,
        &StorageOptions::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("non-canonical CF set"));
    assert!(restore_temp_dirs(root.path()).is_empty());
    assert!(!target.exists());
}

#[test]
fn test_snapshot_meta_max_version() {
    let meta = RaftSnapshotMeta {
        version: u32::MAX,
        ..RaftSnapshotMeta::new(42, 7)
    };

    let json = serde_json::to_string(&meta).unwrap();
    let deserialized: RaftSnapshotMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.version, u32::MAX);
}

/// Test that RaftSnapshotMeta supports per-instance logindex collector states.
#[test]
fn test_raft_snapshot_meta_with_collector_states() {
    let meta = RaftSnapshotMeta {
        logindex_collector_states: vec![
            vec!["100:1000".to_string(), "200:2000".to_string()],
            vec!["150:1500".to_string()],
        ],
        ..RaftSnapshotMeta::new(300, 1)
    };

    let json = serde_json::to_string_pretty(&meta).unwrap();
    let parsed: RaftSnapshotMeta = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.logindex_collector_states.len(), 2);
    assert_eq!(
        parsed.logindex_collector_states[0],
        vec!["100:1000", "200:2000"]
    );
    assert_eq!(parsed.logindex_collector_states[1], vec!["150:1500"]);
}

/// Test that missing collector state field defaults to empty vec.
#[test]
fn test_raft_snapshot_meta_defaults_empty_states() {
    let json = r#"{
        "version": 2,
        "last_included_index": 100,
        "last_included_term": 5
    }"#;

    let parsed: RaftSnapshotMeta = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.logindex_collector_states.len(), 0);
}

/// Test collector state roundtrip via with_collector_states / restore_collector_states.
#[test]
fn test_collector_states_roundtrip() {
    use storage::logindex::LogIndexAndSequenceCollector;

    let inst0 = Arc::new(LogIndexAndSequenceCollector::new(0));
    inst0.update(100, 1000);
    inst0.update(200, 2000);
    inst0.update(300, 3000);

    let inst1 = Arc::new(LogIndexAndSequenceCollector::new(0));
    inst1.update(150, 1500);

    let collectors = [inst0.clone(), inst1.clone()];
    let meta = RaftSnapshotMeta::with_collector_states(300, 1, &collectors);

    assert_eq!(meta.logindex_collector_states.len(), 2);
    assert_eq!(meta.logindex_collector_states[0].len(), 3);
    assert!(meta.logindex_collector_states[0].contains(&"100:1000".to_string()));
    assert!(meta.logindex_collector_states[0].contains(&"200:2000".to_string()));
    assert!(meta.logindex_collector_states[0].contains(&"300:3000".to_string()));
    assert_eq!(
        meta.logindex_collector_states[1],
        vec!["150:1500".to_string()]
    );

    let new_inst0 = Arc::new(LogIndexAndSequenceCollector::new(0));
    let new_inst1 = Arc::new(LogIndexAndSequenceCollector::new(0));
    meta.restore_collector_states(&[new_inst0.clone(), new_inst1.clone()]);

    assert_eq!(new_inst0.find_applied_log_index(1000), 100);
    assert_eq!(new_inst0.find_applied_log_index(1500), 100);
    assert_eq!(new_inst0.find_applied_log_index(2000), 200);
    assert_eq!(new_inst0.find_applied_log_index(2500), 200);
    assert_eq!(new_inst0.find_applied_log_index(3000), 300);
    assert_eq!(new_inst1.find_applied_log_index(1500), 150);
}

/// v2 meta built from a live Storage round trips through the checkpoint
/// directory and passes restore validation against the same instance count.
#[tokio::test]
async fn test_v2_meta_for_storage_roundtrip_and_validate() {
    let db_path = unique_test_db_path();
    let cp_root = unique_test_db_path();

    let mut storage = Storage::new(2, 0);
    let options = Arc::new(StorageOptions::default());
    let _rx = storage.open(options, &db_path).unwrap();

    let collectors: Vec<_> = (0..storage.db_instance_num)
        .filter_map(|i| storage.get_logindex_collector(i))
        .collect();
    let meta = RaftSnapshotMeta::for_storage(42, 7, &collectors, &storage).unwrap();

    assert_eq!(meta.version, 2);
    assert_eq!(meta.db_instance_num, 2);
    assert!(meta.root_manifest_id.is_some());
    assert!(meta.root_manifest_digest.is_some());
    assert_eq!(meta.instance_manifests.len(), 2);
    assert_eq!(meta.storage_incarnations.len(), 2);
    for (i, inst) in storage.insts.iter().enumerate() {
        assert_eq!(
            meta.storage_incarnations[i],
            inst.storage_incarnation().unwrap()
        );
        assert_eq!(meta.instance_manifests[i].instance_id, i as u32);
        assert_eq!(
            meta.instance_manifests[i].storage_incarnation,
            meta.storage_incarnations[i]
        );
    }
    assert_eq!(
        meta.column_families,
        storage::checkpoint::expected_column_families()
    );
    assert_eq!(
        meta.vector_value_format_max,
        storage::format_vector::VECTOR_VALUE_FORMAT
    );

    storage.create_checkpoint(&cp_root, &meta).unwrap();
    assert!(cp_root.join(ROOT_STORAGE_MANIFEST_FILE).is_file());
    let read_back = RaftSnapshotMeta::read_from_dir(&cp_root).unwrap();
    assert_eq!(read_back, meta);
    read_back.validate_for_restore(2).unwrap();

    let restore_path = unique_test_db_path();
    let prepared = prepare_checkpoint_restore(&cp_root, &restore_path, 2).unwrap();
    prepared
        .validate_snapshot_manifests(&ParsedSnapshotMeta::CurrentV2(read_back.clone()))
        .unwrap();

    let mut mismatched = read_back.storage_incarnations.clone();
    mismatched[1] = mismatched[1].wrapping_add(1).max(1);
    let mut mismatched_meta = read_back.clone();
    mismatched_meta.storage_incarnations = mismatched;
    mismatched_meta.instance_manifests[1].storage_incarnation =
        mismatched_meta.storage_incarnations[1];
    let err = prepared
        .validate_snapshot_manifests(&ParsedSnapshotMeta::CurrentV2(mismatched_meta))
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("instance 1 storage incarnation mismatch"),
        "unexpected error: {err}"
    );

    assert!(!restore_path.exists());
    drop(prepared);
    let _ = std::fs::remove_dir_all(db_path);
    let _ = std::fs::remove_dir_all(cp_root);
    let _ = std::fs::remove_dir_all(restore_path);
}

#[tokio::test]
async fn v2_snapshot_requires_exact_root_manifest_digest() {
    let db_path = unique_test_db_path();
    let cp_root = unique_test_db_path();
    let restore_path = unique_test_db_path();
    let mut storage = Storage::new(1, 0);
    let _rx = storage
        .open(Arc::new(StorageOptions::default()), &db_path)
        .unwrap();
    let collectors: Vec<_> = (0..storage.db_instance_num)
        .filter_map(|i| storage.get_logindex_collector(i))
        .collect();
    let meta = RaftSnapshotMeta::for_storage(42, 7, &collectors, &storage).unwrap();
    storage.create_checkpoint(&cp_root, &meta).unwrap();
    let prepared = prepare_checkpoint_restore(&cp_root, &restore_path, 1).unwrap();
    let mut tampered = meta;
    tampered.root_manifest_digest = Some(ManifestDigest::compute(b"wrong root"));

    let error = prepared
        .validate_snapshot_manifests(&ParsedSnapshotMeta::CurrentV2(tampered))
        .unwrap_err();
    assert!(error.to_string().contains("root manifest digest mismatch"));
}

#[tokio::test]
async fn v2_snapshot_requires_every_instance_manifest_digest_and_incarnation() {
    let db_path = unique_test_db_path();
    let cp_root = unique_test_db_path();
    let restore_path = unique_test_db_path();
    let mut storage = Storage::new(1, 0);
    let _rx = storage
        .open(Arc::new(StorageOptions::default()), &db_path)
        .unwrap();
    let collectors: Vec<_> = (0..storage.db_instance_num)
        .filter_map(|i| storage.get_logindex_collector(i))
        .collect();
    let meta = RaftSnapshotMeta::for_storage(42, 7, &collectors, &storage).unwrap();
    storage.create_checkpoint(&cp_root, &meta).unwrap();
    let prepared = prepare_checkpoint_restore(&cp_root, &restore_path, 1).unwrap();

    let mut bad_digest = meta.clone();
    bad_digest.instance_manifests[0].manifest_digest = ManifestDigest::compute(b"wrong instance");
    let error = prepared
        .validate_snapshot_manifests(&ParsedSnapshotMeta::CurrentV2(bad_digest))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("instance 0 manifest digest mismatch")
    );

    let mut bad_incarnation = meta;
    bad_incarnation.instance_manifests[0].storage_incarnation = bad_incarnation.instance_manifests
        [0]
    .storage_incarnation
    .wrapping_add(1)
    .max(1);
    let error = prepared
        .validate_snapshot_manifests(&ParsedSnapshotMeta::CurrentV2(bad_incarnation))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("instance 0 storage incarnation mismatch")
    );
}

#[tokio::test]
async fn rollback_window_closed_snapshot_restores_without_legacy_backup() {
    let source = tempfile::tempdir().unwrap();
    support::vector_v1_storage::create_vector_v1_root(source.path(), 1);
    let options = Arc::new(StorageOptions::default());

    let mut migrated = Storage::new(1, 0);
    let migrated_rx = migrated.open(Arc::clone(&options), source.path()).unwrap();
    migrated.shutdown().await;
    migrated.close();
    drop(migrated_rx);
    drop(migrated);
    assert!(close_rollback_window(source.path()).unwrap());

    let closed_root = storage::RootStorageManifestV2::read_from_dir(source.path()).unwrap();
    let backup_name = closed_root
        .migration()
        .expect("closed migration transaction")
        .backup_name
        .clone();
    std::fs::remove_dir_all(source.path().join(backup_name)).unwrap();

    let mut source_storage = Storage::new(1, 0);
    let source_rx = source_storage
        .open(Arc::clone(&options), source.path())
        .unwrap();
    let meta = RaftSnapshotMeta::for_storage(42, 7, &[], &source_storage).unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    source_storage
        .create_checkpoint(checkpoint.path(), &meta)
        .unwrap();
    source_storage.shutdown().await;
    source_storage.close();
    drop(source_rx);
    drop(source_storage);

    let target_parent = tempfile::tempdir().unwrap();
    let target = target_parent.path().join("restored");
    let parsed = ParsedSnapshotMeta::read_from_dir(checkpoint.path()).unwrap();
    let prepared =
        prepare_classified_checkpoint_restore(checkpoint.path(), &target, 1, &parsed, &options)
            .unwrap();
    assert!(
        !prepared
            .staged_path()
            .join(&closed_root.migration().unwrap().backup_name)
            .exists(),
        "a current snapshot must not depend on the legacy rollback backup"
    );

    let mut staged = Storage::new(1, 0);
    let staged_rx = staged
        .open(Arc::clone(&options), prepared.staged_path())
        .unwrap();
    staged.shutdown().await;
    staged.close();
    drop(staged_rx);
}

#[tokio::test]
async fn merged_head_v2_snapshot_migrates_legacy_vector_manifest_before_install() {
    let checkpoint = tempfile::tempdir().unwrap();
    let identities = support::vector_v1_storage::create_vector_v1_root(checkpoint.path(), 1);
    let old_incarnation = identities[0].0;
    let old_meta = RaftSnapshotMeta {
        version: storage::CURRENT_SNAPSHOT_VERSION,
        last_included_index: 42,
        last_included_term: 7,
        storage_schema_version: 1,
        storage_incarnations: vec![old_incarnation],
        root_manifest_id: None,
        root_manifest_digest: None,
        instance_manifests: Vec::new(),
        db_instance_num: 1,
        column_families: storage::checkpoint::expected_column_families(),
        vector_value_format_max: storage::format_vector::VECTOR_VALUE_FORMAT,
        logindex_collector_states: Vec::new(),
    };
    old_meta.write_to_dir(checkpoint.path()).unwrap();
    let parsed = ParsedSnapshotMeta::read_from_dir(checkpoint.path()).unwrap();
    let target_parent = tempfile::tempdir().unwrap();
    let target = target_parent.path().join("restored");

    let prepared = prepare_classified_checkpoint_restore(
        checkpoint.path(),
        &target,
        1,
        &parsed,
        &StorageOptions::default(),
    )
    .unwrap();
    let root = storage::RootStorageManifestV2::read_from_dir(prepared.staged_path()).unwrap();
    let instance =
        storage::InstanceStorageManifestV2::read_from_dir(&prepared.staged_path().join("0"))
            .unwrap();
    instance.validate_root_binding(0, &root).unwrap();
    assert_eq!(
        instance.storage_incarnation(),
        old_incarnation,
        "Vector-v1 migration must preserve the identity encoded in member keys"
    );

    let mut staged = Storage::new(1, 0);
    let staged_rx = staged
        .open(Arc::new(StorageOptions::default()), prepared.staged_path())
        .unwrap();
    let sample = staged.validate_vector_data_sample(8).unwrap();
    assert_eq!(sample.metas, 1);
    assert_eq!(sample.members, 1);
    staged.shutdown().await;
    staged.close();
    drop(staged_rx);
}

#[test]
fn merged_head_v2_snapshot_requires_manifest_incarnation_match_before_stage() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint = root.path().join("checkpoint");
    let identities = support::vector_v1_storage::create_vector_v1_root(&checkpoint, 1);
    let mismatched_incarnation = identities[0].0.wrapping_add(1).max(1);
    let old_meta = RaftSnapshotMeta {
        version: storage::CURRENT_SNAPSHOT_VERSION,
        last_included_index: 42,
        last_included_term: 7,
        storage_schema_version: 1,
        storage_incarnations: vec![mismatched_incarnation],
        root_manifest_id: None,
        root_manifest_digest: None,
        instance_manifests: Vec::new(),
        db_instance_num: 1,
        column_families: storage::checkpoint::expected_column_families(),
        vector_value_format_max: storage::format_vector::VECTOR_VALUE_FORMAT,
        logindex_collector_states: Vec::new(),
    };
    old_meta.write_to_dir(&checkpoint).unwrap();
    let parsed = ParsedSnapshotMeta::read_from_dir(&checkpoint).unwrap();
    let target = root.path().join("restored");

    let error = prepare_classified_checkpoint_restore(
        &checkpoint,
        &target,
        1,
        &parsed,
        &StorageOptions::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("storage incarnation"));
    assert!(restore_temp_dirs(root.path()).is_empty());
    assert!(!target.exists());
}

#[test]
fn test_validate_for_restore_rejects_bad_schema() {
    use storage::format_vector::VECTOR_VALUE_FORMAT;
    use storage::{ManifestDigest, SnapshotInstanceManifest};

    let valid = || RaftSnapshotMeta {
        db_instance_num: 2,
        storage_incarnations: vec![11, 22],
        root_manifest_id: Some(uuid::Uuid::nil()),
        root_manifest_digest: Some(ManifestDigest::compute(b"test root manifest")),
        instance_manifests: vec![
            SnapshotInstanceManifest {
                instance_id: 0,
                manifest_digest: ManifestDigest::compute(b"test instance 0"),
                storage_incarnation: 11,
            },
            SnapshotInstanceManifest {
                instance_id: 1,
                manifest_digest: ManifestDigest::compute(b"test instance 1"),
                storage_incarnation: 22,
            },
        ],
        ..RaftSnapshotMeta::new(42, 7)
    };

    // Instance count does not match local configuration.
    let err = valid().validate_for_restore(3).unwrap_err();
    assert!(
        err.to_string().contains("db_instance_num"),
        "unexpected error: {err}"
    );

    // Metadata-level validation checks the incarnation list shape. Exact
    // values are paired with staged manifests before the restore commits.
    let mut meta = valid();
    meta.storage_incarnations = vec![11];
    let err = meta.validate_for_restore(2).unwrap_err();
    assert!(
        err.to_string().contains("storage incarnations"),
        "unexpected error: {err}"
    );

    // Differing incarnation values are accepted when the redundant metadata
    // remains internally consistent; exact values are checked against files.
    let mut meta = valid();
    meta.storage_incarnations = vec![999, 888];
    meta.instance_manifests[0].storage_incarnation = 999;
    meta.instance_manifests[1].storage_incarnation = 888;
    meta.validate_for_restore(2).unwrap();

    // Missing / mismatched column families.
    let mut meta = valid();
    meta.column_families.pop();
    let err = meta.validate_for_restore(2).unwrap_err();
    assert!(
        err.to_string().contains("column families"),
        "unexpected error: {err}"
    );

    // Unknown (newer) vector value format.
    let mut meta = valid();
    meta.vector_value_format_max = VECTOR_VALUE_FORMAT + 1;
    let err = meta.validate_for_restore(2).unwrap_err();
    assert!(
        err.to_string().contains("vector value format"),
        "unexpected error: {err}"
    );

    // Unknown (newer) storage schema version.
    let mut meta = valid();
    meta.storage_schema_version = storage::STORAGE_SCHEMA_VERSION + 1;
    let err = meta.validate_for_restore(2).unwrap_err();
    assert!(
        err.to_string().contains("storage schema version"),
        "unexpected error: {err}"
    );
}
