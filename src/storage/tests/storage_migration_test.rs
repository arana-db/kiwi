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

#![cfg(feature = "test-fault-injection")]

mod support;

use std::sync::Arc;

use rocksdb::{DB, Options};
use storage::{
    CANONICAL_COLUMN_FAMILY_NAMES, InstanceStorageManifestV2, MigrationFaultPoint, MigrationPhase,
    MigrationSourceProfile, ROOT_STORAGE_MANIFEST_FILE, RootStorageManifestV2,
    SLOT_MAPPING_VERSION, STORAGE_MANIFEST_FILE, StorageOptions, classify_storage_root,
    close_rollback_window, fail_next_redis_open, fail_next_storage_migration,
    prepare_or_resume_migration, recover_or_rollback_before_admission, slot_mapping_digest,
};
use uuid::Uuid;

use support::legacy_storage::{VECTOR_CF_NAMES, create_legacy_root, list_cf, read_sentinel};
use support::vector_v1_storage::{
    create_vector_v1_root, rewrite_first_member_incarnation, vector_member_key,
    vector_member_value, vector_meta_key, vector_meta_value,
};

fn sorted_canonical_names() -> Vec<String> {
    let mut names: Vec<String> = CANONICAL_COLUMN_FAMILY_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    names.sort();
    names
}

fn open_storage(root: &std::path::Path, instance_count: usize) -> Result<(), String> {
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    let _runtime_guard = runtime.enter();
    let mut storage = storage::storage::Storage::new(instance_count, 0);
    let receiver = storage
        .open(Arc::new(StorageOptions::default()), root)
        .map_err(|error| error.to_string())?;
    drop(receiver);
    storage.close();
    Ok(())
}

#[test]
fn legacy_six_cf_storage_migrates_all_instances_and_reopens() {
    let temp = tempfile::tempdir().expect("temp root");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();

    assert_eq!(
        classify_storage_root(temp.path(), 2, &options).expect("classify Base source"),
        Some(MigrationSourceProfile::BaseV1SixCf)
    );
    assert_eq!(
        prepare_or_resume_migration(temp.path(), 2, &options).expect("migrate Base source"),
        Some(MigrationSourceProfile::BaseV1SixCf)
    );
    open_storage(temp.path(), 2).expect("open migrated Base storage");

    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("root manifest");
    let transaction = root.migration().expect("migration journal");
    assert_eq!(
        transaction.source_profile,
        MigrationSourceProfile::BaseV1SixCf
    );
    assert_eq!(transaction.phase, MigrationPhase::Committed);
    assert_eq!(transaction.source_name, "live");
    assert!(transaction.shadow_name.starts_with(".kiwi-shadow-"));
    assert!(transaction.backup_name.starts_with(".kiwi-backup-"));

    for instance_id in 0..2_u32 {
        let instance = temp.path().join(instance_id.to_string());
        assert_eq!(list_cf(&instance), sorted_canonical_names());
        let manifest =
            InstanceStorageManifestV2::read_from_dir(&instance).expect("instance v2 manifest");
        manifest
            .validate_root_binding(instance_id, &root)
            .expect("instance binds committed root");
        assert_eq!(
            read_sentinel(&instance, "default", b"string:alpha"),
            format!("value-{instance_id}").as_bytes()
        );
        assert_eq!(
            read_sentinel(&instance, "hash_data_cf", b"hash:field"),
            format!("payload-{instance_id}-hash_data_cf").as_bytes()
        );
    }

    close_rollback_window(temp.path()).expect("close rollback window");
    let closed = RootStorageManifestV2::read_from_dir(temp.path()).expect("closed root");
    assert_eq!(
        closed.migration().expect("migration journal").phase,
        MigrationPhase::RollbackWindowClosed
    );
    for instance_id in 0..2_u32 {
        InstanceStorageManifestV2::read_from_dir(&temp.path().join(instance_id.to_string()))
            .expect("closed instance manifest")
            .validate_root_binding(instance_id, &closed)
            .expect("instance binds closed root");
    }
}

#[test]
fn vector_v1_seven_cf_storage_preserves_vector_data_incarnation_and_generation() {
    let temp = tempfile::tempdir().expect("temp root");
    let identities = create_vector_v1_root(temp.path(), 2);
    let options = StorageOptions::default();

    assert_eq!(
        classify_storage_root(temp.path(), 2, &options).expect("classify Vector-v1 source"),
        Some(MigrationSourceProfile::VectorSetV1SevenCf)
    );
    prepare_or_resume_migration(temp.path(), 2, &options).expect("migrate Vector-v1 source");
    open_storage(temp.path(), 2).expect("open migrated Vector-v1 storage");

    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("root manifest");
    assert_eq!(
        root.migration().expect("migration journal").source_profile,
        MigrationSourceProfile::VectorSetV1SevenCf
    );
    for (instance_id, (storage_incarnation, next_generation)) in
        identities.iter().copied().enumerate()
    {
        let instance = temp.path().join(instance_id.to_string());
        let manifest =
            InstanceStorageManifestV2::read_from_dir(&instance).expect("instance v2 manifest");
        assert_eq!(manifest.storage_incarnation(), storage_incarnation);
        assert_eq!(manifest.next_generation(), next_generation);
        manifest
            .validate_root_binding(instance_id as u32, &root)
            .expect("instance binds root");
        assert_eq!(
            read_sentinel(&instance, "default", &vector_meta_key()),
            vector_meta_value(next_generation)
        );
        assert_eq!(
            read_sentinel(
                &instance,
                "vector_data_cf",
                &vector_member_key(storage_incarnation, next_generation),
            ),
            vector_member_value()
        );
    }
}

#[test]
fn unknown_cf_partial_manifest_or_mixed_v1_v2_fails_before_shadow_creation() {
    let options = StorageOptions::default();

    let unknown = tempfile::tempdir().expect("unknown root");
    create_legacy_root(unknown.path(), 1, false);
    {
        let instance = unknown.path().join("0");
        let mut db_options = Options::default();
        db_options.create_if_missing(false);
        db_options.create_missing_column_families(false);
        let db = DB::open_cf_descriptors(
            &db_options,
            &instance,
            support::legacy_storage::descriptors(&support::legacy_storage::BASE_CF_NAMES),
        )
        .expect("open Base fixture");
        db.create_cf("unknown_cf", &Options::default())
            .expect("create unknown CF");
    }
    assert!(classify_storage_root(unknown.path(), 1, &options).is_err());
    assert!(!unknown.path().join(ROOT_STORAGE_MANIFEST_FILE).exists());
    assert!(
        std::fs::read_dir(unknown.path())
            .expect("read unknown root")
            .all(|entry| !entry
                .expect("root entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".kiwi-shadow-"))
    );

    let mixed = tempfile::tempdir().expect("mixed root");
    create_legacy_root(mixed.path(), 2, false);
    {
        let instance = mixed.path().join("1");
        let mut db_options = Options::default();
        db_options.create_if_missing(false);
        db_options.create_missing_column_families(false);
        let db = DB::open_cf_descriptors(
            &db_options,
            &instance,
            support::legacy_storage::descriptors(&support::legacy_storage::BASE_CF_NAMES),
        )
        .expect("open second Base fixture");
        db.create_cf("vector_data_cf", &Options::default())
            .expect("create Vector CF");
    }
    std::fs::write(
        mixed.path().join("1").join(STORAGE_MANIFEST_FILE),
        br#"{"version":1,"storage_incarnation":7,"next_generation":9}"#,
    )
    .expect("write v1 manifest");
    assert!(classify_storage_root(mixed.path(), 2, &options).is_err());
    assert!(!mixed.path().join(ROOT_STORAGE_MANIFEST_FILE).exists());

    let partial = tempfile::tempdir().expect("partial manifest root");
    create_legacy_root(partial.path(), 1, true);
    assert_eq!(list_cf(&partial.path().join("0")), {
        let mut names: Vec<String> = VECTOR_CF_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        names.sort();
        names
    });
    assert!(classify_storage_root(partial.path(), 1, &options).is_err());
    assert!(!partial.path().join(ROOT_STORAGE_MANIFEST_FILE).exists());
}

#[test]
fn resume_rejects_noncanonical_numeric_root_entry() {
    let temp = tempfile::tempdir().expect("non-canonical root entry");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard = fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterSourceDetected);
    prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("stop after persisting SourceDetected");
    std::fs::create_dir(temp.path().join("00")).expect("create ambiguous numeric entry");

    let error = prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("non-canonical numeric entries must fail closed");
    assert!(error.to_string().contains("unexpected entry 00"));
}

#[cfg(unix)]
#[test]
fn resume_rejects_symlinked_shadow_root_before_external_mutation() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().expect("symlinked shadow root");
    let external = tempfile::tempdir().expect("external shadow target");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard = fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterSourceDetected);
    prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("stop after persisting SourceDetected");
    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("migration journal");
    let shadow_name = &root.migration().expect("migration transaction").shadow_name;
    symlink(external.path(), temp.path().join(shadow_name)).expect("link shadow root externally");

    let error = prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("symlinked shadow root must fail closed");
    assert!(error.to_string().contains("not a real directory"));
    assert!(
        std::fs::read_dir(external.path())
            .expect("read external target")
            .next()
            .is_none(),
        "validation must reject the link before copying into its target"
    );
}

#[test]
fn resume_rejects_source_profile_drift_before_shadow_mutation() {
    let temp = tempfile::tempdir().expect("profile drift root");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard = fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterSourceDetected);
    prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("stop after source classification is journaled");
    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("migration journal");
    let transaction = root.migration().expect("migration transaction");

    let replacement = tempfile::tempdir().expect("replacement Vector-v1 root");
    create_vector_v1_root(replacement.path(), 1);
    std::fs::remove_dir_all(temp.path().join("0")).expect("remove original Base instance");
    std::fs::rename(replacement.path().join("0"), temp.path().join("0"))
        .expect("replace Base instance with Vector-v1 instance");

    let error = prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("persisted Base profile must be revalidated on resume");
    assert!(error.to_string().contains("profile"));
    assert!(
        !temp.path().join(&transaction.shadow_name).exists(),
        "profile drift must fail before creating or copying shadow data"
    );
}

#[test]
fn vector_v1_wrong_member_incarnation_fails_before_shadow_creation() {
    let temp = tempfile::tempdir().expect("Vector-v1 identity root");
    let identities = create_vector_v1_root(temp.path(), 1);
    rewrite_first_member_incarnation(&temp.path().join("0"), identities[0].0 + 1);
    let options = StorageOptions::default();

    let error = prepare_or_resume_migration(temp.path(), 1, &options)
        .expect_err("member incarnation must match the v1 manifest");
    assert!(error.to_string().contains("incarnation"));
    assert!(!temp.path().join(ROOT_STORAGE_MANIFEST_FILE).exists());
    assert!(
        std::fs::read_dir(temp.path())
            .expect("read Vector-v1 root")
            .all(|entry| !entry
                .expect("root entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".kiwi-shadow-"))
    );
}

#[test]
fn legacy_root_with_interrupted_root_manifest_temp_retries_for_each_profile() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        let temp = tempfile::tempdir().expect("interrupted Root manifest temp");
        create_profile_root(temp.path(), profile, 2);
        let root_manifest_temp = temp
            .path()
            .join(ROOT_STORAGE_MANIFEST_FILE)
            .with_extension("tmp");
        let file = std::fs::File::create(&root_manifest_temp)
            .expect("create interrupted Root manifest temp");
        file.sync_all()
            .expect("sync interrupted Root manifest temp");
        drop(file);

        open_storage(temp.path(), 2).expect("retry migration with known Root temp evidence");
        assert!(!root_manifest_temp.exists());
        assert_committed_and_bound(temp.path(), profile, 2);
    }
}

#[test]
fn migration_retries_after_source_detected_for_each_registered_profile() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        let temp = tempfile::tempdir().expect("temp root");
        match profile {
            MigrationSourceProfile::BaseV1SixCf => create_legacy_root(temp.path(), 2, false),
            MigrationSourceProfile::VectorSetV1SevenCf => {
                create_vector_v1_root(temp.path(), 2);
            }
        }
        let options = StorageOptions::default();
        let _guard =
            fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterSourceDetected);
        let error = prepare_or_resume_migration(temp.path(), 2, &options)
            .expect_err("fault after SourceDetected");
        assert!(
            error
                .to_string()
                .contains("injected storage migration failure")
        );

        let detected = RootStorageManifestV2::read_from_dir(temp.path()).expect("journal");
        let transaction = detected.migration().expect("migration transaction");
        assert_eq!(transaction.phase, MigrationPhase::SourceDetected);
        assert_eq!(transaction.source_profile, profile);

        assert_eq!(
            prepare_or_resume_migration(temp.path(), 2, &options).expect("resume migration"),
            Some(profile)
        );
        open_storage(temp.path(), 2).expect("open resumed storage");
        let committed = RootStorageManifestV2::read_from_dir(temp.path()).expect("committed root");
        assert_eq!(
            committed.migration().expect("migration transaction").phase,
            MigrationPhase::Committed
        );
    }
}

fn create_profile_root(
    root: &std::path::Path,
    profile: MigrationSourceProfile,
    instance_count: usize,
) {
    match profile {
        MigrationSourceProfile::BaseV1SixCf => {
            create_legacy_root(root, instance_count, false);
        }
        MigrationSourceProfile::VectorSetV1SevenCf => {
            create_vector_v1_root(root, instance_count);
        }
    }
}

fn assert_committed_and_bound(
    root_path: &std::path::Path,
    profile: MigrationSourceProfile,
    instance_count: usize,
) {
    let root = RootStorageManifestV2::read_from_dir(root_path).expect("committed root");
    let transaction = root.migration().expect("migration journal");
    assert_eq!(transaction.source_profile, profile);
    assert_eq!(transaction.phase, MigrationPhase::Committed);
    assert_eq!(transaction.source_name, "live");
    assert!(transaction.shadow_name.starts_with(".kiwi-shadow-"));
    assert!(transaction.backup_name.starts_with(".kiwi-backup-"));
    for instance_id in 0..instance_count as u32 {
        let instance = root_path.join(instance_id.to_string());
        assert_eq!(list_cf(&instance), sorted_canonical_names());
        InstanceStorageManifestV2::read_from_dir(&instance)
            .expect("instance v2 manifest")
            .validate_root_binding(instance_id, &root)
            .expect("instance binds committed root");
        assert_eq!(
            read_sentinel(&instance, "default", b"string:alpha"),
            format!("value-{instance_id}").as_bytes()
        );
        assert_eq!(
            read_sentinel(&instance, "default", b"ttl:alpha"),
            (1_900_000_000_u64 + u64::from(instance_id)).to_le_bytes()
        );
        for (cf_name, key) in [
            ("hash_data_cf", b"hash:field".as_slice()),
            ("set_data_cf", b"set:member".as_slice()),
            ("list_data_cf", b"list:item".as_slice()),
            ("zset_data_cf", b"zset:member".as_slice()),
            (
                "zset_score_cf",
                b"00000000zset-score-key-0000000000000000".as_slice(),
            ),
        ] {
            assert_eq!(
                read_sentinel(&instance, cf_name, key),
                format!("payload-{instance_id}-{cf_name}").as_bytes()
            );
        }
        if profile == MigrationSourceProfile::VectorSetV1SevenCf {
            let manifest =
                InstanceStorageManifestV2::read_from_dir(&instance).expect("instance manifest");
            assert_eq!(
                read_sentinel(&instance, "default", &vector_meta_key()),
                vector_meta_value(manifest.next_generation())
            );
            assert_eq!(
                read_sentinel(
                    &instance,
                    "vector_data_cf",
                    &vector_member_key(manifest.storage_incarnation(), manifest.next_generation(),),
                ),
                vector_member_value()
            );
        }
    }
}

fn run_fault_retry(
    profile: MigrationSourceProfile,
    fault: MigrationFaultPoint,
    expected_phase: MigrationPhase,
    expected_instance: u32,
) {
    let temp = tempfile::tempdir().expect("fault root");
    create_profile_root(temp.path(), profile, 2);
    let _guard = fail_next_storage_migration(temp.path(), fault);
    let error = open_storage(temp.path(), 2).expect_err("migration fault must stop admission");
    assert!(error.contains("injected storage migration failure"));

    let interrupted = RootStorageManifestV2::read_from_dir(temp.path()).expect("journal");
    let transaction = interrupted.migration().expect("migration transaction");
    assert_eq!(transaction.source_profile, profile);
    assert_eq!(transaction.phase, expected_phase);
    assert_eq!(transaction.current_instance, expected_instance);
    assert_eq!(transaction.source_name, "live");
    assert!(transaction.shadow_name.starts_with(".kiwi-shadow-"));
    assert!(transaction.backup_name.starts_with(".kiwi-backup-"));

    open_storage(temp.path(), 2).expect("resume migration through production open");
    assert_committed_and_bound(temp.path(), profile, 2);
}

#[test]
fn migration_retries_after_shadow_prepared() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        run_fault_retry(
            profile,
            MigrationFaultPoint::AfterShadowPrepared,
            MigrationPhase::ShadowPrepared,
            0,
        );
    }
}

#[test]
fn migration_retries_after_each_instance_copied() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        for instance_id in 0..2 {
            run_fault_retry(
                profile,
                MigrationFaultPoint::AfterInstanceCopied(instance_id),
                MigrationPhase::InstanceCopied,
                instance_id,
            );
        }
    }
}

#[test]
fn migration_retries_after_vector_cf_created_before_instance_manifest() {
    for instance_id in 0..2 {
        run_fault_retry(
            MigrationSourceProfile::BaseV1SixCf,
            MigrationFaultPoint::AfterVectorCfCreatedBeforeInstanceManifest(instance_id),
            MigrationPhase::InstanceCopied,
            instance_id,
        );
    }
}

#[test]
fn migration_retries_after_each_instance_upgraded() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        for instance_id in 0..2 {
            run_fault_retry(
                profile,
                MigrationFaultPoint::AfterInstanceUpgraded(instance_id),
                MigrationPhase::InstanceUpgraded,
                instance_id,
            );
        }
    }
}

#[test]
fn migration_retries_after_all_instances_verified() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        run_fault_retry(
            profile,
            MigrationFaultPoint::AfterAllInstancesVerified,
            MigrationPhase::AllInstancesVerified,
            1,
        );
    }
}

#[test]
fn migration_retries_after_switch_prepared() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        run_fault_retry(
            profile,
            MigrationFaultPoint::AfterSwitchPrepared,
            MigrationPhase::SwitchPrepared,
            0,
        );
    }
}

#[test]
fn switch_prepared_rejects_future_instance_promotion() {
    let temp = tempfile::tempdir().expect("SwitchPrepared topology root");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard = fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterSwitchPrepared);
    prepare_or_resume_migration(temp.path(), 2, &options).expect_err("stop at SwitchPrepared");
    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("journal");
    let transaction = root.migration().expect("transaction");
    let shadow_root = temp.path().join(&transaction.shadow_name);
    let backup_root = temp.path().join(&transaction.backup_name);
    std::fs::create_dir_all(&backup_root).expect("create backup root");
    std::fs::rename(temp.path().join("1"), backup_root.join("1"))
        .expect("move future source to backup");
    std::fs::rename(shadow_root.join("1"), temp.path().join("1")).expect("promote future shadow");

    let error = prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("future instance promotion must not be accepted");
    assert!(error.to_string().contains("inconsistent instance 1 layout"));
}

#[test]
fn migration_retries_after_old_moved_to_backup() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        for instance_id in 0..2 {
            run_fault_retry(
                profile,
                MigrationFaultPoint::AfterOldMovedToBackup(instance_id),
                MigrationPhase::OldMovedToBackup,
                instance_id,
            );
        }
    }
}

#[test]
fn old_moved_rejects_previous_instance_regression() {
    let temp = tempfile::tempdir().expect("OldMoved topology root");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard =
        fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterOldMovedToBackup(1));
    prepare_or_resume_migration(temp.path(), 2, &options).expect_err("stop at OldMoved(1)");
    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("journal");
    let transaction = root.migration().expect("transaction");
    let shadow_root = temp.path().join(&transaction.shadow_name);
    let backup_root = temp.path().join(&transaction.backup_name);
    std::fs::rename(temp.path().join("0"), shadow_root.join("0"))
        .expect("move promoted instance back to shadow");
    std::fs::rename(backup_root.join("0"), temp.path().join("0"))
        .expect("restore previous legacy source");

    let error = prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("previous instance regression must not be accepted");
    assert!(error.to_string().contains("inconsistent instance 0 layout"));
}

#[test]
fn migration_retries_after_shadow_promoted() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        for instance_id in 0..2 {
            run_fault_retry(
                profile,
                MigrationFaultPoint::AfterShadowPromoted(instance_id),
                MigrationPhase::ShadowPromoted,
                instance_id,
            );
        }
    }
}

#[test]
fn shadow_promoted_rejects_two_instance_lookahead() {
    let temp = tempfile::tempdir().expect("ShadowPromoted topology root");
    create_legacy_root(temp.path(), 3, false);
    let options = StorageOptions::default();
    let _guard =
        fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterShadowPromoted(0));
    prepare_or_resume_migration(temp.path(), 3, &options).expect_err("stop at ShadowPromoted(0)");
    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("journal");
    let transaction = root.migration().expect("transaction");
    let backup_root = temp.path().join(&transaction.backup_name);
    std::fs::rename(temp.path().join("2"), backup_root.join("2"))
        .expect("move instance two ahead of journal");

    let error = prepare_or_resume_migration(temp.path(), 3, &options)
        .expect_err("two-instance lookahead must not be accepted");
    assert!(error.to_string().contains("inconsistent instance 2 layout"));
}

#[test]
fn single_step_filesystem_progress_ahead_of_journal_resumes() {
    let switch = tempfile::tempdir().expect("SwitchPrepared ahead root");
    create_legacy_root(switch.path(), 2, false);
    let options = StorageOptions::default();
    let _guard =
        fail_next_storage_migration(switch.path(), MigrationFaultPoint::AfterSwitchPrepared);
    prepare_or_resume_migration(switch.path(), 2, &options).expect_err("stop at SwitchPrepared");
    let root = RootStorageManifestV2::read_from_dir(switch.path()).expect("journal");
    let transaction = root.migration().expect("transaction");
    let backup_root = switch.path().join(&transaction.backup_name);
    std::fs::create_dir_all(&backup_root).expect("create backup root");
    std::fs::rename(switch.path().join("0"), backup_root.join("0"))
        .expect("complete source rename before journal update");
    open_storage(switch.path(), 2).expect("resume SwitchPrepared single-step progress");
    assert_committed_and_bound(switch.path(), MigrationSourceProfile::BaseV1SixCf, 2);

    let old_moved = tempfile::tempdir().expect("OldMoved ahead root");
    create_legacy_root(old_moved.path(), 2, false);
    let _guard = fail_next_storage_migration(
        old_moved.path(),
        MigrationFaultPoint::AfterOldMovedToBackup(0),
    );
    prepare_or_resume_migration(old_moved.path(), 2, &options).expect_err("stop at OldMoved(0)");
    let root = RootStorageManifestV2::read_from_dir(old_moved.path()).expect("journal");
    let transaction = root.migration().expect("transaction");
    std::fs::rename(
        old_moved.path().join(&transaction.shadow_name).join("0"),
        old_moved.path().join("0"),
    )
    .expect("complete shadow promotion before journal update");
    open_storage(old_moved.path(), 2).expect("resume OldMoved single-step progress");
    assert_committed_and_bound(old_moved.path(), MigrationSourceProfile::BaseV1SixCf, 2);

    let promoted = tempfile::tempdir().expect("ShadowPromoted ahead root");
    create_legacy_root(promoted.path(), 2, false);
    let _guard =
        fail_next_storage_migration(promoted.path(), MigrationFaultPoint::AfterShadowPromoted(0));
    prepare_or_resume_migration(promoted.path(), 2, &options)
        .expect_err("stop at ShadowPromoted(0)");
    let root = RootStorageManifestV2::read_from_dir(promoted.path()).expect("journal");
    let transaction = root.migration().expect("transaction");
    std::fs::rename(
        promoted.path().join("1"),
        promoted.path().join(&transaction.backup_name).join("1"),
    )
    .expect("complete next source rename before journal update");
    open_storage(promoted.path(), 2).expect("resume ShadowPromoted single-step progress");
    assert_committed_and_bound(promoted.path(), MigrationSourceProfile::BaseV1SixCf, 2);
}

#[test]
fn migration_retries_after_new_storage_opened() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        run_fault_retry(
            profile,
            MigrationFaultPoint::AfterNewStorageOpened,
            MigrationPhase::NewStorageOpened,
            1,
        );
    }
}

#[test]
fn migration_retries_after_committed_before_rollback_window_closed() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        run_fault_retry(
            profile,
            MigrationFaultPoint::AfterCommitted,
            MigrationPhase::Committed,
            1,
        );
    }
}

#[test]
fn pre_admission_failure_restores_verified_base_backup() {
    let temp = tempfile::tempdir().expect("rollback root");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard = fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterCommitted);
    open_storage(temp.path(), 2).expect_err("fail after committed before admission");

    assert!(
        recover_or_rollback_before_admission(temp.path(), 2, &options)
            .expect("restore verified Base backup")
    );
    assert!(!temp.path().join(ROOT_STORAGE_MANIFEST_FILE).exists());
    assert_eq!(
        classify_storage_root(temp.path(), 2, &options).expect("classify restored source"),
        Some(MigrationSourceProfile::BaseV1SixCf)
    );
    for instance_id in 0..2_u32 {
        let instance = temp.path().join(instance_id.to_string());
        assert_eq!(
            read_sentinel(&instance, "default", b"string:alpha"),
            format!("value-{instance_id}").as_bytes()
        );
    }
    assert!(
        std::fs::read_dir(temp.path())
            .expect("read restored root")
            .all(|entry| {
                let name = entry.expect("root entry").file_name();
                let name = name.to_string_lossy();
                !name.starts_with(".kiwi-shadow-") && !name.starts_with(".kiwi-backup-")
            })
    );
}

fn assert_restored_legacy_profile(
    root: &std::path::Path,
    profile: MigrationSourceProfile,
    identities: &[(u64, u64)],
) {
    let expected_cf = match profile {
        MigrationSourceProfile::BaseV1SixCf => {
            let mut names: Vec<String> = support::legacy_storage::BASE_CF_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect();
            names.sort();
            names
        }
        MigrationSourceProfile::VectorSetV1SevenCf => {
            let mut names: Vec<String> = VECTOR_CF_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect();
            names.sort();
            names
        }
    };
    for instance_id in 0..2_u32 {
        let instance = root.join(instance_id.to_string());
        assert_eq!(list_cf(&instance), expected_cf);
        assert_eq!(
            read_sentinel(&instance, "default", b"string:alpha"),
            format!("value-{instance_id}").as_bytes()
        );
        assert_eq!(
            read_sentinel(&instance, "hash_data_cf", b"hash:field"),
            format!("payload-{instance_id}-hash_data_cf").as_bytes()
        );
        if profile == MigrationSourceProfile::VectorSetV1SevenCf {
            let (storage_incarnation, next_generation) = identities[instance_id as usize];
            assert_eq!(
                read_sentinel(&instance, "default", &vector_meta_key()),
                vector_meta_value(next_generation)
            );
            assert_eq!(
                read_sentinel(
                    &instance,
                    "vector_data_cf",
                    &vector_member_key(storage_incarnation, next_generation),
                ),
                vector_member_value()
            );
        }
    }
}

fn run_rollback_fault_retry(profile: MigrationSourceProfile, fault: MigrationFaultPoint) {
    let temp = tempfile::tempdir().expect("rollback fault root");
    let identities = match profile {
        MigrationSourceProfile::BaseV1SixCf => {
            create_legacy_root(temp.path(), 2, false);
            Vec::new()
        }
        MigrationSourceProfile::VectorSetV1SevenCf => create_vector_v1_root(temp.path(), 2),
    };
    let options = StorageOptions::default();
    let _migration_guard =
        fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterCommitted);
    open_storage(temp.path(), 2).expect_err("stop before admission with committed backup");

    let _rollback_guard = fail_next_storage_migration(temp.path(), fault);
    let error = recover_or_rollback_before_admission(temp.path(), 2, &options)
        .expect_err("rollback fault must interrupt recovery");
    assert!(
        error
            .to_string()
            .contains("injected storage migration failure")
    );

    if temp.path().join(ROOT_STORAGE_MANIFEST_FILE).exists() {
        assert!(
            recover_or_rollback_before_admission(temp.path(), 2, &options)
                .expect("resume interrupted rollback")
        );
    } else {
        assert!(
            !recover_or_rollback_before_admission(temp.path(), 2, &options)
                .expect("completed rollback has no journal")
        );
    }
    assert!(!temp.path().join(ROOT_STORAGE_MANIFEST_FILE).exists());
    assert_eq!(
        classify_storage_root(temp.path(), 2, &options).expect("classify restored legacy root"),
        Some(profile)
    );
    assert_restored_legacy_profile(temp.path(), profile, &identities);
}

#[test]
fn rollback_retries_every_rename_and_cleanup_boundary() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        for instance_id in 0..2 {
            run_rollback_fault_retry(
                profile,
                MigrationFaultPoint::AfterRollbackV2MovedAside(instance_id),
            );
            run_rollback_fault_retry(
                profile,
                MigrationFaultPoint::AfterRollbackLegacyRestored(instance_id),
            );
            run_rollback_fault_retry(
                profile,
                MigrationFaultPoint::AfterRollbackShadowRemoved(instance_id),
            );
        }
        for fault in [
            MigrationFaultPoint::AfterRollbackShadowRootRemoved,
            MigrationFaultPoint::AfterRollbackBackupRootRemoved,
            MigrationFaultPoint::AfterRollbackRootManifestRemoved,
        ] {
            run_rollback_fault_retry(profile, fault);
        }
    }
}

#[test]
fn rollback_window_closed_rejects_automatic_backup_restore() {
    let temp = tempfile::tempdir().expect("closed root");
    create_vector_v1_root(temp.path(), 2);
    let options = StorageOptions::default();
    open_storage(temp.path(), 2).expect("migrate Vector-v1 through production open");
    assert!(close_rollback_window(temp.path()).expect("close rollback window"));
    {
        let instance = temp.path().join("0");
        let db = DB::open_cf_descriptors(
            &Options::default(),
            &instance,
            support::legacy_storage::descriptors(&CANONICAL_COLUMN_FAMILY_NAMES),
        )
        .expect("open closed live instance");
        let default = db.cf_handle("default").expect("default CF");
        db.put_cf(&default, b"post-close:write", b"accepted")
            .expect("write after rollback window closes");
    }
    open_storage(temp.path(), 2).expect("reopen live data after backup legitimately diverges");
    assert_eq!(
        read_sentinel(&temp.path().join("0"), "default", b"post-close:write"),
        b"accepted"
    );
    let error = recover_or_rollback_before_admission(temp.path(), 2, &options)
        .expect_err("closed rollback window must reject automatic restore");
    assert!(error.to_string().contains("RollbackWindowClosed"));
}

#[test]
fn rollback_window_closed_reopens_without_legacy_backup() {
    let temp = tempfile::tempdir().expect("closed root without backup");
    create_vector_v1_root(temp.path(), 2);
    let options = StorageOptions::default();
    open_storage(temp.path(), 2).expect("migrate Vector-v1 through production open");
    assert!(close_rollback_window(temp.path()).expect("close rollback window"));

    let closed = RootStorageManifestV2::read_from_dir(temp.path()).expect("closed root");
    let backup_name = closed
        .migration()
        .expect("closed migration transaction")
        .backup_name
        .clone();
    std::fs::remove_dir_all(temp.path().join(&backup_name))
        .expect("simulate a self-contained snapshot without the legacy backup");

    {
        let instance = temp.path().join("0");
        let db = DB::open_cf_descriptors(
            &Options::default(),
            &instance,
            support::legacy_storage::descriptors(&CANONICAL_COLUMN_FAMILY_NAMES),
        )
        .expect("open closed live instance");
        let default = db.cf_handle("default").expect("default CF");
        db.put_cf(&default, b"post-close:no-backup", b"accepted")
            .expect("write after backup removal");
    }

    open_storage(temp.path(), 2).expect("reopen closed storage without legacy backup");
    assert_eq!(
        read_sentinel(&temp.path().join("0"), "default", b"post-close:no-backup"),
        b"accepted"
    );
    let error = recover_or_rollback_before_admission(temp.path(), 2, &options)
        .expect_err("closed rollback window must stay irreversible without backup");
    assert!(error.to_string().contains("RollbackWindowClosed"));
}

#[test]
fn committed_resume_repairs_instance_binding_after_journal_first_crash() {
    let temp = tempfile::tempdir().expect("committed rebinding root");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard =
        fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterNewStorageOpened);
    open_storage(temp.path(), 2).expect_err("stop with NewStorageOpened journal");
    let stale_instance = std::fs::read(temp.path().join("0").join(STORAGE_MANIFEST_FILE))
        .expect("capture NewStorageOpened instance manifest");

    open_storage(temp.path(), 2).expect("commit migration");
    std::fs::write(
        temp.path().join("0").join(STORAGE_MANIFEST_FILE),
        stale_instance,
    )
    .expect("simulate crash before committed instance rebinding");
    let committed = RootStorageManifestV2::read_from_dir(temp.path()).expect("committed root");
    assert!(
        InstanceStorageManifestV2::read_from_dir(&temp.path().join("0"))
            .expect("stale instance manifest")
            .validate_root_binding(0, &committed)
            .is_err()
    );

    prepare_or_resume_migration(temp.path(), 2, &options).expect("resume committed binding repair");
    InstanceStorageManifestV2::read_from_dir(&temp.path().join("0"))
        .expect("repaired instance manifest")
        .validate_root_binding(0, &committed)
        .expect("instance rebinds to committed root");
}

#[test]
fn closed_resume_repairs_instance_binding_without_restoring_backup() {
    let temp = tempfile::tempdir().expect("closed rebinding root");
    create_vector_v1_root(temp.path(), 2);
    let options = StorageOptions::default();
    open_storage(temp.path(), 2).expect("commit migration");
    let committed_instance = std::fs::read(temp.path().join("1").join(STORAGE_MANIFEST_FILE))
        .expect("capture committed instance manifest");
    assert!(close_rollback_window(temp.path()).expect("close rollback window"));
    std::fs::write(
        temp.path().join("1").join(STORAGE_MANIFEST_FILE),
        committed_instance,
    )
    .expect("simulate crash before closed instance rebinding");
    let closed = RootStorageManifestV2::read_from_dir(temp.path()).expect("closed root");
    assert!(
        InstanceStorageManifestV2::read_from_dir(&temp.path().join("1"))
            .expect("stale committed instance manifest")
            .validate_root_binding(1, &closed)
            .is_err()
    );

    prepare_or_resume_migration(temp.path(), 2, &options).expect("resume closed binding repair");
    InstanceStorageManifestV2::read_from_dir(&temp.path().join("1"))
        .expect("repaired closed instance manifest")
        .validate_root_binding(1, &closed)
        .expect("instance rebinds to closed root");
    assert!(
        recover_or_rollback_before_admission(temp.path(), 2, &options).is_err(),
        "closed migration must not restore the legacy backup"
    );
}

#[test]
fn migration_never_removes_the_only_verified_copy() {
    let temp = tempfile::tempdir().expect("copy safety root");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard =
        fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterOldMovedToBackup(0));
    prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("fail after first source moved to backup");

    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("journal");
    let transaction = root.migration().expect("migration transaction");
    let backup = temp.path().join(&transaction.backup_name).join("0");
    let shadow = temp.path().join(&transaction.shadow_name).join("0");
    assert!(backup.exists(), "verified legacy backup must exist");
    assert!(shadow.exists(), "verified upgraded shadow must exist");
    assert!(!temp.path().join("0").exists());
    assert!(
        temp.path().join("1").exists(),
        "next source remains untouched"
    );

    assert!(
        recover_or_rollback_before_admission(temp.path(), 2, &options).expect("restore backup")
    );
    assert_eq!(
        classify_storage_root(temp.path(), 2, &options).expect("restored Base profile"),
        Some(MigrationSourceProfile::BaseV1SixCf)
    );
}

#[test]
fn shadow_data_loss_fails_before_switch_and_leaves_source_untouched() {
    let temp = tempfile::tempdir().expect("shadow verification root");
    create_legacy_root(temp.path(), 2, false);
    let options = StorageOptions::default();
    let _guard =
        fail_next_storage_migration(temp.path(), MigrationFaultPoint::AfterInstanceUpgraded(1));
    prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("stop after all shadows are upgraded");
    let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("journal");
    let transaction = root.migration().expect("migration transaction");
    let shadow = temp.path().join(&transaction.shadow_name).join("0");
    {
        let mut db_options = Options::default();
        db_options.create_if_missing(false);
        db_options.create_missing_column_families(false);
        let db = DB::open_cf_descriptors(
            &db_options,
            &shadow,
            support::legacy_storage::descriptors(&CANONICAL_COLUMN_FAMILY_NAMES),
        )
        .expect("open shadow");
        let default = db.cf_handle("default").expect("default CF");
        db.delete_cf(&default, b"string:alpha")
            .expect("delete shadow sentinel");
    }

    let error = prepare_or_resume_migration(temp.path(), 2, &options)
        .expect_err("logical data mismatch must fail before switch");
    assert!(error.to_string().contains("changed logical data"));
    assert!(!temp.path().join(&transaction.backup_name).exists());
    assert_eq!(
        read_sentinel(&temp.path().join("0"), "default", b"string:alpha"),
        b"value-0"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn live_open_rejects_missing_vector_cf_instead_of_creating_it() {
    let temp = tempfile::tempdir().expect("strict live root");
    create_legacy_root(temp.path(), 1, false);
    let root = RootStorageManifestV2::new(
        Uuid::new_v4(),
        1,
        SLOT_MAPPING_VERSION,
        slot_mapping_digest(1),
        None,
    )
    .expect("root v2");
    root.write_to_dir_atomically(temp.path())
        .expect("write root v2");
    InstanceStorageManifestV2::new(0, Uuid::new_v4(), &root, 7, 9)
        .expect("instance v2")
        .write_to_dir_atomically(&temp.path().join("0"))
        .expect("write instance v2");

    let mut storage = storage::storage::Storage::new(1, 0);
    let error = storage
        .open(Arc::new(StorageOptions::default()), temp.path())
        .expect_err("live open must not add the missing Vector CF");
    assert!(error.to_string().contains("RocksDB") || error.to_string().contains("column family"));
    let mut expected: Vec<String> = support::legacy_storage::BASE_CF_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    expected.sort();
    assert_eq!(list_cf(&temp.path().join("0")), expected);
}

#[test]
fn production_second_instance_open_failure_releases_handles_and_rolls_back_each_profile() {
    for profile in [
        MigrationSourceProfile::BaseV1SixCf,
        MigrationSourceProfile::VectorSetV1SevenCf,
    ] {
        let temp = tempfile::tempdir().expect("production open failure root");
        let identities = match profile {
            MigrationSourceProfile::BaseV1SixCf => {
                create_legacy_root(temp.path(), 2, false);
                Vec::new()
            }
            MigrationSourceProfile::VectorSetV1SevenCf => create_vector_v1_root(temp.path(), 2),
        };
        let options = StorageOptions::default();
        let _open_failure = fail_next_redis_open(&temp.path().join("1"));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let _runtime_guard = runtime.enter();
        let mut storage = storage::storage::Storage::new(2, 0);
        let error = storage
            .open(Arc::new(StorageOptions::default()), temp.path())
            .expect_err("second promoted instance must fail after RocksDB open");
        assert!(error.to_string().contains("injected Redis open failure"));
        assert!(
            storage.insts.is_empty(),
            "failed open must not publish instances"
        );

        let root = RootStorageManifestV2::read_from_dir(temp.path()).expect("migration journal");
        assert_eq!(
            root.migration().expect("transaction").phase,
            MigrationPhase::ShadowPromoted,
            "production open failure must not claim NewStorageOpened"
        );
        {
            let first_live = temp.path().join("0");
            let db = DB::open_cf_descriptors(
                &Options::default(),
                &first_live,
                support::legacy_storage::descriptors(&CANONICAL_COLUMN_FAMILY_NAMES),
            )
            .expect("first successfully opened handle must be released on later failure");
            drop(db);
        }

        assert!(
            recover_or_rollback_before_admission(temp.path(), 2, &options)
                .expect("server-style pre-admission rollback")
        );
        assert_restored_legacy_profile(temp.path(), profile, &identities);
        assert!(!temp.path().join(ROOT_STORAGE_MANIFEST_FILE).exists());
    }
}
