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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::{CommittedLeaderId, LogId, SnapshotMeta, StoredMembership};
use raft::snapshot_install::{
    CURRENT_SNAPSHOT_DATA, CURRENT_SNAPSHOT_META, SnapshotInstallIntent, SnapshotInstallPhase,
    SnapshotInstallRecoveryDecision, abandon_staged_install, complete_pending_cleanup,
    persist_paused_old_storage, persist_phase, persist_staged_validated,
    publish_pending_current_snapshot, recover_snapshot_install, rename_and_sync,
    snapshot_install_marker_path, storage_identity_from_root, validate_install_basename,
};
use storage::{
    ROOT_STORAGE_MANIFEST_FILE, RaftSnapshotMeta, STORAGE_MANIFEST_FILE, StorageOptions,
    storage::Storage,
};

struct InstallFixture {
    _root: tempfile::TempDir,
    target: PathBuf,
    snapshot_work_dir: PathBuf,
    options: Arc<StorageOptions>,
    intent: SnapshotInstallIntent,
}

async fn close_storage(
    mut storage: Storage,
    receiver: tokio::sync::mpsc::Receiver<storage::BgTask>,
) {
    storage.shutdown().await;
    storage.close();
    drop(receiver);
}

async fn create_storage(path: &Path, value: &[u8]) -> anyhow::Result<()> {
    let mut storage = Storage::new(1, 0);
    let receiver = storage.open(Arc::new(StorageOptions::default()), path)?;
    storage.set(b"authority", value)?;
    close_storage(storage, receiver).await;
    Ok(())
}

async fn read_authority(path: &Path) -> anyhow::Result<String> {
    let mut storage = Storage::new(1, 0);
    let receiver = storage.open(Arc::new(StorageOptions::default()), path)?;
    let value = storage.get(b"authority")?;
    close_storage(storage, receiver).await;
    Ok(value)
}

async fn install_fixture() -> anyhow::Result<InstallFixture> {
    let root = tempfile::tempdir()?;
    let target = root.path().join("db");
    let staged = root.path().join(".restore_temp_snapshot-install-test");
    let snapshot_work_dir = root.path().join("raft-snapshots");
    create_storage(&target, b"old").await?;
    create_storage(&staged, b"new").await?;
    let checkpoint_meta_path = root.path().join("checkpoint-meta.json");
    std::fs::write(
        &checkpoint_meta_path,
        serde_json::to_vec(&RaftSnapshotMeta::new(41, 7))?,
    )?;
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(7, 1), 41)),
        last_membership: StoredMembership::default(),
        snapshot_id: "snapshot-install-recovery".to_string(),
    };
    let options = Arc::new(StorageOptions::default());
    let intent = persist_staged_validated(
        &target,
        &staged,
        &snapshot_work_dir,
        &meta,
        b"durable snapshot archive",
        &checkpoint_meta_path,
        1,
        &options,
    )?;
    Ok(InstallFixture {
        _root: root,
        target,
        snapshot_work_dir,
        options,
        intent,
    })
}

fn persist_storage_paused(fixture: &mut InstallFixture) -> anyhow::Result<()> {
    let old = storage_identity_from_root(&fixture.target, 1, &fixture.options)?;
    persist_paused_old_storage(&mut fixture.intent, old)?;
    Ok(())
}

fn drive_to_new_renamed(fixture: &mut InstallFixture) -> anyhow::Result<()> {
    persist_storage_paused(fixture)?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::MarkerPersisted)?;
    rename_and_sync(
        fixture.intent.layout.target_path(),
        fixture.intent.layout.backup_path(),
    )?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::OldRenamedToBackup,
    )?;
    rename_and_sync(
        fixture.intent.layout.staged_path(),
        fixture.intent.layout.target_path(),
    )?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::NewRenamedToTarget,
    )?;
    Ok(())
}

#[test]
fn marker_rejects_absolute_or_non_basename_paths() {
    for invalid in [
        Path::new("/absolute"),
        Path::new("../escape"),
        Path::new("nested/stage"),
        Path::new("."),
        Path::new(".."),
    ] {
        let error = validate_install_basename(invalid)
            .expect_err("snapshot install paths must be one safe relative basename");
        assert!(error.to_string().contains("basename"));
    }

    validate_install_basename(Path::new(".db.restore-stage-123"))
        .expect("one relative basename is valid");
}

#[tokio::test]
async fn restart_resumes_from_staged_validated() -> anyhow::Result<()> {
    let fixture = install_fixture().await?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::Installed);
    assert_eq!(read_authority(&fixture.target).await?, "new");
    assert!(!snapshot_install_marker_path(&fixture.target)?.exists());
    Ok(())
}

#[tokio::test]
async fn abandoning_partial_staged_install_removes_intent_before_cleanup() -> anyhow::Result<()> {
    let fixture = install_fixture().await?;
    std::fs::remove_file(
        fixture
            .intent
            .layout
            .staged_path()
            .join(ROOT_STORAGE_MANIFEST_FILE),
    )?;
    std::fs::remove_file(fixture.intent.layout.pending_snapshot_data_path())?;

    abandon_staged_install(&fixture.intent)?;

    assert_eq!(read_authority(&fixture.target).await?, "old");
    assert!(!snapshot_install_marker_path(&fixture.target)?.exists());
    assert!(!fixture.intent.layout.staged_path().exists());
    Ok(())
}

#[tokio::test]
async fn restart_resumes_from_marker_persisted() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    persist_storage_paused(&mut fixture)?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::MarkerPersisted)?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::Installed);
    assert_eq!(read_authority(&fixture.target).await?, "new");
    Ok(())
}

#[tokio::test]
async fn restart_resumes_after_storage_paused_before_marker_persisted() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    persist_storage_paused(&mut fixture)?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::Installed);
    assert_eq!(read_authority(&fixture.target).await?, "new");
    Ok(())
}

#[tokio::test]
async fn restart_restores_backup_after_old_renamed_before_new_promoted() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    persist_storage_paused(&mut fixture)?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::MarkerPersisted)?;
    rename_and_sync(
        fixture.intent.layout.target_path(),
        fixture.intent.layout.backup_path(),
    )?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::OldRenamedToBackup,
    )?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::RolledBack);
    assert_eq!(read_authority(&fixture.target).await?, "old");
    assert!(!snapshot_install_marker_path(&fixture.target)?.exists());
    Ok(())
}

#[tokio::test]
async fn rollback_pending_survives_restart_after_backup_restore() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    persist_storage_paused(&mut fixture)?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::MarkerPersisted)?;
    rename_and_sync(
        fixture.intent.layout.target_path(),
        fixture.intent.layout.backup_path(),
    )?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::OldRenamedToBackup,
    )?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::RollbackPending)?;
    rename_and_sync(
        fixture.intent.layout.backup_path(),
        fixture.intent.layout.target_path(),
    )?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::RolledBack);
    assert_eq!(read_authority(&fixture.target).await?, "old");
    assert!(!snapshot_install_marker_path(&fixture.target)?.exists());
    Ok(())
}

#[tokio::test]
async fn rollback_cleanup_pending_survives_partial_cleanup_restart() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    persist_storage_paused(&mut fixture)?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::MarkerPersisted)?;
    rename_and_sync(
        fixture.intent.layout.target_path(),
        fixture.intent.layout.backup_path(),
    )?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::OldRenamedToBackup,
    )?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::RollbackPending)?;
    rename_and_sync(
        fixture.intent.layout.backup_path(),
        fixture.intent.layout.target_path(),
    )?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::RollbackCleanupPending,
    )?;
    std::fs::remove_file(
        fixture
            .intent
            .layout
            .staged_path()
            .join(ROOT_STORAGE_MANIFEST_FILE),
    )?;
    std::fs::remove_file(fixture.intent.layout.pending_snapshot_data_path())?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::RolledBack);
    assert_eq!(read_authority(&fixture.target).await?, "old");
    assert!(!snapshot_install_marker_path(&fixture.target)?.exists());
    Ok(())
}

#[tokio::test]
async fn restart_resumes_after_new_renamed_to_target() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    persist_storage_paused(&mut fixture)?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::MarkerPersisted)?;
    rename_and_sync(
        fixture.intent.layout.target_path(),
        fixture.intent.layout.backup_path(),
    )?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::OldRenamedToBackup,
    )?;
    rename_and_sync(
        fixture.intent.layout.staged_path(),
        fixture.intent.layout.target_path(),
    )?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::Installed);
    assert_eq!(read_authority(&fixture.target).await?, "new");
    Ok(())
}

#[tokio::test]
async fn marker_snapshot_metadata_digest_mismatch_fail_closed() -> anyhow::Result<()> {
    let fixture = install_fixture().await?;
    std::fs::write(
        fixture.intent.layout.pending_checkpoint_meta_path(),
        b"tampered checkpoint metadata",
    )?;

    let error = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await
    .expect_err("tampered checkpoint metadata must block recovery");

    assert!(
        error
            .to_string()
            .contains("checkpoint metadata digest mismatch")
    );
    assert_eq!(read_authority(&fixture.target).await?, "old");
    assert!(snapshot_install_marker_path(&fixture.target)?.exists());
    Ok(())
}

#[tokio::test]
async fn marker_root_or_instance_manifest_digest_mismatch_fail_closed() -> anyhow::Result<()> {
    for relative in [
        PathBuf::from(ROOT_STORAGE_MANIFEST_FILE),
        Path::new("0").join(STORAGE_MANIFEST_FILE),
    ] {
        let fixture = install_fixture().await?;
        let path = fixture.intent.layout.staged_path().join(&relative);
        let mut bytes = std::fs::read(&path)?;
        bytes.push(b'\n');
        std::fs::write(&path, bytes)?;

        let error = recover_snapshot_install(
            &fixture.target,
            &fixture.snapshot_work_dir,
            Arc::clone(&fixture.options),
        )
        .await
        .expect_err("tampered storage manifest must block recovery");
        assert!(error.to_string().contains("manifest"));
        assert_eq!(read_authority(&fixture.target).await?, "old");
    }
    Ok(())
}

#[tokio::test]
async fn target_and_backup_both_present_with_digest_mismatch_fail_closed() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    drive_to_new_renamed(&mut fixture)?;
    create_storage(fixture.intent.layout.backup_path(), b"tampered-old").await?;

    let error = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await
    .expect_err("backup content drift must block authority selection");

    assert!(error.to_string().contains("storage digest mismatch"));
    assert!(fixture.intent.layout.backup_path().exists());
    assert!(snapshot_install_marker_path(&fixture.target)?.exists());
    Ok(())
}

#[tokio::test]
async fn restart_reopens_and_verifies_new_storage_before_publication() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    drive_to_new_renamed(&mut fixture)?;
    create_storage(&fixture.target, b"tampered-new").await?;

    let error = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await
    .expect_err("new storage drift must be rejected before snapshot publication");

    assert!(error.to_string().contains("storage digest mismatch"));
    assert!(
        !fixture
            .snapshot_work_dir
            .join(CURRENT_SNAPSHOT_DATA)
            .exists()
    );
    assert!(
        !fixture
            .snapshot_work_dir
            .join(CURRENT_SNAPSHOT_META)
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn restart_persists_raft_metadata_before_cleanup() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    drive_to_new_renamed(&mut fixture)?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::NewStorageReopened,
    )?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::Installed);
    assert!(
        fixture
            .snapshot_work_dir
            .join(CURRENT_SNAPSHOT_DATA)
            .is_file()
    );
    assert!(
        fixture
            .snapshot_work_dir
            .join(CURRENT_SNAPSHOT_META)
            .is_file()
    );
    assert!(!fixture.intent.layout.backup_path().exists());
    Ok(())
}

#[tokio::test]
async fn cleanup_never_runs_before_new_storage_raft_metadata_and_current_snapshot_are_durable()
-> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    drive_to_new_renamed(&mut fixture)?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::NewStorageReopened,
    )?;

    let error = complete_pending_cleanup(&fixture.intent)
        .expect_err("cleanup must require durable current snapshot files");
    assert!(error.to_string().contains("current snapshot archive"));
    assert!(fixture.intent.layout.backup_path().exists());
    Ok(())
}

#[tokio::test]
async fn cleanup_pending_survives_restart_and_completes_idempotently() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    drive_to_new_renamed(&mut fixture)?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::NewStorageReopened,
    )?;
    publish_pending_current_snapshot(&fixture.intent)?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::RaftMetadataPersisted,
    )?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::CleanupPending)?;
    complete_pending_cleanup(&fixture.intent)?;
    assert!(!fixture.intent.layout.backup_path().exists());

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::Installed);
    assert_eq!(read_authority(&fixture.target).await?, "new");
    assert!(!snapshot_install_marker_path(&fixture.target)?.exists());
    Ok(())
}

#[tokio::test]
async fn cleanup_pending_survives_partial_backup_deletion_restart() -> anyhow::Result<()> {
    let mut fixture = install_fixture().await?;
    drive_to_new_renamed(&mut fixture)?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::NewStorageReopened,
    )?;
    publish_pending_current_snapshot(&fixture.intent)?;
    persist_phase(
        &mut fixture.intent,
        SnapshotInstallPhase::RaftMetadataPersisted,
    )?;
    persist_phase(&mut fixture.intent, SnapshotInstallPhase::CleanupPending)?;
    std::fs::remove_file(
        fixture
            .intent
            .layout
            .backup_path()
            .join(ROOT_STORAGE_MANIFEST_FILE),
    )?;

    let decision = recover_snapshot_install(
        &fixture.target,
        &fixture.snapshot_work_dir,
        Arc::clone(&fixture.options),
    )
    .await?;

    assert_eq!(decision, SnapshotInstallRecoveryDecision::Installed);
    assert_eq!(read_authority(&fixture.target).await?, "new");
    assert!(!fixture.intent.layout.backup_path().exists());
    assert!(!snapshot_install_marker_path(&fixture.target)?.exists());
    Ok(())
}
