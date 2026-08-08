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

mod support;

use std::sync::Arc;

use arc_swap::ArcSwap;
use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, LogId, SnapshotMeta, StoredMembership};
use raft::snapshot_archive::pack_dir_to_vec;
use raft::state_machine::{KiwiStateMachine, PauseController, StorageAccessPermit};
use storage::{
    InstanceStorageManifestV2, MigrationPhase, ParsedSnapshotMeta, RootStorageManifestV2,
    StorageOptions, prepare_classified_checkpoint_restore, recover_or_rollback_before_admission,
    storage::Storage,
};
use support::base_v1_snapshot::{HASH_FIELD, HASH_KEY, STRING_KEY, TTL_KEY, ZSET_KEY, ZSET_MEMBER};

async fn close_storage(
    mut storage: Storage,
    storage_rx: tokio::sync::mpsc::Receiver<storage::BgTask>,
) {
    storage.shutdown().await;
    storage.close();
    drop(storage_rx);
}

struct NoopPauseController;
struct NoopStorageAccessPermit;

impl StorageAccessPermit for NoopStorageAccessPermit {}

impl PauseController for NoopPauseController {
    fn request_pause(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn enter(
        self: Arc<Self>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>,
    > {
        Box::pin(async { Box::new(NoopStorageAccessPermit) as Box<dyn StorageAccessPermit> })
    }

    fn resume(&self) {}
}

#[tokio::test]
async fn base_v1_snapshot_restores_string_hash_zset_and_ttl_after_stage_migration()
-> anyhow::Result<()> {
    let checkpoint = tempfile::tempdir()?;
    support::base_v1_snapshot::create(checkpoint.path()).await?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("restored");
    let parsed = ParsedSnapshotMeta::read_from_dir(checkpoint.path())?;
    let prepared = prepare_classified_checkpoint_restore(
        checkpoint.path(),
        &target,
        1,
        &parsed,
        &StorageOptions::default(),
    )?;

    let mut staged = Storage::new(1, 0);
    let staged_rx = staged.open(Arc::new(StorageOptions::default()), prepared.staged_path())?;
    assert_eq!(staged.get(STRING_KEY)?, "string-value");
    assert_eq!(
        staged.hget(HASH_KEY, HASH_FIELD)?,
        Some("hash-value".to_string())
    );
    assert_eq!(
        staged.zscore(ZSET_KEY, ZSET_MEMBER)?,
        Some(b"42.5".to_vec())
    );
    let staged_ttl = staged.ttl(TTL_KEY)?;
    assert!((1..=3_600).contains(&staged_ttl));
    close_storage(staged, staged_rx).await;

    prepared.commit()?;
    let mut restored = Storage::new(1, 0);
    let restored_rx = restored.open(Arc::new(StorageOptions::default()), &target)?;
    assert_eq!(restored.get(STRING_KEY)?, "string-value");
    assert_eq!(
        restored.hget(HASH_KEY, HASH_FIELD)?,
        Some("hash-value".to_string())
    );
    assert_eq!(
        restored.zscore(ZSET_KEY, ZSET_MEMBER)?,
        Some(b"42.5".to_vec())
    );
    let restored_ttl = restored.ttl(TTL_KEY)?;
    assert!((1..=staged_ttl).contains(&restored_ttl));
    close_storage(restored, restored_rx).await;
    Ok(())
}

#[tokio::test]
async fn base_v1_snapshot_restore_generates_new_root_and_instance_incarnations()
-> anyhow::Result<()> {
    let checkpoint = tempfile::tempdir()?;
    let old = support::base_v1_snapshot::create(checkpoint.path()).await?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("restored");
    let parsed = ParsedSnapshotMeta::read_from_dir(checkpoint.path())?;
    let prepared = prepare_classified_checkpoint_restore(
        checkpoint.path(),
        &target,
        1,
        &parsed,
        &StorageOptions::default(),
    )?;

    let new_root = RootStorageManifestV2::read_from_dir(prepared.staged_path())?;
    let new_instance = InstanceStorageManifestV2::read_from_dir(&prepared.staged_path().join("0"))?;
    new_instance.validate_root_binding(0, &new_root)?;
    assert_ne!(new_root.manifest_id().to_string(), old.root_manifest_id);
    assert_ne!(new_instance.storage_incarnation(), old.storage_incarnation);
    Ok(())
}

#[tokio::test]
async fn staged_storage_is_closed_reopened_and_revalidated_before_install() -> anyhow::Result<()> {
    let checkpoint = tempfile::tempdir()?;
    support::base_v1_snapshot::create(checkpoint.path()).await?;
    let target_parent = tempfile::tempdir()?;
    let target = target_parent.path().join("restored");
    let snapshot_work_dir = tempfile::tempdir()?;
    let snapshot_bytes = pack_dir_to_vec(checkpoint.path())?;
    let snapshot_meta = SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(7, 1), 41)),
        last_membership: StoredMembership::default(),
        snapshot_id: "base-v1-compatibility".to_string(),
    };

    let storage_swap = Arc::new(ArcSwap::from_pointee(Storage::new(1, 0)));
    let mut state_machine = KiwiStateMachine::new(
        2,
        Arc::clone(&storage_swap),
        target.clone(),
        snapshot_work_dir.path().to_path_buf(),
        Arc::new(NoopPauseController),
        None,
    );
    state_machine
        .install_snapshot(
            &snapshot_meta,
            Box::new(std::io::Cursor::new(snapshot_bytes)),
        )
        .await?;

    let restored = storage_swap.load_full();
    assert_eq!(restored.get(STRING_KEY)?, "string-value");
    assert_eq!(
        restored.hget(HASH_KEY, HASH_FIELD)?,
        Some("hash-value".to_string())
    );
    assert_eq!(
        restored.zscore(ZSET_KEY, ZSET_MEMBER)?,
        Some(b"42.5".to_vec())
    );
    assert!((1..=3_600).contains(&restored.ttl(TTL_KEY)?));
    restored.set(b"post-install", b"accepted")?;

    drop(state_machine);
    drop(storage_swap);
    let mut restored = Arc::try_unwrap(restored)
        .map_err(|_| anyhow::anyhow!("restored Storage still has Arc owners"))?;
    restored.shutdown().await;
    restored.close();

    let closed_root = RootStorageManifestV2::read_from_dir(&target)?;
    assert_eq!(
        closed_root
            .migration()
            .expect("historical snapshot migration remains recorded")
            .phase,
        MigrationPhase::RollbackWindowClosed
    );
    let mut reopened = Storage::new(1, 0);
    let reopened_rx = reopened.open(Arc::new(StorageOptions::default()), &target)?;
    assert_eq!(reopened.get(b"post-install")?, "accepted");
    reopened.shutdown().await;
    reopened.close();
    drop(reopened_rx);
    let rollback_error =
        recover_or_rollback_before_admission(&target, 1, &StorageOptions::default())
            .expect_err("installed historical snapshot must not reopen a rollback window");
    assert!(rollback_error.to_string().contains("RollbackWindowClosed"));
    Ok(())
}
