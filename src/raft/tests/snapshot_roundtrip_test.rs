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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use raft::state_machine::{
    KiwiStateMachine, PauseController, StorageAccessPermit, preflight_snapshot_install,
    snapshot_install_marker_path,
};
use storage::{StorageOptions, storage::Storage};
use storage::{safe_cleanup_test_db, unique_test_db_path};

async fn close_storage(storage: Arc<Storage>, name: &str) -> anyhow::Result<()> {
    let mut storage = Arc::try_unwrap(storage)
        .map_err(|_| anyhow::Error::msg([name, " still has Arc references"].concat()))?;
    storage.shutdown().await;
    storage.close();
    Ok(())
}

#[derive(Default)]
struct TestPauseController {
    paused: AtomicBool,
    active: AtomicUsize,
    enter_attempts: AtomicUsize,
    pause_count: AtomicUsize,
    resume_count: AtomicUsize,
    state_changed: tokio::sync::Notify,
}

struct TestStorageAccessPermit {
    controller: Arc<TestPauseController>,
}

impl StorageAccessPermit for TestStorageAccessPermit {}

impl Drop for TestStorageAccessPermit {
    fn drop(&mut self) {
        if self.controller.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.controller.state_changed.notify_waiters();
        }
    }
}

impl PauseController for TestPauseController {
    fn request_pause(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async {
            self.pause_count.fetch_add(1, Ordering::SeqCst);
            self.paused.store(true, Ordering::SeqCst);

            loop {
                let drained = self.state_changed.notified();
                tokio::pin!(drained);
                drained.as_mut().enable();

                if self.active.load(Ordering::SeqCst) == 0 {
                    return;
                }
                drained.await;
            }
        })
    }

    fn enter(
        self: Arc<Self>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>,
    > {
        Box::pin(async move {
            self.enter_attempts.fetch_add(1, Ordering::SeqCst);
            loop {
                while self.paused.load(Ordering::SeqCst) {
                    let resumed = self.state_changed.notified();
                    tokio::pin!(resumed);
                    resumed.as_mut().enable();

                    if !self.paused.load(Ordering::SeqCst) {
                        break;
                    }
                    resumed.await;
                }

                self.active.fetch_add(1, Ordering::SeqCst);
                if !self.paused.load(Ordering::SeqCst) {
                    return Box::new(TestStorageAccessPermit { controller: self })
                        as Box<dyn StorageAccessPermit>;
                }

                if self.active.fetch_sub(1, Ordering::SeqCst) == 1 {
                    self.state_changed.notify_waiters();
                }
            }
        })
    }

    fn resume(&self) {
        self.resume_count.fetch_add(1, Ordering::SeqCst);
        self.paused.store(false, Ordering::SeqCst);
        self.state_changed.notify_waiters();
    }
}

#[tokio::test]
async fn active_storage_access_permit_blocks_pause() {
    let controller = Arc::new(TestPauseController::default());
    let permit = Arc::clone(&controller).enter().await;

    let pause_controller = Arc::clone(&controller);
    let pause = tokio::spawn(async move {
        pause_controller.request_pause().await;
    });

    while !controller.paused.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(!pause.is_finished(), "pause must drain the active permit");

    drop(permit);
    pause.await.expect("pause task should not panic");
    assert_eq!(controller.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn paused_reader_never_observes_placeholder_storage() {
    let old_storage = Arc::new(Storage::new(1, 0));
    let placeholder = Arc::new(Storage::new(1, 1));
    let restored = Arc::new(Storage::new(1, 2));
    let storage_swap = Arc::new(ArcSwap::from(Arc::clone(&old_storage)));
    let controller = Arc::new(TestPauseController::default());

    controller.request_pause().await;
    storage_swap.swap(Arc::clone(&placeholder));

    let reader_controller = Arc::clone(&controller);
    let reader_swap = Arc::clone(&storage_swap);
    let reader = tokio::spawn(async move {
        let _permit = reader_controller.enter().await;
        reader_swap.load_full()
    });

    while controller.enter_attempts.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    assert!(
        !reader.is_finished(),
        "reader must remain blocked while paused"
    );

    storage_swap.swap(Arc::clone(&restored));
    assert!(
        !reader.is_finished(),
        "storage swap must not bypass the pause gate"
    );

    controller.resume();
    let observed = reader.await.expect("reader task should not panic");
    assert!(Arc::ptr_eq(&observed, &restored));
    assert!(!Arc::ptr_eq(&observed, &placeholder));
}

#[tokio::test]
async fn snapshot_builder_blocks_install_operation() {
    let db_path = unique_test_db_path();
    let snap_root = unique_test_db_path();
    std::fs::create_dir_all(&snap_root).expect("snapshot root should be created");

    let mut storage = Storage::new(1, 0);
    let options = Arc::new(StorageOptions::default());
    let _storage_rx = storage
        .open(options, &db_path)
        .expect("test storage should open");
    storage
        .set(b"snapshot-key", b"snapshot-value")
        .expect("test should write snapshot data");
    let target_swap = Arc::new(ArcSwap::from_pointee(storage));
    let controller = Arc::new(TestPauseController::default());
    let mut state_machine = KiwiStateMachine::new(
        1,
        Arc::clone(&target_swap),
        db_path.clone(),
        snap_root.clone(),
        None,
    );
    state_machine.set_pause_controller(controller.clone());

    let mut source_builder = state_machine.get_snapshot_builder().await;
    let snapshot = source_builder
        .build_snapshot()
        .await
        .expect("test snapshot should build");
    let snapshot_meta = snapshot.meta.clone();
    let snapshot_bytes = snapshot.snapshot.into_inner();
    drop(source_builder);

    let builder = state_machine.get_snapshot_builder().await;
    let install = tokio::spawn(async move {
        let result = state_machine
            .install_snapshot(
                &snapshot_meta,
                Box::new(std::io::Cursor::new(snapshot_bytes)),
            )
            .await;
        (state_machine, result, snapshot_meta)
    });

    while controller.pause_count.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    assert!(
        !install.is_finished(),
        "install must wait until the snapshot builder releases its operation guard"
    );

    let mut builder = builder;
    let stale_snapshot = builder
        .build_snapshot()
        .await
        .expect("the old builder should complete before install enters its gate");
    assert_ne!(
        stale_snapshot.meta.snapshot_id, "snapshot-1",
        "the held builder must persist a distinct old current snapshot"
    );
    drop(stale_snapshot);
    drop(builder);
    let (mut state_machine, result, installed_meta) =
        install.await.expect("install task should not panic");
    result.expect("valid install should complete after the builder guard drains");
    let current = state_machine
        .get_current_snapshot()
        .await
        .expect("current snapshot read should succeed")
        .expect("install must leave a current snapshot");
    assert_eq!(
        current.meta, installed_meta,
        "a builder created before install must not overwrite the installed current snapshot"
    );

    drop(current);
    drop(state_machine);
    let restored = target_swap.load_full();
    drop(target_swap);
    close_storage(restored, "restored target storage")
        .await
        .expect("target storage should close");
    safe_cleanup_test_db(&db_path);
    safe_cleanup_test_db(&snap_root);
}

#[tokio::test]
async fn cursor_snapshot_roundtrip() -> anyhow::Result<()> {
    let src_db_path = unique_test_db_path();
    let restore_db_path = unique_test_db_path();
    let snap_root = unique_test_db_path();

    std::fs::create_dir_all(&snap_root)?;

    let storage = {
        let mut s = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _rx = s.open(options, &src_db_path)?;
        Arc::new(s)
    };

    storage.set(b"k_l2", b"before")?;

    let storage_swap = Arc::new(ArcSwap::from(storage.clone()));

    let mut sm = KiwiStateMachine::new(
        1,
        storage_swap.clone(),
        src_db_path.clone(),
        snap_root.clone(),
        None,
    );

    let mut builder = sm.get_snapshot_builder().await;
    let snap = builder.build_snapshot().await?;
    assert!(!snap.snapshot.get_ref().is_empty());
    drop(builder);

    let cur = sm
        .get_current_snapshot()
        .await?
        .expect("OpenRaft requires current snapshot after build");
    assert_eq!(cur.meta, snap.meta);

    storage.set(b"k_l2", b"after")?;
    assert_eq!(storage.get(b"k_l2")?, "after");

    drop(sm);
    drop(storage_swap);
    close_storage(storage, "source storage").await?;

    let meta = snap.meta.clone();
    let bytes = snap.snapshot.into_inner();
    // Create target storage but do NOT open it yet - this is expected because
    // install_snapshot will restore the checkpoint directly to db_path, bypassing
    // the normal open flow. The storage is opened after install_snapshot completes.
    let target_storage = Arc::new(Storage::new(1, 0));
    let target_swap = Arc::new(ArcSwap::from(target_storage.clone()));
    let mut sm2 = KiwiStateMachine::new(
        2,
        target_swap.clone(),
        restore_db_path.clone(),
        snap_root.clone(),
        None,
    );
    sm2.install_snapshot(&meta, Box::new(std::io::Cursor::new(bytes)))
        .await?;

    let cur2 = sm2
        .get_current_snapshot()
        .await?
        .expect("OpenRaft requires current snapshot after install");
    assert_eq!(cur2.meta, meta);

    // Verify restored data using the Storage held by ArcSwap.
    // The install_snapshot has already swapped in a new Storage with restored data.
    let restored = target_swap.load_full();
    assert_eq!(restored.get(b"k_l2")?, "before");

    drop(sm2);
    drop(target_swap);
    close_storage(restored, "restored storage").await?;
    close_storage(target_storage, "target placeholder storage").await?;

    safe_cleanup_test_db(&src_db_path);
    safe_cleanup_test_db(&restore_db_path);
    safe_cleanup_test_db(&snap_root);

    Ok(())
}

#[tokio::test]
async fn install_snapshot_with_existing_data() -> anyhow::Result<()> {
    let src_db_path = unique_test_db_path();
    let restore_db_path = unique_test_db_path();
    let snap_root = unique_test_db_path();

    std::fs::create_dir_all(&snap_root)?;

    // Create source storage and populate it with initial data
    let source_storage = {
        let mut s = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _rx = s.open(options, &src_db_path)?;
        Arc::new(s)
    };

    source_storage.set(b"key1", b"value1")?;
    source_storage.set(b"key2", b"value2")?;

    // Build snapshot from source
    let source_swap = Arc::new(ArcSwap::from(source_storage.clone()));
    let mut sm_source = KiwiStateMachine::new(
        1,
        source_swap.clone(),
        src_db_path.clone(),
        snap_root.clone(),
        None,
    );

    let mut builder = sm_source.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await?;
    assert!(!snapshot.snapshot.get_ref().is_empty());

    let snapshot_meta = snapshot.meta.clone();
    let snapshot_bytes = snapshot.snapshot.into_inner();

    // Drop source resources before creating target
    drop(builder);
    drop(sm_source);
    drop(source_swap);
    close_storage(source_storage, "source storage").await?;

    // Create old data directory structure directly without opening Storage.
    // This simulates a Follower that has existing stale data.
    let old_data_dir = restore_db_path.join("0");
    std::fs::create_dir_all(&old_data_dir)?;
    std::fs::write(old_data_dir.join("marker_old_data"), b"stale")?;

    // Install the snapshot - this should REPLACE the old data.
    let target_storage = Arc::new(Storage::new(1, 0));
    let target_swap = Arc::new(ArcSwap::from(target_storage.clone()));
    let mut sm_target = KiwiStateMachine::new(
        2,
        target_swap.clone(),
        restore_db_path.clone(),
        snap_root.clone(),
        None,
    );

    sm_target
        .install_snapshot(
            &snapshot_meta,
            Box::new(std::io::Cursor::new(snapshot_bytes)),
        )
        .await?;

    // Verify snapshot data using the Storage held by ArcSwap.
    // The install_snapshot has already swapped in a new Storage with restored data.
    let restored = target_swap.load_full();
    assert_eq!(restored.get(b"key1")?, "value1");
    assert_eq!(restored.get(b"key2")?, "value2");

    drop(sm_target);
    drop(target_swap);
    close_storage(restored, "restored storage").await?;
    close_storage(target_storage, "target placeholder storage").await?;

    safe_cleanup_test_db(&src_db_path);
    safe_cleanup_test_db(&restore_db_path);
    safe_cleanup_test_db(&snap_root);

    Ok(())
}

#[tokio::test]
async fn install_snapshot_rearms_append_log_hook() -> anyhow::Result<()> {
    let src_db_path = unique_test_db_path();
    let restore_db_path = unique_test_db_path();
    let snap_root = unique_test_db_path();

    std::fs::create_dir_all(&snap_root)?;

    let source_storage = {
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _rx = storage.open(options, &src_db_path)?;
        Arc::new(storage)
    };
    source_storage.set(b"snap_key", b"snap_value")?;

    let source_swap = Arc::new(ArcSwap::from(source_storage.clone()));
    let mut source_sm = KiwiStateMachine::new(
        1,
        source_swap.clone(),
        src_db_path.clone(),
        snap_root.clone(),
        None,
    );

    let mut builder = source_sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await?;
    let snapshot_meta = snapshot.meta.clone();
    let snapshot_bytes = snapshot.snapshot.into_inner();

    drop(builder);
    drop(source_sm);
    drop(source_swap);
    close_storage(source_storage, "source storage").await?;

    let hook_called = Arc::new(AtomicBool::new(false));
    let hook_called_clone = hook_called.clone();
    let append_log_fn: storage::AppendLogFn = Arc::new(move |_binlog| {
        hook_called_clone.store(true, Ordering::SeqCst);
        Ok(conf::raft_type::BinlogResponse::ok())
    });
    let append_log_fn_holder = Arc::new(OnceLock::new());
    let _ = append_log_fn_holder.set(append_log_fn);

    let target_storage = Arc::new(Storage::new(1, 0));
    let target_swap = Arc::new(ArcSwap::from(target_storage.clone()));
    let mut target_sm = KiwiStateMachine::new(
        2,
        target_swap.clone(),
        restore_db_path.clone(),
        snap_root.clone(),
        Some(append_log_fn_holder),
    );

    target_sm
        .install_snapshot(
            &snapshot_meta,
            Box::new(std::io::Cursor::new(snapshot_bytes)),
        )
        .await?;

    let restored = target_swap.load_full();
    assert_eq!(restored.get(b"snap_key")?, "snap_value");
    restored.set(b"after_snapshot", b"goes_through_hook")?;
    assert!(
        hook_called.load(Ordering::SeqCst),
        "restored storage should be re-armed with append_log_fn"
    );

    drop(target_sm);
    drop(target_swap);
    close_storage(restored, "restored storage").await?;
    close_storage(target_storage, "target placeholder storage").await?;

    safe_cleanup_test_db(&src_db_path);
    safe_cleanup_test_db(&restore_db_path);
    safe_cleanup_test_db(&snap_root);

    Ok(())
}

#[tokio::test]
async fn install_snapshot_replaces_open_target_storage() -> anyhow::Result<()> {
    let src_db_path = unique_test_db_path();
    let restore_db_path = unique_test_db_path();
    let snap_root = unique_test_db_path();

    std::fs::create_dir_all(&snap_root)?;

    let source_storage = {
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _rx = storage.open(options, &src_db_path)?;
        Arc::new(storage)
    };
    source_storage.set(b"snapshot_key", b"snapshot_value")?;

    let source_swap = Arc::new(ArcSwap::from(source_storage.clone()));
    let mut source_sm = KiwiStateMachine::new(
        1,
        source_swap.clone(),
        src_db_path.clone(),
        snap_root.clone(),
        None,
    );
    let mut builder = source_sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await?;
    let snapshot_meta = snapshot.meta.clone();
    let snapshot_bytes = snapshot.snapshot.into_inner();

    drop(builder);
    drop(source_sm);
    drop(source_swap);
    close_storage(source_storage, "source storage").await?;

    let mut target_storage = Storage::new(1, 0);
    let options = Arc::new(StorageOptions::default());
    let _target_rx = target_storage.open(options, &restore_db_path)?;
    target_storage.set(b"stale_key", b"stale_value")?;

    let target_swap = Arc::new(ArcSwap::from_pointee(target_storage));
    let pause_controller = Arc::new(TestPauseController::default());
    let mut target_sm = KiwiStateMachine::new(
        2,
        target_swap.clone(),
        restore_db_path.clone(),
        snap_root.clone(),
        None,
    );
    target_sm.set_pause_controller(pause_controller.clone());

    target_sm
        .install_snapshot(
            &snapshot_meta,
            Box::new(std::io::Cursor::new(snapshot_bytes)),
        )
        .await?;

    assert!(
        !pause_controller.paused.load(Ordering::SeqCst),
        "successful install must resume storage access"
    );
    assert_eq!(pause_controller.pause_count.load(Ordering::SeqCst), 1);
    assert_eq!(pause_controller.resume_count.load(Ordering::SeqCst), 1);
    assert!(
        !snapshot_install_marker_path(&restore_db_path)?.exists(),
        "successful install must remove its durable recovery marker"
    );
    let restored = target_swap.load_full();
    assert_eq!(restored.get(b"snapshot_key")?, "snapshot_value");
    assert!(restored.get(b"stale_key").is_err());

    drop(target_sm);
    drop(target_swap);
    close_storage(restored, "restored storage").await?;
    drop(pause_controller);
    drop(_target_rx);

    let reopened = {
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _rx = storage.open(options, &restore_db_path)?;
        Arc::new(storage)
    };
    assert_eq!(reopened.get(b"snapshot_key")?, "snapshot_value");
    assert!(reopened.get(b"stale_key").is_err());

    let reopened_swap = Arc::new(ArcSwap::from(reopened.clone()));
    let mut reopened_sm = KiwiStateMachine::new(
        2,
        reopened_swap.clone(),
        restore_db_path.clone(),
        snap_root.clone(),
        None,
    );
    let (reopened_applied, reopened_membership) = reopened_sm.applied_state().await?;
    assert_eq!(reopened_applied, snapshot_meta.last_log_id);
    assert_eq!(reopened_membership, snapshot_meta.last_membership);
    let reopened_snapshot = reopened_sm
        .get_current_snapshot()
        .await?
        .expect("reopened state machine must load the installed current snapshot");
    assert_eq!(reopened_snapshot.meta, snapshot_meta);

    drop(reopened_snapshot);
    drop(reopened_sm);
    drop(reopened_swap);
    close_storage(reopened, "reopened restored storage").await?;

    safe_cleanup_test_db(&src_db_path);
    safe_cleanup_test_db(&restore_db_path);
    safe_cleanup_test_db(&snap_root);

    Ok(())
}

#[tokio::test]
async fn install_snapshot_stays_paused_after_destructive_failure() -> anyhow::Result<()> {
    let src_db_path = unique_test_db_path();
    let restore_db_path = unique_test_db_path();
    let snap_root = unique_test_db_path();
    let invalid_work_dir = snap_root.join("invalid-work-dir");

    std::fs::create_dir_all(&snap_root)?;

    let source_storage = {
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _rx = storage.open(options, &src_db_path)?;
        Arc::new(storage)
    };
    source_storage.set(b"snapshot_key", b"snapshot_value")?;

    let source_swap = Arc::new(ArcSwap::from(source_storage.clone()));
    let mut source_sm = KiwiStateMachine::new(
        1,
        source_swap.clone(),
        src_db_path.clone(),
        snap_root.clone(),
        None,
    );
    let mut builder = source_sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await?;
    let snapshot_meta = snapshot.meta.clone();
    let snapshot_bytes = snapshot.snapshot.into_inner();

    drop(builder);
    drop(source_sm);
    drop(source_swap);
    close_storage(source_storage, "source storage").await?;

    let mut target_storage = Storage::new(1, 0);
    let options = Arc::new(StorageOptions::default());
    let _target_rx = target_storage.open(options, &restore_db_path)?;
    target_storage.set(b"stale_key", b"stale_value")?;

    // Persisting the current snapshot happens after the target database has been
    // replaced and reopened. A directory at the data-file path forces that
    // post-restore step to fail deterministically on every supported platform.
    std::fs::create_dir_all(invalid_work_dir.join("current_snapshot.tar"))?;

    let target_swap = Arc::new(ArcSwap::from_pointee(target_storage));
    let pause_controller = Arc::new(TestPauseController::default());
    let mut target_sm = KiwiStateMachine::new(
        2,
        target_swap.clone(),
        restore_db_path.clone(),
        invalid_work_dir.clone(),
        None,
    );
    target_sm.set_pause_controller(pause_controller.clone());

    let result = target_sm
        .install_snapshot(
            &snapshot_meta,
            Box::new(std::io::Cursor::new(snapshot_bytes)),
        )
        .await;

    let error = result.expect_err("invalid snapshot work dir must fail install");
    assert!(
        error
            .to_string()
            .contains(&invalid_work_dir.display().to_string()),
        "post-restore error must identify the failing path: {error}"
    );
    assert!(
        pause_controller.paused.load(Ordering::SeqCst),
        "a failure after destructive restore must keep storage access paused"
    );
    assert_eq!(pause_controller.pause_count.load(Ordering::SeqCst), 1);
    assert_eq!(pause_controller.resume_count.load(Ordering::SeqCst), 0);

    let marker_path = snapshot_install_marker_path(&restore_db_path)?;
    assert!(
        marker_path.is_file(),
        "post-marker failure must retain {}",
        marker_path.display()
    );
    let marker_json: serde_json::Value = serde_json::from_slice(&std::fs::read(&marker_path)?)?;
    for field in [
        "version",
        "id",
        "index",
        "term",
        "db",
        "workdir",
        "instances",
    ] {
        assert!(
            marker_json.get(field).is_some(),
            "durable marker must contain {field}: {marker_json}"
        );
    }
    let restart_error = preflight_snapshot_install(&restore_db_path)
        .expect_err("a simulated restart must reject an incomplete snapshot install");
    assert!(
        restart_error
            .to_string()
            .contains(&marker_path.display().to_string()),
        "restart refusal must identify the marker path: {restart_error}"
    );
    assert!(
        restart_error.to_string().contains("new node ID"),
        "restart refusal must give the safe rejoin recovery path: {restart_error}"
    );

    let attempts_before = pause_controller.enter_attempts.load(Ordering::SeqCst);
    let blocked_controller = pause_controller.clone();
    let blocked_read = tokio::spawn(async move { blocked_controller.enter().await });
    while pause_controller.enter_attempts.load(Ordering::SeqCst) == attempts_before {
        tokio::task::yield_now().await;
    }
    assert!(
        !blocked_read.is_finished(),
        "new reads must remain blocked after a destructive install failure"
    );
    blocked_read.abort();
    let cancelled = blocked_read.await;
    assert!(cancelled.is_err(), "blocked read should be cancelled");
    assert_eq!(
        pause_controller.active.load(Ordering::SeqCst),
        0,
        "cancelling a blocked read must not leak an active permit"
    );

    let restored = target_swap.load_full();
    assert_eq!(restored.get(b"snapshot_key")?, "snapshot_value");
    assert!(restored.get(b"stale_key").is_err());

    drop(target_sm);
    drop(target_swap);
    close_storage(restored, "restored storage after failed install").await?;
    drop(pause_controller);
    drop(_target_rx);

    std::fs::remove_file(&marker_path)?;
    safe_cleanup_test_db(&src_db_path);
    safe_cleanup_test_db(&restore_db_path);
    safe_cleanup_test_db(&snap_root);

    Ok(())
}

#[tokio::test]
async fn install_snapshot_resumes_after_pre_restore_failure() -> anyhow::Result<()> {
    let restore_db_path = unique_test_db_path();
    let snap_root = unique_test_db_path();

    std::fs::create_dir_all(&snap_root)?;

    let mut target_storage = Storage::new(1, 0);
    let options = Arc::new(StorageOptions::default());
    let _target_rx = target_storage.open(options, &restore_db_path)?;
    target_storage.set(b"stale_key", b"stale_value")?;

    let target_swap = Arc::new(ArcSwap::from_pointee(target_storage));
    let pause_controller = Arc::new(TestPauseController::default());
    let mut target_sm = KiwiStateMachine::new(
        2,
        target_swap.clone(),
        restore_db_path.clone(),
        snap_root.clone(),
        None,
    );
    target_sm.set_pause_controller(pause_controller.clone());

    let snapshot_meta = openraft::SnapshotMeta::<u64, conf::raft_type::KiwiNode>::default();
    let result = target_sm
        .install_snapshot(
            &snapshot_meta,
            Box::new(std::io::Cursor::new(b"not a tar archive".to_vec())),
        )
        .await;

    assert!(result.is_err(), "invalid archive must fail before restore");
    assert!(
        !pause_controller.paused.load(Ordering::SeqCst),
        "a failure before pause must leave storage access running"
    );
    assert_eq!(pause_controller.pause_count.load(Ordering::SeqCst), 0);
    assert_eq!(pause_controller.resume_count.load(Ordering::SeqCst), 0);
    assert!(
        !snapshot_install_marker_path(&restore_db_path)?.exists(),
        "an archive validation failure must not create an install marker"
    );

    let unchanged = target_swap.load_full();
    assert_eq!(unchanged.get(b"stale_key")?, "stale_value");

    drop(target_sm);
    drop(target_swap);
    close_storage(unchanged, "unchanged target storage").await?;
    drop(pause_controller);
    drop(_target_rx);

    safe_cleanup_test_db(&restore_db_path);
    safe_cleanup_test_db(&snap_root);

    Ok(())
}

#[tokio::test]
async fn missing_checkpoint_instance_does_not_pause_or_replace_live_storage() -> anyhow::Result<()>
{
    let src_db_path = unique_test_db_path();
    let restore_db_path = unique_test_db_path();
    let source_work_dir = unique_test_db_path();
    let target_work_dir = unique_test_db_path();
    std::fs::create_dir_all(&source_work_dir)?;
    std::fs::create_dir_all(&target_work_dir)?;

    let source_storage = {
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _rx = storage.open(options, &src_db_path)?;
        Arc::new(storage)
    };
    source_storage.set(b"snapshot_key", b"snapshot_value")?;
    let source_swap = Arc::new(ArcSwap::from(source_storage.clone()));
    let mut source_sm = KiwiStateMachine::new(
        1,
        source_swap.clone(),
        src_db_path.clone(),
        source_work_dir.clone(),
        None,
    );
    let mut builder = source_sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await?;
    let snapshot_meta = snapshot.meta.clone();
    let snapshot_bytes = snapshot.snapshot.into_inner();
    drop(builder);
    drop(source_sm);
    drop(source_swap);
    close_storage(source_storage, "source storage").await?;

    let mut target_storage = Storage::new(2, 0);
    let options = Arc::new(StorageOptions::default());
    let _target_rx = target_storage.open(options, &restore_db_path)?;
    target_storage.set(b"stale_key", b"stale_value")?;
    let target_swap = Arc::new(ArcSwap::from_pointee(target_storage));
    let pause_controller = Arc::new(TestPauseController::default());
    let mut target_sm = KiwiStateMachine::new(
        2,
        target_swap.clone(),
        restore_db_path.clone(),
        target_work_dir.clone(),
        None,
    );
    target_sm.set_pause_controller(pause_controller.clone());

    let error = target_sm
        .install_snapshot(
            &snapshot_meta,
            Box::new(std::io::Cursor::new(snapshot_bytes)),
        )
        .await
        .expect_err("a checkpoint missing instance 1 must fail during prepare");
    assert!(
        error
            .to_string()
            .contains("missing checkpoint instance directory"),
        "unexpected prepare failure: {error}"
    );
    assert_eq!(pause_controller.pause_count.load(Ordering::SeqCst), 0);
    assert_eq!(pause_controller.resume_count.load(Ordering::SeqCst), 0);
    assert!(!snapshot_install_marker_path(&restore_db_path)?.exists());
    let unchanged = target_swap.load_full();
    assert_eq!(unchanged.get(b"stale_key")?, "stale_value");

    drop(target_sm);
    drop(target_swap);
    close_storage(unchanged, "unchanged two-instance target storage").await?;
    drop(pause_controller);
    drop(_target_rx);
    safe_cleanup_test_db(&src_db_path);
    safe_cleanup_test_db(&restore_db_path);
    safe_cleanup_test_db(&source_work_dir);
    safe_cleanup_test_db(&target_work_dir);
    Ok(())
}
