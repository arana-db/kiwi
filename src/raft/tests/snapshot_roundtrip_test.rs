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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use raft::state_machine::KiwiStateMachine;
use storage::{StorageOptions, storage::Storage};
use storage::{safe_cleanup_test_db, unique_test_db_path};

async fn close_storage(storage: Arc<Storage>, name: &str) -> anyhow::Result<()> {
    let mut storage = Arc::try_unwrap(storage)
        .map_err(|_| anyhow::Error::msg([name, " still has Arc references"].concat()))?;
    storage.shutdown().await;
    storage.close();
    Ok(())
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

    let cur = sm
        .get_current_snapshot()
        .await?
        .expect("OpenRaft requires current snapshot after build");
    assert_eq!(cur.meta, snap.meta);

    storage.set(b"k_l2", b"after")?;
    assert_eq!(storage.get(b"k_l2")?, "after");

    drop(builder);
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
