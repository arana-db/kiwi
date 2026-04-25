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

//! Integration test for snapshot + logindex complete flow
//! Verifies that:
//! 1. on_binlog_write updates collector with (log_index, seqno) mappings
//! 2. build_snapshot saves collector state
//! 3. install_snapshot restores collector state on follower

use std::sync::Arc;

use arc_swap::ArcSwap;

use conf::raft_type::Binlog;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use raft::state_machine::KiwiStateMachine;
use storage::logindex::{LogIndexAndSequenceCollector, LogIndexOfColumnFamilies};
use storage::{RaftSnapshotMeta, StorageOptions, storage::Storage, unique_test_db_path};

#[tokio::test]
async fn test_snapshot_with_logindex_state() -> anyhow::Result<()> {
    let src_db_path = unique_test_db_path();
    let restore_db_path = unique_test_db_path();
    let snap_root = unique_test_db_path();

    std::fs::create_dir_all(&snap_root)?;

    // All source-side operations are in this block to ensure complete cleanup
    // before creating the target state machine (releases RocksDB file locks).
    let (meta, bytes) = {
        // Create source storage and write data
        let storage = {
            let mut s = Storage::new(1, 0);
            let options = Arc::new(StorageOptions::default());
            let _rx = s.open(options, &src_db_path)?;
            Arc::new(s)
        };

        // Use storage.set which uses the proper key format
        storage.set(b"test_key", b"test_value")?;

        // Force flush to ensure data is persisted to SST
        if let Some(inst) = storage.insts.first() {
            if let Some(ref db) = inst.db {
                db.flush().unwrap();
            }
        }

        // Simulate binlog write to update collector with (log_index, seqno) mapping
        // This simulates what happens during Raft replication
        {
            let binlog = Binlog {
                db_id: 0,
                slot_idx: 0,
                entries: vec![conf::raft_type::BinlogEntry {
                    cf_idx: 0,
                    op_type: conf::raft_type::OperateType::Put,
                    key: b"test_key".to_vec(),
                    value: Some(b"test_value".to_vec()),
                }],
            };

            const TEST_LOG_INDEX: u64 = 100;
            storage.on_binlog_write(&binlog, TEST_LOG_INDEX)?;
        } // binlog dropped here

        // Force flush after binlog write
        {
            let inst = storage.insts.first().unwrap();
            inst.db.as_ref().unwrap().flush().unwrap();
        } // inst reference dropped here

        // Verify collector has the (log_index, seqno) mapping
        let collector = storage.get_logindex_collector(0).unwrap();
        assert!(
            collector.size() > 0,
            "Collector should have recorded mappings"
        );

        // Create state machine and build snapshot
        let cf_tracker = storage.get_logindex_cf_tracker(0).unwrap();
        let storage_swap = Arc::new(ArcSwap::from(Arc::clone(&storage)));
        let mut sm = KiwiStateMachine::new(
            1,
            storage_swap,
            src_db_path.clone(),
            snap_root.clone(),
            collector.clone(),
            cf_tracker.clone(),
        );

        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await?;
        assert!(!snap.snapshot.get_ref().is_empty());

        // Verify snapshot was persisted
        let cur = sm
            .get_current_snapshot()
            .await?
            .expect("OpenRaft requires current snapshot after build");
        assert_eq!(cur.meta, snap.meta);

        // Modify source storage after snapshot
        storage.set(b"test_key", b"modified")?;
        assert_eq!(storage.get(b"test_key")?, "modified");

        // Extract snapshot data, then drop all source references
        let meta = snap.meta.clone();
        let bytes = snap.snapshot.into_inner();

        // Explicitly drop all source-side references before leaving this block
        drop(builder);
        drop(sm);
        drop(storage);

        (meta, bytes)
    }; // All source-side references dropped here, RocksDB locks should be released

    // Create target state machine and install snapshot
    let target_storage = Arc::new(Storage::new(1, 0));
    let target_swap = Arc::new(ArcSwap::from(target_storage));

    let target_collector = Arc::new(LogIndexAndSequenceCollector::new(0));
    let target_cf_tracker = Arc::new(LogIndexOfColumnFamilies::new());

    let mut sm2 = KiwiStateMachine::new(
        2,
        target_swap.clone(),
        restore_db_path.clone(),
        snap_root,
        target_collector.clone(),
        target_cf_tracker,
    );

    sm2.install_snapshot(&meta, Box::new(std::io::Cursor::new(bytes)))
        .await?;

    // Verify snapshot was installed
    let cur2 = sm2
        .get_current_snapshot()
        .await?
        .expect("OpenRaft requires current snapshot after install");
    assert_eq!(cur2.meta, meta);

    // Verify restored data using the Storage held by ArcSwap
    // (install_snapshot has already swapped in a new Storage with restored data)
    let restored = target_swap.load();
    assert_eq!(restored.get(b"test_key")?, "test_value");

    // Verify collector state was restored from snapshot.
    // After install_snapshot, self.collector points to the new storage's collector
    // (refreshed during the swap phase). Check the collector via target_swap to get
    // the new storage's collector which received restore_collector_state + purge.
    // After restore_collector_state + purge(last_included_index), the collector
    // is compacted to a single boundary entry at the snapshot's last_included_index.
    let restored_collector = restored
        .get_logindex_collector(0)
        .expect("restored storage should have collector");
    assert!(
        restored_collector.size() > 0,
        "Restored collector should have at least the purge boundary entry"
    );

    Ok(())
}

#[tokio::test]
async fn test_on_binlog_write_updates_collector() -> anyhow::Result<()> {
    let db_path = unique_test_db_path();

    let storage = {
        let mut s = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _rx = s.open(options, &db_path)?;
        Arc::new(s)
    };

    // Get initial collector state
    let collector = storage.get_logindex_collector(0).unwrap();
    let initial_size = collector.size();

    // Write binlog with log_index = 100
    let binlog = Binlog {
        db_id: 0,
        slot_idx: 0,
        entries: vec![conf::raft_type::BinlogEntry {
            cf_idx: 0,
            op_type: conf::raft_type::OperateType::Put,
            key: b"key1".to_vec(),
            value: Some(b"value1".to_vec()),
        }],
    };

    storage.on_binlog_write(&binlog, 100)?;

    // Collector should be updated (size may increase depending on step_length_mask)
    let size_after_first = collector.size();
    assert!(
        size_after_first >= initial_size,
        "Collector should be updated after binlog write"
    );

    // Write another binlog with log_index = 200
    let binlog2 = Binlog {
        db_id: 0,
        slot_idx: 0,
        entries: vec![conf::raft_type::BinlogEntry {
            cf_idx: 0,
            op_type: conf::raft_type::OperateType::Put,
            key: b"key2".to_vec(),
            value: Some(b"value2".to_vec()),
        }],
    };

    storage.on_binlog_write(&binlog2, 200)?;

    let size_after_second = collector.size();
    assert!(
        size_after_second >= size_after_first,
        "Collector should be updated after second binlog write"
    );

    // Verify we can find the log_index from seqno
    let exported = collector.export_state();
    assert!(
        !exported.is_empty(),
        "Collector should have recorded mappings"
    );

    // Parse the exported state to verify format
    for entry in &exported {
        let parts: Vec<&str> = entry.split(':').collect();
        assert_eq!(
            parts.len(),
            2,
            "Exported state format should be 'log_index:seqno'"
        );
        let log_index: i64 = parts[0].parse().expect("log_index should be parseable");
        let _seqno: u64 = parts[1].parse().expect("seqno should be parseable");
        assert!(log_index > 0, "log_index should be positive");
    }

    Ok(())
}

#[tokio::test]
async fn test_collector_state_export_restore() -> anyhow::Result<()> {
    // Create a collector and add some mappings
    let collector = Arc::new(LogIndexAndSequenceCollector::new(0));
    collector.update(100, 1000);
    collector.update(200, 2000);
    collector.update(300, 3000);

    // Export state
    let exported = collector.export_state();
    assert_eq!(exported.len(), 3, "Should export 3 mappings");

    // Create snapshot meta with collector state
    let meta = RaftSnapshotMeta::with_collector_state(
        300, // last_included_index
        1,   // last_included_term
        &collector,
    );

    // Verify collector state is stored
    assert_eq!(meta.logindex_collector_state.len(), 3);
    assert!(
        meta.logindex_collector_state
            .contains(&"100:1000".to_string())
    );
    assert!(
        meta.logindex_collector_state
            .contains(&"200:2000".to_string())
    );
    assert!(
        meta.logindex_collector_state
            .contains(&"300:3000".to_string())
    );

    // Create a new collector and restore state
    let new_collector = Arc::new(LogIndexAndSequenceCollector::new(0));
    meta.restore_collector_state(&new_collector);

    // Verify restored state
    let restored_exported = new_collector.export_state();
    assert_eq!(restored_exported.len(), 3, "Should restore 3 mappings");

    // Verify find_applied_log_index works on restored collector
    assert_eq!(new_collector.find_applied_log_index(1000), 100);
    assert_eq!(new_collector.find_applied_log_index(1500), 100);
    assert_eq!(new_collector.find_applied_log_index(2000), 200);
    assert_eq!(new_collector.find_applied_log_index(2500), 200);
    assert_eq!(new_collector.find_applied_log_index(3000), 300);

    Ok(())
}
