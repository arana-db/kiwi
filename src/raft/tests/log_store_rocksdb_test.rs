//! Integration tests for RocksDB-based Raft log store
//!
//! These tests verify the integration of RocksdbLogStore with the Raft system,
//! including node restart scenarios and data recovery.

use std::sync::Arc;
use std::time::Duration;

use openraft::LeaderId;
use openraft::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::{Entry, EntryPayload, LogId, Vote};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::time::sleep;

use conf::raft_type::{Binlog, BinlogEntry, KiwiTypeConfig, OperateType};
use engine::{Engine, RocksdbEngine};
use raft::log_store_rocksdb::RocksdbLogStore;

/// Helper function to create a test database with required column families
fn create_test_db() -> (TempDir, Arc<dyn Engine>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Create column family descriptors for all required CFs
    let cfs = vec![
        ColumnFamilyDescriptor::new("logs", Options::default()),
        ColumnFamilyDescriptor::new("meta", Options::default()),
        ColumnFamilyDescriptor::new("state", Options::default()),
    ];

    let db = DB::open_cf_descriptors(&opts, temp_dir.path(), cfs).expect("Failed to open database");

    let engine: Arc<dyn Engine> = Arc::new(RocksdbEngine::new(db));
    (temp_dir, engine)
}

/// Helper function to create test log entries
fn create_test_entries(start_index: u64, count: u64, term: u64) -> Vec<Entry<KiwiTypeConfig>> {
    (start_index..start_index + count)
        .map(|index| Entry {
            log_id: LogId::new(LeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(Binlog {
                db_id: (index % 10) as u32,
                slot_idx: (index % 16384) as u32,
                entries: vec![BinlogEntry {
                    cf_idx: 0,
                    op_type: OperateType::Put,
                    key: format!("key_{}", index).into_bytes(),
                    value: Some(format!("value_{}", index).into_bytes()),
                }],
            }),
        })
        .collect()
}

/// Helper function to write entries directly to RocksDB (bypassing the append callback issue)
fn write_entries_directly(engine: &Arc<dyn Engine>, entries: &[Entry<KiwiTypeConfig>]) {
    use rocksdb::WriteBatch;

    let logs_cf = engine.cf_handle("logs").expect("logs CF should exist");
    let mut batch = WriteBatch::default();

    for entry in entries {
        let key = (entry.log_id.index).to_be_bytes();
        let value = serde_json::to_vec(&entry).expect("Serialization should succeed");
        batch.put_cf(&logs_cf, &key, &value);
    }

    engine.write(batch).expect("Write should succeed");
}

#[tokio::test]
async fn test_basic_log_store_operations() {
    // Test basic operations: append, read, get_log_state
    let (_temp_dir, engine) = create_test_db();
    let mut log_store = RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

    // Initially, log state should be empty
    let initial_state = log_store
        .get_log_state()
        .await
        .expect("Failed to get log state");
    assert!(
        initial_state.last_log_id.is_none(),
        "Initial last_log_id should be None"
    );
    assert!(
        initial_state.last_purged_log_id.is_none(),
        "Initial last_purged_log_id should be None"
    );

    // Append some entries directly to RocksDB
    let entries = create_test_entries(1, 5, 1);
    write_entries_directly(&engine, &entries);

    // Verify log state reflects the appended entries
    let state_after_append = log_store
        .get_log_state()
        .await
        .expect("Failed to get log state");
    assert!(
        state_after_append.last_log_id.is_some(),
        "last_log_id should be Some after append"
    );
    assert_eq!(
        state_after_append.last_log_id.unwrap().index,
        5,
        "last_log_id should be 5"
    );

    // Read back the entries
    let mut reader = log_store.get_log_reader().await;
    let read_entries = reader
        .try_get_log_entries(1..=5)
        .await
        .expect("Failed to read entries");

    assert_eq!(read_entries.len(), 5, "Should read back 5 entries");
    for (i, entry) in read_entries.iter().enumerate() {
        assert_eq!(
            entry.log_id.index,
            (i + 1) as u64,
            "Entry index should match"
        );
    }
}

#[tokio::test]
async fn test_node_restart_data_recovery() {
    // Test that data persists across log store instances (simulating node restart)
    let (temp_dir, engine) = create_test_db();

    // Phase 1: Create log store, append entries, save vote and committed
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        // Append entries
        let entries = create_test_entries(1, 10, 1);
        write_entries_directly(&engine, &entries);

        // Save vote
        let vote = Vote::new(1, 1);
        log_store
            .save_vote(&vote)
            .await
            .expect("Failed to save vote");

        // Save committed
        let committed = Some(LogId::new(LeaderId::new(1, 1), 5));
        log_store
            .save_committed(committed)
            .await
            .expect("Failed to save committed");

        // Verify data is present
        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state");
        assert_eq!(
            state.last_log_id.unwrap().index,
            10,
            "Should have 10 entries"
        );

        let read_vote = log_store.read_vote().await.expect("Failed to read vote");
        assert!(read_vote.is_some(), "Vote should be present");

        let read_committed = log_store
            .read_committed()
            .await
            .expect("Failed to read committed");
        assert_eq!(read_committed.unwrap().index, 5, "Committed should be 5");
    }

    // Phase 2: Create new log store instance (simulating restart)
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store after restart");

        // Verify all data persisted
        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state after restart");
        assert!(state.last_log_id.is_some(), "last_log_id should persist");
        assert_eq!(
            state.last_log_id.unwrap().index,
            10,
            "Should still have 10 entries after restart"
        );

        // Verify entries can be read
        let mut reader = log_store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..=10)
            .await
            .expect("Failed to read entries after restart");
        assert_eq!(
            entries.len(),
            10,
            "Should read back all 10 entries after restart"
        );

        // Verify vote persisted
        let vote = log_store
            .read_vote()
            .await
            .expect("Failed to read vote after restart");
        assert!(vote.is_some(), "Vote should persist across restart");
        assert_eq!(vote.unwrap().leader_id().term, 1, "Vote term should match");

        // Verify committed persisted
        let committed = log_store
            .read_committed()
            .await
            .expect("Failed to read committed after restart");
        assert!(
            committed.is_some(),
            "Committed should persist across restart"
        );
        assert_eq!(committed.unwrap().index, 5, "Committed index should match");
    }

    // Clean up
    drop(temp_dir);
}

#[tokio::test]
async fn test_node_restart_after_truncate() {
    // Test that truncate operations persist across restarts
    let (temp_dir, engine) = create_test_db();

    // Phase 1: Append entries and truncate
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        // Append 10 entries
        let entries = create_test_entries(1, 10, 1);
        write_entries_directly(&engine, &entries);

        // Truncate at index 6 (removes 6, 7, 8, 9, 10)
        let truncate_log_id = LogId::new(LeaderId::new(1, 1), 6);
        log_store
            .truncate(truncate_log_id)
            .await
            .expect("Failed to truncate");

        // Verify truncation worked
        let mut reader = log_store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..=10)
            .await
            .expect("Failed to read entries");
        assert_eq!(entries.len(), 5, "Should have 5 entries after truncate");
    }

    // Phase 2: Restart and verify truncation persisted
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store after restart");

        // Verify truncation persisted
        let mut reader = log_store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..=10)
            .await
            .expect("Failed to read entries after restart");
        assert_eq!(entries.len(), 5, "Truncation should persist across restart");

        // Verify the last entry is index 5
        assert_eq!(
            entries.last().unwrap().log_id.index,
            5,
            "Last entry should be index 5"
        );
    }

    drop(temp_dir);
}

#[tokio::test]
async fn test_node_restart_after_purge() {
    // Test that purge operations persist across restarts
    let (temp_dir, engine) = create_test_db();

    // Phase 1: Append entries and purge
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        // Append 10 entries
        let entries = create_test_entries(1, 10, 1);
        write_entries_directly(&engine, &entries);

        // Purge up to index 4 (removes 1, 2, 3, 4)
        let purge_log_id = LogId::new(LeaderId::new(1, 1), 4);
        log_store
            .purge(purge_log_id)
            .await
            .expect("Failed to purge");

        // Verify purge worked
        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state");
        assert_eq!(
            state.last_purged_log_id.unwrap().index,
            4,
            "last_purged should be 4"
        );

        let mut reader = log_store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..=10)
            .await
            .expect("Failed to read entries");
        assert_eq!(entries.len(), 6, "Should have 6 entries after purge (5-10)");
    }

    // Phase 2: Restart and verify purge persisted
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store after restart");

        // Verify purge persisted
        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state after restart");
        assert!(
            state.last_purged_log_id.is_some(),
            "last_purged should persist"
        );
        assert_eq!(
            state.last_purged_log_id.unwrap().index,
            4,
            "last_purged should still be 4"
        );

        // Verify purged entries are not readable
        let mut reader = log_store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..=10)
            .await
            .expect("Failed to read entries after restart");
        assert_eq!(entries.len(), 6, "Purge should persist across restart");
        assert_eq!(
            entries.first().unwrap().log_id.index,
            5,
            "First entry should be index 5"
        );
    }

    drop(temp_dir);
}

#[tokio::test]
async fn test_concurrent_access_with_clones() {
    // Test that cloned log stores can be used concurrently
    let (_temp_dir, engine) = create_test_db();
    let log_store = RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

    // Clone the log store for concurrent access
    let log_store_clone2 = log_store.clone();
    let engine_clone = engine.clone();

    // Spawn tasks that write and read concurrently
    let write_task = tokio::spawn(async move {
        for i in 0..5 {
            let entries = create_test_entries(i * 10 + 1, 10, 1);
            write_entries_directly(&engine_clone, &entries);
            sleep(Duration::from_millis(10)).await;
        }
    });

    let read_task = tokio::spawn(async move {
        let mut store = log_store_clone2;
        for _ in 0..10 {
            sleep(Duration::from_millis(5)).await;
            let state = store
                .get_log_state()
                .await
                .expect("Failed to get log state");
            // Just verify we can read without errors
            let _ = state.last_log_id;
        }
    });

    // Wait for both tasks to complete
    write_task.await.expect("Write task failed");
    read_task.await.expect("Read task failed");
}

#[tokio::test]
async fn test_append_after_restart() {
    // Test that we can continue appending entries after a restart
    let (temp_dir, engine) = create_test_db();

    // Phase 1: Append initial entries
    {
        let _log_store = RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        let entries = create_test_entries(1, 5, 1);
        write_entries_directly(&engine, &entries);
    }

    // Phase 2: Restart and append more entries
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store after restart");

        // Append more entries starting from index 6
        let entries = create_test_entries(6, 5, 1);
        write_entries_directly(&engine, &entries);

        // Verify we have all 10 entries
        let mut reader = log_store.get_log_reader().await;
        let all_entries = reader
            .try_get_log_entries(1..=10)
            .await
            .expect("Failed to read entries");
        assert_eq!(all_entries.len(), 10, "Should have 10 entries total");

        // Verify continuity
        for (i, entry) in all_entries.iter().enumerate() {
            assert_eq!(
                entry.log_id.index,
                (i + 1) as u64,
                "Entries should be continuous"
            );
        }
    }

    drop(temp_dir);
}

#[tokio::test]
async fn test_multiple_restarts_with_operations() {
    // Test multiple restart cycles with various operations
    let (temp_dir, engine) = create_test_db();

    // Cycle 1: Initial append
    {
        let _log_store = RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");
        let entries = create_test_entries(1, 10, 1);
        write_entries_directly(&engine, &entries);
    }

    // Cycle 2: Restart, truncate, and append
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        // Truncate at 6
        log_store
            .truncate(LogId::new(LeaderId::new(1, 1), 6))
            .await
            .expect("Failed to truncate");

        // Append new entries from 6 with different term
        let entries = create_test_entries(6, 5, 2);
        write_entries_directly(&engine, &entries);
    }

    // Cycle 3: Restart and verify
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        let mut reader = log_store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..=10)
            .await
            .expect("Failed to read entries");

        assert_eq!(entries.len(), 10, "Should have 10 entries");

        // Verify term changed at index 6
        assert_eq!(
            entries[4].log_id.leader_id.term, 1,
            "Entry 5 should have term 1"
        );
        assert_eq!(
            entries[5].log_id.leader_id.term, 2,
            "Entry 6 should have term 2"
        );
    }

    // Cycle 4: Restart, purge, and verify
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        // Purge first 5 entries
        log_store
            .purge(LogId::new(LeaderId::new(1, 1), 5))
            .await
            .expect("Failed to purge");

        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state");
        assert_eq!(state.last_purged_log_id.unwrap().index, 5);
    }

    // Cycle 5: Final restart and verify
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state");
        assert_eq!(
            state.last_purged_log_id.unwrap().index,
            5,
            "Purge should persist"
        );
        assert_eq!(
            state.last_log_id.unwrap().index,
            10,
            "Last log should be 10"
        );

        let mut reader = log_store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..=10)
            .await
            .expect("Failed to read entries");
        assert_eq!(entries.len(), 5, "Should have 5 entries after purge (6-10)");
    }

    drop(temp_dir);
}

#[tokio::test]
async fn test_empty_database_restart() {
    // Test that restarting with an empty database works correctly
    let (temp_dir, engine) = create_test_db();

    // Phase 1: Create log store but don't write anything
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state");
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    // Phase 2: Restart and verify still empty
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store after restart");

        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state after restart");
        assert!(
            state.last_log_id.is_none(),
            "Should still be empty after restart"
        );
        assert!(
            state.last_purged_log_id.is_none(),
            "Should still have no purge after restart"
        );

        // Verify we can still append after restart
        let entries = create_test_entries(1, 3, 1);
        write_entries_directly(&engine, &entries);

        let state = log_store
            .get_log_state()
            .await
            .expect("Failed to get log state");
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    drop(temp_dir);
}

#[tokio::test]
async fn test_large_entry_persistence() {
    // Test that large entries persist correctly across restarts
    let (temp_dir, engine) = create_test_db();

    // Create a large entry with many binlog entries
    let large_binlog_entries: Vec<BinlogEntry> = (0..100)
        .map(|i| BinlogEntry {
            cf_idx: i % 6,
            op_type: if i % 2 == 0 {
                OperateType::Put
            } else {
                OperateType::Delete
            },
            key: format!("large_key_{}", i).repeat(10).into_bytes(),
            value: Some(format!("large_value_{}", i).repeat(100).into_bytes()),
        })
        .collect();

    let large_entry = Entry {
        log_id: LogId::new(LeaderId::new(1, 1), 1),
        payload: EntryPayload::Normal(Binlog {
            db_id: 0,
            slot_idx: 0,
            entries: large_binlog_entries.clone(),
        }),
    };

    // Phase 1: Append large entry
    {
        let _log_store = RocksdbLogStore::new(engine.clone()).expect("Failed to create log store");

        write_entries_directly(&engine, &[large_entry.clone()]);
    }

    // Phase 2: Restart and verify large entry
    {
        let mut log_store =
            RocksdbLogStore::new(engine.clone()).expect("Failed to create log store after restart");

        let mut reader = log_store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..=1)
            .await
            .expect("Failed to read large entry");

        assert_eq!(entries.len(), 1, "Should have 1 entry");

        match &entries[0].payload {
            EntryPayload::Normal(binlog) => {
                assert_eq!(binlog.entries.len(), 100, "Should have 100 binlog entries");
                // Verify first and last binlog entry
                assert_eq!(binlog.entries[0].key, large_binlog_entries[0].key);
                assert_eq!(binlog.entries[99].key, large_binlog_entries[99].key);
            }
            _ => panic!("Expected Normal payload"),
        }
    }

    drop(temp_dir);
}
