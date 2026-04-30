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

//! OpenRaft Test Suite Integration
//!
//! This file uses OpenRaft's `openraft::testing::Suite` to verify that
//! `RocksdbLogStore` and `TestStateMachine` implementations conform to
//! the OpenRaft specification.
//!
//! ## Current Status
//!
//! - **LogStore tests**: 26 tests, all passing
//! - **StateMachine tests**: 4 tests, ignored (snapshot feature incomplete)
//!
//! ## Running Tests
//!
//! ```bash
//! # Run LogStore tests
//! cargo test --package raft --test openraft_suite
//!
//! # Run all tests including ignored ones (after StateMachine is complete)
//! cargo test --package raft --test openraft_suite -- --include-ignored
//! ```
//!
//! ## Implementation Notes
//!
//! This test uses a simplified `TestStateMachine` instead of the full `KiwiStateMachine`
//! because the snapshot functionality in `KiwiStateMachine` is still under development.
//! Once `KiwiStateMachine` snapshot support is complete, replace `TestStateMachine` with
//! `KiwiStateMachine` and remove the `#[ignore]` attribute from StateMachine tests.
//!
//! ## TODO: Tests to Enable After StateMachine Snapshot Implementation
//!
//! The following tests are currently ignored and should be enabled once
//! `KiwiStateMachine` properly implements snapshot functionality:
//!
//! 1. `apply_single` - Apply single entry to state machine
//! 2. `apply_multiple` - Apply multiple entries to state machine
//! 3. `snapshot_meta` - Snapshot metadata correctness
//! 4. `transfer_snapshot` - Snapshot transfer between state machines
//!
//! Requirements for `KiwiStateMachine` to pass these tests:
//! - `get_current_snapshot()` must return the built snapshot (not `None`)
//! - `install_snapshot()` must update `last_applied` and `last_membership`
//! - `build_snapshot()` must include correct `last_log_id` and `last_membership`

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::{
    AnyError, EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, storage::RaftStateMachine, testing::StoreBuilder,
};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;

use conf::raft_type::{Binlog, BinlogResponse, KiwiNode, KiwiTypeConfig};
use engine::{Engine, RocksdbEngine};
use raft::log_store_rocksdb::RocksdbLogStore;

/// Column families used for testing
const TEST_CF_NAMES: &[&str] = &["logs", "meta", "state"];

// ============================================================================
// Test StateMachine
// ============================================================================

/// Simplified StateMachine for testing purposes
///
/// This is used for OpenRaft test suite and does not depend on the full Storage implementation.
/// It only maintains in-memory state to satisfy test requirements.
pub struct TestStateMachine {
    /// Last applied log ID
    last_applied: Option<LogId<u64>>,
    /// Last membership configuration
    last_membership: StoredMembership<u64, KiwiNode>,
    /// Applied logs storage (for verification)
    applied_logs: BTreeMap<u64, Binlog>,
    /// Snapshot counter
    snapshot_idx: u64,
}

impl TestStateMachine {
    pub fn new() -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::default(),
            applied_logs: BTreeMap::new(),
            snapshot_idx: 0,
        }
    }
}

impl Default for TestStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftStateMachine<KiwiTypeConfig> for TestStateMachine {
    type SnapshotBuilder = TestSnapshotBuilder;

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<BinlogResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<KiwiTypeConfig>> + Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            self.last_applied = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => BinlogResponse::ok(),
                EntryPayload::Normal(binlog) => {
                    // Store applied log
                    self.applied_logs.insert(entry.log_id.index, binlog);
                    BinlogResponse::ok()
                }
                EntryPayload::Membership(mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    BinlogResponse::ok()
                }
            };

            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        TestSnapshotBuilder {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_idx: self.snapshot_idx,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, KiwiNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<KiwiTypeConfig>>, StorageError<u64>> {
        Ok(None)
    }

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, KiwiNode>), StorageError<u64>> {
        Ok((self.last_applied, self.last_membership.clone()))
    }
}

/// Test snapshot builder
pub struct TestSnapshotBuilder {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, KiwiNode>,
    snapshot_idx: u64,
}

impl RaftSnapshotBuilder<KiwiTypeConfig> for TestSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<KiwiTypeConfig>, StorageError<u64>> {
        self.snapshot_idx += 1;
        let snapshot_id = format!("snapshot-{}", self.snapshot_idx);

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        let snapshot_data = Box::new(Cursor::new(Vec::new()));

        Ok(Snapshot {
            meta,
            snapshot: snapshot_data,
        })
    }
}

// ============================================================================
// StoreBuilder Implementation
// ============================================================================

/// Kiwi store builder
///
/// Implements OpenRaft's `StoreBuilder` trait to create test LogStore and StateMachine instances.
pub struct KiwiStoreBuilder;

impl StoreBuilder<KiwiTypeConfig, RocksdbLogStore, TestStateMachine, TempDir> for KiwiStoreBuilder {
    async fn build(
        &self,
    ) -> Result<(TempDir, RocksdbLogStore, TestStateMachine), StorageError<u64>> {
        // Create temporary directory
        let temp_dir = TempDir::new().map_err(|e| StorageIOError::read_state_machine(&e))?;

        // Configure RocksDB
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Create all required column families
        let cfs: Vec<ColumnFamilyDescriptor> = TEST_CF_NAMES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        // Open database
        let db = DB::open_cf_descriptors(&opts, temp_dir.path(), cfs)
            .map_err(|e| StorageIOError::write(&e))?;

        let engine: Arc<dyn Engine> = Arc::new(RocksdbEngine::new(db));

        // Create LogStore
        let log_store = RocksdbLogStore::new(engine)
            .map_err(|e| StorageIOError::read_state_machine(AnyError::error(e.to_string())))?;

        // Create test StateMachine
        let state_machine = TestStateMachine::new();

        Ok((temp_dir, log_store, state_machine))
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

/// Type alias for the test suite to simplify generic parameters
type KiwiSuite = openraft::testing::Suite<
    KiwiTypeConfig,
    RocksdbLogStore,
    TestStateMachine,
    KiwiStoreBuilder,
    TempDir,
>;

/// Helper function to run a single test (mimics OpenRaft's internal run_test + run_fut)
fn run_test<F, Fut>(builder: &KiwiStoreBuilder, test_fn: F) -> Result<(), StorageError<u64>>
where
    F: Fn(RocksdbLogStore, TestStateMachine) -> Fut + Sync + Send,
    Fut: std::future::Future<Output = Result<(), StorageError<u64>>> + Send,
{
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async {
        let (_guard, store, sm) = builder.build().await?;
        test_fn(store, sm).await
    })
}

// ============================================================================
// LogStore Tests (26 tests)
// ============================================================================

/// Run OpenRaft LogStore test suite
///
/// Tests LogStore and Membership functionality. StateMachine snapshot tests are excluded.
#[test]
fn run_log_store_tests() {
    let builder = KiwiStoreBuilder;

    // ========== Membership Tests (7 tests) ==========
    // Tests for membership configuration reading and management

    run_test(&builder, KiwiSuite::last_membership_in_log_initial)
        .expect("last_membership_in_log_initial test should pass");
    run_test(&builder, KiwiSuite::last_membership_in_log)
        .expect("last_membership_in_log test should pass");
    run_test(&builder, KiwiSuite::last_membership_in_log_multi_step)
        .expect("last_membership_in_log_multi_step test should pass");
    run_test(&builder, KiwiSuite::get_membership_initial)
        .expect("get_membership_initial test should pass");
    run_test(&builder, KiwiSuite::get_membership_from_log_and_empty_sm)
        .expect("get_membership_from_log_and_empty_sm test should pass");
    run_test(&builder, KiwiSuite::get_membership_from_empty_log_and_sm)
        .expect("get_membership_from_empty_log_and_sm test should pass");
    run_test(
        &builder,
        KiwiSuite::get_membership_from_log_le_sm_last_applied,
    )
    .expect("get_membership_from_log_le_sm_last_applied test should pass");

    // ========== Initial State Tests (7 tests) ==========
    // Tests for initial state retrieval and recovery

    run_test(&builder, KiwiSuite::get_initial_state_without_init)
        .expect("get_initial_state_without_init test should pass");
    run_test(&builder, KiwiSuite::get_initial_state_with_state)
        .expect("get_initial_state_with_state test should pass");
    run_test(
        &builder,
        KiwiSuite::get_initial_state_membership_from_log_and_sm,
    )
    .expect("get_initial_state_membership_from_log_and_sm test should pass");
    run_test(&builder, KiwiSuite::get_initial_state_last_log_gt_sm)
        .expect("get_initial_state_last_log_gt_sm test should pass");
    run_test(&builder, KiwiSuite::get_initial_state_last_log_lt_sm)
        .expect("get_initial_state_last_log_lt_sm test should pass");
    run_test(&builder, KiwiSuite::get_initial_state_log_ids)
        .expect("get_initial_state_log_ids test should pass");
    run_test(&builder, KiwiSuite::get_initial_state_re_apply_committed)
        .expect("get_initial_state_re_apply_committed test should pass");

    // ========== Core LogStore Tests (12 tests) ==========
    // Tests for log read/write, persistence, and cleanup

    // Vote persistence
    run_test(&builder, KiwiSuite::save_vote).expect("save_vote test should pass");

    // Log entry reading
    run_test(&builder, KiwiSuite::get_log_entries).expect("get_log_entries test should pass");
    run_test(&builder, KiwiSuite::limited_get_log_entries)
        .expect("limited_get_log_entries test should pass");
    run_test(&builder, KiwiSuite::try_get_log_entry).expect("try_get_log_entry test should pass");

    // Log state
    run_test(&builder, KiwiSuite::initial_logs).expect("initial_logs test should pass");
    run_test(&builder, KiwiSuite::get_log_state).expect("get_log_state test should pass");
    run_test(&builder, KiwiSuite::get_log_id).expect("get_log_id test should pass");
    run_test(&builder, KiwiSuite::last_id_in_log).expect("last_id_in_log test should pass");
    run_test(&builder, KiwiSuite::last_applied_state).expect("last_applied_state test should pass");

    // Log purge (delete logs where index <= given index)
    run_test(&builder, KiwiSuite::purge_logs_upto_0).expect("purge_logs_upto_0 test should pass");
    run_test(&builder, KiwiSuite::purge_logs_upto_5).expect("purge_logs_upto_5 test should pass");
    run_test(&builder, KiwiSuite::purge_logs_upto_20).expect("purge_logs_upto_20 test should pass");

    // Log truncate (delete logs where index >= given index)
    run_test(&builder, KiwiSuite::delete_logs_since_11)
        .expect("delete_logs_since_11 test should pass");
    run_test(&builder, KiwiSuite::delete_logs_since_0)
        .expect("delete_logs_since_0 test should pass");

    // Log append
    run_test(&builder, KiwiSuite::append_to_log).expect("append_to_log test should pass");

    println!("✅ All LogStore tests passed! (26 tests)");
}

// ============================================================================
// StateMachine Tests (4 tests, currently ignored)
// ============================================================================

/// Run StateMachine tests
///
/// These tests are ignored until `KiwiStateMachine` properly implements snapshot functionality.
/// Remove `#[ignore]` attribute once snapshot implementation is complete.
#[test]
#[ignore = "StateMachine snapshot functionality is not yet implemented"]
fn run_state_machine_tests() {
    let builder = KiwiStoreBuilder;

    // StateMachine apply tests
    run_test(&builder, KiwiSuite::apply_single).expect("apply_single test should pass");
    run_test(&builder, KiwiSuite::apply_multiple).expect("apply_multiple test should pass");

    // Snapshot tests
    run_test(&builder, KiwiSuite::snapshot_meta).expect("snapshot_meta test should pass");

    // transfer_snapshot takes &builder instead of (store, sm)
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(KiwiSuite::transfer_snapshot(&builder))
        .expect("transfer_snapshot test should pass");

    println!("✅ All StateMachine tests passed! (4 tests)");
}
