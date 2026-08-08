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

use openraft::{LeaderId, LogId, StoredMembership};
use raft::durable_state_machine_meta::{DurableStateMachineMeta, DurableStateMachineStore};
use raft::log_store_rocksdb::RocksdbLogStore;

fn store_at(temp_dir: &tempfile::TempDir) -> DurableStateMachineStore {
    let log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
        .expect("log store should open with all column families");
    DurableStateMachineStore::new(log_store.db())
}

#[test]
fn sm_meta_cf_exists_on_open() {
    let temp_dir = tempfile::TempDir::new().expect("temporary directory should be created");
    let log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
        .expect("log store should open with all column families");
    let db = log_store.db();
    assert!(
        db.cf_handle("sm_meta").is_some(),
        "sm_meta column family must exist after opening the log store"
    );
}

#[test]
fn roundtrip_saves_and_loads_meta() {
    let temp_dir = tempfile::TempDir::new().expect("temporary directory should be created");
    let store = store_at(&temp_dir);

    let meta = DurableStateMachineMeta::new(
        Some(LogId::new(LeaderId::new(1, 1), 5)),
        StoredMembership::default(),
    );
    store.save_meta(&meta).expect("meta should be persisted");
    assert_eq!(
        store.load_meta().expect("meta should load back"),
        Some(meta),
        "round-tripped metadata must equal the saved value"
    );
}

/// note(guozhihao-224) Corrupt bytes must error, not be treated as an empty frontier.
#[test]
fn corrupt_meta_load_fails_closed() {
    let temp_dir = tempfile::TempDir::new().expect("temporary directory should be created");
    let log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
        .expect("log store should open with all column families");
    let db = log_store.db();
    {
        let cf = db
            .cf_handle("sm_meta")
            .expect("sm_meta column family should exist");
        db.put_cf(&cf, b"state_machine_meta", b"not-valid-json")
            .expect("raw bytes should be written");
    }

    let store = DurableStateMachineStore::new(db);
    let err = store
        .load_meta()
        .expect_err("corrupt metadata must be rejected, not treated as empty");
    let _ = err;
}

#[test]
fn unknown_format_version_is_rejected() {
    let temp_dir = tempfile::TempDir::new().expect("temporary directory should be created");
    let store = store_at(&temp_dir);

    let meta = DurableStateMachineMeta {
        format_version: 99,
        last_applied: Some(LogId::new(LeaderId::new(1, 1), 7)),
        last_membership: StoredMembership::default(),
    };
    store.save_meta(&meta).expect("meta should be persisted");
    let err = store
        .validate()
        .expect_err("unsupported format version must be rejected");
    let _ = err;
}

#[test]
fn validate_on_empty_store_returns_none() {
    let temp_dir = tempfile::TempDir::new().expect("temporary directory should be created");
    let store = store_at(&temp_dir);
    assert_eq!(
        store.validate().expect("empty store should validate"),
        None,
        "no metadata must yield None rather than an error"
    );
}
