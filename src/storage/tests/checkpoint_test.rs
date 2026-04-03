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

use std::sync::Arc;

use storage::{
    RaftSnapshotMeta, StorageOptions, restore_checkpoint_layout, storage::Storage,
    unique_test_db_path,
};

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

    // Verify version is serialized
    let json = serde_json::to_string(&meta).unwrap();
    assert!(json.contains("\"version\":1"));

    // Verify version is deserialized correctly
    let deserialized: RaftSnapshotMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.version, 1);
    assert_eq!(deserialized.last_included_index, 100);
    assert_eq!(deserialized.last_included_term, 5);
}

#[test]
fn snapshot_meta_rejects_missing_version() {
    // Old format JSON without version field
    let old_format_json = r#"{
        "last_included_index": 42,
        "last_included_term": 7
    }"#;

    let result: Result<RaftSnapshotMeta, _> = serde_json::from_str(old_format_json);
    assert!(
        result.is_err(),
        "Should reject old format without version field"
    );
}

#[test]
fn test_snapshot_meta_rejects_version_zero() {
    use std::fs;

    let tmp_dir = tempfile::tempdir().unwrap();
    let meta_path = tmp_dir.path().join("__raft_snapshot_meta");

    // Write invalid version 0 to file
    let invalid_json = r#"{
        "version": 0,
        "last_included_index": 42,
        "last_included_term": 7
    }"#;
    fs::write(&meta_path, invalid_json).unwrap();

    let result = RaftSnapshotMeta::read_from_dir(tmp_dir.path());
    assert!(result.is_err(), "Should reject snapshot with version 0");
}

#[test]
fn test_snapshot_meta_accepts_higher_version() {
    use std::fs;

    let tmp_dir = tempfile::tempdir().unwrap();
    let meta_path = tmp_dir.path().join("__raft_snapshot_meta");

    // Write future version to file
    let future_json = r#"{
        "version": 999,
        "last_included_index": 42,
        "last_included_term": 7
    }"#;
    fs::write(&meta_path, future_json).unwrap();

    let result = RaftSnapshotMeta::read_from_dir(tmp_dir.path());
    assert!(
        result.is_ok(),
        "Should accept higher versions for forward compatibility"
    );
    assert_eq!(result.unwrap().version, 999);
}

#[test]
fn test_snapshot_meta_max_version() {
    let meta = RaftSnapshotMeta {
        version: u32::MAX,
        last_included_index: 42,
        last_included_term: 7,
    };

    let json = serde_json::to_string(&meta).unwrap();
    let deserialized: RaftSnapshotMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.version, u32::MAX);
}
