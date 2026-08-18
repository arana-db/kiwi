#!/usr/bin/env python3

# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exact-ref Vector storage compatibility runner support."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shlex
import sys
import textwrap
import tomllib


EXPECTED_GATES = (
    "base_688d905f_creates_real_six_cf_nonempty_storage",
    "vector_v1_733888fc_creates_real_seven_cf_manifest_v1_storage",
    "head_upgrades_and_reopens_real_base_storage",
    "head_upgrades_and_reopens_real_vector_v1_storage",
    "head_retries_every_migration_phase_for_both_source_profiles",
    "base_reopens_verified_pre_admission_rollback",
    "vector_v1_reopens_verified_pre_admission_rollback",
    "head_rejects_base_rollback_after_rollback_window_closed",
    "base_v1_snapshot_restores_on_head",
    "v1_snapshot_with_unknown_or_vector_schema_is_rejected",
    "head_v2_snapshot_reopens_with_exact_manifest_pairing",
)


LICENSE_HEADER = """\
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
"""


BASE_STORAGE_DRIVER = LICENSE_HEADER + r'''

#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocksdb::DB;
use storage::{
    StorageOptions, ZsetScoreMember,
    slot_indexer::{SlotIndexer, key_to_slot_id},
    storage::Storage,
};

const STRING_KEY: &[u8] = b"compat:string";
const HASH_KEY: &[u8] = b"compat:hash";
const HASH_FIELD: &[u8] = b"field";
const ZSET_KEY: &[u8] = b"compat:zset";
const ZSET_MEMBER: &[u8] = b"member";
const TTL_KEY: &[u8] = b"compat:ttl";
const ROOT_MANIFEST: &str = "__kiwi_root_storage_manifest";
const INSTANCE_MANIFEST: &str = "__kiwi_storage_manifest";

fn fixture_root() -> PathBuf {
    PathBuf::from(std::env::var_os("KIWI_COMPAT_ROOT").expect("KIWI_COMPAT_ROOT is required"))
}

fn partition_key(kind: &str, instance_id: usize) -> Vec<u8> {
    let indexer = SlotIndexer::new(2);
    for nonce in 0..10_000_u32 {
        let key = format!("compat:partition:{kind}:{instance_id}:{nonce}").into_bytes();
        if indexer.get_instance_id(key_to_slot_id(&key)) == instance_id {
            return key;
        }
    }
    panic!("cannot find partitioned key for instance {instance_id}");
}

fn write_partitioned_data(storage: &Storage) {
    for instance_id in 0..2_usize {
        let string_key = partition_key("string", instance_id);
        let hash_key = partition_key("hash", instance_id);
        let zset_key = partition_key("zset", instance_id);
        let ttl_key = partition_key("ttl", instance_id);
        let sentinel_key = partition_key("sentinel", instance_id);
        storage
            .set(&string_key, format!("partition-string-{instance_id}").as_bytes())
            .expect("write partitioned String");
        storage
            .hset(&hash_key, HASH_FIELD, format!("partition-hash-{instance_id}").as_bytes())
            .expect("write partitioned Hash");
        storage
            .zadd(
                &zset_key,
                &[ZsetScoreMember::new(42.5, format!("partition-member-{instance_id}").into_bytes())],
            )
            .expect("write partitioned ZSet");
        storage
            .set(&ttl_key, format!("partition-ttl-{instance_id}").as_bytes())
            .expect("write partitioned TTL value");
        assert!(storage.expire(&ttl_key, 86_400).expect("write partitioned TTL"));
        storage
            .set(&sentinel_key, format!("partition-sentinel-{instance_id}").as_bytes())
            .expect("write partitioned sentinel");
    }
}

fn verify_partitioned_data(storage: &Storage) {
    for instance_id in 0..2_usize {
        let string_key = partition_key("string", instance_id);
        let hash_key = partition_key("hash", instance_id);
        let zset_key = partition_key("zset", instance_id);
        let ttl_key = partition_key("ttl", instance_id);
        let sentinel_key = partition_key("sentinel", instance_id);
        assert_eq!(
            storage.get(&string_key).expect("read partitioned String"),
            format!("partition-string-{instance_id}")
        );
        assert_eq!(
            storage.hget(&hash_key, HASH_FIELD).expect("read partitioned Hash"),
            Some(format!("partition-hash-{instance_id}"))
        );
        assert_eq!(
            storage
                .zscore(&zset_key, format!("partition-member-{instance_id}").as_bytes())
                .expect("read partitioned ZSet"),
            Some(b"42.5".to_vec())
        );
        assert_eq!(
            storage.get(&ttl_key).expect("read partitioned TTL value"),
            format!("partition-ttl-{instance_id}")
        );
        assert!(storage.ttl(&ttl_key).expect("read partitioned TTL") > 0);
        assert_eq!(
            storage.get(&sentinel_key).expect("read partitioned sentinel"),
            format!("partition-sentinel-{instance_id}")
        );
    }
}

fn verify_data(storage: &Storage) {
    assert_eq!(storage.get(STRING_KEY).expect("read String"), "string-value");
    assert_eq!(
        storage.hget(HASH_KEY, HASH_FIELD).expect("read Hash"),
        Some("hash-value".to_string())
    );
    assert_eq!(
        storage.zscore(ZSET_KEY, ZSET_MEMBER).expect("read ZSet"),
        Some(b"42.5".to_vec())
    );
    assert_eq!(storage.get(TTL_KEY).expect("read TTL value"), "ttl-value");
    assert!(storage.ttl(TTL_KEY).expect("read TTL") > 0);
    for index in 0..64_u32 {
        let key = format!("compat:sentinel:{index}");
        let value = format!("value:{index}");
        assert_eq!(storage.get(key.as_bytes()).expect("read sentinel"), value);
    }
    verify_partitioned_data(storage);
}

fn assert_schema(root: &Path) {
    let mut expected = vec![
        "default",
        "hash_data_cf",
        "list_data_cf",
        "set_data_cf",
        "zset_data_cf",
        "zset_score_cf",
    ];
    expected.sort();
    assert!(!root.join(ROOT_MANIFEST).exists(), "Base must not contain a v2 root manifest");
    for instance_id in 0..2 {
        let instance = root.join(instance_id.to_string());
        let mut actual = DB::list_cf(&StorageOptions::default().options, &instance)
            .expect("list Base column families");
        actual.sort();
        assert_eq!(actual, expected, "Base must expose exactly six column families");
        assert!(!instance.join(INSTANCE_MANIFEST).exists(), "Base must not contain v1 Vector manifest");
        assert!(
            std::fs::read_dir(&instance).expect("read instance").any(|entry| {
                entry.expect("instance entry").metadata().expect("entry metadata").len() > 0
            }),
            "Base instance must contain non-empty RocksDB files"
        );
    }
}

fn open_and_verify(root: &Path) {
    let runtime = tokio::runtime::Runtime::new().expect("create Tokio runtime");
    let _runtime_guard = runtime.enter();
    let mut storage = Storage::new(2, 0);
    let receiver = storage
        .open(Arc::new(StorageOptions::default()), root)
        .expect("Base exact-ref binary must reopen fixture");
    verify_data(&storage);
    runtime.block_on(storage.shutdown());
    storage.close();
    drop(receiver);
    assert_schema(root);
}

#[test]
fn create_fixture() {
    let root = fixture_root();
    assert!(!root.exists(), "fixture root must start absent");
    let runtime = tokio::runtime::Runtime::new().expect("create Tokio runtime");
    let _runtime_guard = runtime.enter();
    let mut storage = Storage::new(2, 0);
    let receiver = storage
        .open(Arc::new(StorageOptions::default()), &root)
        .expect("Base exact-ref binary must create fixture");
    storage.set(STRING_KEY, b"string-value").expect("write String");
    storage.hset(HASH_KEY, HASH_FIELD, b"hash-value").expect("write Hash");
    storage
        .zadd(ZSET_KEY, &[ZsetScoreMember::new(42.5, ZSET_MEMBER.to_vec())])
        .expect("write ZSet");
    storage.set(TTL_KEY, b"ttl-value").expect("write TTL value");
    assert!(storage.expire(TTL_KEY, 86_400).expect("write TTL"));
    for index in 0..64_u32 {
        let key = format!("compat:sentinel:{index}");
        let value = format!("value:{index}");
        storage.set(key.as_bytes(), value.as_bytes()).expect("write sentinel");
    }
    write_partitioned_data(&storage);
    verify_data(&storage);
    runtime.block_on(storage.shutdown());
    storage.close();
    drop(receiver);
    assert_schema(&root);
}

#[test]
fn reopen_fixture() {
    open_and_verify(&fixture_root());
}
'''


VECTOR_STORAGE_DRIVER = LICENSE_HEADER + r'''

#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocksdb::DB;
use storage::{
    CanonicalVector, STORAGE_MANIFEST_FILE, StorageOptions, ZsetScoreMember,
    slot_indexer::{SlotIndexer, key_to_slot_id},
    storage::Storage,
};

const STRING_KEY: &[u8] = b"compat:string";
const HASH_KEY: &[u8] = b"compat:hash";
const HASH_FIELD: &[u8] = b"field";
const ZSET_KEY: &[u8] = b"compat:zset";
const ZSET_MEMBER: &[u8] = b"member";
const TTL_KEY: &[u8] = b"compat:ttl";
const VECTOR_KEY: &[u8] = b"compat:vector";
const VECTOR_ELEMENT: &[u8] = b"element";
const ROOT_MANIFEST: &str = "__kiwi_root_storage_manifest";

fn fixture_root() -> PathBuf {
    PathBuf::from(std::env::var_os("KIWI_COMPAT_ROOT").expect("KIWI_COMPAT_ROOT is required"))
}

fn partition_key(kind: &str, instance_id: usize) -> Vec<u8> {
    let indexer = SlotIndexer::new(2);
    for nonce in 0..10_000_u32 {
        let key = format!("compat:partition:{kind}:{instance_id}:{nonce}").into_bytes();
        if indexer.get_instance_id(key_to_slot_id(&key)) == instance_id {
            return key;
        }
    }
    panic!("cannot find partitioned key for instance {instance_id}");
}

fn write_partitioned_data(storage: &Storage) {
    for instance_id in 0..2_usize {
        let string_key = partition_key("string", instance_id);
        let hash_key = partition_key("hash", instance_id);
        let zset_key = partition_key("zset", instance_id);
        let ttl_key = partition_key("ttl", instance_id);
        let sentinel_key = partition_key("sentinel", instance_id);
        let vector_key = partition_key("vector", instance_id);
        storage
            .set(&string_key, format!("partition-string-{instance_id}").as_bytes())
            .expect("write partitioned String");
        storage
            .hset(&hash_key, HASH_FIELD, format!("partition-hash-{instance_id}").as_bytes())
            .expect("write partitioned Hash");
        storage
            .zadd(
                &zset_key,
                &[ZsetScoreMember::new(42.5, format!("partition-member-{instance_id}").into_bytes())],
            )
            .expect("write partitioned ZSet");
        storage
            .set(&ttl_key, format!("partition-ttl-{instance_id}").as_bytes())
            .expect("write partitioned TTL value");
        assert!(storage.expire(&ttl_key, 86_400).expect("write partitioned TTL"));
        storage
            .set(&sentinel_key, format!("partition-sentinel-{instance_id}").as_bytes())
            .expect("write partitioned sentinel");
        let vector = CanonicalVector::from_values(&[instance_id as f32 + 0.25, 0.75])
            .expect("build partitioned Vector member");
        assert!(
            storage
                .vadd(&vector_key, VECTOR_ELEMENT, &vector)
                .expect("write partitioned Vector member")
        );
    }
}

fn verify_partitioned_data(storage: &Storage) {
    for instance_id in 0..2_usize {
        let string_key = partition_key("string", instance_id);
        let hash_key = partition_key("hash", instance_id);
        let zset_key = partition_key("zset", instance_id);
        let ttl_key = partition_key("ttl", instance_id);
        let sentinel_key = partition_key("sentinel", instance_id);
        let vector_key = partition_key("vector", instance_id);
        assert_eq!(
            storage.get(&string_key).expect("read partitioned String"),
            format!("partition-string-{instance_id}")
        );
        assert_eq!(
            storage.hget(&hash_key, HASH_FIELD).expect("read partitioned Hash"),
            Some(format!("partition-hash-{instance_id}"))
        );
        assert_eq!(
            storage
                .zscore(&zset_key, format!("partition-member-{instance_id}").as_bytes())
                .expect("read partitioned ZSet"),
            Some(b"42.5".to_vec())
        );
        assert_eq!(
            storage.get(&ttl_key).expect("read partitioned TTL value"),
            format!("partition-ttl-{instance_id}")
        );
        assert!(storage.ttl(&ttl_key).expect("read partitioned TTL") > 0);
        assert_eq!(
            storage.get(&sentinel_key).expect("read partitioned sentinel"),
            format!("partition-sentinel-{instance_id}")
        );
        let vector = storage
            .vemb(&vector_key, VECTOR_ELEMENT)
            .expect("read partitioned Vector member")
            .expect("partitioned Vector member exists");
        assert_eq!(vector.len(), 2);
        assert!(vector.iter().all(|value| value.is_finite()));
        assert!((vector[0] - (instance_id as f64 + 0.25)).abs() <= 1e-6);
        assert!((vector[1] - 0.75).abs() <= 1e-6);
    }
}

fn verify_data(storage: &Storage) {
    assert_eq!(storage.get(STRING_KEY).expect("read String"), "string-value");
    assert_eq!(
        storage.hget(HASH_KEY, HASH_FIELD).expect("read Hash"),
        Some("hash-value".to_string())
    );
    assert_eq!(
        storage.zscore(ZSET_KEY, ZSET_MEMBER).expect("read ZSet"),
        Some(b"42.5".to_vec())
    );
    assert_eq!(storage.get(TTL_KEY).expect("read TTL value"), "ttl-value");
    assert!(storage.ttl(TTL_KEY).expect("read TTL") > 0);
    let vector = storage
        .vemb(VECTOR_KEY, VECTOR_ELEMENT)
        .expect("read Vector member")
        .expect("Vector member exists");
    assert_eq!(vector.len(), 2);
    assert!(vector.iter().all(|value| value.is_finite()));
    assert!((vector[0] - 0.25).abs() <= 1e-6);
    assert!((vector[1] - 0.75).abs() <= 1e-6);
    for index in 0..64_u32 {
        let key = format!("compat:sentinel:{index}");
        let value = format!("value:{index}");
        assert_eq!(storage.get(key.as_bytes()).expect("read sentinel"), value);
    }
    verify_partitioned_data(storage);
}

fn assert_schema(root: &Path) {
    let mut expected = vec![
        "default",
        "hash_data_cf",
        "list_data_cf",
        "set_data_cf",
        "vector_data_cf",
        "zset_data_cf",
        "zset_score_cf",
    ];
    expected.sort();
    assert!(!root.join(ROOT_MANIFEST).exists(), "Vector-v1 must not contain a v2 root manifest");
    for instance_id in 0..2 {
        let instance = root.join(instance_id.to_string());
        let mut actual = DB::list_cf(&StorageOptions::default().options, &instance)
            .expect("list Vector-v1 column families");
        actual.sort();
        assert_eq!(actual, expected, "Vector-v1 must expose exactly seven column families");
        let manifest_bytes = std::fs::read(instance.join(STORAGE_MANIFEST_FILE))
            .expect("read Vector-v1 manifest");
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)
            .expect("parse Vector-v1 manifest");
        assert_eq!(manifest["version"].as_u64(), Some(1));
        assert!(manifest["storage_incarnation"].as_u64().is_some());
        assert!(manifest["next_generation"].as_u64().is_some());
    }
}

fn open_and_verify(root: &Path) {
    let runtime = tokio::runtime::Runtime::new().expect("create Tokio runtime");
    let _runtime_guard = runtime.enter();
    let mut storage = Storage::new(2, 0);
    let receiver = storage
        .open(Arc::new(StorageOptions::default()), root)
        .expect("Vector-v1 exact-ref binary must reopen fixture");
    verify_data(&storage);
    runtime.block_on(storage.shutdown());
    storage.close();
    drop(receiver);
    assert_schema(root);
}

#[test]
fn create_fixture() {
    let root = fixture_root();
    assert!(!root.exists(), "fixture root must start absent");
    let runtime = tokio::runtime::Runtime::new().expect("create Tokio runtime");
    let _runtime_guard = runtime.enter();
    let mut storage = Storage::new(2, 0);
    let receiver = storage
        .open(Arc::new(StorageOptions::default()), &root)
        .expect("Vector-v1 exact-ref binary must create fixture");
    storage.set(STRING_KEY, b"string-value").expect("write String");
    storage.hset(HASH_KEY, HASH_FIELD, b"hash-value").expect("write Hash");
    storage
        .zadd(ZSET_KEY, &[ZsetScoreMember::new(42.5, ZSET_MEMBER.to_vec())])
        .expect("write ZSet");
    storage.set(TTL_KEY, b"ttl-value").expect("write TTL value");
    assert!(storage.expire(TTL_KEY, 86_400).expect("write TTL"));
    let vector = CanonicalVector::from_values(&[0.25, 0.75]).expect("build Vector member");
    assert!(storage.vadd(VECTOR_KEY, VECTOR_ELEMENT, &vector).expect("write Vector member"));
    for index in 0..64_u32 {
        let key = format!("compat:sentinel:{index}");
        let value = format!("value:{index}");
        storage.set(key.as_bytes(), value.as_bytes()).expect("write sentinel");
    }
    write_partitioned_data(&storage);
    verify_data(&storage);
    runtime.block_on(storage.shutdown());
    storage.close();
    drop(receiver);
    assert_schema(&root);
}

#[test]
fn reopen_fixture() {
    open_and_verify(&fixture_root());
}
'''


BASE_SNAPSHOT_DRIVER = LICENSE_HEADER + r'''

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use raft::state_machine::{KiwiStateMachine, PauseController, StorageAccessPermit};
use storage::{StorageOptions, storage::Storage};

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
async fn build_exact_base_v1_snapshot() -> anyhow::Result<()> {
    let db_root = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_ROOT").expect("KIWI_COMPAT_ROOT is required"),
    );
    let archive = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_ARCHIVE").expect("KIWI_COMPAT_ARCHIVE is required"),
    );
    let snapshot_meta = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_SNAPSHOT_META")
            .expect("KIWI_COMPAT_SNAPSHOT_META is required"),
    );
    let snapshot_work = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_SNAPSHOT_WORK")
            .expect("KIWI_COMPAT_SNAPSHOT_WORK is required"),
    );
    std::fs::create_dir_all(&snapshot_work)?;

    let mut opened = Storage::new(2, 0);
    let receiver = opened.open(Arc::new(StorageOptions::default()), &db_root)?;
    let storage = Arc::new(opened);
    let storage_swap = Arc::new(ArcSwap::from(Arc::clone(&storage)));
    let mut state_machine = KiwiStateMachine::new(
        1,
        Arc::clone(&storage_swap),
        db_root,
        snapshot_work,
        Arc::new(NoopPauseController),
        None,
    );
    let mut builder = state_machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await?;
    anyhow::ensure!(!snapshot.snapshot.get_ref().is_empty(), "Base v1 snapshot archive is empty");
    std::fs::write(&snapshot_meta, serde_json::to_vec_pretty(&snapshot.meta)?)?;
    std::fs::write(&archive, snapshot.snapshot.into_inner())?;
    drop(builder);
    drop(state_machine);
    drop(storage_swap);
    let mut storage = Arc::try_unwrap(storage)
        .map_err(|_| anyhow::anyhow!("Base snapshot Storage still has Arc owners"))?;
    storage.shutdown().await;
    storage.close();
    drop(receiver);
    anyhow::ensure!(archive.metadata()?.len() > 0, "Base v1 archive is empty on disk");
    anyhow::ensure!(snapshot_meta.metadata()?.len() > 0, "Base SnapshotMeta sidecar is empty");
    Ok(())
}
'''


HEAD_STORAGE_DRIVER = LICENSE_HEADER + r'''

#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use rocksdb::DB;
use storage::{
    CANONICAL_COLUMN_FAMILY_NAMES, InstanceStorageManifestV2, MigrationFaultPoint,
    ManifestDigest, MigrationPhase, MigrationSourceProfile, MigrationTransaction,
    ROOT_STORAGE_MANIFEST_FILE, RootStorageManifestV2, STORAGE_MANIFEST_FILE, StorageOptions,
    classify_storage_root, close_rollback_window, fail_next_storage_migration,
    recover_or_rollback_before_admission,
    slot_indexer::{SlotIndexer, key_to_slot_id},
    slot_mapping_digest, storage::Storage,
};
use uuid::Uuid;

const STRING_KEY: &[u8] = b"compat:string";
const HASH_KEY: &[u8] = b"compat:hash";
const HASH_FIELD: &[u8] = b"field";
const ZSET_KEY: &[u8] = b"compat:zset";
const ZSET_MEMBER: &[u8] = b"member";
const TTL_KEY: &[u8] = b"compat:ttl";
const VECTOR_KEY: &[u8] = b"compat:vector";
const VECTOR_ELEMENT: &[u8] = b"element";

fn fixture_root() -> PathBuf {
    PathBuf::from(std::env::var_os("KIWI_COMPAT_ROOT").expect("KIWI_COMPAT_ROOT is required"))
}

fn profile() -> MigrationSourceProfile {
    match std::env::var("KIWI_COMPAT_PROFILE").expect("KIWI_COMPAT_PROFILE is required").as_str() {
        "base" => MigrationSourceProfile::BaseV1SixCf,
        "vector" => MigrationSourceProfile::VectorSetV1SevenCf,
        other => panic!("unknown profile: {other}"),
    }
}

fn partition_key(kind: &str, instance_id: usize) -> Vec<u8> {
    let indexer = SlotIndexer::new(2);
    for nonce in 0..10_000_u32 {
        let key = format!("compat:partition:{kind}:{instance_id}:{nonce}").into_bytes();
        if indexer.get_instance_id(key_to_slot_id(&key)) == instance_id {
            return key;
        }
    }
    panic!("cannot find partitioned key for instance {instance_id}");
}

fn verify_partitioned_instance(
    storage: &Storage,
    source_profile: MigrationSourceProfile,
    original_instance_id: usize,
) {
    let string_key = partition_key("string", original_instance_id);
    let hash_key = partition_key("hash", original_instance_id);
    let zset_key = partition_key("zset", original_instance_id);
    let ttl_key = partition_key("ttl", original_instance_id);
    let sentinel_key = partition_key("sentinel", original_instance_id);
    assert_eq!(
        storage.get(&string_key).expect("read partitioned String"),
        format!("partition-string-{original_instance_id}"),
        "V2 validation copy partitioned String mismatch for original instance {original_instance_id}"
    );
    assert_eq!(
        storage.hget(&hash_key, HASH_FIELD).expect("read partitioned Hash"),
        Some(format!("partition-hash-{original_instance_id}"))
    );
    assert_eq!(
        storage
            .zscore(
                &zset_key,
                format!("partition-member-{original_instance_id}").as_bytes(),
            )
            .expect("read partitioned ZSet"),
        Some(b"42.5".to_vec())
    );
    assert_eq!(
        storage.get(&ttl_key).expect("read partitioned TTL value"),
        format!("partition-ttl-{original_instance_id}")
    );
    assert!(storage.ttl(&ttl_key).expect("read partitioned TTL") > 0);
    assert_eq!(
        storage.get(&sentinel_key).expect("read partitioned sentinel"),
        format!("partition-sentinel-{original_instance_id}")
    );
    if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
        let vector_key = partition_key("vector", original_instance_id);
        let vector = storage
            .vemb(&vector_key, VECTOR_ELEMENT)
            .expect("read partitioned Vector member")
            .expect("partitioned Vector member exists");
        assert_eq!(vector.len(), 2);
        assert!(vector.iter().all(|value| value.is_finite()));
        assert!((vector[0] - (original_instance_id as f64 + 0.25)).abs() <= 1e-6);
        assert!((vector[1] - 0.75).abs() <= 1e-6);
    }
}

fn verify_partitioned_data(storage: &Storage, source_profile: MigrationSourceProfile) {
    for instance_id in 0..2_usize {
        verify_partitioned_instance(storage, source_profile, instance_id);
    }
}

fn read_v1_identities(root: &Path) -> Vec<(u64, u64)> {
    (0..2)
        .map(|instance_id| {
            let bytes = std::fs::read(root.join(instance_id.to_string()).join(STORAGE_MANIFEST_FILE))
                .expect("read Vector-v1 manifest");
            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .expect("parse Vector-v1 manifest");
            assert_eq!(value["version"].as_u64(), Some(1));
            (
                value["storage_incarnation"].as_u64().expect("v1 incarnation"),
                value["next_generation"].as_u64().expect("v1 generation"),
            )
        })
        .collect()
}

fn verify_data(storage: &Storage, source_profile: MigrationSourceProfile) {
    assert_eq!(storage.get(STRING_KEY).expect("read String"), "string-value");
    assert_eq!(
        storage.hget(HASH_KEY, HASH_FIELD).expect("read Hash"),
        Some("hash-value".to_string())
    );
    assert_eq!(
        storage.zscore(ZSET_KEY, ZSET_MEMBER).expect("read ZSet"),
        Some(b"42.5".to_vec())
    );
    assert_eq!(storage.get(TTL_KEY).expect("read TTL value"), "ttl-value");
    assert!(storage.ttl(TTL_KEY).expect("read TTL") > 0);
    if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
        let vector = storage
            .vemb(VECTOR_KEY, VECTOR_ELEMENT)
            .expect("read Vector member")
            .expect("Vector member exists");
        assert_eq!(vector.len(), 2);
        assert!(vector.iter().all(|value| value.is_finite()));
        assert!((vector[0] - 0.25).abs() <= 1e-6);
        assert!((vector[1] - 0.75).abs() <= 1e-6);
    }
    for index in 0..64_u32 {
        let key = format!("compat:sentinel:{index}");
        let value = format!("value:{index}");
        assert_eq!(storage.get(key.as_bytes()).expect("read sentinel"), value);
    }
    verify_partitioned_data(storage, source_profile);
}

fn open_and_verify(root: &Path, source_profile: MigrationSourceProfile) -> Result<(), String> {
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    let _runtime_guard = runtime.enter();
    let mut storage = Storage::new(2, 0);
    let receiver = storage
        .open(Arc::new(StorageOptions::default()), root)
        .map_err(|error| error.to_string())?;
    verify_data(&storage, source_profile);
    runtime.block_on(storage.shutdown());
    storage.close();
    drop(receiver);
    Ok(())
}

fn attempt_open(root: &Path) -> Result<(), String> {
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    let _runtime_guard = runtime.enter();
    let mut storage = Storage::new(2, 0);
    match storage.open(Arc::new(StorageOptions::default()), root) {
        Ok(receiver) => {
            runtime.block_on(storage.shutdown());
            storage.close();
            drop(receiver);
            Ok(())
        }
        Err(error) => {
            storage.close();
            Err(error.to_string())
        }
    }
}

fn assert_safe_basename(value: &str) {
    let path = Path::new(value);
    assert_eq!(path.components().count(), 1, "migration path must be a basename");
    assert!(matches!(path.components().next(), Some(Component::Normal(_))));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedDiskKind {
    Missing,
    Legacy,
    V2,
    PartialBaseVectorCf,
}

fn assert_directory_or_missing(path: &Path, should_exist: bool, label: &str) {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            assert!(should_exist, "{label} unexpectedly exists: {}", path.display());
            assert!(!metadata.file_type().is_symlink(), "{label} must not be a symlink");
            assert!(metadata.is_dir(), "{label} must be a directory");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            assert!(!should_exist, "{label} is missing: {}", path.display());
        }
        Err(error) => panic!("cannot inspect {label} {}: {error}", path.display()),
    }
}

fn sorted_cf_names(path: &Path) -> Vec<String> {
    let mut names = DB::list_cf(&StorageOptions::default().options, path)
        .unwrap_or_else(|error| panic!("list CFs for {}: {error}", path.display()));
    names.sort();
    names
}

fn expected_legacy_cf_names(profile: MigrationSourceProfile) -> Vec<String> {
    let count = if profile == MigrationSourceProfile::BaseV1SixCf { 6 } else { 7 };
    let mut names: Vec<String> = CANONICAL_COLUMN_FAMILY_NAMES[..count]
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    names.sort();
    names
}

fn expected_v2_cf_names() -> Vec<String> {
    let mut names: Vec<String> = CANONICAL_COLUMN_FAMILY_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    names.sort();
    names
}

fn read_v1_identity(instance: &Path) -> (u64, u64) {
    let bytes = fs::read(instance.join(STORAGE_MANIFEST_FILE)).expect("read Vector-v1 manifest");
    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("parse Vector-v1 manifest");
    assert_eq!(value["version"].as_u64(), Some(1));
    assert!(value.get("manifest_version").is_none());
    (
        value["storage_incarnation"].as_u64().expect("v1 incarnation"),
        value["next_generation"].as_u64().expect("v1 generation"),
    )
}

fn assert_disk_kind(
    path: &Path,
    expected: ExpectedDiskKind,
    profile: MigrationSourceProfile,
    instance_id: u32,
    root_manifest: &RootStorageManifestV2,
    v1_identities: &[(u64, u64)],
) {
    if expected == ExpectedDiskKind::Missing {
        assert_directory_or_missing(path, false, "migration instance");
        return;
    }
    assert_directory_or_missing(path, true, "migration instance");
    let actual_cf = sorted_cf_names(path);
    let manifest_path = path.join(STORAGE_MANIFEST_FILE);
    match expected {
        ExpectedDiskKind::Legacy => {
            assert_eq!(actual_cf, expected_legacy_cf_names(profile));
            if profile == MigrationSourceProfile::BaseV1SixCf {
                assert!(!manifest_path.exists(), "Base-v1 authority must not have a manifest");
            } else {
                assert_eq!(read_v1_identity(path), v1_identities[instance_id as usize]);
            }
        }
        ExpectedDiskKind::V2 => {
            assert_eq!(actual_cf, expected_v2_cf_names());
            let manifest = InstanceStorageManifestV2::read_from_dir(path)
                .expect("read and digest-validate interrupted v2 manifest");
            manifest
                .validate_root_binding(instance_id, root_manifest)
                .expect("interrupted v2 manifest must bind current root digest");
            assert_eq!(manifest.manifest_digest().as_str().len(), 64);
            if profile == MigrationSourceProfile::VectorSetV1SevenCf {
                assert_eq!(
                    (manifest.storage_incarnation(), manifest.next_generation()),
                    v1_identities[instance_id as usize],
                    "Vector-v1 identity must survive every interrupted v2 copy"
                );
            }
        }
        ExpectedDiskKind::PartialBaseVectorCf => {
            assert_eq!(profile, MigrationSourceProfile::BaseV1SixCf);
            assert_eq!(actual_cf, expected_v2_cf_names());
            assert!(!manifest_path.exists(), "partial Base shadow must fail before v2 manifest");
        }
        ExpectedDiskKind::Missing => unreachable!(),
    }
}

fn expected_layout(
    fault_name: &str,
    phase: MigrationPhase,
    current_instance: u32,
    instance_id: u32,
) -> (ExpectedDiskKind, ExpectedDiskKind, ExpectedDiskKind) {
    use ExpectedDiskKind::{Legacy, Missing, PartialBaseVectorCf, V2};
    match phase {
        MigrationPhase::SourceDetected | MigrationPhase::ShadowPrepared => (Legacy, Missing, Missing),
        MigrationPhase::InstanceCopied => {
            let shadow = if instance_id < current_instance {
                V2
            } else if instance_id == current_instance {
                if fault_name.starts_with("vector-cf-created-") { PartialBaseVectorCf } else { Legacy }
            } else {
                Missing
            };
            (Legacy, shadow, Missing)
        }
        MigrationPhase::InstanceUpgraded => (
            Legacy,
            if instance_id <= current_instance { V2 } else { Missing },
            Missing,
        ),
        MigrationPhase::AllInstancesVerified | MigrationPhase::SwitchPrepared => (Legacy, V2, Missing),
        MigrationPhase::OldMovedToBackup => {
            if instance_id < current_instance {
                (V2, Missing, Legacy)
            } else if instance_id == current_instance {
                (Missing, V2, Legacy)
            } else {
                (Legacy, V2, Missing)
            }
        }
        MigrationPhase::ShadowPromoted => {
            if instance_id <= current_instance {
                (V2, Missing, Legacy)
            } else {
                (Legacy, V2, Missing)
            }
        }
        MigrationPhase::NewStorageOpened | MigrationPhase::Committed => (V2, Missing, Legacy),
        MigrationPhase::RollbackWindowClosed => panic!("no retry fault targets RollbackWindowClosed"),
    }
}

fn copy_tree(source: &Path, target: &Path) {
    let metadata = fs::symlink_metadata(source).expect("inspect authority source");
    assert!(metadata.is_dir() && !metadata.file_type().is_symlink());
    assert!(!target.exists(), "authority copy target must start absent");
    fs::create_dir(target).expect("create authority copy target");
    for entry in fs::read_dir(source).expect("read authority source") {
        let entry = entry.expect("authority entry");
        let file_type = entry.file_type().expect("authority entry type");
        assert!(!file_type.is_symlink(), "authority copy refuses symlink");
        let destination = target.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            assert!(file_type.is_file(), "authority entry must be a file or directory");
            fs::copy(entry.path(), destination).expect("copy authority file");
        }
    }
}

fn verify_v2_copy_data(
    source: &Path,
    validation_root: &Path,
    source_profile: MigrationSourceProfile,
    original_instance_id: u32,
    disk_kind: ExpectedDiskKind,
    interrupted_root: &RootStorageManifestV2,
    v1_identities: &[(u64, u64)],
) {
    assert!(matches!(disk_kind, ExpectedDiskKind::V2 | ExpectedDiskKind::PartialBaseVectorCf));
    assert!(!validation_root.exists(), "V2 validation root must start absent");
    fs::create_dir(validation_root).expect("create V2 validation root");
    let copied_instance = validation_root.join("0");
    copy_tree(source, &copied_instance);

    let original_manifest = if disk_kind == ExpectedDiskKind::V2 {
        let manifest = InstanceStorageManifestV2::read_from_dir(source)
            .expect("read interrupted V2 manifest before isolated validation");
        manifest
            .validate_root_binding(original_instance_id, interrupted_root)
            .expect("interrupted V2 manifest must bind interrupted Root");
        Some(manifest)
    } else {
        None
    };
    let copied_manifest_path = copied_instance.join(STORAGE_MANIFEST_FILE);
    if copied_manifest_path.exists() {
        fs::remove_file(&copied_manifest_path).expect("remove copied manifest before strict rebind");
    }
    let strict_root = RootStorageManifestV2::new(
        Uuid::new_v4(),
        1,
        interrupted_root.slot_mapping_version(),
        slot_mapping_digest(1),
        None,
    )
    .expect("create strict single-instance Root manifest");
    strict_root
        .write_to_dir_atomically(validation_root)
        .expect("write strict V2 Root manifest");
    let (instance_uuid, storage_incarnation, next_generation) = original_manifest
        .as_ref()
        .map(|manifest| {
            (
                manifest.instance_uuid(),
                manifest.storage_incarnation(),
                manifest.next_generation(),
            )
        })
        .unwrap_or_else(|| (Uuid::new_v4(), 1, 1));
    let strict_instance = InstanceStorageManifestV2::new(
        0,
        instance_uuid,
        &strict_root,
        storage_incarnation,
        next_generation,
    )
    .expect("create strict single-instance manifest");
    strict_instance
        .write_to_dir_atomically(&copied_instance)
        .expect("write strict single-instance manifest");
    strict_instance
        .validate_root_binding(0, &strict_root)
        .expect("strict validation manifest pairing");
    if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
        assert_eq!(
            (storage_incarnation, next_generation),
            v1_identities[original_instance_id as usize],
            "isolated V2 validation must preserve Vector-v1 identity"
        );
    }

    let runtime = tokio::runtime::Runtime::new().expect("create V2 validation runtime");
    let _runtime_guard = runtime.enter();
    let mut storage = Storage::new(1, 0);
    let receiver = storage
        .open(Arc::new(StorageOptions::default()), validation_root)
        .expect("exact Head Storage must strictly reopen isolated V2 copy");
    if std::env::var_os("KIWI_COMPAT_CORRUPT_V2_VALIDATION").is_some() {
        storage
            .set(
                &partition_key("string", original_instance_id as usize),
                b"corrupted-v2-copy",
            )
            .expect("inject V2 validation-copy corruption");
    }
    verify_partitioned_instance(&storage, source_profile, original_instance_id as usize);
    runtime.block_on(storage.shutdown());
    storage.close();
    drop(receiver);
    let reopened_root = RootStorageManifestV2::read_from_dir(validation_root)
        .expect("re-read strict V2 Root manifest");
    let reopened_instance = InstanceStorageManifestV2::read_from_dir(&copied_instance)
        .expect("re-read strict V2 instance manifest");
    reopened_instance
        .validate_root_binding(0, &reopened_root)
        .expect("strict V2 pairing must survive reopen/read");
}

fn legacy_authority_instance(root: &Path, transaction: &MigrationTransaction, instance_id: u32) -> PathBuf {
    let backup = root.join(&transaction.backup_name).join(instance_id.to_string());
    if backup.is_dir() { backup } else { root.join(instance_id.to_string()) }
}

fn read_authority_v1_identities(root: &Path, profile: MigrationSourceProfile) -> Vec<(u64, u64)> {
    if profile == MigrationSourceProfile::BaseV1SixCf {
        return Vec::new();
    }
    let root_manifest = RootStorageManifestV2::read_from_dir(root).expect("read interrupted root");
    let transaction = root_manifest.migration().expect("interrupted journal");
    (0..2_u32)
        .map(|instance_id| read_v1_identity(&legacy_authority_instance(root, transaction, instance_id)))
        .collect()
}

fn assert_exact_root_entries(
    root: &Path,
    transaction: &MigrationTransaction,
    expected_layouts: &[(ExpectedDiskKind, ExpectedDiskKind, ExpectedDiskKind)],
    shadow_root_exists: bool,
    backup_root_exists: bool,
) {
    let mut expected: HashSet<String> = [ROOT_STORAGE_MANIFEST_FILE.to_string()].into_iter().collect();
    for (instance_id, (live, _, _)) in expected_layouts.iter().enumerate() {
        if *live != ExpectedDiskKind::Missing {
            expected.insert(instance_id.to_string());
        }
    }
    if shadow_root_exists {
        expected.insert(transaction.shadow_name.clone());
    }
    if backup_root_exists {
        expected.insert(transaction.backup_name.clone());
    }
    let actual: HashSet<String> = fs::read_dir(root)
        .expect("read interrupted root entries")
        .map(|entry| entry.expect("root entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(actual, expected, "interrupted root entries must match the durable phase");

    for (directory, selector, should_exist, label) in [
        (
            root.join(&transaction.shadow_name),
            1_usize,
            shadow_root_exists,
            "shadow root",
        ),
        (
            root.join(&transaction.backup_name),
            2_usize,
            backup_root_exists,
            "backup root",
        ),
    ] {
        if !should_exist {
            continue;
        }
        let expected_children: HashSet<String> = expected_layouts
            .iter()
            .enumerate()
            .filter_map(|(instance_id, layout)| {
                let kind = match selector {
                    1 => layout.1,
                    2 => layout.2,
                    _ => unreachable!(),
                };
                (kind != ExpectedDiskKind::Missing).then(|| instance_id.to_string())
            })
            .collect();
        let actual_children: HashSet<String> = fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("read {label} {}: {error}", directory.display()))
            .map(|entry| {
                let entry = entry.expect("migration root entry");
                let file_type = entry.file_type().expect("migration root entry type");
                assert!(file_type.is_dir() && !file_type.is_symlink());
                entry.file_name().to_string_lossy().into_owned()
            })
            .collect();
        assert_eq!(
            actual_children, expected_children,
            "{label} children must match the durable phase"
        );
    }
}

fn assert_interrupted_state(
    root: &Path,
    authority_root: &Path,
    v2_authority_root: &Path,
    source_profile: MigrationSourceProfile,
    fault_name: &str,
    expected_phase: MigrationPhase,
    expected_instance: u32,
    v1_identities: &[(u64, u64)],
) -> usize {
    let root_bytes = fs::read(root.join(ROOT_STORAGE_MANIFEST_FILE)).expect("read root journal bytes");
    let root_manifest = RootStorageManifestV2::read_from_dir(root)
        .expect("read and digest-validate interrupted root journal");
    assert_eq!(
        ManifestDigest::compute_payload(&root_bytes).expect("compute root payload digest"),
        root_manifest.manifest_digest().clone()
    );
    root_manifest.validate_runtime_topology(2).expect("validate interrupted topology");
    assert_eq!(root_manifest.db_instance_num(), 2);
    assert_eq!(root_manifest.manifest_digest().as_str().len(), 64);
    let transaction = root_manifest.migration().expect("interrupted migration journal");
    assert_eq!(transaction.from_schema, 1);
    assert_eq!(transaction.to_schema, 2);
    assert_eq!(transaction.source_profile, source_profile);
    assert_eq!(transaction.phase, expected_phase);
    assert_eq!(transaction.current_instance, expected_instance);
    assert_eq!(transaction.source_name, "live");
    assert_eq!(transaction.shadow_name, format!(".kiwi-shadow-{}", transaction.transaction_id));
    assert_eq!(transaction.backup_name, format!(".kiwi-backup-{}", transaction.transaction_id));
    assert_safe_basename(&transaction.source_name);
    assert_safe_basename(&transaction.shadow_name);
    assert_safe_basename(&transaction.backup_name);

    let layouts: Vec<_> = (0..2_u32)
        .map(|instance_id| expected_layout(fault_name, expected_phase, expected_instance, instance_id))
        .collect();
    let shadow_root_exists = !matches!(
        expected_phase,
        MigrationPhase::SourceDetected | MigrationPhase::NewStorageOpened | MigrationPhase::Committed
    );
    let backup_root_exists = matches!(
        expected_phase,
        MigrationPhase::OldMovedToBackup
            | MigrationPhase::ShadowPromoted
            | MigrationPhase::NewStorageOpened
            | MigrationPhase::Committed
    );
    let shadow_root = root.join(&transaction.shadow_name);
    let backup_root = root.join(&transaction.backup_name);
    assert_directory_or_missing(&shadow_root, shadow_root_exists, "shadow root");
    assert_directory_or_missing(&backup_root, backup_root_exists, "backup root");
    assert_exact_root_entries(root, transaction, &layouts, shadow_root_exists, backup_root_exists);

    assert!(!v2_authority_root.exists(), "V2 authority root must start absent");
    fs::create_dir(v2_authority_root).expect("create V2 authority root");
    let mut v2_copy_count = 0_usize;
    for (instance_id, (live, shadow, backup)) in layouts.iter().copied().enumerate() {
        let instance_id = instance_id as u32;
        for (label, path, disk_kind) in [
            ("live", root.join(instance_id.to_string()), live),
            ("shadow", shadow_root.join(instance_id.to_string()), shadow),
            ("backup", backup_root.join(instance_id.to_string()), backup),
        ] {
            assert_disk_kind(
                &path,
                disk_kind,
                source_profile,
                instance_id,
                &root_manifest,
                v1_identities,
            );
            if matches!(disk_kind, ExpectedDiskKind::V2 | ExpectedDiskKind::PartialBaseVectorCf) {
                verify_v2_copy_data(
                    &path,
                    &v2_authority_root.join(format!("{label}-{instance_id}")),
                    source_profile,
                    instance_id,
                    disk_kind,
                    &root_manifest,
                    v1_identities,
                );
                v2_copy_count += 1;
            }
        }
    }
    assert_eq!(
        fs::read(root.join(ROOT_STORAGE_MANIFEST_FILE)).expect("re-read original interrupted journal"),
        root_bytes,
        "isolated V2 data validation must not advance or rewrite the original journal"
    );

    assert!(!authority_root.exists(), "authority verification root must start absent");
    fs::create_dir(authority_root).expect("create authority verification root");
    for instance_id in 0..2_u32 {
        let source = legacy_authority_instance(root, transaction, instance_id);
        assert_disk_kind(
            &source, ExpectedDiskKind::Legacy, source_profile, instance_id,
            &root_manifest, v1_identities,
        );
        copy_tree(&source, &authority_root.join(instance_id.to_string()));
    }
    v2_copy_count
}

fn assert_committed(
    root: &Path,
    source_profile: MigrationSourceProfile,
    v1_identities: &[(u64, u64)],
    expected_phase: MigrationPhase,
) {
    let root_manifest = RootStorageManifestV2::read_from_dir(root).expect("read v2 root manifest");
    let transaction = root_manifest.migration().expect("migration journal");
    assert_eq!(transaction.source_profile, source_profile);
    assert_eq!(transaction.phase, expected_phase);
    assert_safe_basename(&transaction.source_name);
    assert_safe_basename(&transaction.shadow_name);
    assert_safe_basename(&transaction.backup_name);
    let mut expected: Vec<String> = CANONICAL_COLUMN_FAMILY_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    expected.sort();
    for instance_id in 0..2_u32 {
        let instance = root.join(instance_id.to_string());
        let mut actual = DB::list_cf(&StorageOptions::default().options, &instance)
            .expect("list migrated column families");
        actual.sort();
        assert_eq!(actual, expected);
        let manifest = InstanceStorageManifestV2::read_from_dir(&instance)
            .expect("read v2 instance manifest");
        manifest
            .validate_root_binding(instance_id, &root_manifest)
            .expect("instance must bind exact root manifest");
        if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
            assert_eq!(
                (manifest.storage_incarnation(), manifest.next_generation()),
                v1_identities[instance_id as usize],
                "Vector-v1 identity must survive migration"
            );
        }
    }
}

fn parse_fault(value: &str) -> (MigrationFaultPoint, MigrationPhase, u32) {
    match value {
        "source-detected" => (MigrationFaultPoint::AfterSourceDetected, MigrationPhase::SourceDetected, 0),
        "shadow-prepared" => (MigrationFaultPoint::AfterShadowPrepared, MigrationPhase::ShadowPrepared, 0),
        "instance-copied-0" => (MigrationFaultPoint::AfterInstanceCopied(0), MigrationPhase::InstanceCopied, 0),
        "instance-copied-1" => (MigrationFaultPoint::AfterInstanceCopied(1), MigrationPhase::InstanceCopied, 1),
        "vector-cf-created-0" => (MigrationFaultPoint::AfterVectorCfCreatedBeforeInstanceManifest(0), MigrationPhase::InstanceCopied, 0),
        "vector-cf-created-1" => (MigrationFaultPoint::AfterVectorCfCreatedBeforeInstanceManifest(1), MigrationPhase::InstanceCopied, 1),
        "instance-upgraded-0" => (MigrationFaultPoint::AfterInstanceUpgraded(0), MigrationPhase::InstanceUpgraded, 0),
        "instance-upgraded-1" => (MigrationFaultPoint::AfterInstanceUpgraded(1), MigrationPhase::InstanceUpgraded, 1),
        "all-verified" => (MigrationFaultPoint::AfterAllInstancesVerified, MigrationPhase::AllInstancesVerified, 1),
        "switch-prepared" => (MigrationFaultPoint::AfterSwitchPrepared, MigrationPhase::SwitchPrepared, 0),
        "old-moved-0" => (MigrationFaultPoint::AfterOldMovedToBackup(0), MigrationPhase::OldMovedToBackup, 0),
        "old-moved-1" => (MigrationFaultPoint::AfterOldMovedToBackup(1), MigrationPhase::OldMovedToBackup, 1),
        "shadow-promoted-0" => (MigrationFaultPoint::AfterShadowPromoted(0), MigrationPhase::ShadowPromoted, 0),
        "shadow-promoted-1" => (MigrationFaultPoint::AfterShadowPromoted(1), MigrationPhase::ShadowPromoted, 1),
        "new-storage-opened" => (MigrationFaultPoint::AfterNewStorageOpened, MigrationPhase::NewStorageOpened, 1),
        "committed" => (MigrationFaultPoint::AfterCommitted, MigrationPhase::Committed, 1),
        other => panic!("unknown migration fault: {other}"),
    }
}

#[test]
fn head_upgrade_and_reopen_external() {
    let root = fixture_root();
    let source_profile = profile();
    let identities = if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
        read_v1_identities(&root)
    } else {
        Vec::new()
    };
    open_and_verify(&root, source_profile).expect("Head must migrate exact historical fixture");
    open_and_verify(&root, source_profile).expect("Head must reopen migrated fixture");
    assert_committed(&root, source_profile, &identities, MigrationPhase::Committed);
}

#[test]
fn inject_fault_and_assert_interrupted_external() {
    let root = fixture_root();
    let authority_root = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_AUTHORITY_ROOT")
            .expect("KIWI_COMPAT_AUTHORITY_ROOT is required"),
    );
    let v2_authority_root = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_V2_AUTHORITY_ROOT")
            .expect("KIWI_COMPAT_V2_AUTHORITY_ROOT is required"),
    );
    let v2_count_file = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_V2_COUNT_FILE")
            .expect("KIWI_COMPAT_V2_COUNT_FILE is required"),
    );
    let source_profile = profile();
    let identities = if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
        read_v1_identities(&root)
    } else {
        Vec::new()
    };
    let fault_name = std::env::var("KIWI_COMPAT_FAULT").expect("KIWI_COMPAT_FAULT is required");
    let (fault, expected_phase, expected_instance) = parse_fault(&fault_name);
    let guard = fail_next_storage_migration(&root, fault);
    let error = attempt_open(&root).expect_err("fault must stop migration before admission");
    assert!(error.contains("injected storage migration failure"), "unexpected error: {error}");
    drop(guard);
    let interrupted = RootStorageManifestV2::read_from_dir(&root).expect("read interrupted journal");
    let transaction = interrupted.migration().expect("interrupted migration journal");
    assert_eq!(transaction.source_profile, source_profile);
    assert_eq!(transaction.phase, expected_phase);
    assert_eq!(transaction.current_instance, expected_instance);
    let v2_copy_count = assert_interrupted_state(
        &root,
        &authority_root,
        &v2_authority_root,
        source_profile,
        &fault_name,
        expected_phase,
        expected_instance,
        &identities,
    );
    println!(
        "INTERRUPTED_V2_DATA PASS profile={source_profile:?} fault={fault_name} copies={v2_copy_count}"
    );
    fs::write(&v2_count_file, format!("{v2_copy_count}\n"))
        .expect("write completed V2-copy count");
}

#[test]
fn resume_after_asserted_fault_external() {
    let root = fixture_root();
    let source_profile = profile();
    let identities = read_authority_v1_identities(&root, source_profile);
    open_and_verify(&root, source_profile).expect("Head must resume exact historical migration");
    assert_committed(&root, source_profile, &identities, MigrationPhase::Committed);
}

#[test]
fn rollback_to_legacy_external() {
    let root = fixture_root();
    let source_profile = profile();
    let identities = if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
        read_v1_identities(&root)
    } else {
        Vec::new()
    };
    let guard = fail_next_storage_migration(&root, MigrationFaultPoint::AfterCommitted);
    let error = attempt_open(&root).expect_err("committed fault must stop before admission");
    assert!(error.contains("injected storage migration failure"));
    drop(guard);
    assert!(
        recover_or_rollback_before_admission(&root, 2, &StorageOptions::default())
            .expect("rollback verified exact historical backup")
    );
    assert!(!root.join(ROOT_STORAGE_MANIFEST_FILE).exists());
    assert_eq!(
        classify_storage_root(&root, 2, &StorageOptions::default()).expect("classify rollback"),
        Some(source_profile)
    );
    if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
        assert_eq!(read_v1_identities(&root), identities, "rollback must preserve v1 identity");
    }
    assert!(std::fs::read_dir(&root).expect("read rollback root").all(|entry| {
        let name = entry.expect("root entry").file_name();
        let name = name.to_string_lossy();
        !name.starts_with(".kiwi-shadow-") && !name.starts_with(".kiwi-backup-")
    }));
}

#[test]
fn close_window_and_reject_rollback_external() {
    let root = fixture_root();
    let source_profile = profile();
    let identities = if source_profile == MigrationSourceProfile::VectorSetV1SevenCf {
        read_v1_identities(&root)
    } else {
        Vec::new()
    };
    open_and_verify(&root, source_profile).expect("migrate before closing rollback window");
    assert!(close_rollback_window(&root).expect("close rollback window"));
    let error = recover_or_rollback_before_admission(&root, 2, &StorageOptions::default())
        .expect_err("closed rollback window must reject restore");
    assert!(error.to_string().contains("RollbackWindowClosed"));
    open_and_verify(&root, source_profile).expect("closed storage must remain reopenable");
    assert_committed(
        &root,
        source_profile,
        &identities,
        MigrationPhase::RollbackWindowClosed,
    );
}
'''


HEAD_SNAPSHOT_DRIVER = LICENSE_HEADER + r'''

#![allow(clippy::unwrap_used)]

#[path = "../../storage/tests/support/legacy_storage.rs"]
mod legacy_storage;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use conf::raft_type::KiwiNode;
use openraft::RaftSnapshotBuilder;
use openraft::SnapshotMeta;
use openraft::storage::RaftStateMachine;
use raft::snapshot_archive::{pack_dir_to_vec, unpack_tar_to_dir, unpacked_checkpoint_root};
use raft::state_machine::{KiwiStateMachine, PauseController, StorageAccessPermit};
use rocksdb::{DB, Options};
use storage::{
    CanonicalVector, InstanceStorageManifestV2, ManifestDigest, MigrationPhase,
    ParsedSnapshotMeta, RaftSnapshotMeta, RootStorageManifestV2, StorageOptions,
    ZsetScoreMember, format_base_value::DataType,
    slot_indexer::{SlotIndexer, key_to_slot_id},
    storage::Storage,
};

const STRING_KEY: &[u8] = b"compat:string";
const HASH_KEY: &[u8] = b"compat:hash";
const HASH_FIELD: &[u8] = b"field";
const ZSET_KEY: &[u8] = b"compat:zset";
const ZSET_MEMBER: &[u8] = b"member";
const TTL_KEY: &[u8] = b"compat:ttl";
const VECTOR_ELEMENT: &[u8] = b"authority-element";

struct NoopPauseController;
struct NoopStorageAccessPermit;

#[derive(Default)]
struct CountingPauseController {
    requested: AtomicUsize,
    entered: AtomicUsize,
    resumed: AtomicUsize,
}

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

impl PauseController for CountingPauseController {
    fn request_pause(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.requested.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    }

    fn enter(
        self: Arc<Self>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>,
    > {
        self.entered.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Box::new(NoopStorageAccessPermit) as Box<dyn StorageAccessPermit> })
    }

    fn resume(&self) {
        self.resumed.fetch_add(1, Ordering::SeqCst);
    }
}

fn assert_history(storage: &Storage) -> anyhow::Result<()> {
    assert_eq!(storage.get(STRING_KEY)?, "string-value");
    assert_eq!(
        storage.hget(HASH_KEY, HASH_FIELD)?,
        Some("hash-value".to_string())
    );
    assert_eq!(storage.zscore(ZSET_KEY, ZSET_MEMBER)?, Some(b"42.5".to_vec()));
    assert_eq!(storage.get(TTL_KEY)?, "ttl-value");
    assert!(storage.ttl(TTL_KEY)? > 0);
    assert_eq!(storage.get(b"compat:post-restore")?, "accepted");
    Ok(())
}

fn assert_manifest_pairing(root_path: &std::path::Path) -> anyhow::Result<()> {
    let root = RootStorageManifestV2::read_from_dir(root_path)?;
    root.validate_runtime_topology(2)?;
    assert_eq!(root.db_instance_num(), 2);
    assert_eq!(root.manifest_digest().as_str().len(), 64);
    for instance_id in 0..2_u32 {
        let instance = InstanceStorageManifestV2::read_from_dir(
            &root_path.join(instance_id.to_string()),
        )?;
        assert_eq!(instance.instance_id(), instance_id);
        instance.validate_root_binding(instance_id, &root)?;
        assert_eq!(instance.manifest_digest().as_str().len(), 64);
    }
    Ok(())
}

fn authority_key(kind: &str, instance_id: usize) -> Vec<u8> {
    let indexer = SlotIndexer::new(2);
    for nonce in 0..10_000_u32 {
        let key = format!("compat:target-authority:{kind}:{instance_id}:{nonce}").into_bytes();
        if indexer.get_instance_id(key_to_slot_id(&key)) == instance_id {
            return key;
        }
    }
    panic!("cannot find target-authority key for instance {instance_id}");
}

fn write_target_authority(storage: &Storage) -> anyhow::Result<()> {
    for instance_id in 0..2_usize {
        let string_key = authority_key("string", instance_id);
        let hash_key = authority_key("hash", instance_id);
        let zset_key = authority_key("zset", instance_id);
        let ttl_key = authority_key("ttl", instance_id);
        let sentinel_key = authority_key("sentinel", instance_id);
        let vector_key = authority_key("vector", instance_id);
        storage.set(
            &string_key,
            format!("authority-string-{instance_id}").as_bytes(),
        )?;
        storage.hset(
            &hash_key,
            HASH_FIELD,
            format!("authority-hash-{instance_id}").as_bytes(),
        )?;
        storage.zadd(
            &zset_key,
            &[ZsetScoreMember::new(
                42.5,
                format!("authority-member-{instance_id}").into_bytes(),
            )],
        )?;
        storage.set(
            &ttl_key,
            format!("authority-ttl-{instance_id}").as_bytes(),
        )?;
        assert!(storage.expire(&ttl_key, 86_400)?);
        storage.set(
            &sentinel_key,
            format!("authority-sentinel-{instance_id}").as_bytes(),
        )?;
        let vector = CanonicalVector::from_values(&[instance_id as f32 + 0.5, 1.5])?;
        assert!(storage.vadd(&vector_key, VECTOR_ELEMENT, &vector)?);
    }
    Ok(())
}

fn verify_target_authority(storage: &Storage) -> anyhow::Result<()> {
    for instance_id in 0..2_usize {
        let string_key = authority_key("string", instance_id);
        let hash_key = authority_key("hash", instance_id);
        let zset_key = authority_key("zset", instance_id);
        let ttl_key = authority_key("ttl", instance_id);
        let sentinel_key = authority_key("sentinel", instance_id);
        let vector_key = authority_key("vector", instance_id);
        assert_eq!(
            storage.get(&string_key)?,
            format!("authority-string-{instance_id}"),
            "target authority String mismatch instance {instance_id}"
        );
        assert_eq!(
            storage.hget(&hash_key, HASH_FIELD)?,
            Some(format!("authority-hash-{instance_id}")),
            "target authority Hash mismatch instance {instance_id}"
        );
        assert_eq!(
            storage.zscore(
                &zset_key,
                format!("authority-member-{instance_id}").as_bytes(),
            )?,
            Some(b"42.5".to_vec()),
            "target authority ZSet mismatch instance {instance_id}"
        );
        assert_eq!(
            storage.get(&ttl_key)?,
            format!("authority-ttl-{instance_id}"),
            "target authority TTL value mismatch instance {instance_id}"
        );
        assert!(
            storage.ttl(&ttl_key)? > 0,
            "target authority TTL expiry missing instance {instance_id}"
        );
        assert_eq!(
            storage.get(&sentinel_key)?,
            format!("authority-sentinel-{instance_id}"),
            "target authority sentinel mismatch instance {instance_id}"
        );
        let vector = storage
            .vemb(&vector_key, VECTOR_ELEMENT)?
            .expect("target-authority Vector member exists");
        assert_eq!(vector.len(), 2, "target authority Vector dimension instance {instance_id}");
        assert!(
            vector.iter().all(|value| value.is_finite()),
            "target authority Vector contains non-finite values instance {instance_id}"
        );
        assert!(
            (vector[0] - (instance_id as f64 + 0.5)).abs() <= 1e-6,
            "target authority Vector first element mismatch instance {instance_id}"
        );
        assert!(
            (vector[1] - 1.5).abs() <= 1e-6,
            "target authority Vector second element mismatch instance {instance_id}"
        );
    }
    Ok(())
}

fn read_snapshot_meta(path: &Path) -> anyhow::Result<SnapshotMeta<u64, KiwiNode>> {
    let meta: SnapshotMeta<u64, KiwiNode> = serde_json::from_slice(&std::fs::read(path)?)?;
    anyhow::ensure!(!meta.snapshot_id.is_empty(), "SnapshotMeta id is empty");
    Ok(meta)
}

fn assert_archive_meta_matches(
    checkpoint_root: &Path,
    meta: &SnapshotMeta<u64, KiwiNode>,
) -> anyhow::Result<()> {
    let file_meta = ParsedSnapshotMeta::read_from_dir(checkpoint_root)?;
    let expected_index = meta.last_log_id.map(|log_id| log_id.index).unwrap_or(0);
    let expected_term = meta
        .last_log_id
        .map(|log_id| log_id.leader_id.term)
        .unwrap_or(0);
    assert_eq!(file_meta.metadata().last_included_index, expected_index);
    assert_eq!(file_meta.metadata().last_included_term, expected_term);
    Ok(())
}

fn mutate_exact_base_archive(
    bytes: &[u8],
    meta: &SnapshotMeta<u64, KiwiNode>,
    mutation_root: &Path,
    mutation: &str,
    instance_id: usize,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(!mutation_root.exists(), "mutation root must start absent");
    unpack_tar_to_dir(bytes, mutation_root)?;
    let checkpoint_root = unpacked_checkpoint_root(mutation_root);
    assert_archive_meta_matches(&checkpoint_root, meta)?;
    anyhow::ensure!(instance_id < 2, "Base mutation instance must be 0 or 1");
    let instance = checkpoint_root.join(instance_id.to_string());
    let mut db_options = Options::default();
    db_options.create_missing_column_families(true);
    let db = DB::open_cf_descriptors(
        &db_options,
        &instance,
        legacy_storage::descriptors(&legacy_storage::BASE_CF_NAMES),
    )?;
    match mutation {
        "unknown-cf" => db.create_cf("unknown_cf", &Options::default())?,
        "vector-cf" => db.create_cf("vector_data_cf", &Options::default())?,
        "vector-meta" => {
            let meta_cf = db.cf_handle("default").expect("default CF");
            db.put_cf(&meta_cf, b"compat:forbidden-vector-meta", [DataType::VectorSet as u8])?;
        }
        other => anyhow::bail!("unknown exact Base archive mutation: {other}"),
    }
    drop(db);
    assert_archive_meta_matches(&checkpoint_root, meta)?;
    Ok(pack_dir_to_vec(&checkpoint_root)?)
}

fn target_authority_bytes(target: &Path) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut bytes = vec![std::fs::read(target.join("__kiwi_root_storage_manifest"))?];
    for instance_id in 0..2_u32 {
        bytes.push(std::fs::read(
            target
                .join(instance_id.to_string())
                .join("__kiwi_storage_manifest"),
        )?);
    }
    Ok(bytes)
}

async fn assert_full_install_rejected_before_pause(
    meta: &SnapshotMeta<u64, KiwiNode>,
    bytes: Vec<u8>,
    target: PathBuf,
    snapshot_work: PathBuf,
    expected_error: &str,
    expected_location: Option<&str>,
) -> anyhow::Result<()> {
    anyhow::ensure!(!target.exists(), "negative install target must start absent");
    std::fs::create_dir_all(&snapshot_work)?;
    let mut live_storage = Storage::new(2, 0);
    let live_receiver = live_storage.open(Arc::new(StorageOptions::default()), &target)?;
    write_target_authority(&live_storage)?;
    verify_target_authority(&live_storage)?;
    let manifest_bytes_before = target_authority_bytes(&target)?;
    let pairing_before = capture_exact_pairing(&target)?;
    let logical_digests_before = live_storage.logical_snapshot_digests()?;
    anyhow::ensure!(logical_digests_before.len() == 2, "target must expose two logical digests");
    let original = Arc::new(live_storage);
    let storage_swap = Arc::new(ArcSwap::from(Arc::clone(&original)));
    let pause = Arc::new(CountingPauseController::default());
    let mut state_machine = KiwiStateMachine::new(
        2,
        Arc::clone(&storage_swap),
        target.clone(),
        snapshot_work,
        Arc::clone(&pause) as Arc<dyn PauseController>,
        None,
    );
    let error = state_machine
        .install_snapshot(meta, Box::new(std::io::Cursor::new(bytes)))
        .await
        .expect_err("mutated snapshot must be rejected");
    assert!(
        error.to_string().contains(expected_error),
        "unexpected install rejection: {error}"
    );
    if let Some(expected_location) = expected_location {
        assert!(
            error.to_string().contains(expected_location),
            "install rejection did not identify polluted instance: {error}"
        );
    }
    assert_eq!(pause.requested.load(Ordering::SeqCst), 0);
    assert_eq!(pause.entered.load(Ordering::SeqCst), 0);
    assert_eq!(pause.resumed.load(Ordering::SeqCst), 0);
    let unchanged = storage_swap.load_full();
    assert!(Arc::ptr_eq(&unchanged, &original));
    if let Some(mutation) = std::env::var_os("KIWI_COMPAT_CORRUPT_TARGET_AFTER_REJECT") {
        match mutation.to_string_lossy().as_ref() {
            "hash-instance1" => {
                unchanged.hset(&authority_key("hash", 1), HASH_FIELD, b"corrupted-hash")?;
            }
            "zset-instance1" => {
                unchanged.zadd(
                    &authority_key("zset", 1),
                    &[ZsetScoreMember::new(7.0, b"authority-member-1".to_vec())],
                )?;
            }
            "ttl-instance1" => {
                unchanged.set(&authority_key("ttl", 1), b"corrupted-ttl")?;
            }
            other => anyhow::bail!("unknown target-authority corruption: {other}"),
        }
    }
    verify_target_authority(&unchanged)?;
    assert_eq!(unchanged.logical_snapshot_digests()?, logical_digests_before);
    assert_eq!(capture_exact_pairing(&target)?, pairing_before);
    assert_eq!(target_authority_bytes(&target)?, manifest_bytes_before);

    drop(unchanged);
    drop(state_machine);
    drop(storage_swap);
    drop(pause);
    let mut original = Arc::try_unwrap(original)
        .map_err(|_| anyhow::anyhow!("negative target Storage still has Arc owners"))?;
    original.shutdown().await;
    original.close();
    drop(live_receiver);
    let mut reopened = Storage::new(2, 0);
    let reopened_receiver = reopened.open(Arc::new(StorageOptions::default()), &target)?;
    verify_target_authority(&reopened)?;
    assert_eq!(reopened.logical_snapshot_digests()?, logical_digests_before);
    assert_eq!(capture_exact_pairing(&target)?, pairing_before);
    assert_eq!(target_authority_bytes(&target)?, manifest_bytes_before);
    reopened.shutdown().await;
    reopened.close();
    drop(reopened_receiver);
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExactPairing {
    root_id: String,
    root_digest: String,
    instances: Vec<(u32, String, u64, u64, String)>,
}

fn capture_exact_pairing(root_path: &Path) -> anyhow::Result<ExactPairing> {
    let root = RootStorageManifestV2::read_from_dir(root_path)?;
    root.validate_runtime_topology(2)?;
    let mut instances = Vec::new();
    for instance_id in 0..2_u32 {
        let instance = InstanceStorageManifestV2::read_from_dir(
            &root_path.join(instance_id.to_string()),
        )?;
        instance.validate_root_binding(instance_id, &root)?;
        instances.push((
            instance.instance_id(),
            instance.instance_uuid().to_string(),
            instance.storage_incarnation(),
            instance.next_generation(),
            instance.manifest_digest().as_str().to_string(),
        ));
    }
    Ok(ExactPairing {
        root_id: root.manifest_id().to_string(),
        root_digest: root.manifest_digest().as_str().to_string(),
        instances,
    })
}

fn write_head_data(storage: &Storage) -> anyhow::Result<()> {
    storage.set(STRING_KEY, b"string-value")?;
    storage.hset(HASH_KEY, HASH_FIELD, b"hash-value")?;
    storage.zadd(
        ZSET_KEY,
        &[ZsetScoreMember::new(42.5, ZSET_MEMBER.to_vec())],
    )?;
    storage.set(TTL_KEY, b"ttl-value")?;
    assert!(storage.expire(TTL_KEY, 86_400)?);
    for index in 0..64_u32 {
        let key = format!("compat:head-sentinel:{index}");
        let value = format!("value:{index}");
        storage.set(key.as_bytes(), value.as_bytes())?;
    }
    Ok(())
}

fn verify_head_data(storage: &Storage) -> anyhow::Result<()> {
    assert_eq!(storage.get(STRING_KEY)?, "string-value");
    assert_eq!(
        storage.hget(HASH_KEY, HASH_FIELD)?,
        Some("hash-value".to_string())
    );
    assert_eq!(storage.zscore(ZSET_KEY, ZSET_MEMBER)?, Some(b"42.5".to_vec()));
    assert_eq!(storage.get(TTL_KEY)?, "ttl-value");
    assert!(storage.ttl(TTL_KEY)? > 0);
    for index in 0..64_u32 {
        let key = format!("compat:head-sentinel:{index}");
        let value = format!("value:{index}");
        assert_eq!(storage.get(key.as_bytes())?, value);
    }
    Ok(())
}

fn mutate_head_v2_archive(
    bytes: &[u8],
    meta: &SnapshotMeta<u64, KiwiNode>,
    mutation_root: &Path,
    mutation: &str,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(!mutation_root.exists(), "Head mutation root must start absent");
    unpack_tar_to_dir(bytes, mutation_root)?;
    let checkpoint_root = unpacked_checkpoint_root(mutation_root);
    assert_archive_meta_matches(&checkpoint_root, meta)?;
    match mutation {
        "swap-instance-manifests" => {
            let manifest0 = checkpoint_root.join("0").join("__kiwi_storage_manifest");
            let manifest1 = checkpoint_root.join("1").join("__kiwi_storage_manifest");
            let temporary = checkpoint_root.join("manifest.swap.tmp");
            std::fs::rename(&manifest0, &temporary)?;
            std::fs::rename(&manifest1, &manifest0)?;
            std::fs::rename(&temporary, &manifest1)?;
        }
        "instance1-digest" => {
            let mut file_meta = RaftSnapshotMeta::read_from_dir(&checkpoint_root)?;
            file_meta.instance_manifests[1].manifest_digest =
                ManifestDigest::compute(b"tampered instance 1 digest");
            file_meta.write_to_dir(&checkpoint_root)?;
        }
        "instance1-incarnation" => {
            let mut file_meta = RaftSnapshotMeta::read_from_dir(&checkpoint_root)?;
            file_meta.instance_manifests[1].storage_incarnation = file_meta.instance_manifests[1]
                .storage_incarnation
                .wrapping_add(1)
                .max(1);
            file_meta.write_to_dir(&checkpoint_root)?;
        }
        "root-digest" => {
            let mut file_meta = RaftSnapshotMeta::read_from_dir(&checkpoint_root)?;
            file_meta.root_manifest_digest = Some(ManifestDigest::compute(b"tampered root digest"));
            file_meta.write_to_dir(&checkpoint_root)?;
        }
        other => anyhow::bail!("unknown Head v2 archive mutation: {other}"),
    }
    assert_archive_meta_matches(&checkpoint_root, meta)?;
    Ok(pack_dir_to_vec(&checkpoint_root)?)
}

#[tokio::test]
async fn restore_exact_base_v1_archive_external() -> anyhow::Result<()> {
    let archive = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_ARCHIVE").expect("KIWI_COMPAT_ARCHIVE is required"),
    );
    let snapshot_meta_path = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_SNAPSHOT_META")
            .expect("KIWI_COMPAT_SNAPSHOT_META is required"),
    );
    let target = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_TARGET").expect("KIWI_COMPAT_TARGET is required"),
    );
    let snapshot_work = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_SNAPSHOT_WORK")
            .expect("KIWI_COMPAT_SNAPSHOT_WORK is required"),
    );
    let bytes = std::fs::read(&archive)?;
    anyhow::ensure!(!bytes.is_empty(), "exact Base v1 archive is empty");
    let snapshot_meta: SnapshotMeta<u64, KiwiNode> =
        serde_json::from_slice(&std::fs::read(&snapshot_meta_path)?)?;
    anyhow::ensure!(!snapshot_meta.snapshot_id.is_empty(), "Base SnapshotMeta id is empty");
    std::fs::create_dir_all(&snapshot_work)?;

    let mut live_storage = Storage::new(2, 0);
    let live_receiver = live_storage.open(Arc::new(StorageOptions::default()), &target)?;
    let storage_swap = Arc::new(ArcSwap::from_pointee(live_storage));
    let mut state_machine = KiwiStateMachine::new(
        2,
        Arc::clone(&storage_swap),
        target.clone(),
        snapshot_work,
        Arc::new(NoopPauseController),
        None,
    );
    state_machine
        .install_snapshot(&snapshot_meta, Box::new(std::io::Cursor::new(bytes)))
        .await?;

    let restored = storage_swap.load_full();
    restored.set(b"compat:post-restore", b"accepted")?;
    assert_history(&restored)?;
    assert_manifest_pairing(&target)?;

    drop(state_machine);
    drop(storage_swap);
    let mut restored = Arc::try_unwrap(restored)
        .map_err(|_| anyhow::anyhow!("restored Storage still has Arc owners"))?;
    restored.shutdown().await;
    restored.close();
    drop(live_receiver);

    let root = RootStorageManifestV2::read_from_dir(&target)?;
    assert_eq!(
        root.migration().expect("historical snapshot migration journal").phase,
        MigrationPhase::RollbackWindowClosed
    );
    let mut reopened = Storage::new(2, 0);
    let reopened_receiver = reopened.open(Arc::new(StorageOptions::default()), &target)?;
    assert_history(&reopened)?;
    assert_manifest_pairing(&target)?;
    reopened.shutdown().await;
    reopened.close();
    drop(reopened_receiver);
    Ok(())
}

#[tokio::test]
async fn reject_mutated_exact_base_v1_archive_external() -> anyhow::Result<()> {
    let archive = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_ARCHIVE").expect("KIWI_COMPAT_ARCHIVE is required"),
    );
    let snapshot_meta_path = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_SNAPSHOT_META")
            .expect("KIWI_COMPAT_SNAPSHOT_META is required"),
    );
    let mutation =
        std::env::var("KIWI_COMPAT_MUTATION").expect("KIWI_COMPAT_MUTATION is required");
    let mutation_instance: usize = std::env::var("KIWI_COMPAT_MUTATION_INSTANCE")
        .expect("KIWI_COMPAT_MUTATION_INSTANCE is required")
        .parse()?;
    let mutation_root = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_MUTATION_ROOT")
            .expect("KIWI_COMPAT_MUTATION_ROOT is required"),
    );
    let target = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_TARGET").expect("KIWI_COMPAT_TARGET is required"),
    );
    let snapshot_work = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_SNAPSHOT_WORK")
            .expect("KIWI_COMPAT_SNAPSHOT_WORK is required"),
    );
    let bytes = std::fs::read(&archive)?;
    anyhow::ensure!(!bytes.is_empty(), "exact Base v1 archive is empty");
    let snapshot_meta = read_snapshot_meta(&snapshot_meta_path)?;
    let mutated = mutate_exact_base_archive(
        &bytes,
        &snapshot_meta,
        &mutation_root,
        &mutation,
        mutation_instance,
    )?;
    let expected_error = match mutation.as_str() {
        "unknown-cf" => "unregistered legacy column-family layout",
        "vector-cf" => "is missing its v1 manifest",
        "vector-meta" => "contains Vector Set metadata",
        _ => unreachable!(),
    };
    let expected_location = format!("invalid Base-v1 snapshot instance {mutation_instance}");
    assert_full_install_rejected_before_pause(
        &snapshot_meta,
        mutated,
        target,
        snapshot_work,
        expected_error,
        Some(&expected_location),
    )
    .await
}

#[tokio::test]
async fn head_v2_two_instance_exact_pairing_external() -> anyhow::Result<()> {
    let source = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_HEAD_SOURCE")
            .expect("KIWI_COMPAT_HEAD_SOURCE is required"),
    );
    let target = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_TARGET").expect("KIWI_COMPAT_TARGET is required"),
    );
    let build_work = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_BUILD_WORK")
            .expect("KIWI_COMPAT_BUILD_WORK is required"),
    );
    let install_work = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_SNAPSHOT_WORK")
            .expect("KIWI_COMPAT_SNAPSHOT_WORK is required"),
    );
    let negative_root = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_NEGATIVE_ROOT")
            .expect("KIWI_COMPAT_NEGATIVE_ROOT is required"),
    );
    let archive_path = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_HEAD_ARCHIVE")
            .expect("KIWI_COMPAT_HEAD_ARCHIVE is required"),
    );
    let meta_path = PathBuf::from(
        std::env::var_os("KIWI_COMPAT_HEAD_SNAPSHOT_META")
            .expect("KIWI_COMPAT_HEAD_SNAPSHOT_META is required"),
    );
    std::fs::create_dir_all(&build_work)?;
    std::fs::create_dir_all(&install_work)?;
    std::fs::create_dir_all(&negative_root)?;

    let mut source_storage = Storage::new(2, 0);
    let source_receiver =
        source_storage.open(Arc::new(StorageOptions::default()), &source)?;
    write_head_data(&source_storage)?;
    write_target_authority(&source_storage)?;
    verify_head_data(&source_storage)?;
    verify_target_authority(&source_storage)?;
    let expected_pairing = capture_exact_pairing(&source)?;
    assert_eq!(expected_pairing.instances.len(), 2);
    let source_storage = Arc::new(source_storage);
    let source_swap = Arc::new(ArcSwap::from(Arc::clone(&source_storage)));
    let mut source_state_machine = KiwiStateMachine::new(
        1,
        Arc::clone(&source_swap),
        source.clone(),
        build_work,
        Arc::new(NoopPauseController),
        None,
    );
    let mut builder = source_state_machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await?;
    let snapshot_meta = snapshot.meta.clone();
    let snapshot_bytes = snapshot.snapshot.into_inner();
    anyhow::ensure!(!snapshot_bytes.is_empty(), "Head v2 snapshot archive is empty");
    std::fs::write(&archive_path, &snapshot_bytes)?;
    std::fs::write(&meta_path, serde_json::to_vec_pretty(&snapshot_meta)?)?;
    drop(builder);
    drop(source_state_machine);
    drop(source_swap);
    let mut source_storage = Arc::try_unwrap(source_storage)
        .map_err(|_| anyhow::anyhow!("Head snapshot source Storage still has Arc owners"))?;
    source_storage.shutdown().await;
    source_storage.close();
    drop(source_receiver);

    let mut target_storage = Storage::new(2, 0);
    let target_receiver =
        target_storage.open(Arc::new(StorageOptions::default()), &target)?;
    target_storage.set(b"compat:target-authority", b"replace-me")?;
    let target_swap = Arc::new(ArcSwap::from_pointee(target_storage));
    let mut target_state_machine = KiwiStateMachine::new(
        2,
        Arc::clone(&target_swap),
        target.clone(),
        install_work,
        Arc::new(NoopPauseController),
        None,
    );
    target_state_machine
        .install_snapshot(
            &snapshot_meta,
            Box::new(std::io::Cursor::new(snapshot_bytes.clone())),
        )
        .await?;
    let installed = target_swap.load_full();
    verify_head_data(&installed)?;
    verify_target_authority(&installed)?;
    assert_eq!(capture_exact_pairing(&target)?, expected_pairing);
    installed.set(b"compat:head-post-install", b"accepted")?;

    drop(target_state_machine);
    drop(target_swap);
    let mut installed = Arc::try_unwrap(installed)
        .map_err(|_| anyhow::anyhow!("installed Head Storage still has Arc owners"))?;
    installed.shutdown().await;
    installed.close();
    drop(target_receiver);

    let mut reopened = Storage::new(2, 0);
    let reopened_receiver = reopened.open(Arc::new(StorageOptions::default()), &target)?;
    verify_head_data(&reopened)?;
    verify_target_authority(&reopened)?;
    assert_eq!(reopened.get(b"compat:head-post-install")?, "accepted");
    assert_eq!(capture_exact_pairing(&target)?, expected_pairing);
    reopened.shutdown().await;
    reopened.close();
    drop(reopened_receiver);

    for (mutation, expected_error) in [
        (
            "swap-instance-manifests",
            "instance_id 1 does not match expected 0",
        ),
        ("instance1-digest", "instance 1 manifest digest mismatch"),
        (
            "instance1-incarnation",
            "snapshot instance 1 incarnation metadata is inconsistent",
        ),
        ("root-digest", "root manifest digest mismatch"),
    ] {
        let case_root = negative_root.join(mutation);
        let mutation_root = case_root.join("unpack");
        let negative_target = case_root.join("target");
        let negative_work = case_root.join("work");
        let mutated = mutate_head_v2_archive(
            &snapshot_bytes,
            &snapshot_meta,
            &mutation_root,
            mutation,
        )?;
        assert_full_install_rejected_before_pause(
            &snapshot_meta,
            mutated,
            negative_target,
            negative_work,
            expected_error,
            None,
        )
        .await?;
    }
    Ok(())
}
'''


DRIVERS = {
    "base-storage": BASE_STORAGE_DRIVER,
    "vector-storage": VECTOR_STORAGE_DRIVER,
    "base-snapshot": BASE_SNAPSHOT_DRIVER,
    "head-storage": HEAD_STORAGE_DRIVER,
    "head-snapshot": HEAD_SNAPSHOT_DRIVER,
}


def verify_gate_contract(executed: list[str]) -> int:
    duplicates = sorted({gate for gate in executed if executed.count(gate) > 1})
    unknown = sorted(set(executed).difference(EXPECTED_GATES))
    missing = sorted(set(EXPECTED_GATES).difference(executed))
    if duplicates or unknown or missing:
        if duplicates:
            print(f"duplicate gates: {', '.join(duplicates)}", file=sys.stderr)
        if unknown:
            print(f"unknown gates: {', '.join(unknown)}", file=sys.stderr)
        if missing:
            print(f"missing gates: {', '.join(missing)}", file=sys.stderr)
        return 1
    return 0


def emit_rust(kind: str) -> int:
    sys.stdout.write(textwrap.dedent(DRIVERS[kind]).lstrip())
    return 0


def extract_executable(input_path: Path, target_name: str) -> int:
    candidates: list[str] = []
    with input_path.open("r", encoding="utf-8") as stream:
        for raw_line in stream:
            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if record.get("reason") != "compiler-artifact":
                continue
            target = record.get("target") or {}
            if target.get("name") != target_name or "test" not in target.get("kind", []):
                continue
            executable = record.get("executable")
            if executable:
                candidates.append(executable)
    unique = sorted(set(candidates))
    if len(unique) != 1:
        print(
            f"expected exactly one executable for {target_name}, found {len(unique)}: {unique}",
            file=sys.stderr,
        )
        return 1
    executable_path = Path(unique[0])
    if not executable_path.is_file():
        print(f"cargo executable does not exist: {executable_path}", file=sys.stderr)
        return 1
    print(executable_path)
    return 0


def render_cargo_diagnostics(input_path: Path) -> int:
    rendered = 0
    with input_path.open("r", encoding="utf-8") as stream:
        for raw_line in stream:
            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if record.get("reason") != "compiler-message":
                continue
            message = record.get("message") or {}
            diagnostic = message.get("rendered")
            if diagnostic:
                print(diagnostic, file=sys.stderr, end="" if diagnostic.endswith("\n") else "\n")
                rendered += 1
    if rendered == 0:
        print(f"cargo failed without rendered compiler diagnostics: {input_path}", file=sys.stderr)
    return 0


def normalize_cargo_key(key: str) -> str:
    return "".join(character for character in key.lower() if character not in "-_.")


def verify_cargo_config(input_path: Path) -> int:
    try:
        with input_path.open("rb") as stream:
            config = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as error:
        print(f"cannot parse Cargo config {input_path}: {error}", file=sys.stderr)
        return 1

    wrapper_keys = {"buildrustcwrapper", "buildrustcworkspacewrapper"}
    violations: list[str] = []

    def visit(value: object, path: tuple[str, ...]) -> None:
        if not isinstance(value, dict):
            return
        for raw_key, child in value.items():
            child_path = (*path, str(raw_key))
            normalized_path = "".join(normalize_cargo_key(part) for part in child_path)
            if normalized_path in wrapper_keys:
                violations.append(".".join(child_path))
            visit(child, child_path)

    visit(config, ())
    if violations:
        print(
            f"Cargo config declares a compiler wrapper: {input_path}: "
            f"{', '.join(sorted(set(violations)))}",
            file=sys.stderr,
        )
        return 1
    return 0


def verify_cargo_probe(input_path: Path, rustc_path: Path) -> int:
    expected = str(rustc_path)
    matching_commands: list[list[str]] = []
    with input_path.open("r", encoding="utf-8") as stream:
        for raw_line in stream:
            if "Running `" not in raw_line or "--crate-name kiwi_compat_cargo_probe" not in raw_line:
                continue
            command_text = raw_line.split("Running `", 1)[1].rsplit("`", 1)[0]
            try:
                tokens = shlex.split(command_text)
            except ValueError as error:
                print(f"cannot parse Cargo probe command: {error}", file=sys.stderr)
                return 1
            while tokens and "=" in tokens[0] and not tokens[0].startswith(("/", ".")):
                name, _, _ = tokens[0].partition("=")
                if not name.replace("_", "").isalnum():
                    break
                tokens.pop(0)
            if tokens:
                matching_commands.append(tokens)

    if len(matching_commands) != 1:
        print(
            f"expected one Cargo probe compiler command, found {len(matching_commands)}",
            file=sys.stderr,
        )
        return 1
    actual = matching_commands[0][0]
    if actual != expected:
        print(
            f"Cargo probe compiler process mismatch: expected {expected}, got {actual}",
            file=sys.stderr,
        )
        return 1
    return 0


COMMANDS = {
    "verify-gate-contract",
    "emit-rust",
    "extract-executable",
    "render-cargo-diagnostics",
    "verify-cargo-config",
    "verify-cargo-probe",
}


def reject_mixed_help(argv: list[str]) -> int:
    help_flags = {"-h", "--help"}
    if not any(argument in help_flags for argument in argv):
        return 0
    if len(argv) == 1 and argv[0] in help_flags:
        return 0
    if len(argv) == 2 and argv[0] in COMMANDS and argv[1] in help_flags:
        return 0
    print("help must be the only root argument or the only argument after a subcommand", file=sys.stderr)
    return 2


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    gate_contract = subparsers.add_parser("verify-gate-contract")
    gate_contract.add_argument("--executed", action="append", default=[])
    emit = subparsers.add_parser("emit-rust")
    emit.add_argument("--kind", choices=sorted(DRIVERS), required=True)
    executable = subparsers.add_parser("extract-executable")
    executable.add_argument("--input", type=Path, required=True)
    executable.add_argument("--target", required=True)
    diagnostics = subparsers.add_parser("render-cargo-diagnostics")
    diagnostics.add_argument("--input", type=Path, required=True)
    cargo_config = subparsers.add_parser("verify-cargo-config")
    cargo_config.add_argument("--input", type=Path, required=True)
    cargo_probe = subparsers.add_parser("verify-cargo-probe")
    cargo_probe.add_argument("--input", type=Path, required=True)
    cargo_probe.add_argument("--rustc", type=Path, required=True)
    return parser.parse_args(argv)


def main() -> int:
    argv = sys.argv[1:]
    mixed_help = reject_mixed_help(argv)
    if mixed_help != 0:
        return mixed_help
    args = parse_args(argv)
    if args.command == "verify-gate-contract":
        return verify_gate_contract(args.executed)
    if args.command == "emit-rust":
        return emit_rust(args.kind)
    if args.command == "extract-executable":
        return extract_executable(args.input, args.target)
    if args.command == "render-cargo-diagnostics":
        return render_cargo_diagnostics(args.input)
    if args.command == "verify-cargo-config":
        return verify_cargo_config(args.input)
    if args.command == "verify-cargo-probe":
        return verify_cargo_probe(args.input, args.rustc)
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
