#!/usr/bin/env python3
"""Exact-ref Vector storage compatibility runner support."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import textwrap


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
use storage::{StorageOptions, ZsetScoreMember, storage::Storage};

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
    CanonicalVector, STORAGE_MANIFEST_FILE, StorageOptions, ZsetScoreMember, storage::Storage,
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

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use rocksdb::DB;
use storage::{
    CANONICAL_COLUMN_FAMILY_NAMES, InstanceStorageManifestV2, MigrationFaultPoint,
    MigrationPhase, MigrationSourceProfile, ROOT_STORAGE_MANIFEST_FILE, RootStorageManifestV2,
    STORAGE_MANIFEST_FILE, StorageOptions, classify_storage_root, close_rollback_window,
    fail_next_storage_migration, recover_or_rollback_before_admission, storage::Storage,
};

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

fn assert_committed(
    root: &Path,
    source_profile: MigrationSourceProfile,
    v1_identities: &[(u64, u64)],
) {
    let root_manifest = RootStorageManifestV2::read_from_dir(root).expect("read v2 root manifest");
    let transaction = root_manifest.migration().expect("migration journal");
    assert_eq!(transaction.source_profile, source_profile);
    assert!(matches!(transaction.phase, MigrationPhase::Committed | MigrationPhase::RollbackWindowClosed));
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
    assert_committed(&root, source_profile, &identities);
}

#[test]
fn migrate_fault_retry_external() {
    let root = fixture_root();
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
    assert_safe_basename(&transaction.source_name);
    assert_safe_basename(&transaction.shadow_name);
    assert_safe_basename(&transaction.backup_name);
    assert!(root.join(ROOT_STORAGE_MANIFEST_FILE).is_file());
    open_and_verify(&root, source_profile).expect("Head must resume exact historical migration");
    assert_committed(&root, source_profile, &identities);
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
    assert_committed(&root, source_profile, &identities);
}
'''


HEAD_SNAPSHOT_DRIVER = LICENSE_HEADER + r'''

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use conf::raft_type::KiwiNode;
use openraft::SnapshotMeta;
use openraft::storage::RaftStateMachine;
use raft::state_machine::{KiwiStateMachine, PauseController, StorageAccessPermit};
use storage::{MigrationPhase, RootStorageManifestV2, StorageOptions, storage::Storage};

const STRING_KEY: &[u8] = b"compat:string";
const HASH_KEY: &[u8] = b"compat:hash";
const HASH_FIELD: &[u8] = b"field";
const ZSET_KEY: &[u8] = b"compat:zset";
const ZSET_MEMBER: &[u8] = b"member";
const TTL_KEY: &[u8] = b"compat:ttl";

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
    assert_eq!(restored.get(STRING_KEY)?, "string-value");
    assert_eq!(restored.hget(HASH_KEY, HASH_FIELD)?, Some("hash-value".to_string()));
    assert_eq!(restored.zscore(ZSET_KEY, ZSET_MEMBER)?, Some(b"42.5".to_vec()));
    assert_eq!(restored.get(TTL_KEY)?, "ttl-value");
    assert!(restored.ttl(TTL_KEY)? > 0);
    restored.set(b"compat:post-restore", b"accepted")?;

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
    assert_eq!(reopened.get(b"compat:post-restore")?, "accepted");
    reopened.shutdown().await;
    reopened.close();
    drop(reopened_receiver);
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


def parse_args() -> argparse.Namespace:
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "verify-gate-contract":
        return verify_gate_contract(args.executed)
    if args.command == "emit-rust":
        return emit_rust(args.kind)
    if args.command == "extract-executable":
        return extract_executable(args.input, args.target)
    if args.command == "render-cargo-diagnostics":
        return render_cargo_diagnostics(args.input)
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
