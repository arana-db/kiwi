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

use std::ffi::CString;
use std::path::Path;
use std::sync::Arc;

use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use storage::{
    InstanceStorageManifestV2, RAFT_SNAPSHOT_META_FILE, ROOT_STORAGE_MANIFEST_FILE,
    RaftSnapshotMeta, RootStorageManifestV2, STORAGE_MANIFEST_FILE, StorageOptions,
    ZsetScoreMember, canonical_column_family_names, storage::Storage,
};

pub const STRING_KEY: &[u8] = b"base-v1:string";
pub const HASH_KEY: &[u8] = b"base-v1:hash";
pub const HASH_FIELD: &[u8] = b"field";
pub const ZSET_KEY: &[u8] = b"base-v1:zset";
pub const ZSET_MEMBER: &[u8] = b"member";
pub const TTL_KEY: &[u8] = b"base-v1:ttl";

pub struct BaseV1SnapshotIdentity {
    pub root_manifest_id: String,
    pub storage_incarnation: u64,
}

fn descriptor(name: &str) -> ColumnFamilyDescriptor {
    let mut options = Options::default();
    match name {
        "list_data_cf" => options.set_comparator(
            CString::new("floyd.ListsDataKeyComparator").expect("valid comparator name"),
            Box::new(|left, right| left.cmp(right)),
        ),
        "zset_score_cf" => options.set_comparator(
            CString::new("floyd.ZSetsScoreKeyComparator").expect("valid comparator name"),
            Box::new(|left, right| left.cmp(right)),
        ),
        _ => {}
    }
    ColumnFamilyDescriptor::new(name, options)
}

pub async fn create(checkpoint_root: &Path) -> anyhow::Result<BaseV1SnapshotIdentity> {
    std::fs::create_dir_all(checkpoint_root)?;
    let mut storage = Storage::new(1, 0);
    let storage_rx = storage.open(Arc::new(StorageOptions::default()), checkpoint_root)?;

    storage.set(STRING_KEY, b"string-value")?;
    storage.hset(HASH_KEY, HASH_FIELD, b"hash-value")?;
    storage.zadd(
        ZSET_KEY,
        &[ZsetScoreMember::new(42.5, ZSET_MEMBER.to_vec())],
    )?;
    storage.set(TTL_KEY, b"ttl-value")?;
    anyhow::ensure!(storage.expire(TTL_KEY, 3_600)?, "TTL fixture must expire");

    let old_root = RootStorageManifestV2::read_from_dir(checkpoint_root)?;
    let old_instance = InstanceStorageManifestV2::read_from_dir(&checkpoint_root.join("0"))?;
    let identity = BaseV1SnapshotIdentity {
        root_manifest_id: old_root.manifest_id().to_string(),
        storage_incarnation: old_instance.storage_incarnation(),
    };

    storage.shutdown().await;
    storage.close();
    drop(storage_rx);
    drop(storage);

    let instance = checkpoint_root.join("0");
    let names = canonical_column_family_names();
    let descriptors: Vec<_> = names.iter().map(|name| descriptor(name)).collect();
    let db = DB::open_cf_descriptors(&Options::default(), &instance, descriptors)?;
    db.drop_cf("vector_data_cf")?;
    drop(db);

    std::fs::remove_file(checkpoint_root.join(ROOT_STORAGE_MANIFEST_FILE))?;
    std::fs::remove_file(instance.join(STORAGE_MANIFEST_FILE))?;

    let legacy_meta = RaftSnapshotMeta {
        version: 1,
        last_included_index: 41,
        last_included_term: 7,
        storage_schema_version: 0,
        storage_incarnations: Vec::new(),
        root_manifest_id: None,
        root_manifest_digest: None,
        instance_manifests: Vec::new(),
        db_instance_num: 0,
        column_families: Vec::new(),
        vector_value_format_max: 0,
        logindex_collector_states: Vec::new(),
    };
    legacy_meta.write_to_dir(checkpoint_root)?;
    anyhow::ensure!(
        checkpoint_root.join(RAFT_SNAPSHOT_META_FILE).is_file(),
        "Base-v1 snapshot metadata must be written"
    );

    Ok(identity)
}
