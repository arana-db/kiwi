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

#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use kstd::lock_mgr::LockMgr;
use storage::search_types::{
    DistanceMetric, SearchDataType, SearchFieldSchema, SearchIndexSchema, VectorAlgorithm,
    VectorFieldSchema, VectorValueType,
};
use storage::storage::Storage;
use storage::{
    BgTaskHandler, Redis, SearchIndexManager, StorageOptions, safe_cleanup_test_db,
    unique_test_db_path,
};

fn open_test_redis() -> (Redis, PathBuf) {
    let test_db_path = unique_test_db_path();
    safe_cleanup_test_db(&test_db_path);

    let storage_options = Arc::new(StorageOptions::default());
    let (bg_task_handler, _) = BgTaskHandler::new();
    let lock_mgr = Arc::new(LockMgr::new(1000));
    let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);
    redis.open(test_db_path.to_str().unwrap()).unwrap();

    (redis, test_db_path)
}

fn open_test_storage() -> (Storage, PathBuf) {
    let test_db_path = unique_test_db_path();
    safe_cleanup_test_db(&test_db_path);

    let mut storage = Storage::new(1, 0);
    let _receiver = storage
        .open(Arc::new(StorageOptions::default()), &test_db_path)
        .unwrap();

    (storage, test_db_path)
}

fn bytes_set(items: &[Vec<u8>]) -> BTreeSet<Vec<u8>> {
    items.iter().cloned().collect()
}

fn f32_bytes(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn sample_schema() -> SearchIndexSchema {
    SearchIndexSchema {
        name: b"idx".to_vec(),
        on: SearchDataType::Hash,
        prefixes: vec![b"doc:".to_vec()],
        fields: vec![(
            b"emb".to_vec(),
            SearchFieldSchema::Vector(VectorFieldSchema {
                algorithm: VectorAlgorithm::Flat,
                value_type: VectorValueType::Float32,
                dim: 3,
                distance_metric: DistanceMetric::L2,
            }),
        )],
    }
}

#[test]
fn hget_raw_preserves_binary_payload() {
    let (redis, _path) = open_test_redis();
    let value = vec![0, 255, 1, 2, 3];

    assert_eq!(redis.hset(b"doc:1", b"emb", &value).unwrap(), 1);
    assert_eq!(redis.hget_raw(b"doc:1", b"emb").unwrap(), Some(value));
}

#[test]
fn hmget_raw_map_skips_missing_fields() {
    let (redis, _path) = open_test_redis();
    let a = vec![1, 2, 3, 4];
    let b = vec![5, 6, 7, 8];

    redis.hset(b"doc:1", b"a", &a).unwrap();
    redis.hset(b"doc:1", b"b", &b).unwrap();

    let result = redis
        .hmget_raw_map(
            b"doc:1",
            &[b"a".to_vec(), b"missing".to_vec(), b"b".to_vec()],
        )
        .unwrap();

    assert_eq!(result.get(b"a".as_slice()), Some(&a));
    assert_eq!(result.get(b"b".as_slice()), Some(&b));
    assert!(!result.contains_key(b"missing".as_slice()));
}

#[test]
fn scan_hash_keys_by_prefix_returns_only_live_hashes() {
    let (redis, _path) = open_test_redis();

    redis.hset(b"doc:1", b"emb", b"1").unwrap();
    redis.hset(b"doc:2", b"emb", b"2").unwrap();
    redis.hset(b"other:1", b"emb", b"3").unwrap();
    redis.set(b"doc:string", b"value").unwrap();

    let keys = redis.scan_hash_keys_by_prefix(b"doc:").unwrap();
    let keys = bytes_set(&keys);

    let expected = bytes_set(&[b"doc:1".to_vec(), b"doc:2".to_vec()]);
    assert_eq!(keys, expected);
}

#[test]
fn create_and_load_index_schema() {
    let (redis, _path) = open_test_redis();
    let manager = SearchIndexManager::new(&redis);
    let schema = sample_schema();

    manager.create_index(schema.clone()).unwrap();
    let loaded = manager.load_index(b"idx").unwrap().unwrap();

    assert_eq!(loaded.name, schema.name);
    assert_eq!(loaded.on, schema.on);
    assert_eq!(loaded.prefixes, schema.prefixes);
    assert_eq!(loaded.fields, schema.fields);
}

#[test]
fn flat_knn_search_returns_nearest_docs() {
    let (redis, _path) = open_test_redis();
    let manager = SearchIndexManager::new(&redis);
    let schema = sample_schema();

    manager.create_index(schema).unwrap();

    let docs = [
        (b"doc:1".as_slice(), f32_bytes(&[1.0, 0.0, 0.0])),
        (b"doc:2".as_slice(), f32_bytes(&[0.0, 1.0, 0.0])),
        (b"doc:3".as_slice(), f32_bytes(&[0.0, 0.0, 1.0])),
    ];

    for (doc_key, vector) in &docs {
        redis.hset(doc_key, b"emb", vector).unwrap();
        manager.refresh_hash_document(doc_key).unwrap();
    }

    let hits = manager
        .search_knn(b"idx", b"emb", &f32_bytes(&[1.0, 0.0, 0.0]), 2)
        .unwrap();
    let doc_keys: Vec<Vec<u8>> = hits.into_iter().map(|hit| hit.doc_key).collect();

    assert_eq!(doc_keys, vec![b"doc:1".to_vec(), b"doc:2".to_vec()]);
}

#[tokio::test]
async fn storage_hset_refreshes_search_index() {
    let (storage, _path) = open_test_storage();
    let manager = SearchIndexManager::new(storage.insts[0].as_ref());
    manager.create_index(sample_schema()).unwrap();

    let vector = f32_bytes(&[1.0, 0.0, 0.0]);
    storage.hset(b"doc:1", b"emb", &vector).unwrap();

    let hits = manager
        .search_knn(b"idx", b"emb", &f32_bytes(&[1.0, 0.0, 0.0]), 1)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_key, b"doc:1");
}

#[tokio::test]
async fn storage_hset_rejects_invalid_indexed_vector_before_hash_write() {
    let (storage, _path) = open_test_storage();
    let manager = SearchIndexManager::new(storage.insts[0].as_ref());
    manager.create_index(sample_schema()).unwrap();

    let invalid_vector = vec![1, 2, 3];
    let result = storage.hset(b"doc:1", b"emb", &invalid_vector);

    assert!(result.is_err());
    assert_eq!(storage.insts[0].hget_raw(b"doc:1", b"emb").unwrap(), None);
    let hits = manager
        .search_knn(b"idx", b"emb", &f32_bytes(&[1.0, 0.0, 0.0]), 10)
        .unwrap();
    assert!(hits.is_empty());
}

#[tokio::test]
async fn storage_hset_rejects_invalid_indexed_vector_without_overwriting_existing_hash() {
    let (storage, _path) = open_test_storage();
    let manager = SearchIndexManager::new(storage.insts[0].as_ref());
    manager.create_index(sample_schema()).unwrap();

    let original_vector = f32_bytes(&[1.0, 0.0, 0.0]);
    storage.hset(b"doc:1", b"emb", &original_vector).unwrap();

    let result = storage.hset(b"doc:1", b"emb", &[1, 2, 3]);

    assert!(result.is_err());
    assert_eq!(
        storage.insts[0].hget_raw(b"doc:1", b"emb").unwrap(),
        Some(original_vector)
    );
    let hits = manager
        .search_knn(b"idx", b"emb", &f32_bytes(&[1.0, 0.0, 0.0]), 10)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_key, b"doc:1");
}

#[tokio::test]
async fn storage_hdel_and_del_clear_search_index_entries() {
    let (storage, _path) = open_test_storage();
    let manager = SearchIndexManager::new(storage.insts[0].as_ref());
    manager.create_index(sample_schema()).unwrap();

    let vector = f32_bytes(&[1.0, 0.0, 0.0]);
    storage.hset(b"doc:1", b"emb", &vector).unwrap();
    storage.hset(b"doc:2", b"emb", &vector).unwrap();

    storage.hdel(b"doc:1", &[b"emb".to_vec()]).unwrap();
    let hits_after_hdel = manager
        .search_knn(b"idx", b"emb", &f32_bytes(&[1.0, 0.0, 0.0]), 10)
        .unwrap();
    let doc_keys_after_hdel: Vec<Vec<u8>> =
        hits_after_hdel.into_iter().map(|hit| hit.doc_key).collect();
    assert_eq!(doc_keys_after_hdel, vec![b"doc:2".to_vec()]);

    storage.del(&[b"doc:2".to_vec()]).unwrap();
    let hits_after_del = manager
        .search_knn(b"idx", b"emb", &f32_bytes(&[1.0, 0.0, 0.0]), 10)
        .unwrap();
    assert!(hits_after_del.is_empty());
}
