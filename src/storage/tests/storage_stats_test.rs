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

use std::collections::HashMap;
use std::sync::Arc;

use client::storage_stats::{
    RealStorageStatsCollector, STORAGE_STATS_COLLECTOR, StorageStats, StorageStatsCollector,
};
use storage::slot_indexer::key_to_slot_id;
use storage::storage::Storage;
use storage::{StorageOptions, fail_next_rocks_batch_commit};

async fn collect_stats<T>(operation: impl FnOnce() -> T) -> (T, StorageStats) {
    let collector = Arc::new(RealStorageStatsCollector::new());
    let scoped: Arc<dyn StorageStatsCollector + Send + Sync> = collector.clone();
    let result = STORAGE_STATS_COLLECTOR
        .scope(scoped, async { operation() })
        .await;
    (result, collector.finish())
}

fn open_storage(instance_count: usize, path: &std::path::Path) -> Storage {
    let mut storage = Storage::new(instance_count, 0);
    let _background_tasks = storage
        .open(Arc::new(StorageOptions::default()), path)
        .unwrap();
    storage
}

#[tokio::test]
async fn setnx_and_hdel_record_only_actual_mutations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = open_storage(1, temp_dir.path());
    let key = b"stats:key";
    let value = b"value";

    let (set_result, stats) = collect_stats(|| storage.setnx(key, value)).await;
    assert_eq!(set_result.unwrap(), 1);
    assert_eq!(stats.keys_written, 1);
    assert_eq!(stats.bytes_written, (key.len() + value.len()) as u64);

    let (set_result, stats) = collect_stats(|| storage.setnx(key, value)).await;
    assert_eq!(set_result.unwrap(), 0);
    assert_eq!(stats, StorageStats::default());

    let hash_key = b"stats:hash";
    let field = b"field";
    storage.hset(hash_key, field, b"hash-value").unwrap();

    let (delete_result, stats) = collect_stats(|| storage.hdel(hash_key, &[field.to_vec()])).await;
    assert_eq!(delete_result.unwrap(), 1);
    assert_eq!(stats.keys_deleted, 1);
    assert_eq!(stats.bytes_deleted, hash_key.len() as u64);

    let (delete_result, stats) = collect_stats(|| storage.hdel(hash_key, &[field.to_vec()])).await;
    assert_eq!(delete_result.unwrap(), 0);
    assert_eq!(stats, StorageStats::default());
}

#[tokio::test]
async fn failed_mutations_do_not_record_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = open_storage(1, temp_dir.path());

    let (setex_result, stats) = collect_stats(|| storage.setex(b"key", 0, b"value")).await;
    assert!(setex_result.is_err());
    assert_eq!(stats, StorageStats::default());

    storage.set(b"wrong-type", b"string").unwrap();
    let (hset_result, stats) =
        collect_stats(|| storage.hset(b"wrong-type", b"field", b"value")).await;
    assert!(hset_result.is_err());
    assert_eq!(stats, StorageStats::default());

    let _fail_commit = fail_next_rocks_batch_commit(&temp_dir.path().join("0"));
    let (set_result, stats) = collect_stats(|| storage.set(b"commit", b"fails")).await;
    assert!(set_result.is_err());
    assert_eq!(stats, StorageStats::default());
}

#[tokio::test]
async fn successful_noops_do_not_record_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = open_storage(1, temp_dir.path());

    storage.set(b"range", b"value").unwrap();
    let (setrange_result, stats) = collect_stats(|| storage.setrange(b"range", 2, b"")).await;
    assert_eq!(setrange_result.unwrap(), 5);
    assert_eq!(stats, StorageStats::default());

    storage.hset(b"hash", b"field", b"value").unwrap();
    let (hset_result, stats) = collect_stats(|| storage.hset(b"hash", b"field", b"value")).await;
    assert_eq!(hset_result.unwrap(), 0);
    assert_eq!(stats, StorageStats::default());

    let (hset_result, stats) =
        collect_stats(|| storage.hset(b"hash", b"field", b"new-value")).await;
    assert_eq!(hset_result.unwrap(), 0);
    assert_eq!(stats.keys_written, 1);
    assert_eq!(
        stats.bytes_written,
        (b"hash".len() + b"new-value".len()) as u64
    );
}

#[tokio::test]
async fn missing_get_records_a_logical_read() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = open_storage(1, temp_dir.path());
    let key = b"missing";

    let (result, stats) = collect_stats(|| storage.get_binary(key)).await;
    assert!(result.is_err());
    assert_eq!(stats.keys_read, 1);
    assert_eq!(stats.bytes_read, key.len() as u64);
}

#[tokio::test]
async fn multi_instance_mset_records_every_completed_pair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = open_storage(3, temp_dir.path());

    let mut keys_by_instance = HashMap::new();
    for index in 0..10_000 {
        let key = format!("stats:mset:{index}").into_bytes();
        let instance_id = storage.slot_indexer.get_instance_id(key_to_slot_id(&key));
        keys_by_instance.entry(instance_id).or_insert(key);
        if keys_by_instance.len() == 3 {
            break;
        }
    }
    assert_eq!(keys_by_instance.len(), 3);

    let kvs: Vec<(Vec<u8>, Vec<u8>)> = keys_by_instance
        .into_values()
        .enumerate()
        .map(|(index, key)| (key, format!("value-{index}").into_bytes()))
        .collect();
    let expected_bytes = kvs
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum::<usize>() as u64;

    let (result, stats) = collect_stats(|| storage.mset(&kvs)).await;
    result.unwrap();
    assert_eq!(stats.keys_written, kvs.len() as u64);
    assert_eq!(stats.bytes_written, expected_bytes);
}

#[tokio::test]
async fn multi_instance_mset_preserves_stats_for_shards_committed_before_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = open_storage(3, temp_dir.path());

    let mut keys_by_instance = HashMap::new();
    for index in 0..10_000 {
        let key = format!("stats:partial:{index}").into_bytes();
        let instance_id = storage.slot_indexer.get_instance_id(key_to_slot_id(&key));
        keys_by_instance.entry(instance_id).or_insert(key);
        if keys_by_instance.contains_key(&0) && keys_by_instance.contains_key(&1) {
            break;
        }
    }

    let first_key = keys_by_instance.remove(&0).unwrap();
    let failing_key = keys_by_instance.remove(&1).unwrap();
    let first_value = b"committed".to_vec();
    let kvs = vec![
        (failing_key, b"rejected".to_vec()),
        (first_key.clone(), first_value.clone()),
    ];

    let _fail_commit = fail_next_rocks_batch_commit(&temp_dir.path().join("1"));
    let (result, stats) = collect_stats(|| storage.mset(&kvs)).await;

    assert!(result.is_err());
    assert_eq!(stats.keys_written, 1);
    assert_eq!(
        stats.bytes_written,
        (first_key.len() + first_value.len()) as u64
    );
    assert_eq!(storage.get_binary(&first_key).unwrap(), first_value);
}
