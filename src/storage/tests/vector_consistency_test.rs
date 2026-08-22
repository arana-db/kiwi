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

use std::path::Path;
use std::sync::Arc;

use kstd::lock_mgr::LockMgr;
use rocksdb::IteratorMode;
use storage::format_vector::VectorMeta;
use storage::format_vector_member_key::{ParsedVectorMemberDataKey, VectorMemberDataKey};
use storage::slot_indexer::key_to_slot_id;
use storage::storage::Storage;
use storage::{
    BaseMetaKey, BgTaskHandler, CanonicalVector, ColumnFamilyIndex, QuantizationType, Redis,
    StorageOptions, safe_cleanup_test_db, unique_test_db_path,
};

const META_COUNT_RANGE: std::ops::Range<usize> = 1..9;
const META_FORMAT_OFFSET: usize = 17;
const META_METRIC_OFFSET: usize = 19;
const META_DATA_REVISION_RANGE: std::ops::Range<usize> = 25..33;

fn open_redis(path: &Path) -> Redis {
    let (bg_task_handler, _) = BgTaskHandler::new();
    let mut redis = Redis::new(
        Arc::new(StorageOptions::default()),
        1,
        Arc::new(bg_task_handler),
        Arc::new(LockMgr::new(1000)),
    );
    redis
        .open(path.to_str().expect("test path must be UTF-8"))
        .expect("open test Redis");
    redis
}

fn with_redis(test: impl FnOnce(&Redis)) {
    let path = unique_test_db_path();
    safe_cleanup_test_db(&path);
    let redis = open_redis(&path);
    test(&redis);
    drop(redis);
    safe_cleanup_test_db(&path);
}

fn vector(dimension: usize) -> CanonicalVector {
    CanonicalVector::from_values(&vec![1.0; dimension]).expect("valid vector")
}

fn add_members(redis: &Redis, key: &[u8], count: usize, dimension: usize) {
    let vector = vector(dimension);
    for index in 0..count {
        redis
            .vadd(key, format!("e{index:03}").as_bytes(), &vector)
            .expect("add vector member");
    }
}

fn vector_cf(redis: &Redis) -> Arc<rocksdb::BoundColumnFamily<'_>> {
    redis
        .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
        .expect("VectorDataCF")
}

fn meta_cf(redis: &Redis) -> Arc<rocksdb::BoundColumnFamily<'_>> {
    redis
        .get_cf_handle(ColumnFamilyIndex::MetaCF)
        .expect("MetaCF")
}

fn member_entries(redis: &Redis) -> Vec<(Vec<u8>, Vec<u8>)> {
    redis
        .db()
        .expect("db")
        .iterator_cf(&vector_cf(redis), IteratorMode::Start)
        .map(|entry| {
            let (key, value) = entry.expect("member entry");
            (key.to_vec(), value.to_vec())
        })
        .collect()
}

fn first_member_for(redis: &Redis, user_key: &[u8]) -> (Vec<u8>, Vec<u8>) {
    member_entries(redis)
        .into_iter()
        .find(|(encoded_key, _)| {
            ParsedVectorMemberDataKey::decode(encoded_key)
                .map(|key| key.key() == user_key)
                .unwrap_or(false)
        })
        .expect("member for user key")
}

fn meta_raw(redis: &Redis, key: &[u8]) -> Vec<u8> {
    redis
        .db()
        .expect("db")
        .get_cf(
            &meta_cf(redis),
            BaseMetaKey::new(key).encode().expect("meta key"),
        )
        .expect("read meta")
        .expect("stored meta")
}

fn put_meta_raw(redis: &Redis, key: &[u8], value: &[u8]) {
    redis
        .db()
        .expect("db")
        .put_cf(
            &meta_cf(redis),
            BaseMetaKey::new(key).encode().expect("meta key"),
            value,
        )
        .expect("write raw meta");
}

fn expect_invalid(redis: &Redis, fragment: &str) {
    let error = redis
        .validate_vector_consistency()
        .expect_err("corrupt vector state must fail full validation");
    assert!(
        error.to_string().contains(fragment),
        "expected {fragment:?} in validation error: {error}"
    );
}

#[test]
fn full_validation_detects_corrupt_member_after_first_64_entries() {
    with_redis(|redis| {
        add_members(redis, b"vs", 65, 2);
        let (last_key, _) = member_entries(redis).pop().expect("65th member");
        redis
            .db()
            .expect("db")
            .put_cf(&vector_cf(redis), last_key, b"corrupt")
            .expect("corrupt 65th member");

        expect_invalid(redis, "vector value");
    });
}

#[test]
fn full_validation_rejects_wrong_storage_incarnation() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        let (old_key, value) = first_member_for(redis, b"vs");
        let parsed = ParsedVectorMemberDataKey::decode(&old_key).expect("decode member");
        let wrong_key = VectorMemberDataKey {
            key: parsed.key(),
            storage_incarnation: parsed.storage_incarnation() + 1,
            generation_sequence: parsed.generation_sequence(),
            element: parsed.element(),
        }
        .encode_full()
        .expect("encode wrong incarnation");
        let db = redis.db().expect("db");
        db.delete_cf(&vector_cf(redis), old_key)
            .expect("delete old member");
        db.put_cf(&vector_cf(redis), wrong_key, value)
            .expect("write wrong member");

        expect_invalid(redis, "storage incarnation");
    });
}

#[test]
fn full_validation_rejects_member_generation_without_matching_meta() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        let (old_key, value) = first_member_for(redis, b"vs");
        let parsed = ParsedVectorMemberDataKey::decode(&old_key).expect("decode member");
        let wrong_key = VectorMemberDataKey {
            key: parsed.key(),
            storage_incarnation: parsed.storage_incarnation(),
            generation_sequence: parsed.generation_sequence() + 1,
            element: parsed.element(),
        }
        .encode_full()
        .expect("encode wrong generation");
        let db = redis.db().expect("db");
        db.delete_cf(&vector_cf(redis), old_key)
            .expect("delete old member");
        db.put_cf(&vector_cf(redis), wrong_key, value)
            .expect("write wrong member");

        expect_invalid(redis, "generation");
    });
}

#[test]
fn full_validation_rejects_member_without_base_meta() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        redis
            .db()
            .expect("db")
            .delete_cf(
                &meta_cf(redis),
                BaseMetaKey::new(b"vs").encode().expect("meta key"),
            )
            .expect("delete meta");

        expect_invalid(redis, "missing base meta");
    });
}

#[test]
fn full_validation_rejects_non_vector_base_meta_for_member() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        redis.set(b"string", b"value").expect("write string");
        let string_meta = meta_raw(redis, b"string");
        put_meta_raw(redis, b"vs", &string_meta);

        expect_invalid(redis, "non-vector base meta");
    });
}

#[test]
fn full_validation_rejects_meta_count_greater_than_members() {
    with_redis(|redis| {
        add_members(redis, b"vs", 2, 2);
        let mut meta = meta_raw(redis, b"vs");
        meta[META_COUNT_RANGE].copy_from_slice(&3_u64.to_le_bytes());
        put_meta_raw(redis, b"vs", &meta);

        expect_invalid(redis, "count mismatch");
    });
}

#[test]
fn full_validation_rejects_meta_count_less_than_members() {
    with_redis(|redis| {
        add_members(redis, b"vs", 2, 2);
        let mut meta = meta_raw(redis, b"vs");
        meta[META_COUNT_RANGE].copy_from_slice(&1_u64.to_le_bytes());
        put_meta_raw(redis, b"vs", &meta);

        expect_invalid(redis, "count mismatch");
    });
}

#[test]
fn full_validation_rejects_meta_without_member_range() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        let (member_key, _) = first_member_for(redis, b"vs");
        redis
            .db()
            .expect("db")
            .delete_cf(&vector_cf(redis), member_key)
            .expect("delete member");

        expect_invalid(redis, "member range");
    });
}

#[test]
fn full_validation_rejects_member_dimension_mismatch() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        add_members(redis, b"other", 1, 3);
        let (target_key, _) = first_member_for(redis, b"vs");
        let (_, dimension_three_value) = first_member_for(redis, b"other");
        redis
            .db()
            .expect("db")
            .put_cf(&vector_cf(redis), target_key, dimension_three_value)
            .expect("write mismatched dimension");

        expect_invalid(redis, "dimension mismatch");
    });
}

#[test]
fn full_validation_rejects_member_quantization_mismatch() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        let (target_key, mut binary_value) = first_member_for(redis, b"vs");
        binary_value[2] = QuantizationType::Binary as u8;
        binary_value[12..16].copy_from_slice(&1_u32.to_le_bytes());
        binary_value.truncate(17);
        redis
            .db()
            .expect("db")
            .put_cf(&vector_cf(redis), target_key, binary_value)
            .expect("write mismatched quantization");

        expect_invalid(redis, "quantization mismatch");
    });
}

#[test]
fn full_validation_rejects_unknown_member_key_codec_version() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        let (old_key, value) = first_member_for(redis, b"vs");
        let mut bad_key = old_key.clone();
        bad_key[0] = 0xFF;
        let db = redis.db().expect("db");
        db.delete_cf(&vector_cf(redis), old_key)
            .expect("delete old member");
        db.put_cf(&vector_cf(redis), bad_key, value)
            .expect("write unknown key codec");

        expect_invalid(redis, "codec version");
    });
}

#[test]
fn full_validation_rejects_unknown_vector_value_codec_version() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        let (key, mut value) = first_member_for(redis, b"vs");
        value[1] = 0xFF;
        redis
            .db()
            .expect("db")
            .put_cf(&vector_cf(redis), key, value)
            .expect("write unknown value codec");

        expect_invalid(redis, "vector value format");
    });
}

#[test]
fn full_validation_rejects_invalid_meta_metric_or_format() {
    for (offset, fragment) in [
        (META_FORMAT_OFFSET, "vector meta format"),
        (META_METRIC_OFFSET, "vector metric"),
    ] {
        with_redis(|redis| {
            add_members(redis, b"vs", 1, 2);
            let mut meta = meta_raw(redis, b"vs");
            meta[offset] = 0xFF;
            put_meta_raw(redis, b"vs", &meta);

            expect_invalid(redis, fragment);
        });
    }
}

#[test]
fn full_validation_rejects_zero_or_invalid_data_revision() {
    with_redis(|redis| {
        add_members(redis, b"vs", 1, 2);
        let mut meta = meta_raw(redis, b"vs");
        meta[META_DATA_REVISION_RANGE].copy_from_slice(&0_u64.to_le_bytes());
        put_meta_raw(redis, b"vs", &meta);

        expect_invalid(redis, "data revision");
    });
}

#[test]
fn vector_mutations_never_decrease_data_revision() {
    with_redis(|redis| {
        let key = b"vs";
        let vector = vector(2);
        let mut revisions = Vec::new();
        redis.vadd(key, b"a", &vector).expect("create");
        revisions.push(
            VectorMeta::decode(&meta_raw(redis, key))
                .expect("decode meta")
                .data_revision(),
        );
        redis.vadd(key, b"a", &vector).expect("update");
        revisions.push(
            VectorMeta::decode(&meta_raw(redis, key))
                .expect("decode meta")
                .data_revision(),
        );
        redis.vadd(key, b"b", &vector).expect("add b");
        revisions.push(
            VectorMeta::decode(&meta_raw(redis, key))
                .expect("decode meta")
                .data_revision(),
        );
        redis.vrem(key, b"a").expect("remove a");
        revisions.push(
            VectorMeta::decode(&meta_raw(redis, key))
                .expect("decode meta")
                .data_revision(),
        );

        assert!(revisions.iter().all(|revision| *revision != 0));
        assert!(revisions.windows(2).all(|pair| pair[1] >= pair[0]));
        let active_generation = VectorMeta::decode(&meta_raw(redis, key))
            .expect("decode active meta")
            .version();
        redis.del_key(key).expect("DEL active set");
        let tombstone = VectorMeta::decode(&meta_raw(redis, key)).expect("decode tombstone");
        assert!(tombstone.data_revision() >= *revisions.last().expect("active revision"));
        redis.vadd(key, b"c", &vector).expect("recreate after DEL");
        let recreated = VectorMeta::decode(&meta_raw(redis, key)).expect("decode recreated meta");
        assert_ne!(recreated.version(), active_generation);
        assert_ne!(recreated.data_revision(), 0);
        redis
            .validate_vector_consistency()
            .expect("mutated state remains consistent");
    });
}

#[test]
fn full_validation_accepts_del_tombstone_with_deferred_members() {
    with_redis(|redis| {
        add_members(redis, b"vs", 2, 2);
        redis.del_key(b"vs").expect("delete VectorSet");

        let report = redis
            .validate_vector_consistency()
            .expect("DEL tombstone and old members are a valid pre-compaction state");
        assert_eq!(report.instances, 1);
        assert_eq!(report.metas, 1);
        assert_eq!(report.members, 2);
    });
}

#[tokio::test]
async fn full_validation_accepts_multiple_sets_generations_and_instances() {
    let path = unique_test_db_path();
    safe_cleanup_test_db(&path);
    let mut storage = Storage::new(2, 0);
    let receiver = storage
        .open(Arc::new(StorageOptions::default()), &path)
        .expect("open two-instance storage");
    let vector = vector(2);
    let mut keys_by_instance = [None, None];
    for index in 0..10_000 {
        let key = format!("set-{index}").into_bytes();
        let instance = storage.slot_indexer.get_instance_id(key_to_slot_id(&key));
        keys_by_instance[instance].get_or_insert(key);
        if keys_by_instance.iter().all(Option::is_some) {
            break;
        }
    }
    let recreated_key = keys_by_instance[0].clone().expect("key for first instance");
    for (instance, key) in keys_by_instance.into_iter().enumerate() {
        let key = key.unwrap_or_else(|| panic!("no key found for instance {instance}"));
        storage.vadd(&key, b"a", &vector).expect("add a");
        storage.vadd(&key, b"b", &vector).expect("add b");
    }
    assert_eq!(
        storage.del(std::slice::from_ref(&recreated_key)).unwrap(),
        1
    );
    storage
        .vadd(&recreated_key, b"c", &vector)
        .expect("recreate first set with a new generation");

    let report = storage
        .validate_vector_consistency()
        .expect("multi-instance state is consistent");
    assert_eq!(report.instances, 2);
    assert_eq!(report.metas, 2);
    assert_eq!(report.members, 5);

    storage.shutdown().await;
    storage.close();
    drop(receiver);
    safe_cleanup_test_db(&path);
}
