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

use std::{path::Path, sync::Arc, sync::Mutex, time::Duration, time::Instant};

use conf::vector_config::VectorConfig;
use kstd::lock_mgr::LockMgr;
use rocksdb::{IteratorMode, ReadOptions};
use storage::{
    BaseMetaKey, BgTaskHandler, CanonicalVector, ColumnFamilyIndex, FlatQueryCancel, Redis,
    STORAGE_MANIFEST_FILE, StorageOptions, VectorQuery, VectorSearchMode, VectorSearchOptions,
    VectorSetApplyError, VectorSetApplyResult, VectorSetBusinessError, VectorSetMutationV1,
    error::Error, format_vector::VectorMeta, safe_cleanup_test_db, unique_test_db_path,
};
use storage::{slot_indexer::key_to_slot_id, storage::Storage};

fn open_redis_with_options(path: &Path, storage_options: Arc<StorageOptions>) -> Redis {
    let (bg_task_handler, _) = BgTaskHandler::new();
    let lock_mgr = Arc::new(LockMgr::new(1000));
    let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);
    redis
        .open(path.to_str().expect("test path must be UTF-8"))
        .expect("open test db");
    redis
}

fn open_redis(path: &Path) -> Redis {
    open_redis_with_options(path, Arc::new(StorageOptions::default()))
}

fn with_redis(test: impl FnOnce(&Redis)) {
    let path = unique_test_db_path();
    safe_cleanup_test_db(&path);
    let redis = open_redis(&path);

    test(&redis);

    drop(redis);
    safe_cleanup_test_db(&path);
}

fn with_redis_vector_config(edit: impl FnOnce(&mut VectorConfig), test: impl FnOnce(&Redis)) {
    let path = unique_test_db_path();
    safe_cleanup_test_db(&path);
    let mut vector = VectorConfig::default();
    edit(&mut vector);
    let storage_options = StorageOptions {
        vector,
        ..Default::default()
    };
    let redis = open_redis_with_options(&path, Arc::new(storage_options));

    test(&redis);

    drop(redis);
    safe_cleanup_test_db(&path);
}

fn populate_search_vectors(redis: &Redis) -> CanonicalVector {
    let x = CanonicalVector::from_values(&[1.0, 0.0]).expect("x");
    let y = CanonicalVector::from_values(&[0.0, 1.0]).expect("y");
    let neg_x = CanonicalVector::from_values(&[-1.0, 0.0]).expect("negative x");
    redis.vadd(b"search", b"b", &y).expect("insert b");
    redis.vadd(b"search", b"a", &y).expect("insert a");
    redis.vadd(b"search", b"x", &x).expect("insert x");
    redis.vadd(b"search", b"neg", &neg_x).expect("insert neg");
    x
}

fn search_options(count: usize, mode: VectorSearchMode) -> VectorSearchOptions {
    VectorSearchOptions { count, mode }
}

fn count_cf_entries(redis: &Redis, cf_index: ColumnFamilyIndex) -> usize {
    let db = redis.db().expect("db is initialized");
    let cf = redis.get_cf_handle(cf_index).expect("column family exists");
    db.iterator_cf(&cf, IteratorMode::Start)
        .inspect(|entry| {
            entry.as_ref().expect("read column family entry");
        })
        .count()
}

fn read_stored_vector_meta(redis: &Redis, key: &[u8]) -> VectorMeta {
    let db = redis.db().expect("db is initialized");
    let meta_cf = redis
        .get_cf_handle(ColumnFamilyIndex::MetaCF)
        .expect("MetaCF exists");
    let meta_key = BaseMetaKey::new(key).encode().expect("meta key");
    VectorMeta::decode(
        &db.get_cf(&meta_cf, &meta_key)
            .expect("read vector meta")
            .expect("vector meta exists"),
    )
    .expect("decode vector meta")
}

#[test]
fn test_vadd_create_update_and_dimension_guard() {
    with_redis(|redis| {
        let a = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid vector");
        let b = CanonicalVector::from_values(&[0.0, 1.0]).expect("valid vector");
        let wrong_dimension = CanonicalVector::from_values(&[1.0, 0.0, 0.0]).expect("valid vector");

        assert!(redis.vadd(b"vectors", b"a", &a).expect("insert a"));
        assert!(redis.vadd(b"vectors", b"b", &b).expect("insert b"));
        assert!(!redis.vadd(b"vectors", b"a", &b).expect("update a"));
        assert_eq!(redis.vcard(b"vectors").expect("card"), 2);
        assert_eq!(redis.vdim(b"vectors").expect("dim"), 2);
        assert_eq!(
            redis.vemb(b"vectors", b"a").expect("emb"),
            Some(vec![0.0, 1.0])
        );

        assert!(redis.vadd(b"vectors", b"a", &wrong_dimension).is_err());
        assert_eq!(redis.vcard(b"vectors").expect("card after error"), 2);
        assert_eq!(
            redis.vemb(b"vectors", b"a").expect("emb after error"),
            Some(vec![0.0, 1.0])
        );
    });
}

#[test]
fn test_vadd_is_binary_safe_and_accepts_empty_element() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid vector");

        assert!(
            redis
                .vadd(b"vectors\0key", b"\0binary", &vector)
                .expect("binary member")
        );
        assert!(
            redis
                .vismember(b"vectors\0key", b"\0binary")
                .expect("binary membership")
        );
        assert!(
            redis
                .vadd(b"empty-element", b"", &vector)
                .expect("empty member")
        );
        assert!(
            redis
                .vismember(b"empty-element", b"")
                .expect("empty membership")
        );
    });
}

#[test]
fn test_vcard_vdim_vemb_and_vismember_missing_semantics() {
    with_redis(|redis| {
        assert_eq!(redis.vcard(b"missing").expect("missing card"), 0);
        assert!(redis.vdim(b"missing").is_err());
        assert_eq!(
            redis.vemb(b"missing", b"member").expect("missing emb"),
            None
        );
        assert!(
            !redis
                .vismember(b"missing", b"member")
                .expect("missing membership")
        );

        let vector = CanonicalVector::from_values(&[3.0, 4.0]).expect("valid vector");
        assert!(redis.vadd(b"vectors", b"member", &vector).expect("insert"));
        assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
        assert_eq!(redis.vdim(b"vectors").expect("dimension"), 2);
        assert!(redis.vismember(b"vectors", b"member").expect("membership"));
        let restored = redis
            .vemb(b"vectors", b"member")
            .expect("embedding")
            .expect("member exists");
        assert!((restored[0] - 3.0).abs() < 1e-6);
        assert!((restored[1] - 4.0).abs() < 1e-6);
        assert_eq!(redis.vemb(b"vectors", b"absent").expect("absent emb"), None);
    });
}

#[test]
fn test_vrem_deletes_last_member_and_meta() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid vector");
        assert!(redis.vadd(b"vectors", b"a", &vector).expect("insert a"));
        assert!(redis.vadd(b"vectors", b"b", &vector).expect("insert b"));

        assert!(!redis.vrem(b"vectors", b"absent").expect("remove absent"));
        assert!(redis.vrem(b"vectors", b"a").expect("remove a"));
        assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
        assert!(redis.vrem(b"vectors", b"b").expect("remove b"));
        assert_eq!(redis.vcard(b"vectors").expect("missing card"), 0);
        assert!(redis.get_key_type(b"vectors").is_err());
    });
}

#[test]
fn test_vector_commands_return_wrongtype_for_string_key() {
    with_redis(|redis| {
        let key = b"string-key";
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid vector");
        redis.set(key, b"value").expect("set string");

        let errors = [
            redis.vadd(key, b"member", &vector).unwrap_err(),
            redis.vrem(key, b"member").unwrap_err(),
            redis.vcard(key).unwrap_err(),
            redis.vdim(key).unwrap_err(),
            redis.vemb(key, b"member").unwrap_err(),
            redis.vismember(key, b"member").unwrap_err(),
        ];
        assert!(
            errors
                .iter()
                .all(|error| error.to_string().contains("WRONGTYPE"))
        );
    });
}

#[test]
fn test_vector_meta_and_member_are_committed_together() {
    let path = unique_test_db_path();
    safe_cleanup_test_db(&path);
    let redis = open_redis(&path);
    let vector = CanonicalVector::from_values(&[3.0, 4.0]).expect("valid vector");
    assert!(redis.vadd(b"vectors", b"member", &vector).expect("insert"));

    {
        let db = redis.db().expect("db is initialized");
        let meta_cf = redis
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .expect("MetaCF exists");
        let vector_cf = redis
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .expect("VectorDataCF exists");
        let snapshot = db.snapshot();
        let mut meta_options = ReadOptions::default();
        meta_options.set_snapshot(&snapshot);
        let mut vector_options = ReadOptions::default();
        vector_options.set_snapshot(&snapshot);
        let meta_key = BaseMetaKey::new(b"vectors").encode().expect("meta key");

        assert!(
            db.get_cf_opt(&meta_cf, &meta_key, &meta_options)
                .expect("read meta")
                .is_some()
        );
        assert_eq!(
            db.iterator_cf_opt(&vector_cf, vector_options, IteratorMode::Start)
                .inspect(|entry| {
                    entry.as_ref().expect("read vector entry");
                })
                .count(),
            1
        );
    }
    assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
    assert!(redis.vismember(b"vectors", b"member").expect("membership"));

    drop(redis);
    safe_cleanup_test_db(&path);
}

#[test]
fn test_vsim_direct_vector_returns_exact_top_k() {
    with_redis(|redis| {
        let x = populate_search_vectors(redis);
        let hits = redis
            .vsim(
                b"search",
                VectorQuery::Vector(x),
                search_options(3, VectorSearchMode::Approximate),
            )
            .expect("search");

        assert_eq!(
            hits.iter()
                .map(|hit| hit.element.as_slice())
                .collect::<Vec<_>>(),
            vec![b"x".as_slice(), b"a".as_slice(), b"b".as_slice()]
        );
        assert!((hits[0].score - 1.0).abs() < 1e-12);
        assert!((hits[1].score - 0.5).abs() < 1e-12);
        assert!((hits[2].score - 0.5).abs() < 1e-12);
    });
}

#[test]
fn test_vsim_ele_uses_stored_member_as_query() {
    with_redis(|redis| {
        populate_search_vectors(redis);
        let hits = redis
            .vsim(
                b"search",
                VectorQuery::Element(b"x".to_vec()),
                search_options(2, VectorSearchMode::Approximate),
            )
            .expect("element search");

        assert_eq!(hits[0].element, b"x");
        assert_eq!(hits[1].element, b"a");
    });
}

#[test]
fn test_vsim_stable_tie_breaks_by_raw_element_bytes() {
    with_redis(|redis| {
        let x = populate_search_vectors(redis);
        let hits = redis
            .vsim(
                b"search",
                VectorQuery::Vector(x),
                search_options(4, VectorSearchMode::Approximate),
            )
            .expect("search");

        assert_eq!(hits[1].element, b"a");
        assert_eq!(hits[2].element, b"b");
        assert_eq!(hits[1].score, hits[2].score);
    });
}

#[test]
fn test_vsim_truth_matches_approximate_in_phase_one() {
    with_redis(|redis| {
        let x = populate_search_vectors(redis);
        let approximate = redis
            .vsim(
                b"search",
                VectorQuery::Vector(x.clone()),
                search_options(4, VectorSearchMode::Approximate),
            )
            .expect("approximate search");
        let truth = redis
            .vsim(
                b"search",
                VectorQuery::Vector(x),
                search_options(4, VectorSearchMode::Truth),
            )
            .expect("truth search");

        assert_eq!(truth, approximate);
    });
}

#[test]
fn test_vsim_missing_key_is_empty_and_missing_ele_is_error() {
    with_redis(|redis| {
        let x = CanonicalVector::from_values(&[1.0, 0.0]).expect("x");
        assert!(
            redis
                .vsim(
                    b"missing",
                    VectorQuery::Vector(x),
                    search_options(3, VectorSearchMode::Approximate),
                )
                .expect("missing search")
                .is_empty()
        );

        populate_search_vectors(redis);
        assert!(
            redis
                .vsim(
                    b"search",
                    VectorQuery::Element(b"missing".to_vec()),
                    search_options(3, VectorSearchMode::Approximate),
                )
                .is_err()
        );
    });
}

#[test]
fn test_vsim_rejects_query_dimension_mismatch() {
    with_redis(|redis| {
        let x = populate_search_vectors(redis);
        let wrong_dimension = CanonicalVector::from_values(&[1.0, 0.0, 0.0]).expect("3d");
        assert!(
            redis
                .vsim(
                    b"search",
                    VectorQuery::Vector(wrong_dimension),
                    search_options(3, VectorSearchMode::Approximate),
                )
                .is_err()
        );
        assert!(
            redis
                .vsim(
                    b"search",
                    VectorQuery::Vector(x),
                    search_options(0, VectorSearchMode::Approximate),
                )
                .is_err()
        );
    });
}

#[tokio::test]
async fn test_storage_routes_all_members_of_one_vectorset_to_one_instance() {
    let test_db_path = unique_test_db_path();
    safe_cleanup_test_db(&test_db_path);
    let mut storage = Storage::new(3, 0);
    let _receiver = storage
        .open(Arc::new(StorageOptions::default()), &test_db_path)
        .expect("open storage");
    let key = b"routed-vectors";
    let x = CanonicalVector::from_values(&[1.0, 0.0]).expect("x");
    let y = CanonicalVector::from_values(&[0.0, 1.0]).expect("y");

    assert!(storage.vadd(key, b"x", &x).expect("insert x"));
    assert!(storage.vadd(key, b"y", &y).expect("insert y"));
    assert_eq!(storage.vcard(key).expect("card"), 2);
    assert_eq!(storage.vdim(key).expect("dimension"), 2);
    assert!(storage.vismember(key, b"x").expect("membership"));
    assert_eq!(
        storage.vemb(key, b"x").expect("embedding"),
        Some(vec![1.0, 0.0])
    );
    assert_eq!(
        storage
            .vsim(
                key,
                VectorQuery::Vector(x),
                search_options(1, VectorSearchMode::Approximate),
            )
            .expect("search")[0]
            .element,
        b"x"
    );
    assert!(storage.vrem(key, b"y").expect("remove y"));

    let slot_id = key_to_slot_id(key);
    let selected = storage.slot_indexer.get_instance_id(slot_id);
    let meta_key = BaseMetaKey::new(key).encode().expect("meta key");
    for (instance_id, redis) in storage.insts.iter().enumerate() {
        let db = redis.db().expect("db is initialized");
        let meta_cf = redis
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .expect("MetaCF exists");
        assert_eq!(
            db.get_cf(&meta_cf, &meta_key)
                .expect("read routed meta")
                .is_some(),
            instance_id == selected
        );
        assert_eq!(
            count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF),
            usize::from(instance_id == selected)
        );
    }

    storage.shutdown().await;
    safe_cleanup_test_db(&test_db_path);
}

#[test]
fn test_type_returns_vectorset() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"vectors", b"member", &vector).expect("insert");
        assert_eq!(
            storage::data_type_to_string(redis.get_key_type(b"vectors").expect("key type")),
            "vectorset"
        );
    });
}

#[test]
fn test_vadd_rebuilds_expired_vectorset_with_new_generation() {
    with_redis(|redis| {
        let key = b"expiring-vectors";
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(key, b"old", &vector).expect("insert old member");

        let previous_meta = read_stored_vector_meta(redis, key);
        let previous_generation = previous_meta.version();
        assert_eq!(previous_meta.data_revision(), 1);

        // Expire the set in place, keeping the previous generation on disk.
        let db = redis.db().expect("db is initialized");
        let meta_cf = redis
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .expect("MetaCF exists");
        let meta_key = BaseMetaKey::new(key).encode().expect("meta key");
        let mut meta = previous_meta;
        meta.set_etime(1);
        db.put_cf(&meta_cf, &meta_key, meta.encode())
            .expect("store expired vector meta");

        assert_eq!(redis.vcard(key).expect("expired card"), 0);
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF), 1);

        redis
            .vadd(key, b"new", &vector)
            .expect("rebuild vector set");

        assert_eq!(redis.vcard(key).expect("rebuilt card"), 1);
        assert!(!redis.vismember(key, b"old").expect("old membership"));
        assert!(redis.vismember(key, b"new").expect("new membership"));
        let rebuilt_meta = read_stored_vector_meta(redis, key);
        assert!(
            rebuilt_meta.version() > previous_generation,
            "rebuilt set must get a fresh generation: {} <= {}",
            rebuilt_meta.version(),
            previous_generation
        );
        assert_eq!(rebuilt_meta.data_revision(), 1);
    });
}

#[test]
fn test_data_revision_increments_on_successful_vadd_and_vrem() {
    with_redis(|redis| {
        let key = b"revision-vectors";
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");

        redis.vadd(key, b"a", &vector).expect("insert a");
        assert_eq!(read_stored_vector_meta(redis, key).data_revision(), 1);

        redis.vadd(key, b"b", &vector).expect("insert b");
        assert_eq!(read_stored_vector_meta(redis, key).data_revision(), 2);

        // Updating an existing member also bumps the revision.
        assert!(!redis.vadd(key, b"a", &vector).expect("update a"));
        assert_eq!(read_stored_vector_meta(redis, key).data_revision(), 3);

        redis.vrem(key, b"a").expect("remove a");
        assert_eq!(read_stored_vector_meta(redis, key).data_revision(), 4);

        // A failed removal leaves the revision untouched.
        assert!(!redis.vrem(key, b"absent").expect("remove absent"));
        assert_eq!(read_stored_vector_meta(redis, key).data_revision(), 4);
    });
}

#[test]
fn test_generation_is_not_reused_across_restart() {
    let path = unique_test_db_path();
    safe_cleanup_test_db(&path);

    let incarnation;
    let last_generation;
    {
        let redis = open_redis(&path);
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"first", b"a", &vector).expect("insert first");
        redis.vadd(b"second", b"a", &vector).expect("insert second");
        let first_generation = read_stored_vector_meta(&redis, b"first").version();
        last_generation = read_stored_vector_meta(&redis, b"second").version();
        assert!(last_generation > first_generation);
        incarnation = redis.storage_incarnation().expect("incarnation");
    }

    let reopened = open_redis(&path);
    assert_eq!(
        reopened.storage_incarnation().expect("incarnation"),
        incarnation,
        "incarnation must be stable across restarts"
    );
    let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
    reopened
        .vadd(b"third", b"a", &vector)
        .expect("insert third after reopen");
    let generation = read_stored_vector_meta(&reopened, b"third").version();
    assert!(
        generation > last_generation,
        "restarted instance must not reuse generations: {generation} <= {last_generation}"
    );

    drop(reopened);
    safe_cleanup_test_db(&path);
}

#[test]
fn test_open_refuses_non_empty_db_without_manifest() {
    let path = unique_test_db_path();
    safe_cleanup_test_db(&path);

    {
        let redis = open_redis(&path);
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"vectors", b"a", &vector).expect("insert");
    }

    std::fs::remove_file(path.join(STORAGE_MANIFEST_FILE)).expect("remove manifest");

    let storage_options = Arc::new(StorageOptions::default());
    let (bg_task_handler, _) = BgTaskHandler::new();
    let lock_mgr = Arc::new(LockMgr::new(1000));
    let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);
    let result = redis.open(path.to_str().expect("test path must be UTF-8"));
    assert!(
        result.is_err(),
        "opening a non-empty database without a manifest must fail"
    );

    drop(redis);
    safe_cleanup_test_db(&path);
}

#[tokio::test]
async fn test_expired_vectorset_reads_as_missing() {
    let test_db_path = unique_test_db_path();
    safe_cleanup_test_db(&test_db_path);
    let mut storage = Storage::new(1, 0);
    let _receiver = storage
        .open(Arc::new(StorageOptions::default()), &test_db_path)
        .expect("open storage");
    let key = b"expiring-vectors";
    let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
    storage.vadd(key, b"member", &vector).expect("insert");

    assert!(storage.expire(key, 60).expect("expire"));
    assert!(storage.persist(key).expect("persist"));
    assert!(storage.expireat(key, 1).expect("expire in the past"));
    assert_eq!(storage.vcard(key).expect("expired card"), 0);
    assert!(storage.vdim(key).is_err());
    assert_eq!(storage.vemb(key, b"member").expect("expired emb"), None);
    assert!(!storage.vismember(key, b"member").expect("expired member"));
    assert!(
        storage
            .vsim(
                key,
                VectorQuery::Vector(vector),
                search_options(1, VectorSearchMode::Approximate),
            )
            .expect("expired search")
            .is_empty()
    );
    assert_eq!(storage.key_type(key).expect("expired type"), "none");

    storage.shutdown().await;
    safe_cleanup_test_db(&test_db_path);
}

#[test]
fn test_del_tombstones_vector_meta_and_defers_member_cleanup() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"vectors", b"a", &vector).expect("insert a");
        redis.vadd(b"vectors", b"b", &vector).expect("insert b");
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF), 2);
        let previous_version = read_stored_vector_meta(redis, b"vectors").version();

        assert!(redis.del_key(b"vectors").expect("delete vector set"));
        assert_eq!(redis.vcard(b"vectors").expect("missing card"), 0);
        let tombstone = read_stored_vector_meta(redis, b"vectors");
        assert_eq!(tombstone.count(), 0);
        assert_eq!(tombstone.etime(), 0);
        assert!(tombstone.version() > previous_version);
        assert_eq!(
            count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF),
            2,
            "DEL should leave vector members for compaction"
        );
    });
}

#[test]
fn test_flushdb_removes_vector_meta_and_members() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"vectors", b"a", &vector).expect("insert a");
        redis.vadd(b"vectors", b"b", &vector).expect("insert b");

        redis.flush_db().expect("flush db");
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::MetaCF), 0);
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF), 0);
    });
}

#[tokio::test]
async fn test_vector_storage_proposes_binlog_in_cluster_mode() {
    let test_db_path = unique_test_db_path();
    safe_cleanup_test_db(&test_db_path);
    let mut storage = Storage::new(1, 0);
    let _receiver = storage
        .open(Arc::new(StorageOptions::default()), &test_db_path)
        .expect("open storage");

    // Wrap Storage in an Arc so the append_log_fn callback can apply binlogs
    // back to the local instance. A Weak reference is used to avoid creating a
    // strong-reference cycle between the callback and Storage.
    let storage_arc = Arc::new(storage);
    let storage_weak = Arc::downgrade(&storage_arc);
    let captured = Arc::new(Mutex::new(None));
    let captured_clone = captured.clone();
    storage_arc.set_append_log_fn(Arc::new(move |binlog| {
        *captured_clone.lock().expect("lock captured binlog") = Some(binlog.clone());
        let storage = storage_weak
            .upgrade()
            .ok_or("storage dropped before binlog apply")?;
        storage
            .on_binlog_write(&binlog, 1)
            .map_err(|error| error.to_string())?;
        Ok(conf::raft_type::BinlogResponse::ok())
    }));

    let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
    assert!(
        storage_arc
            .vadd(b"vectors", b"member", &vector)
            .expect("vadd in cluster mode"),
        "vadd should insert a new member"
    );
    {
        let binlog = captured.lock().expect("lock captured binlog").take();
        let binlog = binlog.expect("vadd should propose a binlog");
        assert!(
            !binlog.entries.is_empty(),
            "vadd binlog should contain vector writes"
        );
    }

    assert!(
        storage_arc
            .vrem(b"vectors", b"member")
            .expect("vrem in cluster mode"),
        "vrem should remove the member"
    );
    let binlog = captured.lock().expect("lock captured binlog").take();
    let binlog = binlog.expect("vrem should propose a binlog");
    assert!(
        !binlog.entries.is_empty(),
        "vrem binlog should contain vector deletes"
    );

    // Weak reference, so the strong count is 1.
    let mut storage = Arc::try_unwrap(storage_arc)
        .unwrap_or_else(|_| panic!("storage should not be shared after test operations"));
    storage.shutdown().await;
    safe_cleanup_test_db(&test_db_path);
}

#[test]
fn test_vadd_enforces_max_dimension() {
    with_redis_vector_config(
        |vector| vector.max_dimension = 4,
        |redis| {
            let fitting = CanonicalVector::from_values(&[1.0, 0.0, 0.0, 0.0]).expect("4d vector");
            let oversized =
                CanonicalVector::from_values(&[1.0, 0.0, 0.0, 0.0, 0.0]).expect("5d vector");

            assert!(redis.vadd(b"vectors", b"a", &fitting).expect("4d insert"));
            let error = redis
                .vadd(b"vectors", b"b", &oversized)
                .expect_err("5d vector must be rejected");
            assert!(matches!(error, Error::RedisErr { .. }));
            assert!(error.to_string().contains("max_dimension"));
            assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
        },
    );
}

#[test]
fn test_vadd_enforces_max_element_bytes() {
    with_redis_vector_config(
        |vector| vector.max_element_bytes = 4,
        |redis| {
            let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");

            assert!(
                redis
                    .vadd(b"vectors", b"abcd", &vector)
                    .expect("fitting element")
            );
            let error = redis
                .vadd(b"vectors", b"abcde", &vector)
                .expect_err("oversized element must be rejected");
            assert!(matches!(error, Error::RedisErr { .. }));
            assert!(error.to_string().contains("max_element_bytes"));
            assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
        },
    );
}

#[test]
fn test_vadd_enforces_max_vector_bytes() {
    with_redis_vector_config(
        |vector| vector.max_vector_bytes = 8,
        |redis| {
            let fitting = CanonicalVector::from_values(&[1.0, 0.0]).expect("2d vector");
            let oversized = CanonicalVector::from_values(&[1.0, 0.0, 0.0, 0.0]).expect("4d vector");

            assert!(
                redis
                    .vadd(b"vectors", b"a", &fitting)
                    .expect("8-byte vector")
            );
            let error = redis
                .vadd(b"vectors", b"b", &oversized)
                .expect_err("16-byte vector must be rejected");
            assert!(matches!(error, Error::RedisErr { .. }));
            assert!(error.to_string().contains("max_vector_bytes"));
            assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
        },
    );
}

#[test]
fn test_vsim_enforces_max_k() {
    with_redis_vector_config(
        |vector| vector.max_k = 2,
        |redis| {
            let x = populate_search_vectors(redis);

            let error = redis
                .vsim(
                    b"search",
                    VectorQuery::Vector(x.clone()),
                    search_options(3, VectorSearchMode::Approximate),
                )
                .expect_err("count above max_k must be rejected");
            assert!(matches!(error, Error::RedisErr { .. }));
            assert!(error.to_string().contains("max_k"));

            let hits = redis
                .vsim(
                    b"search",
                    VectorQuery::Vector(x),
                    search_options(2, VectorSearchMode::Approximate),
                )
                .expect("count within max_k");
            assert_eq!(hits.len(), 2);
        },
    );
}

#[test]
fn test_vsim_waits_for_flat_query_permit() {
    with_redis_vector_config(
        |vector| vector.max_concurrent_flat_queries = 1,
        |redis| {
            populate_search_vectors(redis);
            let held = redis
                .flat_query_gate
                .acquire(Instant::now() + Duration::from_secs(5))
                .expect("acquire the only permit");

            std::thread::scope(|scope| {
                let query = scope.spawn(|| {
                    redis.vsim(
                        b"search",
                        VectorQuery::Element(b"x".to_vec()),
                        search_options(2, VectorSearchMode::Approximate),
                    )
                });
                std::thread::sleep(Duration::from_millis(200));
                assert!(!query.is_finished(), "query must wait for the permit");
                drop(held);
                let hits = query
                    .join()
                    .expect("query thread")
                    .expect("search after permit release");
                assert_eq!(hits.len(), 2);
            });
            assert_eq!(
                redis.flat_query_gate.available_permits(),
                1,
                "permit must return to the gate"
            );
        },
    );
}

#[test]
fn test_vsim_flat_deadline_covers_queue_wait() {
    with_redis_vector_config(
        |vector| {
            vector.max_concurrent_flat_queries = 1;
            vector.flat_query_timeout_ms = 150;
        },
        |redis| {
            populate_search_vectors(redis);
            let held = redis
                .flat_query_gate
                .acquire(Instant::now() + Duration::from_secs(5))
                .expect("acquire the only permit");

            let started = Instant::now();
            let result = std::thread::scope(|scope| {
                let query = scope.spawn(|| {
                    redis.vsim(
                        b"search",
                        VectorQuery::Element(b"x".to_vec()),
                        search_options(2, VectorSearchMode::Approximate),
                    )
                });
                // Hold the permit well past the query deadline.
                std::thread::sleep(Duration::from_secs(1));
                drop(held);
                query.join().expect("query thread")
            });

            let error = result.expect_err("queued query must time out, no partial results");
            assert!(matches!(error, Error::VectorFlatQueryTimeout { .. }));
            assert!(
                started.elapsed() >= Duration::from_millis(150),
                "deadline must include queue wait"
            );
            assert_eq!(
                redis.flat_query_gate.available_permits(),
                1,
                "permit must be released after the timeout"
            );

            // The gate is fully usable again after the timed-out query.
            let hits = redis
                .vsim(
                    b"search",
                    VectorQuery::Element(b"x".to_vec()),
                    search_options(2, VectorSearchMode::Approximate),
                )
                .expect("search after timeout");
            assert_eq!(hits.len(), 2);
        },
    );
}

#[test]
fn test_vsim_scan_budget_entries_aborts_without_partial_results() {
    with_redis_vector_config(
        |vector| vector.flat_scan_max_entries = 50,
        |redis| {
            let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
            for i in 0..200 {
                redis
                    .vadd(b"budget", format!("member-{i}").as_bytes(), &vector)
                    .expect("insert");
            }

            let error = redis
                .vsim(
                    b"budget",
                    VectorQuery::Vector(vector.clone()),
                    search_options(10, VectorSearchMode::Approximate),
                )
                .expect_err("scan budget must abort the query, no partial results");
            assert!(matches!(error, Error::VectorFlatScanBudgetExceeded { .. }));
            assert_eq!(
                redis.flat_query_gate.available_permits(),
                4,
                "permit must be released after the abort"
            );

            // A set within budget still answers after the aborted scan.
            redis.vadd(b"small", b"a", &vector).expect("insert small");
            let hits = redis
                .vsim(
                    b"small",
                    VectorQuery::Vector(vector),
                    search_options(1, VectorSearchMode::Approximate),
                )
                .expect("small search");
            assert_eq!(hits.len(), 1);
        },
    );
}

#[test]
fn test_vsim_scan_budget_bytes_aborts_without_partial_results() {
    with_redis_vector_config(
        |vector| vector.flat_scan_max_bytes = 128,
        |redis| {
            let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
            for i in 0..50 {
                redis
                    .vadd(b"bytes", format!("member-{i}").as_bytes(), &vector)
                    .expect("insert");
            }

            let error = redis
                .vsim(
                    b"bytes",
                    VectorQuery::Vector(vector),
                    search_options(10, VectorSearchMode::Approximate),
                )
                .expect_err("byte budget must abort the query, no partial results");
            assert!(matches!(error, Error::VectorFlatScanBudgetExceeded { .. }));
            assert_eq!(redis.flat_query_gate.available_permits(), 4);
        },
    );
}

#[test]
fn test_vsim_precancelled_token_aborts_immediately() {
    with_redis(|redis| {
        let x = populate_search_vectors(redis);
        let cancel = FlatQueryCancel::default();
        cancel.cancel();

        let error = redis
            .vsim_with_cancel(
                b"search",
                VectorQuery::Vector(x),
                search_options(2, VectorSearchMode::Approximate),
                &cancel,
            )
            .expect_err("cancelled token must abort the query");
        assert!(matches!(error, Error::VectorFlatQueryCancelled { .. }));
        assert_eq!(redis.flat_query_gate.available_permits(), 4);
    });
}

#[test]
fn test_vsim_cancel_mid_scan_aborts_and_releases_resources() {
    with_redis_vector_config(
        |vector| vector.flat_cancel_check_interval = 16,
        |redis| {
            let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
            for i in 0..50_000 {
                redis
                    .vadd(b"big", format!("member-{i}").as_bytes(), &vector)
                    .expect("insert");
            }

            let cancel = FlatQueryCancel::default();
            let token = cancel.clone();
            let result = std::thread::scope(|scope| {
                let query = scope.spawn(|| {
                    redis.vsim_with_cancel(
                        b"big",
                        VectorQuery::Vector(vector),
                        search_options(10, VectorSearchMode::Approximate),
                        &token,
                    )
                });
                std::thread::sleep(Duration::from_millis(20));
                cancel.cancel();
                query.join().expect("query thread")
            });

            let error = result.expect_err("mid-scan cancel must abort, no partial results");
            assert!(matches!(error, Error::VectorFlatQueryCancelled { .. }));
            assert_eq!(
                redis.flat_query_gate.available_permits(),
                4,
                "permit must be released after cancellation"
            );
        },
    );
}

#[test]
fn test_vinfo_missing_key_and_wrongtype() {
    with_redis(|redis| {
        assert_eq!(redis.vinfo(b"missing").expect("missing vinfo"), None);

        redis.set(b"string", b"value").expect("set string");
        let error = redis.vinfo(b"string").expect_err("string key must fail");
        assert!(error.to_string().contains("WRONGTYPE"));
    });
}

#[test]
fn test_vinfo_reports_meta_and_generation_changes_on_recreate() {
    with_redis(|redis| {
        let vector3 = CanonicalVector::from_values(&[1.0, 0.0, 0.0]).expect("vector");
        assert!(redis.vadd(b"vectors", b"a", &vector3).expect("insert a"));
        assert!(redis.vadd(b"vectors", b"b", &vector3).expect("insert b"));

        let first = redis.vinfo(b"vectors").expect("vinfo").expect("set exists");
        assert_eq!(first.dimension, 3);
        assert_eq!(first.size, 2);
        let first_generation = first.generation;

        // Removing the last member deletes the meta; recreating the set
        // allocates a fresh generation sequence.
        assert!(redis.vrem(b"vectors", b"a").expect("remove a"));
        assert!(redis.vrem(b"vectors", b"b").expect("remove b"));
        assert_eq!(redis.vinfo(b"vectors").expect("vinfo after drain"), None);

        let vector2 = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        assert!(redis.vadd(b"vectors", b"c", &vector2).expect("recreate"));
        let second = redis
            .vinfo(b"vectors")
            .expect("vinfo")
            .expect("recreated set");
        assert_eq!(second.dimension, 2);
        assert_eq!(second.size, 1);
        assert_ne!(
            second.generation, first_generation,
            "recreated set must have a new generation sequence"
        );
    });
}

#[test]
fn test_vector_metrics_count_successful_queries() {
    with_redis(|redis| {
        let x = populate_search_vectors(redis);
        let hits = redis
            .vsim(
                b"search",
                VectorQuery::Vector(x),
                search_options(2, VectorSearchMode::Approximate),
            )
            .expect("search");
        assert_eq!(hits.len(), 2);

        let metrics = redis.vector_metrics.snapshot();
        assert_eq!(metrics.flat_queries_total, 1);
        assert_eq!(metrics.flat_query_duration_count, 1);
        assert_eq!(metrics.flat_query_timeouts_total, 0);
        assert_eq!(metrics.flat_query_errors_total, 0);
        assert_eq!(metrics.capacity_rejected_total, 0);
    });
}

#[test]
fn test_vector_metrics_count_scan_deadline_timeout() {
    with_redis_vector_config(
        |vector| vector.flat_query_timeout_ms = 0,
        |redis| {
            let x = populate_search_vectors(redis);
            let error = redis
                .vsim(
                    b"search",
                    VectorQuery::Vector(x),
                    search_options(2, VectorSearchMode::Approximate),
                )
                .expect_err("zero timeout must abort the query");
            assert!(matches!(error, Error::VectorFlatQueryTimeout { .. }));

            let metrics = redis.vector_metrics.snapshot();
            assert_eq!(metrics.flat_queries_total, 1);
            assert_eq!(metrics.flat_query_timeouts_total, 1);
            assert_eq!(metrics.flat_query_errors_total, 0);
            assert_eq!(metrics.capacity_rejected_total, 0);
            assert_eq!(metrics.flat_query_duration_count, 1);
        },
    );
}

#[test]
fn test_vector_metrics_count_budget_and_cancel_errors() {
    with_redis_vector_config(
        |vector| vector.flat_scan_max_entries = 50,
        |redis| {
            let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
            for i in 0..200 {
                redis
                    .vadd(b"budget", format!("member-{i}").as_bytes(), &vector)
                    .expect("insert");
            }
            let error = redis
                .vsim(
                    b"budget",
                    VectorQuery::Vector(vector.clone()),
                    search_options(10, VectorSearchMode::Approximate),
                )
                .expect_err("budget must abort the query");
            assert!(matches!(error, Error::VectorFlatScanBudgetExceeded { .. }));

            let cancel = FlatQueryCancel::default();
            cancel.cancel();
            let error = redis
                .vsim_with_cancel(
                    b"budget",
                    VectorQuery::Vector(vector),
                    search_options(1, VectorSearchMode::Approximate),
                    &cancel,
                )
                .expect_err("cancelled token must abort the query");
            assert!(matches!(error, Error::VectorFlatQueryCancelled { .. }));

            let metrics = redis.vector_metrics.snapshot();
            assert_eq!(metrics.flat_queries_total, 2);
            assert_eq!(metrics.flat_query_errors_total, 2);
            assert_eq!(metrics.flat_query_timeouts_total, 0);
            assert_eq!(metrics.flat_query_duration_count, 2);
        },
    );
}

#[test]
fn test_vector_metrics_count_capacity_rejected_queries() {
    with_redis_vector_config(
        |vector| {
            vector.max_concurrent_flat_queries = 1;
            vector.flat_query_timeout_ms = 150;
        },
        |redis| {
            populate_search_vectors(redis);
            let held = redis
                .flat_query_gate
                .acquire(Instant::now() + Duration::from_secs(5))
                .expect("acquire the only permit");

            let result = std::thread::scope(|scope| {
                let query = scope.spawn(|| {
                    redis.vsim(
                        b"search",
                        VectorQuery::Element(b"x".to_vec()),
                        search_options(2, VectorSearchMode::Approximate),
                    )
                });
                std::thread::sleep(Duration::from_secs(1));
                drop(held);
                query.join().expect("query thread")
            });
            assert!(matches!(
                result.expect_err("queued query must be rejected"),
                Error::VectorFlatQueryTimeout { .. }
            ));

            let metrics = redis.vector_metrics.snapshot();
            assert_eq!(metrics.capacity_rejected_total, 1);
            // The query never entered the scan path.
            assert_eq!(metrics.flat_queries_total, 0);
            assert_eq!(metrics.flat_query_timeouts_total, 0);
            assert_eq!(metrics.flat_query_duration_count, 0);
        },
    );
}

fn add_mutation(element: &[u8], values: &[f32]) -> VectorSetMutationV1 {
    let vector = CanonicalVector::from_values(values).expect("valid vector");
    VectorSetMutationV1::add_from_canonical(element, &vector).expect("mutation from vector")
}

#[test]
fn test_apply_mutation_create_add_update_and_remove() {
    with_redis(|redis| {
        let key = b"vectors";

        // Create: first add creates the set.
        let result = redis
            .apply_vector_set_mutation(key, &add_mutation(b"a", &[1.0, 0.0]), None)
            .expect("create set");
        assert_eq!(result, VectorSetApplyResult::Added);
        assert_eq!(redis.vcard(key).expect("card"), 1);
        assert_eq!(read_stored_vector_meta(redis, key).data_revision(), 1);

        // Update: same element keeps count, bumps data_revision.
        let result = redis
            .apply_vector_set_mutation(key, &add_mutation(b"a", &[0.0, 1.0]), None)
            .expect("update member");
        assert_eq!(result, VectorSetApplyResult::Updated);
        assert_eq!(redis.vcard(key).expect("card after update"), 1);
        assert_eq!(read_stored_vector_meta(redis, key).data_revision(), 2);
        assert_eq!(redis.vemb(key, b"a").expect("emb"), Some(vec![0.0, 1.0]));

        // Add: a new element increases count.
        let result = redis
            .apply_vector_set_mutation(key, &add_mutation(b"b", &[1.0, 0.0]), None)
            .expect("add member");
        assert_eq!(result, VectorSetApplyResult::Added);
        assert_eq!(redis.vcard(key).expect("card after add"), 2);
        assert_eq!(read_stored_vector_meta(redis, key).data_revision(), 3);

        // Remove: existing member, then a miss.
        let remove_a = VectorSetMutationV1::Remove {
            element: b"a".to_vec(),
        };
        let result = redis
            .apply_vector_set_mutation(key, &remove_a, None)
            .expect("remove member");
        assert_eq!(result, VectorSetApplyResult::Removed);
        assert_eq!(redis.vcard(key).expect("card after remove"), 1);
        let result = redis
            .apply_vector_set_mutation(key, &remove_a, None)
            .expect("remove miss");
        assert_eq!(result, VectorSetApplyResult::RemoveMissed);

        // Removing the last member deletes the meta with it.
        let remove_b = VectorSetMutationV1::Remove {
            element: b"b".to_vec(),
        };
        let result = redis
            .apply_vector_set_mutation(key, &remove_b, None)
            .expect("remove last member");
        assert_eq!(result, VectorSetApplyResult::Removed);
        assert_eq!(redis.vcard(key).expect("missing card"), 0);
        assert!(redis.get_key_type(key).is_err());
    });
}

#[test]
fn test_apply_mutation_remove_miss_on_missing_key() {
    with_redis(|redis| {
        let mutation = VectorSetMutationV1::Remove {
            element: b"member".to_vec(),
        };
        let result = redis
            .apply_vector_set_mutation(b"missing", &mutation, None)
            .expect("remove from missing key");
        assert_eq!(result, VectorSetApplyResult::RemoveMissed);
    });
}

#[test]
fn test_apply_mutation_uses_explicit_create_generation() {
    with_redis(|redis| {
        let result = redis
            .apply_vector_set_mutation(b"vectors", &add_mutation(b"a", &[1.0, 0.0]), Some(42))
            .expect("create with explicit generation");
        assert_eq!(result, VectorSetApplyResult::Added);
        assert_eq!(read_stored_vector_meta(redis, b"vectors").version(), 42);

        // The explicit generation only applies to creation; updates keep it.
        let result = redis
            .apply_vector_set_mutation(b"vectors", &add_mutation(b"b", &[0.0, 1.0]), Some(7))
            .expect("add to existing set");
        assert_eq!(result, VectorSetApplyResult::Added);
        assert_eq!(read_stored_vector_meta(redis, b"vectors").version(), 42);
    });
}

#[test]
fn test_apply_mutation_wrongtype_is_a_business_error() {
    with_redis(|redis| {
        redis.set(b"string-key", b"value").expect("set string");

        let add_error = redis
            .apply_vector_set_mutation(b"string-key", &add_mutation(b"a", &[1.0, 0.0]), None)
            .expect_err("add on string key");
        assert!(matches!(
            add_error,
            VectorSetApplyError::Business(VectorSetBusinessError::WrongType)
        ));

        let remove = VectorSetMutationV1::Remove {
            element: b"a".to_vec(),
        };
        let remove_error = redis
            .apply_vector_set_mutation(b"string-key", &remove, None)
            .expect_err("remove on string key");
        assert!(matches!(
            remove_error,
            VectorSetApplyError::Business(VectorSetBusinessError::WrongType)
        ));

        // The business error maps back to the exact storage error VADD/VREM
        // returned before the apply entry existed.
        let mapped: Error = add_error.into();
        assert!(
            mapped
                .to_string()
                .contains("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    });
}

#[test]
fn test_apply_mutation_dimension_mismatch_is_a_business_error() {
    with_redis(|redis| {
        let result = redis
            .apply_vector_set_mutation(b"vectors", &add_mutation(b"a", &[1.0, 0.0]), None)
            .expect("create set");
        assert_eq!(result, VectorSetApplyResult::Added);

        let error = redis
            .apply_vector_set_mutation(b"vectors", &add_mutation(b"b", &[1.0, 0.0, 0.0]), None)
            .expect_err("dimension mismatch");
        assert!(matches!(
            error,
            VectorSetApplyError::Business(VectorSetBusinessError::DimensionMismatch {
                expected: 2,
                got: 3
            })
        ));
        // A rejected mutation leaves the set untouched.
        assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
    });
}

#[test]
fn test_apply_mutation_codec_round_trip_through_apply() {
    with_redis(|redis| {
        let mutation = add_mutation(b"member", &[3.0, 4.0]);
        let decoded = VectorSetMutationV1::decode(&mutation.encode()).expect("decode mutation");
        assert_eq!(decoded, mutation);

        let result = redis
            .apply_vector_set_mutation(b"vectors", &decoded, None)
            .expect("apply decoded mutation");
        assert_eq!(result, VectorSetApplyResult::Added);
        let restored = redis
            .vemb(b"vectors", b"member")
            .expect("embedding")
            .expect("member exists");
        assert!((restored[0] - 3.0).abs() < 1e-6);
        assert!((restored[1] - 4.0).abs() < 1e-6);
    });
}

#[test]
fn test_vector_meta_read_fault_blocks_reads_and_writes() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid vector");
        redis.vadd(b"vectors", b"a", &vector).expect("insert a");

        redis.vector_fault_hooks.set_fail_meta_read(true);

        // Reads against MetaCF all surface the injected failure as a
        // complete error, never a partial result.
        let error = redis.vcard(b"vectors").expect_err("vcard must fail");
        assert!(error.to_string().contains("injected fault"));
        assert!(redis.vinfo(b"vectors").is_err());
        assert!(redis.vemb(b"vectors", b"a").is_err());
        assert!(redis.vismember(b"vectors", b"a").is_err());
        let error = redis
            .vsim(
                b"vectors",
                VectorQuery::Vector(vector.clone()),
                search_options(1, VectorSearchMode::Truth),
            )
            .expect_err("vsim must fail");
        assert!(error.to_string().contains("injected fault"));

        // Mutations read the meta before deciding the outcome, so both an
        // update on an existing set and the creation of a new set fail
        // without any partial write.
        let error = redis
            .vadd(b"vectors", b"b", &vector)
            .expect_err("vadd on existing set must fail");
        assert!(error.to_string().contains("injected fault"));
        assert!(redis.vadd(b"other", b"a", &vector).is_err());
        assert!(redis.vrem(b"vectors", b"a").is_err());

        // The failed mutations left no trace behind.
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::MetaCF), 1);
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF), 1);

        redis.vector_fault_hooks.set_fail_meta_read(false);

        // After disarming, the pre-fault state is intact and writes work.
        assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
        assert!(redis.vadd(b"vectors", b"b", &vector).expect("retry vadd"));
        assert_eq!(redis.vcard(b"vectors").expect("card after retry"), 2);
        assert!(redis.vrem(b"vectors", b"b").expect("retry vrem"));
        assert_eq!(redis.vcard(b"vectors").expect("card after vrem"), 1);
    });
}

#[test]
fn test_vector_member_read_fault_fails_vsim_without_partial_results() {
    with_redis(|redis| {
        let query = populate_search_vectors(redis);

        let baseline = redis
            .vsim(
                b"search",
                VectorQuery::Vector(query.clone()),
                search_options(2, VectorSearchMode::Truth),
            )
            .expect("baseline vsim");
        assert_eq!(baseline.len(), 2);

        redis.vector_fault_hooks.set_fail_member_read(true);

        // An iterator-level member read failure aborts the whole query
        // instead of skipping the damaged member and returning a partial
        // top-K.
        let error = redis
            .vsim(
                b"search",
                VectorQuery::Vector(query.clone()),
                search_options(2, VectorSearchMode::Truth),
            )
            .expect_err("vsim must fail on member read fault");
        assert!(error.to_string().contains("injected fault"));

        // Writes and meta reads do not go through the scan path and keep
        // working while the member read fault is armed.
        assert_eq!(redis.vcard(b"search").expect("card during fault"), 4);

        redis.vector_fault_hooks.set_fail_member_read(false);

        // The scan recovers and returns the same results as before.
        let recovered = redis
            .vsim(
                b"search",
                VectorQuery::Vector(query),
                search_options(2, VectorSearchMode::Truth),
            )
            .expect("vsim after disarm");
        assert_eq!(recovered.len(), 2);
        assert_eq!(redis.vcard(b"search").expect("card after fault"), 4);
    });
}

#[test]
fn test_vector_batch_commit_fault_preserves_atomicity() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid vector");
        redis.vadd(b"vectors", b"a", &vector).expect("insert a");
        let meta_before = read_stored_vector_meta(redis, b"vectors");
        let members_before = count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF);

        redis.vector_fault_hooks.set_fail_batch_commit(true);

        // Add on an existing set: commit fails, meta (count/data_revision)
        // and member stay invisible together.
        let error = redis
            .vadd(b"vectors", b"b", &vector)
            .expect_err("vadd must fail on commit fault");
        assert!(error.to_string().contains("injected fault"));

        // Create of a new set: commit fails atomically as well.
        assert!(redis.vadd(b"newset", b"x", &vector).is_err());

        // Remove: commit fails, the member and meta survive together.
        assert!(redis.vrem(b"vectors", b"a").is_err());

        // Reads are unaffected by the commit fault and observe exactly the
        // pre-fault state.
        assert_eq!(redis.vcard(b"vectors").expect("card"), 1);
        assert!(redis.vismember(b"vectors", b"a").expect("member a"));
        assert!(!redis.vismember(b"vectors", b"b").expect("member b"));
        assert_eq!(redis.vcard(b"newset").expect("newset card"), 0);
        assert_eq!(
            count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF),
            members_before
        );
        let meta_after = read_stored_vector_meta(redis, b"vectors");
        assert_eq!(meta_after.data_revision(), meta_before.data_revision());
        assert_eq!(meta_after.version(), meta_before.version());

        redis.vector_fault_hooks.reset();

        // After disarming, retried mutations succeed and stay consistent.
        assert!(redis.vadd(b"vectors", b"b", &vector).expect("retry vadd"));
        assert_eq!(redis.vcard(b"vectors").expect("card after retry"), 2);
        assert!(redis.vrem(b"vectors", b"a").expect("retry vrem"));
        assert_eq!(redis.vcard(b"vectors").expect("card after vrem"), 1);
        assert!(redis.vadd(b"newset", b"x", &vector).expect("retry create"));
        assert_eq!(redis.vcard(b"newset").expect("newset card"), 1);
    });
}

#[test]
fn vector_data_sample_validation_passes_on_healthy_data() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"vs", b"e1", &vector).expect("vadd e1");
        redis.vadd(b"vs", b"e2", &vector).expect("vadd e2");

        let sample = redis.validate_vector_data_sample(16).expect("sample");
        assert_eq!(sample.members, 2);
        assert_eq!(sample.metas, 1);

        // A sample size of zero samples nothing and always passes.
        let empty = redis.validate_vector_data_sample(0).expect("empty sample");
        assert_eq!(empty.members, 0);
        assert_eq!(empty.metas, 0);
    });
}

#[test]
fn vector_data_sample_validation_rejects_corrupt_member() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"vs", b"e1", &vector).expect("vadd e1");

        let db = redis.db().expect("db");
        let vector_cf = redis
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .expect("vector cf");
        let member_key = db
            .iterator_cf(&vector_cf, IteratorMode::Start)
            .next()
            .expect("one member entry")
            .expect("member entry")
            .0;
        db.put_cf(&vector_cf, &member_key, b"garbage")
            .expect("corrupt member value");

        assert!(redis.validate_vector_data_sample(16).is_err());
    });
}

#[test]
fn vector_data_sample_validation_rejects_corrupt_meta() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"vs", b"e1", &vector).expect("vadd e1");

        let db = redis.db().expect("db");
        let meta_cf = redis
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .expect("meta cf");
        // A VectorSet-tagged meta value that fails codec decode (too short).
        let corrupt_key = BaseMetaKey::new(b"zz_corrupt").encode().expect("meta key");
        db.put_cf(
            &meta_cf,
            &corrupt_key,
            vec![storage::DataType::VectorSet as u8, 0, 0],
        )
        .expect("corrupt meta value");

        assert!(redis.validate_vector_data_sample(16).is_err());
    });
}
