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

use std::{path::PathBuf, sync::Arc, sync::Mutex};

use kstd::lock_mgr::LockMgr;
use rocksdb::{IteratorMode, ReadOptions};
use storage::{
    BaseMetaKey, BgTaskHandler, CanonicalVector, ColumnFamilyIndex, Redis, StorageOptions,
    VectorQuery, VectorSearchMode, VectorSearchOptions, safe_cleanup_test_db, unique_test_db_path,
};
use storage::{slot_indexer::key_to_slot_id, storage::Storage};

fn open_redis(path: &PathBuf) -> Redis {
    let storage_options = Arc::new(StorageOptions::default());
    let (bg_task_handler, _) = BgTaskHandler::new();
    let lock_mgr = Arc::new(LockMgr::new(1000));
    let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);
    redis
        .open(path.to_str().expect("test path must be UTF-8"))
        .expect("open test db");
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
        .map(|entry| entry.expect("read column family entry"))
        .count()
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
                .map(|entry| entry.expect("read vector entry"))
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
fn test_vadd_rebuilds_expired_vectorset_with_newer_generation() {
    with_redis(|redis| {
        let key = b"expiring-vectors";
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(key, b"old", &vector).expect("insert old member");

        let db = redis.db().expect("db is initialized");
        let meta_cf = redis
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .expect("MetaCF exists");
        let meta_key = BaseMetaKey::new(key).encode().expect("meta key");
        let mut meta = db
            .get_cf(&meta_cf, &meta_key)
            .expect("read vector meta")
            .expect("vector meta exists");
        let previous_generation = u64::MAX - 1;
        meta[9..17].copy_from_slice(&previous_generation.to_le_bytes());
        let etime_offset = meta.len() - size_of::<u64>();
        meta[etime_offset..].copy_from_slice(&1_u64.to_le_bytes());
        db.put_cf(&meta_cf, &meta_key, &meta)
            .expect("store expired vector meta");

        assert_eq!(redis.vcard(key).expect("expired card"), 0);
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF), 1);

        redis
            .vadd(key, b"new", &vector)
            .expect("rebuild vector set");

        assert_eq!(redis.vcard(key).expect("rebuilt card"), 1);
        assert!(!redis.vismember(key, b"old").expect("old membership"));
        assert!(redis.vismember(key, b"new").expect("new membership"));
        let rebuilt_meta = db
            .get_cf(&meta_cf, &meta_key)
            .expect("read rebuilt vector meta")
            .expect("rebuilt vector meta exists");
        let rebuilt_generation =
            u64::from_le_bytes(rebuilt_meta[9..17].try_into().expect("generation bytes"));
        assert!(rebuilt_generation > previous_generation);
    });
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
fn test_del_removes_vector_meta_and_members() {
    with_redis(|redis| {
        let vector = CanonicalVector::from_values(&[1.0, 0.0]).expect("vector");
        redis.vadd(b"vectors", b"a", &vector).expect("insert a");
        redis.vadd(b"vectors", b"b", &vector).expect("insert b");
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF), 2);

        assert!(redis.del_key(b"vectors").expect("delete vector set"));
        assert_eq!(redis.vcard(b"vectors").expect("missing card"), 0);
        assert_eq!(count_cf_entries(redis, ColumnFamilyIndex::VectorDataCF), 0);
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
