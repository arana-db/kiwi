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

use std::{path::PathBuf, sync::Arc};

use kstd::lock_mgr::LockMgr;
use rocksdb::{IteratorMode, ReadOptions};
use storage::{
    BaseMetaKey, BgTaskHandler, CanonicalVector, ColumnFamilyIndex, Redis, StorageOptions,
    safe_cleanup_test_db, unique_test_db_path,
};

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

    redis.set_need_close(true);
    drop(redis);
    safe_cleanup_test_db(&path);
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
        let db = redis.db.as_ref().expect("db is initialized");
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

    redis.set_need_close(true);
    drop(redis);
    safe_cleanup_test_db(&path);
}
