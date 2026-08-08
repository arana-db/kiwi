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
use std::sync::mpsc::{RecvTimeoutError, sync_channel};
use std::time::{Duration, Instant};

use chrono::Utc;
use kstd::lock_mgr::LockMgr;
use storage::error::Error;
use storage::format_vector::VectorMeta;
use storage::{
    BaseMetaKey, BgTaskHandler, CanonicalVector, ColumnFamilyIndex, FlatQueryCancel, Redis, Result,
    StorageOptions, VectorHit, VectorQuery, VectorSearchMode, VectorSearchOptions,
    VectorVsimTestGate, safe_cleanup_test_db, unique_test_db_path,
};

const QUERY_KEY: &[u8] = b"vectors";
const QUERY_MEMBER: &[u8] = b"query";
const OTHER_MEMBER: &[u8] = b"other";

fn open_redis(path: &Path, options: StorageOptions) -> Redis {
    let (bg_task_handler, _) = BgTaskHandler::new();
    let mut redis = Redis::new(
        Arc::new(options),
        1,
        Arc::new(bg_task_handler),
        Arc::new(LockMgr::new(1000)),
    );
    redis
        .open(path.to_str().expect("test path must be UTF-8"))
        .expect("open test Redis");
    redis
}

fn with_redis(options: StorageOptions, test: impl FnOnce(&Redis)) {
    let path = unique_test_db_path();
    safe_cleanup_test_db(&path);
    let redis = open_redis(&path, options);
    test(&redis);
    drop(redis);
    safe_cleanup_test_db(&path);
}

fn vector(values: &[f32]) -> CanonicalVector {
    CanonicalVector::from_values(values).expect("valid vector")
}

fn populate(redis: &Redis) {
    redis
        .vadd(QUERY_KEY, QUERY_MEMBER, &vector(&[1.0, 0.0]))
        .expect("add query member");
    redis
        .vadd(QUERY_KEY, OTHER_MEMBER, &vector(&[0.0, 1.0]))
        .expect("add other member");
}

fn options() -> VectorSearchOptions {
    VectorSearchOptions {
        count: 2,
        mode: VectorSearchMode::Truth,
    }
}

fn score_for(hits: &[VectorHit], element: &[u8]) -> f64 {
    hits.iter()
        .find(|hit| hit.element == element)
        .unwrap_or_else(|| panic!("missing hit for {element:?}: {hits:?}"))
        .score
}

fn assert_old_view(hits: &[VectorHit]) {
    assert_eq!(hits.len(), 2);
    assert!((score_for(hits, QUERY_MEMBER) - 1.0).abs() < 1e-6);
    assert!((score_for(hits, OTHER_MEMBER) - 0.5).abs() < 1e-6);
}

fn run_same_key_interleaving(
    redis: &Redis,
    mutation: impl FnOnce(&Redis) -> Result<()> + Send,
) -> Vec<VectorHit> {
    let gate = Arc::new(VectorVsimTestGate::default());
    redis
        .vector_fault_hooks
        .set_vsim_scan_gate(Some(Arc::clone(&gate)));
    let (writer_started_tx, writer_started_rx) = sync_channel(0);
    let (writer_done_tx, writer_done_rx) = sync_channel(0);

    let hits = std::thread::scope(|scope| {
        let query = scope.spawn(|| {
            redis.vsim(
                QUERY_KEY,
                VectorQuery::Element(QUERY_MEMBER.to_vec()),
                options(),
            )
        });
        assert!(
            gate.wait_until_entered(Duration::from_secs(5)),
            "VSIM did not reach the deterministic scan barrier"
        );

        let writer = scope.spawn(move || {
            writer_started_tx.send(()).expect("announce writer start");
            let result = mutation(redis);
            writer_done_tx.send(result).expect("publish writer result");
        });
        writer_started_rx.recv().expect("writer started");
        match writer_done_rx.recv_timeout(Duration::from_millis(200)) {
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                gate.release();
                panic!("writer disconnected while the query lock was held");
            }
            Ok(result) => {
                gate.release();
                panic!("same-key writer completed before VSIM released its session: {result:?}");
            }
        }

        gate.release();
        let hits = query.join().expect("query thread").expect("VSIM result");
        writer_done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("writer must complete after VSIM releases the session")
            .expect("writer mutation");
        writer.join().expect("writer thread");
        hits
    });
    redis.vector_fault_hooks.set_vsim_scan_gate(None);
    hits
}

#[test]
fn vsim_session_blocks_query_member_update_until_search_finishes() {
    with_redis(StorageOptions::default(), |redis| {
        populate(redis);
        let hits = run_same_key_interleaving(redis, |redis| {
            redis.vadd(QUERY_KEY, QUERY_MEMBER, &vector(&[0.0, 1.0]))?;
            Ok(())
        });

        assert_old_view(&hits);
        let updated = redis.vemb(QUERY_KEY, QUERY_MEMBER).unwrap().unwrap();
        assert!(updated[0].abs() < 1e-6);
        assert!((updated[1] - 1.0).abs() < 1e-6);
    });
}

#[test]
fn vsim_session_blocks_other_member_update_until_search_finishes() {
    with_redis(StorageOptions::default(), |redis| {
        populate(redis);
        let hits = run_same_key_interleaving(redis, |redis| {
            redis.vadd(QUERY_KEY, OTHER_MEMBER, &vector(&[1.0, 0.0]))?;
            Ok(())
        });

        assert_old_view(&hits);
        assert!((redis.vemb(QUERY_KEY, OTHER_MEMBER).unwrap().unwrap()[0] - 1.0).abs() < 1e-6);
    });
}

#[test]
fn vsim_session_blocks_query_member_remove_until_search_finishes() {
    with_redis(StorageOptions::default(), |redis| {
        populate(redis);
        let hits = run_same_key_interleaving(redis, |redis| {
            assert!(redis.vrem(QUERY_KEY, QUERY_MEMBER)?);
            Ok(())
        });

        assert_old_view(&hits);
        assert!(!redis.vismember(QUERY_KEY, QUERY_MEMBER).unwrap());
    });
}

#[test]
fn vsim_session_blocks_del_and_same_name_recreate_until_search_finishes() {
    with_redis(StorageOptions::default(), |redis| {
        populate(redis);
        let hits = run_same_key_interleaving(redis, |redis| {
            assert!(redis.del_key(QUERY_KEY)?);
            assert!(redis.vadd(QUERY_KEY, b"replacement", &vector(&[0.0, 1.0]))?);
            Ok(())
        });

        assert_old_view(&hits);
        assert_eq!(redis.vcard(QUERY_KEY).unwrap(), 1);
        assert!(redis.vismember(QUERY_KEY, b"replacement").unwrap());
    });
}

#[test]
fn vsim_session_does_not_block_write_to_different_key() {
    with_redis(StorageOptions::default(), |redis| {
        populate(redis);
        let gate = Arc::new(VectorVsimTestGate::default());
        redis
            .vector_fault_hooks
            .set_vsim_scan_gate(Some(Arc::clone(&gate)));
        let (writer_done_tx, writer_done_rx) = sync_channel(0);

        let hits = std::thread::scope(|scope| {
            let query = scope.spawn(|| {
                redis.vsim(
                    QUERY_KEY,
                    VectorQuery::Element(QUERY_MEMBER.to_vec()),
                    options(),
                )
            });
            assert!(gate.wait_until_entered(Duration::from_secs(5)));
            let writer = scope.spawn(|| {
                let result = redis.vadd(b"different", b"member", &vector(&[1.0, 0.0]));
                writer_done_tx.send(result).expect("publish writer result");
            });
            assert!(
                writer_done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("different-key writer must not wait for VSIM")
                    .expect("different-key mutation")
            );
            gate.release();
            let hits = query.join().expect("query thread").expect("VSIM result");
            writer.join().expect("writer thread");
            hits
        });
        redis.vector_fault_hooks.set_vsim_scan_gate(None);

        assert_old_view(&hits);
        assert_eq!(redis.vcard(b"different").unwrap(), 1);
    });
}

#[test]
fn vsim_session_direct_vector_and_ele_use_same_captured_time() {
    with_redis(StorageOptions::default(), |redis| {
        populate(redis);
        let logical_now = Utc::now().timestamp_micros() as u64 + 60_000_000;
        let meta_cf = redis
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .expect("MetaCF");
        let meta_key = BaseMetaKey::new(QUERY_KEY).encode().expect("meta key");
        let mut meta = VectorMeta::decode(
            &redis
                .db()
                .unwrap()
                .get_cf(&meta_cf, &meta_key)
                .unwrap()
                .unwrap(),
        )
        .expect("decode meta");
        meta.set_etime(logical_now + 10);
        redis
            .db()
            .unwrap()
            .put_cf(&meta_cf, &meta_key, meta.encode())
            .expect("write future expiry");

        for query in [
            VectorQuery::Vector(vector(&[1.0, 0.0])),
            VectorQuery::Element(QUERY_MEMBER.to_vec()),
        ] {
            redis
                .vector_fault_hooks
                .set_logical_now_micros_override(Some(logical_now));
            let element = match &query {
                VectorQuery::Element(element) => Some(element.as_slice()),
                VectorQuery::Vector(_) => None,
            };
            let session = redis
                .prepare_vsim_session(QUERY_KEY, element)
                .expect("prepare session")
                .expect("live set at captured time");
            redis
                .vector_fault_hooks
                .set_logical_now_micros_override(Some(logical_now + 20));
            let hits = session
                .search(query, options())
                .expect("captured-time search");
            assert_old_view(&hits);
        }
        redis
            .vector_fault_hooks
            .set_logical_now_micros_override(None);
    });
}

#[test]
fn vsim_session_releases_key_lock_on_parse_error() {
    with_redis(StorageOptions::default(), |redis| {
        populate(redis);
        let session = redis
            .prepare_vsim_session(QUERY_KEY, None)
            .expect("prepare session")
            .expect("existing set");
        // The command parser can fail after preparation. Dropping the session
        // must release the key guard without requiring a search call.
        drop(session);

        assert!(
            redis
                .vadd(QUERY_KEY, b"after-parse-error", &vector(&[1.0, 0.0]))
                .unwrap()
        );
    });
}

#[test]
fn vsim_session_releases_key_lock_on_flat_timeout_or_cancel() {
    with_redis(StorageOptions::default(), |redis| {
        populate(redis);
        let session = redis
            .prepare_vsim_session(QUERY_KEY, None)
            .unwrap()
            .unwrap();
        let cancel = FlatQueryCancel::default();
        cancel.cancel();
        let error = session
            .search_with_cancel(VectorQuery::Vector(vector(&[1.0, 0.0])), options(), &cancel)
            .expect_err("cancelled session");
        assert!(matches!(error, Error::VectorFlatQueryCancelled { .. }));
        assert!(
            redis
                .vadd(QUERY_KEY, b"after-cancel", &vector(&[1.0, 0.0]))
                .unwrap()
        );
    });

    let mut timeout_options = StorageOptions::default();
    timeout_options.vector.max_concurrent_flat_queries = 1;
    timeout_options.vector.flat_query_timeout_ms = 20;
    with_redis(timeout_options, |redis| {
        populate(redis);
        let _occupied = redis
            .flat_query_gate
            .acquire(Instant::now() + Duration::from_secs(1))
            .expect("occupy only FLAT permit");
        let session = redis
            .prepare_vsim_session(QUERY_KEY, None)
            .unwrap()
            .unwrap();
        let error = session
            .search(VectorQuery::Vector(vector(&[1.0, 0.0])), options())
            .expect_err("capacity wait must time out");
        assert!(matches!(error, Error::VectorFlatQueryTimeout { .. }));
        assert!(
            redis
                .vadd(QUERY_KEY, b"after-timeout", &vector(&[1.0, 0.0]))
                .unwrap()
        );
    });
}
