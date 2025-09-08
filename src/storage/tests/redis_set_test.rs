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

#[cfg(test)]
mod redis_set_test {
    use std::sync::Arc;

    use bytes::BufMut;
    use kstd::lock_mgr::LockMgr;
    use storage::{BgTaskHandler, ColumnFamilyIndex, Redis, StorageOptions, unique_test_db_path};

    // Build a valid Set meta bytes:
    // layout: type(1) + count(8) + version(8) + reserve(16) + ctime(8) + etime(8)
    fn build_empty_set_meta_bytes() -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 8 + 8 + 16 + 8 + 8);
        // DataType::Set = 2
        buf.put_u8(2u8);
        // count = 0
        buf.put_u64_le(0);
        // version = 0 (will be replaced by initial_meta_value in sadd)
        buf.put_u64_le(0);
        // reserve 16 bytes
        buf.extend_from_slice(&[0u8; 16]);
        // ctime, etime = 0 (may be updated by sadd)
        buf.put_u64_le(0);
        buf.put_u64_le(0);
        buf
    }

    #[test]
    fn test_sadd_basic_and_dedup() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Pre-create set meta (sadd currently requires existing set meta for the key)
        let key = b"set_key_1";
        let meta_bytes = build_empty_set_meta_bytes();
        {
            let db = redis.db.as_ref().unwrap();
            let cf = redis.get_cf_handle(ColumnFamilyIndex::MetaCF).unwrap();
            db.put_cf(&cf, key, &meta_bytes).unwrap();
        }

        // Call sadd with duplicates; expect only unique inserts counted
        let members: Vec<&[u8]> = vec![b"a".as_ref(), b"b".as_ref(), b"a".as_ref()];
        let mut ret = 0;
        let sadd_res = redis.sadd(key, &members, &mut ret);
        assert!(sadd_res.is_ok(), "sadd failed: {:?}", sadd_res.err());
        assert_eq!(ret, 2);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_scard_after_sadd_and_missing_key() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Prepare set meta and add two unique members
        let key = b"set_key_2";
        let meta_bytes = build_empty_set_meta_bytes();
        {
            let db = redis.db.as_ref().unwrap();
            let cf = redis.get_cf_handle(ColumnFamilyIndex::MetaCF).unwrap();
            db.put_cf(&cf, key, &meta_bytes).unwrap();
        }

        let members: Vec<&[u8]> = vec![b"x".as_ref(), b"y".as_ref(), b"x".as_ref()];
        let mut added = 0;
        let sadd_res = redis.sadd(key, &members, &mut added);
        assert!(sadd_res.is_ok(), "sadd failed: {:?}", sadd_res.err());
        assert_eq!(added, 2);

        // scard should report 2
        let mut card = 0;
        let scard_res = redis.scard(key, &mut card);
        assert!(scard_res.is_ok(), "scard failed: {:?}", scard_res.err());
        assert_eq!(card, 2);

        // scard on missing key should error
        let mut card_missing = 0;
        let scard_missing = redis.scard(b"missing_key", &mut card_missing);
        assert!(scard_missing.is_err());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }
}
