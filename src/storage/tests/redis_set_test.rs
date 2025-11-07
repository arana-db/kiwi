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
    use storage::{
        BaseMetaKey, BgTaskHandler, ColumnFamilyIndex, Redis, StorageOptions, unique_test_db_path,
    };

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
    fn test_smembers_basic() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Prepare set meta
        let key = b"set_key_members";
        let meta_bytes = build_empty_set_meta_bytes();
        {
            let db = redis.db.as_ref().unwrap();
            let cf = redis.get_cf_handle(ColumnFamilyIndex::MetaCF).unwrap();
            let encoded_key = BaseMetaKey::new(key).encode().unwrap();
            db.put_cf(&cf, &encoded_key, &meta_bytes).unwrap();
        }

        // Add members (with duplicates) via sadd
        let members: Vec<&[u8]> = vec![b"m1".as_ref(), b"m2".as_ref(), b"m1".as_ref()];
        let added = redis.sadd(key, &members).expect("sadd failed");
        assert_eq!(added, 2);

        // Fetch members
        let mut out = redis.smembers(key).expect("smembers failed");
        out.sort();
        let mut expected = vec!["m1".to_string(), "m2".to_string()];
        expected.sort();
        assert_eq!(out, expected);

        // Missing key should return empty array (Redis behavior)
        let missing_res = redis.smembers(b"missing_key_members");
        assert!(missing_res.is_ok());
        assert_eq!(missing_res.unwrap(), Vec::<String>::new());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }
    #[test]
    fn test_sadd_basic_and_dedup() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Call sadd with duplicates on non-existing key; expect only unique inserts counted
        let key = b"set_key_1";
        let members: Vec<&[u8]> = vec![b"a".as_ref(), b"b".as_ref(), b"a".as_ref()];
        let added = redis.sadd(key, &members).expect("sadd failed");
        assert_eq!(added, 2);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_scard_after_sadd_and_missing_key() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Add two unique members to a new set
        let key = b"set_key_2";
        let members: Vec<&[u8]> = vec![b"x".as_ref(), b"y".as_ref(), b"x".as_ref()];
        let added = redis.sadd(key, &members).expect("sadd failed");
        assert_eq!(added, 2);

        // scard should report 2
        let card = redis.scard(key).expect("scard failed");
        assert_eq!(card, 2);

        // scard on missing key should error
        let scard_missing = redis.scard(b"missing_key");
        assert!(scard_missing.is_err());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sadd_to_existing_set() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"set_key_existing";

        // First sadd - create set with 2 members
        let members1: Vec<&[u8]> = vec![b"a".as_ref(), b"b".as_ref()];
        let added1 = redis.sadd(key, &members1).expect("first sadd failed");
        assert_eq!(added1, 2);

        // Second sadd - add 1 new member and 1 existing member
        let members2: Vec<&[u8]> = vec![b"b".as_ref(), b"c".as_ref()];
        let added2 = redis.sadd(key, &members2).expect("second sadd failed");
        assert_eq!(added2, 1); // Only 'c' should be added, 'b' already exists

        // Verify total count
        let card = redis.scard(key).expect("scard failed");
        assert_eq!(card, 3);

        // Verify all members are present
        let mut members = redis.smembers(key).expect("smembers failed");
        members.sort();
        let mut expected = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        expected.sort();
        assert_eq!(members, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_smembers_empty_set() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"empty_set_key";

        // Create empty set
        let members: Vec<&[u8]> = vec![];
        let added = redis.sadd(key, &members).expect("sadd failed");
        assert_eq!(added, 0);

        // smembers on empty set should return No-op SADD (empty members)
        let members_result = redis.smembers(key);
        assert!(members_result.is_ok());
        assert_eq!(members_result.unwrap(), Vec::<String>::new());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_smembers_large_set() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"large_set_key";

        // Create a set with many members
        let member_count = 100;
        let members: Vec<Vec<u8>> = (0..member_count)
            .map(|i| format!("member_{}", i).into_bytes())
            .collect();
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();

        let added = redis.sadd(key, &member_refs).expect("sadd failed");
        assert_eq!(added, member_count as i32);

        // Verify all members are returned
        let mut result_members = redis.smembers(key).expect("smembers failed");
        result_members.sort();

        let mut expected_members: Vec<String> =
            (0..member_count).map(|i| format!("member_{}", i)).collect();
        expected_members.sort();

        assert_eq!(result_members, expected_members);
        assert_eq!(result_members.len(), member_count);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_smembers_unicode_members() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"unicode_set_key";

        // Add unicode members
        let members: Vec<&[u8]> = vec![
            "你好".as_bytes(),
            "世界".as_bytes(),
            "🚀".as_bytes(),
            "emoji🎉".as_bytes(),
        ];

        let added = redis.sadd(key, &members).expect("sadd failed");
        assert_eq!(added, 4);

        // Verify unicode members are returned correctly
        let mut result_members = redis.smembers(key).expect("smembers failed");
        result_members.sort();

        let mut expected_members = vec![
            "你好".to_string(),
            "世界".to_string(),
            "🚀".to_string(),
            "emoji🎉".to_string(),
        ];
        expected_members.sort();

        assert_eq!(result_members, expected_members);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sdiff_basic() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b, c}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 3);

        // Create second set: {b, c, d}
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"b", b"c", b"d"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 3);

        // SDIFF set1 set2 should return {a}
        let keys: Vec<&[u8]> = vec![key1, key2];
        let mut diff_result = redis.sdiff(&keys).expect("sdiff failed");
        diff_result.sort();
        assert_eq!(diff_result, vec!["a".to_string()]);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sdiff_multiple_sets() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b, c, d, e}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 5);

        // Create second set: {b, c}
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"b", b"c"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 2);

        // Create third set: {d}
        let key3 = b"set3";
        let members3: Vec<&[u8]> = vec![b"d"];
        let added3 = redis.sadd(key3, &members3).expect("sadd failed");
        assert_eq!(added3, 1);

        // SDIFF set1 set2 set3 should return {a, e}
        let keys: Vec<&[u8]> = vec![key1, key2, key3];
        let mut diff_result = redis.sdiff(&keys).expect("sdiff failed");
        diff_result.sort();
        let mut expected = vec!["a".to_string(), "e".to_string()];
        expected.sort();
        assert_eq!(diff_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sdiff_non_existent_keys() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // SDIFF with non-existent first key should return empty
        let keys: Vec<&[u8]> = vec![b"nonexistent1".as_ref(), b"nonexistent2".as_ref()];
        let diff_result = redis.sdiff(&keys).expect("sdiff failed");
        assert_eq!(diff_result, Vec::<String>::new());

        // Create a set
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 2);

        // SDIFF set1 nonexistent should return all members of set1
        let keys: Vec<&[u8]> = vec![key1, b"nonexistent".as_ref()];
        let mut diff_result = redis.sdiff(&keys).expect("sdiff failed");
        diff_result.sort();
        let mut expected = vec!["a".to_string(), "b".to_string()];
        expected.sort();
        assert_eq!(diff_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sdiff_empty_sets() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 2);

        // Create second set: {a, b} (same as first)
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"a", b"b"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 2);

        // SDIFF set1 set2 should return empty (all elements are common)
        let keys: Vec<&[u8]> = vec![key1, key2];
        let diff_result = redis.sdiff(&keys).expect("sdiff failed");
        assert_eq!(diff_result, Vec::<String>::new());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sinter_basic() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b, c}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 3);

        // Create second set: {b, c, d}
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"b", b"c", b"d"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 3);

        // SINTER set1 set2 should return {b, c}
        let keys: Vec<&[u8]> = vec![key1, key2];
        let mut inter_result = redis.sinter(&keys).expect("sinter failed");
        inter_result.sort();
        let mut expected = vec!["b".to_string(), "c".to_string()];
        expected.sort();
        assert_eq!(inter_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sinter_multiple_sets() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b, c, d}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 4);

        // Create second set: {b, c, d, e}
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"b", b"c", b"d", b"e"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 4);

        // Create third set: {c, d, e, f}
        let key3 = b"set3";
        let members3: Vec<&[u8]> = vec![b"c", b"d", b"e", b"f"];
        let added3 = redis.sadd(key3, &members3).expect("sadd failed");
        assert_eq!(added3, 4);

        // SINTER set1 set2 set3 should return {c, d}
        let keys: Vec<&[u8]> = vec![key1, key2, key3];
        let mut inter_result = redis.sinter(&keys).expect("sinter failed");
        inter_result.sort();
        let mut expected = vec!["c".to_string(), "d".to_string()];
        expected.sort();
        assert_eq!(inter_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sinter_empty_result() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 2);

        // Create second set: {c, d} (no common elements)
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"c", b"d"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 2);

        // SINTER set1 set2 should return empty
        let keys: Vec<&[u8]> = vec![key1, key2];
        let inter_result = redis.sinter(&keys).expect("sinter failed");
        assert_eq!(inter_result, Vec::<String>::new());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sinter_non_existent_keys() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // SINTER with non-existent keys should return empty
        let keys: Vec<&[u8]> = vec![b"nonexistent1".as_ref(), b"nonexistent2".as_ref()];
        let inter_result = redis.sinter(&keys).expect("sinter failed");
        assert_eq!(inter_result, Vec::<String>::new());

        // Create a set
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 2);

        // SINTER set1 nonexistent should return empty (intersection with empty set)
        let keys: Vec<&[u8]> = vec![key1, b"nonexistent".as_ref()];
        let inter_result = redis.sinter(&keys).expect("sinter failed");
        assert_eq!(inter_result, Vec::<String>::new());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sinter_single_set() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create a set: {a, b, c}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 3);

        // SINTER with single set should return all its members
        let keys: Vec<&[u8]> = vec![key1];
        let mut inter_result = redis.sinter(&keys).expect("sinter failed");
        inter_result.sort();
        let mut expected = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        expected.sort();
        assert_eq!(inter_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sunion_basic() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 2);

        // Create second set: {b, c}
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"b", b"c"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 2);

        // SUNION set1 set2 should return {a, b, c}
        let keys: Vec<&[u8]> = vec![key1, key2];
        let mut union_result = redis.sunion(&keys).expect("sunion failed");
        union_result.sort();
        let mut expected = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        expected.sort();
        assert_eq!(union_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sunion_multiple_sets() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 2);

        // Create second set: {b, c}
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"b", b"c"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 2);

        // Create third set: {c, d}
        let key3 = b"set3";
        let members3: Vec<&[u8]> = vec![b"c", b"d"];
        let added3 = redis.sadd(key3, &members3).expect("sadd failed");
        assert_eq!(added3, 2);

        // SUNION set1 set2 set3 should return {a, b, c, d}
        let keys: Vec<&[u8]> = vec![key1, key2, key3];
        let mut union_result = redis.sunion(&keys).expect("sunion failed");
        union_result.sort();
        let mut expected = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        expected.sort();
        assert_eq!(union_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sunion_non_existent_keys() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // SUNION with non-existent keys should return empty
        let keys: Vec<&[u8]> = vec![b"nonexistent1".as_ref(), b"nonexistent2".as_ref()];
        let union_result = redis.sunion(&keys).expect("sunion failed");
        assert_eq!(union_result, Vec::<String>::new());

        // Create a set
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 2);

        // SUNION set1 nonexistent should return all members of set1
        let keys: Vec<&[u8]> = vec![key1, b"nonexistent".as_ref()];
        let mut union_result = redis.sunion(&keys).expect("sunion failed");
        union_result.sort();
        let mut expected = vec!["a".to_string(), "b".to_string()];
        expected.sort();
        assert_eq!(union_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sunion_single_set() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create a set: {a, b, c}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 3);

        // SUNION with single set should return all its members
        let keys: Vec<&[u8]> = vec![key1];
        let mut union_result = redis.sunion(&keys).expect("sunion failed");
        union_result.sort();
        let mut expected = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        expected.sort();
        assert_eq!(union_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sunion_uniqueness() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create first set: {a, b, c}
        let key1 = b"set1";
        let members1: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let added1 = redis.sadd(key1, &members1).expect("sadd failed");
        assert_eq!(added1, 3);

        // Create second set: {a, b, c} (same as first)
        let key2 = b"set2";
        let members2: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let added2 = redis.sadd(key2, &members2).expect("sadd failed");
        assert_eq!(added2, 3);

        // SUNION set1 set2 should return {a, b, c} (no duplicates)
        let keys: Vec<&[u8]> = vec![key1, key2];
        let mut union_result = redis.sunion(&keys).expect("sunion failed");
        union_result.sort();
        let mut expected = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        expected.sort();
        assert_eq!(union_result, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sscan_basic() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create a set with multiple members
        let key = b"test_set";
        let members: Vec<&[u8]> = vec![b"member1", b"member2", b"member3", b"member4", b"member5"];
        let added = redis.sadd(key, &members).expect("sadd failed");
        assert_eq!(added, 5);

        // Test SSCAN with cursor 0
        let (next_cursor, scan_members) = redis.sscan(key, 0, None, Some(3)).expect("sscan failed");
        assert_eq!(scan_members.len(), 3);
        assert!(next_cursor > 0); // Should have more data

        // Continue scanning
        let (final_cursor, remaining_members) = redis
            .sscan(key, next_cursor, None, Some(10))
            .expect("sscan failed");
        assert_eq!(remaining_members.len(), 2);
        assert_eq!(final_cursor, 0); // Should be end of iteration

        // Verify all members were returned
        let mut all_scanned = scan_members;
        all_scanned.extend(remaining_members);
        all_scanned.sort();

        let mut expected = vec![
            "member1".to_string(),
            "member2".to_string(),
            "member3".to_string(),
            "member4".to_string(),
            "member5".to_string(),
        ];
        expected.sort();
        assert_eq!(all_scanned, expected);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sscan_pattern_matching() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create a set with various members
        let key = b"test_set";
        let members: Vec<&[u8]> = vec![b"apple", b"banana", b"cherry", b"apricot", b"blueberry"];
        let added = redis.sadd(key, &members).expect("sadd failed");
        assert_eq!(added, 5);

        // Test pattern matching with wildcard
        let (cursor, matched_members) =
            redis.sscan(key, 0, Some("a*"), None).expect("sscan failed");
        assert_eq!(cursor, 0); // Should complete in one scan
        let mut matched_sorted = matched_members;
        matched_sorted.sort();
        let mut expected = vec!["apple".to_string(), "apricot".to_string()];
        expected.sort();
        assert_eq!(matched_sorted, expected);

        // Test pattern matching with single character wildcard
        let (cursor, matched_members) = redis
            .sscan(key, 0, Some("?????"), None)
            .expect("sscan failed");
        assert_eq!(cursor, 0);
        assert_eq!(matched_members, vec!["apple".to_string()]);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sscan_empty_set() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Test SSCAN on non-existent key
        let key = b"nonexistent";
        let (cursor, members) = redis.sscan(key, 0, None, None).expect("sscan failed");
        assert_eq!(cursor, 0);
        assert_eq!(members, Vec::<String>::new());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }

    #[test]
    fn test_sscan_count_parameter() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(&test_db_path);
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create a set with many members
        let key = b"test_set";
        let members: Vec<&[u8]> = (0..20)
            .map(|i| Box::leak(format!("member{:02}", i).into_boxed_str()).as_bytes())
            .collect();
        let added = redis.sadd(key, &members).expect("sadd failed");
        assert_eq!(added, 20);

        // Test with count=1
        let (cursor, scan_members) = redis.sscan(key, 0, None, Some(1)).expect("sscan failed");
        assert_eq!(scan_members.len(), 1);
        assert!(cursor > 0);

        // Test with large count
        let (cursor, scan_members) = redis.sscan(key, 0, None, Some(100)).expect("sscan failed");
        assert_eq!(scan_members.len(), 20); // Should return all members
        assert_eq!(cursor, 0); // Should complete

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            let _ = std::fs::remove_dir_all(test_db_path);
        }
    }
}
