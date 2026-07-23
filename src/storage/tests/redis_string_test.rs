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

#[cfg(test)]
mod redis_string_test {
    use std::{collections::HashSet, mem::size_of, path::Path, sync::Arc, thread, time::Duration};

    use kstd::lock_mgr::LockMgr;
    use storage::{
        BaseMetaKey, BgTaskHandler, ColumnFamilyIndex, DataType, Redis, StorageOptions,
        ZsetScoreMember, safe_cleanup_test_db, slot_indexer::key_to_slot_id, storage::Storage,
        unique_test_db_path,
    };

    fn cleanup_redis(redis: Redis, test_db_path: &Path) {
        drop(redis);
        thread::sleep(Duration::from_millis(10));
        safe_cleanup_test_db(test_db_path);
    }

    #[test]
    fn test_redis_set() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"test_key";
        let value = b"test_value";

        let set_result = redis.set(key, value);
        assert!(
            set_result.is_ok(),
            "set command failed: {:?}",
            set_result.err()
        );

        let get_result = redis.get(key);
        assert!(
            get_result.is_ok(),
            "get command failed: {:?}",
            get_result.err()
        );

        assert_eq!(
            get_result.unwrap(),
            String::from_utf8_lossy(value).to_string()
        );

        cleanup_redis(redis, &test_db_path);
    }

    // Regression for issue #349: binary string reads must not use lossy UTF-8 conversion.
    #[test]
    fn test_redis_binary_get_and_mget_preserve_bytes() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        redis.open(test_db_path.to_str().unwrap()).unwrap();

        let first_key = b"binary:first";
        let second_key = b"binary:second";
        let first_value = [0, 1, 2, 3, 255];
        let second_value = [255, 0, 254];
        redis.set(first_key, &first_value).unwrap();
        redis.set(second_key, &second_value).unwrap();

        assert_eq!(redis.get_binary(first_key).unwrap(), first_value);
        assert_eq!(
            redis
                .mget_binary(&[
                    first_key.to_vec(),
                    b"binary:missing".to_vec(),
                    second_key.to_vec(),
                ])
                .unwrap(),
            vec![
                Some(first_value.to_vec()),
                None,
                Some(second_value.to_vec())
            ]
        );

        cleanup_redis(redis, &test_db_path);
    }

    #[tokio::test]
    async fn test_storage_multi_instance_mget_binary_preserves_mixed_result_order() {
        let test_db_dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::new(3, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), test_db_dir.path())
            .unwrap();

        assert_eq!(storage.insts.len(), 3);

        let first_binary_key = b"binary:instance:0".to_vec();
        let second_binary_key = b"binary:instance:2".to_vec();
        let wrong_type_key = b"binary:instance:4".to_vec();
        let instance_ids = [
            storage
                .slot_indexer
                .get_instance_id(key_to_slot_id(&first_binary_key)),
            storage
                .slot_indexer
                .get_instance_id(key_to_slot_id(&second_binary_key)),
            storage
                .slot_indexer
                .get_instance_id(key_to_slot_id(&wrong_type_key)),
        ];
        assert_eq!(HashSet::from(instance_ids).len(), 3);

        let first_binary_value = vec![0, 0xff, 1, 0xfe];
        let second_binary_value = vec![0xff, 2, 0, 0xfd];
        storage.set(&first_binary_key, &first_binary_value).unwrap();
        storage
            .set(&second_binary_key, &second_binary_value)
            .unwrap();
        storage
            .lpush(&wrong_type_key, &[b"list-value".to_vec()])
            .unwrap();

        let missing_key = b"binary:missing".to_vec();
        let keys = vec![
            second_binary_key.clone(),
            wrong_type_key,
            first_binary_key,
            missing_key,
            second_binary_key,
        ];

        assert_eq!(
            storage.mget_binary(&keys).unwrap(),
            vec![
                Some(second_binary_value.clone()),
                None,
                Some(first_binary_value),
                None,
                Some(second_binary_value),
            ]
        );
    }

    #[tokio::test]
    async fn test_storage_keys_supports_redis_glob_across_all_data_types() {
        let test_db_dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), test_db_dir.path())
            .unwrap();

        storage.set(b"test_alpha", b"string-value").unwrap();
        storage
            .lpush(b"test_beta", &[b"list-value".to_vec()])
            .unwrap();
        storage
            .hset(b"test_charlie", b"field", b"hash-value")
            .unwrap();
        storage.sadd(b"test_delta", &[b"set-value"]).unwrap();
        storage
            .zadd(
                b"test_echo",
                &[ZsetScoreMember::new(1.0, b"zset-value".to_vec())],
            )
            .unwrap();
        storage.set(b"test_xray", b"negated-class").unwrap();
        storage.set(b"other_foxtrot", b"non-match").unwrap();
        storage.set(b"literal*star", b"escaped-wildcard").unwrap();

        let keys = |pattern| {
            storage
                .keys(pattern)
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>()
        };

        assert_eq!(
            keys("test_*"),
            HashSet::from([
                "test_alpha".to_string(),
                "test_beta".to_string(),
                "test_charlie".to_string(),
                "test_delta".to_string(),
                "test_echo".to_string(),
                "test_xray".to_string(),
            ])
        );
        assert_eq!(keys("test_?cho"), HashSet::from(["test_echo".to_string()]));
        assert_eq!(
            keys("test_[ae]*"),
            HashSet::from(["test_alpha".to_string(), "test_echo".to_string()])
        );
        assert_eq!(
            keys("test_[a-c]*"),
            HashSet::from([
                "test_alpha".to_string(),
                "test_beta".to_string(),
                "test_charlie".to_string(),
            ])
        );
        assert_eq!(
            keys("test_[^x]*"),
            HashSet::from([
                "test_alpha".to_string(),
                "test_beta".to_string(),
                "test_charlie".to_string(),
                "test_delta".to_string(),
                "test_echo".to_string(),
            ])
        );
        assert_eq!(
            keys(r"literal\*star"),
            HashSet::from(["literal*star".to_string()])
        );
        assert!(!keys("test_*").contains("other_foxtrot"));
    }

    #[tokio::test]
    async fn test_storage_keys_excludes_empty_and_expired_composite_values() {
        let test_db_dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), test_db_dir.path())
            .unwrap();

        storage.hset(b"empty_hash", b"field", b"value").unwrap();
        storage.lpush(b"empty_list", &[b"value".to_vec()]).unwrap();
        storage.sadd(b"empty_set", &[b"value"]).unwrap();
        storage
            .zadd(
                b"empty_zset",
                &[ZsetScoreMember::new(1.0, b"value".to_vec())],
            )
            .unwrap();
        let redis = &storage.insts[0];
        let meta_cf = redis.get_cf_handle(ColumnFamilyIndex::MetaCF).unwrap();
        for key in [
            b"empty_hash".as_slice(),
            b"empty_list".as_slice(),
            b"empty_set".as_slice(),
            b"empty_zset".as_slice(),
        ] {
            let encoded_key = BaseMetaKey::new(key).encode().unwrap();
            let mut encoded_value = redis
                .db()
                .unwrap()
                .get_cf(&meta_cf, &encoded_key)
                .unwrap()
                .unwrap()
                .to_vec();
            encoded_value[1..1 + size_of::<u64>()].fill(0);
            redis
                .db()
                .unwrap()
                .put_cf(&meta_cf, encoded_key, encoded_value)
                .unwrap();
        }

        storage.hset(b"expired_hash", b"field", b"value").unwrap();
        storage
            .lpush(b"expired_list", &[b"value".to_vec()])
            .unwrap();
        storage.sadd(b"expired_set", &[b"value"]).unwrap();
        storage
            .zadd(
                b"expired_zset",
                &[ZsetScoreMember::new(1.0, b"value".to_vec())],
            )
            .unwrap();
        for key in [
            b"expired_hash".as_slice(),
            b"expired_list".as_slice(),
            b"expired_set".as_slice(),
            b"expired_zset".as_slice(),
        ] {
            assert!(storage.expire(key, 1).unwrap());
        }
        tokio::time::sleep(Duration::from_millis(1100)).await;

        assert!(storage.keys("empty_*").unwrap().is_empty());
        assert!(storage.keys("expired_*").unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_storage_keys_propagates_invalid_metadata_errors() {
        let test_db_dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), test_db_dir.path())
            .unwrap();

        let redis = &storage.insts[0];
        let meta_cf = redis.get_cf_handle(ColumnFamilyIndex::MetaCF).unwrap();
        let encoded_key = BaseMetaKey::new(b"broken_meta").encode().unwrap();
        redis
            .db()
            .unwrap()
            .put_cf(&meta_cf, encoded_key, [DataType::Hash as u8])
            .unwrap();

        assert!(storage.keys("*").is_err());
    }

    #[test]
    fn test_redis_set_multiple() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let test_cases = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ];

        for (key, value) in test_cases {
            let set_result = redis.set(key, value);
            assert!(
                set_result.is_ok(),
                "set command failed for key {:?}: {:?}",
                String::from_utf8_lossy(key),
                set_result.err()
            );
        }

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_getrange() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"test_key";
        let value = "你好世界".as_bytes(); // Each character is 3 bytes in UTF-8
        redis.set(key, value).unwrap();

        // Get first character (first 3 bytes)
        let result = redis.getrange(key, 0, 2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "你".as_bytes());

        // Get second character (bytes 3-5)
        let result = redis.getrange(key, 3, 5);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "好".as_bytes());

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_setrange() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"test_key";
        redis.set(key, b"Hello World").unwrap();

        // Replace "World" with "Redis"
        let result = redis.setrange(key, 6, b"Redis");
        assert!(result.is_ok(), "setrange should succeed");

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Hello Redis");

        let result = redis.setrange(key, 6, b"Rust Programming");
        assert!(result.is_ok());

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Hello Rust Programming");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_setrange_with_nulls() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"test_key";
        redis.set(key, b"Hello").unwrap();

        // Set at offset 10, should pad with null bytes
        let result = redis.setrange(key, 10, b"World");
        assert!(result.is_ok());

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Hello\x00\x00\x00\x00\x00World");

        // Set at beginning with null bytes
        let result = redis.setrange(key, 0, b"\x00\x00\x00\x00\x00Redis");
        assert!(result.is_ok());

        let value = redis.get(key).unwrap();
        // SETRANGE replaces bytes but doesn't truncate - the original string was 15 bytes
        // After replacing bytes 0-9 with "\x00\x00\x00\x00\x00Redis", bytes 10-14 ("World") remain
        assert_eq!(value.as_bytes(), b"\x00\x00\x00\x00\x00RedisWorld");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_setrange_errors() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"test_key";

        // Test negative offset
        let result = redis.setrange(key, -1, b"test");
        assert!(result.is_err(), "setrange should fail for negative offset");

        if let Err(e) = result {
            let message = e.to_string();
            if message.starts_with("ERR") {
                assert!(
                    message.starts_with("ERR"),
                    "Error message should start with ERR prefix"
                );
            }
        }

        // Test very large offset
        let result = redis.setrange(key, i32::MAX as i64 + 1, b"test");
        assert!(
            result.is_err(),
            "setrange should fail for offset > i32::MAX"
        );

        if let Err(e) = result {
            let message = e.to_string();
            if message.starts_with("ERR") {
                assert!(
                    message.starts_with("ERR"),
                    "Error message should start with ERR prefix"
                );
            }
        }

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_setrange_new_key() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"new_key";

        // Set at offset 3 on non-existing key
        let result = redis.setrange(key, 3, b"Hi");
        assert!(result.is_ok());

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"\x00\x00\x00Hi");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_setrange_wrong_type() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"hash_key";
        let field = b"field1";
        let value = b"value1";

        // Create a hash key first
        let hset_result = redis.hset(key, field, value);
        assert_eq!(hset_result.unwrap(), 1);

        // Try to use setrange on hash key
        let result = redis.setrange(key, 0, b"test");
        assert!(result.is_err(), "setrange should fail for non-string type");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_setrange_with_binary() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"binary_key";
        redis.set(key, b"Hello\x00World").unwrap(); // 11 bytes: H-e-l-l-o-\x00-W-o-r-l-d

        // Original: "Hello\x00World" (positions 0-10)
        // After:    "Hel\x00\x00\x00World" (positions 0-10, total 11 bytes)
        let result = redis.setrange(key, 3, b"\x00\x00\x00");
        assert!(result.is_ok());

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Hel\x00\x00\x00World");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_setex() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"test_key";
        let value = b"test_value";

        // Test invalid expire time (negative)
        let result = redis.setex(key, -1, value);
        if let Err(e) = result {
            let message = e.to_string();
            if message.contains("invalid expire time") {
                assert!(message.contains("invalid expire time"));
            }
        }

        // Test invalid expire time (zero)
        let result = redis.setex(key, 0, value);
        if let Err(e) = result {
            let message = e.to_string();
            if message.contains("invalid expire time") {
                assert!(message.contains("invalid expire time"));
            }
        }

        // Test valid setex
        let result = redis.setex(key, 1, value);
        assert!(result.is_ok());

        // Verify value is set
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), String::from_utf8_lossy(value));

        // Wait for expiration
        thread::sleep(Duration::from_secs(2));

        // Verify key is expired
        let get_result = redis.get(key);
        assert!(
            get_result.is_err(),
            "Key should be expired and return error"
        );

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_psetex() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"test_key";
        let value = b"test_value";

        // Test invalid expire time (negative)
        let result = redis.psetex(key, -1, value);
        if let Err(e) = result {
            let message = e.to_string();
            if message.contains("invalid expire time") {
                assert!(message.contains("invalid expire time"));
            }
        }

        // Test invalid expire time (zero)
        let result = redis.psetex(key, 0, value);
        if let Err(e) = result {
            let message = e.to_string();
            if message.contains("invalid expire time") {
                assert!(message.contains("invalid expire time"));
            }
        }

        // Test valid psetex (100ms)
        let result = redis.psetex(key, 100, value);
        assert!(result.is_ok());

        // Verify value is set
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), String::from_utf8_lossy(value));

        // Wait for expiration
        thread::sleep(Duration::from_millis(150));

        // Verify key is expired
        let get_result = redis.get(key);
        assert!(
            get_result.is_err(),
            "Key should be expired and return error"
        );

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_concurrent_set_get() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let redis_arc = Arc::new(redis);
        let num_threads = 8;
        let operations_per_thread = 100;

        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = format!("key_{}_{}", thread_id, i).into_bytes();
                    let value = format!("value_{}_{}", thread_id, i).into_bytes();

                    let result = redis_clone.set(&key, &value);
                    assert!(result.is_ok(), "Thread {}: set failed", thread_id);

                    let get_result = redis_clone.get(&key);
                    assert!(get_result.is_ok(), "Thread {}: get failed", thread_id);
                    assert_eq!(
                        get_result.unwrap(),
                        String::from_utf8_lossy(&value).to_string()
                    );
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        if let Ok(redis) = Arc::try_unwrap(redis_arc) {
            cleanup_redis(redis, &test_db_path);
        } else {
            safe_cleanup_test_db(&test_db_path);
        }
    }

    #[test]
    fn test_redis_setbit_getbit() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"bit_key";

        // Test setbit and getbit
        let result = redis.setbit(key, 7, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // Previous bit value was 0

        let result = redis.getbit(key, 7);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        // Test error cases
        let result = redis.setbit(key, -1, 1);
        assert!(result.is_err(), "setbit should fail with negative offset");

        let result = redis.getbit(key, -1);
        assert!(result.is_err(), "getbit should fail with negative offset");

        // Test invalid bit values
        let result = redis.setbit(key, 0, 2);
        assert!(result.is_err(), "setbit should fail with invalid bit value");

        let result = redis.setbit(key, 0, -1);
        assert!(result.is_err(), "setbit should fail with invalid bit value");

        // Test very large offset
        let result = redis.setbit(key, i64::MAX, 1);
        assert!(result.is_err(), "setbit should fail with very large offset");

        let result = redis.getbit(key, i64::MAX);
        assert!(result.is_err(), "getbit should fail with very large offset");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_setbit_getbit_expired() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"expired_bit_key";

        // Set a bit with expiration
        redis.setex(key, 1, b"test").unwrap();
        thread::sleep(Duration::from_secs(2));

        // Test getbit on expired key
        let result = redis.getbit(key, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0, "bit value should be 0 for expired key");

        // Test setbit on expired key (should create new key)
        let result = redis.setbit(key, 0, 1);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            0,
            "original bit value should be 0 for expired key"
        );

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_bitcount() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"bitcount_key";

        // Test bitcount on "aa" (0x61 0x61)
        redis.set(key, b"aa").unwrap();
        let result = redis.bitcount(key, None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 6, "bitcount should be 6 for \"aa\"");

        // Test bitcount with range - first byte
        let result = redis.bitcount(key, Some(0), Some(0));
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            3,
            "bitcount should be 3 for first byte of \"aa\""
        );

        // Test bitcount with range - second byte
        let result = redis.bitcount(key, Some(1), Some(1));
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            3,
            "bitcount should be 3 for second byte of \"aa\""
        );

        // Test bitcount with range - both bytes
        let result = redis.bitcount(key, Some(0), Some(1));
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            6,
            "bitcount should be 6 for both bytes of \"aa\""
        );

        // Test bitcount on non-existing key
        let result = redis.bitcount(b"nonexistent", None, None);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            0,
            "bitcount should be 0 for non-existing key"
        );

        // Test bitcount on empty string
        redis.set(key, b"").unwrap();
        let result = redis.bitcount(key, None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0, "bitcount should be 0 for empty string");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_bitcount_complex() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"complex_bitcount";

        // Test with "hello"
        redis.set(key, b"hello").unwrap();
        let result = redis.bitcount(key, None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 21, "bitcount should be 21 for \"hello\"");

        // Test with range on "hello" - bytes 2-4 ("llo")
        let result = redis.bitcount(key, Some(2), Some(4));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 14, "bitcount should be 14 for \"llo\"");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_bitcount_with_setbit() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"setbit_bitcount";

        // Set some bits
        redis.setbit(key, 0, 1).unwrap();
        redis.setbit(key, 2, 1).unwrap();
        redis.setbit(key, 4, 1).unwrap();
        redis.setbit(key, 8, 1).unwrap();

        // Count bits
        let result = redis.bitcount(key, None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4);

        // Test with "test" string
        redis.set(key, b"test").unwrap();
        let result = redis.bitcount(key, None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 17, "bitcount should be 17 for \"test\"");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_bitcount_expired() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"expired_bitcount";

        // Set key with expiration
        redis.setex(key, 1, b"test").unwrap();
        thread::sleep(Duration::from_secs(2));

        // Test bitcount on expired key
        let result = redis.bitcount(key, None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0, "bitcount should be 0 for expired key");

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_redis_strlen_and_getrange_wrongtype() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"wrongtype_string_meta";
        redis.hset(key, b"field", b"value").unwrap();

        let get_result = redis.get(key);
        assert!(get_result.is_err(), "get should fail on non-string key");
        assert!(
            get_result.unwrap_err().to_string().contains("WRONGTYPE"),
            "get should return WRONGTYPE"
        );

        let mget_result = redis.mget(&[key.to_vec()]).unwrap();
        assert_eq!(mget_result, vec![None]);

        let strlen_result = redis.strlen(key);
        assert!(
            strlen_result.is_err(),
            "strlen should fail on non-string key"
        );
        assert!(
            strlen_result.unwrap_err().to_string().contains("WRONGTYPE"),
            "strlen should return WRONGTYPE"
        );

        let getrange_result = redis.getrange(key, 0, 2);
        assert!(
            getrange_result.is_err(),
            "getrange should fail on non-string key"
        );
        assert!(
            getrange_result
                .unwrap_err()
                .to_string()
                .contains("WRONGTYPE"),
            "getrange should return WRONGTYPE"
        );

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_getbit_wrongtype_and_expired_wrongtype() {
        let test_db_path = unique_test_db_path();
        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);
        redis.open(test_db_path.to_str().unwrap()).unwrap();

        let live_wrongtype = b"getbit_live_wrongtype";
        redis.hset(live_wrongtype, b"field", b"value").unwrap();
        let err = redis.getbit(live_wrongtype, 0).unwrap_err();
        assert!(err.to_string().contains("WRONGTYPE"));

        let expired_wrongtype = b"getbit_expired_wrongtype";
        redis.hset(expired_wrongtype, b"field", b"value").unwrap();
        assert!(redis.set_key_etime(expired_wrongtype, 1).unwrap());
        assert_eq!(redis.getbit(expired_wrongtype, 0).unwrap(), 0);

        cleanup_redis(redis, &test_db_path);
    }

    #[test]
    fn test_setnx_and_msetnx_keep_live_any_type_existence_semantics() {
        let test_db_path = unique_test_db_path();
        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);
        redis.open(test_db_path.to_str().unwrap()).unwrap();

        let key = b"setnx_existing_hash";
        redis.hset(key, b"field", b"value").unwrap();

        assert_eq!(redis.setnx(key, b"new").unwrap(), 0);
        assert!(!redis.msetnx(&[(key.to_vec(), b"new".to_vec())]).unwrap());
        let getset_result = redis.getset(key, b"other_value");
        assert!(
            getset_result.is_err(),
            "GETSET should reject non-string keys"
        );
        assert!(
            getset_result.unwrap_err().to_string().contains("WRONGTYPE"),
            "GETSET should return WRONGTYPE"
        );
        assert_eq!(redis.get_key_type(key).unwrap(), DataType::Hash);

        cleanup_redis(redis, &test_db_path);
    }
}
