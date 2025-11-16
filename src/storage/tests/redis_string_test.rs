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
mod redis_string_test {
    use std::{sync::Arc, thread, time::Duration};

    use kstd::lock_mgr::LockMgr;
    use storage::{
        BgTaskHandler, Redis, StorageOptions, safe_cleanup_test_db, unique_test_db_path,
    };

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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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
            redis.set_need_close(true);
        }

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
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

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
    }
}
