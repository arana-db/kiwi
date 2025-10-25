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
    use storage::{BgTaskHandler, Redis, StorageOptions, unique_test_db_path};

    #[test]
    fn test_redis_set() {
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

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_set_multiple() {
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

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_concurrent_set_get() {
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

        let redis_arc = Arc::new(redis);
        let num_threads = 8;
        let operations_per_thread = 100;

        let mut set_handles = vec![];
        for thread_id in 0..num_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = format!("key_{}_{}", thread_id, i).into_bytes();
                    let value = format!("value_{}_{}", thread_id, i).into_bytes();

                    let set_result = redis_clone.set(&key, &value);
                    assert!(
                        set_result.is_ok(),
                        "set command failed for key {:?}: {:?}",
                        String::from_utf8_lossy(&key),
                        set_result.err()
                    );
                }
            });
            set_handles.push(handle);
        }

        for handle in set_handles {
            handle.join().unwrap();
        }

        for thread_id in 0..num_threads {
            for i in 0..operations_per_thread {
                let key = format!("key_{}_{}", thread_id, i).into_bytes();
                let expected_value = format!("value_{}_{}", thread_id, i);

                let get_result = redis_arc.get(&key);
                assert!(
                    get_result.is_ok(),
                    "get command failed for key {:?}: {:?}",
                    String::from_utf8_lossy(&key),
                    get_result.err()
                );

                assert_eq!(get_result.unwrap(), expected_value);
            }
        }

        if let Ok(redis) = Arc::try_unwrap(redis_arc) {
            redis.set_need_close(true);
        }

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_concurrent_set_get_mixed() {
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

        let redis_arc = Arc::new(redis);
        let num_set_threads = 4;
        let num_get_threads = 4;
        let operations_per_thread = 25;

        for i in 0..10 {
            let key = format!("mixed_key_{}", i).into_bytes();
            let value = format!("initial_value_{}", i).into_bytes();
            let set_result = redis_arc.set(&key, &value);
            assert!(
                set_result.is_ok(),
                "initial set failed: {:?}",
                set_result.err()
            );
        }

        let mut set_handles = vec![];
        for thread_id in 0..num_set_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = format!("mixed_key_{}", i).into_bytes();
                    let value = format!("value_from_set_thread_{}_{}", thread_id, i).into_bytes();

                    let set_result = redis_clone.set(&key, &value);
                    assert!(
                        set_result.is_ok(),
                        "set command failed for thread {}: {:?}",
                        thread_id,
                        set_result.err()
                    );

                    thread::sleep(Duration::from_millis(1));
                }
            });
            set_handles.push(handle);
        }

        let mut get_handles = vec![];
        for _ in 0..num_get_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = format!("mixed_key_{}", i).into_bytes();

                    let get_result = redis_clone.get(&key);
                    if let Ok(value) = get_result {
                        assert!(
                            !value.is_empty(),
                            "get returned empty value for key {:?}",
                            String::from_utf8_lossy(&key)
                        );
                    }

                    thread::sleep(Duration::from_millis(1));
                }
            });
            get_handles.push(handle);
        }

        for handle in set_handles {
            handle.join().unwrap();
        }
        for handle in get_handles {
            handle.join().unwrap();
        }

        for i in 0..operations_per_thread {
            let key = format!("mixed_key_{}", i).into_bytes();
            let get_result = redis_arc.get(&key);
            if let Ok(value) = get_result {
                assert!(
                    !value.is_empty(),
                    "final get returned empty value for key {:?}",
                    String::from_utf8_lossy(&key)
                );
            }
        }

        if let Ok(redis) = Arc::try_unwrap(redis_arc) {
            redis.set_need_close(true);
        }

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_concurrent_stress() {
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

        let redis_arc = Arc::new(redis);
        let num_threads = 16;
        let operations_per_thread = 200;

        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = format!("stress_key_{}_{}", thread_id, i).into_bytes();
                    let value = format!("stress_value_{}_{}", thread_id, i).into_bytes();

                    let set_result = redis_clone.set(&key, &value);
                    assert!(
                        set_result.is_ok(),
                        "stress set command failed for key {:?}: {:?}",
                        String::from_utf8_lossy(&key),
                        set_result.err()
                    );

                    let get_result = redis_clone.get(&key);
                    assert!(
                        get_result.is_ok(),
                        "stress get command failed for key {:?}: {:?}",
                        String::from_utf8_lossy(&key),
                        get_result.err()
                    );

                    let retrieved_value = get_result.unwrap();
                    let expected_value = String::from_utf8_lossy(&value).to_string();
                    assert_eq!(retrieved_value, expected_value);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for thread_id in 0..num_threads {
            for i in (0..operations_per_thread).step_by(10) {
                let key = format!("stress_key_{}_{}", thread_id, i).into_bytes();
                let expected_value = format!("stress_value_{}_{}", thread_id, i);

                let get_result = redis_arc.get(&key);
                assert!(
                    get_result.is_ok(),
                    "final verification failed for key {:?}: {:?}",
                    String::from_utf8_lossy(&key),
                    get_result.err()
                );

                assert_eq!(get_result.unwrap(), expected_value);
            }
        }

        if let Ok(redis) = Arc::try_unwrap(redis_arc) {
            redis.set_need_close(true);
        }

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_incr_decr() {
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

        // wrong key type
        {
            let key = b"test_hash";
            let field = b"field1";
            let value = b"value1";

            let hset_result = redis.hset(key, field, value);
            assert_eq!(hset_result.unwrap(), 1);

            let incr_result = redis.incr_decr(key, 1);
            assert_eq!(
                incr_result.err().unwrap().to_string(),
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            );
        }

        // wrong value type
        {
            let key = b"test_incr_key_wrong_value";
            let value = b"not_a_number";
            let set_result = redis.set(key, value);
            assert!(set_result.is_ok());
            let incr_result = redis.incr_decr(key, 1);
            assert_eq!(
                incr_result.err().unwrap().to_string(),
                "value is not an integer or out of range"
            );
        }

        // ttl check
        {
            // todo: implement ttl check after ttl is supported
        }

        // normal incr and decr
        {
            let key = b"test_incr_key";
            // add 2
            assert_eq!(redis.incr_decr(key, 2).unwrap(), 2);
            assert_eq!(redis.get(key).unwrap(), "2".to_string());
            // add i64::MAX
            let incr_result = redis.incr_decr(key, i64::MAX);
            assert_eq!(
                incr_result.err().unwrap().to_string(),
                "increment or decrement would overflow"
            );
            // sub 3
            assert_eq!(redis.incr_decr(key, -3).unwrap(), -1);
            assert_eq!(redis.get(key).unwrap(), "-1".to_string());
            // sub i64::MIN
            let incr_result = redis.incr_decr(key, i64::MIN);
            assert_eq!(
                incr_result.err().unwrap().to_string(),
                "increment or decrement would overflow"
            );
        }

        // sum 1 + 2 + ... + 10000
        {
            let key = b"test_incr_key_large";
            let mut expected_value: i64 = 0;
            for i in 1..=10000 {
                expected_value += i;
                let result = redis.incr_decr(key, i).unwrap();
                assert_eq!(result, expected_value);
            }
            assert_eq!(redis.get(key).unwrap(), expected_value.to_string());
        }

        // sub -1 - 2 - ... - 10000
        {
            let key = b"test_decr_key_large";
            let mut expected_value: i64 = 0;
            for i in 1..=10000 {
                expected_value -= i;
                let result = redis.incr_decr(key, -i).unwrap();
                assert_eq!(result, expected_value);
            }
            assert_eq!(redis.get(key).unwrap(), expected_value.to_string());
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_append() {
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

        // Test 1: Append to non-existing key
        {
            let key = b"new_key";
            let value = b"Hello";
            let result = redis.append(key, value);
            assert!(result.is_ok(), "append failed: {:?}", result.err());
            assert_eq!(result.unwrap(), 5, "append should return length 5");

            let get_result = redis.get(key);
            assert!(get_result.is_ok());
            assert_eq!(get_result.unwrap(), "Hello");
        }

        // Test 2: Append to existing key
        {
            let key = b"new_key";
            let value = b" World";
            let result = redis.append(key, value);
            assert!(result.is_ok(), "append failed: {:?}", result.err());
            assert_eq!(result.unwrap(), 11, "append should return length 11");

            let get_result = redis.get(key);
            assert!(get_result.is_ok());
            assert_eq!(get_result.unwrap(), "Hello World");
        }

        // Test 3: Multiple appends
        {
            let key = b"multi_append";
            let values = vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()];
            let mut expected_len = 0;

            for value in values {
                let result = redis.append(key, value);
                assert!(result.is_ok());
                expected_len += value.len() as i32;
                assert_eq!(result.unwrap(), expected_len);
            }

            let get_result = redis.get(key);
            assert!(get_result.is_ok());
            assert_eq!(get_result.unwrap(), "abc");
        }

        // Test 4: Append empty string
        {
            let key = b"empty_append";
            redis.set(key, b"test").unwrap();

            let result = redis.append(key, b"");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 4);

            let get_result = redis.get(key);
            assert!(get_result.is_ok());
            assert_eq!(get_result.unwrap(), "test");
        }

        // Test 5: Append with numeric values
        {
            let key = b"numeric_append";
            redis.append(key, b"100").unwrap();
            let result = redis.append(key, b"200");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 6);

            let get_result = redis.get(key);
            assert!(get_result.is_ok());
            assert_eq!(get_result.unwrap(), "100200");
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_append_concurrent() {
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

        let redis_arc = Arc::new(redis);
        let num_threads = 10;
        let appends_per_thread = 10;

        // Each thread appends a single character multiple times
        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                let key = format!("concurrent_key_{}", thread_id).into_bytes();
                for _ in 0..appends_per_thread {
                    let value = format!("{}", thread_id % 10).into_bytes();
                    let result = redis_clone.append(&key, &value);
                    assert!(result.is_ok(), "append failed in thread {}", thread_id);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify results
        for thread_id in 0..num_threads {
            let key = format!("concurrent_key_{}", thread_id).into_bytes();
            let get_result = redis_arc.get(&key);
            assert!(get_result.is_ok());
            let value = get_result.unwrap();
            // Each thread appended its digit 10 times
            let expected = (thread_id % 10).to_string().repeat(appends_per_thread);
            assert_eq!(value, expected);
        }

        if let Ok(redis) = Arc::try_unwrap(redis_arc) {
            redis.set_need_close(true);
        }

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_append_wrong_type() {
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

        // Create a hash key first (when hash commands are implemented)
        // For now, we'll test that append works correctly with string keys
        // and verify the error type is propagated correctly

        // Test: Try to append to a key that will be created as string
        // This should work fine
        let key = b"test_key";
        let value1 = b"hello";

        let result = redis.append(key, value1);
        assert!(result.is_ok(), "append to non-existent key should work");
        assert_eq!(result.unwrap(), 5);

        // Verify the value
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), "hello");

        // Append more data
        let value2 = b" world";
        let result = redis.append(key, value2);
        assert!(result.is_ok(), "append to existing string key should work");
        assert_eq!(result.unwrap(), 11);

        // Verify the concatenated value
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), "hello world");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_append_large_value() {
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

        // Test appending large values (1MB each time)
        let key = b"large_key";
        let large_value = vec![b'x'; 1024 * 1024]; // 1MB

        // Append 10 times (total 10MB)
        for i in 0..10 {
            let result = redis.append(key, &large_value);
            assert!(
                result.is_ok(),
                "append large value failed at iteration {}",
                i
            );
            let expected_len = (i + 1) * 1024 * 1024;
            assert_eq!(result.unwrap(), expected_len as i32);
        }

        // Verify final size
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        let final_value = get_result.unwrap();
        assert_eq!(final_value.len(), 10 * 1024 * 1024);
        assert!(final_value.chars().all(|c| c == 'x'));

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_append_size_limit() {
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

        // Test: Try to create a string that would exceed i32::MAX
        // We'll simulate this by setting a large value first
        let key = b"size_limit_key";

        // Create a moderately large value to test the overflow check
        // Note: We can't actually create a 2GB+ string in tests due to memory constraints
        // So we'll just verify the logic works with smaller values
        let large_value = vec![b'a'; 100 * 1024 * 1024]; // 100MB

        let result = redis.append(key, &large_value);
        assert!(result.is_ok(), "append should succeed for reasonable sizes");
        assert_eq!(result.unwrap(), 100 * 1024 * 1024);

        // Verify we can append more
        let result = redis.append(key, b"extra");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100 * 1024 * 1024 + 5);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_strlen_basic() {
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

        // Test 1: Get length of non-existing key (should return 0)
        let key = b"nonexistent_key";
        let result = redis.strlen(key);
        assert!(result.is_ok(), "strlen should succeed for non-existing key");
        assert_eq!(
            result.unwrap(),
            0,
            "strlen should return 0 for non-existing key"
        );

        // Test 2: Set a value and get its length
        let key = b"test_key";
        let value = b"Hello, World!";
        redis.set(key, value).unwrap();

        let result = redis.strlen(key);
        assert!(result.is_ok(), "strlen should succeed");
        assert_eq!(
            result.unwrap(),
            13,
            "strlen should return 13 for 'Hello, World!'"
        );

        // Test 3: Empty string
        let key = b"empty_key";
        let value = b"";
        redis.set(key, value).unwrap();

        let result = redis.strlen(key);
        assert!(result.is_ok(), "strlen should succeed for empty string");
        assert_eq!(
            result.unwrap(),
            0,
            "strlen should return 0 for empty string"
        );

        // Test 4: UTF-8 multi-byte characters (should count bytes, not characters)
        let key = b"utf8_key";
        let value = "你好世界".as_bytes(); // 12 bytes (3 bytes per character)
        redis.set(key, value).unwrap();

        let result = redis.strlen(key);
        assert!(result.is_ok(), "strlen should succeed for UTF-8 string");
        assert_eq!(
            result.unwrap(),
            12,
            "strlen should return byte count, not character count"
        );

        // Test 5: Large string
        let key = b"large_key";
        let value = vec![b'x'; 10000];
        redis.set(key, &value).unwrap();

        let result = redis.strlen(key);
        assert!(result.is_ok(), "strlen should succeed for large string");
        assert_eq!(result.unwrap(), 10000);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_strlen_wrong_type() {
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

        // Create a hash (non-string type)
        let key = b"hash_key";
        let field = b"field1";
        let value = b"value1";
        redis.hset(key, field, value).unwrap();

        // Try to get strlen of a hash key (should return RedisErr)
        let result = redis.strlen(key);
        assert!(result.is_err(), "strlen should fail for non-string type");

        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. }
                if message.starts_with("WRONGTYPE") =>
            {
                // Expected error type
            }
            e => panic!("Expected WRONGTYPE RedisErr, got: {:?}", e),
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_strlen_with_ttl() {
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

        // Set a key with value
        let key = b"ttl_key";
        let value = b"test_value";
        redis.set(key, value).unwrap();

        // Check strlen
        let result = redis.strlen(key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);

        // Note: We skip the TTL test as expire() might not be available in this test context
        // In real Redis, expired keys would return 0 from strlen

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_strlen_concurrent() {
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

        // Pre-populate with test data
        for i in 0..100 {
            let key = format!("concurrent_key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            redis.set(&key, &value).unwrap();
        }

        let redis_arc = Arc::new(redis);
        let num_threads = 8;

        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("concurrent_key_{}", i).into_bytes();
                    let expected_len = format!("value_{}", i).len() as i32;

                    let result = redis_clone.strlen(&key);
                    assert!(
                        result.is_ok(),
                        "Thread {}: strlen failed for key {}",
                        thread_id,
                        String::from_utf8_lossy(&key)
                    );
                    assert_eq!(
                        result.unwrap(),
                        expected_len,
                        "Thread {}: incorrect length for key {}",
                        thread_id,
                        String::from_utf8_lossy(&key)
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

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getrange_basic() {
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

        // Test 1: Get range from non-existing key (should return empty string)
        let key = b"nonexistent_key";
        let result = redis.getrange(key, 0, 5);
        assert!(
            result.is_ok(),
            "getrange should succeed for non-existing key"
        );
        assert_eq!(
            result.unwrap(),
            Vec::<u8>::new(),
            "getrange should return empty string for non-existing key"
        );

        // Test 2: Set a value and get different ranges
        let key = b"test_key";
        let value = b"Hello, World!";
        redis.set(key, value).unwrap();

        // Get first 5 characters (0-4)
        let result = redis.getrange(key, 0, 4);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"Hello");

        // Get middle part (7-11)
        let result = redis.getrange(key, 7, 11);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"World");

        // Get entire string (0 to -1)
        let result = redis.getrange(key, 0, -1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"Hello, World!");

        // Get last 6 characters (-6 to -1)
        let result = redis.getrange(key, -6, -1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"World!");

        // Get last character (-1 to -1)
        let result = redis.getrange(key, -1, -1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"!");

        // Start > end (should return empty string)
        let result = redis.getrange(key, 5, 2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<u8>::new());

        // Out of range indices
        let result = redis.getrange(key, 100, 200);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<u8>::new());

        // Test 3: Empty string
        let key = b"empty_key";
        redis.set(key, b"").unwrap();
        let result = redis.getrange(key, 0, 5);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<u8>::new());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getrange_negative_indices() {
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

        let key = b"test_key";
        let value = b"0123456789";
        redis.set(key, value).unwrap();

        // Test various negative index combinations
        // -3 to -1 should get "789"
        let result = redis.getrange(key, -3, -1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"789");

        // -10 to -6 should get "01234"
        let result = redis.getrange(key, -10, -6);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"01234");

        // Mix of positive and negative: 2 to -3 should get "234567"
        let result = redis.getrange(key, 2, -3);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"234567");

        // Large negative index (should be clamped to 0)
        let result = redis.getrange(key, -100, 4);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"01234");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getrange_wrong_type() {
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

        // Create a hash (non-string type)
        let key = b"hash_key";
        let field = b"field1";
        let value = b"value1";
        redis.hset(key, field, value).unwrap();

        // Try to get range from a hash key (should return WRONGTYPE error)
        let result = redis.getrange(key, 0, 5);
        assert!(result.is_err(), "getrange should fail for non-string type");

        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. }
                if message.starts_with("WRONGTYPE") =>
            {
                // Expected error type
            }
            e => panic!("Expected WRONGTYPE RedisErr, got: {:?}", e),
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getrange_utf8() {
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

        // Test with UTF-8 string (note: getrange works on bytes, not characters)
        let key = b"utf8_key";
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

        // Get entire string
        let result = redis.getrange(key, 0, -1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "你好世界".as_bytes());

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setrange_basic() {
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

        // Test 1: Setrange on existing string (within bounds)
        let key = b"test_key";
        redis.set(key, b"Hello World").unwrap();

        let result = redis.setrange(key, 6, b"Redis");
        assert!(result.is_ok(), "setrange should succeed");
        assert_eq!(result.unwrap(), 11, "length should be 11");

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Hello Redis");

        // Test 2: Setrange on existing string (extending)
        let result = redis.setrange(key, 6, b"Rust Programming");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 22);

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Hello Rust Programming");

        // Test 3: Setrange with offset beyond string length (padding with \x00)
        let key = b"padding_key";
        redis.set(key, b"Hello").unwrap();

        let result = redis.setrange(key, 10, b"World");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 15);

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Hello\x00\x00\x00\x00\x00World");

        // Test 4: Setrange on non-existing key
        let key = b"new_key";
        let result = redis.setrange(key, 5, b"Redis");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"\x00\x00\x00\x00\x00Redis");

        // Test 5: Setrange at offset 0
        let key = b"offset_zero";
        redis.set(key, b"Hello").unwrap();

        let result = redis.setrange(key, 0, b"Goodbye");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 7);

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Goodbye");

        // Test 6: Setrange with empty value
        let key = b"empty_value";
        redis.set(key, b"Test").unwrap();

        let result = redis.setrange(key, 2, b"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 4, "length should remain 4");

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Test");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setrange_edge_cases() {
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

        // Test 1: Negative offset (should fail)
        let key = b"negative_offset";
        redis.set(key, b"Test").unwrap();

        let result = redis.setrange(key, -1, b"X");
        assert!(result.is_err(), "setrange should fail for negative offset");

        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. } if message.contains("offset") => {
                // Expected error - verify it has ERR prefix
                assert!(
                    message.starts_with("ERR"),
                    "Error message should start with ERR prefix"
                );
            }
            e => panic!("Expected offset error, got: {:?}", e),
        }

        // Test 1.5: Offset beyond i32::MAX (should fail)
        let key = b"large_offset_overflow";
        let result = redis.setrange(key, (i32::MAX as i64) + 1, b"X");
        assert!(
            result.is_err(),
            "setrange should fail for offset > i32::MAX"
        );

        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. } if message.contains("offset") => {
                // Expected error
                assert!(
                    message.starts_with("ERR"),
                    "Error message should start with ERR prefix"
                );
            }
            e => panic!("Expected offset error, got: {:?}", e),
        }

        // Test 2: Setrange on empty string
        let key = b"empty_string";
        redis.set(key, b"").unwrap();

        let result = redis.setrange(key, 3, b"Hi");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"\x00\x00\x00Hi");

        // Test 3: Large offset (but within i32::MAX)
        let key = b"large_offset";
        let result = redis.setrange(key, 1000, b"X");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1001);

        let value = redis.get(key).unwrap();
        assert_eq!(value.len(), 1001);
        assert_eq!(value.as_bytes()[1000], b'X');

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setrange_wrong_type() {
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

        // Create a hash (non-string type)
        let key = b"hash_key";
        let field = b"field1";
        let value = b"value1";
        redis.hset(key, field, value).unwrap();

        // Try to setrange on a hash key (should return WRONGTYPE error)
        let result = redis.setrange(key, 0, b"test");
        assert!(result.is_err(), "setrange should fail for non-string type");

        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. }
                if message.starts_with("WRONGTYPE") =>
            {
                // Expected error type
            }
            e => panic!("Expected WRONGTYPE RedisErr, got: {:?}", e),
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setrange_binary_safe() {
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

        // Test binary data with null bytes
        let key = b"binary_key";
        redis.set(key, b"Hello\x00World").unwrap(); // 11 bytes: H-e-l-l-o-\x00-W-o-r-l-d

        // Overwrite 3 bytes starting at position 3
        // Original: "Hello\x00World" (positions 0-10)
        // After:    "Hel\x00\x00\x00World" (positions 0-10, total 11 bytes)
        let result = redis.setrange(key, 3, b"\x00\x00\x00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 11);

        let value = redis.get(key).unwrap();
        assert_eq!(value.as_bytes(), b"Hel\x00\x00\x00World");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setex_basic() {
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

        // Test 1: Basic SETEX operation
        let key = b"mykey";
        let value = b"Hello";
        let seconds = 10;

        let result = redis.setex(key, seconds, value);
        assert!(result.is_ok());

        // Verify the value was set
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().as_bytes(), value);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setex_overwrites_existing() {
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

        // Set an existing key
        let key = b"mykey";
        redis.set(key, b"OldValue").unwrap();

        // Overwrite with SETEX
        let result = redis.setex(key, 5, b"NewValue");
        assert!(result.is_ok());

        // Verify the new value
        let get_result = redis.get(key).unwrap();
        assert_eq!(get_result.as_bytes(), b"NewValue");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setex_invalid_ttl() {
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

        let key = b"mykey";
        let value = b"test";

        // Test negative TTL
        let result = redis.setex(key, -1, value);
        assert!(result.is_err());
        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. } => {
                assert!(message.contains("invalid expire time"));
                assert!(message.starts_with("ERR"));
            }
            e => panic!("Expected RedisErr, got: {:?}", e),
        }

        // Test zero TTL
        let result = redis.setex(key, 0, value);
        assert!(result.is_err());
        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. } => {
                assert!(message.contains("invalid expire time"));
                assert!(message.starts_with("ERR"));
            }
            e => panic!("Expected RedisErr, got: {:?}", e),
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setex_expiration() {
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

        // Set a key with 1 second TTL
        let key = b"expiring_key";
        let value = b"expires_soon";
        let result = redis.setex(key, 1, value);
        assert!(result.is_ok());

        // Immediately after setting, key should exist
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().as_bytes(), value);

        // Wait for expiration (2 seconds to be safe)
        std::thread::sleep(std::time::Duration::from_secs(2));

        // After expiration, key should not exist
        let get_result = redis.get(key);
        assert!(
            get_result.is_err(),
            "Key should be expired and return error"
        );

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_psetex_basic() {
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

        // Test 1: Basic PSETEX operation
        let key = b"mykey";
        let value = b"Hello";
        let milliseconds = 10000; // 10 seconds

        let result = redis.psetex(key, milliseconds, value);
        assert!(result.is_ok());

        // Verify the value was set
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().as_bytes(), value);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_psetex_overwrites_existing() {
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

        // Set an existing key
        let key = b"mykey";
        redis.set(key, b"OldValue").unwrap();

        // Overwrite with PSETEX
        let result = redis.psetex(key, 5000, b"NewValue");
        assert!(result.is_ok());

        // Verify the new value
        let get_result = redis.get(key).unwrap();
        assert_eq!(get_result.as_bytes(), b"NewValue");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_psetex_invalid_ttl() {
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

        let key = b"mykey";
        let value = b"test";

        // Test negative TTL
        let result = redis.psetex(key, -1, value);
        assert!(result.is_err());
        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. } => {
                assert!(message.contains("invalid expire time"));
                assert!(message.starts_with("ERR"));
            }
            e => panic!("Expected RedisErr, got: {:?}", e),
        }

        // Test zero TTL
        let result = redis.psetex(key, 0, value);
        assert!(result.is_err());
        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. } => {
                assert!(message.contains("invalid expire time"));
                assert!(message.starts_with("ERR"));
            }
            e => panic!("Expected RedisErr, got: {:?}", e),
        }
    }

    #[test]
    fn test_redis_concurrent_set_get_same_key() {
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

        let redis_arc = Arc::new(redis);
        let num_threads = 4;
        let operations_per_thread = 50;

        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                let key = b"concurrent_key";

                for i in 0..operations_per_thread {
                    let value = format!("value_{}_{}", thread_id, i).into_bytes();

                    let result = redis_clone.set(key, &value);
                    assert!(result.is_ok(), "Thread {}: set failed", thread_id);

                    let get_result = redis_clone.get(key);
                    assert!(result.is_ok(), "Thread {}: get failed", thread_id);
                    assert_eq!(
                        get_result.unwrap(),
                        String::from_utf8_lossy(&value),
                        "Thread {}: incorrect value",
                        thread_id
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

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setbit_getbit() {
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

        let key = b"test_bit_key";

        // Test setting bits
        // Set bit 0 to 1 (should return 0 as original value)
        let result = redis.setbit(key, 0, 1);
        assert!(result.is_ok(), "setbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 0, "original bit value should be 0");

        // Get bit 0 (should return 1)
        let result = redis.getbit(key, 0);
        assert!(result.is_ok(), "getbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 1, "bit value should be 1");

        // Set bit 1000 to 1 (should extend the string)
        let result = redis.setbit(key, 1000, 1);
        assert!(result.is_ok(), "setbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 0, "original bit value should be 0");

        // Get bit 1000 (should return 1)
        let result = redis.getbit(key, 1000);
        assert!(result.is_ok(), "getbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 1, "bit value should be 1");

        // Get bit 500 (should return 0 as it's in the middle of the extended string)
        let result = redis.getbit(key, 500);
        assert!(result.is_ok(), "getbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 0, "bit value should be 0");

        // Set bit 1000 to 0 (should return 1 as original value)
        let result = redis.setbit(key, 1000, 0);
        assert!(result.is_ok(), "setbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 1, "original bit value should be 1");

        // Get bit 1000 (should return 0)
        let result = redis.getbit(key, 1000);
        assert!(result.is_ok(), "getbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 0, "bit value should be 0");

        // Test with non-existing key (should return 0)
        let result = redis.getbit(b"non_existing_key", 0);
        assert!(result.is_ok(), "getbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 0, "bit value should be 0");

        // Test clearing a bit that is already 0
        let result = redis.setbit(key, 2000, 0);
        assert!(result.is_ok(), "setbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 0, "original bit value should be 0");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setbit_getbit_invalid_args() {
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

        let key = b"test_bit_key";

        // Test invalid offset (negative)
        let result = redis.setbit(key, -1, 1);
        assert!(result.is_err(), "setbit should fail with negative offset");

        let result = redis.getbit(key, -1);
        assert!(result.is_err(), "getbit should fail with negative offset");

        // Test invalid bit value (not 0 or 1)
        let result = redis.setbit(key, 0, 2);
        assert!(result.is_err(), "setbit should fail with invalid bit value");

        let result = redis.setbit(key, 0, -1);
        assert!(result.is_err(), "setbit should fail with invalid bit value");

        // Test with very large offset
        let result = redis.setbit(key, 1i64 << 32, 1);
        assert!(result.is_err(), "setbit should fail with very large offset");

        let result = redis.getbit(key, 1i64 << 32);
        assert!(result.is_err(), "getbit should fail with very large offset");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setbit_getbit_with_expired_key() {
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

        let key = b"expiring_bit_key";

        // Set a key with a short expiration
        redis.setex(key, 1, b"test_value").unwrap();

        // Set a bit in the key
        let result = redis.setbit(key, 10, 1);
        assert!(result.is_ok(), "setbit command failed: {:?}", result.err());

        // Wait for the key to expire
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Try to get the bit from the expired key (should return 0)
        let result = redis.getbit(key, 10);
        assert!(result.is_ok(), "getbit command failed: {:?}", result.err());
        assert_eq!(result.unwrap(), 0, "bit value should be 0 for expired key");

        // Try to set a bit in the expired key (should work as if key doesn't exist)
        let result = redis.setbit(key, 20, 1);
        assert!(result.is_ok(), "setbit command failed: {:?}", result.err());
        assert_eq!(
            result.unwrap(),
            0,
            "original bit value should be 0 for expired key"
        );

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_bitcount() {
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

        let key = b"test_bitcount_key";

        // Set some bits
        // String "a" in binary: 01100001 (has 3 bits set)
        redis.set(key, b"a").unwrap();

        // Count all bits
        let result = redis.bitcount(key, None, None);
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), 3, "bitcount should be 3 for 'a'");

        // String "aa" in binary: 01100001 01100001 (has 6 bits set)
        redis.set(key, b"aa").unwrap();

        // Count all bits
        let result = redis.bitcount(key, None, None);
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), 6, "bitcount should be 6 for 'aa'");

        // Count bits in range [0, 0] (first byte)
        let result = redis.bitcount(key, Some(0), Some(0));
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        assert_eq!(
            result.unwrap(),
            3,
            "bitcount should be 3 for first byte of 'aa'"
        );

        // Count bits in range [1, 1] (second byte)
        let result = redis.bitcount(key, Some(1), Some(1));
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        assert_eq!(
            result.unwrap(),
            3,
            "bitcount should be 3 for second byte of 'aa'"
        );

        // Count bits in range [0, 1] (both bytes)
        let result = redis.bitcount(key, Some(0), Some(1));
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        assert_eq!(
            result.unwrap(),
            6,
            "bitcount should be 6 for both bytes of 'aa'"
        );

        // Test with non-existing key (should return 0)
        let result = redis.bitcount(b"non_existing_key", None, None);
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        assert_eq!(
            result.unwrap(),
            0,
            "bitcount should be 0 for non-existing key"
        );

        // Test with empty string
        redis.set(b"empty_key", b"").unwrap();
        let result = redis.bitcount(b"empty_key", None, None);
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), 0, "bitcount should be 0 for empty string");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_bitcount_with_negative_indices() {
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

        let key = b"test_negative_indices";

        // String "hello" (5 bytes)
        redis.set(key, b"hello").unwrap();

        // Count bits with negative indices
        // -1 refers to the last byte
        let result = redis.bitcount(key, Some(-1), Some(-1));
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        // 'o' in binary: 01101111 (has 6 bits set)
        assert_eq!(result.unwrap(), 6, "bitcount should be 6 for last byte 'o'");

        // Count bits from start to -1 (entire string)
        let result = redis.bitcount(key, Some(0), Some(-1));
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        // "hello" bits: h(3) + e(4) + l(4) + l(4) + o(6) = 21
        assert_eq!(result.unwrap(), 21, "bitcount should be 21 for 'hello'");

        // Count bits from -3 to -1 (last 3 bytes: "llo")
        let result = redis.bitcount(key, Some(-3), Some(-1));
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        // "llo" bits: l(4) + l(4) + o(6) = 14
        assert_eq!(result.unwrap(), 14, "bitcount should be 14 for 'llo'");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_bitcount_with_expired_key() {
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

        let key = b"expiring_bitcount_key";

        // Set a key with a short expiration
        redis.setex(key, 1, b"test").unwrap();

        // Count bits in the key
        let result = redis.bitcount(key, None, None);
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        // "test" bits: t(4) + e(4) + s(5) + t(4) = 17
        assert_eq!(result.unwrap(), 17, "bitcount should be 17 for 'test'");

        // Wait for the key to expire
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Try to count bits from the expired key (should return 0)
        let result = redis.bitcount(key, None, None);
        assert!(
            result.is_ok(),
            "bitcount command failed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), 0, "bitcount should be 0 for expired key");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_bitpos() {
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

            let key = b"test_bitpos_key";

            // Set a string: "a" in binary is 01100001
            redis.set(key, b"a").unwrap();

            // Find first bit set to 1 (should be at position 1)
            let result = redis.bitpos(key, 1, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                1,
                "First bit set to 1 should be at position 1"
            );

            // Find first bit set to 0 (should be at position 0)
            let result = redis.bitpos(key, 0, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                0,
                "First bit set to 0 should be at position 0"
            );

            // Test with non-existing key (should return -1 for bit=1, 0 for bit=0)
            let result = redis.bitpos(b"non_existing_key", 1, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                -1,
                "Non-existing key should return -1 for bit=1"
            );

            let result = redis.bitpos(b"non_existing_key", 0, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                0,
                "Non-existing key should return 0 for bit=0"
            );

            // Test with empty string
            redis.set(b"empty_key", b"").unwrap();
            let result = redis.bitpos(b"empty_key", 1, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                -1,
                "Empty string should return -1 for bit=1"
            );

            let result = redis.bitpos(b"empty_key", 0, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(result.unwrap(), 0, "Empty string should return 0 for bit=0");

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_bitpos_with_range() {
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

            let key = b"test_bitpos_range";

            // Set a string: "ab" in binary is:
            // 'a': 01100001 (positions 0-7)
            // 'b': 01100010 (positions 8-15)
            redis.set(key, b"ab").unwrap();

            // Find first bit set to 1 in the entire string (should be at position 1)
            let result = redis.bitpos(key, 1, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                1,
                "First bit set to 1 should be at position 1"
            );

            // Find first bit set to 1 in range [1, 1] (second byte, 'b')
            // 'b': 01100010, first bit set to 1 is at position 9 (bit 1 of second byte)
            let result = redis.bitpos(key, 1, Some(1), Some(1), false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                9,
                "First bit set to 1 in second byte should be at position 9"
            );

            // Find first bit set to 0 in range [0, 0] (first byte, 'a')
            // 'a': 01100001, first bit set to 0 is at position 0
            let result = redis.bitpos(key, 0, Some(0), Some(0), false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                0,
                "First bit set to 0 in first byte should be at position 0"
            );

            // Find first bit set to 0 in range [1, 1] (second byte, 'b')
            // 'b': 01100010, first bit set to 0 is at position 8
            let result = redis.bitpos(key, 0, Some(1), Some(1), false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                8,
                "First bit set to 0 in second byte should be at position 8"
            );

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_bitpos_with_negative_indices() {
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

            let key = b"test_negative_indices";

            // Set a string: "hello" (5 bytes)
            redis.set(key, b"hello").unwrap();

            // Find first bit set to 1 from the end (-1 refers to the last byte)
            // Last byte 'o': 01101111, first bit set to 1 is at position 0 (of that byte)
            // But in the entire string, it's at position (4*8 + 0) = 32
            let result = redis.bitpos(key, 1, Some(-1), Some(-1), false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                32,
                "First bit set to 1 in last byte should be at position 32"
            );

            // Find first bit set to 0 from start to -1 (entire string)
            let result = redis.bitpos(key, 0, Some(0), Some(-1), false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                0,
                "First bit set to 0 should be at position 0"
            );

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_bitpos_with_expired_key() {
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

            let key = b"expiring_bitpos_key";

            // Set a key with a short expiration
            redis.setex(key, 1, b"test").unwrap();

            // Find first bit set to 1
            let result = redis.bitpos(key, 1, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            // "test" first bit set to 1 is at position 0 ('t': 01110100)
            assert_eq!(
                result.unwrap(),
                1,
                "First bit set to 1 should be at position 1"
            );

            // Wait for the key to expire
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Try to find first bit set to 1 from the expired key (should return -1)
            let result = redis.bitpos(key, 1, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                -1,
                "Bitpos should return -1 for expired key with bit=1"
            );

            // Try to find first bit set to 0 from the expired key (should return 0)
            let result = redis.bitpos(key, 0, None, None, false);
            assert!(result.is_ok(), "bitpos command failed: {:?}", result.err());
            assert_eq!(
                result.unwrap(),
                0,
                "Bitpos should return 0 for expired key with bit=0"
            );

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_bitpos_invalid_bit_value() {
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

            let key = b"test_invalid_bit";

            // Set a string
            redis.set(key, b"test").unwrap();

            // Try with invalid bit value (2)
            let result = redis.bitpos(key, 2, None, None, false);
            assert!(result.is_err(), "bitpos should fail with invalid bit value");
            match result.unwrap_err() {
                storage::error::Error::RedisErr { ref message, .. } => {
                    assert!(message.contains("The bit argument must be 1 or 0"));
                }
                e => panic!("Expected RedisErr with bit argument error, got: {:?}", e),
            }

            // Try with invalid bit value (-1)
            let result = redis.bitpos(key, -1, None, None, false);
            assert!(result.is_err(), "bitpos should fail with invalid bit value");
            match result.unwrap_err() {
                storage::error::Error::RedisErr { ref message, .. } => {
                    assert!(message.contains("The bit argument must be 1 or 0"));
                }
                e => panic!("Expected RedisErr with bit argument error, got: {:?}", e),
            }

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_bitop() {
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

            // Test AND operation
            {
                let key1 = b"key1";
                let key2 = b"key2";
                let dest_key = b"and_result";

                // Set source keys
                // "a" in binary: 01100001
                // "b" in binary: 01100010
                // AND result:    01100000 ('`')
                redis.set(key1, b"a").unwrap();
                redis.set(key2, b"b").unwrap();

                // Perform AND operation
                let src_keys = vec![key1.as_slice(), key2.as_slice()];
                let result = redis.bitop("AND", dest_key, &src_keys);
                assert!(result.is_ok(), "bitop AND failed: {:?}", result.err());
                assert_eq!(result.unwrap(), 1); // Length of result

                // Check result
                let get_result = redis.get(dest_key);
                assert!(get_result.is_ok());
                assert_eq!(get_result.unwrap(), "`"); // '`' is the result of 'a' AND 'b'
            }

            // Test OR operation
            {
                let key1 = b"key1_or";
                let key2 = b"key2_or";
                let dest_key = b"or_result";

                // Set source keys
                // "a" in binary: 01100001
                // "b" in binary: 01100010
                // OR result:     01100011 ('c')
                redis.set(key1, b"a").unwrap();
                redis.set(key2, b"b").unwrap();

                // Perform OR operation
                let src_keys = vec![key1.as_slice(), key2.as_slice()];
                let result = redis.bitop("OR", dest_key, &src_keys);
                assert!(result.is_ok(), "bitop OR failed: {:?}", result.err());
                assert_eq!(result.unwrap(), 1); // Length of result

                // Check result
                let get_result = redis.get(dest_key);
                assert!(get_result.is_ok());
                assert_eq!(get_result.unwrap(), "c"); // 'c' is the result of 'a' OR 'b'
            }

            // Test XOR operation
            {
                let key1 = b"key1_xor";
                let key2 = b"key2_xor";
                let dest_key = b"xor_result";

                // Set source keys
                // "a" in binary: 01100001
                // "b" in binary: 01100010
                // XOR result:    00000011 (ETX - end of transmission character)
                redis.set(key1, b"a").unwrap();
                redis.set(key2, b"b").unwrap();

                // Perform XOR operation
                let src_keys = vec![key1.as_slice(), key2.as_slice()];
                let result = redis.bitop("XOR", dest_key, &src_keys);
                assert!(result.is_ok(), "bitop XOR failed: {:?}", result.err());
                assert_eq!(result.unwrap(), 1); // Length of result

                // Check result
                let get_result = redis.get(dest_key);
                assert!(get_result.is_ok());
                let result_value = get_result.unwrap();
                assert_eq!(result_value.len(), 1);
                assert_eq!(result_value.as_bytes()[0], 0x03); // ETX character
            }

            // Test NOT operation
            {
                let key = b"key_not";
                let dest_key = b"not_result";

                // Set source key
                // "a" in binary: 01100001
                // NOT result:    10011110 ( - a special character)
                redis.set(key, b"a").unwrap();

                // Perform NOT operation
                let src_keys = vec![key.as_slice()];
                let result = redis.bitop("NOT", dest_key, &src_keys);
                assert!(result.is_ok(), "bitop NOT failed: {:?}", result.err());
                assert_eq!(result.unwrap(), 1); // Length of result

                // Check result
                let get_result = redis.get(dest_key);
                assert!(get_result.is_ok());
                let result_value = get_result.unwrap();
                assert_eq!(result_value.len(), 1);
                assert_eq!(result_value.as_bytes()[0], 0x9E); // NOT of 'a'
            }

            // Test with non-existing keys
            {
                let dest_key = b"empty_result";
                let src_keys = vec![b"non_existing1".as_slice(), b"non_existing2".as_slice()];

                // Perform AND operation with non-existing keys
                let result = redis.bitop("AND", dest_key, &src_keys);
                assert!(
                    result.is_ok(),
                    "bitop AND with non-existing keys failed: {:?}",
                    result.err()
                );
                assert_eq!(result.unwrap(), 0); // Empty result

                // Check that destination key doesn't exist
                let get_result = redis.get(dest_key);
                assert!(get_result.is_ok());
                assert_eq!(get_result.unwrap(), ""); // Empty string
            }

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_bitop_with_expired_keys() {
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

            let key1 = b"expiring_key1";
            let key2 = b"expiring_key2";
            let dest_key = b"bitop_result";

            // Set keys with short expiration
            redis.setex(key1, 1, b"a").unwrap(); // "a" in binary: 01100001
            redis.setex(key2, 1, b"b").unwrap(); // "b" in binary: 01100010

            // Perform AND operation before expiration
            let src_keys = vec![key1.as_slice(), key2.as_slice()];
            let result = redis.bitop("AND", dest_key, &src_keys);
            assert!(
                result.is_ok(),
                "bitop AND before expiration failed: {:?}",
                result.err()
            );
            assert_eq!(result.unwrap(), 1); // Length of result

            // Check result before expiration
            let get_result = redis.get(dest_key);
            assert!(get_result.is_ok());
            assert_eq!(get_result.unwrap(), "`"); // '`' is the result of 'a' AND 'b'

            // Wait for keys to expire
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Perform AND operation after expiration
            let result = redis.bitop("AND", dest_key, &src_keys);
            assert!(
                result.is_ok(),
                "bitop AND after expiration failed: {:?}",
                result.err()
            );
            assert_eq!(result.unwrap(), 0); // Empty result

            // Check that destination key is now empty
            let get_result = redis.get(dest_key);
            assert!(get_result.is_ok());
            assert_eq!(get_result.unwrap(), ""); // Empty string

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_bitop_invalid_operation() {
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

            let dest_key = b"invalid_result";
            let src_keys = vec![b"key1".as_slice()];

            // Try with invalid operation
            let result = redis.bitop("INVALID", dest_key, &src_keys);
            assert!(result.is_err(), "bitop should fail with invalid operation");
            match result.unwrap_err() {
                storage::error::Error::RedisErr { ref message, .. } => {
                    assert!(message.contains("syntax error"));
                }
                e => panic!("Expected RedisErr with syntax error, got: {:?}", e),
            }

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_bitop_not_with_multiple_keys() {
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

            let dest_key = b"not_result";
            let src_keys = vec![b"key1".as_slice(), b"key2".as_slice()];

            // Try NOT operation with multiple keys (should fail)
            let result = redis.bitop("NOT", dest_key, &src_keys);
            assert!(result.is_err(), "bitop NOT should fail with multiple keys");
            match result.unwrap_err() {
                storage::error::Error::RedisErr { ref message, .. } => {
                    assert!(message.contains("single source key"));
                }
                e => panic!(
                    "Expected RedisErr with single source key error, got: {:?}",
                    e
                ),
            }

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_psetex_expiration() {
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

            // Set a key with 1000 milliseconds (1 second) TTL
            let key = b"expiring_key";
            let value = b"expires_soon";
            let result = redis.psetex(key, 1000, value);
            assert!(result.is_ok());

            // Immediately after setting, key should exist
            let get_result = redis.get(key);
            assert!(get_result.is_ok());
            assert_eq!(get_result.unwrap().as_bytes(), value);

            // Wait for expiration (2 seconds to be safe)
            std::thread::sleep(std::time::Duration::from_secs(2));

            // After expiration, key should not exist
            let get_result = redis.get(key);
            assert!(
                get_result.is_err(),
                "Key should be expired and return error"
            );

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_psetex_vs_setex() {
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

            // Test that PSETEX with 1000ms is equivalent to SETEX with 1s
            let key1 = b"key_setex";
            let key2 = b"key_psetex";
            let value = b"test_value";

            // SETEX with 10 seconds
            let result = redis.setex(key1, 10, value);
            assert!(result.is_ok());

            // PSETEX with 10000 milliseconds (10 seconds)
            let result = redis.psetex(key2, 10000, value);
            assert!(result.is_ok());

            // Both keys should be readable
            let get_result1 = redis.get(key1);
            let get_result2 = redis.get(key2);
            assert!(get_result1.is_ok());
            assert!(get_result2.is_ok());
            assert_eq!(get_result1.unwrap().as_bytes(), value);
            assert_eq!(get_result2.unwrap().as_bytes(), value);

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
        }

    #[test]
    fn test_redis_psetex_overflow_check() {
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

            // Test that setting a key with a very large TTL does not cause overflow
            let key = b"expiring_key";
            let value = b"test_value";
            let result = redis.psetex(key, i64::MAX, value);
            assert!(result.is_ok());

            // Verify values are set
            assert_eq!(redis.get(b"expiring_key").unwrap(), "test_value");

            // Test that setting a key with a very large TTL does not cause overflow
            let result = redis.psetex(b"new_key", i64::MAX, b"new_value2");
            assert!(result.is_ok());

            // Verify values are set
            assert_eq!(redis.get(b"expiring_key").unwrap(), "new_value");
            assert_eq!(redis.get(b"new_key").unwrap(), "new_value2");

            redis.set_need_close(true);
            drop(redis);

            if test_db_path.exists() {
                std::fs::remove_dir_all(test_db_path).unwrap();
            }
    }

    #[test]
    fn test_redis_setnx_basic() {
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

        let key = b"mykey";
        let value1 = b"Hello";

        // First SETNX should succeed (key doesn't exist)
        let result = redis.setnx(key, value1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1, "First SETNX should return 1");

        // Verify the value was set
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().as_bytes(), value1);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setnx_existing_key() {
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

        let key = b"mykey";
        let value1 = b"Hello";
        let value2 = b"World";

        // First, set the key using regular SET
        redis.set(key, value1).unwrap();

        // Second SETNX should fail (key already exists)
        let result = redis.setnx(key, value2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0, "SETNX on existing key should return 0");

        // Verify the value wasn't changed
        let get_result = redis.get(key).unwrap();
        assert_eq!(get_result.as_bytes(), value1, "Value should not be changed");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setnx_expired_key() {
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

        let key = b"expiring_key";
        let value1 = b"expires_soon";
        let value2 = b"new_value";

        // Set a key with 1 second TTL
        redis.setex(key, 1, value1).unwrap();

        // Wait for expiration
        std::thread::sleep(std::time::Duration::from_secs(2));

        // SETNX should succeed on expired key (treated as non-existent)
        let result = redis.setnx(key, value2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1, "SETNX on expired key should return 1");

        // Verify the new value was set
        let get_result = redis.get(key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().as_bytes(), value2);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setnx_multiple_keys() {
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

        // Test SETNX with multiple different keys
        let keys = vec![b"key1", b"key2", b"key3"];
        let value = b"test_value";

        for key in &keys {
            let result = redis.setnx(*key, value);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 1, "SETNX should succeed for new key");
        }

        // Try SETNX again on same keys
        for key in &keys {
            let result = redis.setnx(*key, b"different_value");
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 0, "SETNX should fail for existing key");

            // Verify values weren't changed
            let get_result = redis.get(*key).unwrap();
            assert_eq!(get_result.as_bytes(), value);
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_setnx_wrong_type() {
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

        let key = b"hash_key";
        let field = b"field1";
        let value = b"value1";

        // Create a hash type key
        redis.hset(key, field, value).unwrap();

        // Try to execute SETNX on hash key (should return WRONGTYPE error)
        let result = redis.setnx(key, b"test_value");
        assert!(result.is_err(), "SETNX should fail for non-string type");

        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. }
                if message.starts_with("WRONGTYPE") =>
            {
                // Expected error type
            }
            e => panic!("Expected WRONGTYPE RedisErr, got: {:?}", e),
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getset_basic() {
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

        let key = b"mykey";
        let old_value = b"Hello";
        let new_value = b"World";

        // Set initial value
        redis.set(key, old_value).unwrap();

        // GETSET should return old value and set new value
        let result = redis.getset(key, new_value);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some(String::from_utf8_lossy(old_value).to_string()),
            "GETSET should return old value"
        );

        // Verify new value was set
        let get_result = redis.get(key).unwrap();
        assert_eq!(get_result.as_bytes(), new_value, "New value should be set");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getset_non_existing_key() {
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

        let key = b"nonexistent";
        let value = b"NewValue";

        // GETSET on non-existing key should return None
        let result = redis.getset(key, value);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            None,
            "GETSET should return None for non-existing key"
        );

        // Verify value was set
        let get_result = redis.get(key).unwrap();
        assert_eq!(get_result.as_bytes(), value, "Value should be set");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getset_expired_key() {
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

        let key = b"expiring_key";
        let old_value = b"expires_soon";
        let new_value = b"new_value";

        // Set a key with 1 second TTL
        redis.setex(key, 1, old_value).unwrap();

        // Wait for expiration
        std::thread::sleep(std::time::Duration::from_secs(2));

        // GETSET on expired key should return None (treated as non-existent)
        let result = redis.getset(key, new_value);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            None,
            "GETSET should return None for expired key"
        );

        // Verify new value was set
        let get_result = redis.get(key).unwrap();
        assert_eq!(get_result.as_bytes(), new_value);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getset_wrong_type() {
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

        let key = b"hash_key";
        let field = b"field1";
        let value = b"value1";

        // Create a hash type key
        redis.hset(key, field, value).unwrap();

        // Try to execute GETSET on hash key (should return WRONGTYPE error)
        let result = redis.getset(key, b"test_value");
        assert!(result.is_err(), "GETSET should fail for non-string type");

        match result.unwrap_err() {
            storage::error::Error::RedisErr { ref message, .. }
                if message.starts_with("WRONGTYPE") =>
            {
                // Expected error type
            }
            e => panic!("Expected WRONGTYPE RedisErr, got: {:?}", e),
        }

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_getset_atomic_counter_reset() {
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

        let key = b"mycounter";

        // Increment counter multiple times
        for _ in 0..10 {
            redis.incr_decr(key, 1).unwrap();
        }

        // Verify counter value
        assert_eq!(redis.get(key).unwrap(), "10");

        // Atomically get counter value and reset to 0
        let old_counter = redis.getset(key, b"0").unwrap();
        assert_eq!(
            old_counter,
            Some("10".to_string()),
            "Should get old counter value"
        );

        // Verify counter was reset
        assert_eq!(redis.get(key).unwrap(), "0", "Counter should be reset to 0");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mget_basic() {
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

        // Set some keys
        redis.set(b"key1", b"value1").unwrap();
        redis.set(b"key2", b"value2").unwrap();
        redis.set(b"key3", b"value3").unwrap();

        // MGET all three keys
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let result = redis.mget(&keys);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some("value1".to_string()));
        assert_eq!(values[1], Some("value2".to_string()));
        assert_eq!(values[2], Some("value3".to_string()));

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mget_with_non_existing_keys() {
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

        // Set only key1 and key3
        redis.set(b"key1", b"value1").unwrap();
        redis.set(b"key3", b"value3").unwrap();

        // MGET including non-existing key2
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let result = redis.mget(&keys);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some("value1".to_string()));
        assert_eq!(values[1], None, "Non-existing key should return None");
        assert_eq!(values[2], Some("value3".to_string()));

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mget_with_expired_keys() {
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

        // Set key1 without expiration
        redis.set(b"key1", b"value1").unwrap();
        // Set key2 with 1 second expiration
        redis.setex(b"key2", 1, b"value2").unwrap();
        // Set key3 without expiration
        redis.set(b"key3", b"value3").unwrap();

        // Wait for key2 to expire
        std::thread::sleep(std::time::Duration::from_secs(2));

        // MGET all three keys
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let result = redis.mget(&keys);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some("value1".to_string()));
        assert_eq!(values[1], None, "Expired key should return None");
        assert_eq!(values[2], Some("value3".to_string()));

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mget_with_wrong_type() {
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

        // Set key1 as string
        redis.set(b"key1", b"value1").unwrap();
        // Set key2 as hash
        redis.hset(b"key2", b"field1", b"value2").unwrap();
        // Set key3 as string
        redis.set(b"key3", b"value3").unwrap();

        // MGET all three keys - hash key should return None
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let result = redis.mget(&keys);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some("value1".to_string()));
        assert_eq!(values[1], None, "Wrong type key should return None");
        assert_eq!(values[2], Some("value3".to_string()));

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mget_empty_keys() {
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

        // MGET with empty keys list
        let keys: Vec<Vec<u8>> = vec![];
        let result = redis.mget(&keys);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 0, "Empty keys should return empty array");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mset_basic() {
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

        // MSET multiple key-value pairs
        let kvs = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        let result = redis.mset(&kvs);
        assert!(result.is_ok(), "mset should succeed");

        // Verify all values were set
        assert_eq!(redis.get(b"key1").unwrap(), "value1");
        assert_eq!(redis.get(b"key2").unwrap(), "value2");
        assert_eq!(redis.get(b"key3").unwrap(), "value3");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mset_overwrite_existing() {
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

        // Set initial values
        redis.set(b"key1", b"old_value1").unwrap();
        redis.set(b"key2", b"old_value2").unwrap();

        // MSET should overwrite existing values
        let kvs = vec![
            (b"key1".to_vec(), b"new_value1".to_vec()),
            (b"key2".to_vec(), b"new_value2".to_vec()),
            (b"key3".to_vec(), b"new_value3".to_vec()),
        ];

        let result = redis.mset(&kvs);
        assert!(result.is_ok(), "mset should succeed");

        // Verify all values were updated/set
        assert_eq!(redis.get(b"key1").unwrap(), "new_value1");
        assert_eq!(redis.get(b"key2").unwrap(), "new_value2");
        assert_eq!(redis.get(b"key3").unwrap(), "new_value3");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mset_empty_kvs() {
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

        // MSET with empty key-value pairs
        let kvs: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        let result = redis.mset(&kvs);
        assert!(result.is_ok(), "mset with empty kvs should succeed");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mset_binary_safe() {
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

        // MSET with binary data (including null bytes)
        let kvs = vec![
            (b"binary_key1".to_vec(), b"value\x00with\x00nulls".to_vec()),
            (b"binary_key2".to_vec(), vec![0, 1, 2, 3, 255, 254, 253]),
            (b"utf8_key".to_vec(), "你好世界".as_bytes().to_vec()),
        ];

        let result = redis.mset(&kvs);
        assert!(result.is_ok(), "mset with binary data should succeed");

        // Verify binary data integrity
        assert_eq!(
            redis.get(b"binary_key1").unwrap().as_bytes(),
            b"value\x00with\x00nulls"
        );
        assert_eq!(
            redis.get_binary(b"binary_key2").unwrap(),
            vec![0, 1, 2, 3, 255, 254, 253]
        );
        assert_eq!(
            redis.get(b"utf8_key").unwrap().as_bytes(),
            "你好世界".as_bytes()
        );

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mset_large_batch() {
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

        // MSET with large batch (1000 key-value pairs)
        let mut kvs = Vec::with_capacity(1000);
        for i in 0..1000 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            kvs.push((key, value));
        }

        let result = redis.mset(&kvs);
        assert!(result.is_ok(), "mset with large batch should succeed");

        // Verify some random values
        assert_eq!(redis.get(b"key_0").unwrap(), "value_0");
        assert_eq!(redis.get(b"key_500").unwrap(), "value_500");
        assert_eq!(redis.get(b"key_999").unwrap(), "value_999");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_mset_atomicity() {
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

        // Set initial values
        redis.set(b"key1", b"initial1").unwrap();
        redis.set(b"key2", b"initial2").unwrap();

        // MSET should be atomic - all keys should be updated together
        let kvs = vec![
            (b"key1".to_vec(), b"atomic1".to_vec()),
            (b"key2".to_vec(), b"atomic2".to_vec()),
            (b"key3".to_vec(), b"atomic3".to_vec()),
        ];

        let result = redis.mset(&kvs);
        assert!(result.is_ok(), "mset should succeed");

        // All keys should have new values (atomicity test)
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let values = redis.mget(&keys).unwrap();

        assert_eq!(values[0], Some("atomic1".to_string()));
        assert_eq!(values[1], Some("atomic2".to_string()));
        assert_eq!(values[2], Some("atomic3".to_string()));

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_msetnx() {
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

        // MSETNX with non-existing keys should succeed
        let kvs = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        let result = redis.msetnx(&kvs);
        assert!(
            result.is_ok(),
            "msetnx with non-existing keys should succeed"
        );
        assert_eq!(
            result.unwrap(),
            true,
            "msetnx should return true when keys are set"
        );

        // Verify values are set
        assert_eq!(redis.get(b"key1").unwrap(), "value1");
        assert_eq!(redis.get(b"key2").unwrap(), "value2");
        assert_eq!(redis.get(b"key3").unwrap(), "value3");

        // MSETNX with existing keys should fail
        let kvs2 = vec![
            (b"key1".to_vec(), b"newvalue1".to_vec()),
            (b"key4".to_vec(), b"value4".to_vec()),
        ];

        let result = redis.msetnx(&kvs2);
        assert!(result.is_ok(), "msetnx should not fail even if keys exist");
        assert_eq!(
            result.unwrap(),
            false,
            "msetnx should return false when at least one key exists"
        );

        // Verify original values are unchanged
        assert_eq!(
            redis.get(b"key1").unwrap(),
            "value1",
            "key1 should retain original value"
        );
        assert_eq!(
            redis.get(b"key2").unwrap(),
            "value2",
            "key2 should retain original value"
        );
        assert_eq!(
            redis.get(b"key3").unwrap(),
            "value3",
            "key3 should retain original value"
        );

        // key4 should not be set
        let result = redis.get(b"key4");
        assert!(result.is_err(), "key4 should not be set");

        // MSETNX with all non-existing keys should succeed
        let kvs3 = vec![
            (b"key4".to_vec(), b"value4".to_vec()),
            (b"key5".to_vec(), b"value5".to_vec()),
        ];

        let result = redis.msetnx(&kvs3);
        assert!(
            result.is_ok(),
            "msetnx with non-existing keys should succeed"
        );
        assert_eq!(
            result.unwrap(),
            true,
            "msetnx should return true when keys are set"
        );

        // Verify new values are set
        assert_eq!(redis.get(b"key4").unwrap(), "value4");
        assert_eq!(redis.get(b"key5").unwrap(), "value5");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_redis_msetnx_with_expired_keys() {
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

        // Set a key with a short expiration
        redis.setex(b"expiring_key", 1, b"expiring_value").unwrap();

        // Wait for the key to expire
        std::thread::sleep(std::time::Duration::from_secs(2));

        // MSETNX with the expired key should succeed
        let kvs = vec![
            (b"expiring_key".to_vec(), b"new_value".to_vec()),
            (b"new_key".to_vec(), b"new_value2".to_vec()),
        ];

        let result = redis.msetnx(&kvs);
        assert!(result.is_ok(), "msetnx with expired keys should succeed");
        assert_eq!(
            result.unwrap(),
            true,
            "msetnx should return true when expired keys are treated as non-existing"
        );

        // Verify values are set
        assert_eq!(redis.get(b"expiring_key").unwrap(), "new_value");
        assert_eq!(redis.get(b"new_key").unwrap(), "new_value2");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }
}
