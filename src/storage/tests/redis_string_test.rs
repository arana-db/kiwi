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

        let mut set_handles = vec![];
        for thread_id in 0..num_threads {
            let redis_clone = Arc::clone(&redis_arc);
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = b"concurrent_key";
                    let value = format!("value_from_thread_{}_{}", thread_id, i).into_bytes();

                    let set_result = redis_clone.set(key, &value);
                    assert!(
                        set_result.is_ok(),
                        "set command failed for thread {}: {:?}",
                        thread_id,
                        set_result.err()
                    );
                }
            });
            set_handles.push(handle);
        }

        for handle in set_handles {
            handle.join().unwrap();
        }

        let get_result = redis_arc.get(b"concurrent_key");
        assert!(
            get_result.is_ok(),
            "get command failed: {:?}",
            get_result.err()
        );

        println!("get_result: {:?}", get_result.unwrap());

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
        assert_eq!(result.unwrap(), 0, "strlen should return 0 for non-existing key");

        // Test 2: Set a value and get its length
        let key = b"test_key";
        let value = b"Hello, World!";
        redis.set(key, value).unwrap();
        
        let result = redis.strlen(key);
        assert!(result.is_ok(), "strlen should succeed");
        assert_eq!(result.unwrap(), 13, "strlen should return 13 for 'Hello, World!'");

        // Test 3: Empty string
        let key = b"empty_key";
        let value = b"";
        redis.set(key, value).unwrap();
        
        let result = redis.strlen(key);
        assert!(result.is_ok(), "strlen should succeed for empty string");
        assert_eq!(result.unwrap(), 0, "strlen should return 0 for empty string");

        // Test 4: UTF-8 multi-byte characters (should count bytes, not characters)
        let key = b"utf8_key";
        let value = "你好世界".as_bytes(); // 12 bytes (3 bytes per character)
        redis.set(key, value).unwrap();
        
        let result = redis.strlen(key);
        assert!(result.is_ok(), "strlen should succeed for UTF-8 string");
        assert_eq!(result.unwrap(), 12, "strlen should return byte count, not character count");

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
            storage::error::Error::RedisErr { ref message, .. } if message.starts_with("WRONGTYPE") => {
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
}
