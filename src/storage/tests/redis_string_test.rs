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
    use storage::{BgTaskHandler, BitOpType, Redis, StorageOptions, unique_test_db_path};

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
    fn test_setbit_getbit_basic() {
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

        let key = b"bitkey";

        // Test SETBIT - set bit at offset 7 to 1
        let old_value = redis.setbit(key, 7, 1);
        assert!(old_value.is_ok(), "setbit failed: {:?}", old_value.err());
        assert_eq!(old_value.unwrap(), 0, "old bit value should be 0");

        // Test GETBIT - get bit at offset 7
        let bit_value = redis.getbit(key, 7);
        assert!(bit_value.is_ok(), "getbit failed: {:?}", bit_value.err());
        assert_eq!(bit_value.unwrap(), 1, "bit value should be 1");

        // Test SETBIT - set same bit again (should return 1)
        let old_value = redis.setbit(key, 7, 1);
        assert_eq!(old_value.unwrap(), 1, "old bit value should be 1");

        // Test SETBIT - clear bit
        let old_value = redis.setbit(key, 7, 0);
        assert_eq!(old_value.unwrap(), 1, "old bit value should be 1");

        let bit_value = redis.getbit(key, 7);
        assert_eq!(bit_value.unwrap(), 0, "bit value should be 0");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_setbit_getbit_multiple_offsets() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"multibit";

        // Set multiple bits
        let offsets = vec![0, 7, 15, 100, 1000];
        for &offset in &offsets {
            let result = redis.setbit(key, offset, 1);
            assert!(result.is_ok(), "setbit at offset {} failed", offset);
            assert_eq!(result.unwrap(), 0);
        }

        // Verify all bits are set
        for &offset in &offsets {
            let result = redis.getbit(key, offset);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 1, "bit at offset {} should be 1", offset);
        }

        // Verify unset bits are 0
        let result = redis.getbit(key, 50);
        assert_eq!(result.unwrap(), 0);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitcount_basic() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"bitcountkey";

        // Set some bits
        redis.setbit(key, 0, 1).unwrap();
        redis.setbit(key, 1, 1).unwrap();
        redis.setbit(key, 7, 1).unwrap();
        redis.setbit(key, 15, 1).unwrap();

        // Count all bits
        let count = redis.bitcount(key, None, None);
        assert!(count.is_ok());
        assert_eq!(count.unwrap(), 4, "should have 4 bits set");

        // Count with range
        let count = redis.bitcount(key, Some(0), Some(0));
        assert!(count.is_ok());
        assert_eq!(count.unwrap(), 3, "first byte should have 3 bits set");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitcount_with_range() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"rangekey";

        // Set bits across multiple bytes
        for i in 0..24 {
            redis.setbit(key, i, 1).unwrap();
        }

        // Count entire range
        let count = redis.bitcount(key, None, None);
        assert_eq!(count.unwrap(), 24);

        // Count first byte only
        let count = redis.bitcount(key, Some(0), Some(0));
        assert_eq!(count.unwrap(), 8);

        // Count with negative offsets
        let count = redis.bitcount(key, Some(-2), Some(-1));
        assert_eq!(count.unwrap(), 16);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitop_and() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key1 = b"key1";
        let key2 = b"key2";
        let dest = b"dest";

        // Set bits in key1: 11110000
        for i in 0..4 {
            redis.setbit(key1, i, 1).unwrap();
        }

        // Set bits in key2: 11001100
        redis.setbit(key2, 0, 1).unwrap();
        redis.setbit(key2, 1, 1).unwrap();
        redis.setbit(key2, 4, 1).unwrap();
        redis.setbit(key2, 5, 1).unwrap();

        // Perform AND operation
        let result = redis.bitop(BitOpType::And, dest, &[key1, key2]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1, "result length should be 1 byte");

        // Verify result: 11000000
        assert_eq!(redis.getbit(dest, 0).unwrap(), 1);
        assert_eq!(redis.getbit(dest, 1).unwrap(), 1);
        assert_eq!(redis.getbit(dest, 2).unwrap(), 0);
        assert_eq!(redis.getbit(dest, 3).unwrap(), 0);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitop_or_xor() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key1 = b"key1";
        let key2 = b"key2";
        let dest_or = b"dest_or";
        let dest_xor = b"dest_xor";

        // Set bits
        redis.setbit(key1, 0, 1).unwrap();
        redis.setbit(key1, 2, 1).unwrap();
        redis.setbit(key2, 1, 1).unwrap();
        redis.setbit(key2, 2, 1).unwrap();

        // Test OR
        let result = redis.bitop(BitOpType::Or, dest_or, &[key1, key2]);
        assert_eq!(result.unwrap(), 1);
        assert_eq!(redis.getbit(dest_or, 0).unwrap(), 1);
        assert_eq!(redis.getbit(dest_or, 1).unwrap(), 1);
        assert_eq!(redis.getbit(dest_or, 2).unwrap(), 1);

        // Test XOR
        let result = redis.bitop(BitOpType::Xor, dest_xor, &[key1, key2]);
        assert_eq!(result.unwrap(), 1);
        assert_eq!(redis.getbit(dest_xor, 0).unwrap(), 1);
        assert_eq!(redis.getbit(dest_xor, 1).unwrap(), 1);
        assert_eq!(redis.getbit(dest_xor, 2).unwrap(), 0); // XOR of 1 and 1 is 0  

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitop_not() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"key";
        let dest = b"dest";

        // Set some bits: 10101010
        redis.setbit(key, 0, 1).unwrap();
        redis.setbit(key, 2, 1).unwrap();
        redis.setbit(key, 4, 1).unwrap();
        redis.setbit(key, 6, 1).unwrap();

        // Perform NOT operation
        let result = redis.bitop(BitOpType::Not, dest, &[key]);
        assert!(result.is_ok());

        // Verify inverted bits: 01010101
        assert_eq!(redis.getbit(dest, 0).unwrap(), 0);
        assert_eq!(redis.getbit(dest, 1).unwrap(), 1);
        assert_eq!(redis.getbit(dest, 2).unwrap(), 0);
        assert_eq!(redis.getbit(dest, 3).unwrap(), 1);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitpos_basic() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"bitposkey";

        // Set a bit pattern: 00000001 (bit 7 is set)
        redis.setbit(key, 7, 1).unwrap();

        // Find first 1 bit
        let pos = redis.bitpos(key, 1);
        assert!(pos.is_ok());
        assert_eq!(pos.unwrap(), 7, "first 1 bit should be at position 7");

        // Find first 0 bit
        let pos = redis.bitpos(key, 0);
        assert!(pos.is_ok());
        assert_eq!(pos.unwrap(), 0, "first 0 bit should be at position 0");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitpos_with_start_offset() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"bitposkey2";

        // Set bits in second byte: byte 0 = 00000000, byte 1 = 10000000
        redis.setbit(key, 8, 1).unwrap();

        // Find first 1 bit starting from byte 0
        let pos = redis.bitpos_with_start(key, 1, 0);
        assert!(pos.is_ok());
        assert_eq!(
            pos.unwrap(),
            8,
            "first 1 bit from byte 0 should be at position 8"
        );

        // Find first 1 bit starting from byte 1
        let pos = redis.bitpos_with_start(key, 1, 1);
        assert!(pos.is_ok());
        assert_eq!(
            pos.unwrap(),
            8,
            "first 1 bit from byte 1 should be at position 8"
        );

        // Test negative offset (from end)
        let pos = redis.bitpos_with_start(key, 1, -1);
        assert!(pos.is_ok());
        assert_eq!(
            pos.unwrap(),
            8,
            "first 1 bit from last byte should be at position 8"
        );

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitpos_with_range() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"bitposkey3";

        // Set bits across multiple bytes
        // Byte 0: 00000000, Byte 1: 00000000, Byte 2: 10000000
        redis.setbit(key, 16, 1).unwrap();

        // Search in range [0, 1] - should not find the bit
        let pos = redis.bitpos_with_range(key, 1, 0, 1);
        assert!(pos.is_ok());
        assert_eq!(pos.unwrap(), -1, "should not find 1 bit in bytes 0-1");

        // Search in range [0, 2] - should find the bit
        let pos = redis.bitpos_with_range(key, 1, 0, 2);
        assert!(pos.is_ok());
        assert_eq!(pos.unwrap(), 16, "should find 1 bit at position 16");

        // Test with negative offsets
        let pos = redis.bitpos_with_range(key, 1, -1, -1);
        assert!(pos.is_ok());
        assert_eq!(pos.unwrap(), 16, "should find 1 bit in last byte");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bitpos_not_found() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"allones";

        // Set all bits to 1 in first byte
        for i in 0..8 {
            redis.setbit(key, i, 1).unwrap();
        }

        // Search for 0 bit - should not find in first byte
        let pos = redis.bitpos_with_range(key, 0, 0, 0);
        assert!(pos.is_ok());
        assert_eq!(pos.unwrap(), -1, "should not find 0 bit in all-ones byte");

        // Spec: without range, searching for 0 returns the first bit after the string (bit length)
        let pos = redis.bitpos(key, 0);
        assert!(pos.is_ok());
        assert_eq!(
            pos.unwrap(),
            8,
            "1-byte all-ones => first 0 at bit-length 8"
        );

        // Search for 1 bit in non-existent key
        let pos = redis.bitpos(b"nonexistent", 1);
        assert!(pos.is_ok());
        assert_eq!(
            pos.unwrap(),
            -1,
            "should return -1 for 1 bit in non-existent key"
        );

        // Search for 0 bit in non-existent key
        let pos = redis.bitpos(b"nonexistent", 0);
        assert!(pos.is_ok());
        assert_eq!(
            pos.unwrap(),
            0,
            "should return 0 for 0 bit in non-existent key"
        );

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bit_operations_edge_cases() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        // Test SETBIT with negative offset (should fail)
        let result = redis.setbit(b"key", -1, 1);
        assert!(result.is_err(), "setbit with negative offset should fail");

        // Test GETBIT with negative offset (should fail)
        let result = redis.getbit(b"key", -1);
        assert!(result.is_err(), "getbit with negative offset should fail");

        // Test SETBIT with invalid bit value (should fail)
        let result = redis.setbit(b"key", 0, 2);
        assert!(result.is_err(), "setbit with value != 0/1 should fail");

        // Test BITPOS with invalid bit value
        let result = redis.bitpos(b"key", 2);
        assert!(result.is_err(), "bitpos with bit value 2 should fail");

        // Test BITOP NOT with multiple keys (should fail)
        redis.setbit(b"key1", 0, 1).unwrap();
        redis.setbit(b"key2", 0, 1).unwrap();
        let result = redis.bitop(BitOpType::Not, b"dest", &[b"key1", b"key2"]);
        assert!(result.is_err(), "bitop NOT with multiple keys should fail");

        // Test BITOP with empty keys array (should fail)
        let result = redis.bitop(BitOpType::And, b"dest", &[]);
        assert!(result.is_err(), "bitop with empty keys should fail");

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_bit_operations_large_offsets() {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok());

        let key = b"largekey";

        // Set bit at large offset
        let result = redis.setbit(key, 10000, 1);
        assert!(result.is_ok(), "setbit at large offset should succeed");
        assert_eq!(result.unwrap(), 0);

        // Verify the bit is set
        let result = redis.getbit(key, 10000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        // Count bits - should be 1
        let count = redis.bitcount(key, None, None);
        assert!(count.is_ok());
        assert_eq!(count.unwrap(), 1);

        // Find the bit position
        let pos = redis.bitpos(key, 1);
        assert!(pos.is_ok());
        assert_eq!(pos.unwrap(), 10000);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }
}
