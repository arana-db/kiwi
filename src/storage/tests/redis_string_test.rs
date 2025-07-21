#[cfg(test)]
mod redis_string_test {
    use kstd::lock_mgr::LockMgr;
    use std::{sync::Arc, thread, time::Duration};
    use storage::{unique_test_db_path, BgTaskHandler, Redis, StorageOptions};

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
                    if get_result.is_ok() {
                        let value = get_result.unwrap();
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
            if get_result.is_ok() {
                let value = get_result.unwrap();
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
}
