#[cfg(test)]
mod redis_string_test {
    use kstd::lock_mgr::LockMgr;
    use std::sync::Arc;
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
}
