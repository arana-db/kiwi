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
mod redis_hash_test {
    use std::sync::Arc;

    use kstd::lock_mgr::LockMgr;
    use storage::{BgTaskHandler, Redis, StorageOptions, unique_test_db_path};

    #[test]
    fn test_hset_hget_hexists_basic() {
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

        let key = b"test_hash";
        let field = b"field1";
        let value = b"value1";

        // Test hset - should create new hash and return 1
        let hset_result = redis.hset(key, field, value);
        assert!(
            hset_result.is_ok(),
            "hset command failed: {:?}",
            hset_result.err()
        );
        let hset_count = hset_result.unwrap();
        assert_eq!(hset_count, 1);

        // Test hset again with same field - should return 0 (field already exists)
        let hset_result2 = redis.hset(key, field, b"value2");
        assert!(
            hset_result2.is_ok(),
            "second hset failed: {:?}",
            hset_result2.err()
        );
        let hset_count2 = hset_result2.unwrap();
        assert_eq!(hset_count2, 0);

        // Add a small delay to ensure data is persisted
        std::thread::sleep(std::time::Duration::from_millis(10));

        // // Test hexists first
        let hexists_result = redis.hexists(key, field);
        assert!(
            hexists_result.is_ok(),
            "hexists failed: {:?}",
            hexists_result.err()
        );
        assert_eq!(hexists_result.unwrap(), true);

        let expected_val = b"value2";
        // Test hget - should return the value
        let hget_result = redis.hget(key, field);
        assert!(
            hget_result.is_ok(),
            "hget command failed: {:?}",
            hget_result.err()
        );
        let retrieved_value = hget_result.unwrap();
        assert!(
            retrieved_value.is_some(),
            "hget returned None, expected value: {:?}",
            String::from_utf8_lossy(expected_val)
        );
        assert_eq!(
            retrieved_value.unwrap(),
            String::from_utf8_lossy(expected_val).to_string()
        );

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[test]
    fn test_hget_nonexistent_key() {
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

        let key = b"nonexistent_hash";
        let field = b"field1";

        // Test hget on non-existent key
        let hget_result = redis.hget(key, field);
        assert!(hget_result.is_ok(), "hget failed: {:?}", hget_result.err());
        assert_eq!(hget_result.unwrap(), None);

        redis.set_need_close(true);
        drop(redis);

        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }
}
