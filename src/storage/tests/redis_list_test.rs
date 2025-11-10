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

//! Unit tests for Redis list operations

#[cfg(test)]
mod redis_list_test {
    use std::sync::Arc;

    use kstd::lock_mgr::LockMgr;
    use storage::{BgTaskHandler, Redis, StorageOptions, safe_cleanup_test_db, unique_test_db_path};

    fn create_test_redis() -> Redis {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 0, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        redis
    }

    #[tokio::test]
    async fn test_lpush_and_llen() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test empty list
        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 0);

        // Test single lpush
        let values = vec![b"value1".to_vec()];
        let new_len = redis.lpush(key, &values).expect("lpush should succeed");
        assert_eq!(new_len, 1);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 1);

        // Test multiple lpush
        let values = vec![b"value2".to_vec(), b"value3".to_vec()];
        let new_len = redis.lpush(key, &values).expect("lpush should succeed");
        assert_eq!(new_len, 3);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 3);
    }

    #[tokio::test]
    async fn test_rpush_and_llen() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test single rpush
        let values = vec![b"value1".to_vec()];
        let new_len = redis.rpush(key, &values).expect("rpush should succeed");
        assert_eq!(new_len, 1);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 1);

        // Test multiple rpush
        let values = vec![b"value2".to_vec(), b"value3".to_vec()];
        let new_len = redis.rpush(key, &values).expect("rpush should succeed");
        assert_eq!(new_len, 3);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 3);
    }

    #[tokio::test]
    async fn test_lpop() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test lpop on empty list
        let result = redis.lpop(key, None).expect("lpop should succeed");
        assert!(result.is_none());

        // Add some values
        let values = vec![b"value1".to_vec(), b"value2".to_vec(), b"value3".to_vec()];
        redis.lpush(key, &values).expect("lpush should succeed");

        // Test single lpop
        let result = redis.lpop(key, None).expect("lpop should succeed");
        assert!(result.is_some());
        let popped = result.unwrap();
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0], b"value3"); // Last pushed should be first popped

        // Test multiple lpop
        let result = redis.lpop(key, Some(2)).expect("lpop should succeed");
        assert!(result.is_some());
        let popped = result.unwrap();
        assert_eq!(popped.len(), 2);
        assert_eq!(popped[0], b"value2");
        assert_eq!(popped[1], b"value1");

        // List should be empty now
        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 0);
    }

    #[tokio::test]
    async fn test_rpop() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test rpop on empty list
        let result = redis.rpop(key, None).expect("rpop should succeed");
        assert!(result.is_none());

        // Add some values
        let values = vec![b"value1".to_vec(), b"value2".to_vec(), b"value3".to_vec()];
        redis.rpush(key, &values).expect("rpush should succeed");

        // Test single rpop
        let result = redis.rpop(key, None).expect("rpop should succeed");
        assert!(result.is_some());
        let popped = result.unwrap();
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0], b"value3"); // Last pushed should be first popped

        // Test multiple rpop
        let result = redis.rpop(key, Some(2)).expect("rpop should succeed");
        assert!(result.is_some());
        let popped = result.unwrap();
        assert_eq!(popped.len(), 2);
        assert_eq!(popped[0], b"value2");
        assert_eq!(popped[1], b"value1");

        // List should be empty now
        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 0);
    }

    #[tokio::test]
    async fn test_lindex() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test lindex on empty list
        let result = redis.lindex(key, 0).expect("lindex should succeed");
        assert!(result.is_none());

        // Add some values
        let values = vec![b"value1".to_vec(), b"value2".to_vec(), b"value3".to_vec()];
        redis.rpush(key, &values).expect("rpush should succeed");

        // Test positive indices
        let result = redis.lindex(key, 0).expect("lindex should succeed");
        assert_eq!(result.unwrap(), b"value1");

        let result = redis.lindex(key, 1).expect("lindex should succeed");
        assert_eq!(result.unwrap(), b"value2");

        let result = redis.lindex(key, 2).expect("lindex should succeed");
        assert_eq!(result.unwrap(), b"value3");

        // Test negative indices
        let result = redis.lindex(key, -1).expect("lindex should succeed");
        assert_eq!(result.unwrap(), b"value3");

        let result = redis.lindex(key, -2).expect("lindex should succeed");
        assert_eq!(result.unwrap(), b"value2");

        let result = redis.lindex(key, -3).expect("lindex should succeed");
        assert_eq!(result.unwrap(), b"value1");

        // Test out of bounds
        let result = redis.lindex(key, 3).expect("lindex should succeed");
        assert!(result.is_none());

        let result = redis.lindex(key, -4).expect("lindex should succeed");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_lrange() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test lrange on empty list
        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert!(result.is_empty());

        // Add some values
        let values = vec![
            b"value1".to_vec(),
            b"value2".to_vec(),
            b"value3".to_vec(),
            b"value4".to_vec(),
            b"value5".to_vec(),
        ];
        redis.rpush(key, &values).expect("rpush should succeed");

        // Test full range
        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], b"value1");
        assert_eq!(result[4], b"value5");

        // Test partial range
        let result = redis.lrange(key, 1, 3).expect("lrange should succeed");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"value2");
        assert_eq!(result[2], b"value4");

        // Test negative indices
        let result = redis.lrange(key, -3, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"value3");
        assert_eq!(result[2], b"value5");

        // Test invalid range
        let result = redis.lrange(key, 3, 1).expect("lrange should succeed");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_lset() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test lset on empty list
        let result = redis.lset(key, 0, b"new_value".to_vec());
        assert!(result.is_err());

        // Add some values
        let values = vec![b"value1".to_vec(), b"value2".to_vec(), b"value3".to_vec()];
        redis.rpush(key, &values).expect("rpush should succeed");

        // Test valid lset
        redis
            .lset(key, 1, b"new_value".to_vec())
            .expect("lset should succeed");

        let result = redis.lindex(key, 1).expect("lindex should succeed");
        assert_eq!(result.unwrap(), b"new_value");

        // Test negative index
        redis
            .lset(key, -1, b"last_value".to_vec())
            .expect("lset should succeed");

        let result = redis.lindex(key, -1).expect("lindex should succeed");
        assert_eq!(result.unwrap(), b"last_value");

        // Test out of bounds
        let result = redis.lset(key, 5, b"invalid".to_vec());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ltrim() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test ltrim on empty list
        redis.ltrim(key, 0, 1).expect("ltrim should succeed");

        // Add some values
        let values = vec![
            b"value1".to_vec(),
            b"value2".to_vec(),
            b"value3".to_vec(),
            b"value4".to_vec(),
            b"value5".to_vec(),
        ];
        redis.rpush(key, &values).expect("rpush should succeed");

        // Test trim to middle range
        redis.ltrim(key, 1, 3).expect("ltrim should succeed");

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 3);

        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"value2");
        assert_eq!(result[1], b"value3");
        assert_eq!(result[2], b"value4");
    }

    #[tokio::test]
    async fn test_lrem() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test lrem on empty list
        let removed = redis.lrem(key, 1, b"value").expect("lrem should succeed");
        assert_eq!(removed, 0);

        // Add some values with duplicates
        let values = vec![
            b"value1".to_vec(),
            b"value2".to_vec(),
            b"value1".to_vec(),
            b"value3".to_vec(),
            b"value1".to_vec(),
        ];
        redis.rpush(key, &values).expect("rpush should succeed");

        // Test remove first 2 occurrences
        let removed = redis.lrem(key, 2, b"value1").expect("lrem should succeed");
        assert_eq!(removed, 2);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 3);

        // Test remove all occurrences
        redis
            .rpush(key, &[b"value1".to_vec()])
            .expect("rpush should succeed");

        let removed = redis.lrem(key, 0, b"value1").expect("lrem should succeed");
        assert_eq!(removed, 2); // Should remove the remaining 2 occurrences

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 2);
    }

    #[tokio::test]
    async fn test_mixed_operations() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test mixed lpush and rpush
        redis
            .lpush(key, &[b"left1".to_vec()])
            .expect("lpush should succeed");
        redis
            .rpush(key, &[b"right1".to_vec()])
            .expect("rpush should succeed");
        redis
            .lpush(key, &[b"left2".to_vec()])
            .expect("lpush should succeed");
        redis
            .rpush(key, &[b"right2".to_vec()])
            .expect("rpush should succeed");

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 4);

        // Check order: left2, left1, right1, right2
        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        println!(
            "Mixed operations final result: {:?}",
            result
                .iter()
                .map(|v| String::from_utf8_lossy(v))
                .collect::<Vec<_>>()
        );
        println!("Expected: [left2, left1, right1, right2]");
        if !result.is_empty() {
            assert_eq!(result[0], b"left2");
        } else {
            panic!("Result is empty, expected 4 elements");
        }
        assert_eq!(result[1], b"left1");
        assert_eq!(result[2], b"right1");
        assert_eq!(result[3], b"right2");

        // Test mixed lpop and rpop
        let left_popped = redis.lpop(key, None).expect("lpop should succeed").unwrap();
        assert_eq!(left_popped[0], b"left2");

        let right_popped = redis.rpop(key, None).expect("rpop should succeed").unwrap();
        assert_eq!(right_popped[0], b"right2");

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 2);
    }

    #[tokio::test]
    async fn test_transaction_atomicity() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Add initial values
        let values = vec![b"value1".to_vec(), b"value2".to_vec()];
        redis.rpush(key, &values).expect("rpush should succeed");

        // Test that operations are atomic - if we can read the length,
        // we should be able to read all elements
        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 2);

        for i in 0..len {
            let result = redis.lindex(key, i).expect("lindex should succeed");
            assert!(result.is_some(), "Element at index {} should exist", i);
        }
    }

    #[tokio::test]
    async fn test_error_handling() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test operations on non-existent key
        let result = redis.lindex(key, 0).expect("lindex should succeed");
        assert!(result.is_none());

        let result = redis.lpop(key, None).expect("lpop should succeed");
        assert!(result.is_none());

        let result = redis.rpop(key, None).expect("rpop should succeed");
        assert!(result.is_none());

        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert!(result.is_empty());

        // Test lset on non-existent key
        let result = redis.lset(key, 0, b"value".to_vec());
        assert!(result.is_err());
    }
}
