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
    use storage::{BgTaskHandler, BeforeOrAfter, Redis, StorageOptions, unique_test_db_path};

    fn create_test_redis() -> Redis {
        let test_db_path = unique_test_db_path();

        if test_db_path.exists() {
            std::fs::remove_dir_all(&test_db_path).unwrap();
        }

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

    #[tokio::test]
    async fn test_lpushx() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test lpushx on non-existent list - should return 0 and not create list
        let result = redis
            .lpushx(key, &[b"value1".to_vec()])
            .expect("lpushx should succeed");
        assert_eq!(result, 0);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 0);

        // Create list first with rpush
        redis
            .rpush(key, &[b"initial".to_vec()])
            .expect("rpush should succeed");

        // Test lpushx on existing list - should work
        let result = redis
            .lpushx(key, &[b"value1".to_vec()])
            .expect("lpushx should succeed");
        assert_eq!(result, 2);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 2);

        // Verify order - lpushx adds to head
        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"value1"); // lpushx value should be at head
        assert_eq!(result[1], b"initial"); // original value should be second
    }

    #[tokio::test]
    async fn test_lpushx_multiple_values() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Create list first
        redis
            .rpush(key, &[b"original".to_vec()])
            .expect("rpush should succeed");

        // Test lpushx with multiple values
        let values = vec![b"value1".to_vec(), b"value2".to_vec(), b"value3".to_vec()];
        let result = redis.lpushx(key, &values).expect("lpushx should succeed");
        assert_eq!(result, 4); // original + 3 new values

        // Verify order: value3, value2, value1, original (reversed due to lpush)
        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], b"value3"); // Last lpushx value should be at head
        assert_eq!(result[1], b"value2");
        assert_eq!(result[2], b"value1");
        assert_eq!(result[3], b"original");
    }

    #[tokio::test]
    async fn test_rpushx() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test rpushx on non-existent list - should return 0 and not create list
        let result = redis
            .rpushx(key, &[b"value1".to_vec()])
            .expect("rpushx should succeed");
        assert_eq!(result, 0);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 0);

        // Create list first with lpush
        redis
            .lpush(key, &[b"initial".to_vec()])
            .expect("lpush should succeed");

        // Test rpushx on existing list - should work
        let result = redis
            .rpushx(key, &[b"value1".to_vec()])
            .expect("rpushx should succeed");
        assert_eq!(result, 2);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 2);

        // Verify order - rpushx adds to tail
        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"initial"); // original value should be first
        assert_eq!(result[1], b"value1"); // rpushx value should be at tail
    }

    #[tokio::test]
    async fn test_rpushx_multiple_values() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Create list first
        redis
            .lpush(key, &[b"original".to_vec()])
            .expect("lpush should succeed");

        // Test rpushx with multiple values
        let values = vec![b"value1".to_vec(), b"value2".to_vec(), b"value3".to_vec()];
        let result = redis.rpushx(key, &values).expect("rpushx should succeed");
        assert_eq!(result, 4); // original + 3 new values

        // Verify order: original, value1, value2, value3 (preserved order for rpush)
        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], b"original");
        assert_eq!(result[1], b"value1");
        assert_eq!(result[2], b"value2");
        assert_eq!(result[3], b"value3");
    }

    #[tokio::test]
    async fn test_lpushx_rpushx_mixed_operations() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Start with a basic list
        redis
            .lpush(key, &[b"middle".to_vec()])
            .expect("lpush should succeed");

        // Add elements to both ends using pushx
        redis
            .rpushx(key, &[b"tail1".to_vec(), b"tail2".to_vec()])
            .expect("rpushx should succeed");
        redis
            .lpushx(key, &[b"head1".to_vec()])
            .expect("lpushx should succeed");
        redis
            .rpushx(key, &[b"tail3".to_vec()])
            .expect("rpushx should succeed");

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 5);

        // Verify final order: head1, middle, tail1, tail2, tail3
        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], b"head1");
        assert_eq!(result[1], b"middle");
        assert_eq!(result[2], b"tail1");
        assert_eq!(result[3], b"tail2");
        assert_eq!(result[4], b"tail3");
    }

    #[tokio::test]
    async fn test_lpushx_on_empty_list() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Create an empty list and delete its contents (empty list case)
        redis
            .rpush(key, &[b"temp".to_vec()])
            .expect("rpush should succeed");
        redis.lpop(key, None).expect("lpop should succeed"); // List is now empty but exists

        // Verify list is empty but exists
        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 0);

        // Test lpushx on empty list - should work (list exists, even if empty)
        let result = redis
            .lpushx(key, &[b"new_value".to_vec()])
            .expect("lpushx should succeed");
        assert_eq!(result, 1);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 1);

        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], b"new_value");
    }

    #[tokio::test]
    async fn test_rpushx_on_empty_list() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Create an empty list and delete its contents (empty list case)
        redis
            .rpush(key, &[b"temp".to_vec()])
            .expect("rpush should succeed");
        redis.lpop(key, None).expect("lpop should succeed"); // List is now empty but exists

        // Verify list is empty but exists
        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 0);

        // Test rpushx on empty list - should work (list exists, even if empty)
        let result = redis
            .rpushx(key, &[b"new_value".to_vec()])
            .expect("rpushx should succeed");
        assert_eq!(result, 1);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 1);

        let result = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], b"new_value");
    }

    #[tokio::test]
    async fn test_lpushx_rpushx_edge_cases() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Test with empty values array - should return 0 for non-existent list
        let result = redis.lpushx(key, &[]).expect("lpushx should succeed");
        assert_eq!(result, 0);

        let result = redis.rpushx(key, &[]).expect("rpushx should succeed");
        assert_eq!(result, 0);

        // List still shouldn't exist
        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, 0);

        // Create list
        redis
            .rpush(key, &[b"initial".to_vec()])
            .expect("rpush should succeed");

        // Test with empty values on existing list - should not change length
        let original_len = redis.llen(key).expect("llen should succeed");
        let result = redis.lpushx(key, &[]).expect("lpushx should succeed");
        assert_eq!(result, original_len);

        let len = redis.llen(key).expect("llen should succeed");
        assert_eq!(len, original_len);
    }

    #[tokio::test]
    async fn test_linsert_before_pivot() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Create initial list: [a, b, c]
        redis
            .rpush(key, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .expect("rpush should succeed");

        // Insert 'x' before 'b': should become [a, x, b, c]
        let result = redis
            .linsert(key, BeforeOrAfter::Before, b"b", b"x")
            .expect("linsert should succeed");
        assert_eq!(result, 4);

        // Verify the list content
        let range = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(range, vec![b"a".to_vec(), b"x".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test]
    async fn test_linsert_after_pivot() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Create initial list: [a, b, c]
        redis
            .rpush(key, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .expect("rpush should succeed");

        // Insert 'y' after 'b': should become [a, b, y, c]
        let result = redis
            .linsert(key, BeforeOrAfter::After, b"b", b"y")
            .expect("linsert should succeed");
        assert_eq!(result, 4);

        // Verify the list content
        let range = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(range, vec![b"a".to_vec(), b"b".to_vec(), b"y".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test]
    async fn test_linsert_pivot_not_found() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Create initial list: [a, b, c]
        redis
            .rpush(key, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .expect("rpush should succeed");

        // Try to insert before non-existent pivot 'z'
        let result = redis
            .linsert(key, BeforeOrAfter::Before, b"z", b"x")
            .expect("linsert should succeed");
        assert_eq!(result, -1);

        // Verify list is unchanged
        let range = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(range, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test]
    async fn test_linsert_nonexistent_list() {
        let redis = create_test_redis();
        let key = b"nonexistent_list";

        // Try to insert into non-existent list
        let result = redis
            .linsert(key, BeforeOrAfter::Before, b"pivot", b"value")
            .expect_err("linsert should fail");
        assert!(result.to_string().contains("Key not found"));
    }

    #[tokio::test]
    async fn test_rpoplpush_same_list() {
        let redis = create_test_redis();
        let key = b"test_list";

        // Create initial list: [a, b, c]
        redis
            .rpush(key, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .expect("rpush should succeed");

        // RPOPLPUSH from same list: 'c' should be moved to front
        let result = redis.rpoplpush(key, key).expect("rpoplpush should succeed");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"c");

        // Verify the list content: should become [c, a, b]
        let range = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(range, vec![b"c".to_vec(), b"a".to_vec(), b"b".to_vec()]);
    }

    #[tokio::test]
    async fn test_rpoplpush_different_lists() {
        let redis = create_test_redis();
        let source_key = b"source_list";
        let dest_key = b"dest_list";

        // Create source list: [a, b, c]
        redis
            .rpush(source_key, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .expect("rpush should succeed");

        // Create destination list: [x, y]
        redis
            .rpush(dest_key, &[b"x".to_vec(), b"y".to_vec()])
            .expect("rpush should succeed");

        // RPOPLPUSH from source to destination
        let result = redis.rpoplpush(source_key, dest_key).expect("rpoplpush should succeed");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"c");

        // Verify source list: should become [a, b]
        let source_range = redis.lrange(source_key, 0, -1).expect("lrange should succeed");
        assert_eq!(source_range, vec![b"a".to_vec(), b"b".to_vec()]);

        // Verify destination list: should become [c, x, y]
        let dest_range = redis.lrange(dest_key, 0, -1).expect("lrange should succeed");
        assert_eq!(dest_range, vec![b"c".to_vec(), b"x".to_vec(), b"y".to_vec()]);
    }

    #[tokio::test]
    async fn test_rpoplpush_empty_source() {
        let redis = create_test_redis();
        let source_key = b"empty_source";
        let dest_key = b"dest_list";

        // Create destination list: [x, y]
        redis
            .rpush(dest_key, &[b"x".to_vec(), b"y".to_vec()])
            .expect("rpush should succeed");

        // Try RPOPLPUSH from empty source
        let result = redis.rpoplpush(source_key, dest_key).expect("rpoplpush should succeed");
        assert!(result.is_none());

        // Verify destination list is unchanged
        let dest_range = redis.lrange(dest_key, 0, -1).expect("lrange should succeed");
        assert_eq!(dest_range, vec![b"x".to_vec(), b"y".to_vec()]);
    }

    #[tokio::test]
    async fn test_rpoplpush_nonexistent_destination() {
        let redis = create_test_redis();
        let source_key = b"source_list";
        let dest_key = b"nonexistent_dest";

        // Create source list: [a, b]
        redis
            .rpush(source_key, &[b"a".to_vec(), b"b".to_vec()])
            .expect("rpush should succeed");

        // RPOPLPUSH to non-existent destination (should create it)
        let result = redis.rpoplpush(source_key, dest_key).expect("rpoplpush should succeed");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"b");

        // Verify source list: should become [a]
        let source_range = redis.lrange(source_key, 0, -1).expect("lrange should succeed");
        assert_eq!(source_range, vec![b"a".to_vec()]);

        // Verify destination list: should become [b]
        let dest_range = redis.lrange(dest_key, 0, -1).expect("lrange should succeed");
        assert_eq!(dest_range, vec![b"b".to_vec()]);
    }

    #[tokio::test]
    async fn test_rpoplpush_both_nonexistent() {
        let redis = create_test_redis();
        let source_key = b"nonexistent_source";
        let dest_key = b"nonexistent_dest";

        // Try RPOPLPUSH with both lists non-existent
        let result = redis.rpoplpush(source_key, dest_key).expect("rpoplpush should succeed");
        assert!(result.is_none());

        // Verify neither list was created
        let source_len = redis.llen(source_key).expect("llen should succeed");
        assert_eq!(source_len, 0);
        let dest_len = redis.llen(dest_key).expect("llen should succeed");
        assert_eq!(dest_len, 0);
    }

    #[tokio::test]
    async fn test_concurrent_lpush() {
        use std::sync::Arc;
        use std::thread;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let redis = Arc::new(create_test_redis());
        let key = b"concurrent_list";
        let num_threads = 10;
        let operations_per_thread = 100;

        // Counter to track total successful operations
        let total_ops = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let redis_clone = redis.clone();
            let key = key.to_vec();
            let total_ops_clone = total_ops.clone();

            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let value = format!("thread_{}_value_{}", thread_id, i).into_bytes();
                    match redis_clone.lpush(&key, &[value]) {
                        Ok(_) => total_ops_clone.fetch_add(1, Ordering::Relaxed),
                        Err(e) => {
                            eprintln!("Thread {} operation {} failed: {:?}", thread_id, i, e);
                            0
                        }
                    };
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all operations completed successfully
        let final_count = total_ops.load(Ordering::Relaxed);
        assert_eq!(final_count, num_threads * operations_per_thread);

        // Verify final list length
        let list_len = redis.llen(key).expect("llen should succeed");
        assert_eq!(list_len, final_count as i64);

        // Verify list contents are unique and all present
        let list_content = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(list_content.len(), final_count);

        // Convert to strings for easier verification
        let mut content_strings: Vec<String> = list_content.iter()
            .map(|v| String::from_utf8_lossy(v).to_string())
            .collect();
        content_strings.sort(); // Sort to make verification deterministic

        // Verify we have all expected values
        for thread_id in 0..num_threads {
            for i in 0..operations_per_thread {
                let expected_value = format!("thread_{}_value_{}", thread_id, i);
                assert!(content_strings.contains(&expected_value),
                    "Missing value: {}", expected_value);
            }
        }
    }

    #[tokio::test]
    async fn test_concurrent_lpushx_rpushx() {
        use std::sync::Arc;
        use std::thread;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let redis = Arc::new(create_test_redis());
        let key = b"concurrent_list_pushx";
        let num_threads = 8;

        // Initialize the list first
        redis.lpush(key, &[b"initial".to_vec()]).expect("initial lpush should succeed");

        let successful_lpushx = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Half threads use LPUSHX, half use RPUSHX
        for thread_id in 0..num_threads {
            let redis_clone = redis.clone();
            let key = key.to_vec();
            let successful_lpushx_clone = successful_lpushx.clone();

            let handle = thread::spawn(move || {
                for i in 0..10 {
                    let value = format!("thread_{}_value_{}", thread_id, i).into_bytes();

                    if thread_id % 2 == 0 {
                        // Use LPUSHX
                        match redis_clone.lpushx(&key, &[value.clone()]) {
                            Ok(len) if len > 0 => successful_lpushx_clone.fetch_add(1, Ordering::Relaxed),
                            Ok(_) => 0, // List doesn't exist (shouldn't happen in this test)
                            Err(_) => 0,
                        };
                    } else {
                        // Use RPUSHX
                        match redis_clone.rpushx(&key, &[value.clone()]) {
                            Ok(len) if len > 0 => successful_lpushx_clone.fetch_add(1, Ordering::Relaxed),
                            Ok(_) => 0, // List doesn't exist (shouldn't happen in this test)
                            Err(_) => 0,
                        };
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all operations were successful
        let total_successful = successful_lpushx.load(Ordering::Relaxed);
        assert_eq!(total_successful, num_threads * 10);

        // Verify final list length (1 initial + all operations)
        let list_len = redis.llen(key).expect("llen should succeed");
        assert_eq!(list_len, (total_successful + 1) as i64);
    }

    #[tokio::test]
    async fn test_concurrent_rpoplpush() {
        use std::sync::Arc;
        use std::thread;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let redis = Arc::new(create_test_redis());
        let source_key = b"concurrent_source";
        let dest_key = b"concurrent_dest";
        let num_threads = 5;
        let items_per_thread = 20;

        // Pre-populate source list with known items
        let mut expected_items = vec![];
        for i in 0..(num_threads * items_per_thread) {
            let item = format!("item_{}", i);
            expected_items.push(item.clone());
            redis.rpush(source_key, &[item.into_bytes()]).expect("initial rpush should succeed");
        }

        let moved_items = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let redis_clone = redis.clone();
            let source_key = source_key.to_vec();
            let dest_key = dest_key.to_vec();
            let moved_items_clone = moved_items.clone();

            let handle = thread::spawn(move || {
                let mut local_moved = 0;
                for _ in 0..items_per_thread {
                    match redis_clone.rpoplpush(&source_key, &dest_key) {
                        Ok(Some(_value)) => {
                            local_moved += 1;
                        }
                        Ok(None) => {
                            // Source list empty, stop trying
                            break;
                        }
                        Err(e) => {
                            eprintln!("Thread {} rpoplpush failed: {:?}", thread_id, e);
                            break;
                        }
                    }
                }
                moved_items_clone.fetch_add(local_moved, Ordering::Relaxed);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all items were moved
        let total_moved = moved_items.load(Ordering::Relaxed);
        assert_eq!(total_moved, num_threads * items_per_thread);

        // Verify source list is empty
        let source_len = redis.llen(source_key).expect("llen should succeed");
        assert_eq!(source_len, 0);

        // Verify destination list has all items
        let dest_len = redis.llen(dest_key).expect("llen should succeed");
        assert_eq!(dest_len, total_moved as i64);
    }

    #[tokio::test]
    async fn test_concurrent_linsert() {
        use std::sync::Arc;
        use std::thread;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let redis = Arc::new(create_test_redis());
        let key = b"concurrent_linsert_list";
        let num_threads = 6;
        let inserts_per_thread = 10;

        // Create initial list with some pivot elements
        redis.lpush(key, &[b"pivot_start".to_vec()]).expect("initial lpush failed");
        redis.lpush(key, &[b"pivot_end".to_vec()]).expect("initial lpush failed");

        let successful_inserts = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let redis_clone = redis.clone();
            let key = key.to_vec();
            let successful_inserts_clone = successful_inserts.clone();

            let handle = thread::spawn(move || {
                let mut local_success = 0;
                for i in 0..inserts_per_thread {
                    let value = format!("thread_{}_value_{}", thread_id, i).into_bytes();

                    // Alternate between inserting before and after pivot_start
                    let before_or_after = if thread_id % 2 == 0 {
                        BeforeOrAfter::Before
                    } else {
                        BeforeOrAfter::After
                    };

                    match redis_clone.linsert(&key, before_or_after, b"pivot_start", &value) {
                        Ok(len) if len > 0 => {
                            local_success += 1;
                        }
                        Ok(_) => {
                            // Pivot not found or list doesn't exist
                        }
                        Err(e) => {
                            eprintln!("Thread {} linsert failed: {:?}", thread_id, e);
                        }
                    }
                }
                successful_inserts_clone.fetch_add(local_success, Ordering::Relaxed);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final list contains initial elements plus inserted ones
        let list_len = redis.llen(key).expect("llen should succeed");
        let successful_count = successful_inserts.load(Ordering::Relaxed);
        assert_eq!(list_len, (2 + successful_count) as i64);

        // Verify list content
        let list_content = redis.lrange(key, 0, -1).expect("lrange should succeed");
        assert_eq!(list_content.len(), list_len as usize);

        // Check that pivot elements are still there
        let content_strings: Vec<String> = list_content.iter()
            .map(|v| String::from_utf8_lossy(v).to_string())
            .collect();

        assert!(content_strings.contains(&"pivot_start".to_string()));
        assert!(content_strings.contains(&"pivot_end".to_string()));
    }
}
