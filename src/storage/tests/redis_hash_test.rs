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
    use storage::{
        BgTaskHandler, Redis, StorageOptions, safe_cleanup_test_db, unique_test_db_path,
    };

    #[test]
    fn test_hset_hget_hexists_basic() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

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
        assert!(hexists_result.unwrap());

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

        safe_cleanup_test_db(&test_db_path);
    }

    #[test]
    fn test_hget_nonexistent_key() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

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

        safe_cleanup_test_db(&test_db_path);
    }

    #[test]
    fn test_hscan_basic() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create a hash with multiple fields
        let key = b"test_hash";
        let fields_values = vec![
            (b"field1".as_slice(), b"value1".as_slice()),
            (b"field2".as_slice(), b"value2".as_slice()),
            (b"field3".as_slice(), b"value3".as_slice()),
            (b"field4".as_slice(), b"value4".as_slice()),
            (b"field5".as_slice(), b"value5".as_slice()),
        ];

        for (field, value) in &fields_values {
            let result = redis.hset(key, field, value);
            assert!(result.is_ok(), "hset failed: {:?}", result.err());
        }

        // Test HSCAN with cursor 0, count 3
        let (next_cursor, scan_fields) = redis.hscan(key, 0, None, Some(3)).expect("hscan failed");
        assert_eq!(scan_fields.len(), 3, "Expected 3 fields in first scan");
        assert!(next_cursor > 0, "Should have more data to scan");

        // Continue scanning
        let (final_cursor, remaining_fields) = redis
            .hscan(key, next_cursor, None, Some(10))
            .expect("hscan continuation failed");
        assert_eq!(remaining_fields.len(), 2, "Expected 2 remaining fields");
        assert_eq!(final_cursor, 0, "Should be end of iteration");

        // Verify all fields were returned
        let mut all_scanned: Vec<(String, String)> = scan_fields;
        all_scanned.extend(remaining_fields);
        all_scanned.sort_by(|a, b| a.0.cmp(&b.0));

        let mut expected: Vec<(String, String)> = fields_values
            .iter()
            .map(|(f, v)| {
                (
                    String::from_utf8_lossy(f).to_string(),
                    String::from_utf8_lossy(v).to_string(),
                )
            })
            .collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(all_scanned, expected);

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
    }

    #[test]
    fn test_hscan_with_pattern() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create a hash with fields that match and don't match pattern
        let key = b"test_hash";
        redis.hset(key, b"name", b"Alice").unwrap();
        redis.hset(key, b"age", b"30").unwrap();
        redis.hset(key, b"city", b"NYC").unwrap();
        redis.hset(key, b"country", b"USA").unwrap();

        // Scan with pattern "c*" - should match "city" and "country"
        let (_cursor, fields) = redis
            .hscan(key, 0, Some("c*"), None)
            .expect("hscan with pattern failed");

        // Extract field names
        let field_names: Vec<String> = fields.iter().map(|(f, _)| f.clone()).collect();

        assert!(
            field_names.contains(&"city".to_string()),
            "Should match 'city'"
        );
        assert!(
            field_names.contains(&"country".to_string()),
            "Should match 'country'"
        );
        assert!(
            !field_names.contains(&"name".to_string()),
            "Should not match 'name'"
        );
        assert!(
            !field_names.contains(&"age".to_string()),
            "Should not match 'age'"
        );

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
    }

    #[test]
    fn test_hscan_nonexistent_key() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"nonexistent_hash";

        // Scan non-existent key should return empty result with cursor 0
        let (cursor, fields) = redis.hscan(key, 0, None, None).expect("hscan failed");
        assert_eq!(cursor, 0, "Cursor should be 0 for non-existent key");
        assert!(fields.is_empty(), "Fields should be empty");

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
    }

    #[test]
    fn test_hscan_full_iteration() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        // Create a hash with 20 fields
        let key = b"large_hash";
        for i in 0..20 {
            let field = format!("field{:02}", i);
            let value = format!("value{:02}", i);
            redis.hset(key, field.as_bytes(), value.as_bytes()).unwrap();
        }

        // Scan with small count to test pagination
        let mut all_fields = Vec::new();
        let mut cursor = 0u64;
        let mut iterations = 0;

        loop {
            let (next_cursor, fields) = redis
                .hscan(key, cursor, None, Some(5))
                .expect("hscan failed");

            all_fields.extend(fields);
            iterations += 1;

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;

            // Safety check to prevent infinite loop
            assert!(iterations < 100, "Too many iterations");
        }

        // Verify we got all 20 fields
        assert_eq!(all_fields.len(), 20, "Should have scanned all 20 fields");

        // Verify field names are correct
        let mut field_names: Vec<String> = all_fields.iter().map(|(f, _)| f.clone()).collect();
        field_names.sort();

        for i in 0..20 {
            let expected_field = format!("field{:02}", i);
            assert!(
                field_names.contains(&expected_field),
                "Missing field: {}",
                expected_field
            );
        }

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
    }

    #[test]
    fn test_hscan_with_wildcards() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let key = b"test_hash";
        redis.hset(key, b"user:1:name", b"Alice").unwrap();
        redis.hset(key, b"user:1:age", b"30").unwrap();
        redis.hset(key, b"user:2:name", b"Bob").unwrap();
        redis.hset(key, b"post:1:title", b"Hello").unwrap();

        // Test pattern with wildcard: "user:*"
        let (_, fields) = redis
            .hscan(key, 0, Some("user:*"), None)
            .expect("hscan failed");

        let field_names: Vec<String> = fields.iter().map(|(f, _)| f.clone()).collect();

        assert_eq!(field_names.len(), 3, "Should match 3 user fields");
        assert!(field_names.iter().all(|f| f.starts_with("user:")));

        // Test pattern with question mark: "user:?:name"
        let (_, fields) = redis
            .hscan(key, 0, Some("user:?:name"), None)
            .expect("hscan failed");

        assert_eq!(fields.len(), 2, "Should match user:1:name and user:2:name");

        redis.set_need_close(true);
        drop(redis);

        safe_cleanup_test_db(&test_db_path);
    }
}
