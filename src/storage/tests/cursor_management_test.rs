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
mod cursor_management_test {
    use std::sync::Arc;

    use storage::error::Error;
    use storage::{DataType, StorageOptions, storage::Storage, unique_test_db_path};

    #[tokio::test]
    async fn test_store_and_load_cursor_basic() {
        let test_db_path = unique_test_db_path();
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());

        let _receiver = storage.open(options, &test_db_path).unwrap();

        // Test basic storage and load
        let result =
            storage.store_cursor_start_key(DataType::Set, 12345, 's', "test_key_001".to_string());
        assert!(result.is_ok());

        let (cursor_type, start_key) = storage
            .load_cursor_start_key(DataType::Set, 12345)
            .expect("cursor should exist");
        assert_eq!(cursor_type, 's');
        assert_eq!(start_key, "test_key_001");

        drop(storage);
        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_load_nonexist_cursor() {
        let test_db_path = unique_test_db_path();
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());

        let _receiver = storage.open(options, &test_db_path).unwrap();

        let err = storage
            .load_cursor_start_key(DataType::Set, 99999)
            .unwrap_err();
        assert!(matches!(err, Error::KeyNotFound { .. }));

        drop(storage);
        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_multiple_data_types() {
        let test_db_path = unique_test_db_path();
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());

        let _receiver = storage.open(options, &test_db_path).unwrap();

        // Test different data types
        let test_cases = vec![
            (DataType::String, 'k', "string_key"),
            (DataType::Hash, 'h', "hash_key"),
            (DataType::Set, 's', "set_key"),
            (DataType::List, 'l', "list_key"),
            (DataType::ZSet, 'z', "zset_key"),
        ];

        for (dtype, expected_type, key) in test_cases {
            storage
                .store_cursor_start_key(dtype, 1001, expected_type, key.to_string())
                .unwrap();

            let (cursor_type, start_key) = storage.load_cursor_start_key(dtype, 1001).unwrap();

            assert_eq!(cursor_type, expected_type);
            assert_eq!(start_key, key);
        }

        drop(storage);
        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_empty_next_key_remove() {
        let test_db_path = unique_test_db_path();
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _receiver = storage.open(options, &test_db_path).unwrap();
        storage
            .store_cursor_start_key(DataType::Set, 12345, 's', "test_key".to_string())
            .unwrap();

        let result = storage.load_cursor_start_key(DataType::Set, 12345);
        assert!(result.is_ok());

        // Store with empty next_key should remove the entry
        storage
            .store_cursor_start_key(DataType::Set, 12345, 's', String::new())
            .unwrap();

        let err = storage
            .load_cursor_start_key(DataType::Set, 12345)
            .unwrap_err();
        assert!(matches!(err, Error::KeyNotFound { .. }));

        drop(storage);
        if test_db_path.exists() {
            std::fs::remove_dir_all(test_db_path).unwrap();
        }
    }
}
