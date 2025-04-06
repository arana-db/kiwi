//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use super::storage::*;
use std::collections::HashMap;
use tempfile::TempDir;

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (TempDir, Storage) {
        let temp_dir = TempDir::new().unwrap();
        let opts = StorageOptions::default();
        let storage = Storage::open(opts).unwrap();
        (temp_dir, storage)
    }

    #[test]
    fn test_hash_basic_operations() {
        let (_temp_dir, storage) = setup();

        // Test hash set and get
        storage.hset("hash1", "field1", "value1").unwrap();
        let result = storage.hget("hash1", "field1").unwrap();
        assert_eq!(result, Some("value1".to_string()));

        // Test hash exists
        assert!(storage.hexists("hash1", "field1").unwrap());
        assert!(!storage.hexists("hash1", "nonexistent").unwrap());

        // Test hash delete
        storage.hdel("hash1", "field1").unwrap();
        assert!(!storage.hexists("hash1", "field1").unwrap());
    }

    #[test]
    fn test_hash_multiple_operations() {
        let (_temp_dir, storage) = setup();

        // Test multiple hash set
        let mut fields = HashMap::new();
        fields.insert("field1".to_string(), "value1".to_string());
        fields.insert("field2".to_string(), "value2".to_string());
        storage.hmset("hash1", &fields).unwrap();

        // Test multiple hash get
        let field_names = vec!["field1".to_string(), "field2".to_string()];
        let results = storage.hmget("hash1", &field_names).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value, "value1");
        assert_eq!(results[1].value, "value2");

        // Test get all fields
        let all_fields = storage.hgetall("hash1").unwrap();
        assert_eq!(all_fields.len(), 2);
        assert!(all_fields.contains_key("field1"));
        assert!(all_fields.contains_key("field2"));
    }

    #[test]
    fn test_hash_increment_operations() {
        let (_temp_dir, storage) = setup();

        // Test increment by
        storage.hset("hash1", "counter", "10").unwrap();
        let result = storage.hincrby("hash1", "counter", 5).unwrap();
        assert_eq!(result, 15);

        // Test increment by float
        storage.hset("hash1", "float_counter", "10.5").unwrap();
        let result = storage.hincrbyfloat("hash1", "float_counter", 2.5).unwrap();
        assert_eq!(result, 13.0);
    }

    #[test]
    fn test_hash_scan() {
        let (_temp_dir, storage) = setup();

        // Insert test data
        for i in 0..100 {
            storage.hset("hash1", format!("field{}", i), format!("value{}", i)).unwrap();
        }

        // Test scanning
        let mut cursor = 0;
        let mut total_fields = 0;

        loop {
            let (next_cursor, fields) = storage.hscan("hash1", cursor, None).unwrap();
            total_fields += fields.len();
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        assert_eq!(total_fields, 100);
    }
}