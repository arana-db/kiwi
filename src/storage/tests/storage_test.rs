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
use std::time::{Duration, SystemTime};
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
    fn test_key_value_operations() {
        let (_temp_dir, storage) = setup();

        // Test string operations
        storage.set("key1", "value1").unwrap();
        let result = storage.get("key1").unwrap();
        assert_eq!(result, Some("value1".to_string()));

        // Test key info
        let info = storage.get_key_info().unwrap();
        assert_eq!(info.keys, 1);
        assert_eq!(info.expires, 0);

        // Test expiration
        let ttl = Duration::from_secs(1);
        storage.set_with_ttl("key2", "value2", ttl).unwrap();
        assert_eq!(storage.get("key2").unwrap(), Some("value2".to_string()));

        std::thread::sleep(ttl + Duration::from_millis(100));
        assert_eq!(storage.get("key2").unwrap(), None);
    }

    #[test]
    fn test_batch_operations() {
        let (_temp_dir, storage) = setup();

        // Prepare batch data
        let kvs = vec![
            KeyValue {
                key: "key1".to_string(),
                value: "value1".to_string(),
            },
            KeyValue {
                key: "key2".to_string(),
                value: "value2".to_string(),
            },
        ];

        // Test batch set
        storage.batch_set(&kvs).unwrap();

        // Verify batch results
        for kv in kvs {
            let result = storage.get(&kv.key).unwrap();
            assert_eq!(result, Some(kv.value));
        }

        // Test batch delete
        let keys = vec!["key1".to_string(), "key2".to_string()];
        storage.batch_delete(&keys).unwrap();

        // Verify deletion
        for key in keys {
            let result = storage.get(&key).unwrap();
            assert_eq!(result, None);
        }
    }

    #[test]
    fn test_field_operations() {
        let (_temp_dir, storage) = setup();

        // Test field set and get
        storage.hset("hash1", "field1", "value1").unwrap();
        let result = storage.hget("hash1", "field1").unwrap();
        assert_eq!(result, Some("value1".to_string()));

        // Test multiple fields
        let fields = vec![
            FieldValue::with_values("field2".to_string(), "value2".to_string()),
            FieldValue::with_values("field3".to_string(), "value3".to_string()),
        ];
        storage.hmset("hash1", &fields).unwrap();

        // Verify multiple fields
        let field_names = vec!["field2".to_string(), "field3".to_string()];
        let results = storage.hmget("hash1", &field_names).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value, "value2");
        assert_eq!(results[1].value, "value3");
    }

    #[test]
    fn test_version_control() {
        let (_temp_dir, storage) = setup();

        // Test version increment
        let key = "versioned_key";
        let initial_version = storage.get_version(key).unwrap();

        storage.set(key, "value1").unwrap();
        let version1 = storage.get_version(key).unwrap();
        assert!(version1 > initial_version);

        storage.set(key, "value2").unwrap();
        let version2 = storage.get_version(key).unwrap();
        assert!(version2 > version1);
    }

    #[test]
    fn test_score_operations() {
        let (_temp_dir, storage) = setup();

        // Test score set and get
        let members = vec![
            ScoreMember {
                score: 1.0,
                member: "member1".to_string(),
            },
            ScoreMember {
                score: 2.0,
                member: "member2".to_string(),
            },
        ];

        storage.zadd("zset1", &members).unwrap();

        // Test score range
        let results = storage.zrange_by_score("zset1", 1.0, 2.0).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].member, "member1");
        assert_eq!(results[1].member, "member2");
    }
}