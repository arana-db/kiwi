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
use std::time::Duration;
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
    fn test_list_basic_operations() {
        let (_temp_dir, storage) = setup();

        // Test push operations
        storage.lpush("list1", "value1").unwrap();
        storage.rpush("list1", "value2").unwrap();

        // Test list length
        assert_eq!(storage.llen("list1").unwrap(), 2);

        // Test pop operations
        let left = storage.lpop("list1").unwrap();
        let right = storage.rpop("list1").unwrap();
        assert_eq!(left, Some("value1".to_string()));
        assert_eq!(right, Some("value2".to_string()));

        // Test empty list
        assert_eq!(storage.llen("list1").unwrap(), 0);
    }

    #[test]
    fn test_list_range_operations() {
        let (_temp_dir, storage) = setup();

        // Prepare test data
        for i in 0..5 {
            storage.rpush("list1", format!("value{}", i)).unwrap();
        }

        // Test range query
        let range = storage.lrange("list1", 1, 3).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], "value1");
        assert_eq!(range[2], "value3");

        // Test negative indices
        let range = storage.lrange("list1", -3, -1).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], "value2");
        assert_eq!(range[2], "value4");
    }

    #[test]
    fn test_list_trim_operations() {
        let (_temp_dir, storage) = setup();

        // Prepare test data
        for i in 0..10 {
            storage.rpush("list1", format!("value{}", i)).unwrap();
        }

        // Test trim operation
        storage.ltrim("list1", 2, 5).unwrap();
        
        // Verify trimmed list
        let range = storage.lrange("list1", 0, -1).unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0], "value2");
        assert_eq!(range[3], "value5");
    }

    #[test]
    fn test_list_blocking_operations() {
        let (_temp_dir, storage) = setup();

        // Test blocking pop with timeout
        let timeout = Duration::from_secs(1);
        let result = storage.blpop("empty_list", timeout).unwrap();
        assert_eq!(result, None);

        // Push value and test blocking pop
        storage.rpush("list1", "value1").unwrap();
        let result = storage.blpop("list1", timeout).unwrap();
        assert_eq!(result, Some(("list1".to_string(), "value1".to_string())));
    }

    #[test]
    fn test_list_insert_operations() {
        let (_temp_dir, storage) = setup();

        // Prepare test data
        storage.rpush("list1", "value1").unwrap();
        storage.rpush("list1", "value3").unwrap();

        // Test insert before
        storage.linsert("list1", "value3", "value2", true).unwrap();
        
        // Verify insertion
        let range = storage.lrange("list1", 0, -1).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[1], "value2");

        // Test insert after
        storage.linsert("list1", "value3", "value4", false).unwrap();
        
        // Verify insertion
        let range = storage.lrange("list1", 0, -1).unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[3], "value4");
    }
}