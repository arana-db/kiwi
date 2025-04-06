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
use std::collections::HashSet;
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
    fn test_set_basic_operations() {
        let (_temp_dir, storage) = setup();

        // Test add members
        storage.sadd("set1", "member1").unwrap();
        storage.sadd("set1", "member2").unwrap();
        storage.sadd("set1", "member3").unwrap();

        // Test set cardinality
        assert_eq!(storage.scard("set1").unwrap(), 3);

        // Test membership
        assert!(storage.sismember("set1", "member1").unwrap());
        assert!(!storage.sismember("set1", "nonexistent").unwrap());

        // Test remove members
        storage.srem("set1", "member1").unwrap();
        assert!(!storage.sismember("set1", "member1").unwrap());
    }

    #[test]
    fn test_set_operations() {
        let (_temp_dir, storage) = setup();

        // Prepare test sets
        storage.sadd("set1", "a").unwrap();
        storage.sadd("set1", "b").unwrap();
        storage.sadd("set1", "c").unwrap();

        storage.sadd("set2", "b").unwrap();
        storage.sadd("set2", "c").unwrap();
        storage.sadd("set2", "d").unwrap();

        // Test intersection
        let inter = storage.sinter(&["set1", "set2"]).unwrap();
        assert_eq!(inter.len(), 2);
        assert!(inter.contains("b"));
        assert!(inter.contains("c"));

        // Test union
        let union = storage.sunion(&["set1", "set2"]).unwrap();
        assert_eq!(union.len(), 4);
        assert!(union.contains("a"));
        assert!(union.contains("d"));

        // Test difference
        let diff = storage.sdiff(&["set1", "set2"]).unwrap();
        assert_eq!(diff.len(), 1);
        assert!(diff.contains("a"));
    }

    #[test]
    fn test_set_store_operations() {
        let (_temp_dir, storage) = setup();

        // Prepare test sets
        storage.sadd("set1", "a").unwrap();
        storage.sadd("set1", "b").unwrap();
        storage.sadd("set2", "b").unwrap();
        storage.sadd("set2", "c").unwrap();

        // Test intersection store
        storage.sinterstore("result1", &["set1", "set2"]).unwrap();
        assert_eq!(storage.scard("result1").unwrap(), 1);
        assert!(storage.sismember("result1", "b").unwrap());

        // Test union store
        storage.sunionstore("result2", &["set1", "set2"]).unwrap();
        assert_eq!(storage.scard("result2").unwrap(), 3);

        // Test difference store
        storage.sdiffstore("result3", &["set1", "set2"]).unwrap();
        assert_eq!(storage.scard("result3").unwrap(), 1);
        assert!(storage.sismember("result3", "a").unwrap());
    }

    #[test]
    fn test_set_random_operations() {
        let (_temp_dir, storage) = setup();

        // Prepare test set
        for i in 0..10 {
            storage.sadd("set1", format!("member{}", i)).unwrap();
        }

        // Test random member
        let member = storage.srandmember("set1").unwrap();
        assert!(member.is_some());
        assert!(storage.sismember("set1", member.unwrap()).unwrap());

        // Test pop member
        let popped = storage.spop("set1").unwrap();
        assert!(popped.is_some());
        assert!(!storage.sismember("set1", popped.unwrap()).unwrap());
        assert_eq!(storage.scard("set1").unwrap(), 9);
    }
}