//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

use std::sync::Arc;

/// SlotIndexer is responsible for mapping keys to specific database instances
/// This is the Rust implementation of slot_indexer.h in C++ version
pub struct SlotIndexer {
    // Number of instances
    instance_num: usize,
}

impl SlotIndexer {
    /// Create a new SlotIndexer instance
    pub fn new(instance_num: usize) -> Self {
        Self { instance_num }
    }

    /// Get the instance index for a given key
    pub fn get_instance(&self, key: &[u8]) -> usize {
        if self.instance_num <= 1 {
            return 0;
        }

        // Calculate the hash value of the key and take modulo to determine the instance index
        let hash = self.hash(key);
        (hash % self.instance_num as u64) as usize
    }

    /// Calculate the hash value of a key
    fn hash(&self, key: &[u8]) -> u64 {
        // Use a simple hash algorithm, can be replaced with a more complex algorithm if needed
        let mut hash: u64 = 5381;
        for &byte in key {
            hash = ((hash << 5) + hash) + byte as u64; // hash * 33 + byte
        }
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_instance() {
        let indexer = SlotIndexer::new(3);
        
        // Test that different keys should map to different instances
        let key1 = b"key1";
        let key2 = b"key2";
        let key3 = b"key3";
        
        let instance1 = indexer.get_instance(key1);
        let instance2 = indexer.get_instance(key2);
        let instance3 = indexer.get_instance(key3);
        
        // Verify that instance indices are within valid range
        assert!(instance1 < 3);
        assert!(instance2 < 3);
        assert!(instance3 < 3);
        
        // The same key should always map to the same instance
        assert_eq!(instance1, indexer.get_instance(key1));
        assert_eq!(instance2, indexer.get_instance(key2));
        assert_eq!(instance3, indexer.get_instance(key3));
    }
    
    #[test]
    fn test_single_instance() {
        let indexer = SlotIndexer::new(1);
        
        // When there is only one instance, all keys should map to index 0
        assert_eq!(0, indexer.get_instance(b"key1"));
        assert_eq!(0, indexer.get_instance(b"key2"));
        assert_eq!(0, indexer.get_instance(b"key3"));
    }
}