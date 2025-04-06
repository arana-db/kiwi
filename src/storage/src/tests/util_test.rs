//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

use super::util::*;
use std::thread;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic_operations() {
        let cache = Cache::new(5);
        
        // Test put and get
        cache.put("key1", "value1");
        assert_eq!(cache.get(&"key1"), Some("value1".to_string()));
        
        // Test remove
        cache.remove(&"key1");
        assert_eq!(cache.get(&"key1"), None);
        
        // Test clear
        cache.put("key2", "value2");
        cache.clear();
        assert!(cache.is_empty());
        
        // Test capacity limit
        for i in 0..10 {
            cache.put(format!("key{}", i), format!("value{}", i));
        }
        assert_eq!(cache.len(), 5);
    }

    #[test]
    fn test_cache_concurrent_access() {
        let cache = Arc::new(Cache::new(100));
        let mut handles = vec![];

        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key{}_{}", i, j);
                    let value = format!("value{}_{}", i, j);
                    cache_clone.put(key.clone(), value.clone());
                    assert_eq!(cache_clone.get(&key), Some(value));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_murmurhash() {
        let data = b"Hello, World!";
        let seed = 42;
        let hash = murmurhash(data, seed);
        assert_ne!(hash, 0);
        
        // Test consistency
        let hash2 = murmurhash(data, seed);
        assert_eq!(hash, hash2);
        
        // Test different seeds
        let hash3 = murmurhash(data, 43);
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_bytes_manipulation() {
        // Test copy_bytes
        let value: u32 = 42;
        let bytes = bytes::copy_bytes(&value);
        assert_eq!(bytes.len(), std::mem::size_of::<u32>());

        // Test read_bytes
        let read_value: Option<u32> = bytes::read_bytes(&bytes);
        assert_eq!(read_value, Some(42));

        // Test invalid read
        let invalid_bytes = vec![1, 2, 3]; // Wrong size for u32
        let invalid_read: Option<u32> = bytes::read_bytes(&invalid_bytes);
        assert_eq!(invalid_read, None);
    }

    #[test]
    fn test_crc64() {
        let data = b"Hello, World!";
        let checksum = crc64::checksum(data);
        assert_ne!(checksum, 0);

        // Test consistency
        let checksum2 = crc64::checksum(data);
        assert_eq!(checksum, checksum2);

        // Test different data
        let data2 = b"Hello, World!!";
        let checksum3 = crc64::checksum(data2);
        assert_ne!(checksum, checksum3);
    }
}