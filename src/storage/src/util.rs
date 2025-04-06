//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

//! Utility functions and data structures for the storage engine

use lru::LruCache;
use murmur3::murmur3_32;
use std::hash::Hasher;
use std::num::Wrapping;
use std::sync::Arc;
use parking_lot::Mutex;

/// Thread-safe LRU cache implementation
pub struct Cache<K, V> {
    inner: Arc<Mutex<LruCache<K, V>>>,
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone> Cache<K, V> {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LruCache::new(cap))),
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.lock().get(key).cloned()
    }

    pub fn put(&self, key: K, value: V) -> Option<V> {
        self.inner.lock().put(key, value)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.inner.lock().pop(key)
    }

    pub fn clear(&self) {
        self.inner.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

/// MurmurHash implementation
pub fn murmurhash(data: &[u8], seed: u32) -> u32 {
    murmur3_32(data, seed)
}

/// Utility functions for byte manipulation
pub mod bytes {
    use std::mem;

    pub fn copy_bytes<T: Copy>(value: &T) -> Vec<u8> {
        unsafe {
            let ptr = value as *const T as *const u8;
            std::slice::from_raw_parts(ptr, mem::size_of::<T>()).to_vec()
        }
    }

    pub fn read_bytes<T: Copy>(bytes: &[u8]) -> Option<T> {
        if bytes.len() != mem::size_of::<T>() {
            return None;
        }
        unsafe {
            let mut value = mem::MaybeUninit::uninit();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                value.as_mut_ptr() as *mut u8,
                mem::size_of::<T>(),
            );
            Some(value.assume_init())
        }
    }
}

/// CRC64 implementation
pub mod crc64 {
    const POLY: u64 = 0xC96C5795D7870F42;
    static mut TABLE: [u64; 256] = [0; 256];
    static INITIALIZED: std::sync::Once = std::sync::Once::new();

    fn init_table() {
        for i in 0..256 {
            let mut crc = i as u64;
            for _ in 0..8 {
                if crc & 1 == 1 {
                    crc = (crc >> 1) ^ POLY;
                } else {
                    crc >>= 1;
                }
            }
            unsafe {
                TABLE[i] = crc;
            }
        }
    }

    pub fn checksum(data: &[u8]) -> u64 {
        INITIALIZED.call_once(init_table);
        let mut crc = 0u64;
        for &b in data {
            unsafe {
                crc = TABLE[((crc as u8) ^ b) as usize] ^ (crc >> 8);
            }
        }
        crc
    }
}