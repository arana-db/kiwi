// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use parking_lot::RwLock;
use lru::LruCache;
use std::collections::HashMap;
use crate::error::Result;

/// 缓存值结构
#[derive(Debug, Clone)]
pub struct CachedValue {
    pub data: Vec<u8>,
    pub timestamp: u64,
    pub access_count: u32,
    pub ttl: Option<u64>,
    pub dtype: crate::value_utils::DataType,
}

impl CachedValue {
    pub fn new(data: Vec<u8>, ttl: Option<u64>, dtype: crate::value_utils::DataType) -> Self {
        Self {
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            access_count: 1,
            ttl,
            dtype,
        }
    }
    
    pub fn is_expired(&self) -> bool {
        if let Some(expire_time) = self.ttl {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            current_time >= expire_time
        } else {
            false
        }
    }
    
    pub fn increment_access(&mut self) {
        self.access_count += 1;
    }
}

/// 布隆过滤器（简化实现）
pub struct BloomFilter {
    bits: Vec<bool>,
    size: usize,
    hash_count: usize,
}

impl BloomFilter {
    pub fn new(size: usize, false_positive_rate: f64) -> Self {
        let hash_count = (-false_positive_rate.ln() / 2.0_f64.ln()).ceil() as usize;
        
        Self {
            bits: vec![false; size],
            size,
            hash_count,
        }
    }
    
    pub fn insert(&mut self, key: &str) {
        for i in 0..self.hash_count {
            let hash = self.hash(key, i);
            let index = hash % self.size;
            self.bits[index] = true;
        }
    }
    
    pub fn may_contain(&self, key: &str) -> bool {
        for i in 0..self.hash_count {
            let hash = self.hash(key, i);
            let index = hash % self.size;
            if !self.bits[index] {
                return false;
            }
        }
        true
    }
    
    fn hash(&self, key: &str, seed: usize) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize
    }
}

/// 多级缓存系统
pub struct MultiLevelCache {
    l1_cache: Arc<RwLock<LruCache<String, CachedValue>>>, // 热点数据
    l2_cache: Arc<RwLock<LruCache<String, CachedValue>>>, // 温数据
    bloom_filter: Arc<RwLock<BloomFilter>>, // 布隆过滤器
    stats: Arc<RwLock<CacheStatistics>>,
}

/// 缓存统计
#[derive(Debug, Clone, Default)]
pub struct CacheStatistics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub memory_usage: u64,
    pub bloom_checks: u64,
    pub bloom_false_positives: u64,
}

impl MultiLevelCache {
    pub fn new(l1_capacity: usize, l2_capacity: usize) -> Self {
        Self {
            l1_cache: Arc::new(RwLock::new(LruCache::new(l1_capacity))),
            l2_cache: Arc::new(RwLock::new(LruCache::new(l2_capacity))),
            bloom_filter: Arc::new(RwLock::new(BloomFilter::new(1000000, 0.01))),
            stats: Arc::new(RwLock::new(CacheStatistics::default())),
        }
    }
    
    pub fn get(&self, key: &str) -> Option<CachedValue> {
        // 先检查L1缓存
        if let Some(value) = self.l1_cache.read().get(key).cloned() {
            if !value.is_expired() {
                self.record_hit();
                return Some(value);
            }
        }
        
        // 检查L2缓存
        if let Some(value) = self.l2_cache.read().get(key).cloned() {
            if !value.is_expired() {
                // 提升到L1缓存
                let mut l1 = self.l1_cache.write();
                l1.put(key.to_string(), value.clone());
                self.record_hit();
                return Some(value);
            }
        }
        
        self.record_miss();
        None
    }
    
    pub fn put(&self, key: String, value: CachedValue) {
        // 先放入L1缓存
        let mut l1 = self.l1_cache.write();
        l1.put(key.clone(), value.clone());
        
        // 同时更新布隆过滤器
        self.bloom_filter.write().insert(&key);
    }
    
    pub fn remove(&self, key: &str) {
        self.l1_cache.write().pop(key);
        self.l2_cache.write().pop(key);
    }
    
    pub fn clear(&self) {
        self.l1_cache.write().clear();
        self.l2_cache.write().clear();
        self.bloom_filter.write() = BloomFilter::new(1000000, 0.01);
    }
    
    fn record_hit(&self) {
        let mut stats = self.stats.write();
        stats.hits += 1;
    }
    
    fn record_miss(&self) {
        let mut stats = self.stats.write();
        stats.misses += 1;
    }
    
    pub fn get_statistics(&self) -> CacheStatistics {
        self.stats.read().clone()
    }
    
    pub fn may_contain(&self, key: &str) -> bool {
        let mut stats = self.stats.write();
        stats.bloom_checks += 1;
        
        let result = self.bloom_filter.read().may_contain(key);
        if !result {
            stats.bloom_false_positives += 1;
        }
        
        result
    }
}

/// 缓存管理器
pub struct CacheManager {
    cache: Arc<MultiLevelCache>,
    config: CacheConfig,
}

/// 缓存配置
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub l1_capacity: usize,
    pub l2_capacity: usize,
    pub bloom_filter_size: usize,
    pub bloom_false_positive_rate: f64,
    pub enable_bloom_filter: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            l1_capacity: 100_000,
            l2_capacity: 1_000_000,
            bloom_filter_size: 10_000_000,
            bloom_false_positive_rate: 0.01,
            enable_bloom_filter: true,
        }
    }
}

impl CacheManager {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            cache: Arc::new(MultiLevelCache::new(config.l1_capacity, config.l2_capacity)),
            config,
        }
    }
    
    pub fn get(&self, key: &str) -> Option<CachedValue> {
        // 如果启用布隆过滤器，先检查
        if self.config.enable_bloom_filter && !self.cache.may_contain(key) {
            return None;
        }
        
        self.cache.get(key)
    }
    
    pub fn put(&self, key: String, value: CachedValue) {
        self.cache.put(key, value);
    }
    
    pub fn remove(&self, key: &str) {
        self.cache.remove(key);
    }
    
    pub fn get_statistics(&self) -> CacheStatistics {
        self.cache.get_statistics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_value() {
        let value = CachedValue::new(
            b"test data".to_vec(),
            Some(1000),
            crate::value_utils::DataType::String,
        );
        
        assert_eq!(value.access_count, 1);
        assert!(!value.is_expired());
        
        let mut value = value;
        value.increment_access();
        assert_eq!(value.access_count, 2);
    }
    
    #[test]
    fn test_bloom_filter() {
        let mut filter = BloomFilter::new(1000, 0.01);
        
        filter.insert("key1");
        filter.insert("key2");
        
        assert!(filter.may_contain("key1"));
        assert!(filter.may_contain("key2"));
        assert!(!filter.may_contain("key3"));
    }
    
    #[test]
    fn test_multi_level_cache() {
        let cache = MultiLevelCache::new(10, 20);
        
        let value = CachedValue::new(
            b"test".to_vec(),
            None,
            crate::value_utils::DataType::String,
        );
        
        cache.put("key1".to_string(), value.clone());
        
        let retrieved = cache.get("key1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data, b"test");
        
        let stats = cache.get_statistics();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }
    
    #[test]
    fn test_cache_manager() {
        let config = CacheConfig::default();
        let manager = CacheManager::new(config);
        
        let value = CachedValue::new(
            b"test".to_vec(),
            None,
            crate::value_utils::DataType::String,
        );
        
        manager.put("key1".to_string(), value.clone());
        
        let retrieved = manager.get("key1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data, b"test");
    }
} 