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
use std::collections::HashMap;
use parking_lot::{RwLock, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

/// 分片锁管理器 - 减少锁竞争
pub struct ShardedLockManager {
    locks: Vec<Arc<RwLock<()>>>,
    shard_count: usize,
}

impl ShardedLockManager {
    pub fn new(shard_count: usize) -> Self {
        let mut locks = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            locks.push(Arc::new(RwLock::new(())));
        }
        
        Self {
            locks,
            shard_count,
        }
    }
    
    pub fn get_lock(&self, key: &str) -> Arc<RwLock<()>> {
        let hash = self.hash_key(key);
        let index = hash % self.shard_count;
        Arc::clone(&self.locks[index])
    }
    
    fn hash_key(&self, key: &str) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }
}

/// 高性能统计收集器
pub struct StatisticsCollector {
    operations: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    memory_usage: AtomicU64,
    read_operations: AtomicU64,
    write_operations: AtomicU64,
    error_count: AtomicU64,
    avg_response_time: AtomicU64, // 存储为微秒
    max_response_time: AtomicU64,
    min_response_time: AtomicU64,
}

impl StatisticsCollector {
    pub fn new() -> Self {
        Self {
            operations: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            memory_usage: AtomicU64::new(0),
            read_operations: AtomicU64::new(0),
            write_operations: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            avg_response_time: AtomicU64::new(0),
            max_response_time: AtomicU64::new(0),
            min_response_time: AtomicU64::new(u64::MAX),
        }
    }
    
    pub fn increment_operations(&self) {
        self.operations.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_read_operation(&self) {
        self.read_operations.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_write_operation(&self) {
        self.write_operations.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_response_time(&self, duration_micros: u64) {
        // 更新平均响应时间（简化实现）
        let current_avg = self.avg_response_time.load(Ordering::Relaxed);
        let operations = self.operations.load(Ordering::Relaxed);
        if operations > 0 {
            let new_avg = ((current_avg * (operations - 1)) + duration_micros) / operations;
            self.avg_response_time.store(new_avg, Ordering::Relaxed);
        }
        
        // 更新最大响应时间
        let current_max = self.max_response_time.load(Ordering::Relaxed);
        if duration_micros > current_max {
            self.max_response_time.store(duration_micros, Ordering::Relaxed);
        }
        
        // 更新最小响应时间
        let current_min = self.min_response_time.load(Ordering::Relaxed);
        if duration_micros < current_min {
            self.min_response_time.store(duration_micros, Ordering::Relaxed);
        }
    }
    
    pub fn get_statistics(&self) -> Statistics {
        Statistics {
            operations: self.operations.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            memory_usage: self.memory_usage.load(Ordering::Relaxed),
            read_operations: self.read_operations.load(Ordering::Relaxed),
            write_operations: self.write_operations.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            avg_response_time: self.avg_response_time.load(Ordering::Relaxed) as f64,
            max_response_time: self.max_response_time.load(Ordering::Relaxed),
            min_response_time: self.min_response_time.load(Ordering::Relaxed),
        }
    }
}

/// 统计信息结构
#[derive(Debug, Clone)]
pub struct Statistics {
    pub operations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub memory_usage: u64,
    pub read_operations: u64,
    pub write_operations: u64,
    pub error_count: u64,
    pub avg_response_time: f64, // 微秒
    pub max_response_time: u64, // 微秒
    pub min_response_time: u64, // 微秒
}

/// 线程安全的键值存储
pub struct ConcurrentHashMap<K, V> {
    shards: Vec<Arc<RwLock<HashMap<K, V>>>>,
    shard_count: usize,
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: Clone + std::hash::Hash + Eq + Send + Sync,
    V: Clone + Send + Sync,
{
    pub fn new(shard_count: usize) -> Self {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Arc::new(RwLock::new(HashMap::new())));
        }
        
        Self {
            shards,
            shard_count,
        }
    }
    
    pub fn get(&self, key: &K) -> Option<V> {
        let shard_index = self.get_shard_index(key);
        let shard = &self.shards[shard_index];
        shard.read().get(key).cloned()
    }
    
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let shard_index = self.get_shard_index(&key);
        let shard = &self.shards[shard_index];
        shard.write().insert(key, value)
    }
    
    pub fn remove(&self, key: &K) -> Option<V> {
        let shard_index = self.get_shard_index(key);
        let shard = &self.shards[shard_index];
        shard.write().remove(key)
    }
    
    fn get_shard_index(&self, key: &K) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shard_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharded_lock_manager() {
        let manager = ShardedLockManager::new(16);
        
        let lock1 = manager.get_lock("key1");
        let lock2 = manager.get_lock("key2");
        let lock3 = manager.get_lock("key1"); // 应该得到相同的锁
        
        // 测试锁的获取
        assert!(Arc::ptr_eq(&lock1, &lock3));
        assert!(!Arc::ptr_eq(&lock1, &lock2));
    }
    
    #[test]
    fn test_statistics_collector() {
        let collector = StatisticsCollector::new();
        
        collector.increment_operations();
        collector.record_cache_hit();
        collector.record_read_operation();
        collector.record_response_time(1000);
        
        let stats = collector.get_statistics();
        assert_eq!(stats.operations, 1);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.read_operations, 1);
        assert_eq!(stats.max_response_time, 1000);
    }
    
    #[test]
    fn test_concurrent_hash_map() {
        let map = ConcurrentHashMap::new(4);
        
        map.insert("key1".to_string(), "value1".to_string());
        map.insert("key2".to_string(), "value2".to_string());
        
        assert_eq!(map.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(map.get(&"key2".to_string()), Some("value2".to_string()));
        assert_eq!(map.get(&"key3".to_string()), None);
        
        let removed = map.remove(&"key1".to_string());
        assert_eq!(removed, Some("value1".to_string()));
        assert_eq!(map.get(&"key1".to_string()), None);
    }
} 