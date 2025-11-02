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

use parking_lot::{Mutex, MutexGuard};
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

/// Lock manager backed by a fixed pool of mutex shards.
///
/// Keys are hashed to a shard index so that we can protect hot keys without
/// allocating per-key mutexes. The implementation mirrors the high-performance
/// approach used by kvrocks: lock acquisition is fully blocking and relies on
/// consistent ordering to avoid deadlocks when locking multiple keys.
pub struct LockMgr {
    mask: usize,
    mutex_pool: Vec<Mutex<()>>,
}

impl LockMgr {
    /// Create a new lock manager with at least `num_shards` mutexes.
    /// The actual number of mutexes is rounded up to the next power of two so
    /// that we can use an inexpensive bit-mask when hashing keys.
    pub fn new(num_shards: usize) -> Self {
        let size = num_shards.max(1).next_power_of_two();
        let mutex_pool = (0..size).map(|_| Mutex::new(())).collect();
        Self {
            mask: size - 1,
            mutex_pool,
        }
    }

    /// Total number of mutex shards managed by this instance.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.mutex_pool.len()
    }

    /// Acquire the lock for `key`, blocking until it becomes available.
    pub fn lock(&self, key: &str) -> ScopedLock<'_> {
        let index = self.index_for(key);
        let guard = self.mutex_pool[index].lock();
        ScopedLock { guard: Some(guard) }
    }

    /// Attempt to acquire the lock for `key` without blocking.
    pub fn try_lock(&self, key: &str) -> Option<ScopedLock<'_>> {
        let index = self.index_for(key);
        self.mutex_pool[index]
            .try_lock()
            .map(|guard| ScopedLock { guard: Some(guard) })
    }

    /// Acquire locks for multiple keys using a deterministic order to avoid
    /// deadlocks. Duplicate keys are ignored.
    pub fn multi_lock<'a, I, K>(&'a self, keys: I) -> ScopedMultiLock<'a>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let indexes = self.sorted_indexes(keys);
        let mut guards = Vec::with_capacity(indexes.len());
        for index in indexes {
            guards.push(self.mutex_pool[index].lock());
        }
        ScopedMultiLock { guards }
    }

    /// Attempt to acquire multiple locks without blocking. Returns `None` as
    /// soon as any lock is unavailable and releases all previously acquired
    /// locks.
    pub fn try_multi_lock<'a, I, K>(&'a self, keys: I) -> Option<ScopedMultiLock<'a>>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let indexes = self.sorted_indexes(keys);
        let mut guards = Vec::with_capacity(indexes.len());
        for index in indexes {
            match self.mutex_pool[index].try_lock() {
                Some(guard) => guards.push(guard),
                None => return None,
            }
        }
        Some(ScopedMultiLock { guards })
    }

    #[inline]
    fn index_for(&self, key: &str) -> usize {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & self.mask
    }

    fn sorted_indexes<I, K>(&self, keys: I) -> Vec<usize>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let mut indexes = BTreeSet::new();
        for key in keys {
            indexes.insert(self.index_for(key.as_ref()));
        }
        indexes.into_iter().collect()
    }
}

/// RAII guard for a single mutex shard.
///
/// Dropping the guard releases the underlying mutex.
pub struct ScopedLock<'a> {
    guard: Option<MutexGuard<'a, ()>>,
}

impl<'a> ScopedLock<'a> {
    /// Returns `true` if the guard currently owns a lock.
    pub fn is_locked(&self) -> bool {
        self.guard.is_some()
    }

    /// Consume the guard and return the underlying `MutexGuard`.
    #[allow(dead_code)]
    pub fn into_inner(mut self) -> MutexGuard<'a, ()> {
        self.guard.take().expect("guard already taken")
    }
}

impl<'a> Drop for ScopedLock<'a> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            drop(guard);
        }
    }
}

/// RAII guard for multiple mutex shards.
pub struct ScopedMultiLock<'a> {
    guards: Vec<MutexGuard<'a, ()>>,
}

impl<'a> ScopedMultiLock<'a> {
    pub fn len(&self) -> usize {
        self.guards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.guards.is_empty()
    }
}

impl<'a> Drop for ScopedMultiLock<'a> {
    fn drop(&mut self) {
        for guard in self.guards.drain(..).rev() {
            drop(guard);
        }
    }
}

/// Public RAII wrapper used by storage commands.
pub struct ScopeRecordLock<'a> {
    guard: Option<ScopedLock<'a>>,
}

impl<'a> ScopeRecordLock<'a> {
    pub fn new(mgr: &'a LockMgr, key: &str) -> Self {
        Self {
            guard: Some(mgr.lock(key)),
        }
    }

    pub fn try_new(mgr: &'a LockMgr, key: &str) -> Option<Self> {
        mgr.try_lock(key).map(|guard| Self { guard: Some(guard) })
    }

    pub fn is_locked(&self) -> bool {
        self.guard.is_some()
    }
}

impl<'a> Drop for ScopeRecordLock<'a> {
    fn drop(&mut self) {
        self.guard.take();
    }
}

/// RAII helper for acquiring multiple locks in a single scope.
pub struct ScopeRecordMultiLock<'a> {
    guard: Option<ScopedMultiLock<'a>>,
}

impl<'a> ScopeRecordMultiLock<'a> {
    pub fn new<I, K>(mgr: &'a LockMgr, keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        Self {
            guard: Some(mgr.multi_lock(keys)),
        }
    }

    pub fn try_new<I, K>(mgr: &'a LockMgr, keys: I) -> Option<Self>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        mgr.try_multi_lock(keys)
            .map(|guard| Self { guard: Some(guard) })
    }

    pub fn is_locked(&self) -> bool {
        self.guard.is_some()
    }

    pub fn len(&self) -> usize {
        self.guard.as_ref().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> Drop for ScopeRecordMultiLock<'a> {
    fn drop(&mut self) {
        self.guard.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, AtomicI64, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_lock_unlock() {
        let mgr = LockMgr::new(4);
        {
            let _guard = mgr.lock("test_key");
            assert!(mgr.try_lock("test_key").is_none());
        }
        assert!(mgr.try_lock("test_key").is_some());
    }

    #[test]
    fn test_try_lock_success() {
        let mgr = LockMgr::new(4);
        let guard = mgr.try_lock("test_key");
        assert!(guard.is_some());
    }

    #[test]
    fn test_try_lock_already_locked() {
        let mgr = LockMgr::new(4);
        let first = mgr.try_lock("test_key");
        assert!(first.is_some());
        assert!(mgr.try_lock("test_key").is_none());
    }

    #[test]
    fn test_scope_record_lock() {
        let mgr = LockMgr::new(4);

        {
            let lock = ScopeRecordLock::new(&mgr, "test_key");
            assert!(lock.is_locked());
            assert!(ScopeRecordLock::try_new(&mgr, "test_key").is_none());
        }

        assert!(ScopeRecordLock::try_new(&mgr, "test_key").is_some());
    }

    #[test]
    fn test_concurrent_access() {
        let mgr = Arc::new(LockMgr::new(8));
        let counter = Arc::new(AtomicI64::new(0));
        let key = "shared";

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let mgr = mgr.clone();
                let counter = counter.clone();
                let key = key.to_string();
                thread::spawn(move || {
                    let _lock = ScopeRecordLock::new(&mgr, &key);
                    if _lock.is_locked() {
                        let current = counter.load(Ordering::Acquire);
                        thread::sleep(Duration::from_millis(1));
                        counter.store(current + 1, Ordering::Release);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::Acquire), 10);
    }

    #[test]
    fn test_different_shards() {
        let mgr = LockMgr::new(4);
        let keys = ["key1", "key2", "key3", "key4"];
        let locks: Vec<_> = keys.iter().filter_map(|key| mgr.try_lock(key)).collect();
        assert!(!locks.is_empty());
    }

    #[test]
    fn test_edge_cases() {
        let mgr = LockMgr::new(1);
        assert!(mgr.try_lock("").is_some());
        let long_key = "a".repeat(1024);
        assert!(mgr.try_lock(&long_key).is_some());
    }

    #[test]
    fn test_multiple_threads_same_key_contention() {
        let mgr = Arc::new(LockMgr::new(4));
        let execution_order = Arc::new(Mutex::new(Vec::new()));
        let counter = Arc::new(AtomicI32::new(0));

        let handles: Vec<_> = (0..5)
            .map(|thread_id| {
                let mgr = mgr.clone();
                let order = execution_order.clone();
                let counter = counter.clone();
                let key = "contested".to_string();
                thread::spawn(move || {
                    let lock = ScopeRecordLock::new(&mgr, &key);
                    assert!(lock.is_locked());
                    {
                        let mut order = order.lock();
                        order.push(thread_id);
                    }
                    let current = counter.load(Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(10));
                    counter.store(current + 1, Ordering::SeqCst);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 5);
        let order = execution_order.lock();
        assert_eq!(order.len(), 5);
    }

    #[test]
    fn test_try_lock_contention() {
        let mgr = Arc::new(LockMgr::new(4));
        let key = "try_lock";
        let success = Arc::new(AtomicI64::new(0));
        let failure = Arc::new(AtomicI64::new(0));

        let _first = mgr.try_lock(key);
        assert!(_first.is_some());

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let mgr = mgr.clone();
                let success = success.clone();
                let failure = failure.clone();
                let key = key.to_string();
                thread::spawn(move || {
                    if mgr.try_lock(&key).is_some() {
                        success.fetch_add(1, Ordering::SeqCst);
                    } else {
                        failure.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        thread::sleep(Duration::from_millis(50));
        drop(_first);

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            success.load(Ordering::SeqCst) + failure.load(Ordering::SeqCst),
            10
        );
    }

    #[test]
    fn test_scope_lock_survives_panic() {
        let mgr = Arc::new(LockMgr::new(4));
        let key = "panic".to_string();

        let mgr_clone = mgr.clone();
        let key_clone = key.clone();
        let handle = thread::spawn(move || {
            let lock = ScopeRecordLock::new(&mgr_clone, &key_clone);
            assert!(lock.is_locked());
            panic!("simulated panic");
        });

        assert!(handle.join().is_err());

        thread::sleep(Duration::from_millis(10));
        assert!(mgr.try_lock(&key).is_some());
    }

    #[test]
    fn test_scope_multi_lock() {
        let mgr = LockMgr::new(8);
        let guard = ScopeRecordMultiLock::new(&mgr, ["a", "b", "c", "a"]);
        assert!(guard.is_locked());
        assert_eq!(guard.len(), 3);

        // second attempt should fail while guard is held
        assert!(ScopeRecordMultiLock::try_new(&mgr, ["a", "c"]).is_none());
    }

    #[test]
    fn test_multi_lock_release() {
        let mgr = LockMgr::new(8);
        {
            let _guard = ScopeRecordMultiLock::new(&mgr, ["x", "y"]);
        }
        assert!(ScopeRecordMultiLock::try_new(&mgr, ["x", "y"]).is_some());
    }

    #[test]
    fn test_parallel_try_multi_lock() {
        let mgr = Arc::new(LockMgr::new(16));
        let counter = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let mgr = mgr.clone();
                let counter = counter.clone();
                thread::spawn(move || {
                    if let Some(_guard) = ScopeRecordMultiLock::try_new(&mgr, ["p", "q"]) {
                        counter.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(50));
                        // guard is only released when it leaves the scope
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
