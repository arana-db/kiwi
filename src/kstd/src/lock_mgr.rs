/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use super::status::Status;
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Condvar, Mutex,
    },
};

struct LockMapShard {
    mutex: Mutex<HashSet<String>>,
    condvar: Condvar,
}

impl LockMapShard {
    fn new() -> Self {
        Self {
            mutex: Mutex::new(HashSet::new()),
            condvar: Condvar::new(),
        }
    }
}

struct LockMap {
    shards: Vec<Arc<LockMapShard>>,
    lock_cnt: AtomicI64,
    max_locks: i64, // -1 means no limit
}

impl LockMap {
    fn new(num_shards: usize, max_locks: i64) -> Self {
        Self {
            shards: (0..num_shards)
                .map(|_| Arc::new(LockMapShard::new()))
                .collect(),
            lock_cnt: AtomicI64::new(0),
            max_locks,
        }
    }

    #[inline]
    fn shard_for(&self, key: &str) -> &Arc<LockMapShard> {
        // use ahash
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        &self.shards[hasher.finish() as usize % self.shards.len()]
    }

    #[inline]
    fn has_quota(&self) -> bool {
        if self.max_locks <= 0 {
            return true;
        }
        self.lock_cnt.load(Ordering::Acquire) < self.max_locks
    }
}

pub struct LockMgr {
    map: Arc<LockMap>,
}

impl LockMgr {
    pub fn new(num_shards: usize) -> Self {
        Self::with_max_locks(num_shards, -1)
    }

    pub fn with_max_locks(num_shards: usize, max_locks: i64) -> Self {
        Self {
            map: Arc::new(LockMap::new(num_shards, max_locks)),
        }
    }

    pub fn lock(&self, key: &str) -> Status {
        let shard = self.map.shard_for(key);

        let mut keys: std::sync::MutexGuard<'_, HashSet<String>> =
            shard.mutex.lock().expect("mutex is poisoned");

        while keys.contains(key) || !self.map.has_quota() {
            keys = shard.condvar.wait(keys).expect("condvar is poisoned");
        }

        keys.insert(key.to_string());
        if self.map.max_locks > 0 {
            self.map.lock_cnt.fetch_add(1, Ordering::SeqCst);
        }

        Status::ok()
    }

    pub fn unlock(&self, key: &str) {
        let shard = self.map.shard_for(key);

        let mut keys: std::sync::MutexGuard<'_, HashSet<String>> =
            shard.mutex.lock().expect("mutex is poisoned");

        let removed = keys.remove(key);
        if removed && self.map.max_locks > 0 {
            let prev = self.map.lock_cnt.fetch_sub(1, Ordering::SeqCst);
            debug_assert!(prev > 0, "lock_cnt should stay positive when removing");
        }
        drop(keys);

        shard.condvar.notify_all();
    }

    pub fn try_lock(&self, key: &str) -> Status {
        let shard = self.map.shard_for(key);

        let mut keys: std::sync::MutexGuard<'_, HashSet<String>> =
            shard.mutex.lock().expect("mutex is poisoned");

        if keys.contains(key) {
            return Status::busy("Lock already held");
        }

        if !self.map.has_quota() {
            return Status::busy("Lock limit reached");
        }

        keys.insert(key.to_string());
        if self.map.max_locks > 0 {
            self.map.lock_cnt.fetch_add(1, Ordering::SeqCst);
        }

        Status::ok()
    }
}

/// RAII lock guard
pub struct ScopeRecordLock<'a> {
    mgr: &'a LockMgr,
    key: String,
    locked: bool,
}

impl<'a> ScopeRecordLock<'a> {
    pub fn new(mgr: &'a LockMgr, key: &str) -> Self {
        let key_str = key.to_string();
        let locked = mgr.lock(&key_str).is_ok();
        Self {
            mgr,
            key: key_str,
            locked,
        }
    }

    pub fn try_new(mgr: &'a LockMgr, key: &str) -> Option<Self> {
        let key_str = key.to_string();
        if mgr.try_lock(&key_str).is_ok() {
            Some(Self {
                mgr,
                key: key_str,
                locked: true,
            })
        } else {
            None
        }
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }
}

impl<'a> Drop for ScopeRecordLock<'a> {
    fn drop(&mut self) {
        if self.locked {
            self.mgr.unlock(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, thread, time::Duration};

    #[test]
    fn test_basic_lock_unlock() {
        let mgr = LockMgr::new(4);
        let status = mgr.lock("test_key");
        assert!(status.is_ok());
        mgr.unlock("test_key");
    }

    #[test]
    fn test_try_lock_success() {
        let mgr = LockMgr::new(4);
        let status = mgr.try_lock("test_key");
        assert!(status.is_ok());
        mgr.unlock("test_key");
    }

    #[test]
    fn test_try_lock_already_locked() {
        let mgr = LockMgr::new(4);
        let status1 = mgr.try_lock("test_key");
        assert!(status1.is_ok());
        let status2 = mgr.try_lock("test_key");
        assert!(!status2.is_ok());
        mgr.unlock("test_key");
    }

    #[test]
    fn test_max_locks_limit() {
        let mgr = LockMgr::with_max_locks(4, 2);

        let status1 = mgr.try_lock("key1");
        assert!(status1.is_ok());

        let status2 = mgr.try_lock("key2");
        assert!(status2.is_ok());

        let status3 = mgr.try_lock("key3");
        assert!(!status3.is_ok());

        mgr.unlock("key1");
        let status4 = mgr.try_lock("key3");
        assert!(status4.is_ok());

        mgr.unlock("key2");
        mgr.unlock("key3");
    }

    #[test]
    fn test_scope_record_lock() {
        let mgr = LockMgr::new(4);

        {
            let _lock = ScopeRecordLock::new(&mgr, "test_key");
            assert!(_lock.is_locked());

            let try_lock = ScopeRecordLock::try_new(&mgr, "test_key");
            assert!(try_lock.is_none());
        }

        let try_lock = ScopeRecordLock::try_new(&mgr, "test_key");
        assert!(try_lock.is_some());
    }

    #[test]
    fn test_concurrent_access() {
        let mgr = Arc::new(LockMgr::new(4));
        let key = "shared_key";
        let counter = Arc::new(AtomicI64::new(0));

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let mgr_clone = Arc::clone(&mgr);
                let counter_clone = Arc::clone(&counter);
                let key_str = key.to_string();

                thread::spawn(move || {
                    let _lock = ScopeRecordLock::new(&mgr_clone, &key_str);
                    if _lock.is_locked() {
                        let current = counter_clone.load(Ordering::Acquire);
                        thread::sleep(Duration::from_millis(1));
                        counter_clone.store(current + 1, Ordering::Release);
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

        let keys = vec!["key1", "key2", "key3", "key4"];
        let mut locks = Vec::new();

        for key in &keys {
            let status = mgr.try_lock(key);
            if status.is_ok() {
                locks.push(key);
            }
        }

        assert!(!locks.is_empty());

        for key in locks {
            mgr.unlock(key);
        }
    }

    #[test]
    fn test_edge_cases() {
        let mgr = LockMgr::new(1);

        let status = mgr.try_lock("");
        assert!(status.is_ok());
        mgr.unlock("");

        let long_key = "a".repeat(1000);
        let status = mgr.try_lock(&long_key);
        assert!(status.is_ok());
        mgr.unlock(&long_key);
    }

    #[test]
    fn test_multiple_threads_same_key_contention() {
        use std::sync::atomic::{AtomicI32, Ordering};

        let mgr = Arc::new(LockMgr::new(4));
        let key = "contested_key";
        let execution_order = Arc::new(Mutex::new(Vec::new()));
        let counter = Arc::new(AtomicI32::new(0));

        let handles: Vec<_> = (0..5)
            .map(|thread_id| {
                let mgr_clone = Arc::clone(&mgr);
                let order_clone = Arc::clone(&execution_order);
                let counter_clone = Arc::clone(&counter);
                let key_str = key.to_string();

                thread::spawn(move || {
                    println!("Thread {} attempting to acquire lock", thread_id);

                    let status = mgr_clone.lock(&key_str);
                    assert!(
                        status.is_ok(),
                        "Thread {} failed to acquire lock",
                        thread_id
                    );

                    {
                        let mut order = order_clone.lock().unwrap();
                        order.push(thread_id);
                    }

                    println!("Thread {} acquired lock", thread_id);

                    let current = counter_clone.load(Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(50));
                    counter_clone.store(current + 1, Ordering::SeqCst);

                    let final_val = counter_clone.load(Ordering::SeqCst);
                    assert_eq!(
                        final_val,
                        current + 1,
                        "Race condition detected in thread {}",
                        thread_id
                    );

                    println!("Thread {} releasing lock", thread_id);
                    mgr_clone.unlock(&key_str);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 5);

        let order = execution_order.lock().unwrap();
        assert_eq!(order.len(), 5);
        println!("Execution order: {:?}", *order);

        let final_lock = mgr.try_lock(key);
        assert!(
            final_lock.is_ok(),
            "Lock should be available after all threads finish"
        );
        mgr.unlock(key);
    }

    #[test]
    fn test_try_lock_contention() {
        let mgr = Arc::new(LockMgr::new(4));
        let key = "try_lock_key";
        let success_count = Arc::new(AtomicI64::new(0));
        let failure_count = Arc::new(AtomicI64::new(0));

        let status = mgr.try_lock(key);
        assert!(status.is_ok());

        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let mgr_clone = Arc::clone(&mgr);
                let success_clone = Arc::clone(&success_count);
                let failure_clone = Arc::clone(&failure_count);
                let key_str = key.to_string();

                thread::spawn(move || {
                    let status = mgr_clone.try_lock(&key_str);
                    if status.is_ok() {
                        success_clone.fetch_add(1, Ordering::SeqCst);
                        mgr_clone.unlock(&key_str);
                        println!("Thread {} succeeded in try_lock", thread_id);
                    } else {
                        failure_clone.fetch_add(1, Ordering::SeqCst);
                        println!("Thread {} failed in try_lock (expected)", thread_id);
                    }
                })
            })
            .collect();

        thread::sleep(Duration::from_millis(100));

        mgr.unlock(key);

        for handle in handles {
            handle.join().unwrap();
        }

        let total_attempts =
            success_count.load(Ordering::SeqCst) + failure_count.load(Ordering::SeqCst);
        assert_eq!(total_attempts, 10);

        println!(
            "Success: {}, Failures: {}",
            success_count.load(Ordering::SeqCst),
            failure_count.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn test_scope_lock_survives_panic() {
        let mgr = Arc::new(LockMgr::new(4));
        let key = "panic_key";

        let mgr_clone = Arc::clone(&mgr);
        let key_str = key.to_string();

        let handle = thread::spawn(move || {
            let _lock = ScopeRecordLock::new(&mgr_clone, &key_str);
            assert!(_lock.is_locked());

            println!("Thread acquired lock, about to panic...");

            panic!("Simulated panic while holding lock");
        });

        let result = handle.join();
        assert!(result.is_err(), "Thread should have panicked");

        thread::sleep(Duration::from_millis(10));

        let status = mgr.try_lock(key);
        assert!(status.is_ok(), "Lock should be released after panic");

        mgr.unlock(key);
        println!("Lock successfully acquired after panic - RAII worked!");
    }

    #[test]
    fn test_multiple_panics_with_same_key() {
        let mgr = Arc::new(LockMgr::new(4));
        let key = "multi_panic_key";

        let handles: Vec<_> = (0..3)
            .map(|thread_id| {
                let mgr_clone = Arc::clone(&mgr);
                let key_str = key.to_string();

                thread::spawn(move || {
                    let _lock = ScopeRecordLock::new(&mgr_clone, &key_str);
                    println!("Thread {} acquired lock", thread_id);

                    thread::sleep(Duration::from_millis(thread_id as u64 * 10));
                    panic!("Thread {} panicked!", thread_id);
                })
            })
            .collect();

        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.join();
            assert!(result.is_err(), "Thread {} should have panicked", i);
        }

        thread::sleep(Duration::from_millis(100));

        let status = mgr.try_lock(key);
        assert!(
            status.is_ok(),
            "Lock should be completely released after all panics"
        );
        mgr.unlock(key);
    }

    #[test]
    fn test_nested_scope_locks_with_panic() {
        let mgr = Arc::new(LockMgr::new(4));

        let mgr_clone = Arc::clone(&mgr);
        let handle = thread::spawn(move || {
            let _outer_lock = ScopeRecordLock::new(&mgr_clone, "outer_key");
            {
                let _inner_lock = ScopeRecordLock::new(&mgr_clone, "inner_key");
                assert!(_outer_lock.is_locked());
                assert!(_inner_lock.is_locked());

                panic!("Panic with nested locks");
            }
        });

        let result = handle.join();
        assert!(result.is_err());

        thread::sleep(Duration::from_millis(10));

        let outer_status = mgr.try_lock("outer_key");
        let inner_status = mgr.try_lock("inner_key");

        assert!(outer_status.is_ok(), "Outer lock should be released");
        assert!(inner_status.is_ok(), "Inner lock should be released");

        mgr.unlock("outer_key");
        mgr.unlock("inner_key");
    }
}
