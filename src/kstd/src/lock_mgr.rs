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
    max_locks: i64,
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

    fn shard_for(&self, key: &str) -> &Arc<LockMapShard> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        &self.shards[hasher.finish() as usize % self.shards.len()]
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
        let mut keys = match shard.mutex.lock() {
            Ok(guard) => guard,
            Err(_) => return Status::busy("Failed to acquire mutex"),
        };

        while keys.contains(key)
            || (self.map.max_locks > 0
                && self.map.lock_cnt.load(Ordering::Acquire) >= self.map.max_locks)
        {
            keys = match shard.condvar.wait(keys) {
                Ok(guard) => guard,
                Err(_) => return Status::busy("Failed during wait"),
            };
        }

        keys.insert(key.to_string());
        if self.map.max_locks > 0 {
            self.map.lock_cnt.fetch_add(1, Ordering::SeqCst);
        }
        Status::ok()
    }

    pub fn unlock(&self, key: &str) {
        let shard = self.map.shard_for(key);
        let mut keys = match shard.mutex.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };

        if keys.remove(key) && self.map.max_locks > 0 {
            self.map.lock_cnt.fetch_sub(1, Ordering::SeqCst);
        }

        drop(keys);
        shard.condvar.notify_all();
    }

    pub fn try_lock(&self, key: &str) -> Status {
        let shard = self.map.shard_for(key);
        let mut keys = match shard.mutex.lock() {
            Ok(guard) => guard,
            Err(_) => return Status::busy("Failed to acquire mutex"),
        };

        if keys.contains(key) {
            return Status::busy("Lock already held");
        }

        if self.map.max_locks > 0 && self.map.lock_cnt.load(Ordering::Acquire) >= self.map.max_locks
        {
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

    fn test_lock_operation(mgr: Arc<LockMgr>, id: usize, key: String) {
        let _lock = ScopeRecordLock::new(&mgr, &key);
        println!("thread {} acquired lock for {}", id, key);
        thread::sleep(Duration::from_millis(100));
        println!("thread {} releasing lock for {}", id, key);
    }

    #[test]
    fn test_lock_mgr() {
        let mgr = Arc::new(LockMgr::with_max_locks(4, 3));

        let threads: Vec<_> = (1..=4)
            .map(|i| {
                let mgr_clone = Arc::clone(&mgr);
                let key = format!("key_{}", i);
                thread::spawn(move || test_lock_operation(mgr_clone, i, key))
            })
            .collect();

        for handle in threads {
            handle.join().unwrap();
        }

        let try_lock_result = mgr.try_lock("test_key");
        println!("Try lock result: {}", try_lock_result);
        if try_lock_result.is_ok() {
            mgr.unlock("test_key");
        }
    }
}
