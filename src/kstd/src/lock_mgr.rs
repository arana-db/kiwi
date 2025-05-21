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

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use super::status::Status;

/// TODO: remove allow dead code.
#[allow(dead_code)]
struct LockMapShard {
    // HashSet<String> is locked keys
    mutex: Mutex<HashSet<String>>,
    condvar: Condvar,
}

/// TODO: remove allow dead code.
#[allow(dead_code)]
impl LockMapShard {
    fn new() -> LockMapShard {
        LockMapShard {
            mutex: Mutex::new(HashSet::new()),
            condvar: Condvar::new(),
        }
    }
}

/// TODO: remove allow dead code.
#[allow(dead_code)]
struct LockMap {
    num_shards: usize,
    lock_cnt: std::sync::atomic::AtomicI64,
    shards: Vec<Arc<LockMapShard>>,
}

/// TODO: remove allow dead code.
#[allow(dead_code)]
impl LockMap {
    fn new(num_shards: usize) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Arc::new(LockMapShard::new()));
        }
        Self {
            num_shards,
            lock_cnt: std::sync::atomic::AtomicI64::new(0),
            shards,
        }
    }

    fn get_shard_num(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.num_shards
    }
}

/// TODO: remove allow dead code.
#[allow(dead_code)]
pub struct LockMgr {
    lock_map: Arc<LockMap>,
    max_num_locks: i64,
}

/// TODO: remove allow dead code.
#[allow(dead_code)]
impl LockMgr {
    fn new(default_num_shards: usize) -> Self {
        Self::with_max_locks(default_num_shards, -1)
    }

    pub fn with_max_locks(num_shards: usize, max_num_locks: i64) -> Self {
        Self {
            max_num_locks,
            lock_map: Arc::new(LockMap::new(num_shards)),
        }
    }

    fn try_lock(&self, key: &str) -> Status {
        let shard_num = self.lock_map.get_shard_num(key);
        let shard = &self.lock_map.shards[shard_num];
        self.acquire(shard, key)
    }

    fn acquire(&self, shard: &LockMapShard, key: &str) -> Status {
        let mut keys = match shard.mutex.lock() {
            Ok(guard) => guard,
            Err(_) => return Status::busy("Failed to acquire mutex"),
        };
        let mut result = self.acquire_locked(&mut keys, key);

        while !result.is_ok() {
            keys = match shard.condvar.wait(keys) {
                Ok(guard) => guard,
                Err(_) => return Status::busy("Failed during wait"),
            };
            result = self.acquire_locked(&mut keys, key);
        }
        result
    }

    fn acquire_locked(&self, keys: &mut MutexGuard<HashSet<String>>, key: &str) -> Status {
        if keys.contains(key) {
            return Status::busy("Lock already held");
        }
        if self.max_num_locks > 0
            && self.lock_map.lock_cnt.load(Ordering::Acquire) >= self.max_num_locks
        {
            return Status::busy("Lock limit reached");
        }
        keys.insert(key.to_string());

        if self.max_num_locks > 0 {
            self.lock_map.lock_cnt.fetch_add(1, Ordering::SeqCst);
        }
        Status::ok()
    }

    pub fn unlock(&self, key: &str) {
        let shard_num = self.lock_map.get_shard_num(key);
        let shard = &self.lock_map.shards[shard_num];
        let mut keys = shard.mutex.lock().unwrap();
        if keys.remove(key) && self.max_num_locks > 0 {
            self.lock_map
                .lock_cnt
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }

        drop(keys);
        shard.condvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn func(mgr: Arc<LockMgr>, id: usize, key: String) {
        mgr.try_lock(&key);
        println!("thread {} TryLock {} success", id, key);
        thread::sleep(Duration::from_millis(100));
        mgr.unlock(&key);
        println!("thread {} UnLock {}", id, key);
    }

    #[test]
    fn test_lock_mgr() {
        let mgr = Arc::new(LockMgr::with_max_locks(1, 3));

        let mgr_clone1 = Arc::clone(&mgr);
        let t1 = thread::spawn(move || func(mgr_clone1, 1, "key_1".to_string()));
        thread::sleep(Duration::from_millis(20));

        let mgr_clone2 = Arc::clone(&mgr);
        let t2 = thread::spawn(move || func(mgr_clone2, 2, "key_2".to_string()));
        thread::sleep(Duration::from_millis(20));

        let mgr_clone3 = Arc::clone(&mgr);
        let t3 = thread::spawn(move || func(mgr_clone3, 3, "key_3".to_string()));

        let mgr_clone4 = Arc::clone(&mgr);
        let t4 = thread::spawn(move || func(mgr_clone4, 4, "key_4".to_string()));

        thread::sleep(Duration::from_millis(20));

        let s = mgr.try_lock("key_1");
        println!("thread main TryLock key_1 ret {}", s.to_string());
        mgr.unlock("key_1");
        println!("thread main UnLock key_1");

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
        t4.join().unwrap();
    }
}
