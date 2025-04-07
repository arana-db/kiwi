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
//  of patent rights can be found in the PATENTS file in the same directory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Lock manager for managing locks in the storage engine
/// This is a Rust implementation of the C++ version lock_mgr.h
/// TODO: remove allow dead code
#[allow(dead_code)]
pub struct LockMgr {
    // Lock mapping table, where key is the lock name and value is the lock state
    locks: RwLock<HashMap<String, Arc<Mutex<LockStatus>>>>,
}

/// Lock state
/// TODO: remove allow dead code
#[allow(dead_code)]
struct LockStatus {
    // Number of lock holders
    holders: usize,
    // Last time the lock was acquired
    last_acquired: Instant,
}

impl Default for LockMgr {
    fn default() -> Self {
        Self::new()
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl LockMgr {
    /// Create a new lock manager
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Try to acquire a lock
    ///
    /// # Parameters
    /// * `name` - Name of the lock
    /// * `timeout` - Timeout duration in milliseconds for acquiring the lock
    ///
    /// # Returns
    /// Returns true if the lock is successfully acquired; otherwise returns false
    /// TODO: implement
    pub fn try_lock(&self, name: &str, _timeout: u64) -> bool {
        let locks = self.locks.read().unwrap();

        // Check if the lock exists
        if let Some(lock) = locks.get(name) {
            // Try to acquire the lock
            match lock.try_lock() {
                Ok(mut status) => {
                    // Update lock state
                    status.holders += 1;
                    status.last_acquired = Instant::now();
                    true
                }
                Err(_) => {
                    // Cannot acquire lock, might be held by other threads
                    false
                }
            }
        } else {
            // Lock doesn't exist, need to create a new one
            drop(locks); // Release read lock

            let mut locks = self.locks.write().unwrap();

            // Check again if the lock exists (might have been created by other threads while acquiring write lock)
            if !locks.contains_key(name) {
                // Create new lock
                let status = LockStatus {
                    holders: 1,
                    last_acquired: Instant::now(),
                };
                locks.insert(name.to_string(), Arc::new(Mutex::new(status)));
                true
            } else {
                // Lock already exists, try to acquire it
                let lock = locks.get(name).unwrap();
                match lock.try_lock() {
                    Ok(mut status) => {
                        status.holders += 1;
                        status.last_acquired = Instant::now();
                        true
                    }
                    Err(_) => false,
                }
            }
        }
    }

    /// Release a lock
    ///
    /// # Parameters
    /// * `name` - Name of the lock
    ///
    /// # Returns
    /// Returns true if the lock is successfully released; otherwise returns false
    pub fn unlock(&self, name: &str) -> bool {
        let locks = self.locks.read().unwrap();

        if let Some(lock) = locks.get(name) {
            match lock.try_lock() {
                Ok(mut status) => {
                    if status.holders > 0 {
                        status.holders -= 1;
                        true
                    } else {
                        false
                    }
                }
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Check if a lock is being held
    ///
    /// # Parameters
    /// * `name` - Name of the lock
    ///
    /// # Returns
    /// Returns true if the lock is being held; otherwise returns false
    pub fn is_locked(&self, name: &str) -> bool {
        let locks = self.locks.read().unwrap();

        if let Some(lock) = locks.get(name) {
            match lock.try_lock() {
                Ok(status) => status.holders > 0,
                Err(_) => true, // Cannot acquire lock, indicating it's being held
            }
        } else {
            false
        }
    }

    /// Clean up expired locks
    ///
    /// # Parameters
    /// * `max_idle_time` - Maximum idle time in milliseconds
    pub fn cleanup(&self, max_idle_time: u64) {
        let mut locks = self.locks.write().unwrap();

        // Find expired locks
        let expired_keys: Vec<String> = locks
            .iter()
            .filter_map(|(key, lock)| {
                if let Ok(status) = lock.try_lock() {
                    if status.holders == 0
                        && status.last_acquired.elapsed() > Duration::from_millis(max_idle_time)
                    {
                        Some(key.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // Remove expired locks
        for key in expired_keys {
            locks.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_lock_unlock() {
        let lock_mgr = LockMgr::new();

        // Test acquiring lock
        assert!(lock_mgr.try_lock("test_lock", 1000));

        // Test if lock is held
        assert!(lock_mgr.is_locked("test_lock"));

        // Test releasing lock
        assert!(lock_mgr.unlock("test_lock"));

        // Test if lock is released
        assert!(!lock_mgr.is_locked("test_lock"));
    }

    #[test]
    fn test_concurrent_locks() {
        let lock_mgr = Arc::new(LockMgr::new());

        // Test concurrent acquisition of different locks
        let lock_mgr1 = lock_mgr.clone();
        let handle1 = thread::spawn(move || {
            assert!(lock_mgr1.try_lock("lock1", 1000));
            thread::sleep(Duration::from_millis(100));
            assert!(lock_mgr1.unlock("lock1"));
        });

        let lock_mgr2 = lock_mgr.clone();
        let handle2 = thread::spawn(move || {
            assert!(lock_mgr2.try_lock("lock2", 1000));
            thread::sleep(Duration::from_millis(100));
            assert!(lock_mgr2.unlock("lock2"));
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let lock_mgr = LockMgr::new();

        // Test acquiring lock
        assert!(lock_mgr.try_lock("test_lock", 1000));

        // Test if lock is held
        assert!(lock_mgr.is_locked("test_lock"));

        // Wait for a while
        thread::sleep(Duration::from_millis(100));

        // Clean up expired locks
        lock_mgr.cleanup(50);

        // Test if lock is still held
        assert!(lock_mgr.is_locked("test_lock"));
        assert!(lock_mgr.unlock("test_lock"));
        assert!(!lock_mgr.is_locked("test_lock"));
    }
}
