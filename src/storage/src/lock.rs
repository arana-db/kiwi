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

//! Lock manager implementation for concurrent access control

use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Read,
    Write,
}

#[derive(Debug)]
pub struct LockManager {
    locks: Arc<RwLock<HashMap<Vec<u8>, Mutex<()>>>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn acquire_lock(&self, key: &[u8], lock_type: LockType) -> bool {
        let locks = self.locks.read();
        if let Some(lock) = locks.get(key) {
            match lock_type {
                LockType::Read => {
                    lock.try_lock().is_some()
                }
                LockType::Write => {
                    lock.try_lock_for(Duration::from_secs(1)).is_some()
                }
            }
        } else {
            drop(locks);
            let mut locks = self.locks.write();
            locks.insert(key.to_vec(), Mutex::new(()));
            true
        }
    }

    pub fn release_lock(&self, key: &[u8]) {
        let mut locks = self.locks.write();
        locks.remove(key);
    }

    pub fn clean_expired_locks(&self) {
        let mut locks = self.locks.write();
        locks.clear();
    }
}

#[derive(Debug)]
pub struct LockGuard<'a> {
    manager: &'a LockManager,
    key: Vec<u8>,
}

impl<'a> LockGuard<'a> {
    pub fn new(manager: &'a LockManager, key: Vec<u8>, lock_type: LockType) -> Option<Self> {
        if manager.acquire_lock(&key, lock_type) {
            Some(Self { manager, key })
        } else {
            None
        }
    }
}

impl<'a> Drop for LockGuard<'a> {
    fn drop(&mut self) {
        self.manager.release_lock(&self.key);
    }
}