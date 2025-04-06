//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// 锁管理器，用于管理存储引擎中的锁
/// 这是C++版本的lock_mgr.h的Rust实现
pub struct LockMgr {
    // 锁映射表，键为锁的名称，值为锁的状态
    locks: RwLock<HashMap<String, Arc<Mutex<LockStatus>>>>,
}

/// 锁的状态
struct LockStatus {
    // 锁的持有者数量
    holders: usize,
    // 最后一次获取锁的时间
    last_acquired: Instant,
}

impl LockMgr {
    /// 创建一个新的锁管理器
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// 尝试获取锁
    /// 
    /// # 参数
    /// * `name` - 锁的名称
    /// * `timeout` - 获取锁的超时时间，单位为毫秒
    /// 
    /// # 返回值
    /// 如果成功获取锁，返回true；否则返回false
    pub fn try_lock(&self, name: &str, timeout: u64) -> bool {
        let locks = self.locks.read().unwrap();
        
        // 检查锁是否存在
        if let Some(lock) = locks.get(name) {
            // 尝试获取锁
            match lock.try_lock() {
                Ok(mut status) => {
                    // 更新锁的状态
                    status.holders += 1;
                    status.last_acquired = Instant::now();
                    true
                },
                Err(_) => {
                    // 无法获取锁，可能是被其他线程持有
                    false
                }
            }
        } else {
            // 锁不存在，需要创建新锁
            drop(locks); // 释放读锁
            
            let mut locks = self.locks.write().unwrap();
            
            // 再次检查锁是否存在（可能在获取写锁的过程中被其他线程创建）
            if !locks.contains_key(name) {
                // 创建新锁
                let status = LockStatus {
                    holders: 1,
                    last_acquired: Instant::now(),
                };
                locks.insert(name.to_string(), Arc::new(Mutex::new(status)));
                true
            } else {
                // 锁已经存在，尝试获取
                let lock = locks.get(name).unwrap();
                match lock.try_lock() {
                    Ok(mut status) => {
                        status.holders += 1;
                        status.last_acquired = Instant::now();
                        true
                    },
                    Err(_) => false,
                }
            }
        }
    }

    /// 释放锁
    /// 
    /// # 参数
    /// * `name` - 锁的名称
    /// 
    /// # 返回值
    /// 如果成功释放锁，返回true；否则返回false
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
                },
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// 检查锁是否被持有
    /// 
    /// # 参数
    /// * `name` - 锁的名称
    /// 
    /// # 返回值
    /// 如果锁被持有，返回true；否则返回false
    pub fn is_locked(&self, name: &str) -> bool {
        let locks = self.locks.read().unwrap();
        
        if let Some(lock) = locks.get(name) {
            match lock.try_lock() {
                Ok(status) => status.holders > 0,
                Err(_) => true, // 无法获取锁，说明锁被持有
            }
        } else {
            false
        }
    }

    /// 清理过期的锁
    /// 
    /// # 参数
    /// * `max_idle_time` - 最大空闲时间，单位为毫秒
    pub fn cleanup(&self, max_idle_time: u64) {
        let mut locks = self.locks.write().unwrap();
        
        // 找出过期的锁
        let expired_keys: Vec<String> = locks
            .iter()
            .filter_map(|(key, lock)| {
                if let Ok(status) = lock.try_lock() {
                    if status.holders == 0 && status.last_acquired.elapsed() > Duration::from_millis(max_idle_time) {
                        Some(key.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        
        // 移除过期的锁
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
        
        // 测试获取锁
        assert!(lock_mgr.try_lock("test_lock", 1000));
        
        // 测试锁是否被持有
        assert!(lock_mgr.is_locked("test_lock"));
        
        // 测试释放锁
        assert!(lock_mgr.unlock("test_lock"));
        
        // 测试锁是否被释放
        assert!(!lock_mgr.is_locked("test_lock"));
    }

    #[test]
    fn test_concurrent_locks() {
        let lock_mgr = Arc::new(LockMgr::new());
        
        // 测试并发获取不同的锁
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
        
        // 获取并释放锁
        assert!(lock_mgr.try_lock("test_lock", 1000));
        assert!(lock_mgr.unlock("test_lock"));
        
        // 等待一段时间
        thread::sleep(Duration::from_millis(100));
        
        // 清理过期的锁
        lock_mgr.cleanup(50);
        
        // 检查锁是否被清理
        assert!(!lock_mgr.is_locked("test_lock"));
    }
}