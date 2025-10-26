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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use parking_lot::RwLock;
use tokio::time::interval;

use crate::base_value_format::DataType;
use crate::error::Result;
use crate::storage::BgTaskHandler;

/// Manages key expiration with time-based indexing for efficient cleanup
pub struct ExpirationManager {
    /// Index of expiration times to sets of keys that expire at that time
    expiry_index: Arc<RwLock<BTreeMap<u64, HashSet<String>>>>,
    /// Map from key to its expiration time for quick lookup
    key_expiry: Arc<RwLock<std::collections::HashMap<String, u64>>>,
    /// Background task handler for cleanup operations
    bg_task_handler: Arc<BgTaskHandler>,
    /// Flag to control background cleanup task
    cleanup_running: Arc<parking_lot::Mutex<bool>>,
}

impl ExpirationManager {
    /// Create a new expiration manager
    pub fn new(bg_task_handler: Arc<BgTaskHandler>) -> Self {
        Self {
            expiry_index: Arc::new(RwLock::new(BTreeMap::new())),
            key_expiry: Arc::new(RwLock::new(std::collections::HashMap::new())),
            bg_task_handler,
            cleanup_running: Arc::new(parking_lot::Mutex::new(false)),
        }
    }

    /// Start the background cleanup task
    pub fn start_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let expiry_index = Arc::clone(&self.expiry_index);
        let key_expiry = Arc::clone(&self.key_expiry);
        let bg_task_handler = Arc::clone(&self.bg_task_handler);
        let cleanup_running = Arc::clone(&self.cleanup_running);

        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(1)); // Check every second
            
            loop {
                cleanup_interval.tick().await;
                
                // Check if cleanup is already running
                {
                    let mut running = cleanup_running.lock();
                    if *running {
                        continue;
                    }
                    *running = true;
                }

                let current_micros = Utc::now().timestamp_micros() as u64;
                let mut expired_keys = Vec::new();

                // Find expired keys
                {
                    let expiry_index_read = expiry_index.read();
                    for (&expire_time, keys) in expiry_index_read.range(..=current_micros) {
                        for key in keys {
                            expired_keys.push((key.clone(), expire_time));
                        }
                    }
                }

                // Remove expired keys from indexes
                if !expired_keys.is_empty() {
                    let mut expiry_index_write = expiry_index.write();
                    let mut key_expiry_write = key_expiry.write();

                    for (key, expire_time) in &expired_keys {
                        // Remove from key_expiry map
                        key_expiry_write.remove(key);

                        // Remove from expiry_index
                        if let Some(keys_set) = expiry_index_write.get_mut(&expire_time) {
                            keys_set.remove(key);
                            if keys_set.is_empty() {
                                expiry_index_write.remove(&expire_time);
                            }
                        }
                    }
                }

                // Send cleanup tasks for expired keys
                for (key, _) in expired_keys {
                    // We'll determine the data type during actual cleanup
                    let _ = bg_task_handler
                        .send(crate::storage::BgTask::CompactSpecificKey {
                            dtype: DataType::All,
                            key,
                        })
                        .await;
                }

                // Mark cleanup as finished
                {
                    let mut running = cleanup_running.lock();
                    *running = false;
                }
            }
        })
    }

    /// Set expiration time for a key
    pub fn set_expiration(&self, key: &str, expire_time_micros: u64) {
        let mut expiry_index = self.expiry_index.write();
        let mut key_expiry = self.key_expiry.write();

        // Remove old expiration if exists
        if let Some(old_expire_time) = key_expiry.get(key) {
            if let Some(keys_set) = expiry_index.get_mut(old_expire_time) {
                keys_set.remove(key);
                if keys_set.is_empty() {
                    expiry_index.remove(old_expire_time);
                }
            }
        }

        // Set new expiration
        key_expiry.insert(key.to_string(), expire_time_micros);
        expiry_index
            .entry(expire_time_micros)
            .or_insert_with(HashSet::new)
            .insert(key.to_string());
    }

    /// Remove expiration for a key (make it persistent)
    pub fn remove_expiration(&self, key: &str) -> bool {
        let mut expiry_index = self.expiry_index.write();
        let mut key_expiry = self.key_expiry.write();

        if let Some(expire_time) = key_expiry.remove(key) {
            if let Some(keys_set) = expiry_index.get_mut(&expire_time) {
                keys_set.remove(key);
                if keys_set.is_empty() {
                    expiry_index.remove(&expire_time);
                }
            }
            true
        } else {
            false
        }
    }

    /// Get remaining time to live for a key in seconds
    pub fn get_ttl_seconds(&self, key: &str) -> Option<i64> {
        let key_expiry = self.key_expiry.read();
        if let Some(&expire_time_micros) = key_expiry.get(key) {
            let current_micros = Utc::now().timestamp_micros() as u64;
            if expire_time_micros > current_micros {
                let remaining_micros = expire_time_micros - current_micros;
                Some((remaining_micros / 1_000_000) as i64)
            } else {
                Some(-1) // Key has expired
            }
        } else {
            Some(-1) // Key doesn't exist or has no expiration
        }
    }

    /// Get remaining time to live for a key in milliseconds
    pub fn get_ttl_milliseconds(&self, key: &str) -> Option<i64> {
        let key_expiry = self.key_expiry.read();
        if let Some(&expire_time_micros) = key_expiry.get(key) {
            let current_micros = Utc::now().timestamp_micros() as u64;
            if expire_time_micros > current_micros {
                let remaining_micros = expire_time_micros - current_micros;
                Some((remaining_micros / 1_000) as i64)
            } else {
                Some(-1) // Key has expired
            }
        } else {
            Some(-1) // Key doesn't exist or has no expiration
        }
    }

    /// Check if a key has expiration set
    pub fn has_expiration(&self, key: &str) -> bool {
        let key_expiry = self.key_expiry.read();
        key_expiry.contains_key(key)
    }

    /// Convert seconds to microseconds timestamp
    pub fn seconds_to_expire_time(seconds: i64) -> Result<u64> {
        let current_micros = Utc::now().timestamp_micros() as u64;
        let seconds_micros = (seconds as u64)
            .checked_mul(1_000_000)
            .ok_or_else(|| crate::error::Error::InvalidFormat {
                message: "TTL seconds overflow".to_string(),
                location: snafu::location!(),
            })?;
        
        current_micros
            .checked_add(seconds_micros)
            .ok_or_else(|| crate::error::Error::InvalidFormat {
                message: "TTL timestamp overflow".to_string(),
                location: snafu::location!(),
            })
    }

    /// Convert milliseconds to microseconds timestamp
    pub fn milliseconds_to_expire_time(milliseconds: i64) -> Result<u64> {
        let current_micros = Utc::now().timestamp_micros() as u64;
        let milliseconds_micros = (milliseconds as u64)
            .checked_mul(1_000)
            .ok_or_else(|| crate::error::Error::InvalidFormat {
                message: "TTL milliseconds overflow".to_string(),
                location: snafu::location!(),
            })?;
        
        current_micros
            .checked_add(milliseconds_micros)
            .ok_or_else(|| crate::error::Error::InvalidFormat {
                message: "TTL timestamp overflow".to_string(),
                location: snafu::location!(),
            })
    }

    /// Convert Unix timestamp in seconds to microseconds
    pub fn unix_seconds_to_expire_time(timestamp: i64) -> Result<u64> {
        if timestamp <= 0 {
            return Err(crate::error::Error::InvalidFormat {
                message: "Invalid Unix timestamp".to_string(),
                location: snafu::location!(),
            });
        }

        let timestamp_micros = (timestamp as u64)
            .checked_mul(1_000_000)
            .ok_or_else(|| crate::error::Error::InvalidFormat {
                message: "Unix timestamp overflow".to_string(),
                location: snafu::location!(),
            })?;

        Ok(timestamp_micros)
    }

    /// Convert Unix timestamp in milliseconds to microseconds
    pub fn unix_milliseconds_to_expire_time(timestamp: i64) -> Result<u64> {
        if timestamp <= 0 {
            return Err(crate::error::Error::InvalidFormat {
                message: "Invalid Unix timestamp".to_string(),
                location: snafu::location!(),
            });
        }

        let timestamp_micros = (timestamp as u64)
            .checked_mul(1_000)
            .ok_or_else(|| crate::error::Error::InvalidFormat {
                message: "Unix timestamp overflow".to_string(),
                location: snafu::location!(),
            })?;

        Ok(timestamp_micros)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::BgTaskHandler;

    #[tokio::test]
    async fn test_expiration_manager_basic() {
        let (handler, _receiver) = BgTaskHandler::new();
        let manager = ExpirationManager::new(Arc::new(handler));

        // Test setting and getting expiration
        let expire_time = Utc::now().timestamp_micros() as u64 + 5_000_000; // 5 seconds from now
        manager.set_expiration("test_key", expire_time);

        assert!(manager.has_expiration("test_key"));
        let ttl = manager.get_ttl_seconds("test_key").unwrap();
        assert!(ttl >= 4 && ttl <= 5); // Should be around 5 seconds

        // Test removing expiration
        assert!(manager.remove_expiration("test_key"));
        assert!(!manager.has_expiration("test_key"));
        assert!(!manager.remove_expiration("test_key")); // Should return false for non-existent key
    }

    #[tokio::test]
    async fn test_time_conversions() {
        // Test seconds to expire time
        let expire_time = ExpirationManager::seconds_to_expire_time(60).unwrap();
        let current_micros = Utc::now().timestamp_micros() as u64;
        assert!(expire_time > current_micros);
        assert!(expire_time <= current_micros + 61_000_000); // Allow 1 second tolerance

        // Test milliseconds to expire time
        let expire_time = ExpirationManager::milliseconds_to_expire_time(5000).unwrap();
        assert!(expire_time > current_micros);
        assert!(expire_time <= current_micros + 5_001_000); // Allow 1ms tolerance

        // Test Unix timestamp conversions
        let unix_timestamp = 1640995200; // 2022-01-01 00:00:00 UTC
        let expire_time = ExpirationManager::unix_seconds_to_expire_time(unix_timestamp).unwrap();
        assert_eq!(expire_time, 1640995200_000_000);

        let unix_timestamp_ms = 1640995200000; // 2022-01-01 00:00:00 UTC in milliseconds
        let expire_time = ExpirationManager::unix_milliseconds_to_expire_time(unix_timestamp_ms).unwrap();
        assert_eq!(expire_time, 1640995200_000_000);
    }
}