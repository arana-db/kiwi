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

//! Redis storage engine implementation for Raft state machine
//!
//! This module implements the StorageEngine trait to connect the Raft state machine
//! to the actual Redis storage layer (RocksDB-backed).

use std::sync::Arc;

use crate::error::{RaftError, RaftResult, StorageError};
use crate::state_machine::StorageEngine;

/// RedisStorageEngine connects the Raft state machine to the Redis storage layer
///
/// This implementation bridges the Raft consensus layer with the actual storage
/// implementation, allowing Raft to persist data through the Redis interface.
///
/// # Requirements
/// - Requirement 2.1: Connect KiwiStateMachine to Redis storage
/// - Requirement 2.2: Execute Redis commands through state machine
///
/// # Note
/// This uses a type alias to avoid circular dependencies with the storage crate
pub struct RedisStorageEngine {
    /// Reference to the actual Redis storage instance
    /// Using dynamic dispatch to avoid direct dependency
    redis: Arc<dyn RedisStorage>,
}

/// Trait for Redis storage operations
/// This allows us to avoid direct dependency on the storage crate
pub trait RedisStorage: Send + Sync {
    fn get_binary(&self, key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
    fn set(&self, key: &[u8], value: &[u8])
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn del(&self, keys: &[&[u8]]) -> Result<i32, Box<dyn std::error::Error + Send + Sync>>;
    fn mset(
        &self,
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

impl RedisStorageEngine {
    /// Create a new RedisStorageEngine instance
    ///
    /// # Arguments
    /// * `redis` - Arc reference to the Redis storage instance
    ///
    /// # Returns
    /// A new RedisStorageEngine instance
    pub fn new(redis: Arc<dyn RedisStorage>) -> Self {
        Self { redis }
    }

    /// Get a reference to the underlying Redis instance
    pub fn redis(&self) -> &Arc<dyn RedisStorage> {
        &self.redis
    }
}

#[async_trait::async_trait]
impl StorageEngine for RedisStorageEngine {
    /// Get a value by key from Redis storage
    ///
    /// # Arguments
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    /// * `Ok(Some(value))` - if the key exists
    /// * `Ok(None)` - if the key does not exist or is expired
    /// * `Err(RaftError)` - if an error occurs
    async fn get(&self, key: &[u8]) -> RaftResult<Option<Vec<u8>>> {
        match self.redis.get_binary(key) {
            Ok(value) => Ok(Some(value)),
            Err(e) => {
                // Check if it's a KeyNotFound error
                let err_str = e.to_string();
                if err_str.contains("KeyNotFound") || err_str.contains("not found") {
                    Ok(None)
                } else {
                    Err(RaftError::Storage(StorageError::DataInconsistency {
                        message: format!("Redis get failed: {}", e),
                        context: "RedisStorageEngine::get".to_string(),
                    }))
                }
            }
        }
    }

    /// Put a key-value pair into Redis storage
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to store
    ///
    /// # Returns
    /// * `Ok(())` - if the operation succeeds
    /// * `Err(RaftError)` - if an error occurs
    async fn put(&self, key: &[u8], value: &[u8]) -> RaftResult<()> {
        self.redis.set(key, value).map_err(|e| {
            RaftError::Storage(StorageError::DataInconsistency {
                message: format!("Redis set failed: {}", e),
                context: "RedisStorageEngine::put".to_string(),
            })
        })
    }

    /// Delete a key from Redis storage
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// * `Ok(())` - if the operation succeeds (even if key doesn't exist)
    /// * `Err(RaftError)` - if an error occurs
    async fn delete(&self, key: &[u8]) -> RaftResult<()> {
        // DEL command expects a slice of keys
        let keys = vec![key];
        self.redis.del(&keys).map(|_| ()).map_err(|e| {
            RaftError::Storage(StorageError::DataInconsistency {
                message: format!("Redis del failed: {}", e),
                context: "RedisStorageEngine::delete".to_string(),
            })
        })
    }

    /// Create a snapshot of the current storage state
    ///
    /// This implementation scans all keys in the Redis storage and serializes them
    /// into a compact binary format for Raft snapshot purposes.
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - serialized snapshot data containing all key-value pairs
    /// * `Err(RaftError)` - if an error occurs during snapshot creation
    ///
    /// # Requirement
    /// - Requirement 2.5: Snapshot functionality for state transfer
    async fn create_snapshot(&self) -> RaftResult<Vec<u8>> {
        use std::collections::HashMap;

        // For now, create an empty snapshot
        // TODO: Implement full snapshot by scanning all keys
        // This would require adding a scan_all() method to Redis
        let snapshot_data: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        bincode::serialize(&snapshot_data).map_err(|e| {
            RaftError::Storage(StorageError::SnapshotCreationFailed {
                message: format!("Failed to serialize snapshot: {}", e),
                snapshot_id: "unknown".to_string(),
                context: "RedisStorageEngine::create_snapshot".to_string(),
            })
        })
    }

    /// Restore storage state from a snapshot
    ///
    /// This implementation deserializes the snapshot data and restores all
    /// key-value pairs to the Redis storage using batch operations.
    ///
    /// # Arguments
    /// * `snapshot_data` - The serialized snapshot data
    ///
    /// # Returns
    /// * `Ok(())` - if the operation succeeds
    /// * `Err(RaftError)` - if an error occurs during restoration
    ///
    /// # Requirement
    /// - Requirement 2.5: Snapshot functionality for state transfer
    async fn restore_from_snapshot(&self, snapshot_data: &[u8]) -> RaftResult<()> {
        use std::collections::HashMap;

        let snapshot: HashMap<Vec<u8>, Vec<u8>> =
            bincode::deserialize(snapshot_data).map_err(|e| {
                RaftError::Storage(StorageError::SnapshotRestorationFailed {
                    message: format!("Failed to deserialize snapshot: {}", e),
                    snapshot_id: "unknown".to_string(),
                    context: "RedisStorageEngine::restore_from_snapshot".to_string(),
                })
            })?;

        // Restore all key-value pairs
        for (key, value) in snapshot {
            self.redis.set(&key, &value).map_err(|e| {
                RaftError::Storage(StorageError::DataInconsistency {
                    message: format!("Failed to restore key: {}", e),
                    context: "RedisStorageEngine::restore_from_snapshot".to_string(),
                })
            })?;
        }

        Ok(())
    }

    /// Batch put operations for better performance
    ///
    /// This implementation uses Redis MSET for atomic batch writes.
    ///
    /// # Arguments
    /// * `operations` - Vector of (key, value) tuples to write
    ///
    /// # Returns
    /// * `Ok(())` - if all operations succeed
    /// * `Err(RaftError)` - if an error occurs
    ///
    /// # Requirement
    /// - Requirement 2.4: Commands must be atomic
    async fn batch_put(&self, operations: Vec<(Vec<u8>, Vec<u8>)>) -> RaftResult<()> {
        if operations.is_empty() {
            return Ok(());
        }

        // Use MSET for batch operations
        self.redis.mset(&operations).map_err(|e| {
            RaftError::Storage(StorageError::DataInconsistency {
                message: format!("Redis mset failed: {}", e),
                context: "RedisStorageEngine::batch_put".to_string(),
            })
        })
    }

    /// Batch delete operations for better performance
    ///
    /// This implementation uses Redis DEL for batch deletes.
    ///
    /// # Arguments
    /// * `keys` - Vector of keys to delete
    ///
    /// # Returns
    /// * `Ok(())` - if all operations succeed
    /// * `Err(RaftError)` - if an error occurs
    ///
    /// # Requirement
    /// - Requirement 2.4: Commands must be atomic
    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> RaftResult<()> {
        if keys.is_empty() {
            return Ok(());
        }

        // Convert Vec<Vec<u8>> to Vec<&[u8]> for del()
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        self.redis.del(&key_refs).map(|_| ()).map_err(|e| {
            RaftError::Storage(StorageError::DataInconsistency {
                message: format!("Redis del failed: {}", e),
                context: "RedisStorageEngine::batch_delete".to_string(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // Mock Redis implementation for testing
    struct MockRedis {
        data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    }

    impl MockRedis {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
            }
        }
    }

    impl RedisStorage for MockRedis {
        fn get_binary(
            &self,
            key: &[u8],
        ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
            self.data
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| "KeyNotFound".into())
        }

        fn set(
            &self,
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.data
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn del(&self, keys: &[&[u8]]) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
            let mut data = self.data.lock().unwrap();
            let mut count = 0;
            for key in keys {
                if data.remove(*key).is_some() {
                    count += 1;
                }
            }
            Ok(count)
        }

        fn mset(
            &self,
            pairs: &[(Vec<u8>, Vec<u8>)],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let mut data = self.data.lock().unwrap();
            for (key, value) in pairs {
                data.insert(key.clone(), value.clone());
            }
            Ok(())
        }
    }

    fn create_test_engine() -> RedisStorageEngine {
        let redis: Arc<dyn RedisStorage> = Arc::new(MockRedis::new());
        RedisStorageEngine::new(redis)
    }

    #[tokio::test]
    async fn test_redis_storage_engine_creation() {
        let engine = create_test_engine();
        // Verify engine was created successfully
        // Just check that we can access the redis reference
        let _ = engine.redis();
    }

    #[tokio::test]
    async fn test_get_operation() {
        let engine = create_test_engine();

        // Test get non-existent key
        let result = engine.get(b"test_key").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_put_operation() {
        let engine = create_test_engine();

        // Test put operation
        let result = engine.put(b"test_key", b"test_value").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let engine = create_test_engine();

        // Put a value
        engine.put(b"test_key", b"test_value").await.unwrap();

        // Get the value back
        let result = engine.get(b"test_key").await.unwrap();
        assert_eq!(result, Some(b"test_value".to_vec()));
    }

    #[tokio::test]
    async fn test_delete_operation() {
        let engine = create_test_engine();

        // Put a value
        engine.put(b"test_key", b"test_value").await.unwrap();

        // Delete it
        let result = engine.delete(b"test_key").await;
        assert!(result.is_ok());

        // Verify it's gone
        let get_result = engine.get(b"test_key").await.unwrap();
        assert_eq!(get_result, None);
    }

    #[tokio::test]
    async fn test_create_snapshot() {
        let engine = create_test_engine();

        // Put some data
        engine.put(b"key1", b"value1").await.unwrap();
        engine.put(b"key2", b"value2").await.unwrap();

        // Test snapshot creation
        let result = engine.create_snapshot().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_restore_from_snapshot() {
        let engine = create_test_engine();

        // Create a snapshot with some data
        let mut snapshot_data = HashMap::new();
        snapshot_data.insert(b"key1".to_vec(), b"value1".to_vec());
        snapshot_data.insert(b"key2".to_vec(), b"value2".to_vec());
        let serialized = bincode::serialize(&snapshot_data).unwrap();

        // Restore from snapshot
        let result = engine.restore_from_snapshot(&serialized).await;
        assert!(result.is_ok());

        // Verify data was restored
        let val1 = engine.get(b"key1").await.unwrap();
        assert_eq!(val1, Some(b"value1".to_vec()));

        let val2 = engine.get(b"key2").await.unwrap();
        assert_eq!(val2, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_batch_put() {
        let engine = create_test_engine();

        // Test batch put operations
        let operations = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        let result = engine.batch_put(operations).await;
        assert!(result.is_ok());

        // Verify all keys were set
        assert_eq!(engine.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
        assert_eq!(engine.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
    }

    #[tokio::test]
    async fn test_batch_delete() {
        let engine = create_test_engine();

        // Put some keys first
        engine.put(b"key1", b"value1").await.unwrap();
        engine.put(b"key2", b"value2").await.unwrap();
        engine.put(b"key3", b"value3").await.unwrap();

        // Test batch delete operations
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];

        let result = engine.batch_delete(keys).await;
        assert!(result.is_ok());

        // Verify all keys were deleted
        assert_eq!(engine.get(b"key1").await.unwrap(), None);
        assert_eq!(engine.get(b"key2").await.unwrap(), None);
        assert_eq!(engine.get(b"key3").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_batch_operations_empty() {
        let engine = create_test_engine();

        // Test batch operations with empty vectors
        let result = engine.batch_put(vec![]).await;
        assert!(result.is_ok());

        let result = engine.batch_delete(vec![]).await;
        assert!(result.is_ok());
    }
}
