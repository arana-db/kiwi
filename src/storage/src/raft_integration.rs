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

//! Raft integration for Redis storage
//!
//! This module provides the integration between the storage layer and the Raft consensus layer.

use crate::Redis;
use crate::redis_for_raft::RedisForRaft;
use std::sync::Arc;

/// Implement RedisOperations trait for RedisForRaft
/// 
/// This implementation directly calls the raft_* methods on RedisForRaft,
/// which in turn call Redis's inherent methods. The raft_* prefix avoids
/// method name conflicts with the trait methods.
impl raft::storage_engine::RedisOperations for RedisForRaft {
    fn raft_get_binary(&self, key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        RedisForRaft::raft_get_binary(self, key)
            .map_err(|e| {
                let err_msg = format!("{}", e);
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, err_msg)) as Box<dyn std::error::Error + Send + Sync>
            })
    }

    fn raft_set(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        RedisForRaft::raft_set(self, key, value)
            .map_err(|e| {
                let err_msg = format!("{}", e);
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, err_msg)) as Box<dyn std::error::Error + Send + Sync>
            })
    }

    fn raft_del(&self, keys: &[&[u8]]) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
        let mut deleted_count = 0;
        
        for &key in keys {
            match RedisForRaft::raft_del(self, key) {
                Ok(count) => deleted_count += count,
                Err(e) => {
                    let err_msg = format!("{}", e);
                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, err_msg)) as Box<dyn std::error::Error + Send + Sync>);
                }
            }
        }
        
        Ok(deleted_count)
    }

    fn raft_mset(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        RedisForRaft::raft_mset(self, pairs)
            .map_err(|e| {
                let err_msg = format!("{}", e);
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, err_msg)) as Box<dyn std::error::Error + Send + Sync>
            })
    }
}

/// Helper function to create RedisStorageEngine from Redis
///
/// This is the main entry point for creating a Raft-compatible storage engine
/// from a Redis instance.
///
/// # Arguments
/// * `redis` - Arc reference to the Redis storage instance
///
/// # Returns
/// A RedisStorageEngine that can be used with KiwiStateMachine
///
/// # Example
/// ```ignore
/// let redis = Arc::new(Redis::new(...));
/// let storage_engine = create_raft_storage_engine(redis);
/// let state_machine = KiwiStateMachine::with_storage_engine(node_id, Arc::new(storage_engine));
/// ```
pub fn create_raft_storage_engine(redis: Arc<Redis>) -> raft::storage_engine::RedisStorageEngine {
    let redis_for_raft = Arc::new(RedisForRaft::new(redis));
    let adapter = raft::storage_engine::RedisStorageAdapter::new(redis_for_raft);
    raft::storage_engine::RedisStorageEngine::new(Arc::new(adapter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageOptions, unique_test_db_path, safe_cleanup_test_db};
    use kstd::lock_mgr::LockMgr;

    fn create_test_redis() -> Arc<Redis> {
        let db_path = unique_test_db_path("raft_integration_test");
        let options = Arc::new(StorageOptions::default());
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let bg_task_handler = Arc::new(crate::BgTaskHandler::new().0);
        
        let mut redis = Redis::new(options, 0, bg_task_handler, lock_mgr);
        redis.open(db_path.to_str().unwrap()).expect("Failed to open Redis");
        
        Arc::new(redis)
    }

    #[test]
    fn test_redis_operations_trait() {
        let redis = create_test_redis();
        let redis_for_raft = RedisForRaft::new(redis.clone());
        
        // Test through trait
        let ops: &dyn raft::storage_engine::RedisOperations = &redis_for_raft;
        
        // Test set
        ops.raft_set(b"test_key", b"test_value").unwrap();
        
        // Test get
        let value = ops.raft_get_binary(b"test_key").unwrap();
        assert_eq!(value, b"test_value");
        
        // Test del
        let deleted = ops.raft_del(&[b"test_key"]).unwrap();
        assert_eq!(deleted, 1);
        
        // Verify deleted
        let result = ops.raft_get_binary(b"test_key");
        assert!(result.is_err());
        
        // Cleanup
        safe_cleanup_test_db(&redis);
    }

    #[test]
    fn test_create_raft_storage_engine() {
        let redis = create_test_redis();
        let _engine = create_raft_storage_engine(redis.clone());
        
        // Just verify it compiles and creates successfully
        safe_cleanup_test_db(&redis);
    }

    #[test]
    fn test_mset_operation() {
        let redis = create_test_redis();
        let redis_for_raft = RedisForRaft::new(redis.clone());
        let ops: &dyn raft::storage_engine::RedisOperations = &redis_for_raft;
        
        // Test mset
        let pairs = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];
        
        ops.raft_mset(&pairs).unwrap();
        
        // Verify all keys were set
        assert_eq!(ops.raft_get_binary(b"key1").unwrap(), b"value1");
        assert_eq!(ops.raft_get_binary(b"key2").unwrap(), b"value2");
        assert_eq!(ops.raft_get_binary(b"key3").unwrap(), b"value3");
        
        // Cleanup
        safe_cleanup_test_db(&redis);
    }
}
