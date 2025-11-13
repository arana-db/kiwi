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

//! In-memory storage engine for testing

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

use crate::error::RaftResult;
use crate::state_machine::StorageEngine;

/// Simple in-memory storage engine for testing
pub struct MemoryStorageEngine {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryStorageEngine {
    /// Create a new in-memory storage engine
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StorageEngine for MemoryStorageEngine {
    async fn get(&self, key: &[u8]) -> RaftResult<Option<Vec<u8>>> {
        let data = self.data.read();
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> RaftResult<()> {
        let mut data = self.data.write();
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> RaftResult<()> {
        let mut data = self.data.write();
        data.remove(key);
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<Vec<u8>> {
        let data = self.data.read();
        // Serialize the entire HashMap as a snapshot
        bincode::serialize(&*data).map_err(|e| {
            crate::error::RaftError::Storage(crate::error::StorageError::SnapshotCreationFailed {
                message: format!("Failed to serialize snapshot: {}", e),
                snapshot_id: "memory".to_string(),
                context: "MemoryStorageEngine::create_snapshot".to_string(),
            })
        })
    }

    async fn restore_from_snapshot(&self, snapshot_data: &[u8]) -> RaftResult<()> {
        let restored_data: HashMap<Vec<u8>, Vec<u8>> = bincode::deserialize(snapshot_data)
            .map_err(|e| {
                crate::error::RaftError::Storage(
                    crate::error::StorageError::SnapshotRestorationFailed {
                        message: format!("Failed to deserialize snapshot: {}", e),
                        snapshot_id: "memory".to_string(),
                        context: "MemoryStorageEngine::restore_from_snapshot".to_string(),
                    },
                )
            })?;

        let mut data = self.data.write();
        *data = restored_data;
        Ok(())
    }

    async fn batch_put(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> RaftResult<()> {
        let mut data = self.data.write();
        for (key, value) in pairs {
            data.insert(key, value);
        }
        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> RaftResult<()> {
        let mut data = self.data.write();
        for key in keys {
            data.remove(&key);
        }
        Ok(())
    }
}
