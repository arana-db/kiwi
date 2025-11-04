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

//! Cluster-aware storage layer that integrates with Raft consensus

use crate::storage::Storage;
use log::{info, warn};
use std::sync::Arc;

/// Cluster-aware storage wrapper that routes operations through Raft consensus
pub struct ClusterStorage {
    /// Underlying storage implementation
    local_storage: Arc<Storage>,
    /// Raft node for consensus operations
    #[allow(dead_code)]
    raft_node: Arc<dyn Send + Sync>,
    /// Whether this node is currently the leader
    is_leader: std::sync::atomic::AtomicBool,
}

impl ClusterStorage {
    /// Create a new cluster storage instance
    pub fn new(local_storage: Arc<Storage>, raft_node: Arc<dyn Send + Sync>) -> Self {
        Self {
            local_storage,
            raft_node,
            is_leader: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Get the underlying local storage
    pub fn local_storage(&self) -> &Arc<Storage> {
        &self.local_storage
    }

    /// Check if this node is currently the leader
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Update leadership status
    pub fn set_leader_status(&self, is_leader: bool) {
        self.is_leader
            .store(is_leader, std::sync::atomic::Ordering::Relaxed);
        if is_leader {
            info!("Node became leader");
        } else {
            info!("Node is no longer leader");
        }
    }

    /// Execute a write operation through Raft consensus
    pub async fn execute_write_operation(&self, _operation: &[u8]) -> Result<Vec<u8>, String> {
        if !self.is_leader() {
            return Err("MOVED - not leader".to_string());
        }

        // TODO: Route through Raft consensus
        // For now, execute directly on local storage
        warn!("Write operation executed directly - Raft consensus integration pending");

        // This is a placeholder - actual implementation will route through Raft
        Ok(b"OK".to_vec())
    }

    /// Execute a read operation (can be local or consensus-based depending on consistency level)
    pub async fn execute_read_operation(
        &self,
        _operation: &[u8],
        linearizable: bool,
    ) -> Result<Vec<u8>, String> {
        if linearizable && !self.is_leader() {
            return Err("MOVED - not leader for linearizable reads".to_string());
        }

        // For read operations, we can typically serve from local storage
        // TODO: Implement actual read operation routing
        Ok(b"".to_vec())
    }
}

/// Integration helper for backward compatibility
impl std::ops::Deref for ClusterStorage {
    type Target = Storage;

    fn deref(&self) -> &Self::Target {
        self.local_storage.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    #[test]
    fn test_cluster_storage_creation() {
        let storage = Arc::new(Storage::new(1, 0));
        let raft_node = Arc::new(());
        let cluster_storage = ClusterStorage::new(storage, raft_node);

        assert!(!cluster_storage.is_leader());
    }

    #[test]
    fn test_leader_status_management() {
        let storage = Arc::new(Storage::new(1, 0));
        let raft_node = Arc::new(());
        let cluster_storage = ClusterStorage::new(storage, raft_node);

        cluster_storage.set_leader_status(true);
        assert!(cluster_storage.is_leader());

        cluster_storage.set_leader_status(false);
        assert!(!cluster_storage.is_leader());
    }
}
