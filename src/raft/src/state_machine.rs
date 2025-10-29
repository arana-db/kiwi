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

//! Raft state machine implementation

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::collections::HashMap;

use async_trait::async_trait;
use engine::RocksdbEngine;
use openraft::{SnapshotMeta, StorageError, EffectiveMembership};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use bytes::Bytes;

use crate::error::{RaftError, RaftResult};
use crate::types::{TypeConfig, NodeId, ClientRequest, ClientResponse, RequestId, RedisCommand};

/// Snapshot data structure for state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    pub data: HashMap<String, Vec<u8>>,
    pub applied_index: u64,
}

/// Kiwi state machine for Raft integration
pub struct KiwiStateMachine {
    /// In-memory data store (will be replaced with RocksDB later)
    data: Arc<RwLock<HashMap<String, Bytes>>>,
    /// Last applied log index
    applied_index: AtomicU64,
    /// Snapshot storage
    snapshot_data: Arc<RwLock<Option<StateMachineSnapshot>>>,
    /// Node ID for logging and identification
    node_id: NodeId,
    /// Storage engine (reserved for future persistence)
    #[allow(dead_code)]
    engine: Arc<RocksdbEngine>,
}

impl KiwiStateMachine {
    /// Create a new state machine instance
    pub fn new(engine: Arc<RocksdbEngine>, node_id: NodeId) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            applied_index: AtomicU64::new(0),
            snapshot_data: Arc::new(RwLock::new(None)),
            node_id,
            engine,
        }
    }

    /// Get the last applied index
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    /// Apply a Redis command to the database engine
    async fn apply_redis_command(&self, command: &ClientRequest) -> RaftResult<ClientResponse> {
        let redis_cmd = &command.command;
        
        // Apply the command based on its type
        let result = match redis_cmd.command.to_uppercase().as_str() {
            "SET" => self.handle_set_command(redis_cmd).await,
            "GET" => self.handle_get_command(redis_cmd).await,
            "DEL" => self.handle_del_command(redis_cmd).await,
            "EXISTS" => self.handle_exists_command(redis_cmd).await,
            "PING" => self.handle_ping_command().await,
            _ => Err(RaftError::state_machine(format!(
                "Unsupported command: {}",
                redis_cmd.command
            ))),
        };

        Ok(ClientResponse {
            id: command.id,
            result: result.map_err(|e| e.to_string()),
            leader_id: None,
        })
    }

    /// Handle SET command
    async fn handle_set_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request("SET requires key and value"));
        }

        let key = String::from_utf8_lossy(&cmd.args[0]).to_string();
        let value = cmd.args[1].clone();

        let mut data = self.data.write().await;
        data.insert(key, value);

        Ok(Bytes::from_static(b"OK"))
    }

    /// Handle GET command
    async fn handle_get_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("GET requires key"));
        }

        let key = String::from_utf8_lossy(&cmd.args[0]).to_string();
        
        let data = self.data.read().await;
        match data.get(&key) {
            Some(value) => Ok(value.clone()),
            None => Ok(Bytes::new()), // Redis returns null for missing keys
        }
    }

    /// Handle DEL command
    async fn handle_del_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("DEL requires at least one key"));
        }

        let mut data = self.data.write().await;
        let mut deleted_count = 0u64;
        
        for key_bytes in &cmd.args {
            let key = String::from_utf8_lossy(key_bytes).to_string();
            if data.remove(&key).is_some() {
                deleted_count += 1;
            }
        }

        Ok(Bytes::from(deleted_count.to_string()))
    }

    /// Handle EXISTS command
    async fn handle_exists_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("EXISTS requires at least one key"));
        }

        let data = self.data.read().await;
        let mut exists_count = 0u64;
        
        for key_bytes in &cmd.args {
            let key = String::from_utf8_lossy(key_bytes).to_string();
            if data.contains_key(&key) {
                exists_count += 1;
            }
        }

        Ok(Bytes::from(exists_count.to_string()))
    }

    /// Handle PING command
    async fn handle_ping_command(&self) -> RaftResult<Bytes> {
        Ok(Bytes::from_static(b"PONG"))
    }

    /// Create a snapshot of the current state
    async fn create_snapshot(&self) -> RaftResult<StateMachineSnapshot> {
        let data_guard = self.data.read().await;
        let mut snapshot_data = HashMap::new();
        
        for (key, value) in data_guard.iter() {
            snapshot_data.insert(key.clone(), value.to_vec());
        }

        Ok(StateMachineSnapshot {
            data: snapshot_data,
            applied_index: self.applied_index(),
        })
    }

    /// Restore state from snapshot
    async fn restore_from_snapshot(&self, snapshot: &StateMachineSnapshot) -> RaftResult<()> {
        let mut data = self.data.write().await;
        data.clear();
        
        for (key, value) in &snapshot.data {
            data.insert(key.clone(), Bytes::from(value.clone()));
        }

        self.applied_index.store(snapshot.applied_index, Ordering::Release);
        Ok(())
    }

    /// Get current data size for monitoring
    pub async fn data_size(&self) -> usize {
        self.data.read().await.len()
    }
    
    /// Get all keys for debugging
    pub async fn get_all_keys(&self) -> Vec<String> {
        self.data.read().await.keys().cloned().collect()
    }
}

// Note: We'll implement RaftStateMachine using the Adaptor pattern later
// For now, let's create a basic implementation that can be wrapped

impl KiwiStateMachine {
    /// Get the current applied state for Raft
    pub async fn get_applied_state(&self) -> (Option<openraft::LogId<NodeId>>, EffectiveMembership<NodeId, openraft::BasicNode>) {
        let applied_index = self.applied_index();
        // Note: This function is currently unused but exported for future integration with RaftStateMachine Adaptor.
        // The term is placeholder (1) since we don't track applied term yet. This should be sourced from actual
        // Raft state before this method is used in production.
        let log_id = if applied_index > 0 {
            Some(openraft::LogId::new(openraft::CommittedLeaderId::new(1, self.node_id), applied_index))
        } else {
            None
        };
        
        // Return empty membership for now
        let membership = EffectiveMembership::new(None, openraft::Membership::new(vec![], None));
        
        (log_id, membership)
    }

    /// Apply log entries to the state machine
    pub async fn apply_entries<I>(&mut self, entries: I) -> RaftResult<Vec<ClientResponse>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>>,
    {
        let mut responses = Vec::new();
        let mut last_applied_index = self.applied_index();
        
        for entry in entries {
            // Verify that we're applying entries in order
            if entry.log_id.index != last_applied_index + 1 {
                log::warn!(
                    "Node {} applying entry {} but expected {}",
                    self.node_id,
                    entry.log_id.index,
                    last_applied_index + 1
                );
            }

            match entry.payload {
                openraft::EntryPayload::Blank => {
                    // Blank entries don't need processing
                    log::debug!("Node {} applying blank entry at index {}", self.node_id, entry.log_id.index);
                }
                openraft::EntryPayload::Normal(request) => {
                    log::debug!("Node {} applying command entry at index {}", self.node_id, entry.log_id.index);
                    let response = self.apply_redis_command(&request).await?;
                    responses.push(response);
                }
                openraft::EntryPayload::Membership(_) => {
                    // Membership changes are handled by openraft
                    log::debug!("Node {} applying membership entry at index {}", self.node_id, entry.log_id.index);
                }
            }
            
            // Update applied index
            last_applied_index = entry.log_id.index;
            self.applied_index.store(last_applied_index, Ordering::Release);
        }
        
        Ok(responses)
    }

    /// Begin receiving a snapshot
    pub async fn begin_receiving_snapshot(&self) -> RaftResult<KiwiStateMachine> {
        // Return a clone of self for snapshot building
        Ok(Self {
            data: Arc::clone(&self.data),
            applied_index: AtomicU64::new(self.applied_index()),
            snapshot_data: Arc::clone(&self.snapshot_data),
            node_id: self.node_id,
            engine: Arc::clone(&self.engine),
        })
    }

    /// Install a snapshot
    pub async fn install_snapshot_data(
        &mut self,
        _meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: &KiwiStateMachine,
    ) -> RaftResult<()> {
        // Install the snapshot by copying the state
        let snapshot_data = snapshot.snapshot_data.read().await;
        if let Some(ref snap) = *snapshot_data {
            self.restore_from_snapshot(snap).await?;
        }
        
        Ok(())
    }

    /// Get current snapshot (placeholder implementation)
    pub async fn get_current_snapshot_data(&self) -> RaftResult<Option<StateMachineSnapshot>> {
        // For now, return None - snapshot creation will be implemented later
        Ok(None)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use engine::RocksdbEngine;
    use crate::types::{ClientRequest, RedisCommand, ConsistencyLevel, RequestId};
    use bytes::Bytes;

    fn create_test_engine() -> (Arc<RocksdbEngine>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RocksdbEngine::new(temp_dir.path()).unwrap());
        (engine, temp_dir)
    }

    fn create_test_state_machine() -> (KiwiStateMachine, TempDir) {
        let (engine, temp_dir) = create_test_engine();
        let state_machine = KiwiStateMachine::new(engine, 1);
        (state_machine, temp_dir)
    }

    fn create_test_request(id: RequestId, command: &str, args: Vec<Bytes>) -> ClientRequest {
        ClientRequest {
            id,
            command: RedisCommand {
                command: command.to_string(),
                args,
            },
            consistency_level: ConsistencyLevel::Linearizable,
        }
    }

    #[tokio::test]
    async fn test_state_machine_creation() {
        let (state_machine, _temp_dir) = create_test_state_machine();
        assert_eq!(state_machine.applied_index(), 0);
        assert_eq!(state_machine.node_id, 1);
    }

    #[tokio::test]
    async fn test_set_command() {
        let (state_machine, _temp_dir) = create_test_state_machine();
        
        let request = create_test_request(RequestId(1), "SET", vec![Bytes::from("key1"), Bytes::from("value1")]);
        let response = state_machine.apply_redis_command(&request).await.unwrap();
        
        assert_eq!(response.id, RequestId(1));
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::from_static(b"OK"));
    }

    #[tokio::test]
    async fn test_get_command() {
        let (state_machine, _temp_dir) = create_test_state_machine();
        
        // First set a value
        let set_request = create_test_request(RequestId(1), "SET", vec![Bytes::from("key1"), Bytes::from("value1")]);
        state_machine.apply_redis_command(&set_request).await.unwrap();
        
        // Then get the value
        let get_request = create_test_request(RequestId(2), "GET", vec![Bytes::from("key1")]);
        let response = state_machine.apply_redis_command(&get_request).await.unwrap();
        
        assert_eq!(response.id, RequestId(2));
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::from("value1"));
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let (state_machine, _temp_dir) = create_test_state_machine();
        
        let get_request = create_test_request(RequestId(1), "GET", vec![Bytes::from("nonexistent")]);
        let response = state_machine.apply_redis_command(&get_request).await.unwrap();
        
        assert_eq!(response.id, RequestId(1));
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::new());
    }

    #[tokio::test]
    async fn test_del_command() {
        let (state_machine, _temp_dir) = create_test_state_machine();
        
        // Set some values
        let set1 = create_test_request(RequestId(1), "SET", vec![Bytes::from("key1"), Bytes::from("value1")]);
        let set2 = create_test_request(RequestId(2), "SET", vec![Bytes::from("key2"), Bytes::from("value2")]);
        state_machine.apply_redis_command(&set1).await.unwrap();
        state_machine.apply_redis_command(&set2).await.unwrap();
        
        // Delete one key
        let del_request = create_test_request(RequestId(3), "DEL", vec![Bytes::from("key1")]);
        let response = state_machine.apply_redis_command(&del_request).await.unwrap();
        
        assert_eq!(response.id, RequestId(3));
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::from("1"));
        
        // Verify key is deleted
        let get_request = create_test_request(RequestId(4), "GET", vec![Bytes::from("key1")]);
        let get_response = state_machine.apply_redis_command(&get_request).await.unwrap();
        assert_eq!(get_response.result.unwrap(), bytes::Bytes::new());
    }

    #[tokio::test]
    async fn test_ping_command() {
        let (state_machine, _temp_dir) = create_test_state_machine();
        
        let ping_request = create_test_request(RequestId(1), "PING", vec![]);
        let response = state_machine.apply_redis_command(&ping_request).await.unwrap();
        
        assert_eq!(response.id, RequestId(1));
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::from_static(b"PONG"));
    }

    #[tokio::test]
    async fn test_invalid_command() {
        let (state_machine, _temp_dir) = create_test_state_machine();
        
        let invalid_request = create_test_request(RequestId(1), "INVALID", vec![]);
        let response = state_machine.apply_redis_command(&invalid_request).await.unwrap();
        
        assert_eq!(response.id, RequestId(1));
        assert!(response.result.is_err());
        assert!(response.result.unwrap_err().contains("Unsupported command"));
    }

    #[tokio::test]
    async fn test_applied_index_tracking() {
        let (mut state_machine, _temp_dir) = create_test_state_machine();
        
        // Initially applied index should be 0
        assert_eq!(state_machine.applied_index(), 0);
        
        // Simulate applying entries (this would normally be done by openraft)
        state_machine.applied_index.store(10, Ordering::Release);
        assert_eq!(state_machine.applied_index(), 10);
        
        state_machine.applied_index.store(25, Ordering::Release);
        assert_eq!(state_machine.applied_index(), 25);
    }

    #[tokio::test]
    async fn test_snapshot_creation() {
        let (state_machine, _temp_dir) = create_test_state_machine();
        
        // Add some data
        for i in 1..=5 {
            let set_req = create_test_request(i, "SET", vec![format!("key{}", i), format!("value{}", i)]);
            state_machine.apply_redis_command(&set_req).await.unwrap();
        }
        
        // Update applied index
        state_machine.applied_index.store(5, Ordering::Release);
        
        // Create snapshot
        let snapshot = state_machine.create_snapshot().await.unwrap();
        
        assert_eq!(snapshot.applied_index, 5);
        assert_eq!(snapshot.data.len(), 5);
        
        for i in 1..=5 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            assert_eq!(snapshot.data.get(&key).unwrap(), expected_value.as_bytes());
        }
    }

    // Test for consistency verification removed - API does not exist yet
    // TODO: Add test when ConsistencyChecker integration is complete
}