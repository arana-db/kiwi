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
use openraft::{SnapshotMeta, StorageError, StoredMembership};
use openraft::storage::RaftStateMachine;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use engine::{Engine, RocksdbEngine};
use resp::command::{RespCommand, CommandType};
use resp::types::RespData;
use crate::consistency::{ConsistencyChecker, ConsistencyStatus};
use crate::error::{RaftError, RaftResult};
use crate::types::{TypeConfig, NodeId, ClientRequest, ClientResponse, RequestId};

/// Snapshot data structure for state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    pub data: HashMap<String, Vec<u8>>,
    pub applied_index: u64,
}

/// Kiwi state machine for Raft integration
pub struct KiwiStateMachine {
    /// The underlying database engine
    engine: Arc<RocksdbEngine>,
    /// Last applied log index
    applied_index: AtomicU64,
    /// Snapshot storage
    snapshot_data: Arc<RwLock<Option<StateMachineSnapshot>>>,
    /// Consistency checker for state verification
    consistency_checker: Arc<ConsistencyChecker>,
    /// Node ID for logging and identification
    node_id: NodeId,
}

impl KiwiStateMachine {
    /// Create a new state machine instance
    pub fn new(engine: Arc<RocksdbEngine>, node_id: NodeId) -> Self {
        let consistency_checker = Arc::new(ConsistencyChecker::new(Arc::clone(&engine), node_id));
        
        Self {
            engine,
            applied_index: AtomicU64::new(0),
            snapshot_data: Arc::new(RwLock::new(None)),
            consistency_checker,
            node_id,
        }
    }

    /// Get the last applied index
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    /// Apply a Redis command to the database engine
    async fn apply_redis_command(&self, command: &ClientRequest) -> RaftResult<ClientResponse> {
        let redis_cmd = &command.command;
        
        // Convert to RespCommand for processing
        let resp_command = RespCommand::new(
            self.parse_command_type(&redis_cmd.command)?,
            redis_cmd.args.clone(),
            false,
        );

        // Apply the command based on its type
        let result = match resp_command.command_type {
            CommandType::Set => self.handle_set_command(&resp_command).await,
            CommandType::Get => self.handle_get_command(&resp_command).await,
            CommandType::Del => self.handle_del_command(&resp_command).await,
            CommandType::Exists => self.handle_exists_command(&resp_command).await,
            CommandType::Ping => self.handle_ping_command().await,
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

    /// Parse command string to CommandType
    fn parse_command_type(&self, command: &str) -> RaftResult<CommandType> {
        use std::str::FromStr;
        CommandType::from_str(command).map_err(|_| {
            RaftError::state_machine(format!("Invalid command: {}", command))
        })
    }

    /// Handle SET command
    async fn handle_set_command(&self, cmd: &RespCommand) -> RaftResult<bytes::Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request("SET requires key and value"));
        }

        let key = &cmd.args[0];
        let value = &cmd.args[1];

        self.engine
            .put(key, value)
            .map_err(|e| RaftError::state_machine(format!("SET failed: {}", e)))?;

        Ok(bytes::Bytes::from_static(b"OK"))
    }

    /// Handle GET command
    async fn handle_get_command(&self, cmd: &RespCommand) -> RaftResult<bytes::Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("GET requires key"));
        }

        let key = &cmd.args[0];
        
        match self.engine
            .get(key)
            .map_err(|e| RaftError::state_machine(format!("GET failed: {}", e)))?
        {
            Some(value) => Ok(bytes::Bytes::from(value)),
            None => Ok(bytes::Bytes::new()), // Redis returns null for missing keys
        }
    }

    /// Handle DEL command
    async fn handle_del_command(&self, cmd: &RespCommand) -> RaftResult<bytes::Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("DEL requires at least one key"));
        }

        let mut deleted_count = 0u64;
        for key in &cmd.args {
            // Check if key exists before deletion
            if self.engine
                .get(key)
                .map_err(|e| RaftError::state_machine(format!("DEL check failed: {}", e)))?
                .is_some()
            {
                self.engine
                    .delete(key)
                    .map_err(|e| RaftError::state_machine(format!("DEL failed: {}", e)))?;
                deleted_count += 1;
            }
        }

        Ok(bytes::Bytes::from(deleted_count.to_string()))
    }

    /// Handle EXISTS command
    async fn handle_exists_command(&self, cmd: &RespCommand) -> RaftResult<bytes::Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("EXISTS requires at least one key"));
        }

        let mut exists_count = 0u64;
        for key in &cmd.args {
            if self.engine
                .get(key)
                .map_err(|e| RaftError::state_machine(format!("EXISTS check failed: {}", e)))?
                .is_some()
            {
                exists_count += 1;
            }
        }

        Ok(bytes::Bytes::from(exists_count.to_string()))
    }

    /// Handle PING command
    async fn handle_ping_command(&self) -> RaftResult<bytes::Bytes> {
        Ok(bytes::Bytes::from_static(b"PONG"))
    }

    /// Create a snapshot of the current state
    async fn create_snapshot(&self) -> RaftResult<StateMachineSnapshot> {
        use rocksdb::IteratorMode;
        
        let mut data = HashMap::new();
        let iter = self.engine.iterator(IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item.map_err(|e| {
                RaftError::state_machine(format!("Snapshot iteration failed: {}", e))
            })?;
            
            let key_str = String::from_utf8_lossy(&key).to_string();
            data.insert(key_str, value);
        }

        Ok(StateMachineSnapshot {
            data,
            applied_index: self.applied_index(),
        })
    }

    /// Restore state from snapshot
    async fn restore_from_snapshot(&self, snapshot: &StateMachineSnapshot) -> RaftResult<()> {
        // Clear existing data (in a real implementation, this might be more sophisticated)
        // For now, we'll just restore the snapshot data
        for (key, value) in &snapshot.data {
            self.engine
                .put(key.as_bytes(), value)
                .map_err(|e| RaftError::state_machine(format!("Snapshot restore failed: {}", e)))?;
        }

        self.applied_index.store(snapshot.applied_index, Ordering::Release);
        Ok(())
    }

    /// Verify state machine consistency
    pub async fn verify_consistency(&self, expected_log_index: u64) -> RaftResult<ConsistencyStatus> {
        self.consistency_checker.verify_consistency(expected_log_index).await
    }

    /// Perform deep consistency check
    pub async fn deep_consistency_check(&self) -> RaftResult<ConsistencyStatus> {
        self.consistency_checker.deep_consistency_check().await
    }

    /// Recover from consistency issues
    pub async fn recover_consistency(&self, target_index: u64) -> RaftResult<()> {
        self.consistency_checker.recover_consistency(target_index).await
    }

    /// Get consistency checker for external monitoring
    pub fn consistency_checker(&self) -> Arc<ConsistencyChecker> {
        Arc::clone(&self.consistency_checker)
    }
}

#[async_trait]
impl RaftStateMachine<TypeConfig> for KiwiStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<openraft::LogId<NodeId>>, StoredMembership<NodeId, openraft::BasicNode>), StorageError<NodeId>> {
        // Return the current applied state
        let applied_index = self.applied_index();
        let log_id = if applied_index > 0 {
            Some(openraft::LogId::new(openraft::CommittedLeaderId::new(1, NodeId::default()), applied_index))
        } else {
            None
        };
        
        // For now, return empty membership - this should be properly implemented
        let membership = StoredMembership::default();
        
        Ok((log_id, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClientResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
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
                    let response = self.apply_redis_command(&request).await
                        .map_err(|e| StorageError::read_state_machine(&e))?;
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

        // Perform periodic consistency check (every 100 entries)
        if last_applied_index % 100 == 0 {
            if let Err(e) = self.verify_consistency(last_applied_index).await {
                log::error!("Node {} consistency check failed: {}", self.node_id, e);
            }
        }
        
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotBuilder>, StorageError<NodeId>> {
        // Return a clone of self for snapshot building
        Ok(Box::new(Self {
            engine: Arc::clone(&self.engine),
            applied_index: AtomicU64::new(self.applied_index()),
            snapshot_data: Arc::clone(&self.snapshot_data),
            consistency_checker: Arc::clone(&self.consistency_checker),
            node_id: self.node_id,
        }))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Self::SnapshotBuilder>,
    ) -> Result<(), StorageError<NodeId>> {
        // Install the snapshot by copying the state
        let snapshot_data = snapshot.snapshot_data.read().await;
        if let Some(ref snap) = *snapshot_data {
            self.restore_from_snapshot(snap).await
                .map_err(|e| StorageError::read_state_machine(&e))?;
        }
        
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::Snapshot<NodeId, openraft::BasicNode, Self::SnapshotBuilder>>, StorageError<NodeId>> {
        // For now, return None - snapshot creation will be implemented in a future task
        Ok(None)
    }
}