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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use openraft::EffectiveMembership;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::error::{RaftError, RaftResult};
use crate::types::{ClientRequest, ClientResponse, NodeId, RedisCommand};

/// Snapshot data structure for state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    pub data: HashMap<Vec<u8>, Vec<u8>>,
    pub applied_index: u64,
}

/// Trait for storage engine operations
#[async_trait::async_trait]
pub trait StorageEngine: Send + Sync {
    /// Get a value by key
    async fn get(&self, key: &[u8]) -> RaftResult<Option<Vec<u8>>>;

    /// Put a key-value pair
    async fn put(&self, key: &[u8], value: &[u8]) -> RaftResult<()>;

    /// Delete a key
    async fn delete(&self, key: &[u8]) -> RaftResult<()>;

    /// Create a consistent snapshot for backup/restore
    async fn create_snapshot(&self) -> RaftResult<Vec<u8>>;

    /// Restore from a snapshot
    async fn restore_from_snapshot(&self, snapshot_data: &[u8]) -> RaftResult<()>;
}

/// Kiwi state machine for Raft integration
pub struct KiwiStateMachine {
    /// Last applied log index
    applied_index: AtomicU64,
    /// Snapshot storage
    snapshot_data: Arc<RwLock<Option<StateMachineSnapshot>>>,
    /// Node ID for logging and identification
    pub node_id: NodeId,
    /// Mutex to ensure atomic application of log entries
    apply_mutex: Arc<Mutex<()>>,
    /// Storage engine reference for operations
    storage_engine: Option<Arc<dyn StorageEngine>>,
}

impl KiwiStateMachine {
    /// Create a new state machine instance
    pub fn new(node_id: NodeId) -> Self {
        Self {
            applied_index: AtomicU64::new(0),
            snapshot_data: Arc::new(RwLock::new(None)),
            node_id,
            apply_mutex: Arc::new(Mutex::new(())),
            storage_engine: None,
        }
    }

    /// Create a new state machine instance with storage engine
    pub fn with_storage_engine(node_id: NodeId, storage_engine: Arc<dyn StorageEngine>) -> Self {
        Self {
            applied_index: AtomicU64::new(0),
            snapshot_data: Arc::new(RwLock::new(None)),
            node_id,
            apply_mutex: Arc::new(Mutex::new(())),
            storage_engine: Some(storage_engine),
        }
    }

    /// Get the last applied index
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    /// Apply a Redis command to the database engine
    pub async fn apply_redis_command(&self, command: &ClientRequest) -> RaftResult<ClientResponse> {
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

        let key = &cmd.args[0];
        let value = &cmd.args[1];

        if let Some(storage_engine) = &self.storage_engine {
            storage_engine.put(key, value).await?;
        }

        Ok(Bytes::from("OK"))
    }

    /// Handle GET command
    async fn handle_get_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("GET requires key"));
        }

        let key = &cmd.args[0];

        if let Some(storage_engine) = &self.storage_engine {
            match storage_engine.get(key).await? {
                Some(value) => Ok(Bytes::from(value)),
                None => Ok(Bytes::new()), // Return null for non-existent key
            }
        } else {
            Ok(Bytes::new()) // Return null when no storage engine
        }
    }

    /// Handle DEL command
    async fn handle_del_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("DEL requires at least one key"));
        }

        let mut deleted_count = 0;

        if let Some(storage_engine) = &self.storage_engine {
            for key in &cmd.args {
                // Check if key exists before deleting
                if storage_engine.get(key).await?.is_some() {
                    storage_engine.delete(key).await?;
                    deleted_count += 1;
                }
            }
        }

        Ok(Bytes::from(deleted_count.to_string()))
    }

    /// Handle EXISTS command
    async fn handle_exists_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request(
                "EXISTS requires at least one key",
            ));
        }

        let mut exists_count = 0;

        if let Some(storage_engine) = &self.storage_engine {
            for key in &cmd.args {
                if storage_engine.get(key).await?.is_some() {
                    exists_count += 1;
                }
            }
        }

        Ok(Bytes::from(exists_count.to_string()))
    }

    /// Handle PING command
    async fn handle_ping_command(&self) -> RaftResult<Bytes> {
        Ok(Bytes::from_static(b"PONG"))
    }

    /// Get the current applied state for Raft
    pub async fn get_applied_state(
        &self,
    ) -> (
        Option<openraft::LogId<NodeId>>,
        EffectiveMembership<NodeId, openraft::BasicNode>,
    ) {
        let applied_index = self.applied_index();

        // TODO: Source the actual applied term from Raft state instead of hardcoding term=1
        let log_id = if applied_index > 0 {
            Some(openraft::LogId::new(
                openraft::CommittedLeaderId::new(1, self.node_id),
                applied_index,
            ))
        } else {
            None
        };

        // Return empty membership for now - this should also be sourced from actual Raft state
        let membership = EffectiveMembership::new(None, openraft::Membership::new(vec![], None));

        (log_id, membership)
    }

    /// Create a snapshot of the current state
    pub async fn create_snapshot(&self) -> RaftResult<StateMachineSnapshot> {
        log::info!(
            "Node {} creating snapshot at applied index {}",
            self.node_id,
            self.applied_index()
        );

        let snapshot_data = HashMap::new();

        let snapshot = StateMachineSnapshot {
            data: snapshot_data,
            applied_index: self.applied_index(),
        };

        // Store the snapshot in memory for quick access
        {
            let mut snapshot_store = self.snapshot_data.write().await;
            *snapshot_store = Some(snapshot.clone());
        }

        log::info!(
            "Node {} successfully created snapshot at index {}",
            self.node_id,
            snapshot.applied_index
        );
        Ok(snapshot)
    }

    /// Restore state from snapshot
    pub async fn restore_from_snapshot(&self, snapshot: &StateMachineSnapshot) -> RaftResult<()> {
        log::info!(
            "Node {} restoring from snapshot at index {}",
            self.node_id,
            snapshot.applied_index
        );

        // Acquire the apply mutex to ensure no concurrent operations during restore
        let _apply_lock = self.apply_mutex.lock().await;

        // If we have a storage engine, restore the data
        if let Some(storage_engine) = &self.storage_engine {
            log::debug!(
                "Node {} restoring snapshot data to storage engine",
                self.node_id
            );

            // Restore all key-value pairs from the snapshot
            for (key, value) in &snapshot.data {
                storage_engine.put(key, value).await?;
            }

            log::debug!(
                "Node {} successfully restored {} keys from snapshot",
                self.node_id,
                snapshot.data.len()
            );
        } else {
            log::debug!(
                "Node {} skipping data restore (no storage engine)",
                self.node_id
            );
        }

        // Update the applied index after successful restore
        self.applied_index
            .store(snapshot.applied_index, Ordering::Release);

        // Store the snapshot in memory
        {
            let mut snapshot_store = self.snapshot_data.write().await;
            *snapshot_store = Some(snapshot.clone());
        }

        log::info!(
            "Node {} successfully restored from snapshot at index {}",
            self.node_id,
            snapshot.applied_index
        );
        Ok(())
    }

    /// Get current snapshot data
    pub async fn get_current_snapshot_data(&self) -> RaftResult<Option<StateMachineSnapshot>> {
        let snapshot_store = self.snapshot_data.read().await;
        Ok(snapshot_store.clone())
    }
}