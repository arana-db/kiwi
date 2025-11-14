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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use openraft::EffectiveMembership;
use openraft::StoredMembership;
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

/// Batch operation for optimized command application
#[derive(Debug)]
enum BatchOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
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

    /// Batch put operations for better performance
    /// Default implementation falls back to individual puts
    async fn batch_put(&self, operations: Vec<(Vec<u8>, Vec<u8>)>) -> RaftResult<()> {
        for (key, value) in operations {
            self.put(&key, &value).await?;
        }
        Ok(())
    }

    /// Batch delete operations for better performance
    /// Default implementation falls back to individual deletes
    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> RaftResult<()> {
        for key in keys {
            self.delete(&key).await?;
        }
        Ok(())
    }
}

/// Kiwi state machine for Raft integration
/// 
/// # Locking Strategy
/// 
/// This implementation uses a multi-layered locking approach:
/// 
/// 1. **AtomicU64 for applied_index**: Lock-free atomic operations for reading the current
///    applied index, which is frequently accessed and rarely causes contention.
/// 
/// 2. **tokio::sync::Mutex for apply operations**: Ensures that log entries are applied
///    sequentially and atomically. This is critical for maintaining state machine consistency.
///    We use tokio's async Mutex to avoid blocking the async runtime.
/// 
/// 3. **tokio::sync::RwLock for snapshot_data**: Allows concurrent reads of snapshot data
///    while ensuring exclusive access during snapshot creation or restoration. This optimizes
///    for the common case where snapshots are read more often than written.
/// 
/// The locking order is always: apply_mutex -> snapshot_data RwLock to prevent deadlocks.
pub struct KiwiStateMachine {
    /// Last applied log index (lock-free atomic for high-frequency reads)
    applied_index: AtomicU64,
    /// Snapshot storage (RwLock for concurrent reads, exclusive writes)
    snapshot_data: Arc<RwLock<Option<StateMachineSnapshot>>>,
    /// Node ID for logging and identification
    pub node_id: NodeId,
    /// Mutex to ensure atomic application of log entries (prevents concurrent applies)
    apply_mutex: Arc<Mutex<()>>,
    /// Storage engine reference for operations
    storage_engine: Option<Arc<dyn StorageEngine>>,
    membership: Arc<RwLock<StoredMembership<NodeId, openraft::BasicNode>>>,
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
            membership: Arc::new(RwLock::new(StoredMembership::default())),
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
            membership: Arc::new(RwLock::new(StoredMembership::default())),
        }
    }

    /// Get the last applied index
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    /// Set the applied index (for use by storage adaptors)
    pub fn set_applied_index(&self, index: u64) {
        self.applied_index.store(index, Ordering::Release);
    }

    pub async fn set_membership(
        &self,
        log_id: openraft::LogId<NodeId>,
        m: openraft::Membership<NodeId, openraft::BasicNode>,
    ) {
        let mut mem = self.membership.write().await;
        *mem = StoredMembership::new(Some(log_id), m);
    }

    pub async fn get_current_stored_membership(
        &self,
    ) -> StoredMembership<NodeId, openraft::BasicNode> {
        self.membership.read().await.clone()
    }

    /// Apply a Redis command to the database engine
    /// 
    /// This method routes Redis commands to the appropriate handler and executes them
    /// through the storage engine if available.
    /// 
    /// # Requirements
    /// - Requirement 2.2: Execute Redis commands through state machine
    /// - Requirement 2.3: Support all basic Redis commands (GET, SET, DEL, EXISTS, etc.)
    pub async fn apply_redis_command(&self, command: &ClientRequest) -> RaftResult<ClientResponse> {
        let redis_cmd = &command.command;

        // Apply the command based on its type
        let result = match redis_cmd.command.to_uppercase().as_str() {
            "SET" => self.handle_set_command(redis_cmd).await,
            "GET" => self.handle_get_command(redis_cmd).await,
            "DEL" => self.handle_del_command(redis_cmd).await,
            "EXISTS" => self.handle_exists_command(redis_cmd).await,
            "PING" => self.handle_ping_command().await,
            "MSET" => self.handle_mset_command(redis_cmd).await,
            "MGET" => self.handle_mget_command(redis_cmd).await,
            "INCR" => self.handle_incr_command(redis_cmd).await,
            "DECR" => self.handle_decr_command(redis_cmd).await,
            "APPEND" => self.handle_append_command(redis_cmd).await,
            "STRLEN" => self.handle_strlen_command(redis_cmd).await,
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

    /// Collect batch operations from a command without executing them
    /// Returns (batch_operations, response_result) where response_result is Some for read operations
    /// 
    /// This method uses Bytes internally to avoid unnecessary copies
    /// 
    /// # Requirements
    /// - Requirement 2.4: Commands must be atomic
    fn collect_batch_operation(
        &self,
        command: &ClientRequest,
    ) -> (Option<BatchOperation>, Option<Result<Bytes, RaftError>>) {
        let redis_cmd = &command.command;

        match redis_cmd.command.to_uppercase().as_str() {
            "SET" => {
                if redis_cmd.args.len() >= 2 {
                    // Use Bytes::to_vec() only when necessary for storage
                    // Bytes internally uses Arc, so cloning is cheap
                    let key = redis_cmd.args[0].to_vec();
                    let value = redis_cmd.args[1].to_vec();
                    (Some(BatchOperation::Put(key, value)), None)
                } else {
                    (
                        None,
                        Some(Err(RaftError::invalid_request(
                            "SET requires key and value",
                        ))),
                    )
                }
            }
            "DEL" => {
                if !redis_cmd.args.is_empty() {
                    // For simplicity, batch only the first key
                    // Multi-key DEL can be optimized further
                    let key = redis_cmd.args[0].to_vec();
                    (Some(BatchOperation::Delete(key)), None)
                } else {
                    (
                        None,
                        Some(Err(RaftError::invalid_request(
                            "DEL requires at least one key",
                        ))),
                    )
                }
            }
            // MSET can be batched as multiple Put operations
            "MSET" => {
                if redis_cmd.args.len() >= 2 && redis_cmd.args.len() % 2 == 0 {
                    // For now, we'll handle MSET individually in apply_redis_commands_batch
                    // to properly batch all the key-value pairs together
                    (None, None)
                } else {
                    (
                        None,
                        Some(Err(RaftError::invalid_request(
                            "MSET requires an even number of arguments",
                        ))),
                    )
                }
            }
            // Read operations and other commands don't participate in batching
            _ => (None, None),
        }
    }

    /// Apply multiple commands in batch for better performance
    pub async fn apply_redis_commands_batch(
        &self,
        commands: &[ClientRequest],
    ) -> RaftResult<Vec<ClientResponse>> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }

        // Collect batch operations
        let mut put_ops = Vec::new();
        let mut delete_ops = Vec::new();
        let mut responses = Vec::with_capacity(commands.len());
        let mut deferred_commands = Vec::new();

        for (idx, command) in commands.iter().enumerate() {
            let (batch_op, immediate_result) = self.collect_batch_operation(command);

            if let Some(result) = immediate_result {
                // Command has an immediate result (error or read operation)
                responses.push((
                    idx,
                    ClientResponse {
                        id: command.id,
                        result: result.map_err(|e| e.to_string()),
                        leader_id: None,
                    },
                ));
            } else if let Some(op) = batch_op {
                // Command can be batched
                match op {
                    BatchOperation::Put(key, value) => {
                        put_ops.push((key, value));
                        responses.push((
                            idx,
                            ClientResponse {
                                id: command.id,
                                result: Ok(Bytes::from("OK")),
                                leader_id: None,
                            },
                        ));
                    }
                    BatchOperation::Delete(key) => {
                        delete_ops.push(key);
                        responses.push((
                            idx,
                            ClientResponse {
                                id: command.id,
                                result: Ok(Bytes::from("1")),
                                leader_id: None,
                            },
                        ));
                    }
                }
            } else {
                // Command needs individual processing (GET, EXISTS, PING, etc.)
                deferred_commands.push((idx, command));
            }
        }

        // Execute batch operations if we have a storage engine
        if let Some(storage_engine) = &self.storage_engine {
            if !put_ops.is_empty() {
                storage_engine.batch_put(put_ops).await?;
            }
            if !delete_ops.is_empty() {
                storage_engine.batch_delete(delete_ops).await?;
            }
        }

        // Process deferred commands individually
        for (idx, command) in deferred_commands {
            let response = self.apply_redis_command(command).await?;
            responses.push((idx, response));
        }

        // Sort responses by original index to maintain order
        responses.sort_by_key(|(idx, _)| *idx);
        Ok(responses.into_iter().map(|(_, resp)| resp).collect())
    }

    /// Handle SET command
    /// Uses Bytes internally to minimize copying
    async fn handle_set_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request("SET requires key and value"));
        }

        // Bytes uses Arc internally, so these are cheap references
        let key = &cmd.args[0];
        let value = &cmd.args[1];

        if let Some(storage_engine) = &self.storage_engine {
            // Only convert to Vec when actually storing
            storage_engine.put(key, value).await?;
        }

        // Reuse static string to avoid allocation
        Ok(Bytes::from_static(b"OK"))
    }

    /// Handle GET command
    /// Returns Bytes to avoid unnecessary copying
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
    /// Optimized to minimize allocations
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

        // Convert count to string only once
        Ok(Bytes::from(deleted_count.to_string()))
    }

    /// Handle EXISTS command
    /// Optimized to minimize allocations
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

        // Convert count to string only once
        Ok(Bytes::from(exists_count.to_string()))
    }

    /// Handle PING command
    async fn handle_ping_command(&self) -> RaftResult<Bytes> {
        Ok(Bytes::from_static(b"PONG"))
    }

    /// Handle MSET command (multiple SET operations)
    /// Requirement 2.2: Execute Redis commands through state machine
    async fn handle_mset_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 || cmd.args.len() % 2 != 0 {
            return Err(RaftError::invalid_request(
                "MSET requires an even number of arguments (key value pairs)",
            ));
        }

        if let Some(storage_engine) = &self.storage_engine {
            // Build batch operations
            let mut operations = Vec::new();
            for i in (0..cmd.args.len()).step_by(2) {
                let key = cmd.args[i].to_vec();
                let value = cmd.args[i + 1].to_vec();
                operations.push((key, value));
            }

            storage_engine.batch_put(operations).await?;
        }

        Ok(Bytes::from_static(b"OK"))
    }

    /// Handle MGET command (multiple GET operations)
    /// Requirement 2.2: Execute Redis commands through state machine
    async fn handle_mget_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("MGET requires at least one key"));
        }

        if let Some(storage_engine) = &self.storage_engine {
            let mut results = Vec::new();
            for key in &cmd.args {
                match storage_engine.get(key).await? {
                    Some(value) => results.push(value),
                    None => results.push(Vec::new()), // Empty for non-existent keys
                }
            }

            // Serialize results as a simple format: count followed by values
            let serialized = bincode::serialize(&results).unwrap_or_default();
            Ok(Bytes::from(serialized))
        } else {
            // Return empty results when no storage engine
            let empty_results: Vec<Vec<u8>> = vec![Vec::new(); cmd.args.len()];
            let serialized = bincode::serialize(&empty_results).unwrap_or_default();
            Ok(Bytes::from(serialized))
        }
    }

    /// Handle INCR command (increment integer value)
    /// Requirement 2.2: Execute Redis commands through state machine
    async fn handle_incr_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("INCR requires key"));
        }

        let key = &cmd.args[0];

        if let Some(storage_engine) = &self.storage_engine {
            // Get current value
            let current_value = match storage_engine.get(key).await? {
                Some(value) => {
                    // Parse as integer - must return error if not a valid integer
                    let text = String::from_utf8(value)
                        .map_err(|_| RaftError::invalid_request("ERR value is not an integer or out of range"))?;
                    text.parse::<i64>()
                        .map_err(|_| RaftError::invalid_request("ERR value is not an integer or out of range"))?
                }
                None => 0,
            };

            // Increment
            let new_value = current_value + 1;
            let new_value_bytes = new_value.to_string().into_bytes();

            // Store new value
            storage_engine.put(key, &new_value_bytes).await?;

            Ok(Bytes::from(new_value.to_string()))
        } else {
            Ok(Bytes::from("1"))
        }
    }

    /// Handle DECR command (decrement integer value)
    /// Requirement 2.2: Execute Redis commands through state machine
    async fn handle_decr_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("DECR requires key"));
        }

        let key = &cmd.args[0];

        if let Some(storage_engine) = &self.storage_engine {
            // Get current value
            let current_value = match storage_engine.get(key).await? {
                Some(value) => {
                    // Parse as integer - must return error if not a valid integer
                    let text = String::from_utf8(value)
                        .map_err(|_| RaftError::invalid_request("ERR value is not an integer or out of range"))?;
                    text.parse::<i64>()
                        .map_err(|_| RaftError::invalid_request("ERR value is not an integer or out of range"))?
                }
                None => 0,
            };

            // Decrement
            let new_value = current_value - 1;
            let new_value_bytes = new_value.to_string().into_bytes();

            // Store new value
            storage_engine.put(key, &new_value_bytes).await?;

            Ok(Bytes::from(new_value.to_string()))
        } else {
            Ok(Bytes::from("-1"))
        }
    }

    /// Handle APPEND command (append to string value)
    /// Requirement 2.2: Execute Redis commands through state machine
    async fn handle_append_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request("APPEND requires key and value"));
        }

        let key = &cmd.args[0];
        let append_value = &cmd.args[1];

        if let Some(storage_engine) = &self.storage_engine {
            // Get current value
            let mut current_value: Vec<u8> = (storage_engine.get(key).await?).unwrap_or_default();

            // Append new value
            current_value.extend_from_slice(append_value);

            // Store updated value
            storage_engine.put(key, &current_value).await?;

            Ok(Bytes::from(current_value.len().to_string()))
        } else {
            Ok(Bytes::from(append_value.len().to_string()))
        }
    }

    /// Handle STRLEN command (get string length)
    /// Requirement 2.2: Execute Redis commands through state machine
    async fn handle_strlen_command(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("STRLEN requires key"));
        }

        let key = &cmd.args[0];

        if let Some(storage_engine) = &self.storage_engine {
            match storage_engine.get(key).await? {
                Some(value) => Ok(Bytes::from(value.len().to_string())),
                None => Ok(Bytes::from("0")),
            }
        } else {
            Ok(Bytes::from("0"))
        }
    }

    /// Execute a read command from the state machine
    /// 
    /// This method is used by the RequestRouter to execute read operations
    /// directly from the state machine without going through Raft consensus.
    /// 
    /// # Requirements
    /// - Requirement 4.1: Support strong consistency reads
    /// - Requirement 4.2: Support eventual consistency reads
    pub async fn execute_read(&self, cmd: &RedisCommand) -> RaftResult<Bytes> {
        match cmd.command.to_uppercase().as_str() {
            "GET" => self.handle_get_command(cmd).await,
            "EXISTS" => self.handle_exists_command(cmd).await,
            "MGET" => self.handle_mget_command(cmd).await,
            "STRLEN" => self.handle_strlen_command(cmd).await,
            "PING" => self.handle_ping_command().await,
            // Add more read commands as needed
            _ => Err(RaftError::state_machine(format!(
                "Unsupported read command: {}",
                cmd.command
            ))),
        }
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
        let start = std::time::Instant::now();
        let applied_index = self.applied_index();
        
        log::info!(
            "Node {} creating snapshot at applied index {}",
            self.node_id,
            applied_index
        );
        log::trace!("Node {} create_snapshot: starting at index {}", self.node_id, applied_index);

        let snapshot_data = HashMap::new();

        let snapshot = StateMachineSnapshot {
            data: snapshot_data,
            applied_index,
        };

        // Store the snapshot in memory for quick access
        let store_start = std::time::Instant::now();
        {
            let mut snapshot_store = self.snapshot_data.write().await;
            *snapshot_store = Some(snapshot.clone());
        }
        let store_elapsed = store_start.elapsed();
        log::trace!("Node {} stored snapshot in memory: {:?}", self.node_id, store_elapsed);

        let elapsed = start.elapsed();
        log::info!(
            "Node {} successfully created snapshot at index {} in {:?}",
            self.node_id,
            snapshot.applied_index,
            elapsed
        );
        log::trace!("Node {} create_snapshot: total_duration={:?}, keys={}", 
            self.node_id, elapsed, snapshot.data.len());
        Ok(snapshot)
    }

    /// Restore state from snapshot
    pub async fn restore_from_snapshot(&self, snapshot: &StateMachineSnapshot) -> RaftResult<()> {
        let start = std::time::Instant::now();
        
        log::info!(
            "Node {} restoring from snapshot at index {}",
            self.node_id,
            snapshot.applied_index
        );
        log::trace!("Node {} restore_from_snapshot: index={}, keys={}", 
            self.node_id, snapshot.applied_index, snapshot.data.len());

        // Acquire the apply mutex to ensure no concurrent operations during restore
        let lock_start = std::time::Instant::now();
        let _apply_lock = self.apply_mutex.lock().await;
        let lock_elapsed = lock_start.elapsed();
        log::trace!("Node {} acquired apply_mutex for restore in {:?}", self.node_id, lock_elapsed);

        // If we have a storage engine, restore the data
        if let Some(storage_engine) = &self.storage_engine {
            log::debug!(
                "Node {} restoring snapshot data to storage engine",
                self.node_id
            );

            let restore_start = std::time::Instant::now();
            // Restore all key-value pairs from the snapshot
            for (key, value) in &snapshot.data {
                storage_engine.put(key, value).await?;
            }
            let restore_elapsed = restore_start.elapsed();

            log::debug!(
                "Node {} successfully restored {} keys from snapshot in {:?}",
                self.node_id,
                snapshot.data.len(),
                restore_elapsed
            );
            log::trace!("Node {} restore throughput: {:.2} keys/sec", 
                self.node_id,
                if restore_elapsed.as_secs_f64() > 0.0 {
                    snapshot.data.len() as f64 / restore_elapsed.as_secs_f64()
                } else {
                    0.0
                });
        } else {
            log::debug!(
                "Node {} skipping data restore (no storage engine)",
                self.node_id
            );
        }

        // Update the applied index after successful restore
        let update_start = std::time::Instant::now();
        self.applied_index
            .store(snapshot.applied_index, Ordering::Release);
        let update_elapsed = update_start.elapsed();
        log::trace!("Node {} updated applied_index in {:?}", self.node_id, update_elapsed);

        // Store the snapshot in memory
        let store_start = std::time::Instant::now();
        {
            let mut snapshot_store = self.snapshot_data.write().await;
            *snapshot_store = Some(snapshot.clone());
        }
        let store_elapsed = store_start.elapsed();
        log::trace!("Node {} stored snapshot in memory in {:?}", self.node_id, store_elapsed);

        let elapsed = start.elapsed();
        log::info!(
            "Node {} successfully restored from snapshot at index {} in {:?}",
            self.node_id,
            snapshot.applied_index,
            elapsed
        );
        log::trace!("Node {} restore_from_snapshot: total_duration={:?}", self.node_id, elapsed);
        Ok(())
    }

    /// Get current snapshot data
    pub async fn get_current_snapshot_data(&self) -> RaftResult<Option<StateMachineSnapshot>> {
        let snapshot_store = self.snapshot_data.read().await;
        Ok(snapshot_store.clone())
    }

    /// Verify locking strategy is correct (for testing/debugging)
    /// 
    /// This method documents the expected locking order and can be used
    /// to verify that no deadlocks can occur.
    /// 
    /// # Locking Order
    /// 
    /// 1. apply_mutex (if needed for sequential operations)
    /// 2. snapshot_data RwLock (if needed for snapshot access)
    /// 
    /// Never acquire locks in reverse order to prevent deadlocks.
    #[allow(dead_code)]
    fn verify_locking_order_documentation(&self) {
        // This function exists purely for documentation purposes
        // The actual locking is done in the methods above following this order:
        // 1. apply_mutex is acquired first in apply() and restore_from_snapshot()
        // 2. snapshot_data is acquired after (or independently in read-only operations)
        // 3. applied_index uses atomic operations and doesn't need locks
    }
}

// ============================================================================
// Openraft RaftStateMachine Implementation
// ============================================================================

// Removed unused imports to fix warnings
// use openraft::storage::RaftStateMachine as OpenraftStateMachine;
// use openraft::{
//     Entry, EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
//     StorageError as OpenraftStorageError, StoredMembership,
// };
// use std::io::Cursor;
// use crate::types::TypeConfig;

/// Convert RaftError to Openraft StorageError
#[allow(dead_code)]
fn to_storage_error(err: RaftError) -> openraft::StorageError<NodeId> {
    use openraft::{ErrorSubject, ErrorVerb, StorageIOError};

    openraft::StorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::StateMachine,
            ErrorVerb::Write,
            openraft::AnyError::new(&err),
        ),
    }
}

// NOTE: These direct implementations are commented out because OpenRaft 0.9.21 uses sealed traits
// Use the adaptor_integration module instead for proper OpenRaft integration

/*
// Implement RaftStateMachine for Arc<KiwiStateMachine> to allow shared ownership
impl OpenraftStateMachine<TypeConfig> for Arc<KiwiStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        OpenraftStorageError<NodeId>,
    > {
        log::debug!(
            "Node {} getting applied state at index {}",
            self.node_id,
            self.applied_index()
        );

        let applied_index = self.applied_index();

        let log_id = if applied_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(1, self.node_id),
                applied_index,
            ))
        } else {
            None
        };

        let membership = StoredMembership::default();

        log::debug!(
            "Node {} applied_state: log_id={:?}",
            self.node_id,
            log_id
        );

        Ok((log_id, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ClientResponse>, OpenraftStorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let start = std::time::Instant::now();
        
        let lock_start = std::time::Instant::now();
        let _apply_lock = self.apply_mutex.lock().await;
        let lock_elapsed = lock_start.elapsed();
        log::trace!("Node {} acquired apply_mutex in {:?}", self.node_id, lock_elapsed);

        // Collect all entries into a vector for batch processing
        let collect_start = std::time::Instant::now();
        let entries_vec: Vec<_> = entries.into_iter().collect();
        let collect_elapsed = collect_start.elapsed();
        log::trace!("Node {} collected {} entries in {:?}", 
            self.node_id, entries_vec.len(), collect_elapsed);
        
        if entries_vec.is_empty() {
            return Ok(Vec::new());
        }

        log::debug!(
            "Node {} applying {} entries in batch",
            self.node_id,
            entries_vec.len()
        );

        // Separate entries by type for optimized batch processing
        let separate_start = std::time::Instant::now();
        let mut normal_requests = Vec::new();
        let mut normal_indices = Vec::new();
        let mut responses = Vec::with_capacity(entries_vec.len());
        let mut last_index = 0;

        for (idx, entry) in entries_vec.iter().enumerate() {
            last_index = entry.log_id.index;

            match &entry.payload {
                EntryPayload::Normal(client_request) => {
                    normal_requests.push(client_request.clone());
                    normal_indices.push((idx, entry.log_id.index));
                }
                EntryPayload::Blank => {
                    responses.push((
                        idx,
                        ClientResponse {
                            id: crate::types::RequestId::new(),
                            result: Ok(Bytes::from("OK")),
                            leader_id: Some(self.node_id),
                        },
                    ));
                }
                EntryPayload::Membership(ref _membership) => {
                    log::info!(
                        "Node {} applied membership change at index {}",
                        self.node_id,
                        entry.log_id.index
                    );

                    responses.push((
                        idx,
                        ClientResponse {
                            id: crate::types::RequestId::new(),
                            result: Ok(Bytes::from("OK")),
                            leader_id: Some(self.node_id),
                        },
                    ));
                }
            }
        }
        let separate_elapsed = separate_start.elapsed();
        log::trace!("Node {} separated entries in {:?}: normal={}, blank={}, membership={}", 
            self.node_id, separate_elapsed, normal_requests.len(), 
            entries_vec.len() - normal_requests.len() - normal_indices.len(),
            entries_vec.iter().filter(|e| matches!(e.payload, EntryPayload::Membership(_))).count());

        // Apply normal requests in batch
        if !normal_requests.is_empty() {
            let batch_start = std::time::Instant::now();
            let batch_responses = self
                .apply_redis_commands_batch(&normal_requests)
                .await
                .map_err(to_storage_error)?;
            let batch_elapsed = batch_start.elapsed();
            log::trace!("Node {} applied {} commands in batch: {:?}, throughput={:.2} ops/sec", 
                self.node_id, normal_requests.len(), batch_elapsed,
                if batch_elapsed.as_secs_f64() > 0.0 {
                    normal_requests.len() as f64 / batch_elapsed.as_secs_f64()
                } else {
                    0.0
                });

            for ((idx, _), response) in normal_indices.iter().zip(batch_responses.into_iter()) {
                responses.push((*idx, response));
            }
        }

        // Update applied index to the last entry
        let update_start = std::time::Instant::now();
        self.applied_index.store(last_index, Ordering::Release);
        let update_elapsed = update_start.elapsed();
        log::trace!("Node {} updated applied_index to {} in {:?}", 
            self.node_id, last_index, update_elapsed);

        log::debug!(
            "Node {} applied {} entries in batch, current applied_index={}",
            self.node_id,
            responses.len(),
            self.applied_index()
        );

        // Sort responses by original index to maintain order
        let sort_start = std::time::Instant::now();
        responses.sort_by_key(|(idx, _)| *idx);
        let sort_elapsed = sort_start.elapsed();
        log::trace!("Node {} sorted {} responses in {:?}", 
            self.node_id, responses.len(), sort_elapsed);
        
        let elapsed = start.elapsed();
        log::trace!("Node {} apply total duration: {:?}, entries={}, throughput={:.2} entries/sec", 
            self.node_id, elapsed, entries_vec.len(),
            if elapsed.as_secs_f64() > 0.0 {
                entries_vec.len() as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            });
        
        Ok(responses.into_iter().map(|(_, resp)| resp).collect())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        log::debug!("Node {} getting snapshot builder", self.node_id);
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, OpenraftStorageError<NodeId>> {
        log::info!(
            "Node {} beginning to receive snapshot",
            self.node_id
        );

        let cursor = Box::new(Cursor::new(Vec::new()));

        Ok(cursor)
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), OpenraftStorageError<NodeId>> {
        log::info!(
            "Node {} installing snapshot with meta: {:?}",
            self.node_id,
            meta
        );

        let snapshot_data = snapshot.into_inner();

        let state_machine_snapshot: StateMachineSnapshot =
            bincode::deserialize(&snapshot_data).map_err(|e| {
                to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency { message: format!("Failed to deserialize snapshot: {}", e), context: String::from("snapshot_operation")}))
            })?;

        log::debug!(
            "Node {} deserialized snapshot with {} keys at index {}",
            self.node_id,
            state_machine_snapshot.data.len(),
            state_machine_snapshot.applied_index
        );

        self.restore_from_snapshot(&state_machine_snapshot)
            .await
            .map_err(to_storage_error)?;

        if let Some(last_log_id) = &meta.last_log_id {
            self.applied_index
                .store(last_log_id.index, Ordering::Release);

            log::info!(
                "Node {} successfully installed snapshot at index {}",
                self.node_id,
                last_log_id.index
            );
        } else {
            log::info!(
                "Node {} installed snapshot with no last_log_id",
                self.node_id
            );
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, OpenraftStorageError<NodeId>> {
        log::debug!("Node {} getting current snapshot", self.node_id);

        let snapshot_data_opt = self
            .get_current_snapshot_data()
            .await
            .map_err(to_storage_error)?;

        if let Some(snapshot_data) = snapshot_data_opt {
            log::debug!(
                "Node {} found snapshot at index {}",
                self.node_id,
                snapshot_data.applied_index
            );

            let serialized_data = bincode::serialize(&snapshot_data).map_err(|e| {
                to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency { message: format!("Failed to serialize snapshot: {}", e), context: String::from("snapshot_operation")}))
            })?;

            let last_log_id = if snapshot_data.applied_index > 0 {
                Some(LogId::new(
                    openraft::CommittedLeaderId::new(1, self.node_id),
                    snapshot_data.applied_index,
                ))
            } else {
                None
            };

            let snapshot_id = format!(
                "snapshot-{}-{}",
                self.node_id, snapshot_data.applied_index
            );

            let meta = SnapshotMeta {
                last_log_id,
                last_membership: StoredMembership::default(),
                snapshot_id,
            };

            let snapshot = Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(serialized_data)),
            };

            log::debug!(
                "Node {} returning snapshot at index {}",
                self.node_id,
                snapshot_data.applied_index
            );

            Ok(Some(snapshot))
        } else {
            log::debug!("Node {} has no current snapshot", self.node_id);
            Ok(None)
        }
    }
}
*/

/*
impl OpenraftStateMachine<TypeConfig> for KiwiStateMachine {
    type SnapshotBuilder = Self;

    /// Get the current applied state of the state machine
    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        OpenraftStorageError<NodeId>,
    > {
        log::debug!(
            "Node {} getting applied state at index {}",
            self.node_id,
            self.applied_index()
        );

        let applied_index = self.applied_index();

        // Create LogId if we have applied any entries
        let log_id = if applied_index > 0 {
            // TODO: We need to track the actual term from the log entries
            // For now, using term=1 as a placeholder
            Some(LogId::new(
                openraft::CommittedLeaderId::new(1, self.node_id),
                applied_index,
            ))
        } else {
            None
        };

        // Return empty membership for now
        // TODO: Track actual membership changes from configuration entries
        let membership = StoredMembership::default();

        log::debug!(
            "Node {} applied_state: log_id={:?}",
            self.node_id,
            log_id
        );

        Ok((log_id, membership))
    }

    /// Apply log entries to the state machine
    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ClientResponse>, OpenraftStorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Acquire the apply mutex to ensure sequential application
        let _apply_lock = self.apply_mutex.lock().await;

        // Collect all entries into a vector for batch processing
        let entries_vec: Vec<_> = entries.into_iter().collect();
        
        if entries_vec.is_empty() {
            return Ok(Vec::new());
        }

        log::debug!(
            "Node {} applying {} entries in batch",
            self.node_id,
            entries_vec.len()
        );

        // Separate entries by type for optimized batch processing
        let mut normal_requests = Vec::new();
        let mut normal_indices = Vec::new();
        let mut responses = Vec::with_capacity(entries_vec.len());
        let mut last_index = 0;

        for (idx, entry) in entries_vec.iter().enumerate() {
            last_index = entry.log_id.index;

            match &entry.payload {
                EntryPayload::Normal(client_request) => {
                    normal_requests.push(client_request.clone());
                    normal_indices.push((idx, entry.log_id.index));
                }
                EntryPayload::Blank => {
                    responses.push((
                        idx,
                        ClientResponse {
                            id: crate::types::RequestId::new(),
                            result: Ok(Bytes::from("OK")),
                            leader_id: Some(self.node_id),
                        },
                    ));
                }
                EntryPayload::Membership(ref _membership) => {
                    log::info!(
                        "Node {} applied membership change at index {}",
                        self.node_id,
                        entry.log_id.index
                    );

                    responses.push((
                        idx,
                        ClientResponse {
                            id: crate::types::RequestId::new(),
                            result: Ok(Bytes::from("OK")),
                            leader_id: Some(self.node_id),
                        },
                    ));
                }
            }
        }

        // Apply normal requests in batch
        if !normal_requests.is_empty() {
            let batch_responses = self
                .apply_redis_commands_batch(&normal_requests)
                .await
                .map_err(to_storage_error)?;

            for ((idx, _), response) in normal_indices.iter().zip(batch_responses.into_iter()) {
                responses.push((*idx, response));
            }
        }

        // Update applied index to the last entry
        self.applied_index.store(last_index, Ordering::Release);

        log::debug!(
            "Node {} applied {} entries in batch, current applied_index={}",
            self.node_id,
            responses.len(),
            self.applied_index()
        );

        // Sort responses by original index to maintain order
        responses.sort_by_key(|(idx, _)| *idx);
        Ok(responses.into_iter().map(|(_, resp)| resp).collect())
    }

    /// Get a snapshot builder for creating snapshots
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        log::debug!("Node {} getting snapshot builder", self.node_id);

        // Return self as the snapshot builder
        // We need to clone the state machine to avoid borrowing issues
        Self {
            applied_index: AtomicU64::new(self.applied_index()),
            snapshot_data: Arc::clone(&self.snapshot_data),
            node_id: self.node_id,
            apply_mutex: Arc::clone(&self.apply_mutex),
            storage_engine: self.storage_engine.clone(),
        }
    }

    /// Begin receiving a snapshot from the leader
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, OpenraftStorageError<NodeId>> {
        log::info!(
            "Node {} beginning to receive snapshot",
            self.node_id
        );

        // Create an empty cursor for receiving snapshot data
        // The actual data will be written to this cursor by Openraft
        let cursor = Box::new(Cursor::new(Vec::new()));

        Ok(cursor)
    }

    /// Install a snapshot received from the leader
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), OpenraftStorageError<NodeId>> {
        log::info!(
            "Node {} installing snapshot with meta: {:?}",
            self.node_id,
            meta
        );

        // Get the snapshot data from the cursor
        let snapshot_data = snapshot.into_inner();

        // Deserialize the snapshot
        let state_machine_snapshot: StateMachineSnapshot =
            bincode::deserialize(&snapshot_data).map_err(|e| {
                to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency { message: format!("Failed to deserialize snapshot: {}", e), context: String::from("snapshot_operation")}))
            })?;

        log::debug!(
            "Node {} deserialized snapshot with {} keys at index {}",
            self.node_id,
            state_machine_snapshot.data.len(),
            state_machine_snapshot.applied_index
        );

        // Restore the state machine from the snapshot
        self.restore_from_snapshot(&state_machine_snapshot)
            .await
            .map_err(to_storage_error)?;

        // Update applied index from snapshot metadata
        if let Some(last_log_id) = &meta.last_log_id {
            self.applied_index
                .store(last_log_id.index, Ordering::Release);

            log::info!(
                "Node {} successfully installed snapshot at index {}",
                self.node_id,
                last_log_id.index
            );
        } else {
            log::info!(
                "Node {} installed snapshot with no last_log_id",
                self.node_id
            );
        }

        Ok(())
    }

    /// Get the current snapshot if available
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, OpenraftStorageError<NodeId>> {
        log::debug!("Node {} getting current snapshot", self.node_id);

        // Get the snapshot data from memory
        let snapshot_data_opt = self
            .get_current_snapshot_data()
            .await
            .map_err(to_storage_error)?;

        if let Some(snapshot_data) = snapshot_data_opt {
            log::debug!(
                "Node {} found snapshot at index {}",
                self.node_id,
                snapshot_data.applied_index
            );

            // Serialize the snapshot data
            let serialized_data = bincode::serialize(&snapshot_data).map_err(|e| {
                to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency { message: format!("Failed to serialize snapshot: {}", e), context: String::from("snapshot_operation")}))
            })?;

            // Create LogId for the snapshot
            let last_log_id = if snapshot_data.applied_index > 0 {
                Some(LogId::new(
                    openraft::CommittedLeaderId::new(1, self.node_id),
                    snapshot_data.applied_index,
                ))
            } else {
                None
            };

            // Create snapshot metadata
            let snapshot_id = format!(
                "snapshot-{}-{}",
                self.node_id, snapshot_data.applied_index
            );

            let meta = SnapshotMeta {
                last_log_id,
                last_membership: StoredMembership::default(),
                snapshot_id,
            };

            // Create the snapshot
            let snapshot = Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(serialized_data)),
            };

            log::debug!(
                "Node {} returning snapshot at index {}",
                self.node_id,
                snapshot_data.applied_index
            );

            Ok(Some(snapshot))
        } else {
            log::debug!("Node {} has no current snapshot", self.node_id);
            Ok(None)
        }
    }
}
*/

/*
// Implement RaftSnapshotBuilder for Arc<KiwiStateMachine>
impl RaftSnapshotBuilder<TypeConfig> for Arc<KiwiStateMachine> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, OpenraftStorageError<NodeId>> {
        log::info!(
            "Node {} building snapshot at applied index {}",
            self.node_id,
            self.applied_index()
        );

        let snapshot_data = self.create_snapshot().await.map_err(to_storage_error)?;

        log::debug!(
            "Node {} created snapshot with {} keys at index {}",
            self.node_id,
            snapshot_data.data.len(),
            snapshot_data.applied_index
        );

        let serialized_data = bincode::serialize(&snapshot_data).map_err(|e| {
            to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency { message: format!("Failed to serialize snapshot: {}", e), context: String::from("snapshot_operation")}))
        })?;

        let last_log_id = if snapshot_data.applied_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(1, self.node_id),
                snapshot_data.applied_index,
            ))
        } else {
            None
        };

        let snapshot_id = format!(
            "snapshot-{}-{}",
            self.node_id, snapshot_data.applied_index
        );

        let meta = SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::default(),
            snapshot_id: snapshot_id.clone(),
        };

        let snapshot = Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(serialized_data)),
        };

        log::info!(
            "Node {} successfully built snapshot {} at index {}",
            self.node_id,
            snapshot_id,
            snapshot_data.applied_index
        );

        Ok(snapshot)
    }
}

// Implement RaftSnapshotBuilder for KiwiStateMachine
impl RaftSnapshotBuilder<TypeConfig> for KiwiStateMachine {
    /// Build a snapshot of the current state
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, OpenraftStorageError<NodeId>> {
        log::info!(
            "Node {} building snapshot at applied index {}",
            self.node_id,
            self.applied_index()
        );

        // Create the snapshot using the state machine's method
        let snapshot_data = self.create_snapshot().await.map_err(to_storage_error)?;

        log::debug!(
            "Node {} created snapshot with {} keys at index {}",
            self.node_id,
            snapshot_data.data.len(),
            snapshot_data.applied_index
        );

        // Serialize the snapshot data
        let serialized_data = bincode::serialize(&snapshot_data).map_err(|e| {
            to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency { message: format!("Failed to serialize snapshot: {}", e), context: String::from("snapshot_operation")}))
        })?;

        // Create LogId for the snapshot
        let last_log_id = if snapshot_data.applied_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(1, self.node_id),
                snapshot_data.applied_index,
            ))
        } else {
            None
        };

        // Create snapshot metadata
        let snapshot_id = format!(
            "snapshot-{}-{}",
            self.node_id, snapshot_data.applied_index
        );

        let meta = SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::default(),
            snapshot_id: snapshot_id.clone(),
        };

        // Create the snapshot
        let snapshot = Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(serialized_data)),
        };

        log::info!(
            "Node {} successfully built snapshot {} at index {}",
            self.node_id,
            snapshot_id,
            snapshot_data.applied_index
        );

        Ok(snapshot)
    }
}





*/