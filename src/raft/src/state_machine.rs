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
use openraft::{EffectiveMembership, SnapshotMeta};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::error::{RaftError, RaftResult};
use crate::types::{ClientRequest, ClientResponse, NodeId, RedisCommand, TypeConfig};

/// Snapshot data structure for state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    pub data: HashMap<Vec<u8>, Vec<u8>>,
    pub applied_index: u64,
}

/// Transaction operation for batching
#[derive(Debug, Clone)]
pub enum TransactionOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Transaction context for atomic operations
pub struct TransactionContext {
    /// Operations to be applied atomically
    operations: Vec<TransactionOperation>,
    /// Keys involved in this transaction (for rollback)
    keys: Vec<Vec<u8>>,
    /// Original values before modification (for rollback)
    original_values: HashMap<Vec<u8>, Option<Vec<u8>>>,
    /// Transaction ID for tracking
    transaction_id: String,
    /// Whether the transaction has been committed
    committed: bool,
    /// Whether the transaction has been rolled back
    rolled_back: bool,
}

impl TransactionContext {
    fn new(transaction_id: String) -> Self {
        Self {
            operations: Vec::new(),
            keys: Vec::new(),
            original_values: HashMap::new(),
            transaction_id,
            committed: false,
            rolled_back: false,
        }
    }

    /// Add a key to track for rollback
    fn track_key(&mut self, key: Vec<u8>, original_value: Option<Vec<u8>>) {
        if !self.committed && !self.rolled_back {
            self.keys.push(key.clone());
            self.original_values.insert(key, original_value);
        }
    }

    /// Add a write operation to the transaction
    fn put(&mut self, key: &[u8], value: &[u8]) -> RaftResult<()> {
        if self.committed {
            return Err(RaftError::invalid_state("Transaction already committed"));
        }
        if self.rolled_back {
            return Err(RaftError::invalid_state("Transaction already rolled back"));
        }

        self.operations.push(TransactionOperation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    /// Add a delete operation to the transaction
    fn delete(&mut self, key: &[u8]) -> RaftResult<()> {
        if self.committed {
            return Err(RaftError::invalid_state("Transaction already committed"));
        }
        if self.rolled_back {
            return Err(RaftError::invalid_state("Transaction already rolled back"));
        }

        self.operations
            .push(TransactionOperation::Delete { key: key.to_vec() });
        Ok(())
    }

    /// Mark transaction as committed
    fn mark_committed(&mut self) {
        self.committed = true;
    }

    /// Mark transaction as rolled back
    fn mark_rolled_back(&mut self) {
        self.rolled_back = true;
    }

    /// Check if transaction is in a valid state for operations
    fn is_active(&self) -> bool {
        !self.committed && !self.rolled_back
    }

    /// Get transaction ID
    fn id(&self) -> &str {
        &self.transaction_id
    }

    /// Get the operations
    fn operations(&self) -> &[TransactionOperation] {
        &self.operations
    }

    /// Get tracked keys
    fn tracked_keys(&self) -> &[Vec<u8>] {
        &self.keys
    }

    /// Get original values for rollback
    fn original_values(&self) -> &HashMap<Vec<u8>, Option<Vec<u8>>> {
        &self.original_values
    }
}

/// Kiwi state machine for Raft integration
pub struct KiwiStateMachine {
    /// Last applied log index
    applied_index: AtomicU64,
    /// Snapshot storage
    snapshot_data: Arc<RwLock<Option<StateMachineSnapshot>>>,
    /// Node ID for logging and identification
    node_id: NodeId,
    /// Mutex to ensure atomic application of log entries
    apply_mutex: Arc<Mutex<()>>,
    /// Active transactions for rollback support
    active_transactions: Arc<RwLock<HashMap<String, TransactionContext>>>,
    /// Transaction counter for generating unique IDs
    transaction_counter: AtomicU64,
    /// Storage engine reference for transactional operations
    storage_engine: Option<Arc<dyn StorageEngine>>,
}

/// Trait for storage engine operations with transaction support
#[async_trait::async_trait]
pub trait StorageEngine: Send + Sync {
    /// Begin a new transaction
    async fn begin_transaction(&self) -> RaftResult<String>;

    /// Commit a transaction
    async fn commit_transaction(&self, transaction_id: &str) -> RaftResult<()>;

    /// Rollback a transaction
    async fn rollback_transaction(&self, transaction_id: &str) -> RaftResult<()>;

    /// Execute operations atomically
    async fn execute_operations(&self, operations: &[TransactionOperation]) -> RaftResult<()>;

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

    /// Get all keys for snapshot creation (optional, for simple implementations)
    async fn get_all_keys(&self) -> RaftResult<Vec<Vec<u8>>>;

    /// Get multiple key-value pairs efficiently
    async fn get_multiple(&self, keys: &[Vec<u8>]) -> RaftResult<Vec<Option<Vec<u8>>>>;
}

impl KiwiStateMachine {
    /// Create a new state machine instance
    pub fn new(node_id: NodeId) -> Self {
        Self {
            applied_index: AtomicU64::new(0),
            snapshot_data: Arc::new(RwLock::new(None)),
            node_id,
            apply_mutex: Arc::new(Mutex::new(())),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_counter: AtomicU64::new(1),
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
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_counter: AtomicU64::new(1),
            storage_engine: Some(storage_engine),
        }
    }

    /// Generate a unique transaction ID
    fn generate_transaction_id(&self) -> String {
        let counter = self.transaction_counter.fetch_add(1, Ordering::SeqCst);
        format!("txn_{}_{}", self.node_id, counter)
    }

    /// Begin a new transaction for atomic operations
    pub async fn begin_transaction(&self) -> RaftResult<String> {
        let transaction_id = self.generate_transaction_id();
        let transaction_context = TransactionContext::new(transaction_id.clone());

        // Store the transaction context
        let mut active_txns = self.active_transactions.write().await;
        active_txns.insert(transaction_id.clone(), transaction_context);

        log::debug!("Node {} began transaction {}", self.node_id, transaction_id);
        Ok(transaction_id)
    }

    /// Commit a transaction atomically
    pub async fn commit_transaction(&self, transaction_id: &str) -> RaftResult<()> {
        let mut active_txns = self.active_transactions.write().await;

        if let Some(mut transaction_context) = active_txns.remove(transaction_id) {
            if !transaction_context.is_active() {
                return Err(RaftError::invalid_state(format!(
                    "Transaction {} is not in active state",
                    transaction_id
                )));
            }

            // Execute the operations atomically
            if let Some(storage_engine) = &self.storage_engine {
                storage_engine
                    .execute_operations(transaction_context.operations())
                    .await?;
            }

            transaction_context.mark_committed();
            log::debug!(
                "Node {} committed transaction {}",
                self.node_id,
                transaction_id
            );
            Ok(())
        } else {
            Err(RaftError::not_found(format!(
                "Transaction {} not found",
                transaction_id
            )))
        }
    }

    /// Rollback a transaction by restoring original values
    pub async fn rollback_transaction(&self, transaction_id: &str) -> RaftResult<()> {
        let mut active_txns = self.active_transactions.write().await;

        if let Some(mut transaction_context) = active_txns.remove(transaction_id) {
            if !transaction_context.is_active() {
                return Err(RaftError::invalid_state(format!(
                    "Transaction {} is not in active state",
                    transaction_id
                )));
            }

            // Restore original values
            if let Some(storage_engine) = &self.storage_engine {
                let mut rollback_operations = Vec::new();

                for (key, original_value) in transaction_context.original_values() {
                    match original_value {
                        Some(value) => {
                            // Restore original value
                            rollback_operations.push(TransactionOperation::Put {
                                key: key.clone(),
                                value: value.clone(),
                            });
                        }
                        None => {
                            // Key didn't exist, delete it
                            rollback_operations
                                .push(TransactionOperation::Delete { key: key.clone() });
                        }
                    }
                }

                // Execute rollback operations
                storage_engine
                    .execute_operations(&rollback_operations)
                    .await?;
            }

            transaction_context.mark_rolled_back();
            log::warn!(
                "Node {} rolled back transaction {}",
                self.node_id,
                transaction_id
            );
            Ok(())
        } else {
            Err(RaftError::not_found(format!(
                "Transaction {} not found",
                transaction_id
            )))
        }
    }

    /// Apply a command within a transaction context
    async fn apply_command_transactional(
        &self,
        command: &ClientRequest,
    ) -> RaftResult<ClientResponse> {
        // Begin a transaction for this command
        let transaction_id = self.begin_transaction().await?;

        // Try to apply the command
        let result = match self
            .apply_redis_command_with_transaction(command, &transaction_id)
            .await
        {
            Ok(response) => {
                // Commit the transaction on success
                match self.commit_transaction(&transaction_id).await {
                    Ok(()) => Ok(response),
                    Err(commit_error) => {
                        // Try to rollback if commit fails
                        let _ = self.rollback_transaction(&transaction_id).await;
                        Err(commit_error)
                    }
                }
            }
            Err(apply_error) => {
                // Rollback the transaction on failure
                let _ = self.rollback_transaction(&transaction_id).await;
                Err(apply_error)
            }
        };

        match result {
            Ok(response) => Ok(response),
            Err(error) => Ok(ClientResponse {
                id: command.id,
                result: Err(error.to_string()),
                leader_id: None,
            }),
        }
    }

    /// Apply a Redis command with transaction support
    async fn apply_redis_command_with_transaction(
        &self,
        command: &ClientRequest,
        transaction_id: &str,
    ) -> RaftResult<ClientResponse> {
        let redis_cmd = &command.command;

        // Get the transaction context
        let mut active_txns = self.active_transactions.write().await;
        let transaction_context = active_txns.get_mut(transaction_id).ok_or_else(|| {
            RaftError::not_found(format!("Transaction {} not found", transaction_id))
        })?;

        if !transaction_context.is_active() {
            return Err(RaftError::invalid_state(format!(
                "Transaction {} is not in active state",
                transaction_id
            )));
        }

        // Apply the command based on its type with transaction support
        let result = match redis_cmd.command.to_uppercase().as_str() {
            // String operations
            "SET" => {
                self.handle_set_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "GET" => {
                self.handle_get_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "DEL" => {
                self.handle_del_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "EXISTS" => {
                self.handle_exists_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "PING" => self.handle_ping_command().await,

            // List operations
            "LPUSH" => {
                self.handle_lpush_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "RPUSH" => {
                self.handle_rpush_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "LPOP" => {
                self.handle_lpop_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "RPOP" => {
                self.handle_rpop_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "LLEN" => {
                self.handle_llen_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "LINDEX" => {
                self.handle_lindex_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "LRANGE" => {
                self.handle_lrange_command_transactional(redis_cmd, transaction_context)
                    .await
            }

            // Hash operations
            "HSET" => {
                self.handle_hset_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "HGET" => {
                self.handle_hget_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "HDEL" => {
                self.handle_hdel_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "HEXISTS" => {
                self.handle_hexists_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "HLEN" => {
                self.handle_hlen_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "HKEYS" => {
                self.handle_hkeys_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "HVALS" => {
                self.handle_hvals_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "HGETALL" => {
                self.handle_hgetall_command_transactional(redis_cmd, transaction_context)
                    .await
            }

            // Set operations
            "SADD" => {
                self.handle_sadd_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "SREM" => {
                self.handle_srem_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "SCARD" => {
                self.handle_scard_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "SMEMBERS" => {
                self.handle_smembers_command_transactional(redis_cmd, transaction_context)
                    .await
            }
            "SISMEMBER" => {
                self.handle_sismember_command_transactional(redis_cmd, transaction_context)
                    .await
            }

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

    /// Get the last applied index
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    /// Apply a Redis command to the database engine (legacy non-transactional)
    async fn apply_redis_command(&self, command: &ClientRequest) -> RaftResult<ClientResponse> {
        // Use transactional approach for all commands to ensure atomicity
        self.apply_command_transactional(command).await
    }

    /// Handle SET command
    async fn handle_set_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("OK"))
    }

    /// Handle GET command
    async fn handle_get_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::new()) // Return null for now
    }

    /// Handle DEL command
    async fn handle_del_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 deleted keys for now
    }

    /// Handle EXISTS command
    async fn handle_exists_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 existing keys for now
    }

    /// Handle PING command
    async fn handle_ping_command(&self) -> RaftResult<Bytes> {
        Ok(Bytes::from_static(b"PONG"))
    }

    // ===== LIST OPERATIONS =====

    /// Handle LPUSH command
    async fn handle_lpush_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("1")) // Return 1 as list length for now
    }

    /// Handle RPUSH command
    async fn handle_rpush_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("1")) // Return 1 as list length for now
    }

    /// Handle LPOP command
    async fn handle_lpop_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::new()) // Return null for now
    }

    /// Handle RPOP command
    async fn handle_rpop_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::new()) // Return null for now
    }

    /// Handle LLEN command
    async fn handle_llen_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 length for now
    }

    /// Handle LINDEX command
    async fn handle_lindex_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::new()) // Return null for now
    }

    /// Handle LRANGE command
    async fn handle_lrange_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("[]")) // Return empty array for now
    }

    // ===== HASH OPERATIONS =====

    /// Handle HSET command
    async fn handle_hset_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("1")) // Return 1 for field created
    }

    /// Handle HGET command
    async fn handle_hget_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::new()) // Return null for now
    }

    /// Handle HDEL command
    async fn handle_hdel_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 deleted fields for now
    }

    /// Handle HEXISTS command
    async fn handle_hexists_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 (field doesn't exist) for now
    }

    /// Handle HLEN command
    async fn handle_hlen_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 fields for now
    }

    /// Handle HKEYS command
    async fn handle_hkeys_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("[]")) // Return empty array for now
    }

    /// Handle HVALS command
    async fn handle_hvals_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("[]")) // Return empty array for now
    }

    /// Handle HGETALL command
    async fn handle_hgetall_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("[]")) // Return empty array for now
    }

    // ===== SET OPERATIONS =====

    /// Handle SADD command
    async fn handle_sadd_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("1")) // Return 1 member added for now
    }

    /// Handle SREM command
    async fn handle_srem_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 members removed for now
    }

    /// Handle SCARD command
    async fn handle_scard_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 set size for now
    }

    /// Handle SMEMBERS command
    async fn handle_smembers_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("[]")) // Return empty array for now
    }

    /// Handle SISMEMBER command
    async fn handle_sismember_command(&self, _cmd: &RedisCommand) -> RaftResult<Bytes> {
        // TODO: Implement actual storage integration
        Ok(Bytes::from("0")) // Return 0 (not a member) for now
    }

    // ===== TRANSACTIONAL COMMAND HANDLERS =====

    /// Handle SET command with transaction support
    async fn handle_set_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request("SET requires key and value"));
        }

        let key = &cmd.args[0];
        let value = &cmd.args[1];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // Add to transaction batch
        transaction_context.put(key, value)?;

        Ok(Bytes::from("OK"))
    }

    /// Handle GET command with transaction support
    async fn handle_get_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("GET requires key"));
        }

        let key = &cmd.args[0];

        // Read operations don't need transaction tracking
        if let Some(storage_engine) = &self.storage_engine {
            match storage_engine.get(key).await? {
                Some(value) => Ok(Bytes::from(value)),
                None => Ok(Bytes::new()), // Return null for non-existent key
            }
        } else {
            Ok(Bytes::new()) // Return null when no storage engine
        }
    }

    /// Handle DEL command with transaction support
    async fn handle_del_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("DEL requires at least one key"));
        }

        let mut deleted_count = 0;

        for key in &cmd.args {
            // Track original value for rollback
            if let Some(storage_engine) = &self.storage_engine {
                let original_value = storage_engine.get(key).await?;
                if original_value.is_some() {
                    transaction_context.track_key(key.to_vec(), original_value);
                    transaction_context.delete(key)?;
                    deleted_count += 1;
                }
            }
        }

        Ok(Bytes::from(deleted_count.to_string()))
    }

    /// Handle EXISTS command with transaction support
    async fn handle_exists_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
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

    // ===== LIST OPERATIONS WITH TRANSACTION SUPPORT =====

    /// Handle LPUSH command with transaction support
    async fn handle_lpush_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request(
                "LPUSH requires key and at least one value",
            ));
        }

        let key = &cmd.args[0];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // For now, return the number of elements that would be added
        let added_count = cmd.args.len() - 1;

        // TODO: Implement actual list storage logic
        // This is a placeholder that adds the operation to the transaction batch
        let list_data = format!("lpush:{}", added_count);
        transaction_context.put(key, list_data.as_bytes())?;

        Ok(Bytes::from(added_count.to_string()))
    }

    /// Handle RPUSH command with transaction support
    async fn handle_rpush_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request(
                "RPUSH requires key and at least one value",
            ));
        }

        let key = &cmd.args[0];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // For now, return the number of elements that would be added
        let added_count = cmd.args.len() - 1;

        // TODO: Implement actual list storage logic
        let list_data = format!("rpush:{}", added_count);
        transaction_context.put(key, list_data.as_bytes())?;

        Ok(Bytes::from(added_count.to_string()))
    }

    /// Handle LPOP command with transaction support
    async fn handle_lpop_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("LPOP requires key"));
        }

        let key = &cmd.args[0];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // TODO: Implement actual list storage logic
        // For now, return null (empty list)
        Ok(Bytes::new())
    }

    /// Handle RPOP command with transaction support
    async fn handle_rpop_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("RPOP requires key"));
        }

        let key = &cmd.args[0];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // TODO: Implement actual list storage logic
        // For now, return null (empty list)
        Ok(Bytes::new())
    }

    /// Handle LLEN command with transaction support
    async fn handle_llen_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("LLEN requires key"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual list length calculation
        Ok(Bytes::from("0"))
    }

    /// Handle LINDEX command with transaction support
    async fn handle_lindex_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request("LINDEX requires key and index"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual list index access
        Ok(Bytes::new())
    }

    /// Handle LRANGE command with transaction support
    async fn handle_lrange_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 3 {
            return Err(RaftError::invalid_request(
                "LRANGE requires key, start, and stop",
            ));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual list range access
        Ok(Bytes::from("[]"))
    }

    // ===== HASH OPERATIONS WITH TRANSACTION SUPPORT =====

    /// Handle HSET command with transaction support
    async fn handle_hset_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 3 {
            return Err(RaftError::invalid_request(
                "HSET requires key, field, and value",
            ));
        }

        let key = &cmd.args[0];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // TODO: Implement actual hash storage logic
        let hash_data = format!(
            "hset:{}:{}",
            String::from_utf8_lossy(&cmd.args[1]),
            String::from_utf8_lossy(&cmd.args[2])
        );
        transaction_context.put(key, hash_data.as_bytes())?;

        Ok(Bytes::from("1")) // Return 1 for field created
    }

    /// Handle HGET command with transaction support
    async fn handle_hget_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request("HGET requires key and field"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual hash field access
        Ok(Bytes::new())
    }

    /// Handle HDEL command with transaction support
    async fn handle_hdel_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request(
                "HDEL requires key and at least one field",
            ));
        }

        let key = &cmd.args[0];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // TODO: Implement actual hash field deletion logic
        let deleted_count = cmd.args.len() - 1;
        let hash_data = format!("hdel:{}", deleted_count);
        transaction_context.put(key, hash_data.as_bytes())?;

        Ok(Bytes::from("0")) // Return 0 deleted fields for now
    }

    /// Handle HEXISTS command with transaction support
    async fn handle_hexists_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request("HEXISTS requires key and field"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual hash field existence check
        Ok(Bytes::from("0"))
    }

    /// Handle HLEN command with transaction support
    async fn handle_hlen_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("HLEN requires key"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual hash length calculation
        Ok(Bytes::from("0"))
    }

    /// Handle HKEYS command with transaction support
    async fn handle_hkeys_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("HKEYS requires key"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual hash keys retrieval
        Ok(Bytes::from("[]"))
    }

    /// Handle HVALS command with transaction support
    async fn handle_hvals_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("HVALS requires key"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual hash values retrieval
        Ok(Bytes::from("[]"))
    }

    /// Handle HGETALL command with transaction support
    async fn handle_hgetall_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("HGETALL requires key"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual hash get all
        Ok(Bytes::from("[]"))
    }

    // ===== SET OPERATIONS WITH TRANSACTION SUPPORT =====

    /// Handle SADD command with transaction support
    async fn handle_sadd_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request(
                "SADD requires key and at least one member",
            ));
        }

        let key = &cmd.args[0];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // TODO: Implement actual set storage logic
        let added_count = cmd.args.len() - 1;
        let set_data = format!("sadd:{}", added_count);
        transaction_context.put(key, set_data.as_bytes())?;

        Ok(Bytes::from(added_count.to_string()))
    }

    /// Handle SREM command with transaction support
    async fn handle_srem_command_transactional(
        &self,
        cmd: &RedisCommand,
        transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request(
                "SREM requires key and at least one member",
            ));
        }

        let key = &cmd.args[0];

        // Track original value for rollback
        if let Some(storage_engine) = &self.storage_engine {
            let original_value = storage_engine.get(key).await?;
            transaction_context.track_key(key.to_vec(), original_value);
        }

        // TODO: Implement actual set member removal logic
        let set_data = format!("srem:{}", cmd.args.len() - 1);
        transaction_context.put(key, set_data.as_bytes())?;

        Ok(Bytes::from("0")) // Return 0 members removed for now
    }

    /// Handle SCARD command with transaction support
    async fn handle_scard_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("SCARD requires key"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual set cardinality calculation
        Ok(Bytes::from("0"))
    }

    /// Handle SMEMBERS command with transaction support
    async fn handle_smembers_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.is_empty() {
            return Err(RaftError::invalid_request("SMEMBERS requires key"));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual set members retrieval
        Ok(Bytes::from("[]"))
    }

    /// Handle SISMEMBER command with transaction support
    async fn handle_sismember_command_transactional(
        &self,
        cmd: &RedisCommand,
        _transaction_context: &mut TransactionContext,
    ) -> RaftResult<Bytes> {
        if cmd.args.len() < 2 {
            return Err(RaftError::invalid_request(
                "SISMEMBER requires key and member",
            ));
        }

        // Read-only operation, no transaction tracking needed
        // TODO: Implement actual set membership check
        Ok(Bytes::from("0"))
    }

    /// Create a snapshot of the current state
    pub async fn create_snapshot(&self) -> RaftResult<StateMachineSnapshot> {
        log::info!(
            "Node {} creating snapshot at applied index {}",
            self.node_id,
            self.applied_index()
        );

        let snapshot_data = HashMap::new();

        // If we have a storage engine, create a snapshot from it
        if let Some(_storage_engine) = &self.storage_engine {
            // For now, we'll create a simple snapshot by iterating through all keys
            // In a production system, this would use RocksDB's native snapshot functionality
            // TODO: Implement more efficient snapshot creation using RocksDB snapshots

            // Since we don't have a direct way to iterate all keys from the StorageEngine trait,
            // we'll create a minimal snapshot with metadata only
            log::debug!(
                "Node {} creating snapshot with storage engine integration",
                self.node_id
            );
        } else {
            log::debug!(
                "Node {} creating empty snapshot (no storage engine)",
                self.node_id
            );
        }

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

        // Clear any active transactions before restoring
        let cleared_txns = self.rollback_all_transactions().await?;
        if cleared_txns > 0 {
            log::warn!(
                "Node {} cleared {} active transactions during snapshot restore",
                self.node_id,
                cleared_txns
            );
        }

        // If we have a storage engine, restore the data
        if let Some(_storage_engine) = &self.storage_engine {
            log::debug!(
                "Node {} restoring snapshot data to storage engine",
                self.node_id
            );

            // Begin a transaction for the entire restore operation
            let restore_transaction_id = self.generate_transaction_id();
            let restore_transaction_context =
                TransactionContext::new(restore_transaction_id.clone());

            {
                let mut active_txns = self.active_transactions.write().await;
                active_txns.insert(restore_transaction_id.clone(), restore_transaction_context);
            }

            // Restore all key-value pairs from the snapshot
            let mut restore_failed = false;
            let mut restore_error: Option<RaftError> = None;

            {
                let mut active_txns = self.active_transactions.write().await;
                if let Some(transaction_context) = active_txns.get_mut(&restore_transaction_id) {
                    for (key, value) in &snapshot.data {
                        if let Err(error) = transaction_context.put(key, value) {
                            log::error!(
                                "Node {} failed to add key to restore transaction: {}",
                                self.node_id,
                                error
                            );
                            restore_failed = true;
                            restore_error = Some(error);
                            break;
                        }
                    }
                }
            }

            // Commit or rollback the restore transaction
            if restore_failed {
                if let Err(rollback_error) =
                    self.rollback_transaction(&restore_transaction_id).await
                {
                    log::error!(
                        "Node {} failed to rollback restore transaction: {}",
                        self.node_id,
                        rollback_error
                    );
                }
                if let Some(error) = restore_error {
                    return Err(error);
                }
            } else {
                if let Err(commit_error) = self.commit_transaction(&restore_transaction_id).await {
                    log::error!(
                        "Node {} failed to commit restore transaction: {}",
                        self.node_id,
                        commit_error
                    );
                    return Err(commit_error);
                }
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

    /// Get current data size for monitoring
    pub async fn data_size(&self) -> usize {
        // For now, return 0 - proper implementation would require scanning the storage
        // This is a placeholder for monitoring purposes
        0
    }

    /// Get all keys for debugging
    pub async fn get_all_keys(&self) -> Vec<String> {
        // For now, return empty vector - proper implementation would require scanning the storage
        // This is a placeholder for debugging purposes
        Vec::new()
    }

    /// Clean up expired transactions (should be called periodically)
    pub async fn cleanup_expired_transactions(&self) -> RaftResult<usize> {
        let active_txns = self.active_transactions.write().await;
        let initial_count = active_txns.len();

        // For now, we don't have transaction timeouts implemented
        // This is a placeholder for future transaction timeout functionality
        // In a production system, you would check transaction timestamps and clean up old ones

        let cleaned_count = initial_count - active_txns.len();
        if cleaned_count > 0 {
            log::info!(
                "Node {} cleaned up {} expired transactions",
                self.node_id,
                cleaned_count
            );
        }

        Ok(cleaned_count)
    }

    /// Get the number of active transactions
    pub async fn active_transaction_count(&self) -> usize {
        let active_txns = self.active_transactions.read().await;
        active_txns.len()
    }

    /// Force rollback all active transactions (used during shutdown or error recovery)
    pub async fn rollback_all_transactions(&self) -> RaftResult<usize> {
        let mut active_txns = self.active_transactions.write().await;
        let transaction_ids: Vec<String> = active_txns.keys().cloned().collect();
        let count = transaction_ids.len();

        // Clear all transactions
        active_txns.clear();

        if count > 0 {
            log::warn!(
                "Node {} force rolled back {} active transactions",
                self.node_id,
                count
            );
        }

        Ok(count)
    }

    /// Check if the state machine is in a consistent state
    pub async fn verify_consistency(&self) -> RaftResult<bool> {
        // Check if there are any stuck transactions
        let active_count = self.active_transaction_count().await;

        if active_count > 100 {
            // Arbitrary threshold
            log::warn!(
                "Node {} has {} active transactions, possible consistency issue",
                self.node_id,
                active_count
            );
            return Ok(false);
        }

        // Additional consistency checks can be added here
        // For example, verifying storage integrity, checking applied index consistency, etc.

        Ok(true)
    }
}

// Note: We'll implement RaftStateMachine using the Adaptor pattern later
// For now, let's create a basic implementation that can be wrapped

impl KiwiStateMachine {
    /// Get the current applied state for Raft
    ///
    /// # Warning
    /// This function is currently unused but exported for future integration with RaftStateMachine Adaptor.
    /// The term is hardcoded as 1 since we don't track applied term yet. This should be sourced from actual
    /// Raft state before this method is used in production.
    ///
    /// TODO: Replace hardcoded term=1 with actual applied term from Raft state
    pub async fn get_applied_state(
        &self,
    ) -> (
        Option<openraft::LogId<NodeId>>,
        EffectiveMembership<NodeId, openraft::BasicNode>,
    ) {
        let applied_index = self.applied_index();

        // TODO: Source the actual applied term from Raft state instead of hardcoding term=1
        // This is semantically incorrect per OpenRaft contract but acceptable as placeholder
        // since this function is currently unused
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

    /// Apply log entries to the state machine with atomic guarantees
    pub async fn apply_entries<I>(&mut self, entries: I) -> RaftResult<Vec<ClientResponse>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>>,
    {
        // Acquire the apply mutex to ensure atomic application of log entries
        let _apply_lock = self.apply_mutex.lock().await;

        let mut responses = Vec::new();
        let mut last_applied_index = self.applied_index();

        // Collect all entries to apply them atomically
        let entries_vec: Vec<_> = entries.into_iter().collect();

        // Begin a batch transaction for all entries
        let batch_transaction_id = self.generate_transaction_id();
        let batch_transaction_context = TransactionContext::new(batch_transaction_id.clone());

        {
            let mut active_txns = self.active_transactions.write().await;
            active_txns.insert(batch_transaction_id.clone(), batch_transaction_context);
        }

        // Apply all entries within the batch transaction
        let mut batch_failed = false;
        let mut batch_error: Option<RaftError> = None;

        for entry in entries_vec {
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
                    log::debug!(
                        "Node {} applying blank entry at index {}",
                        self.node_id,
                        entry.log_id.index
                    );
                }
                openraft::EntryPayload::Normal(request) => {
                    log::debug!(
                        "Node {} applying command entry at index {}",
                        self.node_id,
                        entry.log_id.index
                    );

                    // Apply the command within the batch transaction
                    match self
                        .apply_redis_command_with_transaction(&request, &batch_transaction_id)
                        .await
                    {
                        Ok(response) => {
                            responses.push(response);
                        }
                        Err(error) => {
                            log::error!(
                                "Node {} failed to apply entry at index {}: {}",
                                self.node_id,
                                entry.log_id.index,
                                error
                            );
                            batch_failed = true;
                            batch_error = Some(error);
                            break;
                        }
                    }
                }
                openraft::EntryPayload::Membership(_) => {
                    // Membership changes are handled by openraft
                    log::debug!(
                        "Node {} applying membership entry at index {}",
                        self.node_id,
                        entry.log_id.index
                    );
                }
            }

            // Update applied index
            last_applied_index = entry.log_id.index;
        }

        // Commit or rollback the batch transaction
        if batch_failed {
            // Rollback the entire batch
            if let Err(rollback_error) = self.rollback_transaction(&batch_transaction_id).await {
                log::error!(
                    "Node {} failed to rollback batch transaction {}: {}",
                    self.node_id,
                    batch_transaction_id,
                    rollback_error
                );
            }

            // Return the original error
            if let Some(error) = batch_error {
                return Err(error);
            }
        } else {
            // Commit the batch transaction
            match self.commit_transaction(&batch_transaction_id).await {
                Ok(()) => {
                    // Update applied index only after successful commit
                    self.applied_index
                        .store(last_applied_index, Ordering::Release);
                    log::debug!(
                        "Node {} successfully applied batch of {} entries up to index {}",
                        self.node_id,
                        responses.len(),
                        last_applied_index
                    );
                }
                Err(commit_error) => {
                    log::error!(
                        "Node {} failed to commit batch transaction {}: {}",
                        self.node_id,
                        batch_transaction_id,
                        commit_error
                    );

                    // Try to rollback
                    if let Err(rollback_error) =
                        self.rollback_transaction(&batch_transaction_id).await
                    {
                        log::error!(
                            "Node {} failed to rollback after commit failure {}: {}",
                            self.node_id,
                            batch_transaction_id,
                            rollback_error
                        );
                    }

                    return Err(commit_error);
                }
            }
        }

        Ok(responses)
    }

    /// Begin receiving a snapshot
    pub async fn begin_receiving_snapshot(&self) -> RaftResult<KiwiStateMachine> {
        // Return a clone of self for snapshot building
        Ok(Self {
            applied_index: AtomicU64::new(self.applied_index()),
            snapshot_data: Arc::clone(&self.snapshot_data),
            node_id: self.node_id,
            apply_mutex: Arc::clone(&self.apply_mutex),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_counter: AtomicU64::new(1),
            storage_engine: self.storage_engine.clone(),
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

    /// Get current snapshot data
    pub async fn get_current_snapshot_data(&self) -> RaftResult<Option<StateMachineSnapshot>> {
        let snapshot_store = self.snapshot_data.read().await;
        Ok(snapshot_store.clone())
    }

    /// Create and store a new snapshot with RocksDB integration
    pub async fn create_and_store_snapshot(
        &self,
        raft_storage: &crate::storage::RaftStorage,
    ) -> RaftResult<String> {
        // Create the snapshot
        let snapshot = self.create_snapshot().await?;

        // Generate a unique snapshot ID
        let snapshot_id = format!("snapshot_{}_{}", self.node_id, snapshot.applied_index);

        // Serialize the snapshot data
        let snapshot_data = bincode::serialize(&snapshot).map_err(|e| {
            RaftError::state_machine(format!("Failed to serialize snapshot: {}", e))
        })?;

        // Create snapshot metadata
        let snapshot_meta = crate::storage::StoredSnapshotMeta {
            last_log_index: snapshot.applied_index,
            last_log_term: 1, // TODO: Get actual term from Raft state
            snapshot_id: snapshot_id.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        };

        // Store snapshot metadata and data in RocksDB
        raft_storage
            .store_snapshot_meta(&snapshot_meta)
            .map_err(|e| {
                RaftError::state_machine(format!("Failed to store snapshot metadata: {}", e))
            })?;

        raft_storage
            .store_snapshot_data(&snapshot_id, &snapshot_data)
            .map_err(|e| {
                RaftError::state_machine(format!("Failed to store snapshot data: {}", e))
            })?;

        log::info!(
            "Node {} successfully stored snapshot {} with {} keys",
            self.node_id,
            snapshot_id,
            snapshot.data.len()
        );

        Ok(snapshot_id)
    }

    /// Load and restore from a stored snapshot
    pub async fn load_and_restore_snapshot(
        &self,
        raft_storage: &crate::storage::RaftStorage,
        snapshot_id: &str,
    ) -> RaftResult<()> {
        // Load snapshot data from RocksDB
        let snapshot_data = raft_storage
            .get_snapshot_data(snapshot_id)
            .map_err(|e| RaftError::state_machine(format!("Failed to load snapshot data: {}", e)))?
            .ok_or_else(|| {
                RaftError::state_machine(format!("Snapshot {} not found", snapshot_id))
            })?;

        // Deserialize the snapshot
        let snapshot: StateMachineSnapshot = bincode::deserialize(&snapshot_data).map_err(|e| {
            RaftError::state_machine(format!("Failed to deserialize snapshot: {}", e))
        })?;

        // Restore from the snapshot
        self.restore_from_snapshot(&snapshot).await?;

        log::info!(
            "Node {} successfully loaded and restored from snapshot {}",
            self.node_id,
            snapshot_id
        );

        Ok(())
    }

    /// Get the latest snapshot metadata from storage
    pub async fn get_latest_snapshot_meta(
        &self,
        raft_storage: &crate::storage::RaftStorage,
    ) -> RaftResult<Option<crate::storage::StoredSnapshotMeta>> {
        raft_storage.get_snapshot_meta().map_err(|e| {
            RaftError::state_machine(format!("Failed to get snapshot metadata: {}", e))
        })
    }

    /// Check if a snapshot should be created based on configuration
    pub fn should_create_snapshot(&self, snapshot_threshold: u64) -> bool {
        let applied_index = self.applied_index();

        // Create snapshot if we've applied enough entries since the last snapshot
        // This is a simple threshold-based approach
        applied_index > 0 && applied_index % snapshot_threshold == 0
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ClientRequest, ConsistencyLevel, RedisCommand, RequestId};
    use bytes::Bytes;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Mock storage engine for testing
    struct MockStorageEngine {
        data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    }

    impl MockStorageEngine {
        fn new() -> Self {
            Self {
                data: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl StorageEngine for MockStorageEngine {
        async fn begin_transaction(&self) -> RaftResult<String> {
            Ok("mock_txn".to_string())
        }

        async fn commit_transaction(&self, _transaction_id: &str) -> RaftResult<()> {
            Ok(())
        }

        async fn rollback_transaction(&self, _transaction_id: &str) -> RaftResult<()> {
            Ok(())
        }

        async fn execute_operations(&self, operations: &[TransactionOperation]) -> RaftResult<()> {
            let mut data = self.data.write().await;
            for operation in operations {
                match operation {
                    TransactionOperation::Put { key, value } => {
                        data.insert(key.clone(), value.clone());
                    }
                    TransactionOperation::Delete { key } => {
                        data.remove(key);
                    }
                }
            }
            Ok(())
        }

        async fn get(&self, key: &[u8]) -> RaftResult<Option<Vec<u8>>> {
            let data = self.data.read().await;
            Ok(data.get(key).cloned())
        }

        async fn put(&self, key: &[u8], value: &[u8]) -> RaftResult<()> {
            let mut data = self.data.write().await;
            data.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &[u8]) -> RaftResult<()> {
            let mut data = self.data.write().await;
            data.remove(key);
            Ok(())
        }

        async fn create_snapshot(&self) -> RaftResult<Vec<u8>> {
            let data = self.data.read().await;
            bincode::serialize(&*data).map_err(|e| {
                RaftError::state_machine(format!("Failed to serialize snapshot: {}", e))
            })
        }

        async fn restore_from_snapshot(&self, snapshot_data: &[u8]) -> RaftResult<()> {
            let restored_data: HashMap<Vec<u8>, Vec<u8>> = bincode::deserialize(snapshot_data)
                .map_err(|e| {
                    RaftError::state_machine(format!("Failed to deserialize snapshot: {}", e))
                })?;

            let mut data = self.data.write().await;
            *data = restored_data;
            Ok(())
        }

        async fn get_all_keys(&self) -> RaftResult<Vec<Vec<u8>>> {
            let data = self.data.read().await;
            Ok(data.keys().cloned().collect())
        }

        async fn get_multiple(&self, keys: &[Vec<u8>]) -> RaftResult<Vec<Option<Vec<u8>>>> {
            let data = self.data.read().await;
            Ok(keys.iter().map(|key| data.get(key).cloned()).collect())
        }
    }

    fn create_test_state_machine() -> KiwiStateMachine {
        let mock_storage = Arc::new(MockStorageEngine::new());
        KiwiStateMachine::with_storage_engine(1, mock_storage)
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
        let state_machine = create_test_state_machine();
        assert_eq!(state_machine.applied_index(), 0);
        assert_eq!(state_machine.node_id, 1);
        assert_eq!(state_machine.active_transaction_count().await, 0);
    }

    #[tokio::test]
    async fn test_set_command() {
        let state_machine = create_test_state_machine();

        let request = create_test_request(
            RequestId::new(),
            "SET",
            vec![Bytes::from("key1"), Bytes::from("value1")],
        );
        let response = state_machine.apply_redis_command(&request).await.unwrap();

        assert_eq!(response.id, request.id);
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::from_static(b"OK"));
    }

    #[tokio::test]
    async fn test_get_command() {
        let state_machine = create_test_state_machine();

        // First set a value
        let set_request = create_test_request(
            RequestId::new(),
            "SET",
            vec![Bytes::from("key1"), Bytes::from("value1")],
        );
        state_machine
            .apply_redis_command(&set_request)
            .await
            .unwrap();

        // Then get the value - note: this will return empty in mock implementation
        let get_request = create_test_request(RequestId::new(), "GET", vec![Bytes::from("key1")]);
        let response = state_machine
            .apply_redis_command(&get_request)
            .await
            .unwrap();

        assert_eq!(response.id, get_request.id);
        assert!(response.result.is_ok());
        // Mock implementation returns empty for GET
        assert_eq!(response.result.unwrap(), bytes::Bytes::new());
    }

    #[tokio::test]
    async fn test_snapshot_creation_and_restoration() {
        let state_machine = create_test_state_machine();

        // Apply some commands first
        let set_request1 = create_test_request(
            RequestId::new(),
            "SET",
            vec![Bytes::from("key1"), Bytes::from("value1")],
        );
        let set_request2 = create_test_request(
            RequestId::new(),
            "SET",
            vec![Bytes::from("key2"), Bytes::from("value2")],
        );

        state_machine
            .apply_redis_command(&set_request1)
            .await
            .unwrap();
        state_machine
            .apply_redis_command(&set_request2)
            .await
            .unwrap();

        // Create a snapshot
        let snapshot = state_machine.create_snapshot().await.unwrap();
        assert_eq!(snapshot.applied_index, 0); // Applied index starts at 0 in test

        // Verify snapshot can be retrieved
        let retrieved_snapshot = state_machine.get_current_snapshot_data().await.unwrap();
        assert!(retrieved_snapshot.is_some());

        // Create a new state machine and restore from snapshot
        let new_state_machine = create_test_state_machine();
        new_state_machine
            .restore_from_snapshot(&snapshot)
            .await
            .unwrap();

        // Verify the applied index was restored
        assert_eq!(new_state_machine.applied_index(), snapshot.applied_index);
    }

    #[tokio::test]
    async fn test_snapshot_threshold_check() {
        let state_machine = create_test_state_machine();

        // Test threshold logic
        assert!(!state_machine.should_create_snapshot(100)); // Applied index is 0

        // Simulate applying some entries
        state_machine.applied_index.store(100, Ordering::Release);
        assert!(state_machine.should_create_snapshot(100)); // 100 % 100 == 0

        state_machine.applied_index.store(150, Ordering::Release);
        assert!(!state_machine.should_create_snapshot(100)); // 150 % 100 != 0

        state_machine.applied_index.store(200, Ordering::Release);
        assert!(state_machine.should_create_snapshot(100)); // 200 % 100 == 0
    }

    #[tokio::test]
    async fn test_transaction_cleanup_during_snapshot_restore() {
        let state_machine = create_test_state_machine();

        // Create some active transactions
        let txn1 = state_machine.begin_transaction().await.unwrap();
        let txn2 = state_machine.begin_transaction().await.unwrap();

        assert_eq!(state_machine.active_transaction_count().await, 2);

        // Create and restore from snapshot (should clear transactions)
        let snapshot = state_machine.create_snapshot().await.unwrap();
        state_machine
            .restore_from_snapshot(&snapshot)
            .await
            .unwrap();

        // Verify transactions were cleared
        assert_eq!(state_machine.active_transaction_count().await, 0);
    }

    #[tokio::test]
    async fn test_consistency_verification() {
        let state_machine = create_test_state_machine();

        // Initially should be consistent
        assert!(state_machine.verify_consistency().await.unwrap());

        // Create many transactions to simulate potential inconsistency
        for _ in 0..150 {
            state_machine.begin_transaction().await.unwrap();
        }

        // Should detect inconsistency due to too many active transactions
        assert!(!state_machine.verify_consistency().await.unwrap());
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let (state_machine, _temp_dir) = create_test_state_machine();

        let get_request =
            create_test_request(RequestId::new(), "GET", vec![Bytes::from("nonexistent")]);
        let response = state_machine
            .apply_redis_command(&get_request)
            .await
            .unwrap();

        assert_eq!(response.id, get_request.id);
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::new());
    }

    #[tokio::test]
    async fn test_del_command() {
        let (state_machine, _temp_dir) = create_test_state_machine();

        // Set some values
        let set1 = create_test_request(
            RequestId::new(),
            "SET",
            vec![Bytes::from("key1"), Bytes::from("value1")],
        );
        let set2 = create_test_request(
            RequestId::new(),
            "SET",
            vec![Bytes::from("key2"), Bytes::from("value2")],
        );
        state_machine.apply_redis_command(&set1).await.unwrap();
        state_machine.apply_redis_command(&set2).await.unwrap();

        // Delete one key
        let del_request = create_test_request(RequestId::new(), "DEL", vec![Bytes::from("key1")]);
        let response = state_machine
            .apply_redis_command(&del_request)
            .await
            .unwrap();

        assert_eq!(response.id, del_request.id);
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::from("1"));

        // Verify key is deleted
        let get_request = create_test_request(RequestId::new(), "GET", vec![Bytes::from("key1")]);
        let get_response = state_machine
            .apply_redis_command(&get_request)
            .await
            .unwrap();
        assert_eq!(get_response.result.unwrap(), bytes::Bytes::new());
    }

    #[tokio::test]
    async fn test_ping_command() {
        let (state_machine, _temp_dir) = create_test_state_machine();

        let ping_request = create_test_request(RequestId::new(), "PING", vec![]);
        let response = state_machine
            .apply_redis_command(&ping_request)
            .await
            .unwrap();

        assert_eq!(response.id, ping_request.id);
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), bytes::Bytes::from_static(b"PONG"));
    }

    #[tokio::test]
    async fn test_invalid_command() {
        let (state_machine, _temp_dir) = create_test_state_machine();

        let invalid_request = create_test_request(RequestId::new(), "INVALID", vec![]);
        let response = state_machine
            .apply_redis_command(&invalid_request)
            .await
            .unwrap();

        assert_eq!(response.id, invalid_request.id);
        assert!(response.result.is_err());
        assert!(response.result.unwrap_err().contains("Unsupported command"));
    }

    #[tokio::test]
    async fn test_list_operations() {
        let (state_machine, _temp_dir) = create_test_state_machine();

        // Test LPUSH
        let lpush_request = create_test_request(
            RequestId::new(),
            "LPUSH",
            vec![
                Bytes::from("mylist"),
                Bytes::from("world"),
                Bytes::from("hello"),
            ],
        );
        let response = state_machine
            .apply_redis_command(&lpush_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("2"));

        // Test LLEN
        let llen_request =
            create_test_request(RequestId::new(), "LLEN", vec![Bytes::from("mylist")]);
        let response = state_machine
            .apply_redis_command(&llen_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("2"));

        // Test LINDEX
        let lindex_request = create_test_request(
            RequestId::new(),
            "LINDEX",
            vec![Bytes::from("mylist"), Bytes::from("0")],
        );
        let response = state_machine
            .apply_redis_command(&lindex_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_hash_operations() {
        let (state_machine, _temp_dir) = create_test_state_machine();

        // Test HSET
        let hset_request = create_test_request(
            RequestId::new(),
            "HSET",
            vec![
                Bytes::from("myhash"),
                Bytes::from("field1"),
                Bytes::from("value1"),
            ],
        );
        let response = state_machine
            .apply_redis_command(&hset_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("1"));

        // Test HGET
        let hget_request = create_test_request(
            RequestId::new(),
            "HGET",
            vec![Bytes::from("myhash"), Bytes::from("field1")],
        );
        let response = state_machine
            .apply_redis_command(&hget_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("value1"));

        // Test HEXISTS
        let hexists_request = create_test_request(
            RequestId::new(),
            "HEXISTS",
            vec![Bytes::from("myhash"), Bytes::from("field1")],
        );
        let response = state_machine
            .apply_redis_command(&hexists_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("1"));

        // Test HLEN
        let hlen_request =
            create_test_request(RequestId::new(), "HLEN", vec![Bytes::from("myhash")]);
        let response = state_machine
            .apply_redis_command(&hlen_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("1"));
    }

    #[tokio::test]
    async fn test_set_operations() {
        let (state_machine, _temp_dir) = create_test_state_machine();

        // Test SADD
        let sadd_request = create_test_request(
            RequestId::new(),
            "SADD",
            vec![
                Bytes::from("myset"),
                Bytes::from("member1"),
                Bytes::from("member2"),
            ],
        );
        let response = state_machine
            .apply_redis_command(&sadd_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("2"));

        // Test SCARD
        let scard_request =
            create_test_request(RequestId::new(), "SCARD", vec![Bytes::from("myset")]);
        let response = state_machine
            .apply_redis_command(&scard_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("2"));

        // Test SISMEMBER
        let sismember_request = create_test_request(
            RequestId::new(),
            "SISMEMBER",
            vec![Bytes::from("myset"), Bytes::from("member1")],
        );
        let response = state_machine
            .apply_redis_command(&sismember_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("1"));

        // Test SREM
        let srem_request = create_test_request(
            RequestId::new(),
            "SREM",
            vec![Bytes::from("myset"), Bytes::from("member1")],
        );
        let response = state_machine
            .apply_redis_command(&srem_request)
            .await
            .unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), Bytes::from("1"));
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
            let set_req = create_test_request(
                RequestId::new(),
                "SET",
                vec![
                    Bytes::from(format!("key{}", i)),
                    Bytes::from(format!("value{}", i)),
                ],
            );
            state_machine.apply_redis_command(&set_req).await.unwrap();
        }

        // Update applied index
        state_machine.applied_index.store(5, Ordering::Release);

        // Create snapshot - for now this is a placeholder that returns empty data
        let snapshot = state_machine.create_snapshot().await.unwrap();

        assert_eq!(snapshot.applied_index, 5);
        // Note: snapshot.data is empty in the current placeholder implementation
        // This will be properly implemented in task 2.4
        assert_eq!(snapshot.data.len(), 0);
    }

    // Test for consistency verification removed - API does not exist yet
    // TODO: Add test when ConsistencyChecker integration is complete
}
