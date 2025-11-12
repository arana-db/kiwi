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

//! Error types for Raft consensus implementation

use crate::types::NodeId;
use thiserror::Error;

/// Comprehensive error type for Raft operations
/// 
/// This error type provides detailed context for all Raft-related errors,
/// ensuring proper error propagation and debugging information.
#[derive(Debug, Error)]
pub enum RaftError {
    /// Node is not the leader - requests should be redirected
    #[error("Not leader: current leader is {leader_id:?}, context: {context}")]
    NotLeader {
        leader_id: Option<NodeId>,
        context: String,
    },

    /// Network-related errors (connection failures, timeouts, etc.)
    #[error("Network error: {0}")]
    Network(#[from] NetworkError),

    /// Storage-related errors (disk I/O, corruption, etc.)
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// Consensus protocol errors from Openraft
    #[error("Consensus error: {0}")]
    Consensus(#[from] openraft::error::RaftError<NodeId>),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// I/O errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Configuration errors (invalid settings, missing parameters, etc.)
    #[error("Configuration error: {message}, context: {context}")]
    Configuration { message: String, context: String },

    /// Consistency errors (data integrity issues)
    #[error("Consistency error: {message}, context: {context}")]
    Consistency { message: String, context: String },

    /// Fatal errors that require node shutdown
    #[error("Fatal error: {0}")]
    Fatal(#[from] openraft::error::Fatal<NodeId>),

    /// State machine errors (command application failures)
    #[error("State machine error: {message}, context: {context}")]
    StateMachine { message: String, context: String },

    /// Timeout errors
    #[error("Timeout error: {operation}, duration: {duration_ms}ms, context: {context}")]
    Timeout {
        operation: String,
        duration_ms: u64,
        context: String,
    },

    /// Invalid request errors (malformed requests, unsupported operations)
    #[error("Invalid request: {message}, context: {context}")]
    InvalidRequest { message: String, context: String },

    /// Invalid state errors (operation not allowed in current state)
    #[error("Invalid state: {message}, current_state: {current_state}, context: {context}")]
    InvalidState {
        message: String,
        current_state: String,
        context: String,
    },

    /// Resource exhausted errors (memory, disk space, etc.)
    #[error("Resource exhausted: {resource}, message: {message}, context: {context}")]
    ResourceExhausted {
        resource: String,
        message: String,
        context: String,
    },

    /// Not found errors (missing data, unknown nodes, etc.)
    #[error("Not found: {resource}, message: {message}, context: {context}")]
    NotFound {
        resource: String,
        message: String,
        context: String,
    },
}

/// Network-related errors
/// 
/// These errors represent issues with network communication between Raft nodes,
/// including connection failures, timeouts, and protocol violations.
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Connection to a node failed
    #[error("Connection failed to node {node_id} at {endpoint}: {source}, context: {context}")]
    ConnectionFailed {
        node_id: NodeId,
        endpoint: String,
        #[source]
        source: std::io::Error,
        context: String,
    },

    /// Connection to an address failed
    #[error("Connection failed to address {address}: {source}, context: {context}")]
    ConnectionFailedToAddress {
        address: String,
        #[source]
        source: std::io::Error,
        context: String,
    },

    /// Request to a node timed out
    #[error("Request timeout for node {node_id} after {timeout_ms}ms, operation: {operation}, context: {context}")]
    RequestTimeout {
        node_id: NodeId,
        timeout_ms: u64,
        operation: String,
        context: String,
    },

    /// Invalid response received from a node
    #[error("Invalid response from node {node_id}: {message}, expected: {expected}, context: {context}")]
    InvalidResponse {
        node_id: NodeId,
        message: String,
        expected: String,
        context: String,
    },

    /// Network partition detected
    #[error("Network partition detected: {affected_nodes} nodes affected, context: {context}")]
    NetworkPartition {
        affected_nodes: usize,
        context: String,
    },

    /// Message serialization failed
    #[error("Message serialization failed: {source}, context: {context}")]
    SerializationFailed {
        #[source]
        source: serde_json::Error,
        context: String,
    },
}

impl From<serde_json::Error> for NetworkError {
    fn from(source: serde_json::Error) -> Self {
        NetworkError::SerializationFailed {
            source,
            context: String::new(),
        }
    }
}

impl NetworkError {
    /// Create a connection failed error with context
    pub fn connection_failed_with_context<E: Into<String>, C: Into<String>>(
        node_id: NodeId,
        endpoint: E,
        source: std::io::Error,
        context: C,
    ) -> Self {
        NetworkError::ConnectionFailed {
            node_id,
            endpoint: endpoint.into(),
            source,
            context: context.into(),
        }
    }

    /// Create a request timeout error with context
    pub fn request_timeout_with_context<O: Into<String>, C: Into<String>>(
        node_id: NodeId,
        timeout_ms: u64,
        operation: O,
        context: C,
    ) -> Self {
        NetworkError::RequestTimeout {
            node_id,
            timeout_ms,
            operation: operation.into(),
            context: context.into(),
        }
    }

    /// Create an invalid response error with context
    pub fn invalid_response_with_context<M: Into<String>, X: Into<String>, C: Into<String>>(
        node_id: NodeId,
        message: M,
        expected: X,
        context: C,
    ) -> Self {
        NetworkError::InvalidResponse {
            node_id,
            message: message.into(),
            expected: expected.into(),
            context: context.into(),
        }
    }

    /// Create a network partition error with context
    pub fn network_partition_with_context<C: Into<String>>(
        affected_nodes: usize,
        context: C,
    ) -> Self {
        NetworkError::NetworkPartition {
            affected_nodes,
            context: context.into(),
        }
    }

    /// Add context to an existing network error
    pub fn with_context<C: Into<String>>(mut self, context: C) -> Self {
        let ctx = context.into();
        match &mut self {
            NetworkError::ConnectionFailed { context: c, .. }
            | NetworkError::ConnectionFailedToAddress { context: c, .. }
            | NetworkError::RequestTimeout { context: c, .. }
            | NetworkError::InvalidResponse { context: c, .. }
            | NetworkError::NetworkPartition { context: c, .. }
            | NetworkError::SerializationFailed { context: c, .. } => {
                if c.is_empty() {
                    *c = ctx;
                } else {
                    *c = format!("{} | {}", c, ctx);
                }
            }
        }
        self
    }
}

/// Storage-related errors
/// 
/// These errors represent issues with persistent storage operations,
/// including disk I/O, data corruption, and consistency violations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// RocksDB operation failed
    #[error("RocksDB error: {source}, context: {context}")]
    RocksDb {
        #[source]
        source: rocksdb::Error,
        context: String,
    },

    /// Log corruption detected - requires recovery
    #[error("Log corruption detected at index {index}, term: {term}, context: {context}")]
    LogCorruption {
        index: u64,
        term: u64,
        context: String,
    },

    /// Snapshot creation failed
    #[error("Snapshot creation failed: {message}, snapshot_id: {snapshot_id}, context: {context}")]
    SnapshotCreationFailed {
        message: String,
        snapshot_id: String,
        context: String,
    },

    /// Snapshot restoration failed
    #[error("Snapshot restoration failed: {message}, snapshot_id: {snapshot_id}, context: {context}")]
    SnapshotRestorationFailed {
        message: String,
        snapshot_id: String,
        context: String,
    },

    /// Insufficient disk space
    #[error("Disk space insufficient: required {required_bytes} bytes, available {available_bytes} bytes, path: {path}")]
    InsufficientDiskSpace {
        required_bytes: u64,
        available_bytes: u64,
        path: String,
    },

    /// Data inconsistency detected
    #[error("Data inconsistency detected: {message}, context: {context}")]
    DataInconsistency { message: String, context: String },
}

/// Result type alias for Raft operations
pub type RaftResult<T> = Result<T, RaftError>;

impl From<rocksdb::Error> for StorageError {
    fn from(source: rocksdb::Error) -> Self {
        StorageError::RocksDb {
            source,
            context: String::new(),
        }
    }
}

impl StorageError {
    /// Create a RocksDB error with context
    pub fn rocksdb_with_context<C: Into<String>>(source: rocksdb::Error, context: C) -> Self {
        StorageError::RocksDb {
            source,
            context: context.into(),
        }
    }

    /// Create a log corruption error with context
    pub fn log_corruption_with_context<C: Into<String>>(
        index: u64,
        term: u64,
        context: C,
    ) -> Self {
        StorageError::LogCorruption {
            index,
            term,
            context: context.into(),
        }
    }

    /// Create a snapshot creation failed error with context
    pub fn snapshot_creation_failed_with_context<M: Into<String>, S: Into<String>, C: Into<String>>(
        message: M,
        snapshot_id: S,
        context: C,
    ) -> Self {
        StorageError::SnapshotCreationFailed {
            message: message.into(),
            snapshot_id: snapshot_id.into(),
            context: context.into(),
        }
    }

    /// Create a snapshot restoration failed error with context
    pub fn snapshot_restoration_failed_with_context<M: Into<String>, S: Into<String>, C: Into<String>>(
        message: M,
        snapshot_id: S,
        context: C,
    ) -> Self {
        StorageError::SnapshotRestorationFailed {
            message: message.into(),
            snapshot_id: snapshot_id.into(),
            context: context.into(),
        }
    }

    /// Create an insufficient disk space error with details
    pub fn insufficient_disk_space<P: Into<String>>(
        required_bytes: u64,
        available_bytes: u64,
        path: P,
    ) -> Self {
        StorageError::InsufficientDiskSpace {
            required_bytes,
            available_bytes,
            path: path.into(),
        }
    }

    /// Create a data inconsistency error with context
    pub fn data_inconsistency_with_context<M: Into<String>, C: Into<String>>(
        message: M,
        context: C,
    ) -> Self {
        StorageError::DataInconsistency {
            message: message.into(),
            context: context.into(),
        }
    }

    /// Add context to an existing storage error
    pub fn with_context<C: Into<String>>(mut self, context: C) -> Self {
        let ctx = context.into();
        match &mut self {
            StorageError::RocksDb { context: c, .. }
            | StorageError::LogCorruption { context: c, .. }
            | StorageError::SnapshotCreationFailed { context: c, .. }
            | StorageError::SnapshotRestorationFailed { context: c, .. }
            | StorageError::DataInconsistency { context: c, .. } => {
                if c.is_empty() {
                    *c = ctx;
                } else {
                    *c = format!("{} | {}", c, ctx);
                }
            }
            StorageError::InsufficientDiskSpace { .. } => {}
        }
        self
    }
}

impl RaftError {
    /// Create a configuration error with context
    pub fn configuration<S: Into<String>>(message: S) -> Self {
        Self::Configuration {
            message: message.into(),
            context: String::new(),
        }
    }

    /// Create a configuration error with detailed context
    pub fn configuration_with_context<S: Into<String>, C: Into<String>>(
        message: S,
        context: C,
    ) -> Self {
        Self::Configuration {
            message: message.into(),
            context: context.into(),
        }
    }

    /// Create a consistency error with context
    pub fn consistency<S: Into<String>>(message: S) -> Self {
        Self::Consistency {
            message: message.into(),
            context: String::new(),
        }
    }

    /// Create a consistency error with detailed context
    pub fn consistency_with_context<S: Into<String>, C: Into<String>>(
        message: S,
        context: C,
    ) -> Self {
        Self::Consistency {
            message: message.into(),
            context: context.into(),
        }
    }

    /// Create a state machine error with context
    pub fn state_machine<S: Into<String>>(message: S) -> Self {
        Self::StateMachine {
            message: message.into(),
            context: String::new(),
        }
    }

    /// Create a state machine error with detailed context
    pub fn state_machine_with_context<S: Into<String>, C: Into<String>>(
        message: S,
        context: C,
    ) -> Self {
        Self::StateMachine {
            message: message.into(),
            context: context.into(),
        }
    }

    /// Create a timeout error with context
    pub fn timeout<S: Into<String>>(operation: S) -> Self {
        Self::Timeout {
            operation: operation.into(),
            duration_ms: 0,
            context: String::new(),
        }
    }

    /// Create a timeout error with duration and context
    pub fn timeout_with_context<S: Into<String>, C: Into<String>>(
        operation: S,
        duration_ms: u64,
        context: C,
    ) -> Self {
        Self::Timeout {
            operation: operation.into(),
            duration_ms,
            context: context.into(),
        }
    }

    /// Create an invalid request error with context
    pub fn invalid_request<S: Into<String>>(message: S) -> Self {
        Self::InvalidRequest {
            message: message.into(),
            context: String::new(),
        }
    }

    /// Create an invalid request error with detailed context
    pub fn invalid_request_with_context<S: Into<String>, C: Into<String>>(
        message: S,
        context: C,
    ) -> Self {
        Self::InvalidRequest {
            message: message.into(),
            context: context.into(),
        }
    }

    /// Create an invalid state error with context
    pub fn invalid_state<S: Into<String>>(message: S) -> Self {
        Self::InvalidState {
            message: message.into(),
            current_state: String::from("unknown"),
            context: String::new(),
        }
    }

    /// Create an invalid state error with detailed context
    pub fn invalid_state_with_context<S: Into<String>, T: Into<String>, C: Into<String>>(
        message: S,
        current_state: T,
        context: C,
    ) -> Self {
        Self::InvalidState {
            message: message.into(),
            current_state: current_state.into(),
            context: context.into(),
        }
    }

    /// Create a resource exhausted error with context
    pub fn resource_exhausted<S: Into<String>>(message: S) -> Self {
        Self::ResourceExhausted {
            resource: String::from("unknown"),
            message: message.into(),
            context: String::new(),
        }
    }

    /// Create a resource exhausted error with detailed context
    pub fn resource_exhausted_with_context<R: Into<String>, S: Into<String>, C: Into<String>>(
        resource: R,
        message: S,
        context: C,
    ) -> Self {
        Self::ResourceExhausted {
            resource: resource.into(),
            message: message.into(),
            context: context.into(),
        }
    }

    /// Create a not found error with context
    pub fn not_found<S: Into<String>>(message: S) -> Self {
        Self::NotFound {
            resource: String::from("unknown"),
            message: message.into(),
            context: String::new(),
        }
    }

    /// Create a not found error with detailed context
    pub fn not_found_with_context<R: Into<String>, S: Into<String>, C: Into<String>>(
        resource: R,
        message: S,
        context: C,
    ) -> Self {
        Self::NotFound {
            resource: resource.into(),
            message: message.into(),
            context: context.into(),
        }
    }

    /// Create a not leader error with context
    pub fn not_leader<S: Into<String>>(_message: S) -> Self {
        Self::NotLeader {
            leader_id: None,
            context: String::new(),
        }
    }

    /// Create a not leader error with leader ID and context
    pub fn not_leader_with_context<C: Into<String>>(leader_id: Option<NodeId>, context: C) -> Self {
        Self::NotLeader {
            leader_id,
            context: context.into(),
        }
    }

    /// Check if this error indicates the node is not the leader
    pub fn is_not_leader(&self) -> bool {
        matches!(self, RaftError::NotLeader { .. })
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            RaftError::Network(_) | RaftError::Timeout { .. } | RaftError::NotLeader { .. }
        )
    }

    /// Check if this error is fatal and requires node shutdown
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            RaftError::Fatal(_)
                | RaftError::Storage(StorageError::LogCorruption { .. })
                | RaftError::Storage(StorageError::DataInconsistency { .. })
        )
    }

    /// Get error category for metrics and logging
    pub fn category(&self) -> &'static str {
        match self {
            RaftError::NotLeader { .. } => "not_leader",
            RaftError::Network(_) => "network",
            RaftError::Storage(_) => "storage",
            RaftError::Consensus(_) => "consensus",
            RaftError::Serialization(_) => "serialization",
            RaftError::Io(_) => "io",
            RaftError::Configuration { .. } => "configuration",
            RaftError::Consistency { .. } => "consistency",
            RaftError::Fatal(_) => "fatal",
            RaftError::StateMachine { .. } => "state_machine",
            RaftError::Timeout { .. } => "timeout",
            RaftError::InvalidRequest { .. } => "invalid_request",
            RaftError::InvalidState { .. } => "invalid_state",
            RaftError::ResourceExhausted { .. } => "resource_exhausted",
            RaftError::NotFound { .. } => "not_found",
        }
    }

    /// Create a serialization error with context
    pub fn serialization<S: Into<String>>(message: S) -> Self {
        Self::InvalidRequest {
            message: format!("Serialization error: {}", message.into()),
            context: String::from("serialization"),
        }
    }

    /// Create a consensus error with context
    pub fn consensus<S: Into<String>>(message: S) -> Self {
        Self::InvalidRequest {
            message: format!("Consensus error: {}", message.into()),
            context: String::from("consensus"),
        }
    }

    /// Add context to an existing error
    pub fn with_context<C: Into<String>>(mut self, context: C) -> Self {
        let ctx = context.into();
        match &mut self {
            RaftError::NotLeader { context: c, .. }
            | RaftError::Configuration { context: c, .. }
            | RaftError::Consistency { context: c, .. }
            | RaftError::StateMachine { context: c, .. }
            | RaftError::Timeout { context: c, .. }
            | RaftError::InvalidRequest { context: c, .. }
            | RaftError::InvalidState { context: c, .. }
            | RaftError::ResourceExhausted { context: c, .. }
            | RaftError::NotFound { context: c, .. } => {
                if c.is_empty() {
                    *c = ctx;
                } else {
                    *c = format!("{} | {}", c, ctx);
                }
            }
            _ => {}
        }
        self
    }
}

pub mod tests;
