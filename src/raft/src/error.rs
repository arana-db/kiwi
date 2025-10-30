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
#[derive(Debug, Error)]
pub enum RaftError {
    #[error("Not leader: current leader is {leader_id:?}")]
    NotLeader { leader_id: Option<NodeId> },

    #[error("Network error: {0}")]
    Network(#[from] NetworkError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Consensus error: {0}")]
    Consensus(#[from] openraft::error::RaftError<NodeId>),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {message}")]
    Configuration { message: String },

    #[error("Consistency error: {message}")]
    Consistency { message: String },

    #[error("Fatal error: {0}")]
    Fatal(#[from] openraft::error::Fatal<NodeId>),

    #[error("State machine error: {message}")]
    StateMachine { message: String },

    #[error("Timeout error: {operation}")]
    Timeout { operation: String },

    #[error("Invalid request: {message}")]
    InvalidRequest { message: String },

    #[error("Invalid state: {message}")]
    InvalidState { message: String },

    #[error("Resource exhausted: {message}")]
    ResourceExhausted { message: String },

    #[error("Not found: {message}")]
    NotFound { message: String },
}

/// Network-related errors
#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("Connection failed to node {node_id}: {source}")]
    ConnectionFailed {
        node_id: NodeId,
        #[source]
        source: std::io::Error,
    },

    #[error("Connection failed to address {address}: {source}")]
    ConnectionFailedToAddress {
        address: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Request timeout for node {node_id}")]
    RequestTimeout { node_id: NodeId },

    #[error("Invalid response from node {node_id}: {message}")]
    InvalidResponse { node_id: NodeId, message: String },

    #[error("Network partition detected")]
    NetworkPartition,

    #[error("Message serialization failed: {0}")]
    SerializationFailed(#[from] serde_json::Error),
}

/// Storage-related errors
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("Log corruption detected at index {index}")]
    LogCorruption { index: u64 },

    #[error("Snapshot creation failed: {message}")]
    SnapshotCreationFailed { message: String },

    #[error("Snapshot restoration failed: {message}")]
    SnapshotRestorationFailed { message: String },

    #[error("Disk space insufficient")]
    InsufficientDiskSpace,

    #[error("Data inconsistency detected: {message}")]
    DataInconsistency { message: String },
}

/// Result type alias for Raft operations
pub type RaftResult<T> = Result<T, RaftError>;

impl RaftError {
    /// Create a configuration error
    pub fn configuration<S: Into<String>>(message: S) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Create a consistency error
    pub fn consistency<S: Into<String>>(message: S) -> Self {
        Self::Consistency {
            message: message.into(),
        }
    }

    /// Create a state machine error
    pub fn state_machine<S: Into<String>>(message: S) -> Self {
        Self::StateMachine {
            message: message.into(),
        }
    }

    /// Create a timeout error
    pub fn timeout<S: Into<String>>(operation: S) -> Self {
        Self::Timeout {
            operation: operation.into(),
        }
    }

    /// Create an invalid request error
    pub fn invalid_request<S: Into<String>>(message: S) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }

    /// Create an invalid state error
    pub fn invalid_state<S: Into<String>>(message: S) -> Self {
        Self::InvalidState {
            message: message.into(),
        }
    }

    /// Create a resource exhausted error
    pub fn resource_exhausted<S: Into<String>>(message: S) -> Self {
        Self::ResourceExhausted {
            message: message.into(),
        }
    }

    /// Create a not found error
    pub fn not_found<S: Into<String>>(message: S) -> Self {
        Self::NotFound {
            message: message.into(),
        }
    }

    /// Create a not leader error
    pub fn not_leader<S: Into<String>>(message: S) -> Self {
        Self::NotLeader {
            leader_id: None,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let config_err = RaftError::configuration("Invalid config");
        assert!(matches!(config_err, RaftError::Configuration { .. }));

        let state_err = RaftError::state_machine("State error");
        assert!(matches!(state_err, RaftError::StateMachine { .. }));

        let timeout_err = RaftError::timeout("Operation timeout");
        assert!(matches!(timeout_err, RaftError::Timeout { .. }));
    }

    #[test]
    fn test_error_classification() {
        let not_leader_err = RaftError::NotLeader { leader_id: Some(1) };
        assert!(not_leader_err.is_not_leader());
        assert!(not_leader_err.is_retryable());

        let config_err = RaftError::configuration("test");
        assert!(!config_err.is_not_leader());
        assert!(!config_err.is_retryable());
    }
}