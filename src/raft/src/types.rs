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

//! Core type definitions for Raft consensus implementation

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

/// Node identifier type
pub type NodeId = u64;

/// Term identifier type  
pub type Term = u64;

/// Log index type
pub type LogIndex = u64;

/// Log identifier combining term and index
pub type LogId = openraft::LogId<NodeId>;

/// Request identifier for client operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(pub u64);

impl RequestId {
    /// Create a new unique request ID
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Get the raw ID value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Basic node information for cluster membership
pub type BasicNode = openraft::BasicNode;

/// Raft metrics for monitoring and observability
pub type RaftMetrics = openraft::RaftMetrics<NodeId, BasicNode>;

/// Raft configuration change for cluster membership
pub type ConfigChange = openraft::ChangeMembers<NodeId, BasicNode>;

/// Entry type for Raft log
pub type Entry<T> = openraft::Entry<T>;

/// Snapshot metadata
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    pub last_log_id: Option<LogId>,
    pub last_membership: openraft::EffectiveMembership<NodeId, BasicNode>,
    pub snapshot_id: String,
}

/// Redis command representation for Raft log entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisCommand {
    pub command: String,
    pub args: Vec<Bytes>,
}

impl RedisCommand {
    pub fn new(command: String, args: Vec<Bytes>) -> Self {
        Self { command, args }
    }

    /// Create from string arguments
    pub fn from_strings(command: String, args: Vec<String>) -> Self {
        let byte_args = args.into_iter().map(|s| Bytes::from(s)).collect();
        Self {
            command,
            args: byte_args,
        }
    }

    /// Create from byte vector arguments
    pub fn from_bytes(command: String, args: Vec<Vec<u8>>) -> Self {
        let byte_args = args.into_iter().map(|v| Bytes::from(v)).collect();
        Self {
            command,
            args: byte_args,
        }
    }
}

/// Client request for Raft operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequest {
    pub id: RequestId,
    pub command: RedisCommand,
    pub consistency_level: ConsistencyLevel,
}

/// Client response from Raft operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientResponse {
    pub id: RequestId,
    pub result: Result<Bytes, String>,
    pub leader_id: Option<NodeId>,
}

impl ClientResponse {
    /// Create a successful response
    pub fn success(id: RequestId, data: Bytes, leader_id: Option<NodeId>) -> Self {
        Self {
            id,
            result: Ok(data),
            leader_id,
        }
    }

    /// Create an error response
    pub fn error(id: RequestId, error: String, leader_id: Option<NodeId>) -> Self {
        Self {
            id,
            result: Err(error),
            leader_id,
        }
    }
}

/// Type configuration for openraft
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type D = ClientRequest;
    type R = ClientResponse;
    type NodeId = NodeId;
    type Node = BasicNode;
    type Entry = openraft::Entry<TypeConfig>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<TypeConfig>;
}

/// Raft storage type alias - will be defined in storage module
// pub type RaftStorage = openraft::storage::Adaptor<TypeConfig, crate::storage::RaftStorage>;
/// Raft state machine type alias - will be defined in state_machine module
// pub type RaftStateMachine = openraft::storage::Adaptor<TypeConfig, crate::state_machine::KiwiStateMachine>;
/// Raft network type alias
pub type RaftNetwork = crate::network::RaftNetworkClient;

/// Read consistency levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConsistencyLevel {
    /// Linearizable reads - confirm leadership before responding
    Linearizable,
    /// Eventual consistency - read from local state
    Eventual,
}

impl Default for ConsistencyLevel {
    fn default() -> Self {
        ConsistencyLevel::Linearizable
    }
}

/// Cluster configuration for Raft node initialization
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: NodeId,
    pub cluster_members: BTreeSet<String>, // Format: "node_id:host:port"
    pub data_dir: String,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub snapshot_threshold: u64,
    pub max_payload_entries: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 1,
            cluster_members: BTreeSet::new(),
            data_dir: "./raft_data".to_string(),
            heartbeat_interval_ms: 1000,
            election_timeout_min_ms: 3000,
            election_timeout_max_ms: 6000,
            snapshot_threshold: 1000,
            max_payload_entries: 100,
        }
    }
}

/// Cluster health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealth {
    pub total_members: usize,
    pub healthy_members: usize,
    pub learners: usize,
    pub partitioned_nodes: usize,
    pub current_leader: Option<NodeId>,
    pub is_healthy: bool,
    pub last_log_index: u64,
    pub commit_index: u64,
}

pub mod tests;
