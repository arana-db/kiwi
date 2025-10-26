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

use serde::{Deserialize, Serialize};
use std::fmt;
use std::collections::BTreeSet;
use bytes::Bytes;

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
pub struct RequestId(u64);

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
    pub args: Vec<Vec<u8>>,
}

impl RedisCommand {
    pub fn new(command: String, args: Vec<Vec<u8>>) -> Self {
        Self { command, args }
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
    pub result: Result<Vec<u8>, String>,
    pub leader_id: Option<NodeId>,
}

/// Type configuration for openraft
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type D = ClientRequest;
    type R = ClientResponse;
    type NodeId = NodeId;
    type Node = BasicNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_uniqueness() {
        let id1 = RequestId::new();
        let id2 = RequestId::new();
        assert_ne!(id1, id2);
        assert!(id2.as_u64() > id1.as_u64());
    }

    #[test]
    fn test_request_id_serialization() {
        let id = RequestId::new();
        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: RequestId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_request_id_display() {
        let id = RequestId(42);
        assert_eq!(format!("{}", id), "42");
    }
}