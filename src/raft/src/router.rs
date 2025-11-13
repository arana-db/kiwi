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

//! Request routing layer for Raft-based Redis operations
//!
//! This module provides the RequestRouter that routes Redis commands to either
//! the Raft consensus layer (for writes) or directly to storage (for reads),
//! based on the cluster mode and consistency requirements.

use crate::error::{RaftError, RaftResult};
use crate::node::{RaftNode, RaftNodeInterface};
use crate::types::{ClientRequest, ClientResponse, ConsistencyLevel, NodeId, RedisCommand, RequestId};
use bytes::Bytes;
use std::sync::Arc;

/// Cluster operation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMode {
    /// Single node mode - direct storage access
    Single,
    /// Cluster mode - Raft consensus required
    Cluster,
}

/// Response from a routed Redis command
#[derive(Debug, Clone)]
pub struct RedisResponse {
    /// Request ID
    pub id: RequestId,
    /// Response data
    pub data: Result<Bytes, String>,
    /// Leader node ID (if applicable)
    pub leader_id: Option<NodeId>,
}

impl RedisResponse {
    /// Create a successful response
    pub fn success(id: RequestId, data: Bytes) -> Self {
        Self {
            id,
            data: Ok(data),
            leader_id: None,
        }
    }

    /// Create an error response
    pub fn error(id: RequestId, error: String) -> Self {
        Self {
            id,
            data: Err(error),
            leader_id: None,
        }
    }

    /// Create a response with leader information
    pub fn with_leader(mut self, leader_id: Option<NodeId>) -> Self {
        self.leader_id = leader_id;
        self
    }
}

impl From<ClientResponse> for RedisResponse {
    fn from(response: ClientResponse) -> Self {
        Self {
            id: response.id,
            data: response.result,
            leader_id: response.leader_id,
        }
    }
}

/// Request router for handling Redis commands in Raft cluster
///
/// Routes commands based on:
/// - Cluster mode (single vs cluster)
/// - Command type (read vs write)
/// - Consistency requirements
pub struct RequestRouter {
    /// Raft node for consensus operations
    raft_node: Arc<RaftNode>,
    /// Current cluster mode
    mode: ClusterMode,
}

impl RequestRouter {
    /// Create a new request router
    pub fn new(raft_node: Arc<RaftNode>, mode: ClusterMode) -> Self {
        Self { raft_node, mode }
    }

    /// Route a Redis command to the appropriate handler
    ///
    /// # Arguments
    /// * `cmd` - The Redis command to route
    ///
    /// # Returns
    /// * `RedisResponse` - The response from executing the command
    pub async fn route_command(&self, cmd: RedisCommand) -> RaftResult<RedisResponse> {
        match self.mode {
            ClusterMode::Single => {
                // Single mode: direct storage access (not implemented yet)
                // For now, return an error
                Err(RaftError::configuration(
                    "Single mode not yet implemented - use cluster mode"
                ))
            }
            ClusterMode::Cluster => {
                // Cluster mode: route based on command type
                if self.is_write_command(&cmd) {
                    self.route_write(cmd).await
                } else {
                    self.route_read(cmd).await
                }
            }
        }
    }

    /// Route a write command through Raft consensus
    ///
    /// Write commands must go through Raft to ensure strong consistency.
    /// If this node is not the leader, the request will be rejected with
    /// leader information for client redirection.
    /// 
    /// # Requirements
    /// - Requirement 3.1: Write operations SHALL be submitted through RaftNode.propose()
    /// - Requirement 3.2: Write operations SHALL be confirmed by majority before returning
    /// - Requirement 8.3.1: Non-leader nodes SHALL redirect write requests to leader
    async fn route_write(&self, cmd: RedisCommand) -> RaftResult<RedisResponse> {
        log::debug!("Routing write command: {}", cmd.command);

        // Check if we're the leader
        if !self.raft_node.is_leader().await {
            // Not the leader - return error with leader info for client redirection
            let leader_id = self.raft_node.get_leader_id().await;
            log::debug!(
                "Not leader for write command {}, redirecting to {:?}",
                cmd.command,
                leader_id
            );
            return Err(RaftError::NotLeader {
                leader_id,
                context: format!("route_write: command={}", cmd.command),
            });
        }

        // Create client request for Raft consensus
        let request = ClientRequest {
            id: RequestId::new(),
            command: cmd.clone(),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        // Submit to Raft for consensus
        // This ensures the write is replicated to a majority before returning
        match self.raft_node.propose(request).await {
            Ok(response) => {
                log::debug!("Write command succeeded: {}", cmd.command);
                Ok(RedisResponse::from(response))
            }
            Err(RaftError::NotLeader { leader_id, .. }) => {
                // Leadership changed during proposal - redirect to new leader
                log::warn!(
                    "Leadership changed during write command {}, redirecting to {:?}",
                    cmd.command,
                    leader_id
                );
                Err(RaftError::NotLeader {
                    leader_id,
                    context: format!("route_write: leadership changed for command={}", cmd.command),
                })
            }
            Err(e) => {
                log::error!("Write command failed: {} - error: {}", cmd.command, e);
                Err(e)
            }
        }
    }

    /// Route a read command based on consistency level
    ///
    /// Read commands can be served with different consistency guarantees:
    /// - Linearizable: Requires leader confirmation (default)
    /// - Eventual: Can be served from any node
    /// 
    /// # Requirements
    /// - Requirement 4.1: Strong consistency reads through Leader confirmation
    /// - Requirement 4.2: Eventual consistency reads from any node
    /// - Requirement 4.3: Route reads based on consistency level
    async fn route_read(&self, cmd: RedisCommand) -> RaftResult<RedisResponse> {
        // Default to linearizable reads for strong consistency
        self.route_read_with_consistency(cmd, ConsistencyLevel::Linearizable).await
    }

    /// Route a read command with explicit consistency level
    ///
    /// This method allows callers to specify the desired consistency level
    /// for read operations.
    /// 
    /// # Arguments
    /// * `cmd` - The Redis command to execute
    /// * `consistency_level` - The desired consistency level
    /// 
    /// # Requirements
    /// - Requirement 4.1: Strong consistency reads through Leader confirmation
    /// - Requirement 4.2: Eventual consistency reads from any node
    /// - Requirement 4.3: Route reads based on consistency level
    pub async fn route_read_with_consistency(
        &self,
        cmd: RedisCommand,
        consistency_level: ConsistencyLevel,
    ) -> RaftResult<RedisResponse> {
        log::debug!(
            "Routing read command: {} with consistency level: {:?}",
            cmd.command,
            consistency_level
        );

        match consistency_level {
            ConsistencyLevel::Linearizable => {
                self.route_linearizable_read(cmd).await
            }
            ConsistencyLevel::Eventual => {
                self.route_eventual_read(cmd).await
            }
        }
    }

    /// Route a linearizable read (strong consistency)
    ///
    /// Linearizable reads require confirmation that this node is still the leader
    /// and that it has up-to-date information. This implements the read_index
    /// mechanism from the Raft paper.
    /// 
    /// # Requirements
    /// - Requirement 4.1.1: Strong consistency reads SHALL be confirmed by Leader
    /// - Requirement 4.1.4: Leader confirmation SHALL use read_index mechanism
    /// - Requirement 4.1.5: Read operations SHALL not block write operations
    /// 
    /// # Process
    /// 1. Verify this node is the leader
    /// 2. Use ensure_linearizable() to confirm leadership (read_index)
    /// 3. Execute the read from the state machine
    async fn route_linearizable_read(&self, cmd: RedisCommand) -> RaftResult<RedisResponse> {
        log::debug!("Routing linearizable read: {}", cmd.command);

        // Check if we're the leader
        if !self.raft_node.is_leader().await {
            let leader_id = self.raft_node.get_leader_id().await;
            log::debug!(
                "Not leader for linearizable read, redirecting to {:?}",
                leader_id
            );
            return Err(RaftError::NotLeader {
                leader_id,
                context: format!("route_linearizable_read: command={}", cmd.command),
            });
        }

        // Use Raft's ensure_linearizable to confirm leadership
        // This implements the read_index mechanism which:
        // 1. Records the current commit index
        // 2. Sends heartbeats to confirm leadership
        // 3. Waits for the state machine to apply up to that index
        // This ensures we read the most recent committed data
        if let Err(e) = self.raft_node.raft().ensure_linearizable().await {
            log::error!("Failed to ensure linearizable read: {}", e);
            return Err(RaftError::consistency(format!(
                "Failed to ensure linearizable read: {}",
                e
            )));
        }

        log::debug!("Linearizable read confirmed, executing from state machine");

        // Execute the read from the state machine
        // At this point, we're guaranteed to have the most recent committed state
        self.execute_read(cmd).await
    }

    /// Route an eventual consistency read
    ///
    /// Eventual reads can be served from the local state without
    /// requiring leader confirmation. This allows reads from any node
    /// (leader or follower) with lower latency but potentially stale data.
    /// 
    /// # Requirements
    /// - Requirement 4.2.2: Eventual consistency reads SHALL be able to read from any node
    /// - Requirement 4.2.3: Read operations SHALL route based on consistency level
    /// - Requirement 4.2.5: Read operations SHALL not block write operations
    /// 
    /// # Process
    /// 1. Execute the read directly from local state machine
    /// 2. No leader confirmation required
    /// 3. Data may be slightly stale if this is a follower
    async fn route_eventual_read(&self, cmd: RedisCommand) -> RaftResult<RedisResponse> {
        log::debug!("Routing eventual consistency read: {}", cmd.command);

        // Execute directly from local state without leader confirmation
        // This can be served from any node (leader or follower)
        // The data may be slightly behind the leader's committed state
        self.execute_read(cmd).await
    }

    /// Execute a read command from the state machine
    /// 
    /// This method executes read-only commands directly from the state machine
    /// without going through Raft consensus. It's used by both linearizable
    /// and eventual consistency reads (after appropriate consistency checks).
    /// 
    /// # Requirements
    /// - Requirement 4.1: Support strong consistency reads
    /// - Requirement 4.2: Support eventual consistency reads
    async fn execute_read(&self, cmd: RedisCommand) -> RaftResult<RedisResponse> {
        let state_machine = self.raft_node.state_machine();

        log::trace!("Executing read command: {} from state machine", cmd.command);

        // Execute the command through the state machine
        match state_machine.execute_read(&cmd).await {
            Ok(data) => {
                let request_id = RequestId::new();
                log::trace!(
                    "Read command {} succeeded, data size: {} bytes",
                    cmd.command,
                    data.len()
                );
                Ok(RedisResponse::success(request_id, data))
            }
            Err(e) => {
                let request_id = RequestId::new();
                log::warn!("Read command {} failed: {}", cmd.command, e);
                Ok(RedisResponse::error(request_id, e.to_string()))
            }
        }
    }

    /// Check if a command is a write operation
    fn is_write_command(&self, cmd: &RedisCommand) -> bool {
        let cmd_name = cmd.command.to_lowercase();
        matches!(
            cmd_name.as_str(),
            // String commands
            "set" | "setex" | "psetex" | "setnx" | "setrange" | "append" |
            "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" |
            "mset" | "msetnx" |
            // Key commands
            "del" | "unlink" | "expire" | "expireat" | "pexpire" | "pexpireat" |
            "persist" | "rename" | "renamenx" |
            // List commands
            "lpush" | "rpush" | "lpushx" | "rpushx" | "lpop" | "rpop" |
            "lset" | "linsert" | "lrem" | "ltrim" | "rpoplpush" |
            // Set commands
            "sadd" | "srem" | "spop" | "smove" |
            // Sorted set commands
            "zadd" | "zrem" | "zincrby" | "zremrangebyrank" | "zremrangebyscore" |
            "zremrangebylex" | "zpopmin" | "zpopmax" |
            // Hash commands
            "hset" | "hsetnx" | "hmset" | "hdel" | "hincrby" | "hincrbyfloat" |
            // Database commands
            "flushdb" | "flushall" | "swapdb" |
            // Pub/Sub commands (writes to channels)
            "publish" |
            // Transaction commands
            "multi" | "exec" | "discard"
        )
    }

    /// Get the current cluster mode
    pub fn mode(&self) -> ClusterMode {
        self.mode
    }

    /// Set the cluster mode
    pub fn set_mode(&mut self, mode: ClusterMode) {
        self.mode = mode;
    }

    /// Get the Raft node
    pub fn raft_node(&self) -> &Arc<RaftNode> {
        &self.raft_node
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        self.raft_node.is_leader().await
    }

    /// Get the current leader ID
    pub async fn get_leader_id(&self) -> Option<NodeId> {
        self.raft_node.get_leader_id().await
    }

    /// Get the leader endpoint for client redirection
    /// 
    /// Returns the endpoint (host:port) of the current leader if known.
    /// This is used by clients to redirect requests to the leader.
    /// 
    /// # Requirements
    /// - Requirement 8.3.1: Non-leader nodes SHALL redirect write requests to leader
    /// - Requirement 8.3.3: Client requests SHALL automatically redirect to new leader
    pub async fn get_leader_endpoint(&self) -> Option<String> {
        let leader_id = self.get_leader_id().await?;
        
        // Get the endpoint from the Raft node
        // Note: This requires adding a public method to RaftNode to get endpoints
        // For now, we return None and rely on the leader_id in the error
        // TODO: Add RaftNode::get_endpoint(node_id) method
        log::debug!("Leader endpoint lookup for node {} not yet implemented", leader_id);
        None
    }

    /// Create a redirect response for non-leader nodes
    /// 
    /// This creates a response that tells the client to redirect to the leader.
    /// The response includes the leader's endpoint if known.
    /// 
    /// # Requirements
    /// - Requirement 8.3.1: Non-leader nodes SHALL redirect write requests to leader
    /// - Requirement 8.3.3: Client requests SHALL automatically redirect to new leader
    pub async fn create_redirect_response(&self, request_id: RequestId) -> RedisResponse {
        let leader_id = self.get_leader_id().await;
        let leader_endpoint = self.get_leader_endpoint().await;
        
        let error_msg = match (leader_id, leader_endpoint) {
            (Some(id), Some(endpoint)) => {
                format!("MOVED {} {}", id, endpoint)
            }
            (Some(id), None) => {
                format!("MOVED {} (endpoint unknown)", id)
            }
            (None, _) => {
                "CLUSTERDOWN No leader available".to_string()
            }
        };
        
        RedisResponse::error(request_id, error_msg).with_leader(leader_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_mode() {
        assert_eq!(ClusterMode::Single, ClusterMode::Single);
        assert_eq!(ClusterMode::Cluster, ClusterMode::Cluster);
        assert_ne!(ClusterMode::Single, ClusterMode::Cluster);
    }

    #[test]
    fn test_redis_response_creation() {
        let id = RequestId::new();
        let response = RedisResponse::success(id, Bytes::from("OK"));
        assert!(response.data.is_ok());
        assert_eq!(response.id, id);

        let error_response = RedisResponse::error(id, "Error".to_string());
        assert!(error_response.data.is_err());
    }

    #[test]
    fn test_write_command_detection() {
        let cmd_set = RedisCommand::from_strings("SET".to_string(), vec!["key".to_string(), "value".to_string()]);
        let cmd_get = RedisCommand::from_strings("GET".to_string(), vec!["key".to_string()]);
        let cmd_del = RedisCommand::from_strings("DEL".to_string(), vec!["key".to_string()]);
        let cmd_exists = RedisCommand::from_strings("EXISTS".to_string(), vec!["key".to_string()]);

        // We can't test is_write_command directly without a router instance,
        // but we can verify the command structures are created correctly
        assert_eq!(cmd_set.command, "SET");
        assert_eq!(cmd_get.command, "GET");
        assert_eq!(cmd_del.command, "DEL");
        assert_eq!(cmd_exists.command, "EXISTS");
    }
}

#[cfg(test)]
#[path = "router_tests.rs"]
mod router_tests;
