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

//! Redis protocol integration with Raft consensus

use crate::consistency_handler::{ConsistencyHandler, ConsistencyConfig};
use crate::error::{RaftError, RaftResult};
use crate::node::RaftNode;
use crate::types::{ClientRequest, ClientResponse, ConsistencyLevel, RedisCommand, RequestId, NodeId};
use bytes::Bytes;
use resp::RespData;
use std::sync::Arc;
use std::time::Duration;

/// Raft-aware Redis command handler that routes operations through consensus
pub struct RaftRedisHandler {
    /// Reference to the Raft node
    raft_node: Arc<RaftNode>,
    /// Consistency handler for managing read consistency levels
    consistency_handler: ConsistencyHandler,
    /// Default read consistency level
    read_consistency: ConsistencyLevel,
}

impl RaftRedisHandler {
    /// Create a new Raft-aware Redis handler
    pub fn new(raft_node: Arc<RaftNode>) -> Self {
        let consistency_handler = ConsistencyHandler::new(raft_node.clone());
        Self {
            raft_node,
            consistency_handler,
            read_consistency: ConsistencyLevel::Linearizable,
        }
    }

    /// Create handler with custom read consistency
    pub fn with_consistency(raft_node: Arc<RaftNode>, consistency: ConsistencyLevel) -> Self {
        let consistency_handler = ConsistencyHandler::new(raft_node.clone());
        Self {
            raft_node,
            consistency_handler,
            read_consistency: consistency,
        }
    }

    /// Create handler with custom consistency configuration
    pub fn with_consistency_config(raft_node: Arc<RaftNode>, config: ConsistencyConfig) -> Self {
        let consistency_handler = ConsistencyHandler::with_config(raft_node.clone(), config);
        Self {
            raft_node,
            consistency_handler,
            read_consistency: ConsistencyLevel::Linearizable,
        }
    }

    /// Handle a Redis command with appropriate routing based on operation type
    pub async fn handle_command(&self, command: RedisCommand) -> RaftResult<RespData> {
        self.handle_command_with_consistency(command, None).await
    }

    /// Handle a Redis command with explicit consistency level
    pub async fn handle_command_with_consistency(
        &self, 
        command: RedisCommand, 
        consistency: Option<ConsistencyLevel>
    ) -> RaftResult<RespData> {
        let cmd_name = command.command.to_lowercase();
        
        // Determine the appropriate consistency level
        let effective_consistency = consistency.unwrap_or_else(|| {
            self.consistency_handler.get_command_consistency(&cmd_name, None)
        });

        if self.is_write_command(&cmd_name) {
            // Write commands always go through Raft consensus
            self.handle_write_command(command).await
        } else {
            // Read commands use the specified consistency level
            self.handle_read_command_with_consistency(command, effective_consistency).await
        }
    }

    /// Handle write commands through Raft consensus
    pub async fn handle_write_command(&self, command: RedisCommand) -> RaftResult<RespData> {
        log::debug!("Handling write command: {}", command.command);

        // Check if we're the leader
        if !self.raft_node.is_leader().await {
            return self.redirect_to_leader(command).await;
        }

        // Create client request for Raft consensus
        let request = ClientRequest {
            id: RequestId::new(),
            command,
            consistency_level: ConsistencyLevel::Linearizable, // Write operations always require linearizable consistency
        };

        // Submit to Raft for consensus
        match self.raft_node.propose(request).await {
            Ok(response) => {
                match response.result {
                    Ok(data) => {
                        // Convert response data to RespData
                        if data.is_empty() {
                            Ok(RespData::SimpleString("OK".into()))
                        } else {
                            Ok(RespData::BulkString(Some(Bytes::from(data))))
                        }
                    }
                    Err(error_msg) => {
                        log::error!("Write command failed: {}", error_msg);
                        Ok(RespData::Error(error_msg.into()))
                    }
                }
            }
            Err(RaftError::NotLeader { leader_id }) => {
                log::debug!("Not leader, redirecting to leader: {:?}", leader_id);
                self.redirect_to_leader_with_id(command, leader_id).await
            }
            Err(e) => {
                log::error!("Raft consensus failed: {}", e);
                Ok(RespData::Error(format!("ERR consensus failed: {}", e).into()))
            }
        }
    }

    /// Handle read commands with appropriate consistency level
    pub async fn handle_read_command(&self, command: RedisCommand) -> RaftResult<RespData> {
        self.handle_read_command_with_consistency(command, self.read_consistency).await
    }

    /// Handle read commands with explicit consistency level
    pub async fn handle_read_command_with_consistency(
        &self, 
        command: RedisCommand, 
        consistency: ConsistencyLevel
    ) -> RaftResult<RespData> {
        log::debug!("Handling read command: {} with consistency: {:?}", 
                   command.command, consistency);

        // Ensure the requested consistency level is achievable
        if !self.consistency_handler.is_consistency_supported(consistency).await? {
            let recommended = self.consistency_handler.get_recommended_consistency().await?;
            log::warn!("Requested consistency {:?} not supported, using {:?}", 
                      consistency, recommended);
            return self.handle_read_command_with_consistency(command, recommended).await;
        }

        // Ensure consistency requirements are met before proceeding
        self.consistency_handler.ensure_read_consistency(consistency).await?;

        match consistency {
            ConsistencyLevel::Linearizable => {
                self.handle_linearizable_read(command).await
            }
            ConsistencyLevel::Eventual => {
                self.handle_eventual_read(command).await
            }
        }
    }

    /// Handle linearizable reads (confirm leadership before responding)
    async fn handle_linearizable_read(&self, command: RedisCommand) -> RaftResult<RespData> {
        // The consistency handler has already ensured linearizable consistency
        // We can now safely execute the read operation
        self.execute_local_read(command).await
    }

    /// Handle eventual consistency reads (read from local state)
    async fn handle_eventual_read(&self, command: RedisCommand) -> RaftResult<RespData> {
        // The consistency handler has already ensured eventual consistency requirements
        // We can now safely execute the read operation from local state
        self.execute_local_read(command).await
    }

    /// Execute a read operation on the local state machine
    async fn execute_local_read(&self, command: RedisCommand) -> RaftResult<RespData> {
        // Get the state machine and execute the read operation
        let state_machine = self.raft_node.state_machine();
        
        // For now, we'll create a mock response since the state machine integration
        // is not fully implemented yet. In a complete implementation, this would
        // delegate to the actual state machine.
        match command.command.to_lowercase().as_str() {
            "get" => {
                if command.args.len() != 1 {
                    return Ok(RespData::Error("ERR wrong number of arguments for 'get' command".into()));
                }
                // Mock response - in real implementation, would query state machine
                Ok(RespData::BulkString(None)) // Key not found
            }
            "exists" => {
                if command.args.is_empty() {
                    return Ok(RespData::Error("ERR wrong number of arguments for 'exists' command".into()));
                }
                // Mock response - in real implementation, would check state machine
                Ok(RespData::Integer(0)) // No keys exist
            }
            "keys" => {
                if command.args.len() != 1 {
                    return Ok(RespData::Error("ERR wrong number of arguments for 'keys' command".into()));
                }
                // Mock response - in real implementation, would query state machine
                Ok(RespData::Array(Some(vec![]))) // No keys match
            }
            "info" => {
                // Handle INFO command with Raft-specific information
                self.handle_info_command(command).await
            }
            _ => {
                // For other read commands, return a generic response
                log::warn!("Unhandled read command: {}", command.command);
                Ok(RespData::Error(format!("ERR command '{}' not implemented in Raft mode", command.command).into()))
            }
        }
    }

    /// Handle INFO command with Raft-specific information
    async fn handle_info_command(&self, command: RedisCommand) -> RaftResult<RespData> {
        let section = if !command.args.is_empty() {
            String::from_utf8_lossy(&command.args[0]).to_lowercase()
        } else {
            "default".to_string()
        };

        let mut info = String::new();

        match section.as_str() {
            "raft" => {
                let metrics = self.raft_node.get_metrics().await?;
                let state = self.raft_node.get_node_state().await;
                let term = self.raft_node.get_current_term().await;
                let leader_id = self.raft_node.get_leader_id().await;

                info.push_str("# Raft\r\n");
                info.push_str(&format!("raft_state:{:?}\r\n", state));
                info.push_str(&format!("raft_term:{}\r\n", term));
                info.push_str(&format!("raft_leader:{}\r\n", 
                    leader_id.map(|id| id.to_string()).unwrap_or_else(|| "none".to_string())));
                info.push_str(&format!("raft_commit_index:{}\r\n", 
                    metrics.last_applied.map(|id| id.index).unwrap_or(0)));
                info.push_str(&format!("raft_last_applied:{}\r\n", 
                    metrics.last_applied.map(|id| id.index).unwrap_or(0)));
                info.push_str(&format!("raft_log_size:{}\r\n", 
                    metrics.last_log_index.map(|id| id.index).unwrap_or(0)));
            }
            "cluster" => {
                let health = self.raft_node.get_cluster_health().await?;
                
                info.push_str("# Cluster\r\n");
                info.push_str("cluster_enabled:1\r\n");
                info.push_str(&format!("cluster_state:{}\r\n", 
                    if health.is_healthy { "ok" } else { "fail" }));
                info.push_str("cluster_slots_assigned:16384\r\n");
                info.push_str("cluster_slots_ok:16384\r\n");
                info.push_str("cluster_slots_pfail:0\r\n");
                info.push_str("cluster_slots_fail:0\r\n");
                info.push_str(&format!("cluster_known_nodes:{}\r\n", health.total_members));
                info.push_str(&format!("cluster_size:{}\r\n", health.healthy_members));
                info.push_str("cluster_current_epoch:1\r\n");
                info.push_str("cluster_my_epoch:1\r\n");
            }
            "server" | "default" | _ => {
                info.push_str("# Server\r\n");
                info.push_str("redis_version:7.0.0\r\n");
                info.push_str("redis_git_sha1:00000000\r\n");
                info.push_str("redis_git_dirty:0\r\n");
                info.push_str("redis_build_id:0\r\n");
                info.push_str("redis_mode:cluster\r\n");
                info.push_str("os:Windows\r\n");
                info.push_str("arch_bits:64\r\n");
                info.push_str("multiplexing_api:select\r\n");
                info.push_str("atomicvar_api:atomic-builtin\r\n");
                info.push_str("gcc_version:0.0.0\r\n");
                info.push_str("process_id:1\r\n");
                info.push_str("tcp_port:7379\r\n");
                info.push_str("uptime_in_seconds:1\r\n");
                info.push_str("uptime_in_days:0\r\n");
                info.push_str("hz:10\r\n");
                info.push_str("configured_hz:10\r\n");
                info.push_str("lru_clock:1\r\n");
                info.push_str("executable:/path/to/kiwi-server\r\n");
                info.push_str("config_file:\r\n");
                
                if section == "default" {
                    let health = self.raft_node.get_cluster_health().await?;
                    info.push_str("\r\n# Cluster\r\n");
                    info.push_str("cluster_enabled:1\r\n");
                    info.push_str(&format!("cluster_state:{}\r\n", 
                        if health.is_healthy { "ok" } else { "fail" }));
                    info.push_str(&format!("cluster_known_nodes:{}\r\n", health.total_members));
                    info.push_str(&format!("cluster_size:{}\r\n", health.healthy_members));
                }
            }
        }

        Ok(RespData::BulkString(Some(Bytes::from(info))))
    }

    /// Redirect write commands to the current leader
    pub async fn redirect_to_leader(&self, command: RedisCommand) -> RaftResult<RespData> {
        let leader_id = self.raft_node.get_leader_id().await;
        self.redirect_to_leader_with_id(command, leader_id).await
    }

    /// Redirect to a specific leader
    async fn redirect_to_leader_with_id(&self, _command: RedisCommand, leader_id: Option<NodeId>) -> RaftResult<RespData> {
        match leader_id {
            Some(leader) => {
                // In a full implementation, we would forward the command to the leader
                // For now, we return a MOVED response similar to Redis Cluster
                Ok(RespData::Error(format!("MOVED 0 leader-{}", leader).into()))
            }
            None => {
                // No leader available, cluster is in election
                Ok(RespData::Error("CLUSTERDOWN The cluster is down".into()))
            }
        }
    }

    /// Static helper to determine if a command is a write operation (for testing)
    pub(crate) fn is_write_command_name(cmd_name: &str) -> bool {
        matches!(cmd_name, 
            "set" | "del" | "expire" | "expireat" | "persist" | "rename" | "renamenx" |
            "lpush" | "rpush" | "lpop" | "rpop" | "lset" | "lrem" | "ltrim" |
            "sadd" | "srem" | "spop" | "smove" |
            "zadd" | "zrem" | "zincrby" | "zremrangebyrank" | "zremrangebyscore" |
            "hset" | "hdel" | "hincrby" | "hincrbyfloat" |
            "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" |
            "append" | "setrange" | "setex" | "setnx" | "mset" | "msetnx" |
            "flushdb" | "flushall"
        )
    }

    /// Determine if a command is a write operation
    fn is_write_command(&self, cmd_name: &str) -> bool {
        Self::is_write_command_name(cmd_name)
    }

    /// Set the read consistency level
    pub fn set_read_consistency(&mut self, consistency: ConsistencyLevel) {
        self.read_consistency = consistency;
    }

    /// Get the current read consistency level
    pub fn get_read_consistency(&self) -> ConsistencyLevel {
        self.read_consistency
    }

    /// Update consistency configuration
    pub fn update_consistency_config(&mut self, config: ConsistencyConfig) {
        self.consistency_handler.update_config(config);
    }

    /// Get current consistency configuration
    pub fn get_consistency_config(&self) -> &ConsistencyConfig {
        self.consistency_handler.get_config()
    }

    /// Check if a consistency level is supported
    pub async fn is_consistency_supported(&self, consistency: ConsistencyLevel) -> RaftResult<bool> {
        self.consistency_handler.is_consistency_supported(consistency).await
    }

    /// Get recommended consistency level for current cluster state
    pub async fn get_recommended_consistency(&self) -> RaftResult<ConsistencyLevel> {
        self.consistency_handler.get_recommended_consistency().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_command_detection() {
        assert!(RaftRedisHandler::is_write_command_name("set"));
        assert!(RaftRedisHandler::is_write_command_name("del"));
        assert!(RaftRedisHandler::is_write_command_name("lpush"));
        assert!(!RaftRedisHandler::is_write_command_name("get"));
        assert!(!RaftRedisHandler::is_write_command_name("exists"));
        assert!(!RaftRedisHandler::is_write_command_name("keys"));
    }

    #[test]
    #[ignore = "Needs RaftNode mock; follow-up PR will add a test double."]
    fn test_consistency_level_management() {
        // TODO: Add proper test with mock RaftNode
        // This test requires a RaftNode instance which is complex to mock
    }
}