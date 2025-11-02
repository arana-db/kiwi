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

//! Redis protocol compatibility layer for Raft-enabled Kiwi

use crate::error::{RaftError, RaftResult};
use crate::node::RaftNode;
use crate::placeholder_types::Client;
use crate::placeholder_types::RespData;
use crate::redis_integration::RaftRedisHandler;
use crate::types::{ConsistencyLevel, NodeId, RedisCommand};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Redis protocol compatibility layer that ensures existing Redis clients work seamlessly
pub struct RedisProtocolCompatibility {
    /// Raft-aware Redis handler
    redis_handler: Arc<RaftRedisHandler>,
    /// Raft node reference
    raft_node: Arc<RaftNode>,
    /// Client connection tracking for topology changes
    client_connections: Arc<tokio::sync::RwLock<HashMap<String, ClientConnectionInfo>>>,
    /// Cluster topology information
    topology: Arc<tokio::sync::RwLock<ClusterTopology>>,
}

/// Information about client connections
#[derive(Debug, Clone)]
struct ClientConnectionInfo {
    /// Client identifier
    client_id: String,
    /// Last activity timestamp
    last_activity: std::time::Instant,
    /// Preferred consistency level for this client
    preferred_consistency: Option<ConsistencyLevel>,
    /// Whether client supports cluster redirections
    supports_cluster_redirects: bool,
}

/// Cluster topology information for Redis compatibility
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// Current cluster nodes and their endpoints
    pub nodes: HashMap<NodeId, NodeEndpoint>,
    /// Current leader node
    pub leader_id: Option<NodeId>,
    /// Cluster state (ok, fail, etc.)
    pub state: ClusterState,
    /// Last topology update timestamp
    pub last_update: std::time::Instant,
}

/// Node endpoint information
#[derive(Debug, Clone)]
pub struct NodeEndpoint {
    /// Node ID
    pub node_id: NodeId,
    /// Host address
    pub host: String,
    /// Port number
    pub port: u16,
    /// Whether node is currently reachable
    pub is_reachable: bool,
}

/// Cluster state enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterState {
    Ok,
    Fail,
    Degraded,
}

/// Cluster-specific error types for proper Redis error formatting
#[derive(Debug, Clone)]
pub enum ClusterErrorType {
    ClusterDown,
    NoLeader,
    NotLeader { leader_id: Option<NodeId> },
    NodeNotFound,
    InvalidSlot,
    ConfigurationError,
    NetworkPartition,
}

/// Redis protocol error types
#[derive(Debug, Clone)]
pub enum ProtocolError {
    InvalidCommand,
    WrongType,
    OutOfRange,
    SyntaxError,
    NoAuth,
    Loading,
    Busy,
    ReadOnly,
    NoScript,
    MasterDown,
}

impl RedisProtocolCompatibility {
    /// Create a new Redis protocol compatibility layer
    pub fn new(raft_node: Arc<RaftNode>) -> Self {
        let redis_handler = Arc::new(RaftRedisHandler::new(raft_node.clone()));

        Self {
            redis_handler,
            raft_node,
            client_connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            topology: Arc::new(tokio::sync::RwLock::new(ClusterTopology {
                nodes: HashMap::new(),
                leader_id: None,
                state: ClusterState::Ok,
                last_update: std::time::Instant::now(),
            })),
        }
    }

    /// Process a Redis command from a client with full compatibility
    pub async fn process_redis_command(
        &self,
        client: &Client,
        command: RedisCommand,
    ) -> RaftResult<RespData> {
        // Update client activity
        self.update_client_activity(client).await;

        // Validate command first
        if let Err(e) = self.validate_command(&command) {
            return Ok(self.format_redis_error(&e));
        }

        // Check if command is supported in cluster mode
        if !self.is_command_supported(&command.command) {
            return Ok(self.get_unsupported_command_error(&command.command));
        }

        // Handle cluster-specific commands first
        if let Some(response) = self.handle_cluster_commands(&command).await? {
            return Ok(response);
        }

        // Handle READONLY/READWRITE commands
        match command.command.to_lowercase().as_str() {
            "readonly" => {
                return Ok(self.handle_readonly_command(client).await?);
            }
            "readwrite" => {
                return Ok(self.handle_readwrite_command(client).await?);
            }
            _ => {}
        }

        // Handle topology change notifications
        self.check_and_handle_topology_changes(client).await?;

        // Route command based on type and current node state
        self.route_command(client, command).await
    }

    /// Route command to appropriate handler based on command type and cluster state
    async fn route_command(&self, client: &Client, command: RedisCommand) -> RaftResult<RespData> {
        let cmd_name = command.command.to_lowercase();
        let is_write = self.is_write_command(&cmd_name);
        let is_leader = self.raft_node.is_leader().await;

        // Check cluster health before processing
        if !self.is_cluster_healthy().await? {
            return Ok(self.format_cluster_error(ClusterErrorType::ClusterDown, None));
        }

        // Handle special cases that might need different error handling
        match self.handle_special_command_cases(&cmd_name, &command).await {
            Some(response) => return Ok(response),
            None => {}
        }

        if is_write {
            // Write commands must go to leader
            if is_leader {
                self.process_write_command(command).await
            } else {
                self.redirect_write_to_leader(client, command).await
            }
        } else {
            // Read commands can be handled based on consistency requirements
            self.process_read_command(client, command).await
        }
    }

    /// Handle special command cases that need specific error handling
    async fn handle_special_command_cases(
        &self,
        cmd_name: &str,
        command: &RedisCommand,
    ) -> Option<RespData> {
        match cmd_name {
            // Commands that need special validation
            "eval" | "evalsha" => Some(self.format_protocol_error(ProtocolError::NoScript)),
            "auth" => {
                // Authentication not implemented in cluster mode
                Some(RespData::SimpleString("OK".into()))
            }
            "select" => {
                // Database selection not supported in cluster mode
                Some(RespData::Error(
                    "ERR SELECT is not allowed in cluster mode".into(),
                ))
            }
            "multi" | "exec" | "discard" | "watch" | "unwatch" => {
                // Transactions have limited support in cluster mode
                Some(RespData::Error(
                    "ERR Transactions not fully supported in cluster mode".into(),
                ))
            }
            "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe" | "publish" => {
                // Pub/Sub not implemented in cluster mode
                Some(RespData::Error(
                    "ERR Pub/Sub not supported in cluster mode".into(),
                ))
            }
            "script" => {
                if !command.args.is_empty() {
                    let subcommand = String::from_utf8_lossy(&command.args[0]).to_lowercase();
                    match subcommand.as_str() {
                        "load" | "exists" | "flush" | "kill" => {
                            Some(self.format_protocol_error(ProtocolError::NoScript))
                        }
                        _ => None,
                    }
                } else {
                    Some(RespData::Error(
                        "ERR wrong number of arguments for 'script' command".into(),
                    ))
                }
            }
            _ => None,
        }
    }

    /// Process write command on leader node
    async fn process_write_command(&self, command: RedisCommand) -> RaftResult<RespData> {
        log::debug!("Processing write command on leader: {}", command.command);

        // Additional validation for write commands
        if let Err(validation_error) = self.validate_write_command(&command) {
            return Ok(self.format_validation_error(&command.command, &validation_error));
        }

        // Process the command through the Raft handler
        match self.redis_handler.handle_command(command.clone()).await {
            Ok(response) => Ok(response),
            Err(RaftError::NotLeader { leader_id }) => {
                // This shouldn't happen as we checked leadership, but handle it
                log::warn!("Lost leadership while processing write command");
                Ok(self.format_cluster_error(ClusterErrorType::NotLeader { leader_id }, None))
            }
            Err(e) => {
                log::error!("Write command processing failed: {}", e);
                Ok(self.format_redis_error(&e))
            }
        }
    }

    /// Process read command with appropriate consistency
    async fn process_read_command(
        &self,
        client: &Client,
        command: RedisCommand,
    ) -> RaftResult<RespData> {
        let client_info = self.get_client_info(client).await;
        let is_leader = self.raft_node.is_leader().await;

        // Determine consistency level based on client preferences and node state
        let consistency = client_info.preferred_consistency.unwrap_or_else(|| {
            if is_leader {
                ConsistencyLevel::Linearizable
            } else {
                ConsistencyLevel::Eventual
            }
        });

        log::debug!(
            "Processing read command with consistency {:?}: {}",
            consistency,
            command.command
        );

        // Additional validation for read commands
        if let Err(validation_error) = self.validate_read_command(&command) {
            return Ok(self.format_validation_error(&command.command, &validation_error));
        }

        // Process through Raft handler with appropriate consistency
        match self
            .redis_handler
            .handle_command_with_consistency(command.clone(), Some(consistency))
            .await
        {
            Ok(response) => Ok(response),
            Err(RaftError::NotLeader { leader_id })
                if consistency == ConsistencyLevel::Linearizable =>
            {
                // Linearizable reads require leader, redirect
                Ok(self.format_cluster_error(ClusterErrorType::NotLeader { leader_id }, None))
            }
            Err(e) => {
                log::error!("Read command processing failed: {}", e);
                Ok(self.format_redis_error(&e))
            }
        }
    }

    /// Validate write command arguments and constraints
    fn validate_write_command(&self, command: &RedisCommand) -> Result<(), String> {
        let cmd_name = command.command.to_lowercase();

        match cmd_name.as_str() {
            "set" => {
                if command.args.len() < 2 {
                    return Err("wrong number of arguments for 'set' command".to_string());
                }
                // Additional SET option validation could go here
            }
            "del" => {
                if command.args.is_empty() {
                    return Err("wrong number of arguments for 'del' command".to_string());
                }
            }
            "expire" | "expireat" => {
                if command.args.len() != 2 {
                    return Err(format!(
                        "wrong number of arguments for '{}' command",
                        cmd_name
                    ));
                }
                // Validate timeout value
                if let Ok(timeout_str) = String::from_utf8(command.args[1].to_vec()) {
                    if timeout_str.parse::<i64>().is_err() {
                        return Err("invalid expire time".to_string());
                    }
                }
            }
            _ => {} // Other commands pass through
        }

        Ok(())
    }

    /// Validate read command arguments
    fn validate_read_command(&self, command: &RedisCommand) -> Result<(), String> {
        let cmd_name = command.command.to_lowercase();

        match cmd_name.as_str() {
            "get" => {
                if command.args.len() != 1 {
                    return Err("wrong number of arguments for 'get' command".to_string());
                }
            }
            "exists" => {
                if command.args.is_empty() {
                    return Err("wrong number of arguments for 'exists' command".to_string());
                }
            }
            "keys" => {
                if command.args.len() != 1 {
                    return Err("wrong number of arguments for 'keys' command".to_string());
                }
            }
            _ => {} // Other commands pass through
        }

        Ok(())
    }

    /// Redirect write command to current leader
    async fn redirect_write_to_leader(
        &self,
        client: &Client,
        command: RedisCommand,
    ) -> RaftResult<RespData> {
        let leader_id = self.raft_node.get_leader_id().await;

        log::debug!("Redirecting write command to leader: {:?}", leader_id);

        match leader_id {
            Some(leader) => {
                // Check if we can proxy the command or need to redirect
                if self.should_proxy_command(client, &command).await {
                    self.proxy_command_to_leader(leader, command).await
                } else {
                    self.send_redirect_response(leader).await
                }
            }
            None => {
                // No leader available, cluster is in election
                Ok(RespData::Error("CLUSTERDOWN The cluster is down".into()))
            }
        }
    }

    /// Check if we should proxy the command instead of redirecting
    async fn should_proxy_command(&self, client: &Client, _command: &RedisCommand) -> bool {
        let client_info = self.get_client_info(client).await;

        // Don't proxy if client supports redirects (let client handle it)
        // Proxy for clients that don't support cluster redirects
        !client_info.supports_cluster_redirects
    }

    /// Proxy command to leader node
    async fn proxy_command_to_leader(
        &self,
        leader_id: NodeId,
        _command: RedisCommand,
    ) -> RaftResult<RespData> {
        let topology = self.topology.read().await;

        if let Some(leader_endpoint) = topology.nodes.get(&leader_id) {
            log::debug!(
                "Proxying command to leader at {}:{}",
                leader_endpoint.host,
                leader_endpoint.port
            );

            // In a full implementation, this would:
            // 1. Create HTTP/TCP connection to leader
            // 2. Send the command
            // 3. Return the response

            // For now, return an error indicating proxy is not implemented
            Ok(RespData::Error(
                "ERR command proxying not yet implemented, please connect to leader".into(),
            ))
        } else {
            Ok(RespData::Error("ERR leader endpoint not found".into()))
        }
    }

    /// Send redirect response to client
    async fn send_redirect_response(&self, leader_id: NodeId) -> RaftResult<RespData> {
        let topology = self.topology.read().await;

        if let Some(leader_endpoint) = topology.nodes.get(&leader_id) {
            Ok(RespData::Error(
                format!("MOVED 0 {}:{}", leader_endpoint.host, leader_endpoint.port).into(),
            ))
        } else {
            Ok(RespData::Error("CLUSTERDOWN The cluster is down".into()))
        }
    }

    /// Handle leader redirection with specific leader ID
    async fn handle_leader_redirection_with_id(
        &self,
        _command: RedisCommand,
        leader_id: Option<NodeId>,
    ) -> RaftResult<RespData> {
        match leader_id {
            Some(leader) => self.send_redirect_response(leader).await,
            None => Ok(RespData::Error("CLUSTERDOWN The cluster is down".into())),
        }
    }

    /// Determine if a command is a write operation
    fn is_write_command(&self, cmd_name: &str) -> bool {
        matches!(
            cmd_name,
            "set"
                | "del"
                | "expire"
                | "expireat"
                | "persist"
                | "rename"
                | "renamenx"
                | "lpush"
                | "rpush"
                | "lpop"
                | "rpop"
                | "lset"
                | "lrem"
                | "ltrim"
                | "sadd"
                | "srem"
                | "spop"
                | "smove"
                | "zadd"
                | "zrem"
                | "zincrby"
                | "zremrangebyrank"
                | "zremrangebyscore"
                | "hset"
                | "hdel"
                | "hincrby"
                | "hincrbyfloat"
                | "incr"
                | "decr"
                | "incrby"
                | "decrby"
                | "incrbyfloat"
                | "append"
                | "setrange"
                | "setex"
                | "setnx"
                | "mset"
                | "msetnx"
                | "flushdb"
                | "flushall"
        )
    }

    /// Process a Redis command from client connection data (integration with existing command flow)
    pub async fn process_client_command(&self, _client: &Client) -> RaftResult<RespData> {
        // Convert client command data to RedisCommand format
        // TODO: Implement proper client command extraction when Client type is finalized
        // For now, return an error since Client is a placeholder type
        Err(RaftError::invalid_request("Client command extraction not yet implemented for placeholder Client type"))
    }

    /// Validate and parse Redis command arguments
    pub fn validate_command(&self, command: &RedisCommand) -> RaftResult<()> {
        let cmd_name = command.command.to_lowercase();

        match cmd_name.as_str() {
            // String commands
            "get" => {
                if command.args.len() != 1 {
                    return Err(RaftError::configuration(
                        "ERR wrong number of arguments for 'get' command",
                    ));
                }
            }
            "set" => {
                if command.args.len() < 2 || command.args.len() > 5 {
                    return Err(RaftError::configuration(
                        "ERR wrong number of arguments for 'set' command",
                    ));
                }
            }
            "del" => {
                if command.args.is_empty() {
                    return Err(RaftError::configuration(
                        "ERR wrong number of arguments for 'del' command",
                    ));
                }
            }

            // List commands
            "lpush" | "rpush" => {
                if command.args.len() < 2 {
                    return Err(RaftError::configuration(&format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                }
            }
            "lpop" | "rpop" => {
                if command.args.is_empty() || command.args.len() > 2 {
                    return Err(RaftError::configuration(&format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                }
            }
            "llen" | "lindex" => {
                if command.args.len() != 1 && command.args.len() != 2 {
                    return Err(RaftError::configuration(&format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                }
            }

            // Hash commands
            "hget" => {
                if command.args.len() != 2 {
                    return Err(RaftError::configuration(
                        "ERR wrong number of arguments for 'hget' command",
                    ));
                }
            }
            "hset" => {
                if command.args.len() < 3 || command.args.len() % 2 == 0 {
                    return Err(RaftError::configuration(
                        "ERR wrong number of arguments for 'hset' command",
                    ));
                }
            }
            "hdel" => {
                if command.args.len() < 2 {
                    return Err(RaftError::configuration(
                        "ERR wrong number of arguments for 'hdel' command",
                    ));
                }
            }

            // Set commands
            "sadd" | "srem" => {
                if command.args.len() < 2 {
                    return Err(RaftError::configuration(&format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                }
            }
            "sismember" | "scard" => {
                if command.args.len() != 1 && command.args.len() != 2 {
                    return Err(RaftError::configuration(&format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                }
            }

            // Sorted set commands
            "zadd" => {
                if command.args.len() < 3 || (command.args.len() - 1) % 2 != 0 {
                    return Err(RaftError::configuration(
                        "ERR wrong number of arguments for 'zadd' command",
                    ));
                }
            }
            "zrem" => {
                if command.args.len() < 2 {
                    return Err(RaftError::configuration(
                        "ERR wrong number of arguments for 'zrem' command",
                    ));
                }
            }
            "zscore" | "zcard" => {
                if command.args.len() != 1 && command.args.len() != 2 {
                    return Err(RaftError::configuration(&format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                }
            }

            // Generic commands
            "exists" | "type" | "ttl" => {
                if command.args.len() != 1 {
                    return Err(RaftError::configuration(&format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                }
            }
            "expire" | "expireat" => {
                if command.args.len() != 2 {
                    return Err(RaftError::configuration(&format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd_name
                    )));
                }
            }

            // Info and cluster commands are handled separately
            "info" | "cluster" | "readonly" | "readwrite" => {
                // These are handled in handle_cluster_commands
            }

            _ => {
                // Unknown command - let it pass through for now
                log::warn!("Unknown command validation: {}", cmd_name);
            }
        }

        Ok(())
    }

    /// Handle Redis cluster-specific commands
    async fn handle_cluster_commands(
        &self,
        command: &RedisCommand,
    ) -> RaftResult<Option<RespData>> {
        match command.command.to_lowercase().as_str() {
            "cluster" => {
                if command.args.is_empty() {
                    return Ok(Some(RespData::Error(
                        "ERR wrong number of arguments for 'cluster' command".into(),
                    )));
                }

                let subcommand = String::from_utf8_lossy(&command.args[0]).to_lowercase();
                match subcommand.as_str() {
                    "nodes" => Ok(Some(self.handle_cluster_nodes().await?)),
                    "info" => Ok(Some(self.handle_cluster_info().await?)),
                    "slots" => Ok(Some(self.handle_cluster_slots().await?)),
                    "meet" => Ok(Some(self.handle_cluster_meet(command).await?)),
                    "forget" => Ok(Some(self.handle_cluster_forget(command).await?)),
                    "reset" => Ok(Some(self.handle_cluster_reset(command).await?)),
                    "addslots" => Ok(Some(self.handle_cluster_addslots(command).await?)),
                    "delslots" => Ok(Some(self.handle_cluster_delslots(command).await?)),
                    "failover" => Ok(Some(self.handle_cluster_failover(command).await?)),
                    "replicate" => Ok(Some(self.handle_cluster_replicate(command).await?)),
                    "saveconfig" => Ok(Some(self.handle_cluster_saveconfig(command).await?)),
                    "set-config-epoch" => {
                        Ok(Some(self.handle_cluster_set_config_epoch(command).await?))
                    }
                    "myid" => Ok(Some(self.handle_cluster_myid().await?)),
                    "count-failure-reports" => Ok(Some(
                        self.handle_cluster_count_failure_reports(command).await?,
                    )),
                    "keyslot" => Ok(Some(self.handle_cluster_keyslot(command).await?)),
                    "countkeysinslot" => {
                        Ok(Some(self.handle_cluster_countkeysinslot(command).await?))
                    }
                    "getkeysinslot" => Ok(Some(self.handle_cluster_getkeysinslot(command).await?)),
                    _ => Ok(Some(RespData::Error(
                        format!("ERR unknown CLUSTER subcommand '{}'", subcommand).into(),
                    ))),
                }
            }
            "readonly" => {
                // This will be handled in the main process_redis_command method
                Ok(None)
            }
            "readwrite" => {
                // This will be handled in the main process_redis_command method
                Ok(None)
            }
            _ => Ok(None), // Not a cluster command
        }
    }

    /// Handle CLUSTER NODES command
    async fn handle_cluster_nodes(&self) -> RaftResult<RespData> {
        let topology = self.topology.read().await;
        let mut nodes_info = String::new();

        for (node_id, endpoint) in &topology.nodes {
            let is_leader = topology.leader_id == Some(*node_id);
            let is_self = *node_id == self.raft_node.node_id();

            let flags = if is_self {
                if is_leader {
                    "myself,master"
                } else {
                    "myself,slave"
                }
            } else if is_leader {
                "master"
            } else {
                "slave"
            };

            let status = if endpoint.is_reachable {
                "connected"
            } else {
                "disconnected"
            };

            let master_id = if is_leader {
                "-".to_string()
            } else {
                topology
                    .leader_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "0".to_string())
            };

            // Format: node_id ip:port flags master ping_sent ping_recv config_epoch link_state slots
            let slots = if is_leader { "0-16383" } else { "-" };
            nodes_info.push_str(&format!(
                "{} {}:{} {} {} 0 0 1 {} {}\n",
                node_id, endpoint.host, endpoint.port, flags, master_id, status, slots
            ));
        }

        Ok(RespData::BulkString(Some(Bytes::from(nodes_info))))
    }

    /// Handle CLUSTER INFO command
    async fn handle_cluster_info(&self) -> RaftResult<RespData> {
        let health = self.raft_node.get_cluster_health().await?;
        let _topology = self.topology.read().await;

        let mut info = String::new();
        info.push_str(&format!(
            "cluster_state:{}\n",
            if health.is_healthy { "ok" } else { "fail" }
        ));
        info.push_str("cluster_slots_assigned:16384\n");
        info.push_str("cluster_slots_ok:16384\n");
        info.push_str("cluster_slots_pfail:0\n");
        info.push_str("cluster_slots_fail:0\n");
        info.push_str(&format!("cluster_known_nodes:{}\n", health.total_members));
        info.push_str(&format!("cluster_size:{}\n", health.healthy_members));
        info.push_str("cluster_current_epoch:1\n");
        info.push_str("cluster_my_epoch:1\n");
        info.push_str("cluster_stats_messages_sent:0\n");
        info.push_str("cluster_stats_messages_received:0\n");

        Ok(RespData::BulkString(Some(Bytes::from(info))))
    }

    /// Handle CLUSTER SLOTS command
    async fn handle_cluster_slots(&self) -> RaftResult<RespData> {
        let topology = self.topology.read().await;
        let mut slots_info = Vec::new();

        // For simplicity, assign all slots (0-16383) to the leader
        if let Some(leader_id) = topology.leader_id {
            if let Some(leader_endpoint) = topology.nodes.get(&leader_id) {
                let slot_range = vec![
                    RespData::Integer(0),     // Start slot
                    RespData::Integer(16383), // End slot
                    RespData::Array(Some(vec![
                        RespData::BulkString(Some(Bytes::from(leader_endpoint.host.clone()))),
                        RespData::Integer(leader_endpoint.port as i64),
                        RespData::BulkString(Some(Bytes::from(leader_id.to_string()))),
                    ])),
                ];
                slots_info.push(RespData::Array(Some(slot_range)));
            }
        }

        Ok(RespData::Array(Some(slots_info)))
    }

    /// Handle CLUSTER MEET command
    async fn handle_cluster_meet(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.len() < 2 {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster meet' command".into(),
            ));
        }

        let host = String::from_utf8_lossy(&command.args[0]);
        let port: u16 = String::from_utf8_lossy(&command.args[1])
            .parse()
            .map_err(|_| RaftError::configuration("Invalid port number"))?;

        // Check if we're the leader (only leader can initiate cluster changes)
        if !self.raft_node.is_leader().await {
            return Ok(RespData::Error(
                "ERR only leader can execute CLUSTER MEET".into(),
            ));
        }

        // Validate the endpoint
        if host.is_empty() || port == 0 {
            return Ok(RespData::Error("ERR invalid host or port".into()));
        }

        // In a full implementation, this would:
        // 1. Validate the new node is reachable
        // 2. Initiate Raft membership change
        // 3. Update cluster topology
        log::info!("CLUSTER MEET requested for {}:{}", host, port);

        // For now, simulate adding the node to topology
        // In real implementation, this would go through Raft consensus
        let new_node_id = self.generate_node_id(&host, port).await;
        self.update_node_endpoint(new_node_id, host.to_string(), port)
            .await?;

        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle CLUSTER FORGET command
    async fn handle_cluster_forget(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.len() != 1 {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster forget' command".into(),
            ));
        }

        let node_id_str = String::from_utf8_lossy(&command.args[0]);
        let node_id: NodeId = node_id_str
            .parse()
            .map_err(|_| RaftError::configuration("Invalid node ID"))?;

        // Check if we're the leader (only leader can initiate cluster changes)
        if !self.raft_node.is_leader().await {
            return Ok(RespData::Error(
                "ERR only leader can execute CLUSTER FORGET".into(),
            ));
        }

        // Prevent forgetting self
        if node_id == self.raft_node.node_id() {
            return Ok(RespData::Error("ERR can't forget myself".into()));
        }

        // Check if node exists in cluster
        let topology = self.topology.read().await;
        if !topology.nodes.contains_key(&node_id) {
            return Ok(RespData::Error("ERR unknown node".into()));
        }
        drop(topology);

        // In a full implementation, this would:
        // 1. Initiate Raft membership change to remove the node
        // 2. Update cluster topology
        log::info!("CLUSTER FORGET requested for node {}", node_id);

        // For now, simulate removing the node
        self.remove_node_from_topology(node_id).await?;

        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle CLUSTER RESET command
    async fn handle_cluster_reset(&self, command: &RedisCommand) -> RaftResult<RespData> {
        // Parse reset type (HARD or SOFT)
        let reset_type = if !command.args.is_empty() {
            String::from_utf8_lossy(&command.args[0]).to_uppercase()
        } else {
            "SOFT".to_string()
        };

        match reset_type.as_str() {
            "HARD" | "SOFT" => {
                // CLUSTER RESET is dangerous in Raft mode as it would break consensus
                // Only allow if we're the only node in the cluster
                let health = self.raft_node.get_cluster_health().await?;
                if health.total_members > 1 {
                    return Ok(RespData::Error(
                        "ERR CLUSTER RESET not allowed with multiple nodes in Raft mode".into(),
                    ));
                }

                log::warn!(
                    "CLUSTER RESET {} requested - this is dangerous in Raft mode",
                    reset_type
                );
                Ok(RespData::Error(
                    "ERR CLUSTER RESET not supported in Raft mode".into(),
                ))
            }
            _ => Ok(RespData::Error("ERR invalid CLUSTER RESET option".into())),
        }
    }

    /// Handle additional cluster commands
    async fn handle_cluster_addslots(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.is_empty() {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster addslots' command".into(),
            ));
        }

        // In Redis cluster, slots are assigned to nodes
        // In our Raft implementation, we use a single slot space managed by the leader
        Ok(RespData::Error(
            "ERR CLUSTER ADDSLOTS not applicable in Raft mode - slots are managed automatically"
                .into(),
        ))
    }

    /// Handle CLUSTER DELSLOTS command
    async fn handle_cluster_delslots(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.is_empty() {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster delslots' command".into(),
            ));
        }

        Ok(RespData::Error(
            "ERR CLUSTER DELSLOTS not applicable in Raft mode - slots are managed automatically"
                .into(),
        ))
    }

    /// Handle CLUSTER FAILOVER command
    async fn handle_cluster_failover(&self, command: &RedisCommand) -> RaftResult<RespData> {
        let force = if !command.args.is_empty() {
            String::from_utf8_lossy(&command.args[0]).to_uppercase() == "FORCE"
        } else {
            false
        };

        if force {
            log::warn!("CLUSTER FAILOVER FORCE requested");
            // In Raft, forced failover would be dangerous
            Ok(RespData::Error(
                "ERR CLUSTER FAILOVER FORCE not supported in Raft mode".into(),
            ))
        } else {
            // Normal failover in Raft happens automatically through leader election
            Ok(RespData::Error(
                "ERR CLUSTER FAILOVER not needed in Raft mode - failover is automatic".into(),
            ))
        }
    }

    /// Handle CLUSTER REPLICATE command
    async fn handle_cluster_replicate(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.len() != 1 {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster replicate' command".into(),
            ));
        }

        // In Raft, all nodes are replicas of the leader automatically
        Ok(RespData::Error(
            "ERR CLUSTER REPLICATE not applicable in Raft mode - replication is automatic".into(),
        ))
    }

    /// Handle CLUSTER SAVECONFIG command
    async fn handle_cluster_saveconfig(&self, _command: &RedisCommand) -> RaftResult<RespData> {
        // In Raft mode, configuration is managed through consensus
        log::info!("CLUSTER SAVECONFIG requested - configuration is managed by Raft");
        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle CLUSTER SET-CONFIG-EPOCH command
    async fn handle_cluster_set_config_epoch(
        &self,
        command: &RedisCommand,
    ) -> RaftResult<RespData> {
        if command.args.len() != 1 {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster set-config-epoch' command".into(),
            ));
        }

        // In Raft, the term serves as the configuration epoch
        Ok(RespData::Error("ERR CLUSTER SET-CONFIG-EPOCH not applicable in Raft mode - epoch is managed by Raft term".into()))
    }

    /// Generate a node ID based on host and port
    async fn generate_node_id(&self, host: &str, port: u16) -> NodeId {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        format!("{}:{}", host, port).hash(&mut hasher);
        hasher.finish()
    }

    /// Remove a node from the topology
    async fn remove_node_from_topology(&self, node_id: NodeId) -> RaftResult<()> {
        let mut topology = self.topology.write().await;
        topology.nodes.remove(&node_id);
        topology.last_update = std::time::Instant::now();
        Ok(())
    }

    /// Handle CLUSTER MYID command
    async fn handle_cluster_myid(&self) -> RaftResult<RespData> {
        let my_id = self.raft_node.node_id();
        Ok(RespData::BulkString(Some(Bytes::from(my_id.to_string()))))
    }

    /// Handle CLUSTER COUNT-FAILURE-REPORTS command
    async fn handle_cluster_count_failure_reports(
        &self,
        command: &RedisCommand,
    ) -> RaftResult<RespData> {
        if command.args.len() != 2 {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster count-failure-reports' command".into(),
            ));
        }

        let node_id_str = String::from_utf8_lossy(&command.args[1]);
        let _node_id: NodeId = node_id_str
            .parse()
            .map_err(|_| RaftError::configuration("Invalid node ID"))?;

        // In Raft mode, failure detection is handled by the consensus algorithm
        // Return 0 as we don't track failure reports the same way
        Ok(RespData::Integer(0))
    }

    /// Handle CLUSTER KEYSLOT command
    async fn handle_cluster_keyslot(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.len() != 2 {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster keyslot' command".into(),
            ));
        }

        let key = &command.args[1];

        // Calculate Redis cluster slot for the key
        let slot = self.calculate_redis_slot(key);
        Ok(RespData::Integer(slot as i64))
    }

    /// Handle CLUSTER COUNTKEYSINSLOT command
    async fn handle_cluster_countkeysinslot(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.len() != 2 {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster countkeysinslot' command".into(),
            ));
        }

        let slot_str = String::from_utf8_lossy(&command.args[1]);
        let slot: u16 = slot_str
            .parse()
            .map_err(|_| RaftError::configuration("Invalid slot number"))?;

        if slot > 16383 {
            return Ok(RespData::Error("ERR slot out of range".into()));
        }

        // In our implementation, we would need to query the state machine
        // For now, return 0 as a placeholder
        // TODO: Integrate with state machine to count keys in slot
        Ok(RespData::Integer(0))
    }

    /// Handle CLUSTER GETKEYSINSLOT command
    async fn handle_cluster_getkeysinslot(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.len() != 3 {
            return Ok(RespData::Error(
                "ERR wrong number of arguments for 'cluster getkeysinslot' command".into(),
            ));
        }

        let slot_str = String::from_utf8_lossy(&command.args[1]);
        let slot: u16 = slot_str
            .parse()
            .map_err(|_| RaftError::configuration("Invalid slot number"))?;

        let count_str = String::from_utf8_lossy(&command.args[2]);
        let count: usize = count_str
            .parse()
            .map_err(|_| RaftError::configuration("Invalid count"))?;

        if slot > 16383 {
            return Ok(RespData::Error("ERR slot out of range".into()));
        }

        if count > 1000 {
            return Ok(RespData::Error("ERR count too large".into()));
        }

        // In our implementation, we would need to query the state machine
        // For now, return empty array as a placeholder
        // TODO: Integrate with state machine to get keys in slot
        Ok(RespData::Array(Some(vec![])))
    }

    /// Calculate Redis cluster slot for a key (CRC16 mod 16384)
    fn calculate_redis_slot(&self, key: &[u8]) -> u16 {
        // Extract hash tag if present (content between first { and first })
        let hash_key = if let Some(start) = key.iter().position(|&b| b == b'{') {
            if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
                if end > 0 {
                    &key[start + 1..start + 1 + end]
                } else {
                    key
                }
            } else {
                key
            }
        } else {
            key
        };

        // Calculate CRC16 using the same algorithm as Redis
        let mut crc: u16 = 0;
        for &byte in hash_key {
            crc = ((crc << 8) ^ Self::crc16_table()[((crc >> 8) ^ byte as u16) as usize]) & 0xFFFF;
        }
        crc % 16384
    }

    /// CRC16 lookup table for Redis cluster slot calculation
    fn crc16_table() -> &'static [u16; 256] {
        static CRC16_TABLE: [u16; 256] = [
            0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a,
            0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294,
            0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462,
            0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509,
            0xe5ee, 0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695,
            0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5,
            0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948,
            0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
            0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87, 0x4ce4,
            0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b,
            0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f,
            0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
            0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046,
            0x6067, 0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290,
            0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e,
            0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
            0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691,
            0x16b0, 0x6657, 0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9,
            0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d,
            0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16,
            0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8,
            0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1, 0xef1f, 0xff3e,
            0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93,
            0x3eb2, 0x0ed1, 0x1ef0,
        ];
        &CRC16_TABLE
    }

    /// Handle READONLY command
    async fn handle_readonly_command(&self, client: &Client) -> RaftResult<RespData> {
        // In Redis cluster, READONLY allows reading from replicas
        // In our Raft implementation, this sets eventual consistency for reads
        self.set_client_consistency(client, ConsistencyLevel::Eventual)
            .await?;

        log::debug!("Client set to READONLY mode (eventual consistency)");
        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle READWRITE command
    async fn handle_readwrite_command(&self, client: &Client) -> RaftResult<RespData> {
        // READWRITE disables replica reads, requiring linearizable consistency
        self.set_client_consistency(client, ConsistencyLevel::Linearizable)
            .await?;

        log::debug!("Client set to READWRITE mode (linearizable consistency)");
        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle leader redirection transparently for clients
    async fn handle_leader_redirection(
        &self,
        client: &Client,
        _command: RedisCommand,
        leader_id: Option<NodeId>,
    ) -> RaftResult<RespData> {
        let client_info = self.get_client_info(client).await;

        if client_info.supports_cluster_redirects {
            // Client supports cluster redirects, send MOVED response
            match leader_id {
                Some(leader) => {
                    let topology = self.topology.read().await;
                    if let Some(leader_endpoint) = topology.nodes.get(&leader) {
                        Ok(RespData::Error(
                            format!("MOVED 0 {}:{}", leader_endpoint.host, leader_endpoint.port)
                                .into(),
                        ))
                    } else {
                        Ok(RespData::Error("CLUSTERDOWN The cluster is down".into()))
                    }
                }
                None => Ok(RespData::Error("CLUSTERDOWN The cluster is down".into())),
            }
        } else {
            // Client doesn't support redirects, try to proxy the command
            // For now, return an error asking client to reconnect
            Ok(RespData::Error(
                "ERR please reconnect to the current leader".into(),
            ))
        }
    }

    /// Convert Raft errors to Redis-compatible error responses
    pub fn format_redis_error(&self, error: &RaftError) -> RespData {
        match error {
            RaftError::NotLeader { leader_id } => {
                match leader_id {
                    Some(leader) => {
                        // Try to get leader endpoint for MOVED response
                        RespData::Error(format!("MOVED 0 leader-{}", leader).into())
                    }
                    None => RespData::Error("CLUSTERDOWN The cluster is down".into()),
                }
            }
            RaftError::Timeout { operation: _ } => RespData::Error("ERR timeout".into()),
            RaftError::Configuration { message } => {
                // Map configuration errors to appropriate Redis error types
                if message.contains("wrong number of arguments") {
                    RespData::Error(message.clone().into())
                } else if message.contains("invalid") || message.contains("Invalid") {
                    RespData::Error(format!("ERR {}", message).into())
                } else {
                    RespData::Error(format!("ERR {}", message).into())
                }
            }
            RaftError::Storage(err) => {
                // Map storage errors to Redis-compatible errors
                let msg = err.to_string();
                if msg.contains("not found") || msg.contains("key not found") {
                    RespData::BulkString(None) // Redis returns nil for missing keys
                } else if msg.contains("out of memory") {
                    RespData::Error("OOM command not allowed when used memory > 'maxmemory'".into())
                } else {
                    RespData::Error(format!("ERR {}", msg).into())
                }
            }
            RaftError::Network(err) => {
                // Map network errors to cluster-related errors
                let msg = err.to_string();
                if msg.contains("connection") || msg.contains("timeout") {
                    RespData::Error("CLUSTERDOWN The cluster is down".into())
                } else {
                    RespData::Error(format!("ERR {}", msg).into())
                }
            }
            RaftError::Serialization(msg) => {
                RespData::Error(format!("ERR protocol error: {}", msg).into())
            }
            _ => RespData::Error(format!("ERR {}", error).into()),
        }
    }

    /// Format validation errors in Redis-compatible format
    pub fn format_validation_error(&self, command: &str, message: &str) -> RespData {
        if message.contains("wrong number of arguments") {
            RespData::Error(
                format!("ERR wrong number of arguments for '{}' command", command).into(),
            )
        } else if message.contains("invalid") {
            RespData::Error(format!("ERR invalid argument for '{}' command", command).into())
        } else {
            RespData::Error(format!("ERR {}", message).into())
        }
    }

    /// Format cluster-specific errors
    pub fn format_cluster_error(
        &self,
        error_type: ClusterErrorType,
        message: Option<&str>,
    ) -> RespData {
        match error_type {
            ClusterErrorType::ClusterDown => {
                RespData::Error("CLUSTERDOWN The cluster is down".into())
            }
            ClusterErrorType::NoLeader => RespData::Error("CLUSTERDOWN No leader available".into()),
            ClusterErrorType::NotLeader { leader_id } => match leader_id {
                Some(leader) => RespData::Error(format!("MOVED 0 leader-{}", leader).into()),
                None => RespData::Error("CLUSTERDOWN The cluster is down".into()),
            },
            ClusterErrorType::NodeNotFound => RespData::Error("ERR unknown node".into()),
            ClusterErrorType::InvalidSlot => RespData::Error("ERR slot out of range".into()),
            ClusterErrorType::ConfigurationError => {
                let msg = message.unwrap_or("configuration error");
                RespData::Error(format!("ERR {}", msg).into())
            }
            ClusterErrorType::NetworkPartition => {
                RespData::Error("CLUSTERDOWN Network partition detected".into())
            }
        }
    }

    /// Handle Redis protocol-specific error formatting
    pub fn format_protocol_error(&self, error: ProtocolError) -> RespData {
        match error {
            ProtocolError::InvalidCommand => RespData::Error("ERR unknown command".into()),
            ProtocolError::WrongType => RespData::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            ProtocolError::OutOfRange => RespData::Error("ERR index out of range".into()),
            ProtocolError::SyntaxError => RespData::Error("ERR syntax error".into()),
            ProtocolError::NoAuth => RespData::Error("NOAUTH Authentication required".into()),
            ProtocolError::Loading => {
                RespData::Error("LOADING Redis is loading the dataset in memory".into())
            }
            ProtocolError::Busy => RespData::Error("BUSY Redis is busy running a script".into()),
            ProtocolError::ReadOnly => {
                RespData::Error("READONLY You can't write against a read only replica".into())
            }
            ProtocolError::NoScript => {
                RespData::Error("NOSCRIPT No matching script. Please use EVAL".into())
            }
            ProtocolError::MasterDown => RespData::Error(
                "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'"
                    .into(),
            ),
        }
    }

    /// Check if a command is supported in cluster mode
    pub fn is_command_supported(&self, command: &str) -> bool {
        let cmd_name = command.to_lowercase();

        // Commands that are not supported in cluster mode
        let unsupported_commands = [
            "flushall", // Would affect all nodes inconsistently
            "flushdb",  // Would affect all nodes inconsistently
            "debug",    // Debug commands are node-specific
            "config",   // Configuration changes should go through Raft
            "shutdown", // Node shutdown should be coordinated
            "save",     // Snapshots are handled by Raft
            "bgsave",   // Snapshots are handled by Raft
            "lastsave", // Not applicable in Raft mode
            "monitor",  // Monitoring is node-specific
            "sync",     // Replication is handled by Raft
            "psync",    // Replication is handled by Raft
        ];

        !unsupported_commands.contains(&cmd_name.as_str())
    }

    /// Get appropriate error message for unsupported commands
    pub fn get_unsupported_command_error(&self, command: &str) -> RespData {
        let cmd_name = command.to_lowercase();

        match cmd_name.as_str() {
            "flushall" | "flushdb" => {
                RespData::Error("ERR FLUSHALL/FLUSHDB not supported in cluster mode".into())
            }
            "config" => RespData::Error("ERR CONFIG commands not supported in cluster mode".into()),
            "shutdown" => RespData::Error("ERR SHUTDOWN not supported in cluster mode".into()),
            "save" | "bgsave" | "lastsave" => RespData::Error(
                "ERR Manual save commands not supported in cluster mode, snapshots are automatic"
                    .into(),
            ),
            "monitor" => RespData::Error("ERR MONITOR not supported in cluster mode".into()),
            "sync" | "psync" => RespData::Error(
                "ERR Replication commands not supported in cluster mode, use Raft consensus".into(),
            ),
            "debug" => RespData::Error("ERR DEBUG commands not supported in cluster mode".into()),
            _ => RespData::Error(
                format!("ERR command '{}' not supported in cluster mode", command).into(),
            ),
        }
    }

    /// Update client activity tracking
    async fn update_client_activity(&self, client: &Client) -> RaftResult<()> {
        let client_id = format!("{:p}", client as *const Client);
        let mut connections = self.client_connections.write().await;

        // Check if this is a new client or update existing
        if let Some(existing) = connections.get_mut(&client_id) {
            existing.last_activity = std::time::Instant::now();
        } else {
            // New client - detect capabilities
            let supports_redirects = self.detect_client_cluster_support(client).await;

            connections.insert(
                client_id.clone(),
                ClientConnectionInfo {
                    client_id: client_id.clone(),
                    last_activity: std::time::Instant::now(),
                    preferred_consistency: None,
                    supports_cluster_redirects: supports_redirects,
                },
            );
        }

        Ok(())
    }

    /// Detect if client supports cluster redirects
    async fn detect_client_cluster_support(&self, _client: &Client) -> bool {
        // In a full implementation, this would:
        // 1. Check client connection info/user agent
        // 2. Look for previous CLUSTER commands from this client
        // 3. Check if client has handled MOVED responses correctly

        // For now, assume all clients support redirects
        // This can be made configurable or more sophisticated
        true
    }

    /// Set client consistency preference
    pub async fn set_client_consistency(
        &self,
        client: &Client,
        consistency: ConsistencyLevel,
    ) -> RaftResult<()> {
        let client_id = format!("{:p}", client as *const Client);
        let mut connections = self.client_connections.write().await;

        if let Some(client_info) = connections.get_mut(&client_id) {
            client_info.preferred_consistency = Some(consistency);
            log::debug!(
                "Set client {} consistency preference to {:?}",
                client_id,
                consistency
            );
        }

        Ok(())
    }

    /// Set client cluster redirect support
    pub async fn set_client_redirect_support(
        &self,
        client: &Client,
        supports_redirects: bool,
    ) -> RaftResult<()> {
        let client_id = format!("{:p}", client as *const Client);
        let mut connections = self.client_connections.write().await;

        if let Some(client_info) = connections.get_mut(&client_id) {
            client_info.supports_cluster_redirects = supports_redirects;
            log::debug!(
                "Set client {} redirect support to {}",
                client_id,
                supports_redirects
            );
        }

        Ok(())
    }

    /// Get client connection information
    async fn get_client_info(&self, client: &Client) -> ClientConnectionInfo {
        let client_id = format!("{:p}", client as *const Client);
        let connections = self.client_connections.read().await;

        connections
            .get(&client_id)
            .cloned()
            .unwrap_or_else(|| ClientConnectionInfo {
                client_id,
                last_activity: std::time::Instant::now(),
                preferred_consistency: None,
                supports_cluster_redirects: true,
            })
    }

    /// Check and handle topology changes
    async fn check_and_handle_topology_changes(&self, _client: &Client) -> RaftResult<()> {
        // Update topology information from Raft node
        let health = self.raft_node.get_cluster_health().await?;
        let membership = self.raft_node.get_membership().await?;
        let leader_id = self.raft_node.get_leader_id().await;

        let mut topology = self.topology.write().await;

        // Update leader information
        topology.leader_id = leader_id;

        // Update cluster state
        topology.state = if health.is_healthy {
            ClusterState::Ok
        } else if health.healthy_members > 0 {
            ClusterState::Degraded
        } else {
            ClusterState::Fail
        };

        // Update node information
        // TODO: Add public endpoint accessor to RaftNode and wire real endpoints
        // RaftNode stores endpoints internally but doesn't expose them publicly.
        // Need to add: pub async fn get_endpoint(&self, node_id: NodeId) -> Option<String>
        for &node_id in &membership {
            topology.nodes.entry(node_id).or_insert_with(|| {
                // TODO: Replace with actual endpoint from RaftNode.get_endpoint(node_id)
                NodeEndpoint {
                    node_id,
                    host: "127.0.0.1".to_string(), // Placeholder - should be from RaftNode
                    port: 7379,                    // Placeholder - should be from RaftNode
                    is_reachable: true,            // TODO: Should check actual reachability
                }
            });
        }

        topology.last_update = std::time::Instant::now();

        Ok(())
    }

    /// Update cluster topology with actual node endpoints
    pub async fn update_node_endpoint(
        &self,
        node_id: NodeId,
        host: String,
        port: u16,
    ) -> RaftResult<()> {
        let mut topology = self.topology.write().await;

        topology.nodes.insert(
            node_id,
            NodeEndpoint {
                node_id,
                host,
                port,
                is_reachable: true,
            },
        );

        topology.last_update = std::time::Instant::now();

        Ok(())
    }

    /// Mark a node as unreachable
    pub async fn mark_node_unreachable(&self, node_id: NodeId) -> RaftResult<()> {
        let mut topology = self.topology.write().await;

        if let Some(endpoint) = topology.nodes.get_mut(&node_id) {
            endpoint.is_reachable = false;
        }

        Ok(())
    }

    /// Clean up inactive client connections
    pub async fn cleanup_inactive_clients(&self, max_idle: Duration) -> RaftResult<()> {
        let mut connections = self.client_connections.write().await;
        let now = std::time::Instant::now();

        connections.retain(|_, info| now.duration_since(info.last_activity) < max_idle);

        Ok(())
    }

    /// Get current cluster topology
    pub async fn get_topology(&self) -> ClusterTopology {
        self.topology.read().await.clone()
    }

    /// Check if the cluster is in a healthy state for client operations
    pub async fn is_cluster_healthy(&self) -> RaftResult<bool> {
        let topology = self.topology.read().await;
        Ok(topology.state == ClusterState::Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_state_transitions() {
        // Test cluster state logic
        assert_eq!(ClusterState::Ok, ClusterState::Ok);
        assert_ne!(ClusterState::Ok, ClusterState::Fail);
    }

    #[test]
    fn test_node_endpoint_creation() {
        let endpoint = NodeEndpoint {
            node_id: 1,
            host: "localhost".to_string(),
            port: 7379,
            is_reachable: true,
        };

        assert_eq!(endpoint.node_id, 1);
        assert_eq!(endpoint.host, "localhost");
        assert_eq!(endpoint.port, 7379);
        assert!(endpoint.is_reachable);
    }
}
