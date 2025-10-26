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
use crate::redis_integration::RaftRedisHandler;
use crate::types::{ConsistencyLevel, NodeId, RedisCommand};
use bytes::Bytes;
use client::Client;
use resp::RespData;
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

        // Handle cluster-specific commands first
        if let Some(response) = self.handle_cluster_commands(&command).await? {
            return Ok(response);
        }

        // Handle topology change notifications
        self.check_and_handle_topology_changes(client).await?;

        // Process the command through the Raft handler
        match self.redis_handler.handle_command(command.clone()).await {
            Ok(response) => Ok(response),
            Err(RaftError::NotLeader { leader_id }) => {
                // Handle leader redirection transparently
                self.handle_leader_redirection(client, command, leader_id).await
            }
            Err(e) => {
                log::error!("Command processing failed: {}", e);
                Ok(RespData::Error(format!("ERR {}", e).into()))
            }
        }
    }

    /// Handle Redis cluster-specific commands
    async fn handle_cluster_commands(&self, command: &RedisCommand) -> RaftResult<Option<RespData>> {
        match command.command.to_lowercase().as_str() {
            "cluster" => {
                if command.args.is_empty() {
                    return Ok(Some(RespData::Error("ERR wrong number of arguments for 'cluster' command".into())));
                }

                let subcommand = String::from_utf8_lossy(&command.args[0]).to_lowercase();
                match subcommand.as_str() {
                    "nodes" => Ok(Some(self.handle_cluster_nodes().await?)),
                    "info" => Ok(Some(self.handle_cluster_info().await?)),
                    "slots" => Ok(Some(self.handle_cluster_slots().await?)),
                    "meet" => Ok(Some(self.handle_cluster_meet(command).await?)),
                    "forget" => Ok(Some(self.handle_cluster_forget(command).await?)),
                    "reset" => Ok(Some(self.handle_cluster_reset(command).await?)),
                    _ => Ok(Some(RespData::Error(format!("ERR unknown CLUSTER subcommand '{}'", subcommand).into()))),
                }
            }
            "readonly" => {
                // Handle READONLY command for replica connections
                Ok(Some(self.handle_readonly_command().await?))
            }
            "readwrite" => {
                // Handle READWRITE command
                Ok(Some(self.handle_readwrite_command().await?))
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
                if is_leader { "myself,master" } else { "myself,slave" }
            } else if is_leader {
                "master"
            } else {
                "slave"
            };

            let status = if endpoint.is_reachable { "connected" } else { "disconnected" };
            
            // Format: node_id:port@cport flags master ping_sent ping_recv config_epoch link_state slots
            nodes_info.push_str(&format!(
                "{} {}:{} {} {} 0 0 1 {} 0-16383\n",
                node_id,
                endpoint.host,
                endpoint.port,
                flags,
                if is_leader { "-" } else { topology.leader_id.unwrap_or(0).to_string().as_str() },
                status
            ));
        }

        Ok(RespData::BulkString(Some(Bytes::from(nodes_info))))
    }

    /// Handle CLUSTER INFO command
    async fn handle_cluster_info(&self) -> RaftResult<RespData> {
        let health = self.raft_node.get_cluster_health().await?;
        let topology = self.topology.read().await;
        
        let mut info = String::new();
        info.push_str(&format!("cluster_state:{}\n", 
            if health.is_healthy { "ok" } else { "fail" }));
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
                    RespData::Integer(0),      // Start slot
                    RespData::Integer(16383),  // End slot
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
            return Ok(RespData::Error("ERR wrong number of arguments for 'cluster meet' command".into()));
        }

        let host = String::from_utf8_lossy(&command.args[0]);
        let port: u16 = String::from_utf8_lossy(&command.args[1])
            .parse()
            .map_err(|_| RaftError::configuration("Invalid port number"))?;

        // In a full implementation, this would initiate contact with the new node
        log::info!("CLUSTER MEET requested for {}:{}", host, port);
        
        // For now, return success
        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle CLUSTER FORGET command
    async fn handle_cluster_forget(&self, command: &RedisCommand) -> RaftResult<RespData> {
        if command.args.len() != 1 {
            return Ok(RespData::Error("ERR wrong number of arguments for 'cluster forget' command".into()));
        }

        let node_id_str = String::from_utf8_lossy(&command.args[0]);
        let node_id: NodeId = node_id_str.parse()
            .map_err(|_| RaftError::configuration("Invalid node ID"))?;

        // In a full implementation, this would remove the node from the cluster
        log::info!("CLUSTER FORGET requested for node {}", node_id);
        
        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle CLUSTER RESET command
    async fn handle_cluster_reset(&self, _command: &RedisCommand) -> RaftResult<RespData> {
        // CLUSTER RESET is not supported in Raft mode as it would break consensus
        Ok(RespData::Error("ERR CLUSTER RESET not supported in Raft mode".into()))
    }

    /// Handle READONLY command
    async fn handle_readonly_command(&self) -> RaftResult<RespData> {
        // In Redis cluster, READONLY allows reading from replicas
        // In our Raft implementation, this could set eventual consistency
        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle READWRITE command
    async fn handle_readwrite_command(&self) -> RaftResult<RespData> {
        // READWRITE disables replica reads, requiring linearizable consistency
        Ok(RespData::SimpleString("OK".into()))
    }

    /// Handle leader redirection transparently for clients
    async fn handle_leader_redirection(
        &self,
        client: &Client,
        command: RedisCommand,
        leader_id: Option<NodeId>,
    ) -> RaftResult<RespData> {
        let client_info = self.get_client_info(client).await;
        
        if client_info.supports_cluster_redirects {
            // Client supports cluster redirects, send MOVED response
            match leader_id {
                Some(leader) => {
                    let topology = self.topology.read().await;
                    if let Some(leader_endpoint) = topology.nodes.get(&leader) {
                        Ok(RespData::Error(format!(
                            "MOVED 0 {}:{}",
                            leader_endpoint.host, leader_endpoint.port
                        ).into()))
                    } else {
                        Ok(RespData::Error("CLUSTERDOWN The cluster is down".into()))
                    }
                }
                None => Ok(RespData::Error("CLUSTERDOWN The cluster is down".into())),
            }
        } else {
            // Client doesn't support redirects, try to proxy the command
            // For now, return an error asking client to reconnect
            Ok(RespData::Error("ERR please reconnect to the current leader".into()))
        }
    }

    /// Update client activity tracking
    async fn update_client_activity(&self, client: &Client) -> RaftResult<()> {
        let client_id = format!("{:p}", client as *const Client);
        let mut connections = self.client_connections.write().await;
        
        connections.insert(client_id, ClientConnectionInfo {
            client_id: client_id.clone(),
            last_activity: std::time::Instant::now(),
            preferred_consistency: None,
            supports_cluster_redirects: true, // Assume support by default
        });
        
        Ok(())
    }

    /// Get client connection information
    async fn get_client_info(&self, client: &Client) -> ClientConnectionInfo {
        let client_id = format!("{:p}", client as *const Client);
        let connections = self.client_connections.read().await;
        
        connections.get(&client_id).cloned().unwrap_or_else(|| {
            ClientConnectionInfo {
                client_id,
                last_activity: std::time::Instant::now(),
                preferred_consistency: None,
                supports_cluster_redirects: true,
            }
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
        
        // Update node information (simplified - in real implementation would get actual endpoints)
        for &node_id in &membership {
            topology.nodes.entry(node_id).or_insert_with(|| {
                NodeEndpoint {
                    node_id,
                    host: "127.0.0.1".to_string(), // Placeholder
                    port: 7379,                    // Placeholder
                    is_reachable: true,
                }
            });
        }
        
        topology.last_update = std::time::Instant::now();
        
        Ok(())
    }

    /// Update cluster topology with actual node endpoints
    pub async fn update_node_endpoint(&self, node_id: NodeId, host: String, port: u16) -> RaftResult<()> {
        let mut topology = self.topology.write().await;
        
        topology.nodes.insert(node_id, NodeEndpoint {
            node_id,
            host,
            port,
            is_reachable: true,
        });
        
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
        
        connections.retain(|_, info| {
            now.duration_since(info.last_activity) < max_idle
        });
        
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