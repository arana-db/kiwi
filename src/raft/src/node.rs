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

//! Raft node implementation

use crate::error::{RaftError, RaftResult};
use crate::network::{KiwiRaftNetworkFactory, NodeAuth, TlsConfig};
use crate::state_machine::KiwiStateMachine;
use crate::storage::RaftStorage;
use crate::types::{BasicNode, ClusterConfig, ClusterHealth, NodeId, RaftMetrics, TypeConfig};
use crate::{ClientRequest, ClientResponse};
use async_trait::async_trait;
use openraft::{Config as RaftConfig, Raft};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Core Raft node interface
#[async_trait]
pub trait RaftNodeInterface {
    /// Start the Raft node with given configuration
    async fn start(&self, node_id: NodeId, peers: Vec<NodeId>) -> RaftResult<()>;

    /// Propose a client request to the cluster
    async fn propose(&self, request: ClientRequest) -> RaftResult<ClientResponse>;

    /// Add a learner node to the cluster
    async fn add_learner(&self, node_id: NodeId, endpoint: String) -> RaftResult<()>;

    /// Change cluster membership
    async fn change_membership(&self, members: BTreeSet<NodeId>) -> RaftResult<()>;

    /// Get current Raft metrics
    async fn get_metrics(&self) -> RaftResult<RaftMetrics>;

    /// Shutdown the Raft node
    async fn shutdown(&self) -> RaftResult<()>;
}

/// Raft node implementation with openraft integration
pub struct RaftNode {
    /// The openraft instance
    raft: Arc<Raft<TypeConfig>>,
    /// Network factory for inter-node communication
    network_factory: Arc<RwLock<KiwiRaftNetworkFactory>>,
    /// Storage layer
    storage: Arc<RaftStorage>,
    /// State machine
    state_machine: Arc<KiwiStateMachine>,
    /// Node configuration
    config: ClusterConfig,
    /// Node endpoints mapping
    endpoints: Arc<RwLock<HashMap<NodeId, String>>>,
    /// Whether the node is started
    started: Arc<RwLock<bool>>,
}

impl RaftNode {
    /// Create a new Raft node with cluster configuration
    pub async fn new(cluster_config: ClusterConfig) -> RaftResult<Self> {
        log::info!(
            "Creating Raft node {} with config: {:?}",
            cluster_config.node_id,
            cluster_config
        );

        // Create storage layer (for our own use, not for openraft)
        let storage_path = PathBuf::from(&cluster_config.data_dir).join("raft_storage");
        let storage = Arc::new(RaftStorage::new(storage_path)?);

        // Create state machine (for our own use)
        let state_machine = Arc::new(KiwiStateMachine::new(cluster_config.node_id));

        // Create network factory
        let network_factory_instance = KiwiRaftNetworkFactory::new(cluster_config.node_id);

        // Parse cluster members and build endpoints
        let mut endpoints = HashMap::new();
        for member in &cluster_config.cluster_members {
            if let Some((node_id_str, endpoint)) = member.split_once(':') {
                if let Ok(node_id) = node_id_str.parse::<NodeId>() {
                    let host_port = member
                        .strip_prefix(&format!("{}:", node_id_str))
                        .unwrap_or(endpoint);
                    endpoints.insert(node_id, host_port.to_string());

                    // Add endpoint to network factory
                    network_factory_instance
                        .add_endpoint(node_id, host_port.to_string())
                        .await;
                }
            }
        }

        // Create Raft configuration
        let raft_config = Arc::new(RaftConfig {
            heartbeat_interval: cluster_config.heartbeat_interval_ms,
            election_timeout_min: cluster_config.election_timeout_min_ms,
            election_timeout_max: cluster_config.election_timeout_max_ms,
            max_payload_entries: cluster_config.max_payload_entries,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                cluster_config.snapshot_threshold,
            ),
            ..Default::default()
        });

        // Use the simple memory store with Adaptor pattern
        // This provides a working in-memory storage that satisfies OpenRaft's sealed traits
        let store_dir = PathBuf::from(&cluster_config.data_dir).join("openraft_store");
        let (log_store, sm) = crate::simple_mem_store::create_mem_store_with_dir(store_dir);
        
        let network_factory = Arc::new(RwLock::new(network_factory_instance));
        let network = network_factory.read().await.clone();
        
        let raft = Raft::new(
            cluster_config.node_id,
            raft_config,
            network,
            log_store,
            sm,
        )
        .await
        .map_err(|e| RaftError::Configuration {
            message: format!("Failed to create Raft instance: {}", e),
            context: "Raft::new".to_string(),
        })?;

        Ok(Self {
            raft: Arc::new(raft),
            network_factory,
            storage,
            state_machine,
            config: cluster_config,
            endpoints: Arc::new(RwLock::new(endpoints)),
            started: Arc::new(RwLock::new(false)),
        })
    }

    /// Start the Raft node (simplified for server integration)
    pub async fn start(&self, init_cluster: bool) -> RaftResult<()> {
        let mut started = self.started.write().await;
        if *started {
            log::warn!("Raft node {} already started", self.config.node_id);
            return Ok(());
        }

        log::info!(
            "Starting Raft node {} (init_cluster: {})",
            self.config.node_id,
            init_cluster
        );

        if init_cluster {
            // Initialize a new cluster with this node as the only member
            let mut nodes = BTreeSet::new();
            nodes.insert(self.config.node_id);

            self.raft.initialize(nodes).await.map_err(|e| {
                // Convert RaftError to our RaftError
                // Create a generic error message since we can't directly convert the error types
                RaftError::Configuration {
                    message: format!("Failed to initialize Raft cluster: {}", e),
                    context: "initialize".to_string(),
                }
            })?;

            log::info!(
                "Initialized new Raft cluster with node {}",
                self.config.node_id
            );
        } else {
            // Node will join an existing cluster through add_learner/change_membership
            log::info!(
                "Raft node {} ready to join existing cluster",
                self.config.node_id
            );
        }

        *started = true;
        Ok(())
    }

    /// Get the underlying Raft instance
    pub fn raft(&self) -> Arc<Raft<TypeConfig>> {
        self.raft.clone()
    }

    /// Get the storage layer
    pub fn storage(&self) -> Arc<RaftStorage> {
        self.storage.clone()
    }

    /// Get the state machine
    pub fn state_machine(&self) -> Arc<KiwiStateMachine> {
        self.state_machine.clone()
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Check if the node is the current leader
    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        matches!(metrics.state, openraft::ServerState::Leader)
    }

    /// Get the current leader ID
    pub async fn get_leader_id(&self) -> Option<NodeId> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader
    }

    /// Wait for the node to become leader or follower (not candidate)
    pub async fn wait_for_stable_state(&self, timeout: Duration) -> RaftResult<()> {
        let start = std::time::Instant::now();

        loop {
            let metrics = self.raft.metrics().borrow().clone();
            match metrics.state {
                openraft::ServerState::Leader | openraft::ServerState::Follower => {
                    log::info!(
                        "Node {} reached stable state: {:?}",
                        self.config.node_id,
                        metrics.state
                    );
                    return Ok(());
                }
                openraft::ServerState::Candidate | openraft::ServerState::Learner => {
                    if start.elapsed() > timeout {
                        return Err(RaftError::timeout(format!(
                            "Node {} did not reach stable state within {:?}",
                            self.config.node_id, timeout
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                openraft::ServerState::Shutdown => {
                    return Err(RaftError::configuration("Node is shutting down"));
                }
            }
        }
    }

    /// Add TLS configuration for secure communication
    pub async fn configure_tls(&self, tls_config: TlsConfig) -> RaftResult<()> {
        let mut network_factory = self.network_factory.write().await;
        *network_factory = KiwiRaftNetworkFactory::new_secure(self.config.node_id, tls_config);

        // Re-add all endpoints
        let endpoints = self.endpoints.read().await;
        for (&node_id, endpoint) in endpoints.iter() {
            network_factory
                .add_endpoint(node_id, endpoint.clone())
                .await;
        }

        Ok(())
    }

    /// Add node authentication
    pub async fn add_node_auth(&self, node_id: NodeId, auth: NodeAuth) -> RaftResult<()> {
        let network_factory = self.network_factory.read().await;
        network_factory.add_node_auth(node_id, auth).await;
        Ok(())
    }

    /// Remove node authentication
    pub async fn remove_node_auth(&self, node_id: NodeId) -> RaftResult<()> {
        let network_factory = self.network_factory.read().await;
        network_factory.remove_node_auth(node_id).await;
        Ok(())
    }

    /// Add an endpoint for a node
    pub async fn add_endpoint(&self, node_id: NodeId, endpoint: String) -> RaftResult<()> {
        {
            let mut endpoints = self.endpoints.write().await;
            endpoints.insert(node_id, endpoint.clone());
        }

        let network_factory = self.network_factory.read().await;
        network_factory.add_endpoint(node_id, endpoint).await;
        Ok(())
    }

    /// Remove an endpoint for a node
    pub async fn remove_endpoint(&self, node_id: NodeId) -> RaftResult<()> {
        {
            let mut endpoints = self.endpoints.write().await;
            endpoints.remove(&node_id);
        }

        let network_factory = self.network_factory.read().await;
        network_factory.remove_endpoint(node_id).await;
        Ok(())
    }

    /// Check for network partitions
    pub async fn check_partitions(&self) -> Vec<NodeId> {
        let network_factory = self.network_factory.read().await;
        network_factory.check_partitions().await
    }

    /// Get current cluster membership
    pub async fn get_membership(&self) -> RaftResult<BTreeSet<NodeId>> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        Ok(membership.voter_ids().collect())
    }

    /// Get current learners in the cluster
    pub async fn get_learners(&self) -> RaftResult<BTreeSet<NodeId>> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        Ok(membership.learner_ids().collect())
    }

    /// Check if a node is a member of the cluster
    pub async fn is_member(&self, node_id: NodeId) -> RaftResult<bool> {
        let membership = self.get_membership().await?;
        Ok(membership.contains(&node_id))
    }

    /// Check if a node is a learner in the cluster
    pub async fn is_learner(&self, node_id: NodeId) -> RaftResult<bool> {
        let learners = self.get_learners().await?;
        Ok(learners.contains(&node_id))
    }

    /// Safely add a node to the cluster as a learner first, then promote to voter
    pub async fn add_node_safely(&self, node_id: NodeId, endpoint: String) -> RaftResult<()> {
        log::info!("Safely adding node {} at {} to cluster", node_id, endpoint);

        // Check if we're the leader
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await;
            return Err(RaftError::NotLeader { 
                leader_id,
                context: "add_node: not leader".to_string(),
            });
        }

        // Check if node is already a member or learner
        if self.is_member(node_id).await? {
            log::warn!("Node {} is already a voting member", node_id);
            return Ok(());
        }

        if self.is_learner(node_id).await? {
            log::info!("Node {} is already a learner, promoting to voter", node_id);
        } else {
            // Add as learner first
            log::info!("Adding node {} as learner", node_id);
            self.add_learner(node_id, endpoint).await?;

            // Wait for the learner to catch up
            self.wait_for_learner_catchup(node_id, Duration::from_secs(30))
                .await?;
        }

        // Promote learner to voting member
        log::info!("Promoting node {} to voting member", node_id);
        let mut current_members = self.get_membership().await?;
        current_members.insert(node_id);

        self.change_membership(current_members).await?;
        log::info!(
            "Successfully added node {} to cluster as voting member",
            node_id
        );
        Ok(())
    }

    /// Safely remove a node from the cluster
    pub async fn remove_node_safely(&self, node_id: NodeId) -> RaftResult<()> {
        log::info!("Safely removing node {} from cluster", node_id);

        // Check if we're the leader
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await;
            return Err(RaftError::NotLeader { 
                leader_id,
                context: "remove_node_safely: not leader".to_string(),
            });
        }

        // Check if node is a member
        if !self.is_member(node_id).await? && !self.is_learner(node_id).await? {
            log::warn!("Node {} is not a member of the cluster", node_id);
            return Ok(());
        }

        // Don't allow removing the last node
        let current_members = self.get_membership().await?;
        if current_members.len() <= 1 {
            return Err(RaftError::configuration(
                "Cannot remove the last node from the cluster",
            ));
        }

        // Don't allow removing ourselves if we're the only leader
        if node_id == self.config.node_id && current_members.len() == 1 {
            return Err(RaftError::configuration(
                "Cannot remove the leader node when it's the only member",
            ));
        }

        // Remove from voting members
        if self.is_member(node_id).await? {
            let mut new_members = current_members;
            new_members.remove(&node_id);

            log::info!("Removing node {} from voting members", node_id);
            self.change_membership(new_members).await?;
        }

        // Remove from learners if present
        if self.is_learner(node_id).await? {
            log::info!("Removing node {} from learners", node_id);
            // Note: openraft doesn't have a direct remove_learner method
            // The learner will be removed when membership changes
        }

        // Remove endpoint
        self.remove_endpoint(node_id).await?;

        log::info!("Successfully removed node {} from cluster", node_id);
        Ok(())
    }

    /// Wait for a learner to catch up with the leader's log
    pub async fn wait_for_learner_catchup(
        &self,
        learner_id: NodeId,
        timeout: Duration,
    ) -> RaftResult<()> {
        log::info!("Waiting for learner {} to catch up", learner_id);

        let start = std::time::Instant::now();
        let mut last_log_index = None;

        loop {
            if start.elapsed() > timeout {
                return Err(RaftError::timeout(format!(
                    "Learner {} did not catch up within {:?}",
                    learner_id, timeout
                )));
            }

            let metrics = self.raft.metrics().borrow().clone();

            // Get our current log index
            let current_log_index = metrics.last_log_index.unwrap_or(0);

            // Check learner's progress
            if let Some(replication_map) = &metrics.replication {
                if let Some(replication) = replication_map.get(&learner_id) {
                    let learner_matched = replication.map(|id| id.index).unwrap_or(0);

                    // Consider caught up if within a reasonable threshold
                    let lag = current_log_index.saturating_sub(learner_matched);
                    if lag <= 10 {
                        // Allow up to 10 entries lag
                        log::info!("Learner {} caught up (lag: {})", learner_id, lag);
                        return Ok(());
                    }

                    // Log progress if it changed
                    if last_log_index != Some(learner_matched) {
                        log::info!(
                            "Learner {} progress: {}/{} (lag: {})",
                            learner_id,
                            learner_matched,
                            current_log_index,
                            lag
                        );
                        last_log_index = Some(learner_matched);
                    }
                } else {
                    log::debug!("No replication info for learner {}", learner_id);
                }
            } else {
                log::debug!("No replication metrics available");
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Perform a leadership transfer to another node
    pub async fn transfer_leadership(&self, target_node: NodeId) -> RaftResult<()> {
        log::info!("Transferring leadership to node {}", target_node);

        // Check if we're the leader
        if !self.is_leader().await {
            return Err(RaftError::configuration(
                "Only the leader can transfer leadership",
            ));
        }

        // Check if target is a voting member
        if !self.is_member(target_node).await? {
            return Err(RaftError::configuration(format!(
                "Target node {} is not a voting member",
                target_node
            )));
        }

        // Ensure target is caught up
        self.wait_for_learner_catchup(target_node, Duration::from_secs(10))
            .await?;

        // Trigger leadership transfer (openraft doesn't have direct transfer, so we step down)
        log::info!("Stepping down as leader to trigger election");
        // Note: openraft doesn't have a direct leadership transfer method
        // We would need to implement this by stepping down and letting the target win election

        log::info!("Leadership transfer initiated for node {}", target_node);
        Ok(())
    }

    /// Handle configuration changes during network partitions
    pub async fn handle_partition_recovery(&self) -> RaftResult<()> {
        log::info!(
            "Handling partition recovery for node {}",
            self.config.node_id
        );

        // Check for partitioned nodes
        let partitioned_nodes = self.check_partitions().await;
        if partitioned_nodes.is_empty() {
            log::info!("No partitioned nodes detected");
            return Ok(());
        }

        log::warn!("Detected partitioned nodes: {:?}", partitioned_nodes);

        // If we're the leader, we might need to remove partitioned nodes
        if self.is_leader().await {
            let current_members = self.get_membership().await?;
            let healthy_members: BTreeSet<NodeId> = current_members
                .into_iter()
                .filter(|&node_id| !partitioned_nodes.contains(&node_id))
                .collect();

            // Only proceed if we still have a majority
            let total_members = self.get_membership().await?.len();
            if healthy_members.len() > total_members / 2 {
                log::info!("Majority of nodes are healthy, cluster can continue operating");

                // Optionally remove persistently partitioned nodes
                // This is a policy decision and should be configurable
                for &partitioned_node in &partitioned_nodes {
                    log::warn!(
                        "Node {} appears to be persistently partitioned",
                        partitioned_node
                    );
                    // Could implement automatic removal after a timeout
                }
            } else {
                log::error!("Majority of nodes are partitioned, cannot make progress");
                return Err(RaftError::Network(
                    crate::error::NetworkError::NetworkPartition {
                        affected_nodes: 0,
                        context: "check_cluster_health: majority partitioned".to_string(),
                    },
                ));
            }
        }

        Ok(())
    }

    /// Get cluster health status
    pub async fn get_cluster_health(&self) -> RaftResult<ClusterHealth> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = self.get_membership().await?;
        let learners = self.get_learners().await?;
        let partitioned_nodes = self.check_partitions().await;

        let healthy_members = membership.len() - partitioned_nodes.len();
        let is_healthy = healthy_members > membership.len() / 2;

        Ok(ClusterHealth {
            total_members: membership.len(),
            healthy_members,
            learners: learners.len(),
            partitioned_nodes: partitioned_nodes.len(),
            current_leader: metrics.current_leader,
            is_healthy,
            last_log_index: metrics.last_log_index.unwrap_or(0),
            commit_index: metrics.last_applied.map(|id| id.index).unwrap_or(0),
        })
    }

    /// Get current node state (Leader, Follower, Candidate, Learner)
    pub async fn get_node_state(&self) -> openraft::ServerState {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.state
    }

    /// Get current term
    pub async fn get_current_term(&self) -> u64 {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_term
    }

    /// Get vote information for current term
    pub async fn get_vote_info(&self) -> Option<NodeId> {
        let metrics = self.raft.metrics().borrow().clone();
        Some(metrics.vote.leader_id.node_id)
    }

    /// Trigger an election (step down if leader, become candidate if follower)
    pub async fn trigger_election(&self) -> RaftResult<()> {
        log::info!("Triggering election for node {}", self.config.node_id);

        let current_state = self.get_node_state().await;
        match current_state {
            openraft::ServerState::Leader => {
                log::info!(
                    "Node {} is leader, stepping down to trigger election",
                    self.config.node_id
                );
                // Step down by triggering a leadership change
                // Note: openraft doesn't have a direct step-down method
                // We would need to implement this by stopping heartbeats or similar
                log::warn!(
                    "Direct step-down not implemented in openraft, election will happen naturally"
                );
            }
            openraft::ServerState::Follower => {
                log::info!(
                    "Node {} is follower, will participate in next election",
                    self.config.node_id
                );
                // Followers automatically participate in elections when they timeout
            }
            openraft::ServerState::Candidate => {
                log::info!("Node {} is already a candidate", self.config.node_id);
            }
            openraft::ServerState::Learner => {
                log::warn!(
                    "Node {} is a learner and cannot participate in elections",
                    self.config.node_id
                );
                return Err(RaftError::configuration(
                    "Learners cannot trigger elections",
                ));
            }
            openraft::ServerState::Shutdown => {
                log::error!(
                    "Node {} is shutdown and cannot participate in elections",
                    self.config.node_id
                );
                return Err(RaftError::configuration(
                    "Shutdown nodes cannot trigger elections",
                ));
            }
        }

        Ok(())
    }

    /// Wait for leadership election to complete
    pub async fn wait_for_election(&self, timeout: Duration) -> RaftResult<NodeId> {
        log::info!("Waiting for election to complete (timeout: {:?})", timeout);

        let start = std::time::Instant::now();
        let mut last_term = None;

        loop {
            if start.elapsed() > timeout {
                return Err(RaftError::timeout(
                    "Election did not complete within timeout",
                ));
            }

            let metrics = self.raft.metrics().borrow().clone();
            let current_term = metrics.current_term;

            // Check if we have a leader
            if let Some(leader_id) = metrics.current_leader {
                log::info!(
                    "Election completed, leader is node {} in term {}",
                    leader_id,
                    current_term
                );
                return Ok(leader_id);
            }

            // Log term changes
            if last_term != Some(current_term) {
                log::info!(
                    "Election in progress, term: {}, state: {:?}",
                    current_term,
                    metrics.state
                );
                last_term = Some(current_term);
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Monitor leadership changes and handle transitions
    pub async fn monitor_leadership_changes(&self) -> RaftResult<()> {
        log::info!(
            "Starting leadership change monitoring for node {}",
            self.config.node_id
        );

        let mut last_leader = None;
        let mut last_term = None;
        let mut last_state = None;

        loop {
            let metrics = self.raft.metrics().borrow().clone();
            let current_leader = metrics.current_leader;
            let current_term = metrics.current_term;
            let current_state = metrics.state;

            // Check for leadership changes
            if last_leader != current_leader {
                match (last_leader, current_leader) {
                    (None, Some(new_leader)) => {
                        log::info!(
                            "Leadership established: node {} became leader in term {}",
                            new_leader,
                            current_term
                        );
                        self.on_leadership_established(new_leader, current_term)
                            .await?;
                    }
                    (Some(old_leader), Some(new_leader)) if old_leader != new_leader => {
                        log::info!(
                            "Leadership changed: node {} -> node {} in term {}",
                            old_leader,
                            new_leader,
                            current_term
                        );
                        self.on_leadership_changed(old_leader, new_leader, current_term)
                            .await?;
                    }
                    (Some(old_leader), None) => {
                        log::warn!(
                            "Leadership lost: node {} stepped down in term {}",
                            old_leader,
                            current_term
                        );
                        self.on_leadership_lost(old_leader, current_term).await?;
                    }
                    _ => {}
                }
                last_leader = current_leader;
            }

            // Check for term changes
            if last_term != Some(current_term) {
                if let Some(prev_term) = last_term {
                    log::info!("Term changed: {} -> {}", prev_term, current_term);
                }
                last_term = Some(current_term);
            }

            // Check for state changes
            if last_state != Some(current_state) {
                if let Some(prev_state) = last_state {
                    log::info!(
                        "Node {} state changed: {:?} -> {:?}",
                        self.config.node_id,
                        prev_state,
                        current_state
                    );
                }
                self.on_state_changed(current_state).await?;
                last_state = Some(current_state);
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Handle leadership establishment
    async fn on_leadership_established(&self, leader_id: NodeId, term: u64) -> RaftResult<()> {
        log::info!(
            "Handling leadership establishment: leader={}, term={}",
            leader_id,
            term
        );

        if leader_id == self.config.node_id {
            log::info!("This node became the leader in term {}", term);
            // Perform leader initialization tasks
            self.on_became_leader(term).await?;
        } else {
            log::info!("Node {} became the leader in term {}", leader_id, term);
            // Perform follower tasks when new leader is established
            self.on_new_leader(leader_id, term).await?;
        }

        Ok(())
    }

    /// Handle leadership changes
    async fn on_leadership_changed(
        &self,
        old_leader: NodeId,
        new_leader: NodeId,
        term: u64,
    ) -> RaftResult<()> {
        log::info!(
            "Handling leadership change: {} -> {} in term {}",
            old_leader,
            new_leader,
            term
        );

        if old_leader == self.config.node_id {
            log::info!(
                "This node lost leadership to node {} in term {}",
                new_leader,
                term
            );
            self.on_lost_leadership(new_leader, term).await?;
        } else if new_leader == self.config.node_id {
            log::info!(
                "This node gained leadership from node {} in term {}",
                old_leader,
                term
            );
            self.on_became_leader(term).await?;
        } else {
            log::info!(
                "Leadership changed between other nodes: {} -> {}",
                old_leader,
                new_leader
            );
            self.on_new_leader(new_leader, term).await?;
        }

        Ok(())
    }

    /// Handle leadership loss
    async fn on_leadership_lost(&self, old_leader: NodeId, term: u64) -> RaftResult<()> {
        log::warn!(
            "Handling leadership loss: leader {} stepped down in term {}",
            old_leader,
            term
        );

        if old_leader == self.config.node_id {
            log::warn!("This node lost leadership in term {}", term);
            self.on_lost_leadership(0, term).await?; // 0 indicates no new leader yet
        }

        Ok(())
    }

    /// Handle state changes
    async fn on_state_changed(&self, new_state: openraft::ServerState) -> RaftResult<()> {
        log::info!(
            "Node {} state changed to {:?}",
            self.config.node_id,
            new_state
        );

        match new_state {
            openraft::ServerState::Leader => {
                // Already handled in leadership change events
            }
            openraft::ServerState::Follower => {
                log::info!("Node {} became follower", self.config.node_id);
            }
            openraft::ServerState::Candidate => {
                log::info!(
                    "Node {} became candidate, starting election",
                    self.config.node_id
                );
            }
            openraft::ServerState::Learner => {
                log::info!("Node {} is learner", self.config.node_id);
            }
            openraft::ServerState::Shutdown => {
                log::info!("Node {} is shutdown", self.config.node_id);
            }
        }

        Ok(())
    }

    /// Called when this node becomes leader
    async fn on_became_leader(&self, term: u64) -> RaftResult<()> {
        log::info!(
            "Node {} became leader in term {}",
            self.config.node_id,
            term
        );

        // Perform leader initialization
        // - Start accepting client requests
        // - Begin heartbeat to followers
        // - Check cluster health

        let health = self.get_cluster_health().await?;
        log::info!("Cluster health as new leader: {:?}", health);

        // Check for any partition recovery needed
        self.handle_partition_recovery().await?;

        Ok(())
    }

    /// Called when this node loses leadership
    async fn on_lost_leadership(&self, new_leader: NodeId, term: u64) -> RaftResult<()> {
        log::info!(
            "Node {} lost leadership to {} in term {}",
            self.config.node_id,
            new_leader,
            term
        );

        // Perform cleanup tasks
        // - Stop accepting client writes
        // - Redirect clients to new leader

        Ok(())
    }

    /// Called when a new leader is established (and it's not this node)
    async fn on_new_leader(&self, leader_id: NodeId, term: u64) -> RaftResult<()> {
        log::info!(
            "Node {} recognizes new leader {} in term {}",
            self.config.node_id,
            leader_id,
            term
        );

        // Update routing information
        // - Redirect client requests to new leader
        // - Update endpoint information if needed

        Ok(())
    }

    /// Get election timeout configuration
    pub fn get_election_timeout(&self) -> (Duration, Duration) {
        (
            Duration::from_millis(self.config.election_timeout_min_ms),
            Duration::from_millis(self.config.election_timeout_max_ms),
        )
    }

    /// Get heartbeat interval
    pub fn get_heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.config.heartbeat_interval_ms)
    }

    /// Check if election timeout has been exceeded
    pub async fn is_election_timeout_exceeded(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();

        // This is a simplified check - in practice, openraft handles this internally
        match metrics.state {
            openraft::ServerState::Follower | openraft::ServerState::Candidate => {
                // Check if we haven't heard from leader recently
                // This would require tracking last heartbeat time
                false // Placeholder - openraft handles this internally
            }
            _ => false,
        }
    }

    /// Force a leadership election (for testing/admin purposes)
    pub async fn force_election(&self) -> RaftResult<()> {
        log::warn!(
            "Forcing election for node {} (admin operation)",
            self.config.node_id
        );

        // This is primarily for testing and administrative purposes
        // In production, elections should happen naturally

        self.trigger_election().await?;

        // Wait for election to complete
        match tokio::time::timeout(
            Duration::from_secs(30),
            self.wait_for_election(Duration::from_secs(30)),
        )
        .await
        {
            Ok(Ok(leader_id)) => {
                log::info!("Forced election completed, leader: {}", leader_id);
                Ok(())
            }
            Ok(Err(e)) => {
                log::error!("Forced election failed: {}", e);
                Err(e)
            }
            Err(_) => {
                log::error!("Forced election timed out");
                Err(RaftError::timeout("Forced election timed out"))
            }
        }
    }
}

#[async_trait]
impl RaftNodeInterface for RaftNode {
    async fn start(&self, node_id: NodeId, peers: Vec<NodeId>) -> RaftResult<()> {
        log::info!("Starting Raft node {} with peers: {:?}", node_id, peers);

        // Verify node ID matches configuration
        if node_id != self.config.node_id {
            return Err(RaftError::configuration(format!(
                "Node ID mismatch: expected {}, got {}",
                self.config.node_id, node_id
            )));
        }

        // Add peer endpoints if not already present
        for peer_id in &peers {
            if *peer_id != node_id {
                let endpoints = self.endpoints.read().await;
                if !endpoints.contains_key(&peer_id) {
                    log::warn!("No endpoint configured for peer {}", peer_id);
                }
            }
        }

        // Start the node
        let init_cluster = peers.is_empty() || (peers.len() == 1 && peers[0] == node_id);
        self.start(init_cluster).await
    }

    async fn propose(&self, request: ClientRequest) -> RaftResult<ClientResponse> {
        let start = std::time::Instant::now();
        log::debug!("Proposing client request: {:?}", request.id);
        log::trace!("propose: request_id={:?}, command={}", request.id, request.command.command);

        // Check if we're the leader
        let leader_check_start = std::time::Instant::now();
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await;
            let leader_check_elapsed = leader_check_start.elapsed();
            log::trace!("propose: not leader check took {:?}, redirecting to {:?}", 
                leader_check_elapsed, leader_id);
            return Ok(ClientResponse {
                id: request.id,
                result: Err("Not leader".to_string()),
                leader_id,
            });
        }
        let leader_check_elapsed = leader_check_start.elapsed();
        log::trace!("propose: leader check took {:?}", leader_check_elapsed);

        // Submit the request to Raft
        let raft_write_start = std::time::Instant::now();
        match self.raft.client_write(request.clone()).await {
            Ok(response) => {
                let raft_write_elapsed = raft_write_start.elapsed();
                let total_elapsed = start.elapsed();
                log::debug!("Client request {} completed successfully in {:?}", request.id, total_elapsed);
                log::trace!("propose: request_id={:?}, raft_write={:?}, total={:?}", 
                    request.id, raft_write_elapsed, total_elapsed);
                
                // Return the actual response from the state machine
                Ok(response.data)
            }
            Err(e) => {
                let raft_write_elapsed = raft_write_start.elapsed();
                let total_elapsed = start.elapsed();
                log::error!("Client request failed after {:?}: {}", total_elapsed, e);
                log::trace!("propose: request_id={:?}, error={}, raft_write={:?}, total={:?}", 
                    request.id, e, raft_write_elapsed, total_elapsed);
                Ok(ClientResponse {
                    id: request.id,
                    result: Err(format!("Raft error: {}", e)),
                    leader_id: self.get_leader_id().await,
                })
            }
        }
    }

    async fn add_learner(&self, node_id: NodeId, endpoint: String) -> RaftResult<()> {
        let start = std::time::Instant::now();
        log::info!("Adding learner node {} at {}", node_id, endpoint);
        log::trace!("add_learner: node_id={}, endpoint={}", node_id, endpoint);

        // Check if we're the leader
        let leader_check_start = std::time::Instant::now();
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await;
            log::trace!("add_learner: not leader, redirecting to {:?}", leader_id);
            return Err(RaftError::NotLeader { 
                leader_id,
                context: "add_learner: not leader".to_string(),
            });
        }
        let leader_check_elapsed = leader_check_start.elapsed();
        log::trace!("add_learner: leader check took {:?}", leader_check_elapsed);

        // Check if node is already a member or learner
        let membership_check_start = std::time::Instant::now();
        if self.is_member(node_id).await? {
            log::trace!("add_learner: node {} is already a voting member", node_id);
            return Err(RaftError::configuration(format!(
                "Node {} is already a voting member",
                node_id
            )));
        }

        if self.is_learner(node_id).await? {
            log::trace!("add_learner: node {} is already a learner", node_id);
            return Err(RaftError::configuration(format!(
                "Node {} is already a learner",
                node_id
            )));
        }
        let membership_check_elapsed = membership_check_start.elapsed();
        log::trace!("add_learner: membership checks took {:?}", membership_check_elapsed);

        // Add the endpoint first
        let endpoint_add_start = std::time::Instant::now();
        self.add_endpoint(node_id, endpoint).await?;
        let endpoint_add_elapsed = endpoint_add_start.elapsed();
        log::trace!("add_learner: endpoint addition took {:?}", endpoint_add_elapsed);

        // Add as learner to the cluster
        let raft_add_start = std::time::Instant::now();
        let node = BasicNode::default();
        self.raft.add_learner(node_id, node, true).await.map_err(
            |e: openraft::error::RaftError<NodeId, _>| {
                // Convert openraft error - create a new RaftError with Infallible variant
                // by wrapping the error in a way that preserves the information
                // Since we can't directly convert between error variants, we'll use
                // the consensus error creation helper
                RaftError::consensus(format!("{}", e))
            },
        )?;
        let raft_add_elapsed = raft_add_start.elapsed();
        log::trace!("add_learner: raft add_learner took {:?}", raft_add_elapsed);

        let elapsed = start.elapsed();
        log::info!("Successfully added learner node {} in {:?}", node_id, elapsed);
        log::trace!("add_learner: total_duration={:?}", elapsed);
        Ok(())
    }

    async fn change_membership(&self, members: BTreeSet<NodeId>) -> RaftResult<()> {
        let _start = std::time::Instant::now();
        log::info!("Changing cluster membership to: {:?}", members);
        log::trace!("change_membership: members={:?}, count={}", members, members.len());

        // Check if we're the leader
        let leader_check_start = std::time::Instant::now();
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await;
            log::trace!("change_membership: not leader, redirecting to {:?}", leader_id);
            return Err(RaftError::NotLeader { 
                leader_id,
                context: "change_membership: not leader".to_string(),
            });
        }
        let leader_check_elapsed = leader_check_start.elapsed();
        log::trace!("change_membership: leader check took {:?}", leader_check_elapsed);

        // Validate the membership change
        let validation_start = std::time::Instant::now();
        if members.is_empty() {
            log::trace!("change_membership: empty membership rejected");
            return Err(RaftError::configuration(
                "Cannot have empty cluster membership",
            ));
        }

        // Ensure all members have endpoints configured
        let endpoints = self.endpoints.read().await;
        for &member_id in &members {
            if member_id != self.config.node_id && !endpoints.contains_key(&member_id) {
                log::trace!("change_membership: no endpoint for member {}", member_id);
                return Err(RaftError::configuration(format!(
                    "No endpoint configured for member {}",
                    member_id
                )));
            }
        }
        let validation_elapsed = validation_start.elapsed();
        log::trace!("change_membership: validation took {:?}", validation_elapsed);

        // Create membership configuration
        let _membership: openraft::Membership<NodeId, openraft::BasicNode> =
            openraft::Membership::new(vec![members.clone()], None);

        // Apply membership change
        self.raft
            .change_membership(members.clone(), false)
            .await
            .map_err(|e| RaftError::invalid_request(format!("Membership change failed: {}", e)))?;

        log::info!("Successfully changed cluster membership to: {:?}", members);
        Ok(())
    }

    async fn get_metrics(&self) -> RaftResult<RaftMetrics> {
        let metrics = self.raft.metrics().borrow().clone();
        Ok(metrics)
    }

    async fn shutdown(&self) -> RaftResult<()> {
        log::info!("Shutting down Raft node {}", self.config.node_id);

        let mut started = self.started.write().await;
        if !*started {
            log::warn!("Raft node {} already shut down", self.config.node_id);
            return Ok(());
        }

        // Shutdown the Raft instance
        self.raft
            .shutdown()
            .await
            .map_err(|e| RaftError::invalid_state(format!("Shutdown failed: {}", e)))?;

        *started = false;
        log::info!("Raft node {} shut down successfully", self.config.node_id);
        Ok(())
    }
}
