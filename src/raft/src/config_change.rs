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

//! Configuration change operations for Raft cluster

use crate::cluster_config::{ClusterConfiguration, NodeEndpoint};
use crate::discovery::HealthMonitor;
use crate::error::{RaftError, RaftResult};
use crate::node::RaftNodeInterface;
use crate::types::{BasicNode, NodeId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

/// Types of configuration changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigChangeType {
    /// Add a new node as a learner
    AddLearner {
        node_id: NodeId,
        endpoint: NodeEndpoint,
    },
    /// Promote a learner to a voting member
    PromoteLearner { node_id: NodeId },
    /// Add a node directly as a voting member (unsafe)
    AddVoter {
        node_id: NodeId,
        endpoint: NodeEndpoint,
    },
    /// Remove a node from the cluster
    RemoveNode { node_id: NodeId },
    /// Replace a failed node with a new one
    ReplaceNode {
        old_node_id: NodeId,
        new_node_id: NodeId,
        new_endpoint: NodeEndpoint,
    },
    /// Update node endpoint information
    UpdateEndpoint {
        node_id: NodeId,
        new_endpoint: NodeEndpoint,
    },
}

/// Configuration change request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChangeRequest {
    pub change_type: ConfigChangeType,
    pub force: bool,
    pub timeout: Duration,
    pub reason: String,
}

/// Configuration change result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChangeResult {
    pub success: bool,
    pub old_membership: BTreeSet<NodeId>,
    pub new_membership: BTreeSet<NodeId>,
    pub duration: Duration,
    pub error_message: Option<String>,
}

/// Configuration change operation status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigChangeStatus {
    /// Change is being prepared
    Preparing,
    /// Change is in progress
    InProgress,
    /// Change completed successfully
    Completed,
    /// Change failed
    Failed,
    /// Change was rolled back
    RolledBack,
}

/// Configuration change operation tracker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChangeOperation {
    pub id: String,
    pub request: ConfigChangeRequest,
    pub status: ConfigChangeStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<ConfigChangeResult>,
}

/// Configuration change entry for Raft log replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChangeEntry {
    pub change_id: String,
    pub change_type: ConfigChangeType,
    pub requested_by: NodeId,
    pub timestamp: DateTime<Utc>,
    pub force: bool,
    pub reason: String,
}

/// Safe configuration change manager
pub struct ConfigChangeManager {
    node_id: NodeId,
    health_monitor: Option<Arc<HealthMonitor>>,
    active_operations: Arc<RwLock<Vec<ConfigChangeOperation>>>,
    config: Arc<RwLock<ClusterConfiguration>>,
    /// Mutex to ensure only one configuration change happens at a time
    change_mutex: Arc<Mutex<()>>,
    /// Callback for notifying about configuration changes
    change_callbacks: Arc<RwLock<Vec<Box<dyn Fn(&ConfigChangeResult) + Send + Sync>>>>,
    /// Reference to the Raft node for actual membership changes
    raft_node: Option<Arc<dyn RaftNodeInterface + Send + Sync>>,
}

impl ConfigChangeManager {
    pub fn new(
        node_id: NodeId,
        config: Arc<RwLock<ClusterConfiguration>>,
        health_monitor: Option<Arc<HealthMonitor>>,
    ) -> Self {
        Self {
            node_id,
            health_monitor,
            active_operations: Arc::new(RwLock::new(Vec::new())),
            config,
            change_mutex: Arc::new(Mutex::new(())),
            change_callbacks: Arc::new(RwLock::new(Vec::new())),
            raft_node: None,
        }
    }

    /// Create a new ConfigChangeManager with Raft node integration
    pub fn new_with_raft(
        node_id: NodeId,
        config: Arc<RwLock<ClusterConfiguration>>,
        health_monitor: Option<Arc<HealthMonitor>>,
        raft_node: Arc<dyn RaftNodeInterface + Send + Sync>,
    ) -> Self {
        Self {
            node_id,
            health_monitor,
            active_operations: Arc::new(RwLock::new(Vec::new())),
            config,
            change_mutex: Arc::new(Mutex::new(())),
            change_callbacks: Arc::new(RwLock::new(Vec::new())),
            raft_node: Some(raft_node),
        }
    }

    /// Set the Raft node reference for integration
    pub fn set_raft_node(&mut self, raft_node: Arc<dyn RaftNodeInterface + Send + Sync>) {
        self.raft_node = Some(raft_node);
    }

    /// Validate a configuration change request
    pub async fn validate_change(&self, request: &ConfigChangeRequest) -> RaftResult<()> {
        // First validate cluster health and leadership
        self.validate_cluster_health().await?;
        self.validate_quorum_safety(request).await?;

        let config = self.config.read().await;

        match &request.change_type {
            ConfigChangeType::AddLearner { node_id, endpoint } => {
                self.validate_add_learner(*node_id, endpoint, &config)
                    .await?;
            }
            ConfigChangeType::PromoteLearner { node_id } => {
                self.validate_promote_learner(*node_id, &config).await?;
            }
            ConfigChangeType::AddVoter { node_id, endpoint } => {
                self.validate_add_voter(*node_id, endpoint, &config).await?;
            }
            ConfigChangeType::RemoveNode { node_id } => {
                self.validate_remove_node(*node_id, &config).await?;
            }
            ConfigChangeType::ReplaceNode {
                old_node_id,
                new_node_id,
                new_endpoint,
            } => {
                self.validate_replace_node(*old_node_id, *new_node_id, new_endpoint, &config)
                    .await?;
            }
            ConfigChangeType::UpdateEndpoint {
                node_id,
                new_endpoint,
            } => {
                self.validate_update_endpoint(*node_id, new_endpoint, &config)
                    .await?;
            }
        }

        Ok(())
    }

    async fn validate_add_learner(
        &self,
        node_id: NodeId,
        endpoint: &NodeEndpoint,
        config: &ClusterConfiguration,
    ) -> RaftResult<()> {
        // Check if node already exists
        if config.endpoints.contains_key(&node_id) {
            return Err(RaftError::configuration(format!(
                "Node {} already exists in cluster",
                node_id
            )));
        }

        // Validate endpoint
        endpoint.socket_addr()?;

        // Check if endpoint is already in use
        for existing_endpoint in config.endpoints.values() {
            if existing_endpoint.address() == endpoint.address() {
                return Err(RaftError::configuration(format!(
                    "Endpoint {} is already in use by node {}",
                    endpoint.address(),
                    existing_endpoint.node_id
                )));
            }
        }

        Ok(())
    }

    async fn validate_promote_learner(
        &self,
        node_id: NodeId,
        config: &ClusterConfiguration,
    ) -> RaftResult<()> {
        // Check if node exists
        if !config.endpoints.contains_key(&node_id) {
            return Err(RaftError::configuration(format!(
                "Node {} does not exist in cluster",
                node_id
            )));
        }

        // Check if node is healthy (if health monitor is available)
        if let Some(health_monitor) = &self.health_monitor {
            if let Some(health) = health_monitor.get_node_health(node_id).await {
                if !health.is_healthy() {
                    return Err(RaftError::configuration(format!(
                        "Node {} is not healthy (status: {:?})",
                        node_id, health.status
                    )));
                }
            }
        }

        Ok(())
    }

    async fn validate_add_voter(
        &self,
        node_id: NodeId,
        endpoint: &NodeEndpoint,
        config: &ClusterConfiguration,
    ) -> RaftResult<()> {
        // This is generally unsafe - should add as learner first
        log::warn!("Adding node {} directly as voter is unsafe", node_id);

        self.validate_add_learner(node_id, endpoint, config).await
    }

    async fn validate_remove_node(
        &self,
        node_id: NodeId,
        config: &ClusterConfiguration,
    ) -> RaftResult<()> {
        // Check if node exists
        if !config.endpoints.contains_key(&node_id) {
            return Err(RaftError::configuration(format!(
                "Node {} does not exist in cluster",
                node_id
            )));
        }

        // Don't allow removing the last node
        if config.endpoints.len() <= 1 {
            return Err(RaftError::configuration(
                "Cannot remove the last node from cluster",
            ));
        }

        // Don't allow removing ourselves if we're the only node
        if node_id == self.node_id && config.endpoints.len() == 1 {
            return Err(RaftError::configuration(
                "Cannot remove self when it's the only node",
            ));
        }

        Ok(())
    }

    async fn validate_replace_node(
        &self,
        old_node_id: NodeId,
        new_node_id: NodeId,
        new_endpoint: &NodeEndpoint,
        config: &ClusterConfiguration,
    ) -> RaftResult<()> {
        // Check if old node exists
        if !config.endpoints.contains_key(&old_node_id) {
            return Err(RaftError::configuration(format!(
                "Old node {} does not exist in cluster",
                old_node_id
            )));
        }

        // Check if new node already exists
        if config.endpoints.contains_key(&new_node_id) {
            return Err(RaftError::configuration(format!(
                "New node {} already exists in cluster",
                new_node_id
            )));
        }

        // Validate new endpoint
        new_endpoint.socket_addr()?;

        Ok(())
    }

    async fn validate_update_endpoint(
        &self,
        node_id: NodeId,
        new_endpoint: &NodeEndpoint,
        config: &ClusterConfiguration,
    ) -> RaftResult<()> {
        // Check if node exists
        if !config.endpoints.contains_key(&node_id) {
            return Err(RaftError::configuration(format!(
                "Node {} does not exist in cluster",
                node_id
            )));
        }

        // Validate new endpoint
        new_endpoint.socket_addr()?;

        Ok(())
    }

    /// Add a callback for configuration change notifications
    pub async fn add_change_callback<F>(&self, callback: F)
    where
        F: Fn(&ConfigChangeResult) + Send + Sync + 'static,
    {
        let mut callbacks = self.change_callbacks.write().await;
        callbacks.push(Box::new(callback));
    }

    /// Notify all registered callbacks about a configuration change
    async fn notify_callbacks(&self, result: &ConfigChangeResult) {
        let callbacks = self.change_callbacks.read().await;
        for callback in callbacks.iter() {
            callback(result);
        }
    }

    /// Check if the current node is the leader (required for configuration changes)
    async fn ensure_leadership(&self) -> RaftResult<()> {
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;
            if !matches!(metrics.state, openraft::ServerState::Leader) {
                return Err(RaftError::NotLeader {
                    leader_id: metrics.current_leader,
                });
            }
        } else {
            log::warn!("No Raft node reference available, cannot verify leadership");
        }
        Ok(())
    }

    /// Execute a configuration change with safety checks
    pub async fn execute_change(
        &self,
        request: ConfigChangeRequest,
    ) -> RaftResult<ConfigChangeResult> {
        // Acquire the change mutex to ensure only one change at a time
        let _lock = self.change_mutex.lock().await;

        // Ensure we're the leader (if Raft node is available)
        self.ensure_leadership().await?;

        // Validate the change
        self.validate_change(&request).await?;

        let operation_id = format!("config_change_{}", Uuid::new_v4());
        let start_time = Utc::now();

        // Create operation tracker
        let mut operation = ConfigChangeOperation {
            id: operation_id.clone(),
            request: request.clone(),
            status: ConfigChangeStatus::Preparing,
            started_at: start_time,
            completed_at: None,
            result: None,
        };

        // Add to active operations
        {
            let mut operations = self.active_operations.write().await;
            operations.push(operation.clone());
        }

        log::info!("Starting configuration change: {:?}", request.change_type);

        // Execute the change
        let result = match self.execute_change_internal(&request).await {
            Ok(result) => {
                operation.status = ConfigChangeStatus::Completed;
                operation.result = Some(result.clone());
                log::info!("Configuration change completed successfully");
                result
            }
            Err(e) => {
                operation.status = ConfigChangeStatus::Failed;
                let error_result = ConfigChangeResult {
                    success: false,
                    old_membership: BTreeSet::new(),
                    new_membership: BTreeSet::new(),
                    duration: (Utc::now() - start_time).to_std().unwrap_or(Duration::ZERO),
                    error_message: Some(e.to_string()),
                };
                operation.result = Some(error_result.clone());
                log::error!("Configuration change failed: {}", e);
                return Err(e);
            }
        };

        operation.completed_at = Some(Utc::now());

        // Update operation tracker
        {
            let mut operations = self.active_operations.write().await;
            if let Some(op) = operations.iter_mut().find(|op| op.id == operation_id) {
                *op = operation;
            }
        }

        // Notify callbacks about the change
        self.notify_callbacks(&result).await;

        Ok(result)
    }

    async fn execute_change_internal(
        &self,
        request: &ConfigChangeRequest,
    ) -> RaftResult<ConfigChangeResult> {
        let start_time = Utc::now();
        let old_membership = self.get_current_membership().await?;

        match &request.change_type {
            ConfigChangeType::AddLearner { node_id, endpoint } => {
                self.execute_add_learner(*node_id, endpoint.clone()).await?;
            }
            ConfigChangeType::PromoteLearner { node_id } => {
                self.execute_promote_learner(*node_id).await?;
            }
            ConfigChangeType::AddVoter { node_id, endpoint } => {
                self.execute_add_voter(*node_id, endpoint.clone()).await?;
            }
            ConfigChangeType::RemoveNode { node_id } => {
                self.execute_remove_node(*node_id).await?;
            }
            ConfigChangeType::ReplaceNode {
                old_node_id,
                new_node_id,
                new_endpoint,
            } => {
                self.execute_replace_node(*old_node_id, *new_node_id, new_endpoint.clone())
                    .await?;
            }
            ConfigChangeType::UpdateEndpoint {
                node_id,
                new_endpoint,
            } => {
                self.execute_update_endpoint(*node_id, new_endpoint.clone())
                    .await?;
            }
        }

        let new_membership = self.get_current_membership().await?;

        Ok(ConfigChangeResult {
            success: true,
            old_membership,
            new_membership,
            duration: (Utc::now() - start_time).to_std().unwrap_or(Duration::ZERO),
            error_message: None,
        })
    }

    async fn execute_add_learner(&self, node_id: NodeId, endpoint: NodeEndpoint) -> RaftResult<()> {
        // Add endpoint to configuration first
        {
            let mut config = self.config.write().await;
            config.endpoints.insert(node_id, endpoint.clone());
        }

        // If we have a Raft node reference, use it to add the learner
        if let Some(raft_node) = &self.raft_node {
            let endpoint_str = endpoint.address();
            raft_node.add_learner(node_id, endpoint_str).await?;
            log::info!(
                "Added learner node {} to Raft cluster at {}",
                node_id,
                endpoint.address()
            );
        } else {
            log::warn!("No Raft node reference available, only updated configuration");
        }

        log::info!("Added learner node {} to configuration", node_id);
        Ok(())
    }

    async fn execute_promote_learner(&self, node_id: NodeId) -> RaftResult<()> {
        // If we have a Raft node reference, use it to promote the learner
        if let Some(raft_node) = &self.raft_node {
            // Get current membership and add the learner as a voting member
            let current_membership = self.get_current_membership().await?;
            let mut new_membership = current_membership;
            new_membership.insert(node_id);

            raft_node.change_membership(new_membership).await?;
            log::info!(
                "Promoted learner node {} to voting member via Raft",
                node_id
            );
        } else {
            log::warn!(
                "No Raft node reference available, cannot promote learner {}",
                node_id
            );
            return Err(RaftError::configuration(
                "Raft node not available for membership change",
            ));
        }

        log::info!("Promoted learner node {} to voting member", node_id);
        Ok(())
    }

    async fn execute_add_voter(&self, node_id: NodeId, endpoint: NodeEndpoint) -> RaftResult<()> {
        // Add endpoint to configuration first
        {
            let mut config = self.config.write().await;
            config.endpoints.insert(node_id, endpoint.clone());
        }

        // If we have a Raft node reference, use it to add the voter directly
        if let Some(raft_node) = &self.raft_node {
            // First add as learner
            let endpoint_str = endpoint.address();
            raft_node.add_learner(node_id, endpoint_str).await?;

            // Then promote to voting member
            let current_membership = self.get_current_membership().await?;
            let mut new_membership = current_membership;
            new_membership.insert(node_id);

            raft_node.change_membership(new_membership).await?;
            log::info!(
                "Added voter node {} to Raft cluster at {}",
                node_id,
                endpoint.address()
            );
        } else {
            log::warn!("No Raft node reference available, only updated configuration");
        }

        log::info!("Added voter node {} to configuration", node_id);
        Ok(())
    }

    async fn execute_remove_node(&self, node_id: NodeId) -> RaftResult<()> {
        // If we have a Raft node reference, use it to remove the node from cluster
        if let Some(raft_node) = &self.raft_node {
            // Get current membership and remove the node
            let current_membership = self.get_current_membership().await?;
            let mut new_membership = current_membership;
            new_membership.remove(&node_id);

            if !new_membership.is_empty() {
                raft_node.change_membership(new_membership).await?;
                log::info!("Removed node {} from Raft cluster", node_id);
            } else {
                return Err(RaftError::configuration(
                    "Cannot remove the last node from cluster",
                ));
            }
        } else {
            log::warn!("No Raft node reference available, only updating configuration");
        }

        // Remove endpoint from configuration
        {
            let mut config = self.config.write().await;
            config.endpoints.remove(&node_id);
        }

        log::info!("Removed node {} from configuration", node_id);
        Ok(())
    }

    async fn execute_replace_node(
        &self,
        old_node_id: NodeId,
        new_node_id: NodeId,
        new_endpoint: NodeEndpoint,
    ) -> RaftResult<()> {
        // If we have a Raft node reference, perform the replacement atomically
        if let Some(raft_node) = &self.raft_node {
            // First add the new node as a learner
            let endpoint_str = new_endpoint.address();
            raft_node.add_learner(new_node_id, endpoint_str).await?;

            // Wait for the new node to catch up (if the method is available)
            // This would require access to the concrete RaftNode type, not just the interface
            log::info!(
                "New node {} added as learner, waiting for catchup",
                new_node_id
            );

            // Get current membership, replace old with new
            let current_membership = self.get_current_membership().await?;
            let mut new_membership = current_membership;
            new_membership.remove(&old_node_id);
            new_membership.insert(new_node_id);

            raft_node.change_membership(new_membership).await?;
            log::info!(
                "Replaced node {} with node {} in Raft cluster",
                old_node_id,
                new_node_id
            );
        } else {
            log::warn!("No Raft node reference available, only updating configuration");
        }

        // Update configuration
        {
            let mut config = self.config.write().await;
            config.endpoints.remove(&old_node_id);
            config.endpoints.insert(new_node_id, new_endpoint);
        }

        log::info!("Replaced node {} with node {}", old_node_id, new_node_id);
        Ok(())
    }

    async fn execute_update_endpoint(
        &self,
        node_id: NodeId,
        new_endpoint: NodeEndpoint,
    ) -> RaftResult<()> {
        // Update endpoint in configuration
        {
            let mut config = self.config.write().await;
            config.endpoints.insert(node_id, new_endpoint);
        }

        log::info!("Updated endpoint for node {}", node_id);
        Ok(())
    }

    async fn get_current_membership(&self) -> RaftResult<BTreeSet<NodeId>> {
        let config = self.config.read().await;
        Ok(config.endpoints.keys().copied().collect())
    }

    /// Get active configuration change operations
    pub async fn get_active_operations(&self) -> Vec<ConfigChangeOperation> {
        self.active_operations.read().await.clone()
    }

    /// Cancel a configuration change operation
    pub async fn cancel_operation(&self, operation_id: &str) -> RaftResult<()> {
        let mut operations = self.active_operations.write().await;

        if let Some(operation) = operations.iter_mut().find(|op| op.id == operation_id) {
            if operation.status == ConfigChangeStatus::InProgress {
                operation.status = ConfigChangeStatus::RolledBack;
                operation.completed_at = Some(Utc::now());
                log::info!("Cancelled configuration change operation: {}", operation_id);
                return Ok(());
            }
        }

        Err(RaftError::configuration(format!(
            "Operation {} not found or cannot be cancelled",
            operation_id
        )))
    }

    /// Clean up completed operations
    pub async fn cleanup_operations(&self, max_age: Duration) {
        let mut operations = self.active_operations.write().await;
        let now = Utc::now();

        operations.retain(|op| {
            if let Some(completed_at) = op.completed_at {
                (now - completed_at).to_std().unwrap_or(Duration::MAX) < max_age
            } else {
                true // Keep active operations
            }
        });
    }

    /// Handle node failures during configuration changes
    pub async fn handle_node_failure(&self, failed_node_id: NodeId) -> RaftResult<()> {
        log::warn!(
            "Handling failure of node {} during configuration changes",
            failed_node_id
        );

        // Check if there are any active operations involving this node
        let operations = self.active_operations.read().await;
        let affected_operations: Vec<_> = operations
            .iter()
            .filter(|op| {
                op.status == ConfigChangeStatus::InProgress
                    && self.operation_involves_node(&op.request, failed_node_id)
            })
            .cloned()
            .collect();

        drop(operations);

        // Handle affected operations
        for operation in affected_operations {
            log::warn!(
                "Configuration change operation {} affected by node {} failure",
                operation.id,
                failed_node_id
            );

            // For now, we just mark them as failed
            // In a real implementation, we might try to recover or rollback
            self.cancel_operation(&operation.id).await?;
        }

        Ok(())
    }

    fn operation_involves_node(&self, request: &ConfigChangeRequest, node_id: NodeId) -> bool {
        match &request.change_type {
            ConfigChangeType::AddLearner { node_id: n, .. } => *n == node_id,
            ConfigChangeType::PromoteLearner { node_id: n } => *n == node_id,
            ConfigChangeType::AddVoter { node_id: n, .. } => *n == node_id,
            ConfigChangeType::RemoveNode { node_id: n } => *n == node_id,
            ConfigChangeType::ReplaceNode {
                old_node_id,
                new_node_id,
                ..
            } => *old_node_id == node_id || *new_node_id == node_id,
            ConfigChangeType::UpdateEndpoint { node_id: n, .. } => *n == node_id,
        }
    }

    /// Validate cluster health before making changes
    async fn validate_cluster_health(&self) -> RaftResult<()> {
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;

            // Check if we have a stable leader
            if metrics.current_leader.is_none() {
                return Err(RaftError::configuration(
                    "No leader available, cluster is not stable for configuration changes",
                ));
            }

            // Check if the cluster is in a healthy state
            if matches!(metrics.state, openraft::ServerState::Candidate) {
                return Err(RaftError::configuration(
                    "Cluster is in election state, wait for stable leadership",
                ));
            }
        }
        Ok(())
    }

    /// Validate that the change won't break cluster quorum
    async fn validate_quorum_safety(&self, request: &ConfigChangeRequest) -> RaftResult<()> {
        let current_membership = self.get_current_membership().await?;
        let current_size = current_membership.len();

        match &request.change_type {
            ConfigChangeType::RemoveNode { node_id } => {
                if current_size <= 1 {
                    return Err(RaftError::configuration(
                        "Cannot remove node from single-node cluster",
                    ));
                }

                // Ensure we maintain quorum after removal
                let new_size = current_size - 1;
                let required_for_quorum = (current_size / 2) + 1;

                if new_size < required_for_quorum {
                    return Err(RaftError::configuration(format!(
                        "Removing node {} would break quorum (current: {}, after removal: {}, required: {})",
                        node_id, current_size, new_size, required_for_quorum
                    )));
                }
            }
            ConfigChangeType::ReplaceNode { old_node_id, .. } => {
                // Replacement maintains cluster size, but validate the old node exists
                if !current_membership.contains(old_node_id) {
                    return Err(RaftError::configuration(format!(
                        "Cannot replace node {} that is not in current membership",
                        old_node_id
                    )));
                }
            }
            _ => {
                // Adding nodes doesn't break quorum
            }
        }

        Ok(())
    }

    /// Get the current Raft membership from the node
    async fn get_raft_membership(&self) -> RaftResult<BTreeSet<NodeId>> {
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;
            let membership = metrics.membership_config.membership();
            Ok(membership.voter_ids().collect())
        } else {
            // Fallback to configuration-based membership
            self.get_current_membership().await
        }
    }

    /// Perform a dry run of the configuration change to validate it
    pub async fn dry_run_change(
        &self,
        request: &ConfigChangeRequest,
    ) -> RaftResult<ConfigChangeResult> {
        log::info!(
            "Performing dry run for configuration change: {:?}",
            request.change_type
        );

        // Validate leadership
        self.ensure_leadership().await?;

        // Validate cluster health
        self.validate_cluster_health().await?;

        // Validate the change itself
        self.validate_change(request).await?;

        // Validate quorum safety
        self.validate_quorum_safety(request).await?;

        let old_membership = self.get_current_membership().await?;
        let new_membership = self
            .simulate_membership_change(request, &old_membership)
            .await?;

        Ok(ConfigChangeResult {
            success: true,
            old_membership,
            new_membership,
            duration: Duration::ZERO,
            error_message: None,
        })
    }

    /// Synchronize configuration changes across all cluster nodes through Raft protocol
    pub async fn sync_configuration_change(
        &self,
        request: ConfigChangeRequest,
    ) -> RaftResult<ConfigChangeResult> {
        log::info!(
            "Synchronizing configuration change across cluster: {:?}",
            request.change_type
        );

        // Acquire the change mutex to ensure only one change at a time
        let _lock = self.change_mutex.lock().await;

        // Ensure we're the leader before attempting synchronization
        self.ensure_leadership().await?;

        // Validate the change before synchronization
        self.validate_change(&request).await?;

        let operation_id = format!("sync_config_change_{}", Uuid::new_v4());
        let start_time = Utc::now();

        // Create operation tracker for synchronization
        let mut operation = ConfigChangeOperation {
            id: operation_id.clone(),
            request: request.clone(),
            status: ConfigChangeStatus::Preparing,
            started_at: start_time,
            completed_at: None,
            result: None,
        };

        // Add to active operations
        {
            let mut operations = self.active_operations.write().await;
            operations.push(operation.clone());
        }

        // Create a configuration change entry for Raft log
        let config_change_entry = ConfigChangeEntry {
            change_id: operation_id.clone(),
            change_type: request.change_type.clone(),
            requested_by: self.node_id,
            timestamp: Utc::now(),
            force: request.force,
            reason: request.reason.clone(),
        };

        operation.status = ConfigChangeStatus::InProgress;

        // Update operation status
        {
            let mut operations = self.active_operations.write().await;
            if let Some(op) = operations.iter_mut().find(|op| op.id == operation_id) {
                op.status = ConfigChangeStatus::InProgress;
            }
        }

        let sync_result = match self
            .execute_synchronized_change(config_change_entry, &request)
            .await
        {
            Ok(result) => {
                operation.status = ConfigChangeStatus::Completed;
                operation.result = Some(result.clone());
                log::info!("Configuration change synchronized successfully across cluster");
                result
            }
            Err(e) => {
                operation.status = ConfigChangeStatus::Failed;
                let error_result = ConfigChangeResult {
                    success: false,
                    old_membership: self.get_current_membership().await.unwrap_or_default(),
                    new_membership: BTreeSet::new(),
                    duration: (Utc::now() - start_time).to_std().unwrap_or(Duration::ZERO),
                    error_message: Some(e.to_string()),
                };
                operation.result = Some(error_result.clone());
                log::error!("Configuration change synchronization failed: {}", e);

                // Attempt rollback on failure
                let error_msg = e.to_string();
                if let Err(rollback_err) = self
                    .handle_config_change_failure(
                        operation_id.clone(),
                        RaftError::configuration(error_msg.clone()),
                    )
                    .await
                {
                    log::error!(
                        "Failed to rollback configuration change {}: {}",
                        operation_id,
                        rollback_err
                    );
                }

                return Err(e);
            }
        };

        operation.completed_at = Some(Utc::now());

        // Update operation tracker
        {
            let mut operations = self.active_operations.write().await;
            if let Some(op) = operations.iter_mut().find(|op| op.id == operation_id) {
                *op = operation;
            }
        }

        // Notify callbacks about the change
        self.notify_callbacks(&sync_result).await;

        Ok(sync_result)
    }

    /// Execute a synchronized configuration change through Raft protocol
    async fn execute_synchronized_change(
        &self,
        config_change_entry: ConfigChangeEntry,
        request: &ConfigChangeRequest,
    ) -> RaftResult<ConfigChangeResult> {
        let start_time = Utc::now();
        let old_membership = self.get_current_membership().await?;

        // Step 1: Submit the configuration change through Raft protocol for replication
        log::info!("Step 1: Submitting configuration change to Raft log for replication");
        self.submit_config_change_to_raft(config_change_entry.clone())
            .await?;

        // Step 2: Wait for the change to be committed and applied on all nodes
        log::info!("Step 2: Waiting for configuration change to be committed across cluster");
        self.wait_for_config_sync(config_change_entry.change_id.clone(), request.timeout)
            .await?;

        // Step 3: Execute the actual membership change through Raft
        log::info!("Step 3: Executing membership change through Raft");
        self.execute_raft_membership_change(request).await?;

        // Step 4: Wait for membership change to be applied
        log::info!("Step 4: Waiting for membership change to be applied");
        self.wait_for_membership_sync(request.timeout).await?;

        // Step 5: Verify that all nodes have consistent configuration
        log::info!("Step 5: Verifying configuration consistency across cluster");
        self.verify_config_consistency().await?;

        let new_membership = self.get_current_membership().await?;

        Ok(ConfigChangeResult {
            success: true,
            old_membership,
            new_membership,
            duration: (Utc::now() - start_time).to_std().unwrap_or(Duration::ZERO),
            error_message: None,
        })
    }

    /// Execute the actual Raft membership change
    async fn execute_raft_membership_change(
        &self,
        request: &ConfigChangeRequest,
    ) -> RaftResult<()> {
        if let Some(raft_node) = &self.raft_node {
            match &request.change_type {
                ConfigChangeType::AddLearner { node_id, endpoint } => {
                    let endpoint_str = endpoint.address();
                    raft_node.add_learner(*node_id, endpoint_str).await?;
                    log::info!("Added learner node {} via Raft", node_id);
                }
                ConfigChangeType::PromoteLearner { node_id } => {
                    let current_membership = self.get_raft_membership().await?;
                    let mut new_membership = current_membership;
                    new_membership.insert(*node_id);
                    raft_node.change_membership(new_membership).await?;
                    log::info!(
                        "Promoted learner node {} to voting member via Raft",
                        node_id
                    );
                }
                ConfigChangeType::AddVoter { node_id, endpoint } => {
                    // First add as learner, then promote
                    let endpoint_str = endpoint.address();
                    raft_node.add_learner(*node_id, endpoint_str).await?;

                    // Wait a bit for the learner to catch up
                    tokio::time::sleep(Duration::from_millis(500)).await;

                    let current_membership = self.get_raft_membership().await?;
                    let mut new_membership = current_membership;
                    new_membership.insert(*node_id);
                    raft_node.change_membership(new_membership).await?;
                    log::info!("Added voter node {} via Raft", node_id);
                }
                ConfigChangeType::RemoveNode { node_id } => {
                    let current_membership = self.get_raft_membership().await?;
                    let mut new_membership = current_membership;
                    new_membership.remove(node_id);

                    if !new_membership.is_empty() {
                        raft_node.change_membership(new_membership).await?;
                        log::info!("Removed node {} via Raft", node_id);
                    } else {
                        return Err(RaftError::configuration(
                            "Cannot remove the last node from cluster",
                        ));
                    }
                }
                ConfigChangeType::ReplaceNode {
                    old_node_id,
                    new_node_id,
                    new_endpoint,
                } => {
                    // First add the new node as a learner
                    let endpoint_str = new_endpoint.address();
                    raft_node.add_learner(*new_node_id, endpoint_str).await?;

                    // Wait for the new node to catch up
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    // Replace old with new in membership
                    let current_membership = self.get_raft_membership().await?;
                    let mut new_membership = current_membership;
                    new_membership.remove(old_node_id);
                    new_membership.insert(*new_node_id);
                    raft_node.change_membership(new_membership).await?;
                    log::info!(
                        "Replaced node {} with {} via Raft",
                        old_node_id,
                        new_node_id
                    );
                }
                ConfigChangeType::UpdateEndpoint { .. } => {
                    // Endpoint updates don't require Raft membership changes
                    log::info!("Endpoint update doesn't require Raft membership change");
                }
            }
        } else {
            return Err(RaftError::configuration(
                "Raft node not available for membership change",
            ));
        }

        Ok(())
    }

    /// Wait for membership change to be synchronized across all nodes
    async fn wait_for_membership_sync(&self, timeout: Duration) -> RaftResult<()> {
        log::info!("Waiting for membership change to sync across cluster");

        let start_time = std::time::Instant::now();
        let check_interval = Duration::from_millis(200);

        while start_time.elapsed() < timeout {
            // Check if membership is consistent across the cluster
            if self.check_membership_consistency().await? {
                log::info!("Membership change synchronized across all nodes");
                return Ok(());
            }

            // Check if we're still the leader
            if let Some(raft_node) = &self.raft_node {
                let metrics = raft_node.get_metrics().await?;
                if !matches!(metrics.state, openraft::ServerState::Leader) {
                    return Err(RaftError::NotLeader {
                        leader_id: metrics.current_leader,
                    });
                }
            }

            tokio::time::sleep(check_interval).await;
        }

        Err(RaftError::timeout(format!(
            "Membership change did not sync within {:?}",
            timeout
        )))
    }

    /// Check if membership is consistent across the cluster
    async fn check_membership_consistency(&self) -> RaftResult<bool> {
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;

            // Check if all followers have the same membership configuration
            if let Some(replication_map) = &metrics.replication {
                let current_log_index = metrics.last_log_index.unwrap_or(0);
                let committed_index = metrics.last_applied.map(|log_id| log_id.index).unwrap_or(0);

                // Ensure the membership change has been committed
                if current_log_index > committed_index + 10 {
                    log::debug!("Membership change not yet committed, waiting...");
                    return Ok(false);
                }

                // Check if all followers are caught up with the membership change
                for (node_id, replication_state) in replication_map {
                    if let Some(replication_data) = replication_state {
                        let matched_index = replication_data.index;
                        let lag = committed_index.saturating_sub(matched_index);
                        if lag > 2 {
                            // Allow minimal lag
                            log::debug!("Node {} still syncing membership, lag: {}", node_id, lag);
                            return Ok(false);
                        }
                    } else {
                        log::debug!("Node {} replication state unknown", node_id);
                        return Ok(false);
                    }
                }
            }

            return Ok(true);
        }

        Ok(false)
    }

    /// Submit configuration change to Raft log for replication
    async fn submit_config_change_to_raft(
        &self,
        config_change: ConfigChangeEntry,
    ) -> RaftResult<()> {
        if let Some(raft_node) = &self.raft_node {
            log::info!(
                "Submitting configuration change {} to Raft log",
                config_change.change_id
            );

            // Serialize the configuration change entry
            let config_change_data = serde_json::to_vec(&config_change).map_err(|e| {
                RaftError::serialization(format!("Failed to serialize config change: {}", e))
            })?;

            // Create a client request for the configuration change
            let client_request = crate::types::ClientRequest {
                id: crate::types::RequestId::new(),
                command: crate::types::RedisCommand::new(
                    "CONFIG_CHANGE".to_string(),
                    vec![config_change_data.into()],
                ),
                consistency_level: crate::types::ConsistencyLevel::Linearizable,
            };

            // Submit through Raft for replication to all nodes
            let response = raft_node.propose(client_request).await?;

            match response.result {
                Ok(_) => {
                    log::info!(
                        "Configuration change {} successfully submitted to Raft log",
                        config_change.change_id
                    );
                    Ok(())
                }
                Err(error) => {
                    log::error!(
                        "Failed to submit configuration change {} to Raft log: {}",
                        config_change.change_id,
                        error
                    );
                    Err(RaftError::consensus(format!(
                        "Failed to replicate configuration change: {}",
                        error
                    )))
                }
            }
        } else {
            Err(RaftError::configuration(
                "Raft node not available for configuration synchronization",
            ))
        }
    }

    /// Wait for configuration change to be synchronized across all nodes
    async fn wait_for_config_sync(&self, change_id: String, timeout: Duration) -> RaftResult<()> {
        log::info!(
            "Waiting for configuration change {} to sync across cluster",
            change_id
        );

        let start_time = std::time::Instant::now();
        let check_interval = Duration::from_millis(200);
        let mut last_log_index = 0u64;

        while start_time.elapsed() < timeout {
            // Check if we're still the leader
            if let Some(raft_node) = &self.raft_node {
                let metrics = raft_node.get_metrics().await?;
                if !matches!(metrics.state, openraft::ServerState::Leader) {
                    return Err(RaftError::NotLeader {
                        leader_id: metrics.current_leader,
                    });
                }

                // Get current log index
                let current_log_index = metrics.last_log_index.unwrap_or(0);
                let committed_index = metrics.last_applied.map(|log_id| log_id.index).unwrap_or(0);

                // Check if the configuration change has been committed
                if committed_index >= current_log_index {
                    // Check if all nodes have applied the configuration change
                    if self.check_all_nodes_synced(&change_id).await? {
                        log::info!(
                            "Configuration change {} synchronized across all nodes",
                            change_id
                        );
                        return Ok(());
                    }
                }

                // Log progress if log index changed
                if current_log_index != last_log_index {
                    log::debug!(
                        "Configuration sync progress - log index: {}, committed: {}",
                        current_log_index,
                        committed_index
                    );
                    last_log_index = current_log_index;
                }
            }

            tokio::time::sleep(check_interval).await;
        }

        Err(RaftError::timeout(format!(
            "Configuration change {} did not sync within {:?}",
            change_id, timeout
        )))
    }

    /// Check if all nodes have synchronized the configuration change
    async fn check_all_nodes_synced(&self, change_id: &str) -> RaftResult<bool> {
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;

            // Check if the change has been committed and applied
            let committed_index = metrics.last_applied.map(|log_id| log_id.index).unwrap_or(0);
            let current_log_index = metrics.last_log_index.unwrap_or(0);

            // Ensure the configuration change has been committed locally first
            if committed_index < current_log_index {
                log::debug!(
                    "Configuration change {} not yet committed locally (committed: {}, current: {})",
                    change_id,
                    committed_index,
                    current_log_index
                );
                return Ok(false);
            }

            // Check replication to all followers
            if let Some(replication_map) = &metrics.replication {
                let mut all_synced = true;
                let mut total_nodes = 1; // Count leader
                let mut synced_nodes = 1; // Leader is always synced

                for (node_id, replication_state) in replication_map {
                    total_nodes += 1;

                    if let Some(replication_data) = replication_state {
                        // Allow small lag for network delays
                        let matched_index = replication_data.index;
                        let lag = committed_index.saturating_sub(matched_index);
                        if lag <= 2 {
                            synced_nodes += 1;
                            log::debug!("Node {} synced (lag: {})", node_id, lag);
                        } else {
                            log::debug!("Node {} still syncing, lag: {}", node_id, lag);
                            all_synced = false;
                        }
                    } else {
                        log::debug!("Node {} replication state unknown", node_id);
                        all_synced = false;
                    }
                }

                log::debug!(
                    "Configuration sync status: {}/{} nodes synced",
                    synced_nodes,
                    total_nodes
                );

                // Require majority of nodes to be synced for safety
                let majority_threshold = (total_nodes / 2) + 1;
                if synced_nodes >= majority_threshold {
                    if all_synced {
                        log::debug!(
                            "All {} nodes have synchronized configuration change {}",
                            total_nodes,
                            change_id
                        );
                    } else {
                        log::debug!(
                            "Majority ({}/{}) nodes have synchronized configuration change {}",
                            synced_nodes,
                            total_nodes,
                            change_id
                        );
                    }
                    return Ok(all_synced);
                } else {
                    log::debug!(
                        "Only {}/{} nodes synced, need majority ({})",
                        synced_nodes,
                        total_nodes,
                        majority_threshold
                    );
                    return Ok(false);
                }
            } else {
                // Single node cluster
                log::debug!(
                    "Single node cluster, configuration change {} is synced",
                    change_id
                );
                return Ok(true);
            }
        }

        log::debug!("Raft node not available, cannot check sync status");
        Ok(false)
    }

    /// Verify configuration consistency across all cluster nodes
    async fn verify_config_consistency(&self) -> RaftResult<()> {
        log::info!("Verifying configuration consistency across cluster");

        // Get current configuration from this node
        let local_config = {
            let config = self.config.read().await;
            config.clone()
        };

        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;

            // Check that membership is consistent between local config and Raft
            let raft_membership = metrics.membership_config.membership();
            let config_membership: BTreeSet<NodeId> =
                local_config.endpoints.keys().copied().collect();
            let raft_voters: BTreeSet<NodeId> = raft_membership.voter_ids().collect();
            let raft_learners: BTreeSet<NodeId> = raft_membership.learner_ids().collect();
            let all_raft_members: BTreeSet<NodeId> =
                raft_voters.union(&raft_learners).copied().collect();

            log::debug!("Local config members: {:?}", config_membership);
            log::debug!("Raft voters: {:?}", raft_voters);
            log::debug!("Raft learners: {:?}", raft_learners);

            // Verify that all Raft members have endpoints configured
            for &member_id in &all_raft_members {
                if !config_membership.contains(&member_id) {
                    log::warn!("Raft member {} not found in local configuration", member_id);
                    return Err(RaftError::configuration(format!(
                        "Configuration inconsistency: Raft member {} not in local config",
                        member_id
                    )));
                }
            }

            // Verify that all configured endpoints are either voters or learners
            for &config_member in &config_membership {
                if !all_raft_members.contains(&config_member) {
                    log::warn!(
                        "Local config member {} not found in Raft membership",
                        config_member
                    );
                    // This might be acceptable if the node is being added/removed
                    log::debug!(
                        "Config member {} not in Raft membership - may be in transition",
                        config_member
                    );
                }
            }

            // Check cluster health indicators
            if let Some(current_leader) = metrics.current_leader {
                if !all_raft_members.contains(&current_leader) {
                    return Err(RaftError::configuration(format!(
                        "Current leader {} not in cluster membership",
                        current_leader
                    )));
                }
                log::debug!("Current leader: {}", current_leader);
            } else {
                log::warn!("No current leader - cluster may be in election");
            }

            // Verify replication health
            if let Some(replication_map) = &metrics.replication {
                let mut healthy_followers = 0;
                let total_followers = replication_map.len();

                for (node_id, replication_state) in replication_map {
                    if let Some(matched_index) = replication_state.map(|state| state.index) {
                        let current_index = metrics.last_log_index.unwrap_or(0);
                        let lag = current_index.saturating_sub(matched_index);

                        if lag <= 10 {
                            // Allow reasonable lag
                            healthy_followers += 1;
                        } else {
                            log::warn!("Node {} has high replication lag: {}", node_id, lag);
                        }
                    }
                }

                log::debug!(
                    "Replication health: {}/{} followers healthy",
                    healthy_followers,
                    total_followers
                );

                // Ensure majority of followers are healthy
                if total_followers > 0 {
                    let healthy_ratio = healthy_followers as f64 / total_followers as f64;
                    if healthy_ratio < 0.5 {
                        log::warn!(
                            "Less than 50% of followers are healthy ({}/{})",
                            healthy_followers,
                            total_followers
                        );
                    }
                }
            }

            log::info!("Configuration consistency verification completed successfully");
        } else {
            log::warn!("Cannot verify configuration consistency - Raft node not available");
        }

        Ok(())
    }

    /// Handle configuration change failures and implement rollback
    pub async fn handle_config_change_failure(
        &self,
        change_id: String,
        error: RaftError,
    ) -> RaftResult<()> {
        log::error!(
            "Handling configuration change failure for {}: {}",
            change_id,
            error
        );

        // Mark the operation as failed
        {
            let mut operations = self.active_operations.write().await;
            if let Some(operation) = operations.iter_mut().find(|op| op.id == change_id) {
                operation.status = ConfigChangeStatus::Failed;
                operation.completed_at = Some(Utc::now());
                if let Some(ref mut result) = operation.result {
                    result.success = false;
                    result.error_message = Some(error.to_string());
                } else {
                    operation.result = Some(ConfigChangeResult {
                        success: false,
                        old_membership: BTreeSet::new(),
                        new_membership: BTreeSet::new(),
                        duration: Duration::ZERO,
                        error_message: Some(error.to_string()),
                    });
                }
            }
        }

        // Determine if rollback is safe and necessary
        let should_rollback = match &error {
            RaftError::NotLeader { .. } => {
                log::info!("Not attempting rollback for leadership change");
                false
            }
            RaftError::Timeout { .. } => {
                log::info!("Attempting rollback for timeout error");
                true
            }
            RaftError::Configuration { .. } => {
                log::info!("Attempting rollback for configuration error");
                true
            }
            _ => {
                log::info!("Attempting rollback for general error");
                true
            }
        };

        if should_rollback {
            // Attempt to rollback if possible and safe
            if let Err(rollback_error) = self.attempt_rollback(&change_id).await {
                log::error!(
                    "Failed to rollback configuration change {}: {}",
                    change_id,
                    rollback_error
                );
                // Don't propagate rollback errors - the original error is more important
            }
        }

        // Notify callbacks about the failure
        if let Some(operation) = {
            let operations = self.active_operations.read().await;
            operations.iter().find(|op| op.id == change_id).cloned()
        } {
            if let Some(result) = &operation.result {
                self.notify_callbacks(result).await;
            }
        }

        Ok(())
    }

    /// Attempt to rollback a failed configuration change
    async fn attempt_rollback(&self, change_id: &str) -> RaftResult<()> {
        log::warn!("Attempting rollback for configuration change {}", change_id);

        // Find the operation to rollback
        let operation = {
            let operations = self.active_operations.read().await;
            operations.iter().find(|op| op.id == change_id).cloned()
        };

        if let Some(operation) = operation {
            // Create reverse operation
            let rollback_request = self.create_rollback_request(&operation.request)?;

            // Execute rollback
            match self.execute_change_internal(&rollback_request).await {
                Ok(_) => {
                    log::info!(
                        "Successfully rolled back configuration change {}",
                        change_id
                    );

                    // Update operation status
                    let mut operations = self.active_operations.write().await;
                    if let Some(op) = operations.iter_mut().find(|op| op.id == change_id) {
                        op.status = ConfigChangeStatus::RolledBack;
                    }
                }
                Err(rollback_error) => {
                    log::error!(
                        "Failed to rollback configuration change {}: {}",
                        change_id,
                        rollback_error
                    );
                    return Err(rollback_error);
                }
            }
        }

        Ok(())
    }

    /// Create a rollback request for a failed configuration change
    fn create_rollback_request(
        &self,
        original_request: &ConfigChangeRequest,
    ) -> RaftResult<ConfigChangeRequest> {
        let rollback_change_type = match &original_request.change_type {
            ConfigChangeType::AddLearner { node_id, .. } => {
                ConfigChangeType::RemoveNode { node_id: *node_id }
            }
            ConfigChangeType::PromoteLearner { node_id } => {
                // Cannot directly demote, would need to remove and re-add as learner
                ConfigChangeType::RemoveNode { node_id: *node_id }
            }
            ConfigChangeType::AddVoter { node_id, .. } => {
                ConfigChangeType::RemoveNode { node_id: *node_id }
            }
            ConfigChangeType::RemoveNode { node_id: _ } => {
                // Cannot easily rollback node removal without endpoint info
                return Err(RaftError::configuration(
                    "Cannot rollback node removal without endpoint information",
                ));
            }
            ConfigChangeType::ReplaceNode {
                old_node_id: _,
                new_node_id: _,
                ..
            } => {
                // Reverse the replacement
                return Err(RaftError::configuration(
                    "Node replacement rollback not implemented",
                ));
            }
            ConfigChangeType::UpdateEndpoint { node_id: _, .. } => {
                // Would need original endpoint info
                return Err(RaftError::configuration(
                    "Endpoint update rollback not implemented",
                ));
            }
        };

        Ok(ConfigChangeRequest {
            change_type: rollback_change_type,
            force: true, // Force rollback
            timeout: original_request.timeout,
            reason: format!("Rollback of failed change: {}", original_request.reason),
        })
    }

    /// Ensure all nodes have consistent configuration state
    pub async fn ensure_config_consistency(&self) -> RaftResult<()> {
        log::info!("Ensuring configuration consistency across cluster");

        // Ensure we're the leader before attempting to enforce consistency
        self.ensure_leadership().await?;

        // Verify current configuration consistency
        self.verify_config_consistency().await?;

        // Check if there are any pending configuration changes that need to be synchronized
        let active_operations = self.get_active_operations().await;
        let pending_operations: Vec<_> = active_operations
            .iter()
            .filter(|op| op.status == ConfigChangeStatus::InProgress)
            .collect();

        if !pending_operations.is_empty() {
            log::warn!(
                "Found {} pending configuration operations",
                pending_operations.len()
            );

            for operation in pending_operations {
                log::info!("Checking status of pending operation: {}", operation.id);

                // Check if the operation has timed out
                let elapsed = Utc::now() - operation.started_at;
                if elapsed.to_std().unwrap_or(Duration::MAX) > Duration::from_secs(300) {
                    log::warn!(
                        "Operation {} has timed out, marking as failed",
                        operation.id
                    );
                    self.cancel_operation(&operation.id).await?;
                }
            }
        }

        // Perform additional consistency checks
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;

            // Check if cluster is in a stable state
            if matches!(metrics.state, openraft::ServerState::Candidate) {
                log::warn!("Cluster is in election state, consistency may be affected");
                return Err(RaftError::configuration(
                    "Cluster is not in stable state for consistency enforcement",
                ));
            }

            // Ensure all followers are reasonably caught up
            if let Some(replication_map) = &metrics.replication {
                let current_index = metrics.last_log_index.unwrap_or(0);
                let mut lagging_nodes = Vec::new();

                for (node_id, replication_state) in replication_map {
                    if let Some(matched_index) = replication_state.map(|state| state.index) {
                        let lag = current_index.saturating_sub(matched_index);
                        if lag > 50 {
                            // Significant lag threshold
                            lagging_nodes.push((*node_id, lag));
                        }
                    }
                }

                if !lagging_nodes.is_empty() {
                    log::warn!(
                        "Found nodes with significant replication lag: {:?}",
                        lagging_nodes
                    );
                    // In a production system, we might want to wait for them to catch up
                    // or take corrective action
                }
            }
        }

        log::info!("Configuration consistency enforcement completed");
        Ok(())
    }

    /// Force synchronization of configuration across all nodes
    /// This method should be used carefully, typically after network partition recovery
    pub async fn force_config_synchronization(&self) -> RaftResult<()> {
        log::warn!("Forcing configuration synchronization across cluster");

        // Ensure we're the leader
        self.ensure_leadership().await?;

        // Get current configuration
        let current_config = {
            let config = self.config.read().await;
            config.clone()
        };

        // Create a special synchronization entry
        let sync_entry = ConfigChangeEntry {
            change_id: format!("force_sync_{}", Uuid::new_v4()),
            change_type: ConfigChangeType::UpdateEndpoint {
                node_id: self.node_id,
                new_endpoint: current_config
                    .endpoints
                    .get(&self.node_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        crate::cluster_config::NodeEndpoint::new(
                            self.node_id,
                            "127.0.0.1".to_string(),
                            7379,
                        )
                    }),
            },
            requested_by: self.node_id,
            timestamp: Utc::now(),
            force: true,
            reason: "Force configuration synchronization".to_string(),
        };

        // Submit the synchronization entry through Raft
        self.submit_config_change_to_raft(sync_entry.clone())
            .await?;

        // Wait for synchronization
        self.wait_for_config_sync(sync_entry.change_id, Duration::from_secs(60))
            .await?;

        // Verify consistency
        self.verify_config_consistency().await?;

        log::info!("Forced configuration synchronization completed");
        Ok(())
    }

    /// Simulate membership change for dry run validation
    async fn simulate_membership_change(
        &self,
        request: &ConfigChangeRequest,
        current_membership: &BTreeSet<NodeId>,
    ) -> RaftResult<BTreeSet<NodeId>> {
        let mut new_membership = current_membership.clone();

        match &request.change_type {
            ConfigChangeType::AddLearner { node_id, .. }
            | ConfigChangeType::PromoteLearner { node_id }
            | ConfigChangeType::AddVoter { node_id, .. } => {
                new_membership.insert(*node_id);
            }
            ConfigChangeType::RemoveNode { node_id } => {
                new_membership.remove(node_id);
            }
            ConfigChangeType::ReplaceNode {
                old_node_id,
                new_node_id,
                ..
            } => {
                new_membership.remove(old_node_id);
                new_membership.insert(*new_node_id);
            }
            ConfigChangeType::UpdateEndpoint { .. } => {
                // No membership change for endpoint updates
            }
        }

        Ok(new_membership)
    }

    /// Enhanced safety mechanism: Ensure configuration changes are atomic and safe
    pub async fn execute_safe_config_change(
        &self,
        request: ConfigChangeRequest,
    ) -> RaftResult<ConfigChangeResult> {
        log::info!("Executing safe configuration change with enhanced safety mechanisms");

        // Step 1: Acquire exclusive lock to prevent concurrent changes
        let _change_lock = self.change_mutex.lock().await;
        log::debug!("Acquired configuration change mutex");

        // Step 2: Create safety checkpoint before starting
        let safety_checkpoint = self.create_safety_checkpoint().await?;
        log::info!("Created safety checkpoint: {}", safety_checkpoint.id);

        // Step 3: Validate pre-conditions with enhanced checks
        self.validate_change_preconditions(&request).await?;

        // Step 4: Pause ongoing operations that might conflict
        let paused_operations = self.pause_conflicting_operations(&request).await?;
        log::info!("Paused {} conflicting operations", paused_operations.len());

        let operation_id = format!("safe_config_change_{}", Uuid::new_v4());
        let _start_time = Utc::now();

        // Step 5: Execute change with rollback capability
        let result = match self.execute_atomic_change(&request, &operation_id).await {
            Ok(result) => {
                log::info!("Configuration change executed successfully");

                // Step 6: Verify post-change consistency
                if let Err(consistency_error) = self.verify_post_change_consistency(&request).await
                {
                    log::error!(
                        "Post-change consistency check failed: {}",
                        consistency_error
                    );

                    // Rollback the change
                    if let Err(rollback_error) =
                        self.rollback_from_checkpoint(&safety_checkpoint).await
                    {
                        log::error!(
                            "Critical: Failed to rollback from checkpoint: {}",
                            rollback_error
                        );
                        return Err(RaftError::configuration(format!(
                            "Configuration change succeeded but consistency check failed, and rollback failed: {}",
                            rollback_error
                        )));
                    }

                    return Err(consistency_error);
                }

                result
            }
            Err(e) => {
                log::error!("Configuration change failed: {}", e);

                // Attempt rollback from checkpoint
                if let Err(rollback_error) = self.rollback_from_checkpoint(&safety_checkpoint).await
                {
                    log::error!("Failed to rollback from checkpoint: {}", rollback_error);
                }

                return Err(e);
            }
        };

        // Step 7: Resume paused operations
        self.resume_operations(paused_operations).await?;
        log::info!("Resumed all paused operations");

        // Step 8: Clean up checkpoint
        self.cleanup_checkpoint(&safety_checkpoint).await?;
        log::info!("Cleaned up safety checkpoint");

        Ok(result)
    }

    /// Create a safety checkpoint before configuration changes
    async fn create_safety_checkpoint(&self) -> RaftResult<SafetyCheckpoint> {
        let checkpoint_id = format!("checkpoint_{}", Uuid::new_v4());

        // Capture current state
        let current_config = {
            let config = self.config.read().await;
            config.clone()
        };

        let current_membership = self.get_current_membership().await?;

        let raft_state = if let Some(raft_node) = &self.raft_node {
            Some(raft_node.get_metrics().await?)
        } else {
            None
        };

        let checkpoint = SafetyCheckpoint {
            id: checkpoint_id,
            timestamp: Utc::now(),
            config_snapshot: current_config,
            membership_snapshot: current_membership,
            raft_metrics_snapshot: raft_state,
        };

        log::debug!(
            "Created safety checkpoint with {} members",
            checkpoint.membership_snapshot.len()
        );

        Ok(checkpoint)
    }

    /// Validate enhanced pre-conditions for configuration changes
    async fn validate_change_preconditions(&self, request: &ConfigChangeRequest) -> RaftResult<()> {
        log::debug!("Validating enhanced pre-conditions for configuration change");

        // Standard validation
        self.validate_change(request).await?;

        // Enhanced safety checks

        // 1. Check for ongoing critical operations
        if self.has_critical_operations_in_progress().await? {
            return Err(RaftError::configuration(
                "Cannot perform configuration change while critical operations are in progress",
            ));
        }

        // 2. Verify cluster stability
        self.verify_cluster_stability().await?;

        // 3. Check resource availability
        self.check_resource_availability(request).await?;

        // 4. Validate timing constraints
        self.validate_timing_constraints(request).await?;

        log::debug!("All enhanced pre-conditions validated successfully");
        Ok(())
    }

    /// Check if there are critical operations in progress that would conflict
    async fn has_critical_operations_in_progress(&self) -> RaftResult<bool> {
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;

            // Check if there are pending client requests
            let current_index = metrics.last_log_index.unwrap_or(0);
            let applied_index = metrics.last_applied.map(|log_id| log_id.index).unwrap_or(0);

            // If there's a significant backlog, consider it critical
            if current_index > applied_index + 100 {
                log::warn!(
                    "High pending operation backlog: {} pending",
                    current_index - applied_index
                );
                return Ok(true);
            }

            // Check if there are active configuration changes
            let active_ops = self.active_operations.read().await;
            let in_progress_count = active_ops
                .iter()
                .filter(|op| op.status == ConfigChangeStatus::InProgress)
                .count();

            if in_progress_count > 0 {
                log::warn!(
                    "Found {} configuration changes in progress",
                    in_progress_count
                );
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Verify cluster stability before configuration changes
    async fn verify_cluster_stability(&self) -> RaftResult<()> {
        let raft_node = self.raft_node.as_ref().ok_or_else(|| {
            RaftError::configuration("No Raft node available for cluster stability verification")
        })?;
        let metrics = raft_node.get_metrics().await?;

        // Check leadership stability
        if !matches!(metrics.state, openraft::ServerState::Leader) {
            return Err(RaftError::configuration(
                "Node is not the leader, cannot perform configuration changes",
            ));
        }

            // Check if leadership is recent and stable
            // This would require additional metrics in a real implementation

            // Check follower health
            if let Some(replication_map) = &metrics.replication {
                let total_followers = replication_map.len();
                let healthy_followers = replication_map
                    .iter()
                    .filter(|(_, state)| {
                        state
                            .map(|s| {
                                let current_index = metrics.last_log_index.unwrap_or(0);
                                let lag = current_index.saturating_sub(s.index);
                                lag <= 10 // Healthy if lag is small
                            })
                            .unwrap_or(false)
                    })
                    .count();

                let health_ratio = if total_followers > 0 {
                    healthy_followers as f64 / total_followers as f64
                } else {
                    1.0
                };

                if health_ratio < 0.7 {
                    return Err(RaftError::configuration(format!(
                        "Cluster health insufficient for configuration changes: {:.1}% healthy",
                        health_ratio * 100.0
                    )));
                }
            }

        Ok(())
    }

    /// Check resource availability for the configuration change
    async fn check_resource_availability(&self, request: &ConfigChangeRequest) -> RaftResult<()> {
        match &request.change_type {
            ConfigChangeType::AddLearner { endpoint, .. }
            | ConfigChangeType::AddVoter { endpoint, .. } => {
                // Verify the endpoint is reachable
                if let Err(e) = endpoint.socket_addr() {
                    return Err(RaftError::configuration(format!(
                        "Invalid endpoint address: {}",
                        e
                    )));
                }

                // In a real implementation, we might ping the endpoint
                log::debug!("Resource check passed for endpoint: {}", endpoint.address());
            }
            _ => {
                // Other operations don't require additional resources
            }
        }

        Ok(())
    }

    /// Validate timing constraints for the configuration change
    async fn validate_timing_constraints(&self, request: &ConfigChangeRequest) -> RaftResult<()> {
        // Check if enough time has passed since the last configuration change
        let operations = self.active_operations.read().await;
        if let Some(last_completed) = operations
            .iter()
            .filter(|op| op.status == ConfigChangeStatus::Completed)
            .max_by_key(|op| op.completed_at.unwrap_or(op.started_at))
        {
            if let Some(completed_at) = last_completed.completed_at {
                let time_since_last = Utc::now() - completed_at;
                let min_interval = Duration::from_secs(30); // Minimum 30 seconds between changes

                if time_since_last.to_std().unwrap_or(Duration::ZERO) < min_interval {
                    if !request.force {
                        return Err(RaftError::configuration(format!(
                            "Configuration changes must be at least {:?} apart (last change: {:?} ago)",
                            min_interval,
                            time_since_last.to_std().unwrap_or(Duration::ZERO)
                        )));
                    } else {
                        log::warn!("Forcing configuration change despite timing constraints");
                    }
                }
            }
        }

        Ok(())
    }

    /// Pause operations that might conflict with the configuration change
    async fn pause_conflicting_operations(
        &self,
        request: &ConfigChangeRequest,
    ) -> RaftResult<Vec<String>> {
        let paused_operations = Vec::new();

        // In a real implementation, this would pause specific types of operations
        // that might conflict with configuration changes, such as:
        // - Snapshot operations
        // - Log compaction
        // - Other administrative operations

        match &request.change_type {
            ConfigChangeType::RemoveNode { node_id } => {
                log::info!("Pausing operations that might involve node {}", node_id);
                // Pause operations targeting the node being removed
            }
            ConfigChangeType::ReplaceNode { old_node_id, .. } => {
                log::info!("Pausing operations that might involve node {}", old_node_id);
                // Pause operations targeting the node being replaced
            }
            _ => {
                log::debug!("No specific operations need to be paused for this change type");
            }
        }

        Ok(paused_operations)
    }

    /// Execute configuration change atomically with enhanced error handling
    async fn execute_atomic_change(
        &self,
        request: &ConfigChangeRequest,
        operation_id: &str,
    ) -> RaftResult<ConfigChangeResult> {
        log::info!("Executing atomic configuration change: {}", operation_id);

        // Create operation tracker
        let mut operation = ConfigChangeOperation {
            id: operation_id.to_string(),
            request: request.clone(),
            status: ConfigChangeStatus::Preparing,
            started_at: Utc::now(),
            completed_at: None,
            result: None,
        };

        // Add to active operations
        {
            let mut operations = self.active_operations.write().await;
            operations.push(operation.clone());
        }

        // Execute with enhanced error handling
        operation.status = ConfigChangeStatus::InProgress;

        // Update status
        {
            let mut operations = self.active_operations.write().await;
            if let Some(op) = operations.iter_mut().find(|op| op.id == operation_id) {
                op.status = ConfigChangeStatus::InProgress;
            }
        }

        let start_time = Utc::now();

        // Execute the change with timeout and cancellation support
        let result = tokio::time::timeout(
            request.timeout,
            self.execute_change_with_monitoring(request, operation_id),
        )
        .await;

        let change_result = match result {
            Ok(Ok(result)) => {
                operation.status = ConfigChangeStatus::Completed;
                operation.result = Some(result.clone());
                log::info!("Atomic configuration change completed successfully");
                result
            }
            Ok(Err(e)) => {
                operation.status = ConfigChangeStatus::Failed;
                let error_result = ConfigChangeResult {
                    success: false,
                    old_membership: self.get_current_membership().await.unwrap_or_default(),
                    new_membership: BTreeSet::new(),
                    duration: (Utc::now() - start_time).to_std().unwrap_or(Duration::ZERO),
                    error_message: Some(e.to_string()),
                };
                operation.result = Some(error_result.clone());
                log::error!("Atomic configuration change failed: {}", e);
                return Err(e);
            }
            Err(_) => {
                operation.status = ConfigChangeStatus::Failed;
                let timeout_error = RaftError::timeout(format!(
                    "Configuration change timed out after {:?}",
                    request.timeout
                ));
                let error_result = ConfigChangeResult {
                    success: false,
                    old_membership: self.get_current_membership().await.unwrap_or_default(),
                    new_membership: BTreeSet::new(),
                    duration: request.timeout,
                    error_message: Some(timeout_error.to_string()),
                };
                operation.result = Some(error_result.clone());
                log::error!("Atomic configuration change timed out");
                return Err(timeout_error);
            }
        };

        operation.completed_at = Some(Utc::now());

        // Update operation tracker
        {
            let mut operations = self.active_operations.write().await;
            if let Some(op) = operations.iter_mut().find(|op| op.id == operation_id) {
                *op = operation;
            }
        }

        Ok(change_result)
    }

    /// Execute change with continuous monitoring for safety
    async fn execute_change_with_monitoring(
        &self,
        request: &ConfigChangeRequest,
        operation_id: &str,
    ) -> RaftResult<ConfigChangeResult> {
        log::debug!("Executing change with monitoring: {}", operation_id);

        // Monitor cluster health during the change
        let health_monitor = tokio::spawn({
            let raft_node = self.raft_node.clone();
            let operation_id = operation_id.to_string();

            async move {
                let mut interval = tokio::time::interval(Duration::from_millis(500));

                loop {
                    interval.tick().await;

                    if let Some(ref raft_node) = raft_node {
                        match raft_node.get_metrics().await {
                            Ok(metrics) => {
                                // Check if we're still the leader
                                if !matches!(metrics.state, openraft::ServerState::Leader) {
                                    log::error!(
                                        "Lost leadership during configuration change {}",
                                        operation_id
                                    );
                                    break;
                                }

                                // Check cluster health
                                if let Some(replication_map) = &metrics.replication {
                                    let unhealthy_count = replication_map
                                        .iter()
                                        .filter(|(_, state)| state.is_none())
                                        .count();

                                    if unhealthy_count > replication_map.len() / 2 {
                                        log::warn!(
                                            "Majority of followers unhealthy during change {}",
                                            operation_id
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to get metrics during monitoring: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Execute the actual change
        let change_result = self.execute_change_internal(request).await;

        // Stop monitoring
        health_monitor.abort();

        change_result
    }

    /// Verify consistency after configuration change
    async fn verify_post_change_consistency(
        &self,
        request: &ConfigChangeRequest,
    ) -> RaftResult<()> {
        log::info!("Verifying post-change consistency");

        // Wait a bit for changes to propagate
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify configuration consistency
        self.verify_config_consistency().await?;

        // Verify membership consistency
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;
            let raft_membership = metrics.membership_config.membership();

            match &request.change_type {
                ConfigChangeType::AddLearner { node_id, .. } => {
                    if !raft_membership.learner_ids().any(|id| id == *node_id) {
                        return Err(RaftError::configuration(format!(
                            "Node {} not found in learners after add operation",
                            node_id
                        )));
                    }
                }
                ConfigChangeType::PromoteLearner { node_id }
                | ConfigChangeType::AddVoter { node_id, .. } => {
                    if !raft_membership.voter_ids().any(|id| id == *node_id) {
                        return Err(RaftError::configuration(format!(
                            "Node {} not found in voters after promotion/add operation",
                            node_id
                        )));
                    }
                }
                ConfigChangeType::RemoveNode { node_id } => {
                    if raft_membership.voter_ids().any(|id| id == *node_id)
                        || raft_membership.learner_ids().any(|id| id == *node_id)
                    {
                        return Err(RaftError::configuration(format!(
                            "Node {} still found in membership after remove operation",
                            node_id
                        )));
                    }
                }
                _ => {
                    // Other operations have different consistency requirements
                }
            }
        }

        log::info!("Post-change consistency verification passed");
        Ok(())
    }

    /// Resume operations that were paused during configuration change
    async fn resume_operations(&self, paused_operations: Vec<String>) -> RaftResult<()> {
        log::info!("Resuming {} paused operations", paused_operations.len());

        for operation_id in paused_operations {
            log::debug!("Resuming operation: {}", operation_id);
            // In a real implementation, this would resume specific operations
        }

        Ok(())
    }

    /// Rollback configuration from safety checkpoint
    async fn rollback_from_checkpoint(&self, checkpoint: &SafetyCheckpoint) -> RaftResult<()> {
        log::warn!(
            "Rolling back configuration from checkpoint: {}",
            checkpoint.id
        );

        // Restore configuration
        {
            let mut config = self.config.write().await;
            *config = checkpoint.config_snapshot.clone();
        }

        // Restore Raft membership if possible
        if let Some(raft_node) = &self.raft_node {
            // This is a simplified rollback - in practice, this would be more complex
            let target_membership = checkpoint.membership_snapshot.clone();

            if let Err(e) = raft_node.change_membership(target_membership).await {
                log::error!("Failed to rollback Raft membership: {}", e);
                return Err(RaftError::configuration(format!(
                    "Failed to rollback Raft membership: {}",
                    e
                )));
            }
        }

        log::info!("Successfully rolled back configuration from checkpoint");
        Ok(())
    }

    /// Clean up safety checkpoint
    async fn cleanup_checkpoint(&self, checkpoint: &SafetyCheckpoint) -> RaftResult<()> {
        log::debug!("Cleaning up safety checkpoint: {}", checkpoint.id);

        // In a real implementation, this might clean up checkpoint files or database entries

        Ok(())
    }

    /// Handle exceptions during configuration changes with enhanced recovery
    pub async fn handle_config_change_exception(
        &self,
        operation_id: String,
        exception: RaftError,
        checkpoint: Option<SafetyCheckpoint>,
    ) -> RaftResult<()> {
        log::error!(
            "Handling configuration change exception for {}: {}",
            operation_id,
            exception
        );

        // Mark operation as failed
        {
            let mut operations = self.active_operations.write().await;
            if let Some(operation) = operations.iter_mut().find(|op| op.id == operation_id) {
                operation.status = ConfigChangeStatus::Failed;
                operation.completed_at = Some(Utc::now());

                let error_result = ConfigChangeResult {
                    success: false,
                    old_membership: self.get_current_membership().await.unwrap_or_default(),
                    new_membership: BTreeSet::new(),
                    duration: (Utc::now() - operation.started_at)
                        .to_std()
                        .unwrap_or(Duration::ZERO),
                    error_message: Some(exception.to_string()),
                };
                operation.result = Some(error_result);
            }
        }

        // Attempt recovery based on exception type
        match &exception {
            RaftError::NotLeader { .. } => {
                log::info!("Leadership lost during configuration change - no rollback needed");
            }
            RaftError::Timeout { .. } => {
                log::warn!("Configuration change timed out - attempting rollback");
                if let Some(checkpoint) = checkpoint {
                    self.rollback_from_checkpoint(&checkpoint).await?;
                }
            }
            RaftError::Configuration { .. } => {
                log::warn!("Configuration error - attempting rollback");
                if let Some(checkpoint) = checkpoint {
                    self.rollback_from_checkpoint(&checkpoint).await?;
                }
            }
            _ => {
                log::warn!("General error during configuration change - attempting rollback");
                if let Some(checkpoint) = checkpoint {
                    self.rollback_from_checkpoint(&checkpoint).await?;
                }
            }
        }

        // Ensure cluster is in a consistent state
        if let Err(consistency_error) = self.ensure_config_consistency().await {
            log::error!(
                "Failed to ensure consistency after exception handling: {}",
                consistency_error
            );
        }

        Ok(())
    }
}

/// Safety checkpoint for configuration changes
#[derive(Debug, Clone)]
pub struct SafetyCheckpoint {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub config_snapshot: ClusterConfiguration,
    pub membership_snapshot: BTreeSet<NodeId>,
    pub raft_metrics_snapshot: Option<openraft::RaftMetrics<NodeId, BasicNode>>,
}

#[cfg(test)]
mod tests;
