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

use crate::cluster_config::{NodeEndpoint, ClusterConfiguration};
use crate::discovery::{HealthMonitor, NodeStatus};
use crate::error::{RaftError, RaftResult};
use crate::types::{NodeId, BasicNode};
use crate::node::RaftNodeInterface;
use openraft::Membership;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use tokio::sync::{RwLock, Mutex};
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
    PromoteLearner {
        node_id: NodeId,
    },
    /// Add a node directly as a voting member (unsafe)
    AddVoter {
        node_id: NodeId,
        endpoint: NodeEndpoint,
    },
    /// Remove a node from the cluster
    RemoveNode {
        node_id: NodeId,
    },
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
                self.validate_add_learner(*node_id, endpoint, &config).await?;
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
            ConfigChangeType::ReplaceNode { old_node_id, new_node_id, new_endpoint } => {
                self.validate_replace_node(*old_node_id, *new_node_id, new_endpoint, &config).await?;
            }
            ConfigChangeType::UpdateEndpoint { node_id, new_endpoint } => {
                self.validate_update_endpoint(*node_id, new_endpoint, &config).await?;
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
                "Cannot remove the last node from cluster"
            ));
        }

        // Don't allow removing ourselves if we're the only node
        if node_id == self.node_id && config.endpoints.len() == 1 {
            return Err(RaftError::configuration(
                "Cannot remove self when it's the only node"
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
                    leader_id: metrics.current_leader 
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
            ConfigChangeType::ReplaceNode { old_node_id, new_node_id, new_endpoint } => {
                self.execute_replace_node(*old_node_id, *new_node_id, new_endpoint.clone()).await?;
            }
            ConfigChangeType::UpdateEndpoint { node_id, new_endpoint } => {
                self.execute_update_endpoint(*node_id, new_endpoint.clone()).await?;
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
            log::info!("Added learner node {} to Raft cluster at {}", node_id, endpoint.address());
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
            log::info!("Promoted learner node {} to voting member via Raft", node_id);
        } else {
            log::warn!("No Raft node reference available, cannot promote learner {}", node_id);
            return Err(RaftError::configuration("Raft node not available for membership change"));
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
            log::info!("Added voter node {} to Raft cluster at {}", node_id, endpoint.address());
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
                return Err(RaftError::configuration("Cannot remove the last node from cluster"));
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
            log::info!("New node {} added as learner, waiting for catchup", new_node_id);
            
            // Get current membership, replace old with new
            let current_membership = self.get_current_membership().await?;
            let mut new_membership = current_membership;
            new_membership.remove(&old_node_id);
            new_membership.insert(new_node_id);
            
            raft_node.change_membership(new_membership).await?;
            log::info!("Replaced node {} with node {} in Raft cluster", old_node_id, new_node_id);
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
        log::warn!("Handling failure of node {} during configuration changes", failed_node_id);

        // Check if there are any active operations involving this node
        let operations = self.active_operations.read().await;
        let affected_operations: Vec<_> = operations
            .iter()
            .filter(|op| {
                op.status == ConfigChangeStatus::InProgress && 
                self.operation_involves_node(&op.request, failed_node_id)
            })
            .cloned()
            .collect();

        drop(operations);

        // Handle affected operations
        for operation in affected_operations {
            log::warn!("Configuration change operation {} affected by node {} failure", 
                     operation.id, failed_node_id);
            
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
            ConfigChangeType::ReplaceNode { old_node_id, new_node_id, .. } => {
                *old_node_id == node_id || *new_node_id == node_id
            }
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
                    "No leader available, cluster is not stable for configuration changes"
                ));
            }
            
            // Check if the cluster is in a healthy state
            if matches!(metrics.state, openraft::ServerState::Candidate) {
                return Err(RaftError::configuration(
                    "Cluster is in election state, wait for stable leadership"
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
                        "Cannot remove node from single-node cluster"
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
    pub async fn dry_run_change(&self, request: &ConfigChangeRequest) -> RaftResult<ConfigChangeResult> {
        log::info!("Performing dry run for configuration change: {:?}", request.change_type);
        
        // Validate leadership
        self.ensure_leadership().await?;
        
        // Validate cluster health
        self.validate_cluster_health().await?;
        
        // Validate the change itself
        self.validate_change(request).await?;
        
        // Validate quorum safety
        self.validate_quorum_safety(request).await?;
        
        let old_membership = self.get_current_membership().await?;
        let new_membership = self.simulate_membership_change(request, &old_membership).await?;
        
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
        log::info!("Synchronizing configuration change across cluster: {:?}", request.change_type);
        
        // Ensure we're the leader before attempting synchronization
        self.ensure_leadership().await?;
        
        // Validate the change before synchronization
        self.validate_change(&request).await?;
        
        // Create a configuration change entry for Raft log
        let config_change_entry = ConfigChangeEntry {
            change_id: Uuid::new_v4().to_string(),
            change_type: request.change_type.clone(),
            requested_by: self.node_id,
            timestamp: Utc::now(),
            force: request.force,
            reason: request.reason.clone(),
        };
        
        // Submit the configuration change through Raft protocol
        let sync_result = self.submit_config_change_to_raft(config_change_entry).await?;
        
        // Wait for the change to be committed and applied on all nodes
        self.wait_for_config_sync(sync_result.change_id.clone(), request.timeout).await?;
        
        // Verify that all nodes have the same configuration
        self.verify_config_consistency().await?;
        
        log::info!("Configuration change synchronized successfully across cluster");
        Ok(sync_result)
    }

    /// Submit configuration change to Raft log for replication
    async fn submit_config_change_to_raft(
        &self,
        config_change: ConfigChangeEntry,
    ) -> RaftResult<ConfigChangeResult> {
        if let Some(raft_node) = &self.raft_node {
            // Create a client request for the configuration change
            let client_request = crate::types::ClientRequest {
                id: crate::types::RequestId::new(),
                command: crate::types::RedisCommand::new(
                    "CONFIG_CHANGE".to_string(),
                    vec![
                        serde_json::to_vec(&config_change)
                            .map_err(|e| RaftError::serialization(e.to_string()))?
                            .into()
                    ]
                ),
                consistency_level: crate::types::ConsistencyLevel::Linearizable,
            };
            
            // Submit through Raft for replication
            let response = raft_node.propose(client_request).await?;
            
            match response.result {
                Ok(_) => {
                    let old_membership = self.get_current_membership().await?;
                    let new_membership = self.simulate_membership_change(
                        &ConfigChangeRequest {
                            change_type: config_change.change_type,
                            force: config_change.force,
                            timeout: Duration::from_secs(30),
                            reason: config_change.reason,
                        },
                        &old_membership
                    ).await?;
                    
                    Ok(ConfigChangeResult {
                        success: true,
                        old_membership,
                        new_membership,
                        duration: Duration::ZERO,
                        error_message: None,
                    })
                }
                Err(error) => Err(RaftError::consensus(format!(
                    "Failed to replicate configuration change: {}", error
                )))
            }
        } else {
            Err(RaftError::configuration("Raft node not available for configuration synchronization"))
        }
    }

    /// Wait for configuration change to be synchronized across all nodes
    async fn wait_for_config_sync(
        &self,
        change_id: String,
        timeout: Duration,
    ) -> RaftResult<()> {
        log::info!("Waiting for configuration change {} to sync across cluster", change_id);
        
        let start_time = std::time::Instant::now();
        let check_interval = Duration::from_millis(500);
        
        while start_time.elapsed() < timeout {
            // Check if all nodes have applied the configuration change
            if self.check_all_nodes_synced(&change_id).await? {
                log::info!("Configuration change {} synchronized across all nodes", change_id);
                return Ok(());
            }
            
            // Check if we're still the leader
            if let Some(raft_node) = &self.raft_node {
                let metrics = raft_node.get_metrics().await?;
                if !matches!(metrics.state, openraft::ServerState::Leader) {
                    return Err(RaftError::NotLeader { 
                        leader_id: metrics.current_leader 
                    });
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
            
            // Check if the change has been committed
            if let Some(last_applied) = metrics.last_applied {
                // In a real implementation, we would track configuration changes
                // and verify they've been applied on all nodes
                // For now, we check if the log has been replicated to all followers
                
                if let Some(replication_map) = &metrics.replication {
                    let current_log_index = metrics.last_log_index.unwrap_or(0);
                    
                    // Check if all followers are caught up
                    for (node_id, replication_state) in replication_map {
                        if let Some(matched_index) = replication_state.map(|state| state.index) {
                            let lag = current_log_index.saturating_sub(matched_index);
                            if lag > 5 { // Allow small lag
                                log::debug!("Node {} still syncing, lag: {}", node_id, lag);
                                return Ok(false);
                            }
                        } else {
                            log::debug!("Node {} replication state unknown", node_id);
                            return Ok(false);
                        }
                    }
                }
                
                // All nodes appear to be caught up
                return Ok(true);
            }
        }
        
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
        
        // In a real implementation, we would query other nodes to verify
        // their configurations match. For now, we verify through Raft metrics
        if let Some(raft_node) = &self.raft_node {
            let metrics = raft_node.get_metrics().await?;
            
            // Check that membership is consistent
            let raft_membership = metrics.membership_config.membership();
            let config_membership: BTreeSet<NodeId> = local_config.endpoints.keys().copied().collect();
            let raft_voters: BTreeSet<NodeId> = raft_membership.voter_ids().collect();
            
            // Allow for learners not being in the voting set
            if !raft_voters.is_subset(&config_membership) {
                return Err(RaftError::configuration(format!(
                    "Configuration inconsistency detected: Raft voters {:?} not subset of config members {:?}",
                    raft_voters, config_membership
                )));
            }
            
            log::info!("Configuration consistency verified");
        }
        
        Ok(())
    }

    /// Handle configuration change failures and implement rollback
    pub async fn handle_config_change_failure(
        &self,
        change_id: String,
        error: RaftError,
    ) -> RaftResult<()> {
        log::error!("Handling configuration change failure for {}: {}", change_id, error);
        
        // Mark the operation as failed
        {
            let mut operations = self.active_operations.write().await;
            if let Some(operation) = operations.iter_mut().find(|op| op.id == change_id) {
                operation.status = ConfigChangeStatus::Failed;
                operation.completed_at = Some(Utc::now());
                if let Some(ref mut result) = operation.result {
                    result.success = false;
                    result.error_message = Some(error.to_string());
                }
            }
        }
        
        // Attempt to rollback if possible
        self.attempt_rollback(&change_id).await?;
        
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
                    log::info!("Successfully rolled back configuration change {}", change_id);
                    
                    // Update operation status
                    let mut operations = self.active_operations.write().await;
                    if let Some(op) = operations.iter_mut().find(|op| op.id == change_id) {
                        op.status = ConfigChangeStatus::RolledBack;
                    }
                }
                Err(rollback_error) => {
                    log::error!("Failed to rollback configuration change {}: {}", 
                              change_id, rollback_error);
                    return Err(rollback_error);
                }
            }
        }
        
        Ok(())
    }

    /// Create a rollback request for a failed configuration change
    fn create_rollback_request(&self, original_request: &ConfigChangeRequest) -> RaftResult<ConfigChangeRequest> {
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
            ConfigChangeType::RemoveNode { node_id } => {
                // Cannot easily rollback node removal without endpoint info
                return Err(RaftError::configuration(
                    "Cannot rollback node removal without endpoint information"
                ));
            }
            ConfigChangeType::ReplaceNode { old_node_id, new_node_id, .. } => {
                // Reverse the replacement
                return Err(RaftError::configuration(
                    "Node replacement rollback not implemented"
                ));
            }
            ConfigChangeType::UpdateEndpoint { node_id, .. } => {
                // Would need original endpoint info
                return Err(RaftError::configuration(
                    "Endpoint update rollback not implemented"
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
        
        // This is called periodically or after network partition recovery
        self.verify_config_consistency().await?;
        
        // If inconsistencies are found, attempt to reconcile
        // In a real implementation, this might involve:
        // 1. Querying all nodes for their configurations
        // 2. Determining the authoritative configuration (usually from leader)
        // 3. Pushing updates to inconsistent nodes
        
        log::info!("Configuration consistency check completed");
        Ok(())
    }

    /// Simulate what the membership would look like after a change
    async fn simulate_membership_change(
        &self,
        request: &ConfigChangeRequest,
        current_membership: &BTreeSet<NodeId>,
    ) -> RaftResult<BTreeSet<NodeId>> {
        let mut new_membership = current_membership.clone();
        
        match &request.change_type {
            ConfigChangeType::AddLearner { .. } => {
                // Learners don't change voting membership
            }
            ConfigChangeType::PromoteLearner { node_id } => {
                new_membership.insert(*node_id);
            }
            ConfigChangeType::AddVoter { node_id, .. } => {
                new_membership.insert(*node_id);
            }
            ConfigChangeType::RemoveNode { node_id } => {
                new_membership.remove(node_id);
            }
            ConfigChangeType::ReplaceNode { old_node_id, new_node_id, .. } => {
                new_membership.remove(old_node_id);
                new_membership.insert(*new_node_id);
            }
            ConfigChangeType::UpdateEndpoint { .. } => {
                // Endpoint updates don't change membership
            }
        }
        
        Ok(new_membership)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_config::ClusterConfiguration;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn test_validate_add_learner() {
        let config = Arc::new(RwLock::new(ClusterConfiguration::default()));
        let manager = ConfigChangeManager::new(1, config, None);

        let endpoint = NodeEndpoint::new(2, "127.0.0.1".to_string(), 8080);
        let request = ConfigChangeRequest {
            change_type: ConfigChangeType::AddLearner {
                node_id: 2,
                endpoint: endpoint.clone(),
            },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test".to_string(),
        };

        assert!(manager.validate_change(&request).await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_remove_last_node() {
        let mut cluster_config = ClusterConfiguration::default();
        cluster_config.endpoints.insert(1, NodeEndpoint::new(1, "127.0.0.1".to_string(), 8080));
        
        let config = Arc::new(RwLock::new(cluster_config));
        let manager = ConfigChangeManager::new(1, config, None);

        let request = ConfigChangeRequest {
            change_type: ConfigChangeType::RemoveNode { node_id: 1 },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test".to_string(),
        };

        assert!(manager.validate_change(&request).await.is_err());
    }

    #[tokio::test]
    async fn test_operation_tracking() {
        let config = Arc::new(RwLock::new(ClusterConfiguration::default()));
        let manager = ConfigChangeManager::new(1, config, None);

        let endpoint = NodeEndpoint::new(2, "127.0.0.1".to_string(), 8080);
        let request = ConfigChangeRequest {
            change_type: ConfigChangeType::AddLearner {
                node_id: 2,
                endpoint: endpoint.clone(),
            },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test".to_string(),
        };

        let result = manager.execute_change(request).await;
        assert!(result.is_ok());

        let operations = manager.get_active_operations().await;
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].status, ConfigChangeStatus::Completed);
    }
}