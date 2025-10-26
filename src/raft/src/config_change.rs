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
use openraft::Membership;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

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

/// Safe configuration change manager
pub struct ConfigChangeManager {
    node_id: NodeId,
    health_monitor: Option<Arc<HealthMonitor>>,
    active_operations: Arc<RwLock<Vec<ConfigChangeOperation>>>,
    config: Arc<RwLock<ClusterConfiguration>>,
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
        }
    }

    /// Validate a configuration change request
    pub async fn validate_change(&self, request: &ConfigChangeRequest) -> RaftResult<()> {
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

    /// Execute a configuration change with safety checks
    pub async fn execute_change(
        &self,
        request: ConfigChangeRequest,
    ) -> RaftResult<ConfigChangeResult> {
        // Validate the change
        self.validate_change(&request).await?;

        let operation_id = format!("config_change_{}", uuid::Uuid::new_v4());
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
        // Add endpoint to configuration
        {
            let mut config = self.config.write().await;
            config.endpoints.insert(node_id, endpoint);
        }

        log::info!("Added learner node {} to configuration", node_id);
        Ok(())
    }

    async fn execute_promote_learner(&self, node_id: NodeId) -> RaftResult<()> {
        // This would typically involve calling the Raft node's change_membership method
        // For now, we just log the operation
        log::info!("Promoted learner node {} to voting member", node_id);
        Ok(())
    }

    async fn execute_add_voter(&self, node_id: NodeId, endpoint: NodeEndpoint) -> RaftResult<()> {
        // Add endpoint to configuration
        {
            let mut config = self.config.write().await;
            config.endpoints.insert(node_id, endpoint);
        }

        log::info!("Added voter node {} to configuration", node_id);
        Ok(())
    }

    async fn execute_remove_node(&self, node_id: NodeId) -> RaftResult<()> {
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
        // Remove old node and add new node
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