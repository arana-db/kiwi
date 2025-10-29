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

//! Node discovery and health monitoring for Raft cluster

use crate::cluster_config::{NodeEndpoint, ClusterConfiguration};
use crate::error::{RaftError, RaftResult};
use crate::types::{NodeId, ClusterHealth, Term, LogIndex};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{interval, timeout};

/// Node health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is healthy and reachable
    Healthy,
    /// Node is unreachable but may recover
    Unreachable,
    /// Node is suspected to be failed
    Suspected,
    /// Node is confirmed failed
    Failed,
    /// Node status is unknown
    Unknown,
}

impl Default for NodeStatus {
    fn default() -> Self {
        NodeStatus::Unknown
    }
}

/// Health check result for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub node_id: NodeId,
    pub status: NodeStatus,
    pub last_seen: SystemTime,
    pub response_time: Option<Duration>,
    pub error_message: Option<String>,
    pub consecutive_failures: u32,
    pub total_checks: u64,
    pub successful_checks: u64,
}

impl HealthCheckResult {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            status: NodeStatus::Unknown,
            last_seen: UNIX_EPOCH,
            response_time: None,
            error_message: None,
            consecutive_failures: 0,
            total_checks: 0,
            successful_checks: 0,
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_checks == 0 {
            0.0
        } else {
            self.successful_checks as f64 / self.total_checks as f64
        }
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.status, NodeStatus::Healthy)
    }

    pub fn is_failed(&self) -> bool {
        matches!(self.status, NodeStatus::Failed | NodeStatus::Suspected)
    }
}

/// Configuration for health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMonitorConfig {
    /// Interval between health checks
    pub check_interval: Duration,
    /// Timeout for individual health checks
    pub check_timeout: Duration,
    /// Number of consecutive failures before marking as suspected
    pub failure_threshold: u32,
    /// Number of consecutive failures before marking as failed
    pub critical_threshold: u32,
    /// Time to wait before retrying a failed node
    pub retry_interval: Duration,
    /// Maximum time to keep trying a failed node
    pub max_retry_duration: Duration,
}

impl Default for HealthMonitorConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            check_timeout: Duration::from_secs(3),
            failure_threshold: 3,
            critical_threshold: 6,
            retry_interval: Duration::from_secs(30),
            max_retry_duration: Duration::from_secs(300),
        }
    }
}

/// Node discovery service for finding cluster members
pub struct NodeDiscovery {
    config: Arc<ClusterConfiguration>,
    known_nodes: Arc<RwLock<HashMap<NodeId, NodeEndpoint>>>,
}

impl NodeDiscovery {
    pub fn new(config: Arc<ClusterConfiguration>) -> Self {
        let known_nodes = Arc::new(RwLock::new(config.endpoints.iter().map(|(k, v)| (*k, v.clone())).collect()));
        
        Self {
            config,
            known_nodes,
        }
    }

    /// Discover nodes in the cluster
    pub async fn discover_nodes(&self) -> RaftResult<Vec<NodeEndpoint>> {
        let mut discovered = Vec::new();
        let known = self.known_nodes.read().await;
        
        // Start with configured endpoints
        for endpoint in known.values() {
            discovered.push(endpoint.clone());
        }

        // TODO: Implement additional discovery mechanisms:
        // - DNS-based discovery
        // - Consul/etcd integration
        // - Multicast discovery
        // - Static file-based discovery

        Ok(discovered)
    }

    /// Add a newly discovered node
    pub async fn add_discovered_node(&self, endpoint: NodeEndpoint) -> RaftResult<()> {
        // Validate the endpoint
        endpoint.socket_addr()?;
        
        let node_id = endpoint.node_id;
        let mut known = self.known_nodes.write().await;
        known.insert(node_id, endpoint.clone());
        
        log::info!("Discovered new node: {}", endpoint);
        Ok(())
    }

    /// Remove a node from discovery
    pub async fn remove_node(&self, node_id: NodeId) {
        let mut known = self.known_nodes.write().await;
        if let Some(endpoint) = known.remove(&node_id) {
            log::info!("Removed node from discovery: {}", endpoint);
        }
    }

    /// Get all known nodes
    pub async fn get_known_nodes(&self) -> HashMap<NodeId, NodeEndpoint> {
        self.known_nodes.read().await.clone()
    }

    /// Check if a node is known
    pub async fn is_known_node(&self, node_id: NodeId) -> bool {
        self.known_nodes.read().await.contains_key(&node_id)
    }
}

/// Health monitoring service for cluster nodes
pub struct HealthMonitor {
    config: HealthMonitorConfig,
    node_id: NodeId,
    discovery: Arc<NodeDiscovery>,
    health_results: Arc<RwLock<HashMap<NodeId, HealthCheckResult>>>,
    running: Arc<RwLock<bool>>,
}

impl HealthMonitor {
    pub fn new(
        config: HealthMonitorConfig,
        node_id: NodeId,
        discovery: Arc<NodeDiscovery>,
    ) -> Self {
        Self {
            config,
            node_id,
            discovery,
            health_results: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the health monitoring service
    pub async fn start(&self) -> RaftResult<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        log::info!("Starting health monitor for node {}", self.node_id);

        // Spawn monitoring task
        let monitor = self.clone();
        tokio::spawn(async move {
            monitor.monitoring_loop().await;
        });

        Ok(())
    }

    /// Stop the health monitoring service
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
        log::info!("Stopped health monitor for node {}", self.node_id);
    }

    /// Main monitoring loop
    async fn monitoring_loop(&self) {
        let mut check_interval = interval(self.config.check_interval);
        
        while *self.running.read().await {
            check_interval.tick().await;
            
            if let Err(e) = self.perform_health_checks().await {
                log::error!("Health check failed: {}", e);
            }
        }
    }

    /// Perform health checks on all known nodes
    async fn perform_health_checks(&self) -> RaftResult<()> {
        let known_nodes = self.discovery.get_known_nodes().await;
        
        // Limit concurrent health checks to prevent resource exhaustion
        const MAX_CONCURRENT_CHECKS: usize = 10;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CHECKS));
        let mut check_tasks = Vec::new();

        for (node_id, endpoint) in known_nodes {
            // Skip checking ourselves
            if node_id == self.node_id {
                continue;
            }

            let endpoint = endpoint.clone();
            let config = self.config.clone();
            let health_results = self.health_results.clone();
            let sem = semaphore.clone();
            
            // Get previous health check result to preserve counters
            let prev_result = self.health_results.read().await.get(&node_id).cloned();

            let task = tokio::spawn(async move {
                // Acquire semaphore permit before checking; if closed, skip this check
                let permit = match sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        log::warn!("Health check semaphore closed; skipping node {}", node_id);
                        return;
                    }
                };
                let result = Self::check_node_health(&endpoint, &config, prev_result).await;
                let mut results = health_results.write().await;
                results.insert(node_id, result);
                drop(permit);
            });

            check_tasks.push(task);
        }

        // Wait for all health checks to complete
        for task in check_tasks {
            if let Err(e) = task.await {
                log::error!("Health check task failed: {}", e);
            }
        }

        Ok(())
    }

    /// Check health of a specific node
    async fn check_node_health(
        endpoint: &NodeEndpoint,
        config: &HealthMonitorConfig,
        prev_result: Option<HealthCheckResult>,
    ) -> HealthCheckResult {
        // Start with previous result or create new one
        let mut result = prev_result.unwrap_or_else(|| HealthCheckResult::new(endpoint.node_id));
        result.total_checks = result.total_checks.saturating_add(1);

        let start_time = Instant::now();
        
        match timeout(config.check_timeout, Self::tcp_health_check(endpoint)).await {
            Ok(Ok(())) => {
                // Health check succeeded
                result.status = NodeStatus::Healthy;
                result.last_seen = SystemTime::now();
                result.response_time = Some(start_time.elapsed());
                result.successful_checks = result.successful_checks.saturating_add(1);
                result.consecutive_failures = 0;
                result.error_message = None;
            }
            Ok(Err(e)) => {
                // Health check failed
                result.consecutive_failures = result.consecutive_failures.saturating_add(1);
                result.error_message = Some(e.to_string());
                result.status = Self::determine_status(result.consecutive_failures, config);
            }
            Err(_) => {
                // Health check timed out
                result.consecutive_failures = result.consecutive_failures.saturating_add(1);
                result.error_message = Some("Health check timed out".to_string());
                result.status = Self::determine_status(result.consecutive_failures, config);
            }
        }

        result
    }

    /// Perform TCP-based health check
    async fn tcp_health_check(endpoint: &NodeEndpoint) -> RaftResult<()> {
        let addr = endpoint.socket_addr()?;
        
        match TcpStream::connect(addr).await {
            Ok(_) => Ok(()),
            Err(e) => Err(RaftError::Network(crate::error::NetworkError::ConnectionFailedToAddress {
                address: addr.to_string(),
                source: e,
            })),
        }
    }

    /// Determine node status based on consecutive failures
    fn determine_status(consecutive_failures: u32, config: &HealthMonitorConfig) -> NodeStatus {
        if consecutive_failures == 0 {
            NodeStatus::Healthy
        } else if consecutive_failures < config.failure_threshold {
            NodeStatus::Unreachable
        } else if consecutive_failures < config.critical_threshold {
            NodeStatus::Suspected
        } else {
            NodeStatus::Failed
        }
    }

    /// Get health status for a specific node
    pub async fn get_node_health(&self, node_id: NodeId) -> Option<HealthCheckResult> {
        self.health_results.read().await.get(&node_id).cloned()
    }

    /// Get health status for all nodes
    pub async fn get_all_health(&self) -> HashMap<NodeId, HealthCheckResult> {
        self.health_results.read().await.clone()
    }

    /// Get healthy nodes
    pub async fn get_healthy_nodes(&self) -> Vec<NodeId> {
        self.health_results
            .read()
            .await
            .iter()
            .filter(|(_, result)| result.is_healthy())
            .map(|(&node_id, _)| node_id)
            .collect()
    }

    /// Get failed nodes
    pub async fn get_failed_nodes(&self) -> Vec<NodeId> {
        self.health_results
            .read()
            .await
            .iter()
            .filter(|(_, result)| result.is_failed())
            .map(|(&node_id, _)| node_id)
            .collect()
    }

    /// Get cluster health summary
    pub async fn get_cluster_health(&self) -> ClusterHealth {
        let all_health = self.get_all_health().await;
        let known_nodes = self.discovery.get_known_nodes().await;
        
        let total_members = known_nodes.len();
        let healthy_members = all_health.values().filter(|r| r.is_healthy()).count();
        let failed_members = all_health.values().filter(|r| r.is_failed()).count();
        
        // A cluster is healthy if majority of nodes are healthy
        let is_healthy = healthy_members > total_members / 2;

        ClusterHealth {
            total_members,
            healthy_members,
            learners: 0, // This would be populated by the Raft node
            partitioned_nodes: failed_members,
            current_leader: None, // This would be populated by the Raft node
            is_healthy,
            last_log_index: 0, // This would be populated by the Raft node
            commit_index: 0, // This would be populated by the Raft node
        }
    }

    /// Check if the cluster has a healthy majority
    pub async fn has_healthy_majority(&self) -> bool {
        let cluster_health = self.get_cluster_health().await;
        cluster_health.is_healthy
    }

    /// Get nodes that should be considered for removal due to persistent failures
    pub async fn get_nodes_for_removal(&self) -> Vec<NodeId> {
        let all_health = self.get_all_health().await;
        let now = SystemTime::now();
        
        all_health
            .iter()
            .filter(|(_, result)| {
                // Only consider nodes that have been seen at least once (not just initialized)
                if result.total_checks == 0 || result.successful_checks == 0 {
                    return false;
                }
                
                // Node must be marked as Failed
                if result.status != NodeStatus::Failed {
                    return false;
                }
                
                // Check if node has been failing for longer than max_retry_duration
                match now.duration_since(result.last_seen) {
                    Ok(duration) => duration > self.config.max_retry_duration,
                    Err(_) => false, // Clock skew; don't remove
                }
            })
            .map(|(&node_id, _)| node_id)
            .collect()
    }
}

impl Clone for HealthMonitor {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id,
            discovery: self.discovery.clone(),
            health_results: self.health_results.clone(),
            running: self.running.clone(),
        }
    }
}

/// Cluster topology discovery and management
pub struct ClusterTopology {
    discovery: Arc<NodeDiscovery>,
    health_monitor: Arc<HealthMonitor>,
}

impl ClusterTopology {
    pub fn new(discovery: Arc<NodeDiscovery>, health_monitor: Arc<HealthMonitor>) -> Self {
        Self {
            discovery,
            health_monitor,
        }
    }

    /// Get current cluster topology
    pub async fn get_topology(&self) -> RaftResult<HashMap<NodeId, (NodeEndpoint, HealthCheckResult)>> {
        let known_nodes = self.discovery.get_known_nodes().await;
        let health_results = self.health_monitor.get_all_health().await;
        
        let mut topology = HashMap::new();
        
        for (node_id, endpoint) in known_nodes {
            let health = health_results
                .get(&node_id)
                .cloned()
                .unwrap_or_else(|| HealthCheckResult::new(node_id));
            
            topology.insert(node_id, (endpoint, health));
        }
        
        Ok(topology)
    }

    /// Get nodes that are candidates for leadership
    pub async fn get_leadership_candidates(&self) -> Vec<NodeId> {
        self.health_monitor.get_healthy_nodes().await
    }

    /// Get nodes that should be excluded from operations
    pub async fn get_excluded_nodes(&self) -> Vec<NodeId> {
        self.health_monitor.get_failed_nodes().await
    }

    /// Update topology with new node information
    pub async fn update_node(&self, endpoint: NodeEndpoint) -> RaftResult<()> {
        self.discovery.add_discovered_node(endpoint).await
    }

    /// Remove node from topology
    pub async fn remove_node(&self, node_id: NodeId) {
        self.discovery.remove_node(node_id).await;
    }
}

/// Comprehensive cluster status report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatusReport {
    /// Timestamp when the report was generated
    pub timestamp: u64,
    /// Node that generated this report
    pub reporter_node_id: NodeId,
    /// Overall cluster health
    pub cluster_health: ClusterHealth,
    /// Individual node statuses
    pub node_statuses: HashMap<NodeId, NodeStatusReport>,
    /// Leadership information
    pub leadership: LeadershipStatus,
    /// Replication status
    pub replication: ReplicationStatus,
    /// Performance metrics summary
    pub performance: ClusterPerformanceMetrics,
}

/// Individual node status in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatusReport {
    pub node_id: NodeId,
    pub endpoint: NodeEndpoint,
    pub health: HealthCheckResult,
    pub role: NodeRole,
    pub last_contact: Option<SystemTime>,
    pub replication_lag: Option<LogIndex>,
    pub is_voting_member: bool,
}

/// Node role in the Raft cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
    Learner,
    Unknown,
}

/// Leadership status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadershipStatus {
    pub current_leader: Option<NodeId>,
    pub current_term: Term,
    pub leader_uptime: Option<Duration>,
    pub leadership_changes: u64,
    pub last_election: Option<SystemTime>,
    pub election_timeout_remaining: Option<Duration>,
}

/// Replication status across the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub last_log_index: LogIndex,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub replication_lag: HashMap<NodeId, LogIndex>,
    pub max_replication_lag: LogIndex,
    pub avg_replication_lag: f64,
    pub nodes_behind: Vec<NodeId>,
    pub nodes_up_to_date: Vec<NodeId>,
}

/// Cluster-wide performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterPerformanceMetrics {
    pub total_requests_per_second: f64,
    pub avg_request_latency_ms: f64,
    pub leader_election_count: u64,
    pub failed_requests_per_second: f64,
    pub network_partition_events: u64,
    pub snapshot_operations: u64,
}

/// Cluster status reporter that generates comprehensive status reports
pub struct ClusterStatusReporter {
    node_id: NodeId,
    topology: Arc<ClusterTopology>,
    leadership_changes: Arc<RwLock<u64>>,
    election_history: Arc<RwLock<Vec<SystemTime>>>,
    performance_metrics: Arc<RwLock<ClusterPerformanceMetrics>>,
}

impl ClusterStatusReporter {
    /// Create a new cluster status reporter
    pub fn new(node_id: NodeId, topology: Arc<ClusterTopology>) -> Self {
        Self {
            node_id,
            topology,
            leadership_changes: Arc::new(RwLock::new(0)),
            election_history: Arc::new(RwLock::new(Vec::new())),
            performance_metrics: Arc::new(RwLock::new(ClusterPerformanceMetrics {
                total_requests_per_second: 0.0,
                avg_request_latency_ms: 0.0,
                leader_election_count: 0,
                failed_requests_per_second: 0.0,
                network_partition_events: 0,
                snapshot_operations: 0,
            })),
        }
    }

    /// Generate a comprehensive cluster status report
    pub async fn generate_status_report(
        &self,
        current_leader: Option<NodeId>,
        current_term: Term,
        last_log_index: LogIndex,
        commit_index: LogIndex,
        applied_index: LogIndex,
        replication_lag: HashMap<NodeId, LogIndex>,
    ) -> RaftResult<ClusterStatusReport> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_else(|_| {
                log::warn!("System clock appears to be before UNIX_EPOCH, using 0");
                0
            });

        // Get cluster health
        let cluster_health = self.topology.health_monitor.get_cluster_health().await;

        // Get topology information
        let topology_info = self.topology.get_topology().await?;

        // Build node status reports
        let mut node_statuses = HashMap::new();
        for (node_id, (endpoint, health)) in topology_info {
            let role = if Some(node_id) == current_leader {
                NodeRole::Leader
            } else if health.is_healthy() {
                NodeRole::Follower
            } else {
                NodeRole::Unknown
            };

            let replication_lag_value = replication_lag.get(&node_id).copied();
            
            let node_status = NodeStatusReport {
                node_id,
                endpoint,
                health,
                role,
                last_contact: None, // TODO: Track actual last contact time from Raft metrics
                replication_lag: replication_lag_value,
                is_voting_member: true, // TODO: Determine from actual cluster configuration
            };

            node_statuses.insert(node_id, node_status);
        }

        // Build leadership status
        let leadership_changes = *self.leadership_changes.read().await;
        let election_history = self.election_history.read().await;
        let last_election = election_history.last().copied();

        let leadership = LeadershipStatus {
            current_leader,
            current_term,
            leader_uptime: None, // This would be calculated from leader start time
            leadership_changes,
            last_election,
            election_timeout_remaining: None, // This would be calculated from Raft state
        };

        // Build replication status
        let max_lag = replication_lag.values().max().copied().unwrap_or(0);
        let avg_lag = if !replication_lag.is_empty() {
            replication_lag.values().sum::<LogIndex>() as f64 / replication_lag.len() as f64
        } else {
            0.0
        };

        let nodes_behind: Vec<NodeId> = replication_lag
            .iter()
            .filter(|(_, &lag)| lag > 10) // Consider nodes with lag > 10 as behind
            .map(|(&node_id, _)| node_id)
            .collect();

        let nodes_up_to_date: Vec<NodeId> = replication_lag
            .iter()
            .filter(|(_, &lag)| lag <= 10)
            .map(|(&node_id, _)| node_id)
            .collect();

        let replication = ReplicationStatus {
            last_log_index,
            commit_index,
            applied_index,
            replication_lag,
            max_replication_lag: max_lag,
            avg_replication_lag: avg_lag,
            nodes_behind,
            nodes_up_to_date,
        };

        // Get performance metrics
        let performance = self.performance_metrics.read().await.clone();

        Ok(ClusterStatusReport {
            timestamp,
            reporter_node_id: self.node_id,
            cluster_health,
            node_statuses,
            leadership,
            replication,
            performance,
        })
    }

    /// Record a leadership change event
    pub async fn record_leadership_change(&self, new_leader: Option<NodeId>) {
        let mut changes = self.leadership_changes.write().await;
        *changes += 1;

        let mut history = self.election_history.write().await;
        history.push(SystemTime::now());
        
        // Keep only recent elections (last 100)
        if history.len() > 100 {
            history.remove(0);
        }

        log::info!("Leadership change recorded: new leader {:?}, total changes: {}", new_leader, *changes);
    }

    /// Update performance metrics
    pub async fn update_performance_metrics(&self, metrics: ClusterPerformanceMetrics) {
        let mut perf = self.performance_metrics.write().await;
        *perf = metrics;
    }

    /// Get cluster health summary
    pub async fn get_cluster_health_summary(&self) -> ClusterHealthSummary {
        let cluster_health = self.topology.health_monitor.get_cluster_health().await;
        let healthy_nodes = self.topology.health_monitor.get_healthy_nodes().await;
        let failed_nodes = self.topology.health_monitor.get_failed_nodes().await;

        ClusterHealthSummary {
            is_healthy: cluster_health.is_healthy,
            total_nodes: cluster_health.total_members,
            healthy_nodes: healthy_nodes.len(),
            failed_nodes: failed_nodes.len(),
            has_leader: cluster_health.current_leader.is_some(),
            has_quorum: healthy_nodes.len() > cluster_health.total_members / 2,
        }
    }

    /// Get replication lag summary
    pub async fn get_replication_lag_summary(&self, replication_lag: &HashMap<NodeId, LogIndex>) -> ReplicationLagSummary {
        if replication_lag.is_empty() {
            return ReplicationLagSummary {
                max_lag: 0,
                avg_lag: 0.0,
                nodes_behind_count: 0,
                critical_lag_nodes: Vec::new(),
            };
        }

        let max_lag = replication_lag.values().max().copied().unwrap_or(0);
        let avg_lag = replication_lag.values().sum::<LogIndex>() as f64 / replication_lag.len() as f64;
        let nodes_behind_count = replication_lag.values().filter(|&&lag| lag > 10).count();
        let critical_lag_nodes: Vec<NodeId> = replication_lag
            .iter()
            .filter(|(_, &lag)| lag > 100) // Consider lag > 100 as critical
            .map(|(&node_id, _)| node_id)
            .collect();

        ReplicationLagSummary {
            max_lag,
            avg_lag,
            nodes_behind_count,
            critical_lag_nodes,
        }
    }
}

/// Simplified cluster health summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealthSummary {
    pub is_healthy: bool,
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub failed_nodes: usize,
    pub has_leader: bool,
    pub has_quorum: bool,
}

/// Replication lag summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationLagSummary {
    pub max_lag: LogIndex,
    pub avg_lag: f64,
    pub nodes_behind_count: usize,
    pub critical_lag_nodes: Vec<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_health_check_result() {
        let mut result = HealthCheckResult::new(1);
        assert_eq!(result.success_rate(), 0.0);
        assert!(!result.is_healthy());
        assert!(!result.is_failed());

        result.total_checks = 10;
        result.successful_checks = 8;
        assert_eq!(result.success_rate(), 0.8);

        result.status = NodeStatus::Healthy;
        assert!(result.is_healthy());
        assert!(!result.is_failed());

        result.status = NodeStatus::Failed;
        assert!(!result.is_healthy());
        assert!(result.is_failed());
    }

    #[test]
    fn test_node_status_determination() {
        let config = HealthMonitorConfig::default();
        
        assert_eq!(HealthMonitor::determine_status(0, &config), NodeStatus::Healthy);
        assert_eq!(HealthMonitor::determine_status(1, &config), NodeStatus::Unreachable);
        assert_eq!(HealthMonitor::determine_status(3, &config), NodeStatus::Suspected);
        assert_eq!(HealthMonitor::determine_status(6, &config), NodeStatus::Failed);
    }

    #[tokio::test]
    async fn test_node_discovery() {
        let config = Arc::new(ClusterConfiguration::default());
        let discovery = NodeDiscovery::new(config);
        
        let endpoint = NodeEndpoint::new(1, "127.0.0.1".to_string(), 8080);
        discovery.add_discovered_node(endpoint.clone()).await.unwrap();
        
        assert!(discovery.is_known_node(1).await);
        assert!(!discovery.is_known_node(2).await);
        
        let known = discovery.get_known_nodes().await;
        assert_eq!(known.len(), 1);
        assert_eq!(known.get(&1), Some(&endpoint));
    }

    #[tokio::test]
    async fn test_health_monitor_creation() {
        let config = Arc::new(ClusterConfiguration::default());
        let discovery = Arc::new(NodeDiscovery::new(config));
        let health_config = HealthMonitorConfig::default();
        
        let monitor = HealthMonitor::new(health_config, 1, discovery);
        
        let cluster_health = monitor.get_cluster_health().await;
        assert_eq!(cluster_health.total_members, 0);
        assert_eq!(cluster_health.healthy_members, 0);
    }
}