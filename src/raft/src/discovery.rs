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
use crate::error::{RaftError, RaftResult, NetworkError};
use crate::types::{NodeId, ClusterHealth, Term, LogIndex};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, BTreeSet};
use std::net::SocketAddr;
use std::str::FromStr;
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
#[derive(Debug)]
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
        drop(known);

        // Implement DNS-based discovery
        if let Ok(dns_nodes) = self.discover_via_dns().await {
            for node in dns_nodes {
                if !discovered.iter().any(|ep| ep.node_id == node.node_id) {
                    discovered.push(node);
                }
            }
        }

        // Implement static file-based discovery
        if let Ok(file_nodes) = self.discover_via_file().await {
            for node in file_nodes {
                if !discovered.iter().any(|ep| ep.node_id == node.node_id) {
                    discovered.push(node);
                }
            }
        }

        log::debug!("Discovered {} nodes in cluster", discovered.len());
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

    /// Discover nodes via DNS SRV records
    async fn discover_via_dns(&self) -> RaftResult<Vec<NodeEndpoint>> {
        // For now, return empty as DNS discovery requires external dependencies
        // This can be implemented with trust-dns-resolver crate if needed
        log::debug!("DNS-based discovery not implemented yet");
        Ok(Vec::new())
    }

    /// Discover nodes via static file
    async fn discover_via_file(&self) -> RaftResult<Vec<NodeEndpoint>> {
        use std::path::Path;
        use tokio::fs;

        let discovery_file = Path::new("cluster_discovery.txt");
        if !discovery_file.exists() {
            return Ok(Vec::new());
        }

        match fs::read_to_string(discovery_file).await {
            Ok(content) => {
                let mut nodes = Vec::new();
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    
                    match NodeEndpoint::from_str(line) {
                        Ok(endpoint) => {
                            log::debug!("Discovered node from file: {}", endpoint);
                            nodes.push(endpoint);
                        }
                        Err(e) => {
                            log::warn!("Failed to parse discovery file line '{}': {}", line, e);
                        }
                    }
                }
                Ok(nodes)
            }
            Err(e) => {
                log::warn!("Failed to read discovery file: {}", e);
                Ok(Vec::new())
            }
        }
    }

    /// Validate node connectivity
    pub async fn validate_node_connectivity(&self, endpoint: &NodeEndpoint) -> RaftResult<bool> {
        use tokio::net::TcpStream;
        use tokio::time::{timeout, Duration};

        let addr = endpoint.socket_addr()?;
        let connect_timeout = Duration::from_secs(5);

        match timeout(connect_timeout, TcpStream::connect(addr)).await {
            Ok(Ok(_)) => {
                log::debug!("Node {} is reachable at {}", endpoint.node_id, addr);
                Ok(true)
            }
            Ok(Err(e)) => {
                log::debug!("Node {} is not reachable at {}: {}", endpoint.node_id, addr, e);
                Ok(false)
            }
            Err(_) => {
                log::debug!("Connection to node {} at {} timed out", endpoint.node_id, addr);
                Ok(false)
            }
        }
    }

    /// Resolve node address with hostname resolution
    pub async fn resolve_node_address(&self, endpoint: &NodeEndpoint) -> RaftResult<Vec<SocketAddr>> {
        use tokio::net::lookup_host;

        let address = endpoint.address();
        match lookup_host(&address).await {
            Ok(addrs) => {
                let resolved: Vec<SocketAddr> = addrs.collect();
                log::debug!("Resolved {} to {} addresses", address, resolved.len());
                Ok(resolved)
            }
            Err(e) => {
                log::warn!("Failed to resolve address {}: {}", address, e);
                Err(RaftError::Network(NetworkError::ConnectionFailedToAddress {
                    address,
                    source: e,
                }))
            }
        }
    }

    /// Discover and validate all nodes
    pub async fn discover_and_validate_nodes(&self) -> RaftResult<Vec<NodeEndpoint>> {
        let discovered = self.discover_nodes().await?;
        let mut validated = Vec::new();

        let discovered_count = discovered.len();
        
        // Validate connectivity for each discovered node
        for endpoint in discovered {
            if self.validate_node_connectivity(&endpoint).await.unwrap_or(false) {
                validated.push(endpoint);
            }
        }

        log::info!("Validated {} out of {} discovered nodes", validated.len(), discovered_count);
        Ok(validated)
    }
}

/// Health monitoring service for cluster nodes
#[derive(Debug)]
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
    partition_detector: Option<Arc<NetworkPartitionDetector>>,
}

impl ClusterTopology {
    pub fn new(discovery: Arc<NodeDiscovery>, health_monitor: Arc<HealthMonitor>) -> Self {
        Self {
            discovery,
            health_monitor,
            partition_detector: None,
        }
    }

    /// Create cluster topology with partition detection
    pub fn with_partition_detection(
        discovery: Arc<NodeDiscovery>,
        health_monitor: Arc<HealthMonitor>,
        partition_detector: Arc<NetworkPartitionDetector>,
    ) -> Self {
        Self {
            discovery,
            health_monitor,
            partition_detector: Some(partition_detector),
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

    /// Check if the cluster is currently experiencing a network partition
    pub async fn is_partitioned(&self) -> bool {
        match &self.partition_detector {
            Some(detector) => detector.is_cluster_partitioned().await,
            None => false,
        }
    }

    /// Get current partition state if any
    pub async fn get_partition_state(&self) -> Option<NetworkPartitionState> {
        match &self.partition_detector {
            Some(detector) => detector.get_current_partition_state().await,
            None => None,
        }
    }

    /// Get partition event history
    pub async fn get_partition_history(&self) -> Vec<NetworkPartitionEvent> {
        match &self.partition_detector {
            Some(detector) => detector.get_partition_events().await,
            None => Vec::new(),
        }
    }

    /// Get partition statistics
    pub async fn get_partition_statistics(&self) -> Option<PartitionStatistics> {
        match &self.partition_detector {
            Some(detector) => Some(detector.get_partition_statistics().await),
            None => None,
        }
    }

    /// Start partition detection if available
    pub async fn start_partition_detection(&self) -> RaftResult<()> {
        if let Some(detector) = &self.partition_detector {
            detector.start().await?;
            log::info!("Network partition detection started");
        }
        Ok(())
    }

    /// Create a comprehensive cluster monitoring setup with partition detection
    pub fn create_with_partition_monitoring(
        node_id: NodeId,
        cluster_config: Arc<ClusterConfiguration>,
        health_config: HealthMonitorConfig,
        partition_config: PartitionDetectorConfig,
    ) -> (Arc<Self>, Arc<NetworkPartitionDetector>) {
        let discovery = Arc::new(NodeDiscovery::new(cluster_config));
        let health_monitor = Arc::new(HealthMonitor::new(health_config, node_id, discovery.clone()));
        let partition_detector = Arc::new(NetworkPartitionDetector::new(
            node_id,
            health_monitor.clone(),
            discovery.clone(),
            partition_config,
        ));
        
        let topology = Arc::new(Self::with_partition_detection(
            discovery,
            health_monitor,
            partition_detector.clone(),
        ));
        
        (topology, partition_detector)
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

/// Network partition event information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkPartitionEvent {
    pub event_id: u64,
    pub timestamp: SystemTime,
    pub event_type: PartitionEventType,
    pub affected_nodes: Vec<NodeId>,
    pub partition_groups: Vec<Vec<NodeId>>,
    pub duration: Option<Duration>,
    pub recovery_timestamp: Option<SystemTime>,
}

/// Types of network partition events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionEventType {
    /// Network partition detected
    PartitionDetected,
    /// Network partition recovered
    PartitionRecovered,
    /// Split-brain scenario detected
    SplitBrainDetected,
    /// Minority partition isolated
    MinorityIsolated,
    /// Majority partition maintained
    MajorityMaintained,
}

/// Network partition detector with advanced detection capabilities
#[derive(Debug)]
pub struct NetworkPartitionDetector {
    node_id: NodeId,
    health_monitor: Arc<HealthMonitor>,
    discovery: Arc<NodeDiscovery>,
    partition_events: Arc<RwLock<Vec<NetworkPartitionEvent>>>,
    current_partition_state: Arc<RwLock<Option<NetworkPartitionState>>>,
    event_counter: Arc<RwLock<u64>>,
    config: PartitionDetectorConfig,
}

/// Configuration for network partition detection
#[derive(Debug, Clone)]
pub struct PartitionDetectorConfig {
    /// Minimum number of nodes that must be unreachable to consider a partition
    pub min_partition_size: usize,
    /// Time window for detecting partition patterns
    pub detection_window: Duration,
    /// Minimum duration a partition must persist to be considered real
    pub min_partition_duration: Duration,
    /// Maximum time to wait for partition recovery before taking action
    pub max_recovery_wait: Duration,
    /// Threshold for considering a cluster split-brain (percentage of nodes)
    pub split_brain_threshold: f64,
}

impl Default for PartitionDetectorConfig {
    fn default() -> Self {
        Self {
            min_partition_size: 1,
            detection_window: Duration::from_secs(30),
            min_partition_duration: Duration::from_secs(10),
            max_recovery_wait: Duration::from_secs(300),
            split_brain_threshold: 0.4, // 40% of nodes
        }
    }
}

/// Current network partition state
#[derive(Debug, Clone)]
pub struct NetworkPartitionState {
    pub partition_id: u64,
    pub detected_at: SystemTime,
    pub partition_groups: Vec<Vec<NodeId>>,
    pub majority_group: Option<Vec<NodeId>>,
    pub minority_groups: Vec<Vec<NodeId>>,
    pub is_split_brain: bool,
    pub affected_operations: Vec<String>,
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

    /// Record a network partition event
    pub async fn record_partition_event(&self, event_type: PartitionEventType, affected_nodes: Vec<NodeId>) {
        let mut metrics = self.performance_metrics.write().await;
        metrics.network_partition_events += 1;
        
        log::warn!("Network partition event recorded: {:?}, affected nodes: {:?}", event_type, affected_nodes);
    }

    /// Update performance metrics
    pub async fn update_performance_metrics(&self, metrics: ClusterPerformanceMetrics) {
        let mut current_metrics = self.performance_metrics.write().await;
        *current_metrics = metrics;
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

impl NetworkPartitionDetector {
    /// Create a new network partition detector
    pub fn new(
        node_id: NodeId,
        health_monitor: Arc<HealthMonitor>,
        discovery: Arc<NodeDiscovery>,
        config: PartitionDetectorConfig,
    ) -> Self {
        Self {
            node_id,
            health_monitor,
            discovery,
            partition_events: Arc::new(RwLock::new(Vec::new())),
            current_partition_state: Arc::new(RwLock::new(None)),
            event_counter: Arc::new(RwLock::new(0)),
            config,
        }
    }

    /// Start the partition detection service
    pub async fn start(&self) -> RaftResult<()> {
        log::info!("Starting network partition detector for node {}", self.node_id);
        
        // Spawn partition detection task
        let detector = self.clone();
        tokio::spawn(async move {
            detector.detection_loop().await;
        });

        Ok(())
    }

    /// Main partition detection loop
    async fn detection_loop(&self) {
        let mut detection_interval = interval(self.config.detection_window);
        
        loop {
            detection_interval.tick().await;
            
            if let Err(e) = self.detect_partitions().await {
                log::error!("Partition detection failed: {}", e);
            }
        }
    }

    /// Detect network partitions based on node health and connectivity
    pub async fn detect_partitions(&self) -> RaftResult<()> {
        let all_health = self.health_monitor.get_all_health().await;
        let known_nodes = self.discovery.get_known_nodes().await;
        
        // Categorize nodes by health status
        let mut healthy_nodes = Vec::new();
        let mut unreachable_nodes = Vec::new();
        let mut failed_nodes = Vec::new();
        
        for (node_id, endpoint) in &known_nodes {
            if *node_id == self.node_id {
                healthy_nodes.push(*node_id); // Assume self is healthy
                continue;
            }
            
            match all_health.get(node_id) {
                Some(health) => {
                    match health.status {
                        NodeStatus::Healthy => healthy_nodes.push(*node_id),
                        NodeStatus::Unreachable | NodeStatus::Suspected => unreachable_nodes.push(*node_id),
                        NodeStatus::Failed => failed_nodes.push(*node_id),
                        NodeStatus::Unknown => unreachable_nodes.push(*node_id),
                    }
                }
                None => unreachable_nodes.push(*node_id),
            }
        }

        // Check if we have enough unreachable nodes to consider a partition
        let total_unreachable = unreachable_nodes.len() + failed_nodes.len();
        if total_unreachable < self.config.min_partition_size {
            // No significant partition detected, check for recovery
            if let Some(current_state) = self.current_partition_state.read().await.as_ref() {
                self.handle_partition_recovery(current_state.partition_id).await?;
            }
            return Ok(());
        }

        // Analyze partition topology
        let partition_analysis = self.analyze_partition_topology(
            &healthy_nodes,
            &unreachable_nodes,
            &failed_nodes,
        ).await?;

        // Check if this is a new partition or continuation of existing one
        let current_state = self.current_partition_state.read().await;
        match current_state.as_ref() {
            Some(existing_state) => {
                // Update existing partition state
                self.update_partition_state(&partition_analysis, existing_state.partition_id).await?;
            }
            None => {
                // New partition detected
                if partition_analysis.is_significant_partition() {
                    self.handle_new_partition(partition_analysis).await?;
                }
            }
        }

        Ok(())
    }

    /// Analyze the topology of network partitions
    async fn analyze_partition_topology(
        &self,
        healthy_nodes: &[NodeId],
        unreachable_nodes: &[NodeId],
        failed_nodes: &[NodeId],
    ) -> RaftResult<PartitionAnalysis> {
        let total_nodes = healthy_nodes.len() + unreachable_nodes.len() + failed_nodes.len();
        let healthy_count = healthy_nodes.len();
        let unreachable_count = unreachable_nodes.len() + failed_nodes.len();

        // Determine if we have a majority
        let has_majority = healthy_count > total_nodes / 2;
        
        // Check for split-brain scenario
        let split_brain_threshold_count = (total_nodes as f64 * self.config.split_brain_threshold) as usize;
        let is_split_brain = healthy_count >= split_brain_threshold_count && 
                            unreachable_count >= split_brain_threshold_count;

        // Create partition groups
        let mut partition_groups = Vec::new();
        if !healthy_nodes.is_empty() {
            partition_groups.push(healthy_nodes.to_vec());
        }
        
        // Group unreachable nodes (in real implementation, we'd try to determine
        // which unreachable nodes can communicate with each other)
        if !unreachable_nodes.is_empty() {
            partition_groups.push(unreachable_nodes.to_vec());
        }
        if !failed_nodes.is_empty() {
            partition_groups.push(failed_nodes.to_vec());
        }

        Ok(PartitionAnalysis {
            total_nodes,
            healthy_nodes: healthy_nodes.to_vec(),
            unreachable_nodes: unreachable_nodes.to_vec(),
            failed_nodes: failed_nodes.to_vec(),
            partition_groups,
            has_majority,
            is_split_brain,
            partition_severity: self.calculate_partition_severity(healthy_count, total_nodes),
        })
    }

    /// Calculate the severity of the partition
    fn calculate_partition_severity(&self, healthy_count: usize, total_nodes: usize) -> PartitionSeverity {
        let healthy_ratio = healthy_count as f64 / total_nodes as f64;
        
        if healthy_ratio >= 0.8 {
            PartitionSeverity::Minor
        } else if healthy_ratio >= 0.5 {
            PartitionSeverity::Moderate
        } else if healthy_ratio >= 0.3 {
            PartitionSeverity::Major
        } else {
            PartitionSeverity::Critical
        }
    }

    /// Handle detection of a new network partition
    async fn handle_new_partition(&self, analysis: PartitionAnalysis) -> RaftResult<()> {
        let partition_id = {
            let mut counter = self.event_counter.write().await;
            *counter += 1;
            *counter
        };

        let now = SystemTime::now();
        
        // Determine partition type
        let event_type = if analysis.is_split_brain {
            PartitionEventType::SplitBrainDetected
        } else if analysis.has_majority {
            PartitionEventType::MinorityIsolated
        } else {
            PartitionEventType::PartitionDetected
        };

        // Create partition state
        let partition_state = NetworkPartitionState {
            partition_id,
            detected_at: now,
            partition_groups: analysis.partition_groups.clone(),
            majority_group: if analysis.has_majority {
                Some(analysis.healthy_nodes.clone())
            } else {
                None
            },
            minority_groups: if analysis.has_majority {
                vec![analysis.unreachable_nodes.clone(), analysis.failed_nodes.clone()]
                    .into_iter()
                    .filter(|group| !group.is_empty())
                    .collect()
            } else {
                analysis.partition_groups.clone()
            },
            is_split_brain: analysis.is_split_brain,
            affected_operations: self.determine_affected_operations(&analysis).await,
        };

        // Update current state
        {
            let mut current_state = self.current_partition_state.write().await;
            *current_state = Some(partition_state.clone());
        }

        // Record the event
        let partition_event = NetworkPartitionEvent {
            event_id: partition_id,
            timestamp: now,
            event_type: event_type.clone(),
            affected_nodes: [&analysis.unreachable_nodes[..], &analysis.failed_nodes[..]].concat(),
            partition_groups: analysis.partition_groups.clone(),
            duration: None,
            recovery_timestamp: None,
        };

        {
            let mut events = self.partition_events.write().await;
            events.push(partition_event);
            
            // Keep only recent events (last 1000)
            if events.len() > 1000 {
                events.remove(0);
            }
        }

        // Log the partition detection
        log::error!(
            "Network partition detected! Type: {:?}, Partition ID: {}, Affected nodes: {:?}, Severity: {:?}",
            event_type,
            partition_id,
            [&analysis.unreachable_nodes[..], &analysis.failed_nodes[..]].concat(),
            analysis.partition_severity
        );

        // Take appropriate action based on partition type
        self.handle_partition_action(&partition_state, &analysis).await?;

        Ok(())
    }

    /// Handle partition recovery
    async fn handle_partition_recovery(&self, partition_id: u64) -> RaftResult<()> {
        let now = SystemTime::now();
        
        // Update current state
        let recovered_state = {
            let mut current_state = self.current_partition_state.write().await;
            current_state.take()
        };

        if let Some(state) = recovered_state {
            let duration = now.duration_since(state.detected_at).unwrap_or_default();
            
            // Update the partition event with recovery information
            {
                let mut events = self.partition_events.write().await;
                if let Some(event) = events.iter_mut().find(|e| e.event_id == partition_id) {
                    event.duration = Some(duration);
                    event.recovery_timestamp = Some(now);
                }
            }

            // Record recovery event
            let recovery_event = NetworkPartitionEvent {
                event_id: partition_id,
                timestamp: now,
                event_type: PartitionEventType::PartitionRecovered,
                affected_nodes: state.partition_groups.clone().into_iter().flatten().collect(),
                partition_groups: Vec::new(),
                duration: Some(duration),
                recovery_timestamp: Some(now),
            };

            {
                let mut events = self.partition_events.write().await;
                events.push(recovery_event);
            }

            log::info!(
                "Network partition recovered! Partition ID: {}, Duration: {:?}",
                partition_id,
                duration
            );

            // Perform recovery actions
            self.handle_partition_recovery_actions(&state).await?;
        }

        Ok(())
    }

    /// Update existing partition state
    async fn update_partition_state(&self, analysis: &PartitionAnalysis, partition_id: u64) -> RaftResult<()> {
        let mut current_state = self.current_partition_state.write().await;
        
        if let Some(state) = current_state.as_mut() {
            // Update partition groups
            state.partition_groups = analysis.partition_groups.clone();
            state.majority_group = if analysis.has_majority {
                Some(analysis.healthy_nodes.clone())
            } else {
                None
            };
            state.is_split_brain = analysis.is_split_brain;
            state.affected_operations = self.determine_affected_operations(analysis).await;

            log::debug!(
                "Updated partition state for partition ID: {}, Split-brain: {}, Has majority: {}",
                partition_id,
                analysis.is_split_brain,
                analysis.has_majority
            );
        }

        Ok(())
    }

    /// Determine which operations are affected by the partition
    async fn determine_affected_operations(&self, analysis: &PartitionAnalysis) -> Vec<String> {
        let mut affected = Vec::new();

        if !analysis.has_majority {
            affected.push("Write operations suspended".to_string());
            affected.push("Leader election disabled".to_string());
        }

        if analysis.is_split_brain {
            affected.push("Split-brain protection activated".to_string());
            affected.push("Automatic recovery disabled".to_string());
        }

        match analysis.partition_severity {
            PartitionSeverity::Critical => {
                affected.push("Cluster operations severely limited".to_string());
            }
            PartitionSeverity::Major => {
                affected.push("Reduced cluster performance".to_string());
            }
            _ => {}
        }

        affected
    }

    /// Handle actions to take when a partition is detected
    async fn handle_partition_action(&self, state: &NetworkPartitionState, analysis: &PartitionAnalysis) -> RaftResult<()> {
        if analysis.is_split_brain {
            log::error!("Split-brain scenario detected! Taking protective measures.");
            // In a real implementation, this might involve:
            // - Stopping write operations
            // - Alerting administrators
            // - Implementing split-brain resolution strategies
        }

        if !analysis.has_majority {
            log::warn!("Lost majority quorum due to partition. Entering read-only mode.");
            // In a real implementation:
            // - Switch to read-only mode
            // - Stop accepting write requests
            // - Wait for partition recovery
        }

        // Record metrics
        // This would integrate with the metrics system
        log::info!("Partition action completed for partition ID: {}", state.partition_id);

        Ok(())
    }

    /// Handle recovery actions after partition is resolved
    async fn handle_partition_recovery_actions(&self, state: &NetworkPartitionState) -> RaftResult<()> {
        log::info!("Performing partition recovery actions for partition ID: {}", state.partition_id);

        // In a real implementation, this might involve:
        // - Re-enabling write operations
        // - Triggering log synchronization
        // - Updating cluster membership
        // - Clearing split-brain protection

        if state.is_split_brain {
            log::info!("Recovering from split-brain scenario");
            // Implement split-brain recovery logic
        }

        log::info!("Partition recovery actions completed");
        Ok(())
    }

    /// Get current partition state
    pub async fn get_current_partition_state(&self) -> Option<NetworkPartitionState> {
        self.current_partition_state.read().await.clone()
    }

    /// Get partition event history
    pub async fn get_partition_events(&self) -> Vec<NetworkPartitionEvent> {
        self.partition_events.read().await.clone()
    }

    /// Get partition events within a time range
    pub async fn get_partition_events_in_range(&self, start: SystemTime, end: SystemTime) -> Vec<NetworkPartitionEvent> {
        self.partition_events
            .read()
            .await
            .iter()
            .filter(|event| event.timestamp >= start && event.timestamp <= end)
            .cloned()
            .collect()
    }

    /// Check if the cluster is currently partitioned
    pub async fn is_cluster_partitioned(&self) -> bool {
        self.current_partition_state.read().await.is_some()
    }

    /// Get partition statistics
    pub async fn get_partition_statistics(&self) -> PartitionStatistics {
        let events = self.partition_events.read().await;
        let total_events = events.len();
        let total_partitions = events.iter().filter(|e| matches!(e.event_type, PartitionEventType::PartitionDetected)).count();
        let split_brain_events = events.iter().filter(|e| matches!(e.event_type, PartitionEventType::SplitBrainDetected)).count();
        
        let avg_duration = if !events.is_empty() {
            let total_duration: Duration = events
                .iter()
                .filter_map(|e| e.duration)
                .sum();
            total_duration / events.len() as u32
        } else {
            Duration::ZERO
        };

        PartitionStatistics {
            total_events,
            total_partitions,
            split_brain_events,
            average_duration: avg_duration,
            current_partition: self.current_partition_state.read().await.clone(),
        }
    }
}

impl Clone for NetworkPartitionDetector {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            health_monitor: self.health_monitor.clone(),
            discovery: self.discovery.clone(),
            partition_events: self.partition_events.clone(),
            current_partition_state: self.current_partition_state.clone(),
            event_counter: self.event_counter.clone(),
            config: self.config.clone(),
        }
    }
}

/// Analysis result of network partition topology
#[derive(Debug, Clone)]
struct PartitionAnalysis {
    total_nodes: usize,
    healthy_nodes: Vec<NodeId>,
    unreachable_nodes: Vec<NodeId>,
    failed_nodes: Vec<NodeId>,
    partition_groups: Vec<Vec<NodeId>>,
    has_majority: bool,
    is_split_brain: bool,
    partition_severity: PartitionSeverity,
}

impl PartitionAnalysis {
    fn is_significant_partition(&self) -> bool {
        !self.unreachable_nodes.is_empty() || !self.failed_nodes.is_empty()
    }
}

/// Severity levels for network partitions
#[derive(Debug, Clone, Copy)]
enum PartitionSeverity {
    Minor,    // < 20% nodes affected
    Moderate, // 20-50% nodes affected
    Major,    // 50-70% nodes affected
    Critical, // > 70% nodes affected
}

/// Statistics about network partitions
#[derive(Debug, Clone)]
pub struct PartitionStatistics {
    pub total_events: usize,
    pub total_partitions: usize,
    pub split_brain_events: usize,
    pub average_duration: Duration,
    pub current_partition: Option<NetworkPartitionState>,
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

    #[tokio::test]
    async fn test_network_partition_detector_creation() {
        let config = Arc::new(ClusterConfiguration::default());
        let discovery = Arc::new(NodeDiscovery::new(config));
        let health_config = HealthMonitorConfig::default();
        let health_monitor = Arc::new(HealthMonitor::new(health_config, 1, discovery.clone()));
        
        let partition_config = PartitionDetectorConfig::default();
        let detector = NetworkPartitionDetector::new(1, health_monitor, discovery, partition_config);
        
        // Test that detector is created successfully
        assert!(!detector.is_cluster_partitioned().await);
        
        // Test partition statistics
        let stats = detector.get_partition_statistics().await;
        assert_eq!(stats.total_events, 0);
        assert_eq!(stats.total_partitions, 0);
        assert_eq!(stats.split_brain_events, 0);
    }

    #[tokio::test]
    async fn test_partition_event_creation() {
        let event = NetworkPartitionEvent {
            event_id: 1,
            timestamp: SystemTime::now(),
            event_type: PartitionEventType::PartitionDetected,
            affected_nodes: vec![2, 3],
            partition_groups: vec![vec![1], vec![2, 3]],
            duration: None,
            recovery_timestamp: None,
        };
        
        assert_eq!(event.event_id, 1);
        assert_eq!(event.affected_nodes, vec![2, 3]);
        assert!(matches!(event.event_type, PartitionEventType::PartitionDetected));
    }
}