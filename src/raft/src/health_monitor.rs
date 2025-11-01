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

//! Cluster health monitoring and anomaly detection
//!
//! This module provides comprehensive cluster health monitoring, including
//! real-time health status updates, anomaly detection, and health history tracking.

use crate::error::RaftResult;
use crate::metrics::MetricsCollector;
use crate::types::{ClusterHealth, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::interval;

/// Health status levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// All systems operating normally
    Healthy,
    /// Some degradation but still functional
    Degraded,
    /// Critical issues affecting functionality
    Critical,
    /// System is down or unreachable
    Down,
}

/// Types of health anomalies that can be detected
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnomalyType {
    /// High request latency
    HighLatency,
    /// High error rate
    HighErrorRate,
    /// Network partition detected
    NetworkPartition,
    /// Leader election timeout
    LeaderElectionTimeout,
    /// Replication lag too high
    ReplicationLag,
    /// Storage issues
    StorageIssue,
    /// Memory usage too high
    HighMemoryUsage,
    /// Node unresponsive
    NodeUnresponsive,
}

/// Individual anomaly record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    /// Type of anomaly
    pub anomaly_type: AnomalyType,
    /// Severity level
    pub severity: HealthStatus,
    /// Timestamp when detected
    pub timestamp: u64,
    /// Affected node (if specific to a node)
    pub affected_node: Option<NodeId>,
    /// Description of the anomaly
    pub description: String,
    /// Metric values that triggered the anomaly
    pub metrics: HashMap<String, f64>,
    /// Whether the anomaly is still active
    pub active: bool,
}

/// Comprehensive cluster health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealthStatus {
    /// Overall cluster health
    pub overall_status: HealthStatus,
    /// Individual node health status
    pub node_status: HashMap<NodeId, HealthStatus>,
    /// Current active anomalies
    pub active_anomalies: Vec<Anomaly>,
    /// Timestamp of last health check
    pub last_updated: u64,
    /// Basic cluster health info
    pub cluster_health: ClusterHealth,
}

/// Historical health record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthRecord {
    /// Timestamp
    pub timestamp: u64,
    /// Health status at that time
    pub status: HealthStatus,
    /// Key metrics at that time
    pub metrics: HashMap<String, f64>,
    /// Active anomalies at that time
    pub anomalies: Vec<AnomalyType>,
}

/// Health monitoring configuration
#[derive(Debug, Clone)]
pub struct HealthMonitorConfig {
    /// How often to check health (in seconds)
    pub check_interval_secs: u64,
    /// Maximum request latency before considering degraded (ms)
    pub max_latency_ms: f64,
    /// Maximum error rate before considering degraded (%)
    pub max_error_rate_percent: f64,
    /// Maximum replication lag before considering degraded
    pub max_replication_lag: u64,
    /// Maximum memory usage before considering degraded (bytes)
    pub max_memory_usage_bytes: u64,
    /// How long to keep health history (hours)
    pub history_retention_hours: u64,
    /// Maximum number of anomalies to keep in memory
    pub max_anomalies: usize,
}

impl Default for HealthMonitorConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 5,
            max_latency_ms: 1000.0,
            max_error_rate_percent: 5.0,
            max_replication_lag: 100,
            max_memory_usage_bytes: 1024 * 1024 * 1024, // 1GB
            history_retention_hours: 24,
            max_anomalies: 1000,
        }
    }
}

/// Cluster health monitor
pub struct ClusterHealthMonitor {
    /// Configuration
    config: HealthMonitorConfig,
    /// Current health status
    current_status: Arc<RwLock<ClusterHealthStatus>>,
    /// Health history (circular buffer)
    health_history: Arc<RwLock<VecDeque<HealthRecord>>>,
    /// Active anomalies
    anomalies: Arc<RwLock<Vec<Anomaly>>>,
    /// Metrics collector reference
    metrics_collector: Arc<MetricsCollector>,
    /// Health status change broadcaster
    status_broadcaster: broadcast::Sender<ClusterHealthStatus>,
    /// Whether monitoring is running
    running: Arc<RwLock<bool>>,
}

impl ClusterHealthMonitor {
    /// Create a new health monitor
    pub fn new(config: HealthMonitorConfig, metrics_collector: Arc<MetricsCollector>) -> Self {
        let (status_broadcaster, _) = broadcast::channel(100);

        let initial_status = ClusterHealthStatus {
            overall_status: HealthStatus::Healthy,
            node_status: HashMap::new(),
            active_anomalies: Vec::new(),
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            cluster_health: ClusterHealth {
                total_members: 1,
                healthy_members: 1,
                learners: 0,
                partitioned_nodes: 0,
                current_leader: None,
                is_healthy: true,
                last_log_index: 0,
                commit_index: 0,
            },
        };

        Self {
            config,
            current_status: Arc::new(RwLock::new(initial_status)),
            health_history: Arc::new(RwLock::new(VecDeque::new())),
            anomalies: Arc::new(RwLock::new(Vec::new())),
            metrics_collector,
            status_broadcaster,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the health monitoring loop
    pub async fn start(&self) -> RaftResult<()> {
        {
            let mut running = self.running.write().unwrap();
            if *running {
                return Ok(());
            }
            *running = true;
        }

        let config = self.config.clone();
        let current_status = Arc::clone(&self.current_status);
        let health_history = Arc::clone(&self.health_history);
        let anomalies = Arc::clone(&self.anomalies);
        let metrics_collector = Arc::clone(&self.metrics_collector);
        let status_broadcaster = self.status_broadcaster.clone();
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(config.check_interval_secs));

            while *running.read().unwrap() {
                interval.tick().await;

                if let Err(e) = Self::perform_health_check(
                    &config,
                    &current_status,
                    &health_history,
                    &anomalies,
                    &metrics_collector,
                    &status_broadcaster,
                )
                .await
                {
                    log::error!("Health check failed: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Stop the health monitoring
    pub fn stop(&self) {
        let mut running = self.running.write().unwrap();
        *running = false;
    }

    /// Get current health status
    pub fn get_current_status(&self) -> ClusterHealthStatus {
        self.current_status.read().unwrap().clone()
    }

    /// Get health history
    pub fn get_health_history(&self, limit: Option<usize>) -> Vec<HealthRecord> {
        let history = self.health_history.read().unwrap();
        let limit = limit.unwrap_or(history.len());
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get active anomalies
    pub fn get_active_anomalies(&self) -> Vec<Anomaly> {
        self.anomalies
            .read()
            .unwrap()
            .iter()
            .filter(|a| a.active)
            .cloned()
            .collect()
    }

    /// Subscribe to health status changes
    pub fn subscribe(&self) -> broadcast::Receiver<ClusterHealthStatus> {
        self.status_broadcaster.subscribe()
    }

    /// Update cluster health information
    pub fn update_cluster_health(&self, cluster_health: ClusterHealth) {
        let mut status = self.current_status.write().unwrap();
        status.cluster_health = cluster_health;
        status.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Perform a single health check
    async fn perform_health_check(
        config: &HealthMonitorConfig,
        current_status: &Arc<RwLock<ClusterHealthStatus>>,
        health_history: &Arc<RwLock<VecDeque<HealthRecord>>>,
        anomalies: &Arc<RwLock<Vec<Anomaly>>>,
        metrics_collector: &Arc<MetricsCollector>,
        status_broadcaster: &broadcast::Sender<ClusterHealthStatus>,
    ) -> RaftResult<()> {
        let metrics = metrics_collector.get_metrics();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Detect anomalies
        let mut detected_anomalies = Vec::new();

        // Check latency
        if metrics.performance.avg_request_latency_ms > config.max_latency_ms {
            detected_anomalies.push(Anomaly {
                anomaly_type: AnomalyType::HighLatency,
                severity: if metrics.performance.avg_request_latency_ms
                    > config.max_latency_ms * 2.0
                {
                    HealthStatus::Critical
                } else {
                    HealthStatus::Degraded
                },
                timestamp: now,
                affected_node: None,
                description: format!(
                    "High average request latency: {:.2}ms (threshold: {:.2}ms)",
                    metrics.performance.avg_request_latency_ms, config.max_latency_ms
                ),
                metrics: {
                    let mut m = HashMap::new();
                    m.insert(
                        "avg_latency_ms".to_string(),
                        metrics.performance.avg_request_latency_ms,
                    );
                    m.insert(
                        "p95_latency_ms".to_string(),
                        metrics.performance.p95_request_latency_ms,
                    );
                    m.insert(
                        "p99_latency_ms".to_string(),
                        metrics.performance.p99_request_latency_ms,
                    );
                    m
                },
                active: true,
            });
        }

        // Check error rate
        if metrics.errors.error_rate_percent > config.max_error_rate_percent {
            detected_anomalies.push(Anomaly {
                anomaly_type: AnomalyType::HighErrorRate,
                severity: if metrics.errors.error_rate_percent > config.max_error_rate_percent * 2.0
                {
                    HealthStatus::Critical
                } else {
                    HealthStatus::Degraded
                },
                timestamp: now,
                affected_node: None,
                description: format!(
                    "High error rate: {:.2}% (threshold: {:.2}%)",
                    metrics.errors.error_rate_percent, config.max_error_rate_percent
                ),
                metrics: {
                    let mut m = HashMap::new();
                    m.insert(
                        "error_rate_percent".to_string(),
                        metrics.errors.error_rate_percent,
                    );
                    m.insert(
                        "total_operations".to_string(),
                        metrics.errors.total_operations as f64,
                    );
                    m.insert(
                        "failed_operations".to_string(),
                        metrics.errors.failed_operations as f64,
                    );
                    m
                },
                active: true,
            });
        }

        // Check replication lag
        for (node_id, lag) in &metrics.replication.replication_lag {
            if *lag > config.max_replication_lag {
                detected_anomalies.push(Anomaly {
                    anomaly_type: AnomalyType::ReplicationLag,
                    severity: if *lag > config.max_replication_lag * 2 {
                        HealthStatus::Critical
                    } else {
                        HealthStatus::Degraded
                    },
                    timestamp: now,
                    affected_node: Some(*node_id),
                    description: format!(
                        "High replication lag for node {}: {} (threshold: {})",
                        node_id, lag, config.max_replication_lag
                    ),
                    metrics: {
                        let mut m = HashMap::new();
                        m.insert("replication_lag".to_string(), *lag as f64);
                        m.insert("node_id".to_string(), *node_id as f64);
                        m
                    },
                    active: true,
                });
            }
        }

        // Check memory usage
        if metrics.performance.memory_usage_bytes > config.max_memory_usage_bytes {
            detected_anomalies.push(Anomaly {
                anomaly_type: AnomalyType::HighMemoryUsage,
                severity: if metrics.performance.memory_usage_bytes
                    > config.max_memory_usage_bytes * 2
                {
                    HealthStatus::Critical
                } else {
                    HealthStatus::Degraded
                },
                timestamp: now,
                affected_node: None,
                description: format!(
                    "High memory usage: {} bytes (threshold: {} bytes)",
                    metrics.performance.memory_usage_bytes, config.max_memory_usage_bytes
                ),
                metrics: {
                    let mut m = HashMap::new();
                    m.insert(
                        "memory_usage_bytes".to_string(),
                        metrics.performance.memory_usage_bytes as f64,
                    );
                    m
                },
                active: true,
            });
        }

        // Update anomalies
        {
            let mut anomalies_guard = anomalies.write().unwrap();

            // Mark existing anomalies as inactive if they're resolved
            for anomaly in anomalies_guard.iter_mut() {
                if anomaly.active {
                    let resolved = match anomaly.anomaly_type {
                        AnomalyType::HighLatency => {
                            metrics.performance.avg_request_latency_ms <= config.max_latency_ms
                        }
                        AnomalyType::HighErrorRate => {
                            metrics.errors.error_rate_percent <= config.max_error_rate_percent
                        }
                        AnomalyType::ReplicationLag => {
                            if let Some(node_id) = anomaly.affected_node {
                                metrics
                                    .replication
                                    .replication_lag
                                    .get(&node_id)
                                    .map(|lag| *lag <= config.max_replication_lag)
                                    .unwrap_or(true)
                            } else {
                                false
                            }
                        }
                        AnomalyType::HighMemoryUsage => {
                            metrics.performance.memory_usage_bytes <= config.max_memory_usage_bytes
                        }
                        _ => false, // Other anomaly types need manual resolution
                    };

                    if resolved {
                        anomaly.active = false;
                    }
                }
            }

            // Add new anomalies
            anomalies_guard.extend(detected_anomalies.clone());

            // Keep only recent anomalies
            let max_anomalies = config.max_anomalies;
            if anomalies_guard.len() > max_anomalies {
                let drain_count = anomalies_guard.len() - max_anomalies;
                anomalies_guard.drain(0..drain_count);
            }
        }

        // Determine overall health status
        let overall_status = if detected_anomalies
            .iter()
            .any(|a| a.severity == HealthStatus::Critical)
        {
            HealthStatus::Critical
        } else if detected_anomalies
            .iter()
            .any(|a| a.severity == HealthStatus::Degraded)
        {
            HealthStatus::Degraded
        } else if !metrics.state.node_state.contains("Leader")
            && !metrics.state.node_state.contains("Follower")
        {
            HealthStatus::Down
        } else {
            HealthStatus::Healthy
        };

        // Update current status
        let updated_status = {
            let mut status = current_status.write().unwrap();
            status.overall_status = overall_status;
            status.active_anomalies = detected_anomalies.clone();
            status.last_updated = now;

            // Update individual node status based on replication metrics
            status.node_status.clear();
            status
                .node_status
                .insert(metrics.state.node_id, overall_status);

            for (node_id, lag) in &metrics.replication.replication_lag {
                let node_status = if *lag > config.max_replication_lag * 2 {
                    HealthStatus::Critical
                } else if *lag > config.max_replication_lag {
                    HealthStatus::Degraded
                } else {
                    HealthStatus::Healthy
                };
                status.node_status.insert(*node_id, node_status);
            }

            status.clone()
        };

        // Add to health history
        {
            let mut history = health_history.write().unwrap();
            let record = HealthRecord {
                timestamp: now,
                status: overall_status,
                metrics: {
                    let mut m = HashMap::new();
                    m.insert(
                        "avg_latency_ms".to_string(),
                        metrics.performance.avg_request_latency_ms,
                    );
                    m.insert(
                        "error_rate_percent".to_string(),
                        metrics.errors.error_rate_percent,
                    );
                    m.insert(
                        "memory_usage_bytes".to_string(),
                        metrics.performance.memory_usage_bytes as f64,
                    );
                    m.insert(
                        "requests_per_second".to_string(),
                        metrics.performance.requests_per_second,
                    );
                    m
                },
                anomalies: detected_anomalies
                    .iter()
                    .map(|a| a.anomaly_type.clone())
                    .collect(),
            };

            history.push_back(record);

            // Keep only recent history
            let max_history_size =
                (config.history_retention_hours * 3600 / config.check_interval_secs) as usize;
            while history.len() > max_history_size {
                history.pop_front();
            }
        }

        // Broadcast status change
        let _ = status_broadcaster.send(updated_status);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricsCollector;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_health_monitor_creation() {
        let metrics_collector = Arc::new(MetricsCollector::new(1));
        let config = HealthMonitorConfig::default();
        let monitor = ClusterHealthMonitor::new(config, metrics_collector);

        let status = monitor.get_current_status();
        assert_eq!(status.overall_status, HealthStatus::Healthy);
        assert!(status.active_anomalies.is_empty());
    }

    #[tokio::test]
    async fn test_health_monitor_start_stop() {
        let metrics_collector = Arc::new(MetricsCollector::new(1));
        let config = HealthMonitorConfig {
            check_interval_secs: 1,
            ..Default::default()
        };
        let monitor = ClusterHealthMonitor::new(config, metrics_collector);

        // Start monitoring
        monitor.start().await.unwrap();

        // Let it run for a bit
        sleep(Duration::from_millis(100)).await;

        // Stop monitoring
        monitor.stop();

        // Should not panic
    }

    #[tokio::test]
    async fn test_anomaly_detection() {
        let metrics_collector = Arc::new(MetricsCollector::new(1));
        let config = HealthMonitorConfig {
            max_latency_ms: 100.0,
            max_error_rate_percent: 1.0,
            ..Default::default()
        };
        let monitor = ClusterHealthMonitor::new(config, metrics_collector.clone());

        // Simulate high latency
        metrics_collector.record_request_latency(Duration::from_millis(200));

        // Simulate errors
        metrics_collector.record_operation_failure("TestError", "Test error", "test_op");
        metrics_collector.record_operation_success(); // To get a meaningful error rate

        // Perform health check manually
        let config = HealthMonitorConfig {
            max_latency_ms: 100.0,
            max_error_rate_percent: 1.0,
            ..Default::default()
        };

        ClusterHealthMonitor::perform_health_check(
            &config,
            &monitor.current_status,
            &monitor.health_history,
            &monitor.anomalies,
            &metrics_collector,
            &monitor.status_broadcaster,
        )
        .await
        .unwrap();

        let status = monitor.get_current_status();
        assert!(!status.active_anomalies.is_empty());

        let anomalies = monitor.get_active_anomalies();
        assert!(
            anomalies
                .iter()
                .any(|a| a.anomaly_type == AnomalyType::HighLatency)
        );
    }

    #[test]
    fn test_health_status_levels() {
        assert_eq!(HealthStatus::Healthy as u8, 0);
        assert_ne!(HealthStatus::Healthy, HealthStatus::Degraded);
    }

    #[test]
    fn test_anomaly_types() {
        let anomaly = Anomaly {
            anomaly_type: AnomalyType::HighLatency,
            severity: HealthStatus::Degraded,
            timestamp: 1234567890,
            affected_node: Some(1),
            description: "Test anomaly".to_string(),
            metrics: HashMap::new(),
            active: true,
        };

        assert_eq!(anomaly.anomaly_type, AnomalyType::HighLatency);
        assert_eq!(anomaly.severity, HealthStatus::Degraded);
        assert!(anomaly.active);
    }
}
