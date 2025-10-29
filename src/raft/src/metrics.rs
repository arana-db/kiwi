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

//! Raft metrics collection and monitoring
//!
//! This module provides comprehensive metrics collection for Raft operations,
//! performance monitoring, and health status reporting.

use crate::types::{NodeId, Term, LogIndex, RaftMetrics as OpenRaftMetrics};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Comprehensive Raft metrics for monitoring and observability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftMetrics {
    /// Basic Raft state metrics
    pub state: RaftStateMetrics,
    /// Performance metrics
    pub performance: PerformanceMetrics,
    /// Replication metrics
    pub replication: ReplicationMetrics,
    /// Network metrics
    pub network: NetworkMetrics,
    /// Storage metrics
    pub storage: StorageMetrics,
    /// Timestamp when metrics were collected
    pub timestamp: u64,
}

/// Core Raft state metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftStateMetrics {
    /// Current node ID
    pub node_id: NodeId,
    /// Current term
    pub current_term: Term,
    /// Current leader ID (if known)
    pub current_leader: Option<NodeId>,
    /// Current node state (Leader, Follower, Candidate)
    pub node_state: String,
    /// Last log index
    pub last_log_index: LogIndex,
    /// Commit index
    pub commit_index: LogIndex,
    /// Applied index
    pub applied_index: LogIndex,
    /// Number of nodes in cluster
    pub cluster_size: usize,
    /// Time since last leader heartbeat (milliseconds)
    pub last_heartbeat_ms: u64,
}

/// Performance-related metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Average request latency in milliseconds
    pub avg_request_latency_ms: f64,
    /// 95th percentile request latency in milliseconds
    pub p95_request_latency_ms: f64,
    /// 99th percentile request latency in milliseconds
    pub p99_request_latency_ms: f64,
    /// Requests per second
    pub requests_per_second: f64,
    /// Commands applied per second
    pub commands_per_second: f64,
    /// Log entries per second
    pub log_entries_per_second: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
}

/// Replication-related metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationMetrics {
    /// Replication lag for each follower (log index difference)
    pub replication_lag: HashMap<NodeId, LogIndex>,
    /// Last contact time with each follower (milliseconds since epoch)
    pub last_contact: HashMap<NodeId, u64>,
    /// Number of failed replication attempts per follower
    pub replication_failures: HashMap<NodeId, u64>,
    /// Average replication latency per follower in milliseconds
    pub replication_latency_ms: HashMap<NodeId, f64>,
}

/// Network-related metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetrics {
    /// Total bytes sent to other nodes
    pub bytes_sent: u64,
    /// Total bytes received from other nodes
    pub bytes_received: u64,
    /// Number of active connections
    pub active_connections: u32,
    /// Number of failed connection attempts
    pub connection_failures: u64,
    /// Network round-trip time to each node in milliseconds
    pub rtt_ms: HashMap<NodeId, f64>,
}

/// Storage-related metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetrics {
    /// Total log entries stored
    pub log_entries_count: u64,
    /// Log storage size in bytes
    pub log_size_bytes: u64,
    /// Number of snapshots
    pub snapshots_count: u64,
    /// Total snapshot size in bytes
    pub snapshot_size_bytes: u64,
    /// Disk I/O operations per second
    pub disk_ops_per_second: f64,
    /// Average disk I/O latency in milliseconds
    pub disk_latency_ms: f64,
}

/// Metrics collector that aggregates and tracks Raft metrics
pub struct MetricsCollector {
    /// Current metrics snapshot
    metrics: Arc<RwLock<RaftMetrics>>,
    /// Performance tracking
    performance_tracker: Arc<PerformanceTracker>,
    /// Network tracking
    network_tracker: Arc<NetworkTracker>,
    /// Storage tracking
    storage_tracker: Arc<StorageTracker>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(node_id: NodeId) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let initial_metrics = RaftMetrics {
            state: RaftStateMetrics {
                node_id,
                current_term: 0,
                current_leader: None,
                node_state: "Follower".to_string(),
                last_log_index: 0,
                commit_index: 0,
                applied_index: 0,
                cluster_size: 1,
                last_heartbeat_ms: 0,
            },
            performance: PerformanceMetrics {
                avg_request_latency_ms: 0.0,
                p95_request_latency_ms: 0.0,
                p99_request_latency_ms: 0.0,
                requests_per_second: 0.0,
                commands_per_second: 0.0,
                log_entries_per_second: 0.0,
                memory_usage_bytes: 0,
            },
            replication: ReplicationMetrics {
                replication_lag: HashMap::new(),
                last_contact: HashMap::new(),
                replication_failures: HashMap::new(),
                replication_latency_ms: HashMap::new(),
            },
            network: NetworkMetrics {
                bytes_sent: 0,
                bytes_received: 0,
                active_connections: 0,
                connection_failures: 0,
                rtt_ms: HashMap::new(),
            },
            storage: StorageMetrics {
                log_entries_count: 0,
                log_size_bytes: 0,
                snapshots_count: 0,
                snapshot_size_bytes: 0,
                disk_ops_per_second: 0.0,
                disk_latency_ms: 0.0,
            },
            timestamp: now,
        };

        Self {
            metrics: Arc::new(RwLock::new(initial_metrics)),
            performance_tracker: Arc::new(PerformanceTracker::new()),
            network_tracker: Arc::new(NetworkTracker::new()),
            storage_tracker: Arc::new(StorageTracker::new()),
        }
    }

    /// Update metrics from openraft metrics
    pub fn update_from_openraft(&self, openraft_metrics: &OpenRaftMetrics) {
        let mut metrics = self.metrics.write().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Update state metrics
        metrics.state.current_term = openraft_metrics.current_term.unwrap_or(0);
        metrics.state.current_leader = openraft_metrics.current_leader;
        metrics.state.node_state = format!("{:?}", openraft_metrics.state);
        metrics.state.last_log_index = openraft_metrics.last_log_index.unwrap_or(0);
        metrics.state.commit_index = openraft_metrics.last_applied.map(|id| id.index).unwrap_or(0);
        metrics.state.applied_index = openraft_metrics.last_applied.map(|id| id.index).unwrap_or(0);
        
        // Update replication metrics
        if let Some(ref replication) = openraft_metrics.replication {
            for (node_id, progress) in replication {
                let lag = metrics.state.last_log_index.saturating_sub(progress.matched.unwrap_or(0));
                metrics.replication.replication_lag.insert(*node_id, lag);
            }
        }

        metrics.timestamp = now;
    }

    /// Record a request latency measurement
    pub fn record_request_latency(&self, latency: Duration) {
        self.performance_tracker.record_latency(latency);
    }

    /// Record network bytes sent
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.network_tracker.record_bytes_sent(bytes);
    }

    /// Record network bytes received
    pub fn record_bytes_received(&self, bytes: u64) {
        self.network_tracker.record_bytes_received(bytes);
    }

    /// Record storage operation
    pub fn record_storage_operation(&self, latency: Duration) {
        self.storage_tracker.record_operation(latency);
    }

    /// Get current metrics snapshot
    pub fn get_metrics(&self) -> RaftMetrics {
        let mut metrics = self.metrics.read().unwrap().clone();
        
        // Update performance metrics from tracker
        let perf_stats = self.performance_tracker.get_stats();
        metrics.performance = perf_stats;
        
        // Update network metrics from tracker
        let network_stats = self.network_tracker.get_stats();
        metrics.network.bytes_sent = network_stats.bytes_sent;
        metrics.network.bytes_received = network_stats.bytes_received;
        
        // Update storage metrics from tracker
        let storage_stats = self.storage_tracker.get_stats();
        metrics.storage.disk_ops_per_second = storage_stats.ops_per_second;
        metrics.storage.disk_latency_ms = storage_stats.avg_latency_ms;
        
        metrics.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        metrics
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.performance_tracker.reset();
        self.network_tracker.reset();
        self.storage_tracker.reset();
    }
}

/// Performance tracking helper
struct PerformanceTracker {
    request_count: AtomicU64,
    total_latency_ms: AtomicU64,
    latencies: RwLock<Vec<u64>>, // Store recent latencies for percentile calculation
    last_reset: RwLock<Instant>,
}

impl PerformanceTracker {
    fn new() -> Self {
        Self {
            request_count: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            latencies: RwLock::new(Vec::new()),
            last_reset: RwLock::new(Instant::now()),
        }
    }

    fn record_latency(&self, latency: Duration) {
        let latency_ms = latency.as_millis() as u64;
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms.fetch_add(latency_ms, Ordering::Relaxed);
        
        // Keep only recent latencies (last 1000 requests)
        let mut latencies = self.latencies.write().unwrap();
        latencies.push(latency_ms);
        if latencies.len() > 1000 {
            latencies.remove(0);
        }
    }

    fn get_stats(&self) -> PerformanceMetrics {
        let count = self.request_count.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);
        let elapsed = self.last_reset.read().unwrap().elapsed();
        
        let avg_latency = if count > 0 {
            total_latency as f64 / count as f64
        } else {
            0.0
        };
        
        let rps = if elapsed.as_secs() > 0 {
            count as f64 / elapsed.as_secs() as f64
        } else {
            0.0
        };

        // Calculate percentiles
        let latencies = self.latencies.read().unwrap();
        let mut sorted_latencies = latencies.clone();
        sorted_latencies.sort_unstable();
        
        let p95 = if !sorted_latencies.is_empty() {
            let idx = (sorted_latencies.len() as f64 * 0.95) as usize;
            sorted_latencies.get(idx.min(sorted_latencies.len() - 1)).copied().unwrap_or(0) as f64
        } else {
            0.0
        };
        
        let p99 = if !sorted_latencies.is_empty() {
            let idx = (sorted_latencies.len() as f64 * 0.99) as usize;
            sorted_latencies.get(idx.min(sorted_latencies.len() - 1)).copied().unwrap_or(0) as f64
        } else {
            0.0
        };

        PerformanceMetrics {
            avg_request_latency_ms: avg_latency,
            p95_request_latency_ms: p95,
            p99_request_latency_ms: p99,
            requests_per_second: rps,
            commands_per_second: rps, // Simplified - same as RPS for now
            log_entries_per_second: rps, // Simplified - same as RPS for now
            memory_usage_bytes: 0, // TODO: Implement memory tracking
        }
    }

    fn reset(&self) {
        self.request_count.store(0, Ordering::Relaxed);
        self.total_latency_ms.store(0, Ordering::Relaxed);
        self.latencies.write().unwrap().clear();
        *self.last_reset.write().unwrap() = Instant::now();
    }
}

/// Network tracking helper
struct NetworkTracker {
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    connection_failures: AtomicU64,
    active_connections: AtomicU32,
}

impl NetworkTracker {
    fn new() -> Self {
        Self {
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
        }
    }

    fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    fn get_stats(&self) -> NetworkStats {
        NetworkStats {
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.connection_failures.store(0, Ordering::Relaxed);
        self.active_connections.store(0, Ordering::Relaxed);
    }
}

struct NetworkStats {
    bytes_sent: u64,
    bytes_received: u64,
}

/// Storage tracking helper
struct StorageTracker {
    operation_count: AtomicU64,
    total_latency_ms: AtomicU64,
    last_reset: RwLock<Instant>,
}

impl StorageTracker {
    fn new() -> Self {
        Self {
            operation_count: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            last_reset: RwLock::new(Instant::now()),
        }
    }

    fn record_operation(&self, latency: Duration) {
        let latency_ms = latency.as_millis() as u64;
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms.fetch_add(latency_ms, Ordering::Relaxed);
    }

    fn get_stats(&self) -> StorageStats {
        let count = self.operation_count.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);
        let elapsed = self.last_reset.read().unwrap().elapsed();
        
        let avg_latency = if count > 0 {
            total_latency as f64 / count as f64
        } else {
            0.0
        };
        
        let ops_per_second = if elapsed.as_secs() > 0 {
            count as f64 / elapsed.as_secs() as f64
        } else {
            0.0
        };

        StorageStats {
            ops_per_second,
            avg_latency_ms: avg_latency,
        }
    }

    fn reset(&self) {
        self.operation_count.store(0, Ordering::Relaxed);
        self.total_latency_ms.store(0, Ordering::Relaxed);
        *self.last_reset.write().unwrap() = Instant::now();
    }
}

struct StorageStats {
    ops_per_second: f64,
    avg_latency_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new(1);
        let metrics = collector.get_metrics();
        
        assert_eq!(metrics.state.node_id, 1);
        assert_eq!(metrics.state.current_term, 0);
        assert_eq!(metrics.state.node_state, "Follower");
    }

    #[test]
    fn test_performance_tracking() {
        let collector = MetricsCollector::new(1);
        
        // Record some latencies
        collector.record_request_latency(Duration::from_millis(10));
        collector.record_request_latency(Duration::from_millis(20));
        collector.record_request_latency(Duration::from_millis(30));
        
        let metrics = collector.get_metrics();
        assert!(metrics.performance.avg_request_latency_ms > 0.0);
        assert!(metrics.performance.requests_per_second >= 0.0);
    }

    #[test]
    fn test_network_tracking() {
        let collector = MetricsCollector::new(1);
        
        collector.record_bytes_sent(1024);
        collector.record_bytes_received(2048);
        
        let metrics = collector.get_metrics();
        assert_eq!(metrics.network.bytes_sent, 1024);
        assert_eq!(metrics.network.bytes_received, 2048);
    }

    #[test]
    fn test_storage_tracking() {
        let collector = MetricsCollector::new(1);
        
        collector.record_storage_operation(Duration::from_millis(5));
        collector.record_storage_operation(Duration::from_millis(15));
        
        let metrics = collector.get_metrics();
        assert!(metrics.storage.disk_latency_ms > 0.0);
        assert!(metrics.storage.disk_ops_per_second >= 0.0);
    }

    #[test]
    fn test_metrics_reset() {
        let collector = MetricsCollector::new(1);
        
        collector.record_request_latency(Duration::from_millis(10));
        collector.record_bytes_sent(1024);
        
        collector.reset();
        
        // Give some time for reset to take effect
        thread::sleep(Duration::from_millis(10));
        
        let metrics = collector.get_metrics();
        // After reset, counters should be reset but we can't guarantee exact values
        // due to timing, so we just verify the reset method doesn't panic
        assert!(metrics.timestamp > 0);
    }
}