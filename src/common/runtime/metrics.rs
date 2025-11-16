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

//! Metrics collection and monitoring for dual runtime architecture
//!
//! This module provides comprehensive metrics collection for monitoring
//! runtime performance, channel communication, and storage operations.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;

use crate::error::DualRuntimeError;

/// Comprehensive runtime metrics for monitoring and observability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeMetrics {
    /// Network runtime metrics
    pub network_metrics: NetworkRuntimeMetrics,
    /// Storage runtime metrics
    pub storage_metrics: StorageRuntimeMetrics,
    /// Channel communication metrics
    pub channel_metrics: ChannelMetrics,
    /// Overall system health status
    pub health_status: HealthStatus,
    /// Timestamp when metrics were collected
    pub timestamp: u64,
}

/// Network runtime specific metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkRuntimeMetrics {
    /// Number of active network connections
    pub active_connections: usize,
    /// Total requests processed
    pub total_requests: u64,
    /// Requests per second
    pub requests_per_second: f64,
    /// Average request processing time
    pub avg_request_time_ms: f64,
    /// 95th percentile request time
    pub p95_request_time_ms: f64,
    /// 99th percentile request time
    pub p99_request_time_ms: f64,
    /// Number of failed requests
    pub failed_requests: u64,
    /// Error rate percentage
    pub error_rate_percent: f64,
    /// Network I/O statistics
    pub network_io: NetworkIOStats,
}

/// Network I/O statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkIOStats {
    /// Total bytes received
    pub bytes_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Bytes per second received
    pub bytes_per_second_received: f64,
    /// Bytes per second sent
    pub bytes_per_second_sent: f64,
}

/// Storage runtime specific metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageRuntimeMetrics {
    /// Number of pending storage requests
    pub pending_requests: usize,
    /// Total storage operations processed
    pub total_operations: u64,
    /// Operations per second
    pub operations_per_second: f64,
    /// Average operation execution time
    pub avg_execution_time_ms: f64,
    /// 95th percentile execution time
    pub p95_execution_time_ms: f64,
    /// 99th percentile execution time
    pub p99_execution_time_ms: f64,
    /// Number of failed operations
    pub failed_operations: u64,
    /// RocksDB specific metrics
    pub rocksdb_metrics: RocksDBMetrics,
    /// Batch processing metrics
    pub batch_metrics: BatchMetrics,
}

/// RocksDB performance and health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDBMetrics {
    /// Number of keys in database
    pub total_keys: u64,
    /// Total database size in bytes
    pub total_size_bytes: u64,
    /// Number of SST files
    pub sst_file_count: u64,
    /// Number of memtables
    pub memtable_count: u64,
    /// Compaction statistics
    pub compaction_stats: CompactionStats,
    /// Flush statistics
    pub flush_stats: FlushStats,
    /// Write stall information
    pub write_stall_active: bool,
    /// Cache hit rate percentage
    pub cache_hit_rate_percent: f64,
    /// Bloom filter effectiveness
    pub bloom_filter_useful_percent: f64,
}

/// RocksDB compaction statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionStats {
    /// Number of compactions in progress
    pub compactions_pending: u64,
    /// Total compactions completed
    pub compactions_completed: u64,
    /// Bytes compacted
    pub bytes_compacted: u64,
    /// Average compaction time
    pub avg_compaction_time_ms: f64,
}

/// RocksDB flush statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushStats {
    /// Number of flushes pending
    pub flushes_pending: u64,
    /// Total flushes completed
    pub flushes_completed: u64,
    /// Bytes flushed
    pub bytes_flushed: u64,
    /// Average flush time
    pub avg_flush_time_ms: f64,
}

/// Batch processing metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchMetrics {
    /// Total batches processed
    pub total_batches: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Average batch processing time
    pub avg_batch_time_ms: f64,
    /// Batch efficiency (operations per batch)
    pub batch_efficiency: f64,
    /// Backpressure events
    pub backpressure_events: u64,
}

/// Channel communication metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelMetrics {
    /// Message passing statistics
    pub message_stats: MessageStats,
    /// Buffer utilization metrics
    pub buffer_metrics: BufferMetrics,
    /// Request/response correlation metrics
    pub correlation_metrics: CorrelationMetrics,
    /// Timeout and error statistics
    pub timeout_stats: TimeoutStats,
}

/// Message passing statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageStats {
    /// Total messages sent
    pub messages_sent: u64,
    /// Total messages received
    pub messages_received: u64,
    /// Messages per second
    pub messages_per_second: f64,
    /// Average message size in bytes
    pub avg_message_size_bytes: f64,
    /// Message processing latency
    pub avg_processing_latency_ms: f64,
}

/// Buffer utilization metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferMetrics {
    /// Current buffer utilization percentage
    pub buffer_utilization_percent: f64,
    /// Maximum buffer utilization seen
    pub max_buffer_utilization_percent: f64,
    /// Number of buffer full events
    pub buffer_full_events: u64,
    /// Backpressure activation count
    pub backpressure_activations: u64,
    /// Average time spent in backpressure
    pub avg_backpressure_time_ms: f64,
}

/// Request/response correlation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationMetrics {
    /// Total request/response pairs
    pub total_correlations: u64,
    /// Average correlation time (request to response)
    pub avg_correlation_time_ms: f64,
    /// 95th percentile correlation time
    pub p95_correlation_time_ms: f64,
    /// 99th percentile correlation time
    pub p99_correlation_time_ms: f64,
    /// Number of orphaned requests (no response)
    pub orphaned_requests: u64,
}

/// Timeout and error statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutStats {
    /// Total timeout events
    pub total_timeouts: u64,
    /// Timeout rate percentage
    pub timeout_rate_percent: f64,
    /// Average timeout duration
    pub avg_timeout_duration_ms: f64,
    /// Channel errors
    pub channel_errors: u64,
    /// Send failures
    pub send_failures: u64,
    /// Receive failures
    pub receive_failures: u64,
}

/// Overall system health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Network runtime health
    pub network_runtime_health: RuntimeHealth,
    /// Storage runtime health
    pub storage_runtime_health: RuntimeHealth,
    /// Channel health
    pub channel_health: ChannelHealth,
    /// Overall system health
    pub overall_health: SystemHealth,
    /// Last health check timestamp
    pub last_health_check: u64,
}

/// Runtime health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuntimeHealth {
    /// Runtime is healthy and operating normally
    Healthy,
    /// Runtime is degraded but still functional
    Degraded { reason: String },
    /// Runtime is unhealthy and may not be functional
    Unhealthy { reason: String },
}

/// Channel health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChannelHealth {
    /// Channel is healthy
    Healthy,
    /// Channel is experiencing backpressure
    Backpressure { utilization_percent: f64 },
    /// Channel is unhealthy
    Unhealthy { reason: String },
}

/// Overall system health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemHealth {
    /// All components are healthy
    Healthy,
    /// Some components are degraded
    Degraded { degraded_components: Vec<String> },
    /// System is unhealthy
    Unhealthy { unhealthy_components: Vec<String> },
}

/// Metrics collector that aggregates and tracks all runtime metrics
pub struct MetricsCollector {
    /// Network runtime metrics tracker
    network_tracker: Arc<NetworkMetricsTracker>,
    /// Storage runtime metrics tracker
    storage_tracker: Arc<StorageMetricsTracker>,
    /// Channel metrics tracker
    channel_tracker: Arc<ChannelMetricsTracker>,
    /// Health monitor
    health_monitor: Arc<HealthMonitor>,
    /// Metrics collection configuration
    config: MetricsConfig,
    /// Last metrics snapshot
    last_metrics: Arc<RwLock<Option<RuntimeMetrics>>>,
}

/// Configuration for metrics collection
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Metrics collection interval
    pub collection_interval: Duration,
    /// Number of samples to keep for percentile calculations
    pub sample_window_size: usize,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Enable detailed RocksDB metrics
    pub enable_rocksdb_metrics: bool,
    /// Enable batch processing metrics
    pub enable_batch_metrics: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(10),
            sample_window_size: 1000,
            health_check_interval: Duration::from_secs(30),
            enable_rocksdb_metrics: true,
            enable_batch_metrics: true,
        }
    }
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(config: MetricsConfig) -> Self {
        Self {
            network_tracker: Arc::new(NetworkMetricsTracker::new(config.sample_window_size)),
            storage_tracker: Arc::new(StorageMetricsTracker::new(config.sample_window_size)),
            channel_tracker: Arc::new(ChannelMetricsTracker::new(config.sample_window_size)),
            health_monitor: Arc::new(HealthMonitor::new()),
            config,
            last_metrics: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a metrics collector with default configuration
    pub fn with_defaults() -> Self {
        Self::new(MetricsConfig::default())
    }

    /// Start the metrics collection background task
    pub async fn start_collection(&self) -> Result<(), DualRuntimeError> {
        info!(
            "Starting metrics collection with interval: {:?}",
            self.config.collection_interval
        );

        let network_tracker = Arc::clone(&self.network_tracker);
        let storage_tracker = Arc::clone(&self.storage_tracker);
        let channel_tracker = Arc::clone(&self.channel_tracker);
        let health_monitor = Arc::clone(&self.health_monitor);
        let last_metrics = Arc::clone(&self.last_metrics);
        let collection_interval = self.config.collection_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(collection_interval);

            loop {
                interval.tick().await;

                debug!("Collecting runtime metrics");

                // Collect metrics from all trackers
                let network_metrics = network_tracker.get_metrics().await;
                let storage_metrics = storage_tracker.get_metrics().await;
                let channel_metrics = channel_tracker.get_metrics().await;
                let health_status = health_monitor.get_health_status().await;

                let runtime_metrics = RuntimeMetrics {
                    network_metrics,
                    storage_metrics,
                    channel_metrics,
                    health_status,
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                // Store the latest metrics
                *last_metrics.write().unwrap() = Some(runtime_metrics.clone());

                // Log key metrics periodically
                if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
                    if elapsed.as_secs() % 60 == 0 {
                        // Log every minute
                        info!(
                            "Runtime Metrics - Network RPS: {:.2}, Storage OPS: {:.2}, Channel Util: {:.1}%, Health: {:?}",
                            runtime_metrics.network_metrics.requests_per_second,
                            runtime_metrics.storage_metrics.operations_per_second,
                            runtime_metrics
                                .channel_metrics
                                .buffer_metrics
                                .buffer_utilization_percent,
                            runtime_metrics.health_status.overall_health
                        );
                    }
                }
            }
        });

        Ok(())
    }

    /// Get the latest metrics snapshot
    pub fn get_metrics(&self) -> Option<RuntimeMetrics> {
        self.last_metrics.read().unwrap().clone()
    }

    /// Get network metrics tracker for recording network events
    pub fn network_tracker(&self) -> &Arc<NetworkMetricsTracker> {
        &self.network_tracker
    }

    /// Get storage metrics tracker for recording storage events
    pub fn storage_tracker(&self) -> &Arc<StorageMetricsTracker> {
        &self.storage_tracker
    }

    /// Get channel metrics tracker for recording channel events
    pub fn channel_tracker(&self) -> &Arc<ChannelMetricsTracker> {
        &self.channel_tracker
    }

    /// Get health monitor for health status updates
    pub fn health_monitor(&self) -> &Arc<HealthMonitor> {
        &self.health_monitor
    }

    /// Reset all metrics
    pub async fn reset_metrics(&self) {
        info!("Resetting all runtime metrics");

        self.network_tracker.reset().await;
        self.storage_tracker.reset().await;
        self.channel_tracker.reset().await;
        self.health_monitor.reset().await;

        *self.last_metrics.write().unwrap() = None;
    }
}
/// Network metrics tracker for recording network runtime events
pub struct NetworkMetricsTracker {
    /// Connection count
    active_connections: AtomicUsize,
    /// Request statistics
    request_stats: Arc<Mutex<RequestStats>>,
    /// Network I/O statistics
    io_stats: Arc<Mutex<IOStats>>,
    /// Request latency samples
    latency_samples: Arc<Mutex<LatencySamples>>,
    /// Sample window size
    sample_window_size: usize,
}

/// Storage metrics tracker for recording storage runtime events
pub struct StorageMetricsTracker {
    /// Pending request count
    pending_requests: AtomicUsize,
    /// Operation statistics
    operation_stats: Arc<Mutex<OperationStats>>,
    /// RocksDB metrics
    rocksdb_metrics: Arc<Mutex<RocksDBMetricsData>>,
    /// Batch processing metrics
    batch_metrics: Arc<Mutex<BatchMetricsData>>,
    /// Operation latency samples
    latency_samples: Arc<Mutex<LatencySamples>>,
    /// Sample window size
    sample_window_size: usize,
}

/// Channel metrics tracker for recording channel communication events
pub struct ChannelMetricsTracker {
    /// Message statistics
    message_stats: Arc<Mutex<MessageStatsData>>,
    /// Buffer metrics
    buffer_metrics: Arc<Mutex<BufferMetricsData>>,
    /// Correlation metrics
    correlation_metrics: Arc<Mutex<CorrelationMetricsData>>,
    /// Timeout statistics
    timeout_stats: Arc<Mutex<TimeoutStatsData>>,
    /// Correlation time samples
    correlation_samples: Arc<Mutex<LatencySamples>>,
    /// Sample window size
    sample_window_size: usize,
}

/// Health monitor for tracking system health
pub struct HealthMonitor {
    /// Network runtime health
    network_health: Arc<RwLock<RuntimeHealth>>,
    /// Storage runtime health
    storage_health: Arc<RwLock<RuntimeHealth>>,
    /// Channel health
    channel_health: Arc<RwLock<ChannelHealth>>,
    /// Last health check time
    last_health_check: Arc<RwLock<Instant>>,
}

// Internal data structures for tracking

#[derive(Debug, Default)]
struct RequestStats {
    total_requests: u64,
    failed_requests: u64,
    start_time: Option<Instant>,
}

#[derive(Debug, Default)]
struct IOStats {
    bytes_received: u64,
    bytes_sent: u64,
    start_time: Option<Instant>,
}

#[derive(Debug, Default)]
struct OperationStats {
    total_operations: u64,
    failed_operations: u64,
    start_time: Option<Instant>,
}

#[derive(Debug, Default)]
struct RocksDBMetricsData {
    total_keys: u64,
    total_size_bytes: u64,
    sst_file_count: u64,
    memtable_count: u64,
    compaction_stats: CompactionStatsData,
    flush_stats: FlushStatsData,
    write_stall_active: bool,
    cache_hit_rate_percent: f64,
    bloom_filter_useful_percent: f64,
}

#[derive(Debug, Default)]
struct CompactionStatsData {
    compactions_pending: u64,
    compactions_completed: u64,
    bytes_compacted: u64,
    total_compaction_time_ms: u64,
}

#[derive(Debug, Default)]
struct FlushStatsData {
    flushes_pending: u64,
    flushes_completed: u64,
    bytes_flushed: u64,
    total_flush_time_ms: u64,
}

#[derive(Debug, Default)]
struct BatchMetricsData {
    total_batches: u64,
    total_operations_in_batches: u64,
    total_batch_time_ms: u64,
    backpressure_events: u64,
}

#[derive(Debug, Default)]
struct MessageStatsData {
    messages_sent: u64,
    messages_received: u64,
    total_message_size_bytes: u64,
    total_processing_time_ms: u64,
    start_time: Option<Instant>,
}

#[derive(Debug, Default)]
struct BufferMetricsData {
    current_utilization_percent: f64,
    max_utilization_percent: f64,
    buffer_full_events: u64,
    backpressure_activations: u64,
    total_backpressure_time_ms: u64,
}

#[derive(Debug, Default)]
struct CorrelationMetricsData {
    total_correlations: u64,
    orphaned_requests: u64,
}

#[derive(Debug, Default)]
struct TimeoutStatsData {
    total_timeouts: u64,
    total_timeout_duration_ms: u64,
    channel_errors: u64,
    send_failures: u64,
    receive_failures: u64,
}

#[derive(Debug, Default)]
struct LatencySamples {
    samples: Vec<u64>,
    total_samples: u64,
}

impl LatencySamples {
    fn add_sample(&mut self, latency_ms: u64, max_samples: usize) {
        self.samples.push(latency_ms);
        self.total_samples += 1;

        // Keep only the most recent samples
        if self.samples.len() > max_samples {
            self.samples.remove(0);
        }
    }

    fn calculate_percentiles(&self) -> (f64, f64, f64) {
        if self.samples.is_empty() {
            return (0.0, 0.0, 0.0);
        }

        let mut sorted_samples = self.samples.clone();
        sorted_samples.sort_unstable();

        let avg = sorted_samples.iter().sum::<u64>() as f64 / sorted_samples.len() as f64;

        // Use the standard percentile formula: index = (n - 1) * percentile
        let p95_idx = ((sorted_samples.len() - 1) as f64 * 0.95).round() as usize;
        let p99_idx = ((sorted_samples.len() - 1) as f64 * 0.99).round() as usize;

        let p95 = sorted_samples
            .get(p95_idx.min(sorted_samples.len() - 1))
            .copied()
            .unwrap_or(0) as f64;
        let p99 = sorted_samples
            .get(p99_idx.min(sorted_samples.len() - 1))
            .copied()
            .unwrap_or(0) as f64;

        (avg, p95, p99)
    }
}

// NetworkMetricsTracker implementation
impl NetworkMetricsTracker {
    pub fn new(sample_window_size: usize) -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            request_stats: Arc::new(Mutex::new(RequestStats {
                start_time: Some(Instant::now()),
                ..Default::default()
            })),
            io_stats: Arc::new(Mutex::new(IOStats {
                start_time: Some(Instant::now()),
                ..Default::default()
            })),
            latency_samples: Arc::new(Mutex::new(LatencySamples::default())),
            sample_window_size,
        }
    }

    /// Record a new network connection
    pub fn record_connection_established(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a network connection closed
    pub fn record_connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a successful request
    pub async fn record_request_success(&self, processing_time: Duration) {
        let mut stats = self.request_stats.lock().await;
        stats.total_requests += 1;

        let mut samples = self.latency_samples.lock().await;
        samples.add_sample(processing_time.as_millis() as u64, self.sample_window_size);
    }

    /// Record a failed request
    pub async fn record_request_failure(&self, processing_time: Duration) {
        let mut stats = self.request_stats.lock().await;
        stats.total_requests += 1;
        stats.failed_requests += 1;

        let mut samples = self.latency_samples.lock().await;
        samples.add_sample(processing_time.as_millis() as u64, self.sample_window_size);
    }

    /// Record bytes received
    pub async fn record_bytes_received(&self, bytes: u64) {
        let mut stats = self.io_stats.lock().await;
        stats.bytes_received += bytes;
    }

    /// Record bytes sent
    pub async fn record_bytes_sent(&self, bytes: u64) {
        let mut stats = self.io_stats.lock().await;
        stats.bytes_sent += bytes;
    }

    /// Get current network metrics
    pub async fn get_metrics(&self) -> NetworkRuntimeMetrics {
        let active_connections = self.active_connections.load(Ordering::Relaxed);
        let request_stats = self.request_stats.lock().await;
        let io_stats = self.io_stats.lock().await;
        let samples = self.latency_samples.lock().await;

        let elapsed = request_stats
            .start_time
            .map(|start| start.elapsed())
            .unwrap_or(Duration::from_secs(1));

        let requests_per_second = if elapsed.as_secs() > 0 {
            request_stats.total_requests as f64 / elapsed.as_secs() as f64
        } else {
            0.0
        };

        let error_rate_percent = if request_stats.total_requests > 0 {
            (request_stats.failed_requests as f64 / request_stats.total_requests as f64) * 100.0
        } else {
            0.0
        };

        let (avg_request_time, p95_request_time, p99_request_time) =
            samples.calculate_percentiles();

        let io_elapsed = io_stats
            .start_time
            .map(|start| start.elapsed())
            .unwrap_or(Duration::from_secs(1));

        let bytes_per_second_received = if io_elapsed.as_secs() > 0 {
            io_stats.bytes_received as f64 / io_elapsed.as_secs() as f64
        } else {
            0.0
        };

        let bytes_per_second_sent = if io_elapsed.as_secs() > 0 {
            io_stats.bytes_sent as f64 / io_elapsed.as_secs() as f64
        } else {
            0.0
        };

        NetworkRuntimeMetrics {
            active_connections,
            total_requests: request_stats.total_requests,
            requests_per_second,
            avg_request_time_ms: avg_request_time,
            p95_request_time_ms: p95_request_time,
            p99_request_time_ms: p99_request_time,
            failed_requests: request_stats.failed_requests,
            error_rate_percent,
            network_io: NetworkIOStats {
                bytes_received: io_stats.bytes_received,
                bytes_sent: io_stats.bytes_sent,
                bytes_per_second_received,
                bytes_per_second_sent,
            },
        }
    }

    /// Reset network metrics
    pub async fn reset(&self) {
        self.active_connections.store(0, Ordering::Relaxed);

        let mut request_stats = self.request_stats.lock().await;
        *request_stats = RequestStats {
            start_time: Some(Instant::now()),
            ..Default::default()
        };

        let mut io_stats = self.io_stats.lock().await;
        *io_stats = IOStats {
            start_time: Some(Instant::now()),
            ..Default::default()
        };

        let mut samples = self.latency_samples.lock().await;
        *samples = LatencySamples::default();
    }
}

// StorageMetricsTracker implementation
impl StorageMetricsTracker {
    pub fn new(sample_window_size: usize) -> Self {
        Self {
            pending_requests: AtomicUsize::new(0),
            operation_stats: Arc::new(Mutex::new(OperationStats {
                start_time: Some(Instant::now()),
                ..Default::default()
            })),
            rocksdb_metrics: Arc::new(Mutex::new(RocksDBMetricsData::default())),
            batch_metrics: Arc::new(Mutex::new(BatchMetricsData::default())),
            latency_samples: Arc::new(Mutex::new(LatencySamples::default())),
            sample_window_size,
        }
    }

    /// Record a storage operation started
    pub fn record_operation_started(&self) {
        self.pending_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful storage operation
    pub async fn record_operation_success(&self, execution_time: Duration) {
        self.pending_requests.fetch_sub(1, Ordering::Relaxed);

        let mut stats = self.operation_stats.lock().await;
        stats.total_operations += 1;

        let mut samples = self.latency_samples.lock().await;
        samples.add_sample(execution_time.as_millis() as u64, self.sample_window_size);
    }

    /// Record a failed storage operation
    pub async fn record_operation_failure(&self, execution_time: Duration) {
        self.pending_requests.fetch_sub(1, Ordering::Relaxed);

        let mut stats = self.operation_stats.lock().await;
        stats.total_operations += 1;
        stats.failed_operations += 1;

        let mut samples = self.latency_samples.lock().await;
        samples.add_sample(execution_time.as_millis() as u64, self.sample_window_size);
    }

    /// Update RocksDB metrics
    pub async fn update_rocksdb_metrics(
        &self,
        total_keys: u64,
        total_size_bytes: u64,
        sst_file_count: u64,
        memtable_count: u64,
        write_stall_active: bool,
        cache_hit_rate_percent: f64,
        bloom_filter_useful_percent: f64,
    ) {
        let mut metrics = self.rocksdb_metrics.lock().await;
        metrics.total_keys = total_keys;
        metrics.total_size_bytes = total_size_bytes;
        metrics.sst_file_count = sst_file_count;
        metrics.memtable_count = memtable_count;
        metrics.write_stall_active = write_stall_active;
        metrics.cache_hit_rate_percent = cache_hit_rate_percent;
        metrics.bloom_filter_useful_percent = bloom_filter_useful_percent;
    }

    /// Record compaction event
    pub async fn record_compaction_started(&self) {
        let mut metrics = self.rocksdb_metrics.lock().await;
        metrics.compaction_stats.compactions_pending += 1;
    }

    /// Record compaction completed
    pub async fn record_compaction_completed(&self, bytes_compacted: u64, duration: Duration) {
        let mut metrics = self.rocksdb_metrics.lock().await;
        metrics.compaction_stats.compactions_pending = metrics
            .compaction_stats
            .compactions_pending
            .saturating_sub(1);
        metrics.compaction_stats.compactions_completed += 1;
        metrics.compaction_stats.bytes_compacted += bytes_compacted;
        metrics.compaction_stats.total_compaction_time_ms += duration.as_millis() as u64;
    }

    /// Record flush event
    pub async fn record_flush_started(&self) {
        let mut metrics = self.rocksdb_metrics.lock().await;
        metrics.flush_stats.flushes_pending += 1;
    }

    /// Record flush completed
    pub async fn record_flush_completed(&self, bytes_flushed: u64, duration: Duration) {
        let mut metrics = self.rocksdb_metrics.lock().await;
        metrics.flush_stats.flushes_pending = metrics.flush_stats.flushes_pending.saturating_sub(1);
        metrics.flush_stats.flushes_completed += 1;
        metrics.flush_stats.bytes_flushed += bytes_flushed;
        metrics.flush_stats.total_flush_time_ms += duration.as_millis() as u64;
    }

    /// Record batch processing
    pub async fn record_batch_processed(&self, batch_size: usize, processing_time: Duration) {
        let mut metrics = self.batch_metrics.lock().await;
        metrics.total_batches += 1;
        metrics.total_operations_in_batches += batch_size as u64;
        metrics.total_batch_time_ms += processing_time.as_millis() as u64;
    }

    /// Record backpressure event
    pub async fn record_backpressure_event(&self) {
        let mut metrics = self.batch_metrics.lock().await;
        metrics.backpressure_events += 1;
    }

    /// Get current storage metrics
    pub async fn get_metrics(&self) -> StorageRuntimeMetrics {
        let pending_requests = self.pending_requests.load(Ordering::Relaxed);
        let operation_stats = self.operation_stats.lock().await;
        let rocksdb_metrics = self.rocksdb_metrics.lock().await;
        let batch_metrics = self.batch_metrics.lock().await;
        let samples = self.latency_samples.lock().await;

        let elapsed = operation_stats
            .start_time
            .map(|start| start.elapsed())
            .unwrap_or(Duration::from_secs(1));

        let operations_per_second = if elapsed.as_secs() > 0 {
            operation_stats.total_operations as f64 / elapsed.as_secs() as f64
        } else {
            0.0
        };

        let (avg_execution_time, p95_execution_time, p99_execution_time) =
            samples.calculate_percentiles();

        // Calculate RocksDB metrics
        let avg_compaction_time = if rocksdb_metrics.compaction_stats.compactions_completed > 0 {
            rocksdb_metrics.compaction_stats.total_compaction_time_ms as f64
                / rocksdb_metrics.compaction_stats.compactions_completed as f64
        } else {
            0.0
        };

        let avg_flush_time = if rocksdb_metrics.flush_stats.flushes_completed > 0 {
            rocksdb_metrics.flush_stats.total_flush_time_ms as f64
                / rocksdb_metrics.flush_stats.flushes_completed as f64
        } else {
            0.0
        };

        // Calculate batch metrics
        let avg_batch_size = if batch_metrics.total_batches > 0 {
            batch_metrics.total_operations_in_batches as f64 / batch_metrics.total_batches as f64
        } else {
            0.0
        };

        let avg_batch_time = if batch_metrics.total_batches > 0 {
            batch_metrics.total_batch_time_ms as f64 / batch_metrics.total_batches as f64
        } else {
            0.0
        };

        let batch_efficiency = if batch_metrics.total_batches > 0 {
            batch_metrics.total_operations_in_batches as f64 / batch_metrics.total_batches as f64
        } else {
            0.0
        };

        StorageRuntimeMetrics {
            pending_requests,
            total_operations: operation_stats.total_operations,
            operations_per_second,
            avg_execution_time_ms: avg_execution_time,
            p95_execution_time_ms: p95_execution_time,
            p99_execution_time_ms: p99_execution_time,
            failed_operations: operation_stats.failed_operations,
            rocksdb_metrics: RocksDBMetrics {
                total_keys: rocksdb_metrics.total_keys,
                total_size_bytes: rocksdb_metrics.total_size_bytes,
                sst_file_count: rocksdb_metrics.sst_file_count,
                memtable_count: rocksdb_metrics.memtable_count,
                compaction_stats: CompactionStats {
                    compactions_pending: rocksdb_metrics.compaction_stats.compactions_pending,
                    compactions_completed: rocksdb_metrics.compaction_stats.compactions_completed,
                    bytes_compacted: rocksdb_metrics.compaction_stats.bytes_compacted,
                    avg_compaction_time_ms: avg_compaction_time,
                },
                flush_stats: FlushStats {
                    flushes_pending: rocksdb_metrics.flush_stats.flushes_pending,
                    flushes_completed: rocksdb_metrics.flush_stats.flushes_completed,
                    bytes_flushed: rocksdb_metrics.flush_stats.bytes_flushed,
                    avg_flush_time_ms: avg_flush_time,
                },
                write_stall_active: rocksdb_metrics.write_stall_active,
                cache_hit_rate_percent: rocksdb_metrics.cache_hit_rate_percent,
                bloom_filter_useful_percent: rocksdb_metrics.bloom_filter_useful_percent,
            },
            batch_metrics: BatchMetrics {
                total_batches: batch_metrics.total_batches,
                avg_batch_size,
                avg_batch_time_ms: avg_batch_time,
                batch_efficiency,
                backpressure_events: batch_metrics.backpressure_events,
            },
        }
    }

    /// Reset storage metrics
    pub async fn reset(&self) {
        self.pending_requests.store(0, Ordering::Relaxed);

        let mut operation_stats = self.operation_stats.lock().await;
        *operation_stats = OperationStats {
            start_time: Some(Instant::now()),
            ..Default::default()
        };

        let mut rocksdb_metrics = self.rocksdb_metrics.lock().await;
        *rocksdb_metrics = RocksDBMetricsData::default();

        let mut batch_metrics = self.batch_metrics.lock().await;
        *batch_metrics = BatchMetricsData::default();

        let mut samples = self.latency_samples.lock().await;
        *samples = LatencySamples::default();
    }
}

// ChannelMetricsTracker implementation
impl ChannelMetricsTracker {
    pub fn new(sample_window_size: usize) -> Self {
        Self {
            message_stats: Arc::new(Mutex::new(MessageStatsData {
                start_time: Some(Instant::now()),
                ..Default::default()
            })),
            buffer_metrics: Arc::new(Mutex::new(BufferMetricsData::default())),
            correlation_metrics: Arc::new(Mutex::new(CorrelationMetricsData::default())),
            timeout_stats: Arc::new(Mutex::new(TimeoutStatsData::default())),
            correlation_samples: Arc::new(Mutex::new(LatencySamples::default())),
            sample_window_size,
        }
    }

    /// Record a message sent
    pub async fn record_message_sent(&self, message_size_bytes: u64) {
        let mut stats = self.message_stats.lock().await;
        stats.messages_sent += 1;
        stats.total_message_size_bytes += message_size_bytes;
    }

    /// Record a message received
    pub async fn record_message_received(&self, processing_time: Duration) {
        let mut stats = self.message_stats.lock().await;
        stats.messages_received += 1;
        stats.total_processing_time_ms += processing_time.as_millis() as u64;
    }

    /// Update buffer utilization
    pub async fn update_buffer_utilization(&self, utilization_percent: f64) {
        let mut metrics = self.buffer_metrics.lock().await;
        metrics.current_utilization_percent = utilization_percent;
        metrics.max_utilization_percent = metrics.max_utilization_percent.max(utilization_percent);
    }

    /// Record buffer full event
    pub async fn record_buffer_full_event(&self) {
        let mut metrics = self.buffer_metrics.lock().await;
        metrics.buffer_full_events += 1;
    }

    /// Record backpressure activation
    pub async fn record_backpressure_activation(&self, duration: Duration) {
        let mut metrics = self.buffer_metrics.lock().await;
        metrics.backpressure_activations += 1;
        metrics.total_backpressure_time_ms += duration.as_millis() as u64;
    }

    /// Record request/response correlation
    pub async fn record_correlation(&self, correlation_time: Duration) {
        let mut metrics = self.correlation_metrics.lock().await;
        metrics.total_correlations += 1;

        let mut samples = self.correlation_samples.lock().await;
        samples.add_sample(correlation_time.as_millis() as u64, self.sample_window_size);
    }

    /// Record orphaned request (no response received)
    pub async fn record_orphaned_request(&self) {
        let mut metrics = self.correlation_metrics.lock().await;
        metrics.orphaned_requests += 1;
    }

    /// Record timeout event
    pub async fn record_timeout(&self, timeout_duration: Duration) {
        let mut stats = self.timeout_stats.lock().await;
        stats.total_timeouts += 1;
        stats.total_timeout_duration_ms += timeout_duration.as_millis() as u64;
    }

    /// Record channel error
    pub async fn record_channel_error(&self) {
        let mut stats = self.timeout_stats.lock().await;
        stats.channel_errors += 1;
    }

    /// Record send failure
    pub async fn record_send_failure(&self) {
        let mut stats = self.timeout_stats.lock().await;
        stats.send_failures += 1;
    }

    /// Record receive failure
    pub async fn record_receive_failure(&self) {
        let mut stats = self.timeout_stats.lock().await;
        stats.receive_failures += 1;
    }

    /// Get current channel metrics
    pub async fn get_metrics(&self) -> ChannelMetrics {
        let message_stats = self.message_stats.lock().await;
        let buffer_metrics = self.buffer_metrics.lock().await;
        let correlation_metrics = self.correlation_metrics.lock().await;
        let timeout_stats = self.timeout_stats.lock().await;
        let correlation_samples = self.correlation_samples.lock().await;

        let elapsed = message_stats
            .start_time
            .map(|start| start.elapsed())
            .unwrap_or(Duration::from_secs(1));

        let messages_per_second = if elapsed.as_secs() > 0 {
            (message_stats.messages_sent + message_stats.messages_received) as f64
                / elapsed.as_secs() as f64
        } else {
            0.0
        };

        let avg_message_size = if message_stats.messages_sent > 0 {
            message_stats.total_message_size_bytes as f64 / message_stats.messages_sent as f64
        } else {
            0.0
        };

        let avg_processing_latency = if message_stats.messages_received > 0 {
            message_stats.total_processing_time_ms as f64 / message_stats.messages_received as f64
        } else {
            0.0
        };

        let avg_backpressure_time = if buffer_metrics.backpressure_activations > 0 {
            buffer_metrics.total_backpressure_time_ms as f64
                / buffer_metrics.backpressure_activations as f64
        } else {
            0.0
        };

        let (avg_correlation_time, p95_correlation_time, p99_correlation_time) =
            correlation_samples.calculate_percentiles();

        let timeout_rate_percent = if correlation_metrics.total_correlations > 0 {
            (timeout_stats.total_timeouts as f64 / correlation_metrics.total_correlations as f64)
                * 100.0
        } else {
            0.0
        };

        let avg_timeout_duration = if timeout_stats.total_timeouts > 0 {
            timeout_stats.total_timeout_duration_ms as f64 / timeout_stats.total_timeouts as f64
        } else {
            0.0
        };

        ChannelMetrics {
            message_stats: MessageStats {
                messages_sent: message_stats.messages_sent,
                messages_received: message_stats.messages_received,
                messages_per_second,
                avg_message_size_bytes: avg_message_size,
                avg_processing_latency_ms: avg_processing_latency,
            },
            buffer_metrics: BufferMetrics {
                buffer_utilization_percent: buffer_metrics.current_utilization_percent,
                max_buffer_utilization_percent: buffer_metrics.max_utilization_percent,
                buffer_full_events: buffer_metrics.buffer_full_events,
                backpressure_activations: buffer_metrics.backpressure_activations,
                avg_backpressure_time_ms: avg_backpressure_time,
            },
            correlation_metrics: CorrelationMetrics {
                total_correlations: correlation_metrics.total_correlations,
                avg_correlation_time_ms: avg_correlation_time,
                p95_correlation_time_ms: p95_correlation_time,
                p99_correlation_time_ms: p99_correlation_time,
                orphaned_requests: correlation_metrics.orphaned_requests,
            },
            timeout_stats: TimeoutStats {
                total_timeouts: timeout_stats.total_timeouts,
                timeout_rate_percent,
                avg_timeout_duration_ms: avg_timeout_duration,
                channel_errors: timeout_stats.channel_errors,
                send_failures: timeout_stats.send_failures,
                receive_failures: timeout_stats.receive_failures,
            },
        }
    }

    /// Reset channel metrics
    pub async fn reset(&self) {
        let mut message_stats = self.message_stats.lock().await;
        *message_stats = MessageStatsData {
            start_time: Some(Instant::now()),
            ..Default::default()
        };

        let mut buffer_metrics = self.buffer_metrics.lock().await;
        *buffer_metrics = BufferMetricsData::default();

        let mut correlation_metrics = self.correlation_metrics.lock().await;
        *correlation_metrics = CorrelationMetricsData::default();

        let mut timeout_stats = self.timeout_stats.lock().await;
        *timeout_stats = TimeoutStatsData::default();

        let mut correlation_samples = self.correlation_samples.lock().await;
        *correlation_samples = LatencySamples::default();
    }
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// HealthMonitor implementation
impl HealthMonitor {
    pub fn new() -> Self {
        Self {
            network_health: Arc::new(RwLock::new(RuntimeHealth::Healthy)),
            storage_health: Arc::new(RwLock::new(RuntimeHealth::Healthy)),
            channel_health: Arc::new(RwLock::new(ChannelHealth::Healthy)),
            last_health_check: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Update network runtime health
    pub fn update_network_health(&self, health: RuntimeHealth) {
        if let Ok(mut current_health) = self.network_health.write() {
            *current_health = health;
        }
        self.update_last_health_check();
    }

    /// Update storage runtime health
    pub fn update_storage_health(&self, health: RuntimeHealth) {
        if let Ok(mut current_health) = self.storage_health.write() {
            *current_health = health;
        }
        self.update_last_health_check();
    }

    /// Update channel health
    pub fn update_channel_health(&self, health: ChannelHealth) {
        if let Ok(mut current_health) = self.channel_health.write() {
            *current_health = health;
        }
        self.update_last_health_check();
    }

    /// Update channel health based on buffer utilization
    pub fn update_channel_health_from_utilization(&self, utilization_percent: f64) {
        let health = if utilization_percent >= 90.0 {
            ChannelHealth::Unhealthy {
                reason: format!("Buffer utilization too high: {:.1}%", utilization_percent),
            }
        } else if utilization_percent >= 80.0 {
            ChannelHealth::Backpressure {
                utilization_percent,
            }
        } else {
            ChannelHealth::Healthy
        };

        self.update_channel_health(health);
    }

    /// Get current health status
    pub async fn get_health_status(&self) -> HealthStatus {
        let network_health = self.network_health.read().unwrap().clone();
        let storage_health = self.storage_health.read().unwrap().clone();
        let channel_health = self.channel_health.read().unwrap().clone();
        let _last_check = *self.last_health_check.read().unwrap();

        // Determine overall system health
        let overall_health =
            self.calculate_overall_health(&network_health, &storage_health, &channel_health);

        HealthStatus {
            network_runtime_health: network_health,
            storage_runtime_health: storage_health,
            channel_health,
            overall_health,
            last_health_check: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Calculate overall system health based on component health
    fn calculate_overall_health(
        &self,
        network_health: &RuntimeHealth,
        storage_health: &RuntimeHealth,
        channel_health: &ChannelHealth,
    ) -> SystemHealth {
        let mut degraded_components = Vec::new();
        let mut unhealthy_components = Vec::new();

        // Check network health
        match network_health {
            RuntimeHealth::Degraded { reason } => {
                degraded_components.push(format!("Network: {}", reason));
            }
            RuntimeHealth::Unhealthy { reason } => {
                unhealthy_components.push(format!("Network: {}", reason));
            }
            RuntimeHealth::Healthy => {}
        }

        // Check storage health
        match storage_health {
            RuntimeHealth::Degraded { reason } => {
                degraded_components.push(format!("Storage: {}", reason));
            }
            RuntimeHealth::Unhealthy { reason } => {
                unhealthy_components.push(format!("Storage: {}", reason));
            }
            RuntimeHealth::Healthy => {}
        }

        // Check channel health
        match channel_health {
            ChannelHealth::Backpressure {
                utilization_percent,
            } => {
                degraded_components
                    .push(format!("Channel: Backpressure {:.1}%", utilization_percent));
            }
            ChannelHealth::Unhealthy { reason } => {
                unhealthy_components.push(format!("Channel: {}", reason));
            }
            ChannelHealth::Healthy => {}
        }

        // Determine overall health
        if !unhealthy_components.is_empty() {
            SystemHealth::Unhealthy {
                unhealthy_components,
            }
        } else if !degraded_components.is_empty() {
            SystemHealth::Degraded {
                degraded_components,
            }
        } else {
            SystemHealth::Healthy
        }
    }

    /// Update last health check timestamp
    fn update_last_health_check(&self) {
        if let Ok(mut last_check) = self.last_health_check.write() {
            *last_check = Instant::now();
        }
    }

    /// Reset health monitor
    pub async fn reset(&self) {
        self.update_network_health(RuntimeHealth::Healthy);
        self.update_storage_health(RuntimeHealth::Healthy);
        self.update_channel_health(ChannelHealth::Healthy);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_metrics_collector_creation() {
        let collector = MetricsCollector::with_defaults();
        assert!(collector.get_metrics().is_none());
    }

    #[tokio::test]
    async fn test_network_metrics_tracking() {
        let tracker = NetworkMetricsTracker::new(100);

        tracker.record_connection_established();
        tracker.record_connection_established();
        tracker
            .record_request_success(Duration::from_millis(10))
            .await;
        tracker
            .record_request_failure(Duration::from_millis(20))
            .await;
        tracker.record_bytes_sent(1024).await;
        tracker.record_bytes_received(2048).await;

        let metrics = tracker.get_metrics().await;
        assert_eq!(metrics.active_connections, 2);
        assert_eq!(metrics.total_requests, 2);
        assert_eq!(metrics.failed_requests, 1);
        assert_eq!(metrics.error_rate_percent, 50.0);
        assert_eq!(metrics.network_io.bytes_sent, 1024);
        assert_eq!(metrics.network_io.bytes_received, 2048);
    }

    #[tokio::test]
    async fn test_storage_metrics_tracking() {
        let tracker = StorageMetricsTracker::new(100);

        tracker.record_operation_started();
        tracker
            .record_operation_success(Duration::from_millis(5))
            .await;
        tracker
            .record_operation_failure(Duration::from_millis(15))
            .await;
        tracker
            .record_batch_processed(10, Duration::from_millis(50))
            .await;
        tracker.record_backpressure_event().await;

        let metrics = tracker.get_metrics().await;
        assert_eq!(metrics.total_operations, 2);
        assert_eq!(metrics.failed_operations, 1);
        assert_eq!(metrics.batch_metrics.total_batches, 1);
        assert_eq!(metrics.batch_metrics.backpressure_events, 1);
    }

    #[tokio::test]
    async fn test_channel_metrics_tracking() {
        let tracker = ChannelMetricsTracker::new(100);

        tracker.record_message_sent(256).await;
        tracker
            .record_message_received(Duration::from_millis(2))
            .await;
        tracker.update_buffer_utilization(75.0).await;
        tracker.record_correlation(Duration::from_millis(10)).await;
        tracker.record_timeout(Duration::from_millis(1000)).await;

        let metrics = tracker.get_metrics().await;
        assert_eq!(metrics.message_stats.messages_sent, 1);
        assert_eq!(metrics.message_stats.messages_received, 1);
        assert_eq!(metrics.buffer_metrics.buffer_utilization_percent, 75.0);
        assert_eq!(metrics.correlation_metrics.total_correlations, 1);
        assert_eq!(metrics.timeout_stats.total_timeouts, 1);
    }

    #[tokio::test]
    async fn test_health_monitor() {
        let monitor = HealthMonitor::new();

        // Initially healthy
        let status = monitor.get_health_status().await;
        assert!(matches!(status.overall_health, SystemHealth::Healthy));

        // Update to degraded
        monitor.update_network_health(RuntimeHealth::Degraded {
            reason: "High latency".to_string(),
        });

        let status = monitor.get_health_status().await;
        assert!(matches!(
            status.overall_health,
            SystemHealth::Degraded { .. }
        ));

        // Update to unhealthy
        monitor.update_storage_health(RuntimeHealth::Unhealthy {
            reason: "Database connection lost".to_string(),
        });

        let status = monitor.get_health_status().await;
        assert!(matches!(
            status.overall_health,
            SystemHealth::Unhealthy { .. }
        ));
    }

    #[tokio::test]
    async fn test_latency_samples_percentiles() {
        let mut samples = LatencySamples::default();

        // Add some sample latencies
        for i in 1..=100 {
            samples.add_sample(i, 1000);
        }

        let (avg, p95, p99) = samples.calculate_percentiles();
        assert!(avg > 0.0);
        assert!(p95 >= avg);
        assert!(p99 >= p95);
        assert_eq!(p95, 95.0);
        assert_eq!(p99, 99.0);
    }
}

/// Health check endpoints for HTTP monitoring
pub struct HealthCheckEndpoints {
    /// Metrics collector for health data
    metrics_collector: Arc<MetricsCollector>,
    /// Health check configuration
    config: HealthCheckConfig,
}

/// Configuration for health check endpoints
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Port for health check server
    pub port: u16,
    /// Enable detailed health information
    pub enable_detailed_info: bool,
    /// Health check timeout
    pub timeout: Duration,
    /// Custom health check thresholds
    pub thresholds: HealthThresholds,
}

/// Health check thresholds for determining health status
#[derive(Debug, Clone)]
pub struct HealthThresholds {
    /// Maximum acceptable error rate percentage
    pub max_error_rate_percent: f64,
    /// Maximum acceptable response time in milliseconds
    pub max_response_time_ms: f64,
    /// Maximum acceptable buffer utilization percentage
    pub max_buffer_utilization_percent: f64,
    /// Maximum acceptable pending requests
    pub max_pending_requests: usize,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            enable_detailed_info: true,
            timeout: Duration::from_secs(5),
            thresholds: HealthThresholds::default(),
        }
    }
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            max_error_rate_percent: 5.0,
            max_response_time_ms: 1000.0,
            max_buffer_utilization_percent: 80.0,
            max_pending_requests: 1000,
        }
    }
}

/// Health check response for HTTP endpoints
#[derive(Debug, Serialize)]
pub struct HealthCheckResponse {
    /// Overall health status
    pub status: String,
    /// Timestamp of health check
    pub timestamp: u64,
    /// Detailed component health (if enabled)
    pub components: Option<ComponentHealth>,
    /// Health check version
    pub version: String,
}

/// Detailed component health information
#[derive(Debug, Serialize)]
pub struct ComponentHealth {
    /// Network runtime health
    pub network_runtime: ComponentStatus,
    /// Storage runtime health
    pub storage_runtime: ComponentStatus,
    /// Channel health
    pub channel: ComponentStatus,
}

/// Individual component status
#[derive(Debug, Serialize)]
pub struct ComponentStatus {
    /// Component status (healthy, degraded, unhealthy)
    pub status: String,
    /// Status message
    pub message: Option<String>,
    /// Component metrics (if detailed info enabled)
    pub metrics: Option<serde_json::Value>,
}

impl HealthCheckEndpoints {
    /// Create new health check endpoints
    pub fn new(metrics_collector: Arc<MetricsCollector>, config: HealthCheckConfig) -> Self {
        Self {
            metrics_collector,
            config,
        }
    }

    /// Create health check endpoints with default configuration
    pub fn with_defaults(metrics_collector: Arc<MetricsCollector>) -> Self {
        Self::new(metrics_collector, HealthCheckConfig::default())
    }

    /// Start the health check HTTP server
    pub async fn start_server(&self) -> Result<(), DualRuntimeError> {
        info!("Starting health check server on port {}", self.config.port);

        // Note: This is a simplified implementation. In a real scenario, you would use
        // a proper HTTP server like axum, warp, or hyper

        let metrics_collector = Arc::clone(&self.metrics_collector);
        let config = self.config.clone();

        tokio::spawn(async move {
            // Simulate HTTP server - in real implementation, use proper HTTP framework
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                // Simulate health check requests
                let _health_response = Self::handle_health_check(&metrics_collector, &config).await;
                let _readiness_response =
                    Self::handle_readiness_check(&metrics_collector, &config).await;
                let _liveness_response =
                    Self::handle_liveness_check(&metrics_collector, &config).await;

                // In real implementation, these would be HTTP endpoint handlers
            }
        });

        Ok(())
    }

    /// Handle /health endpoint
    pub async fn handle_health_check(
        metrics_collector: &Arc<MetricsCollector>,
        config: &HealthCheckConfig,
    ) -> HealthCheckResponse {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metrics = metrics_collector.get_metrics();

        let (overall_status, components) = if let Some(metrics) = metrics {
            let component_health = Self::evaluate_component_health(&metrics, &config.thresholds);
            let overall_status = Self::determine_overall_status(&component_health);

            let components = if config.enable_detailed_info {
                Some(component_health)
            } else {
                None
            };

            (overall_status, components)
        } else {
            ("unhealthy".to_string(), None)
        };

        HealthCheckResponse {
            status: overall_status,
            timestamp,
            components,
            version: "1.0.0".to_string(),
        }
    }

    /// Handle /ready endpoint (readiness probe)
    pub async fn handle_readiness_check(
        metrics_collector: &Arc<MetricsCollector>,
        config: &HealthCheckConfig,
    ) -> HealthCheckResponse {
        let mut response = Self::handle_health_check(metrics_collector, config).await;

        // Readiness check is more strict - system must be fully operational
        if let Some(metrics) = metrics_collector.get_metrics() {
            // Check if system is ready to serve requests
            let is_ready = metrics.network_metrics.active_connections > 0
                || metrics.storage_metrics.pending_requests
                    < config.thresholds.max_pending_requests;

            if !is_ready {
                response.status = "not_ready".to_string();
            }
        } else {
            response.status = "not_ready".to_string();
        }

        response
    }

    /// Handle /live endpoint (liveness probe)
    pub async fn handle_liveness_check(
        metrics_collector: &Arc<MetricsCollector>,
        _config: &HealthCheckConfig,
    ) -> HealthCheckResponse {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Liveness check is basic - just check if the system is running
        let status = if metrics_collector.get_metrics().is_some() {
            "alive"
        } else {
            "dead"
        };

        HealthCheckResponse {
            status: status.to_string(),
            timestamp,
            components: None,
            version: "1.0.0".to_string(),
        }
    }

    /// Evaluate health of individual components
    fn evaluate_component_health(
        metrics: &RuntimeMetrics,
        thresholds: &HealthThresholds,
    ) -> ComponentHealth {
        // Evaluate network runtime health
        let network_status = Self::evaluate_network_health(&metrics.network_metrics, thresholds);

        // Evaluate storage runtime health
        let storage_status = Self::evaluate_storage_health(&metrics.storage_metrics, thresholds);

        // Evaluate channel health
        let channel_status = Self::evaluate_channel_health(&metrics.channel_metrics, thresholds);

        ComponentHealth {
            network_runtime: network_status,
            storage_runtime: storage_status,
            channel: channel_status,
        }
    }

    /// Evaluate network runtime health
    fn evaluate_network_health(
        metrics: &NetworkRuntimeMetrics,
        thresholds: &HealthThresholds,
    ) -> ComponentStatus {
        let mut issues = Vec::new();

        if metrics.error_rate_percent > thresholds.max_error_rate_percent {
            issues.push(format!(
                "High error rate: {:.1}%",
                metrics.error_rate_percent
            ));
        }

        if metrics.avg_request_time_ms > thresholds.max_response_time_ms {
            issues.push(format!(
                "High response time: {:.1}ms",
                metrics.avg_request_time_ms
            ));
        }

        let status = if issues.is_empty() {
            "healthy"
        } else if metrics.error_rate_percent > thresholds.max_error_rate_percent * 2.0 {
            "unhealthy"
        } else {
            "degraded"
        };

        let message = if issues.is_empty() {
            None
        } else {
            Some(issues.join(", "))
        };

        ComponentStatus {
            status: status.to_string(),
            message,
            metrics: Some(json!({
                "active_connections": metrics.active_connections,
                "requests_per_second": metrics.requests_per_second,
                "error_rate_percent": metrics.error_rate_percent,
                "avg_request_time_ms": metrics.avg_request_time_ms
            })),
        }
    }

    /// Evaluate storage runtime health
    fn evaluate_storage_health(
        metrics: &StorageRuntimeMetrics,
        thresholds: &HealthThresholds,
    ) -> ComponentStatus {
        let mut issues = Vec::new();

        if metrics.pending_requests > thresholds.max_pending_requests {
            issues.push(format!(
                "High pending requests: {}",
                metrics.pending_requests
            ));
        }

        if metrics.avg_execution_time_ms > thresholds.max_response_time_ms {
            issues.push(format!(
                "High execution time: {:.1}ms",
                metrics.avg_execution_time_ms
            ));
        }

        if metrics.rocksdb_metrics.write_stall_active {
            issues.push("Write stall active".to_string());
        }

        let status = if issues.is_empty() {
            "healthy"
        } else if metrics.rocksdb_metrics.write_stall_active {
            "unhealthy"
        } else {
            "degraded"
        };

        let message = if issues.is_empty() {
            None
        } else {
            Some(issues.join(", "))
        };

        ComponentStatus {
            status: status.to_string(),
            message,
            metrics: Some(json!({
                "pending_requests": metrics.pending_requests,
                "operations_per_second": metrics.operations_per_second,
                "avg_execution_time_ms": metrics.avg_execution_time_ms,
                "rocksdb_total_keys": metrics.rocksdb_metrics.total_keys,
                "rocksdb_write_stall": metrics.rocksdb_metrics.write_stall_active
            })),
        }
    }

    /// Evaluate channel health
    fn evaluate_channel_health(
        metrics: &ChannelMetrics,
        thresholds: &HealthThresholds,
    ) -> ComponentStatus {
        let mut issues = Vec::new();

        if metrics.buffer_metrics.buffer_utilization_percent
            > thresholds.max_buffer_utilization_percent
        {
            issues.push(format!(
                "High buffer utilization: {:.1}%",
                metrics.buffer_metrics.buffer_utilization_percent
            ));
        }

        if metrics.timeout_stats.timeout_rate_percent > thresholds.max_error_rate_percent {
            issues.push(format!(
                "High timeout rate: {:.1}%",
                metrics.timeout_stats.timeout_rate_percent
            ));
        }

        let status = if issues.is_empty() {
            "healthy"
        } else if metrics.buffer_metrics.buffer_utilization_percent > 95.0 {
            "unhealthy"
        } else {
            "degraded"
        };

        let message = if issues.is_empty() {
            None
        } else {
            Some(issues.join(", "))
        };

        ComponentStatus {
            status: status.to_string(),
            message,
            metrics: Some(json!({
                "buffer_utilization_percent": metrics.buffer_metrics.buffer_utilization_percent,
                "messages_per_second": metrics.message_stats.messages_per_second,
                "timeout_rate_percent": metrics.timeout_stats.timeout_rate_percent,
                "backpressure_activations": metrics.buffer_metrics.backpressure_activations
            })),
        }
    }

    /// Determine overall system status from component health
    fn determine_overall_status(components: &ComponentHealth) -> String {
        let statuses = [
            &components.network_runtime.status,
            &components.storage_runtime.status,
            &components.channel.status,
        ];

        if statuses.iter().any(|s| *s == "unhealthy") {
            "unhealthy".to_string()
        } else if statuses.iter().any(|s| *s == "degraded") {
            "degraded".to_string()
        } else {
            "healthy".to_string()
        }
    }

    /// Get health check configuration
    pub fn config(&self) -> &HealthCheckConfig {
        &self.config
    }

    /// Update health check thresholds
    pub fn update_thresholds(&mut self, thresholds: HealthThresholds) {
        self.config.thresholds = thresholds;
    }
}

#[cfg(test)]
mod health_tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check_endpoints_creation() {
        let metrics_collector = Arc::new(MetricsCollector::with_defaults());
        let health_endpoints = HealthCheckEndpoints::with_defaults(metrics_collector);

        assert_eq!(health_endpoints.config().port, 8080);
        assert!(health_endpoints.config().enable_detailed_info);
    }

    #[tokio::test]
    async fn test_health_check_response() {
        let metrics_collector = Arc::new(MetricsCollector::with_defaults());
        let config = HealthCheckConfig::default();

        let response = HealthCheckEndpoints::handle_health_check(&metrics_collector, &config).await;

        assert_eq!(response.status, "unhealthy"); // No metrics available initially
        assert_eq!(response.version, "1.0.0");
    }

    #[tokio::test]
    async fn test_readiness_check() {
        let metrics_collector = Arc::new(MetricsCollector::with_defaults());
        let config = HealthCheckConfig::default();

        let response =
            HealthCheckEndpoints::handle_readiness_check(&metrics_collector, &config).await;

        assert_eq!(response.status, "not_ready"); // No metrics available initially
    }

    #[tokio::test]
    async fn test_liveness_check() {
        let metrics_collector = Arc::new(MetricsCollector::with_defaults());
        let config = HealthCheckConfig::default();

        let response =
            HealthCheckEndpoints::handle_liveness_check(&metrics_collector, &config).await;

        assert_eq!(response.status, "dead"); // No metrics available initially
    }

    #[test]
    fn test_health_thresholds_default() {
        let thresholds = HealthThresholds::default();

        assert_eq!(thresholds.max_error_rate_percent, 5.0);
        assert_eq!(thresholds.max_response_time_ms, 1000.0);
        assert_eq!(thresholds.max_buffer_utilization_percent, 80.0);
        assert_eq!(thresholds.max_pending_requests, 1000);
    }
}
