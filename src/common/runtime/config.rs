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

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for dynamic thread pool scaling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingConfig {
    /// Enable dynamic thread pool scaling
    pub enabled: bool,
    /// Minimum number of threads for network runtime
    pub min_network_threads: usize,
    /// Maximum number of threads for network runtime
    pub max_network_threads: usize,
    /// Minimum number of threads for storage runtime
    pub min_storage_threads: usize,
    /// Maximum number of threads for storage runtime
    pub max_storage_threads: usize,
    /// Queue length threshold (percentage) to trigger scale-up
    pub scale_up_threshold: usize,
    /// Queue length threshold (percentage) to trigger scale-down
    pub scale_down_threshold: usize,
    /// Number of threads to add/remove during scaling
    pub scale_increment: usize,
    /// Interval between scaling evaluations
    pub evaluation_interval: Duration,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        let cpu_count = num_cpus::get();
        Self {
            enabled: false, // Disabled by default for backward compatibility
            min_network_threads: cpu_count.clamp(1, 2),
            max_network_threads: cpu_count.clamp(4, 16),
            min_storage_threads: cpu_count.clamp(2, 4),
            max_storage_threads: cpu_count.clamp(8, 32),
            scale_up_threshold: 80,   // Scale up when queue is 80% full
            scale_down_threshold: 20, // Scale down when queue is 20% full
            scale_increment: 1,       // Add/remove 1 thread at a time
            evaluation_interval: Duration::from_secs(1),
        }
    }
}

/// Configuration for request priority handling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityConfig {
    /// Enable priority-based request processing
    pub enabled: bool,
    /// Processing weight for high priority requests
    pub high_priority_weight: usize,
    /// Processing weight for normal priority requests
    pub normal_priority_weight: usize,
    /// Processing weight for low priority requests
    pub low_priority_weight: usize,
    /// Maximum queue size per priority level
    pub max_queue_size_per_priority: usize,
}

impl Default for PriorityConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for backward compatibility
            high_priority_weight: 3,
            normal_priority_weight: 2,
            low_priority_weight: 1,
            max_queue_size_per_priority: 10000,
        }
    }
}

/// Configuration for Raft metrics collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftMetricsConfig {
    /// Enable Raft metrics collection
    pub enabled: bool,
    /// Interval between metric collections
    pub collection_interval: Duration,
    /// How long to retain metrics data
    pub retention_period: Duration,
    /// Enable detailed per-follower replication metrics
    pub track_replication_latency: bool,
    /// Enable leader election event tracking
    pub track_election_events: bool,
}

impl Default for RaftMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true, // Enabled by default for observability
            collection_interval: Duration::from_millis(100),
            retention_period: Duration::from_secs(3600), // 1 hour
            track_replication_latency: true,
            track_election_events: true,
        }
    }
}

/// Configuration for fault injection testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultInjectionConfig {
    /// Enable fault injection (should only be true in test environments)
    pub enabled: bool,
    /// Default network delay duration for fault injection
    pub default_network_delay: Duration,
    /// Default message drop rate (0.0 to 1.0)
    pub default_drop_rate: f64,
    /// Enable logging of all fault injection events
    pub log_events: bool,
}

impl Default for FaultInjectionConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for safety
            default_network_delay: Duration::from_millis(100),
            default_drop_rate: 0.1, // 10% drop rate
            log_events: true,
        }
    }
}

/// Configuration for the dual runtime architecture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Number of threads for the network runtime
    pub network_threads: usize,
    /// Number of threads for the storage runtime  
    pub storage_threads: usize,
    /// Buffer size for the message channel between runtimes
    pub channel_buffer_size: usize,
    /// Timeout for storage requests
    pub request_timeout: Duration,
    /// Batch size for storage operations
    pub batch_size: usize,
    /// Timeout for batch processing
    pub batch_timeout: Duration,
    /// Dynamic thread pool scaling configuration
    #[serde(default)]
    pub scaling: ScalingConfig,
    /// Request priority configuration
    #[serde(default)]
    pub priority: PriorityConfig,
    /// Raft metrics collection configuration
    #[serde(default)]
    pub raft_metrics: RaftMetricsConfig,
    /// Fault injection configuration (for testing)
    #[serde(default)]
    pub fault_injection: FaultInjectionConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let cpu_count = num_cpus::get();

        Self {
            // Network runtime: Use fewer threads, optimized for I/O
            network_threads: cpu_count.clamp(1, 4),
            // Storage runtime: Use more threads for RocksDB operations
            storage_threads: cpu_count.clamp(2, 8),
            // Channel buffer size: Allow for high throughput
            channel_buffer_size: 10000,
            // Request timeout: 30 seconds default
            request_timeout: Duration::from_secs(30),
            // Batch size: Process up to 100 requests in a batch
            batch_size: 100,
            // Batch timeout: Wait max 10ms to form a batch
            batch_timeout: Duration::from_millis(10),
            // Use default configurations for new features
            scaling: ScalingConfig::default(),
            priority: PriorityConfig::default(),
            raft_metrics: RaftMetricsConfig::default(),
            fault_injection: FaultInjectionConfig::default(),
        }
    }
}

impl RuntimeConfig {
    /// Create a new RuntimeConfig with custom parameters
    pub fn new(
        network_threads: usize,
        storage_threads: usize,
        channel_buffer_size: usize,
        request_timeout: Duration,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Result<Self, String> {
        let config = Self {
            network_threads,
            storage_threads,
            channel_buffer_size,
            request_timeout,
            batch_size,
            batch_timeout,
            scaling: ScalingConfig::default(),
            priority: PriorityConfig::default(),
            raft_metrics: RaftMetricsConfig::default(),
            fault_injection: FaultInjectionConfig::default(),
        };

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration parameters
    pub fn validate(&self) -> Result<(), String> {
        if self.network_threads == 0 {
            return Err("network_threads must be greater than 0".to_string());
        }

        if self.storage_threads == 0 {
            return Err("storage_threads must be greater than 0".to_string());
        }

        if self.channel_buffer_size == 0 {
            return Err("channel_buffer_size must be greater than 0".to_string());
        }

        if self.request_timeout.is_zero() {
            return Err("request_timeout must be greater than 0".to_string());
        }

        if self.batch_size == 0 {
            return Err("batch_size must be greater than 0".to_string());
        }

        if self.batch_timeout.is_zero() {
            return Err("batch_timeout must be greater than 0".to_string());
        }

        // Validate scaling configuration
        self.validate_scaling_config()?;

        // Validate priority configuration
        self.validate_priority_config()?;

        // Validate raft metrics configuration
        self.validate_raft_metrics_config()?;

        // Validate fault injection configuration
        self.validate_fault_injection_config()?;

        // Warn if configuration seems suboptimal
        let cpu_count = num_cpus::get();
        if self.network_threads + self.storage_threads > cpu_count * 2 {
            log::warn!(
                "Total thread count ({}) exceeds 2x CPU cores ({}). This may cause context switching overhead.",
                self.network_threads + self.storage_threads,
                cpu_count
            );
        }

        Ok(())
    }

    /// Validate scaling configuration
    fn validate_scaling_config(&self) -> Result<(), String> {
        if !self.scaling.enabled {
            return Ok(());
        }

        if self.scaling.min_network_threads == 0 {
            return Err("scaling.min_network_threads must be greater than 0".to_string());
        }

        if self.scaling.max_network_threads < self.scaling.min_network_threads {
            return Err("scaling.max_network_threads must be >= min_network_threads".to_string());
        }

        if self.scaling.min_storage_threads == 0 {
            return Err("scaling.min_storage_threads must be greater than 0".to_string());
        }

        if self.scaling.max_storage_threads < self.scaling.min_storage_threads {
            return Err("scaling.max_storage_threads must be >= min_storage_threads".to_string());
        }

        if self.scaling.scale_up_threshold == 0 || self.scaling.scale_up_threshold > 100 {
            return Err("scaling.scale_up_threshold must be between 1 and 100".to_string());
        }

        if self.scaling.scale_down_threshold == 0 || self.scaling.scale_down_threshold > 100 {
            return Err("scaling.scale_down_threshold must be between 1 and 100".to_string());
        }

        if self.scaling.scale_down_threshold >= self.scaling.scale_up_threshold {
            return Err("scaling.scale_down_threshold must be < scale_up_threshold".to_string());
        }

        if self.scaling.scale_increment == 0 {
            return Err("scaling.scale_increment must be greater than 0".to_string());
        }

        if self.scaling.evaluation_interval.is_zero() {
            return Err("scaling.evaluation_interval must be greater than 0".to_string());
        }

        Ok(())
    }

    /// Validate priority configuration
    fn validate_priority_config(&self) -> Result<(), String> {
        if !self.priority.enabled {
            return Ok(());
        }

        if self.priority.high_priority_weight == 0 {
            return Err("priority.high_priority_weight must be greater than 0".to_string());
        }

        if self.priority.normal_priority_weight == 0 {
            return Err("priority.normal_priority_weight must be greater than 0".to_string());
        }

        if self.priority.low_priority_weight == 0 {
            return Err("priority.low_priority_weight must be greater than 0".to_string());
        }

        if self.priority.max_queue_size_per_priority == 0 {
            return Err("priority.max_queue_size_per_priority must be greater than 0".to_string());
        }

        Ok(())
    }

    /// Validate Raft metrics configuration
    fn validate_raft_metrics_config(&self) -> Result<(), String> {
        if !self.raft_metrics.enabled {
            return Ok(());
        }

        if self.raft_metrics.collection_interval.is_zero() {
            return Err("raft_metrics.collection_interval must be greater than 0".to_string());
        }

        if self.raft_metrics.retention_period.is_zero() {
            return Err("raft_metrics.retention_period must be greater than 0".to_string());
        }

        if self.raft_metrics.collection_interval > self.raft_metrics.retention_period {
            log::warn!(
                "raft_metrics.collection_interval ({:?}) is greater than retention_period ({:?})",
                self.raft_metrics.collection_interval,
                self.raft_metrics.retention_period
            );
        }

        Ok(())
    }

    /// Validate fault injection configuration
    fn validate_fault_injection_config(&self) -> Result<(), String> {
        if !self.fault_injection.enabled {
            return Ok(());
        }

        if self.fault_injection.default_drop_rate < 0.0
            || self.fault_injection.default_drop_rate > 1.0
        {
            return Err(
                "fault_injection.default_drop_rate must be between 0.0 and 1.0".to_string(),
            );
        }

        if self.fault_injection.default_network_delay.is_zero() {
            log::warn!(
                "fault_injection.default_network_delay is zero, which may not be useful for testing"
            );
        }

        // Warn if fault injection is enabled in production
        #[cfg(not(test))]
        if self.fault_injection.enabled {
            log::warn!(
                "Fault injection is enabled! This should only be used in test environments."
            );
        }

        Ok(())
    }

    /// Create a configuration optimized for high throughput
    pub fn high_throughput() -> Self {
        let cpu_count = num_cpus::get();

        Self {
            network_threads: cpu_count.clamp(2, 6),
            storage_threads: cpu_count.clamp(4, 12),
            channel_buffer_size: 50000,
            request_timeout: Duration::from_secs(60),
            batch_size: 500,
            batch_timeout: Duration::from_millis(5),
            scaling: ScalingConfig {
                enabled: true,
                min_network_threads: cpu_count.clamp(2, 4),
                max_network_threads: cpu_count.clamp(8, 16),
                min_storage_threads: cpu_count.clamp(4, 8),
                max_storage_threads: cpu_count.clamp(16, 32),
                scale_up_threshold: 75,
                scale_down_threshold: 25,
                scale_increment: 2,
                evaluation_interval: Duration::from_millis(500),
            },
            priority: PriorityConfig {
                enabled: true,
                high_priority_weight: 4,
                normal_priority_weight: 2,
                low_priority_weight: 1,
                max_queue_size_per_priority: 20000,
            },
            raft_metrics: RaftMetricsConfig::default(),
            fault_injection: FaultInjectionConfig::default(),
        }
    }

    /// Create a configuration optimized for low latency
    pub fn low_latency() -> Self {
        let cpu_count = num_cpus::get();

        Self {
            network_threads: cpu_count.clamp(2, 8),
            storage_threads: cpu_count.clamp(2, 6),
            channel_buffer_size: 5000,
            request_timeout: Duration::from_secs(10),
            batch_size: 10,
            batch_timeout: Duration::from_millis(1),
            scaling: ScalingConfig {
                enabled: true,
                min_network_threads: cpu_count.clamp(2, 4),
                max_network_threads: cpu_count.clamp(6, 12),
                min_storage_threads: cpu_count.clamp(2, 4),
                max_storage_threads: cpu_count.clamp(8, 16),
                scale_up_threshold: 85,
                scale_down_threshold: 15,
                scale_increment: 1,
                evaluation_interval: Duration::from_millis(200),
            },
            priority: PriorityConfig {
                enabled: true,
                high_priority_weight: 5,
                normal_priority_weight: 2,
                low_priority_weight: 1,
                max_queue_size_per_priority: 5000,
            },
            raft_metrics: RaftMetricsConfig {
                enabled: true,
                collection_interval: Duration::from_millis(50),
                retention_period: Duration::from_secs(1800),
                track_replication_latency: true,
                track_election_events: true,
            },
            fault_injection: FaultInjectionConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = RuntimeConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        // Test invalid network_threads
        let result = RuntimeConfig::new(
            0,
            2,
            1000,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("network_threads"));

        // Test invalid storage_threads
        let result = RuntimeConfig::new(
            2,
            0,
            1000,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("storage_threads"));

        // Test invalid channel_buffer_size
        let result = RuntimeConfig::new(
            2,
            2,
            0,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("channel_buffer_size"));

        // Test valid configuration
        let result = RuntimeConfig::new(
            2,
            4,
            1000,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_preset_configurations() {
        let high_throughput = RuntimeConfig::high_throughput();
        assert!(high_throughput.validate().is_ok());
        assert!(high_throughput.batch_size > RuntimeConfig::default().batch_size);

        let low_latency = RuntimeConfig::low_latency();
        assert!(low_latency.validate().is_ok());
        assert!(low_latency.batch_timeout < RuntimeConfig::default().batch_timeout);
    }

    #[test]
    fn test_scaling_config_validation() {
        let mut config = RuntimeConfig::default();
        config.scaling.enabled = true;

        // Valid scaling config
        assert!(config.validate().is_ok());

        // Invalid: min > max for network threads
        config.scaling.min_network_threads = 10;
        config.scaling.max_network_threads = 5;
        assert!(config.validate().is_err());
        config.scaling.min_network_threads = 2;
        config.scaling.max_network_threads = 16;

        // Invalid: min > max for storage threads
        config.scaling.min_storage_threads = 20;
        config.scaling.max_storage_threads = 10;
        assert!(config.validate().is_err());
        config.scaling.min_storage_threads = 4;
        config.scaling.max_storage_threads = 32;

        // Invalid: scale_up_threshold out of range
        config.scaling.scale_up_threshold = 150;
        assert!(config.validate().is_err());
        config.scaling.scale_up_threshold = 80;

        // Invalid: scale_down_threshold >= scale_up_threshold
        config.scaling.scale_down_threshold = 90;
        assert!(config.validate().is_err());
        config.scaling.scale_down_threshold = 20;

        // Valid again
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_priority_config_validation() {
        let mut config = RuntimeConfig::default();
        config.priority.enabled = true;

        // Valid priority config
        assert!(config.validate().is_ok());

        // Invalid: zero weight
        config.priority.high_priority_weight = 0;
        assert!(config.validate().is_err());
        config.priority.high_priority_weight = 3;

        // Invalid: zero queue size
        config.priority.max_queue_size_per_priority = 0;
        assert!(config.validate().is_err());
        config.priority.max_queue_size_per_priority = 10000;

        // Valid again
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_raft_metrics_config_validation() {
        let mut config = RuntimeConfig::default();
        config.raft_metrics.enabled = true;

        // Valid metrics config
        assert!(config.validate().is_ok());

        // Invalid: zero collection interval
        config.raft_metrics.collection_interval = Duration::from_secs(0);
        assert!(config.validate().is_err());
        config.raft_metrics.collection_interval = Duration::from_millis(100);

        // Invalid: zero retention period
        config.raft_metrics.retention_period = Duration::from_secs(0);
        assert!(config.validate().is_err());
        config.raft_metrics.retention_period = Duration::from_secs(3600);

        // Valid again
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_fault_injection_config_validation() {
        let mut config = RuntimeConfig::default();
        config.fault_injection.enabled = true;

        // Valid fault injection config
        assert!(config.validate().is_ok());

        // Invalid: drop rate out of range
        config.fault_injection.default_drop_rate = 1.5;
        assert!(config.validate().is_err());
        config.fault_injection.default_drop_rate = -0.1;
        assert!(config.validate().is_err());
        config.fault_injection.default_drop_rate = 0.1;

        // Valid again
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_scaling_config_default() {
        let config = ScalingConfig::default();
        assert!(!config.enabled);
        assert!(config.min_network_threads > 0);
        assert!(config.max_network_threads >= config.min_network_threads);
        assert!(config.min_storage_threads > 0);
        assert!(config.max_storage_threads >= config.min_storage_threads);
        assert!(config.scale_up_threshold > config.scale_down_threshold);
    }

    #[test]
    fn test_priority_config_default() {
        let config = PriorityConfig::default();
        assert!(!config.enabled);
        assert!(config.high_priority_weight > config.normal_priority_weight);
        assert!(config.normal_priority_weight > config.low_priority_weight);
        assert!(config.max_queue_size_per_priority > 0);
    }

    #[test]
    fn test_raft_metrics_config_default() {
        let config = RaftMetricsConfig::default();
        assert!(config.enabled);
        assert!(!config.collection_interval.is_zero());
        assert!(!config.retention_period.is_zero());
        assert!(config.track_replication_latency);
        assert!(config.track_election_events);
    }

    #[test]
    fn test_fault_injection_config_default() {
        let config = FaultInjectionConfig::default();
        assert!(!config.enabled);
        assert!(config.default_drop_rate >= 0.0 && config.default_drop_rate <= 1.0);
        assert!(!config.default_network_delay.is_zero());
        assert!(config.log_events);
    }

    #[test]
    fn test_high_throughput_preset() {
        let config = RuntimeConfig::high_throughput();
        assert!(config.validate().is_ok());
        assert!(config.scaling.enabled);
        assert!(config.priority.enabled);
        assert!(config.batch_size > RuntimeConfig::default().batch_size);
    }

    #[test]
    fn test_low_latency_preset() {
        let config = RuntimeConfig::low_latency();
        assert!(config.validate().is_ok());
        assert!(config.scaling.enabled);
        assert!(config.priority.enabled);
        assert!(config.batch_timeout < RuntimeConfig::default().batch_timeout);
    }
}
