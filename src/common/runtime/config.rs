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
}
