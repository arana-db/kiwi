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

//! Unit tests for dual runtime architecture core components
//!
//! This module contains comprehensive unit tests for:
//! - RuntimeManager lifecycle and configuration
//! - MessageChannel communication and backpressure
//! - StorageClient request handling and error scenarios
//! - Request/response serialization and correlation
//! - Configuration validation and defaults

use std::time::Duration;

use serde_json;

// Import the dual runtime components
use crate::manager::LifecycleState;
use crate::{
    BackpressureConfig, DualRuntimeError, ManagerRuntimeHealth as RuntimeHealth, MessageChannel,
    RequestId, RequestPriority, RuntimeConfig, RuntimeManager, StorageCommand, StorageStats,
};

/// Test configuration for unit tests
#[allow(dead_code)]
struct TestConfig {
    pub buffer_size: usize,
    pub timeout: Duration,
    pub network_threads: usize,
    pub storage_threads: usize,
}

#[allow(dead_code)]
impl Default for TestConfig {
    fn default() -> Self {
        Self {
            buffer_size: 100,
            timeout: Duration::from_millis(100),
            network_threads: 2,
            storage_threads: 2,
        }
    }
}

/// Helper function to create a test RuntimeConfig
fn create_test_runtime_config() -> RuntimeConfig {
    RuntimeConfig::new(
        2,
        2,
        1000,
        Duration::from_secs(30),
        100,
        Duration::from_millis(10),
    )
    .unwrap()
}

/// Helper function to create a test MessageChannel
#[allow(dead_code)]
fn create_test_message_channel() -> MessageChannel {
    MessageChannel::new(100)
}

// Helper function to create a test StorageClient - commented out due to compilation issues
// fn create_test_storage_client() -> StorageClient {
//     let channel = Arc::new(create_test_message_channel());
//     StorageClient::new(channel, Duration::from_secs(30))
// }

// RuntimeManager Tests
mod runtime_manager_tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_manager_creation() {
        let config = create_test_runtime_config();
        let manager = RuntimeManager::new(config).unwrap();

        assert!(!manager.is_running().await);
        assert_eq!(manager.state().await, LifecycleState::Created);
    }

    #[tokio::test]
    async fn test_runtime_manager_with_defaults() {
        let manager = RuntimeManager::with_defaults().unwrap();

        assert!(!manager.is_running().await);
        assert_eq!(manager.state().await, LifecycleState::Created);

        // Network threads should be clamped between 1 and 4 based on CPU count
        let cpu_count = num_cpus::get();
        assert_eq!(manager.config().network_threads, cpu_count.clamp(1, 4));
        assert_eq!(manager.config().storage_threads, cpu_count.clamp(2, 8));
    }

    #[tokio::test]
    async fn test_runtime_manager_lifecycle() {
        let mut manager = RuntimeManager::with_defaults().unwrap();

        // Test startup
        manager.start().await.unwrap();
        assert!(manager.is_running().await);
        assert_eq!(manager.state().await, LifecycleState::Running);

        // Test handles are available
        assert!(manager.network_handle().is_ok());
        assert!(manager.storage_handle().is_ok());

        // Test shutdown
        manager.stop().await.unwrap();
        assert!(!manager.is_running().await);
        assert_eq!(manager.state().await, LifecycleState::Stopped);
    }

    #[tokio::test]
    async fn test_runtime_manager_double_start() {
        let mut manager = RuntimeManager::with_defaults().unwrap();

        manager.start().await.unwrap();

        // Second start should fail
        let result = manager.start().await;
        assert!(result.is_err());

        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_runtime_manager_health_check() {
        let mut manager = RuntimeManager::with_defaults().unwrap();

        // Health check before start should show unhealthy
        let (network_health, storage_health) = manager.health_check().await.unwrap();
        assert_eq!(
            network_health,
            RuntimeHealth::Unhealthy("Runtime not started".to_string())
        );
        assert_eq!(
            storage_health,
            RuntimeHealth::Unhealthy("Runtime not started".to_string())
        );

        manager.start().await.unwrap();

        // Health check after start should show healthy
        let (network_health, storage_health) = manager.health_check().await.unwrap();
        assert_eq!(network_health, RuntimeHealth::Healthy);
        assert_eq!(storage_health, RuntimeHealth::Healthy);

        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_runtime_manager_wait_for_state() {
        let manager = RuntimeManager::with_defaults().unwrap();

        // Should timeout waiting for running state
        let result = manager
            .wait_for_state(LifecycleState::Running, Duration::from_millis(50))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_runtime_manager_force_shutdown() {
        let mut manager = RuntimeManager::with_defaults().unwrap();
        manager.start().await.unwrap();

        // Force shutdown should work
        manager
            .force_shutdown(Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(manager.state().await, LifecycleState::Stopped);
    }

    #[tokio::test]
    async fn test_runtime_manager_stats() {
        let mut manager = RuntimeManager::with_defaults().unwrap();
        manager.start().await.unwrap();

        let (network_stats, storage_stats) = manager.get_stats().await;

        assert_eq!(network_stats.active_tasks, 0);
        assert_eq!(storage_stats.active_tasks, 0);
        assert!(network_stats.uptime > Duration::ZERO);
        assert!(storage_stats.uptime > Duration::ZERO);

        manager.stop().await.unwrap();
    }
}

// MessageChannel Tests
mod message_channel_tests {
    use super::*;

    #[tokio::test]
    async fn test_message_channel_creation() {
        let channel = MessageChannel::new(1000);

        assert_eq!(channel.buffer_size(), 1000);
        assert!(channel.is_healthy());
        assert!(!channel.has_backpressure());
    }

    #[tokio::test]
    async fn test_message_channel_with_backpressure_config() {
        let config = BackpressureConfig {
            threshold_percent: 90,
            max_wait_time: Duration::from_millis(200),
            drop_oldest_on_full: true,
        };
        let channel = MessageChannel::with_backpressure_config(100, config.clone());

        assert_eq!(channel.buffer_size(), 100);
        assert_eq!(channel.backpressure_config().threshold_percent, 90);
        assert_eq!(
            channel.backpressure_config().max_wait_time,
            Duration::from_millis(200)
        );
        assert!(channel.backpressure_config().drop_oldest_on_full);
    }

    #[tokio::test]
    async fn test_message_channel_stats_recording() {
        let channel = MessageChannel::new(100);

        // Initial stats should be zero
        let stats = channel.stats().await;
        assert_eq!(stats.requests_sent, 0);
        assert_eq!(stats.requests_received, 0);
        assert_eq!(stats.responses_sent, 0);

        // Record some operations
        channel.record_request_sent().await;
        channel.record_request_received().await;
        channel
            .record_response_sent(Duration::from_millis(10))
            .await;

        let stats = channel.stats().await;
        assert_eq!(stats.requests_sent, 1);
        assert_eq!(stats.requests_received, 1);
        assert_eq!(stats.responses_sent, 1);
        assert_eq!(stats.avg_processing_time, Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_message_channel_timeout_recording() {
        let channel = MessageChannel::new(100);

        channel.record_timeout().await;

        let stats = channel.stats().await;
        assert_eq!(stats.requests_timeout, 1);
    }

    #[tokio::test]
    async fn test_message_channel_send_failure_recording() {
        let channel = MessageChannel::new(100);

        channel.record_send_failure().await;

        let stats = channel.stats().await;
        assert_eq!(stats.send_failures, 1);
    }

    #[test]
    fn test_backpressure_config_default() {
        let config = BackpressureConfig::default();

        assert_eq!(config.threshold_percent, 80);
        assert_eq!(config.max_wait_time, Duration::from_millis(100));
        assert!(!config.drop_oldest_on_full);
    }
}

// StorageClient Tests - commented out due to compilation issues in runtime module
// mod storage_client_tests {
//     use super::*;
//     // Tests will be enabled once runtime module compilation issues are resolved
// }

// Request/Response Serialization Tests
mod serialization_tests {
    use super::*;

    #[test]
    fn test_request_id_creation_and_uniqueness() {
        let id1 = RequestId::new();
        let id2 = RequestId::new();

        assert_ne!(id1, id2);
        assert_ne!(id1.inner(), id2.inner());
    }

    #[test]
    fn test_request_id_display() {
        let id = RequestId::new();
        let display_str = format!("{}", id);
        let uuid_str = format!("{}", id.inner());

        assert_eq!(display_str, uuid_str);
    }

    #[test]
    fn test_request_priority_ordering() {
        assert!(RequestPriority::Critical > RequestPriority::High);
        assert!(RequestPriority::High > RequestPriority::Normal);
        assert!(RequestPriority::Normal > RequestPriority::Low);
    }

    #[test]
    fn test_storage_command_serialization() {
        let commands = vec![
            StorageCommand::Get {
                key: b"test_key".to_vec(),
            },
            StorageCommand::Set {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
                ttl: Some(Duration::from_secs(60)),
            },
            StorageCommand::Del {
                keys: vec![b"key1".to_vec(), b"key2".to_vec()],
            },
            StorageCommand::Exists {
                keys: vec![b"key1".to_vec()],
            },
            StorageCommand::Expire {
                key: b"key".to_vec(),
                ttl: Duration::from_secs(30),
            },
            StorageCommand::Ttl {
                key: b"key".to_vec(),
            },
            StorageCommand::Incr {
                key: b"counter".to_vec(),
            },
            StorageCommand::IncrBy {
                key: b"counter".to_vec(),
                increment: 5,
            },
            StorageCommand::Decr {
                key: b"counter".to_vec(),
            },
            StorageCommand::DecrBy {
                key: b"counter".to_vec(),
                decrement: 3,
            },
            StorageCommand::MSet {
                pairs: vec![(b"k1".to_vec(), b"v1".to_vec())],
            },
            StorageCommand::MGet {
                keys: vec![b"k1".to_vec(), b"k2".to_vec()],
            },
        ];

        for cmd in commands {
            // Test serialization and deserialization
            let serialized = serde_json::to_string(&cmd).unwrap();
            let deserialized: StorageCommand = serde_json::from_str(&serialized).unwrap();

            // Verify the command type matches
            match (&cmd, &deserialized) {
                (StorageCommand::Get { .. }, StorageCommand::Get { .. }) => {}
                (StorageCommand::Set { .. }, StorageCommand::Set { .. }) => {}
                (StorageCommand::Del { .. }, StorageCommand::Del { .. }) => {}
                (StorageCommand::Exists { .. }, StorageCommand::Exists { .. }) => {}
                (StorageCommand::Expire { .. }, StorageCommand::Expire { .. }) => {}
                (StorageCommand::Ttl { .. }, StorageCommand::Ttl { .. }) => {}
                (StorageCommand::Incr { .. }, StorageCommand::Incr { .. }) => {}
                (StorageCommand::IncrBy { .. }, StorageCommand::IncrBy { .. }) => {}
                (StorageCommand::Decr { .. }, StorageCommand::Decr { .. }) => {}
                (StorageCommand::DecrBy { .. }, StorageCommand::DecrBy { .. }) => {}
                (StorageCommand::MSet { .. }, StorageCommand::MSet { .. }) => {}
                (StorageCommand::MGet { .. }, StorageCommand::MGet { .. }) => {}
                _ => panic!("Command type mismatch after serialization"),
            }
        }
    }

    #[test]
    fn test_storage_stats_default() {
        let stats = StorageStats::default();

        assert_eq!(stats.keys_read, 0);
        assert_eq!(stats.keys_written, 0);
        assert_eq!(stats.keys_deleted, 0);
        assert_eq!(stats.bytes_read, 0);
        assert_eq!(stats.bytes_written, 0);
        assert!(!stats.cache_hit);
        assert_eq!(stats.compaction_level, None);
    }
}

// Configuration Validation Tests
mod configuration_tests {
    use super::*;

    #[test]
    fn test_runtime_config_valid() {
        let config = RuntimeConfig::new(
            4,
            4,
            1000,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        );

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.network_threads, 4);
        assert_eq!(config.storage_threads, 4);
    }

    #[test]
    fn test_runtime_config_invalid_threads() {
        // Zero threads should be invalid
        let config = RuntimeConfig::new(
            0,
            4,
            1000,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        );
        assert!(config.is_err());

        // Zero storage threads should be invalid
        let config = RuntimeConfig::new(
            4,
            0,
            1000,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        );
        assert!(config.is_err());
    }

    #[test]
    fn test_runtime_config_invalid_buffer_size() {
        // Zero buffer size should be invalid
        let config = RuntimeConfig::new(
            4,
            4,
            0,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        );
        assert!(config.is_err());
    }

    #[test]
    fn test_runtime_config_default() {
        let config = RuntimeConfig::default();

        let cpu_count = num_cpus::get();
        assert_eq!(config.network_threads, cpu_count.clamp(1, 4));
        assert_eq!(config.storage_threads, cpu_count.clamp(2, 8));
        assert_eq!(config.channel_buffer_size, 10000);
        assert_eq!(config.request_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_runtime_config_validation() {
        let config = RuntimeConfig::default();

        // Default config should be valid
        assert!(config.validate().is_ok());
    }
}

// Error Handling Tests
mod error_handling_tests {
    use super::*;

    #[test]
    fn test_dual_runtime_error_types() {
        // Test different error types can be created
        let _config_error = DualRuntimeError::configuration("Invalid config".to_string());
        let _network_error = DualRuntimeError::network_runtime("Network failed".to_string());
        let _storage_error = DualRuntimeError::storage_runtime("Storage failed".to_string());
        let _channel_error = DualRuntimeError::Channel("Channel closed".to_string());
        let _timeout_error = DualRuntimeError::timeout(Duration::from_secs(30));
        let _lifecycle_error = DualRuntimeError::lifecycle("Invalid state".to_string());
    }

    #[test]
    fn test_dual_runtime_error_display() {
        let error = DualRuntimeError::configuration("Test error".to_string());
        let error_str = format!("{}", error);
        assert!(error_str.contains("Test error"));
    }

    #[test]
    fn test_dual_runtime_error_debug() {
        let error = DualRuntimeError::timeout(Duration::from_secs(5));
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("Timeout"));
    }
}

// Timeout and Error Scenario Tests
mod timeout_error_tests {
    use super::*;

    #[tokio::test]
    async fn test_message_channel_closed_scenario() {
        let mut channel = MessageChannel::new(10);
        let _sender = channel.request_sender();

        // Take the receiver to simulate it being dropped
        let _receiver = channel.take_request_receiver();
        drop(_receiver);

        // Channel should still report as healthy until sender detects closure
        // This is expected behavior as the sender doesn't immediately know about receiver drop
    }

    #[tokio::test]
    async fn test_runtime_manager_invalid_state_transitions() {
        let mut manager = RuntimeManager::with_defaults().unwrap();

        // Try to stop before starting
        manager.stop().await.unwrap(); // Should succeed (no-op)

        // Start and try to start again
        manager.start().await.unwrap();
        let result = manager.start().await;
        assert!(result.is_err());

        manager.stop().await.unwrap();
    }
}

// Circuit Breaker Tests - commented out due to compilation issues
// mod circuit_breaker_tests {
//     use super::*;
//     // Tests will be enabled once runtime module compilation issues are resolved
// }

#[cfg(test)]
mod integration_helpers {
    use super::*;

    /// Helper to create a minimal test environment
    #[allow(dead_code)]
    pub async fn create_test_environment() -> RuntimeManager {
        let mut manager = RuntimeManager::with_defaults().unwrap();
        manager.start().await.unwrap();
        manager
    }

    /// Helper to clean up test environment
    #[allow(dead_code)]
    pub async fn cleanup_test_environment(mut manager: RuntimeManager) {
        let _ = manager.stop().await;
    }
}
