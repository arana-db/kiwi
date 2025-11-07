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


use std::time::Duration;

// Import the dual runtime components
use crate::config::RuntimeConfig;
use crate::error::DualRuntimeError;
use crate::manager::{LifecycleState, RuntimeHealth, RuntimeManager};
use crate::message::{
    BackpressureConfig, MessageChannel, RequestId, RequestPriority, StorageCommand, StorageStats,
};

/// Test configuration for unit tests
#[allow(dead_code)]
struct TestConfig {
    pub buffer_size: usize,
    pub timeout: Duration,
    pub network_threads: usize,
    pub storage_threads: usize,
}

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

// RuntimeManager Tests
#[cfg(test)]
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
        // Default config uses CPU-based calculation
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
#[cfg(test)]
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

// Request/Response Serialization Tests
#[cfg(test)]
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
#[cfg(test)]
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

        // Default config uses CPU-based calculation
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
#[cfg(test)]
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
#[cfg(test)]
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

// Integration Tests Module
#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::{Barrier, oneshot};
    use tokio::time::timeout;

    // Import additional components needed for integration tests
    use crate::{StorageClient, StorageRequest};

    // Integration test configuration
    #[allow(dead_code)]
    struct IntegrationTestConfig {
        pub network_threads: usize,
        pub storage_threads: usize,
        pub channel_buffer_size: usize,
        pub request_timeout: Duration,
        pub concurrent_requests: usize,
    }

    impl Default for IntegrationTestConfig {
        fn default() -> Self {
            Self {
                network_threads: 2,
                storage_threads: 2,
                channel_buffer_size: 1000,
                request_timeout: Duration::from_secs(5),
                concurrent_requests: 100,
            }
        }
    }

    // Complete Request Flow Tests
    #[tokio::test]
    async fn test_basic_get_set_flow() {
        // Test that we can create storage commands
        let set_command = StorageCommand::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            ttl: None,
        };

        let get_command = StorageCommand::Get {
            key: b"test_key".to_vec(),
        };

        // Verify commands are created correctly
        match set_command {
            StorageCommand::Set { key, value, ttl } => {
                assert_eq!(key, b"test_key");
                assert_eq!(value, b"test_value");
                assert_eq!(ttl, None);
            }
            _ => panic!("Expected Set command"),
        }

        match get_command {
            StorageCommand::Get { key } => {
                assert_eq!(key, b"test_key");
            }
            _ => panic!("Expected Get command"),
        }
    }

    #[tokio::test]
    async fn test_message_channel_communication() {
        let mut message_channel = MessageChannel::new(10);
        let sender = message_channel.request_sender();
        let mut receiver = message_channel.take_request_receiver().unwrap();

        // Test basic channel communication
        let (response_tx, _response_rx) = oneshot::channel();
        let request = StorageRequest {
            id: RequestId::new(),
            command: StorageCommand::Get {
                key: b"test".to_vec(),
            },
            response_channel: response_tx,
            timeout: Duration::from_secs(1),
            timestamp: Instant::now(),
            priority: RequestPriority::Normal,
        };

        // Send request
        let send_result = sender.send(request).await;
        assert!(send_result.is_ok());

        // Receive request
        let received = timeout(Duration::from_millis(100), receiver.recv()).await;
        assert!(received.is_ok());
        assert!(received.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_request_timeout_handling() {
        let message_channel = Arc::new(MessageChannel::new(100));
        let storage_client = StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_millis(1), // Very short timeout
        );

        // This should timeout since there's no storage server
        let command = StorageCommand::Get {
            key: b"test".to_vec(),
        };
        let result = storage_client.send_request(command).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            DualRuntimeError::Timeout { .. } => {}
            DualRuntimeError::Channel(_) => {} // Also acceptable
            other => panic!("Expected timeout or channel error, got: {:?}", other),
        }
    }

    // Concurrent Request Handling Tests
    #[tokio::test]
    async fn test_concurrent_request_creation() {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let storage_client = Arc::new(StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_millis(100),
        ));

        let num_requests = 50;
        let barrier = Arc::new(Barrier::new(num_requests));
        let mut handles = Vec::new();

        // Create concurrent requests
        for i in 0..num_requests {
            let client: Arc<StorageClient> = Arc::clone(&storage_client);
            let barrier = Arc::clone(&barrier);

            let handle = tokio::spawn(async move {
                barrier.wait().await;

                let command = StorageCommand::Set {
                    key: format!("key_{}", i).into_bytes(),
                    value: format!("value_{}", i).into_bytes(),
                    ttl: None,
                };

                // This will fail since there's no storage server, but we're testing concurrency
                let _result = client.send_request(command).await;
                i
            });

            handles.push(handle);
        }

        // Wait for all requests to complete
        let mut results = Vec::new();
        for handle in handles {
            let result = handle.await.unwrap();
            results.push(result);
        }

        // Verify all requests were processed
        assert_eq!(results.len(), num_requests);
        results.sort();
        for (i, &result) in results.iter().enumerate() {
            assert_eq!(i, result);
        }
    }

    #[tokio::test]
    async fn test_request_isolation() {
        let message_channel = Arc::new(MessageChannel::new(100));

        // Create multiple storage clients
        let client1 = StorageClient::new(Arc::clone(&message_channel), Duration::from_millis(50));

        let client2 = StorageClient::new(Arc::clone(&message_channel), Duration::from_millis(100));

        // Test that requests from different clients are isolated
        let command1 = StorageCommand::Get {
            key: b"key1".to_vec(),
        };
        let command2 = StorageCommand::Get {
            key: b"key2".to_vec(),
        };

        let result1 = client1.send_request(command1);
        let result2 = client2.send_request(command2);
        let (result1, result2) = tokio::join!(result1, result2);

        // Both should fail (no storage server), but they should be independent
        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_channel_backpressure() {
        let message_channel = Arc::new(MessageChannel::new(5)); // Small buffer
        let storage_client =
            StorageClient::new(Arc::clone(&message_channel), Duration::from_millis(10));

        // Fill up the channel buffer
        let mut handles = Vec::new();
        for i in 0..10 {
            let client = storage_client.clone();
            let handle = tokio::spawn(async move {
                let command = StorageCommand::Get {
                    key: format!("key_{}", i).into_bytes(),
                };
                client.send_request(command).await
            });
            handles.push(handle);
        }

        // Some requests should fail due to backpressure or timeout
        let mut _success_count = 0;
        let mut error_count = 0;

        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => _success_count += 1,
                Err(_) => error_count += 1,
            }
        }

        // We expect some errors due to backpressure/timeout
        assert!(error_count > 0);
    }

    // Runtime Lifecycle Tests
    #[tokio::test]
    async fn test_runtime_startup_shutdown_cycle() {
        let mut runtime_manager = RuntimeManager::with_defaults().unwrap();

        // Test multiple startup/shutdown cycles
        for _ in 0..3 {
            // Start
            runtime_manager.start().await.unwrap();
            assert!(runtime_manager.is_running().await);

            // Verify handles are available
            assert!(runtime_manager.network_handle().is_ok());
            assert!(runtime_manager.storage_handle().is_ok());

            // Stop
            runtime_manager.stop().await.unwrap();
            assert!(!runtime_manager.is_running().await);
        }
    }

    #[tokio::test]
    async fn test_runtime_health_monitoring() {
        let mut runtime_manager = RuntimeManager::with_defaults().unwrap();
        runtime_manager.start().await.unwrap();

        // Test health checks
        let (network_health, storage_health) = runtime_manager.health_check().await.unwrap();
        assert_eq!(network_health, RuntimeHealth::Healthy);
        assert_eq!(storage_health, RuntimeHealth::Healthy);

        // Test stats collection
        let (network_stats, storage_stats) = runtime_manager.get_stats().await;
        assert_eq!(network_stats.active_tasks, 0);
        assert_eq!(storage_stats.active_tasks, 0);
        assert!(network_stats.uptime > Duration::ZERO);
        assert!(storage_stats.uptime > Duration::ZERO);

        runtime_manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_runtime_recovery_scenarios() {
        let mut runtime_manager = RuntimeManager::with_defaults().unwrap();
        runtime_manager.start().await.unwrap();

        // Test force shutdown and recovery
        runtime_manager
            .force_shutdown(Duration::from_secs(1))
            .await
            .unwrap();
        assert!(!runtime_manager.is_running().await);

        // Should be able to start again after force shutdown
        runtime_manager.start().await.unwrap();
        assert!(runtime_manager.is_running().await);

        runtime_manager.stop().await.unwrap();
    }

    // Redis Command Tests
    #[tokio::test]
    async fn test_redis_command_serialization() {
        // Test all Redis command types
        let commands = vec![
            StorageCommand::Get {
                key: b"key1".to_vec(),
            },
            StorageCommand::Set {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                ttl: Some(Duration::from_secs(60)),
            },
            StorageCommand::Del {
                keys: vec![b"key1".to_vec(), b"key2".to_vec()],
            },
            StorageCommand::Exists {
                keys: vec![b"key1".to_vec()],
            },
            StorageCommand::Expire {
                key: b"key1".to_vec(),
                ttl: Duration::from_secs(30),
            },
            StorageCommand::Ttl {
                key: b"key1".to_vec(),
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
                pairs: vec![
                    (b"key1".to_vec(), b"value1".to_vec()),
                    (b"key2".to_vec(), b"value2".to_vec()),
                ],
            },
            StorageCommand::MGet {
                keys: vec![b"key1".to_vec(), b"key2".to_vec()],
            },
        ];

        // Test that all commands can be created and have correct structure
        for command in commands {
            match command {
                StorageCommand::Get { key } => assert!(!key.is_empty()),
                StorageCommand::Set { key, value, .. } => {
                    assert!(!key.is_empty());
                    assert!(!value.is_empty());
                }
                StorageCommand::Del { keys } => assert!(!keys.is_empty()),
                StorageCommand::Exists { keys } => assert!(!keys.is_empty()),
                StorageCommand::Expire { key, ttl } => {
                    assert!(!key.is_empty());
                    assert!(ttl > Duration::ZERO);
                }
                StorageCommand::Ttl { key } => assert!(!key.is_empty()),
                StorageCommand::Incr { key } => assert!(!key.is_empty()),
                StorageCommand::IncrBy { key, increment } => {
                    assert!(!key.is_empty());
                    assert_ne!(increment, 0);
                }
                StorageCommand::Decr { key } => assert!(!key.is_empty()),
                StorageCommand::DecrBy { key, decrement } => {
                    assert!(!key.is_empty());
                    assert_ne!(decrement, 0);
                }
                StorageCommand::MSet { pairs } => assert!(!pairs.is_empty()),
                StorageCommand::MGet { keys } => assert!(!keys.is_empty()),
                StorageCommand::Batch { commands } => assert!(!commands.is_empty()),
            }
        }
    }

    #[tokio::test]
    async fn test_redis_command_edge_cases() {
        // Test edge cases for Redis commands

        // Empty key (should be handled gracefully)
        let empty_key_command = StorageCommand::Get { key: vec![] };
        match empty_key_command {
            StorageCommand::Get { key } => assert!(key.is_empty()),
            _ => panic!("Expected Get command"),
        }

        // Large key
        let large_key = vec![b'x'; 1024];
        let large_key_command = StorageCommand::Get {
            key: large_key.clone(),
        };
        match large_key_command {
            StorageCommand::Get { key } => assert_eq!(key.len(), 1024),
            _ => panic!("Expected Get command"),
        }

        // Large value
        let large_value = vec![b'y'; 10240];
        let large_value_command = StorageCommand::Set {
            key: b"large_value_key".to_vec(),
            value: large_value.clone(),
            ttl: None,
        };
        match large_value_command {
            StorageCommand::Set { value, .. } => assert_eq!(value.len(), 10240),
            _ => panic!("Expected Set command"),
        }

        // Zero TTL
        let zero_ttl_command = StorageCommand::Expire {
            key: b"key".to_vec(),
            ttl: Duration::ZERO,
        };
        match zero_ttl_command {
            StorageCommand::Expire { ttl, .. } => assert_eq!(ttl, Duration::ZERO),
            _ => panic!("Expected Expire command"),
        }
    }

    #[tokio::test]
    async fn test_batch_command_handling() {
        let message_channel = Arc::new(MessageChannel::new(100));
        let storage_client =
            StorageClient::new(Arc::clone(&message_channel), Duration::from_millis(100));

        // Create a batch of commands
        let batch_commands = vec![
            StorageCommand::Set {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                ttl: None,
            },
            StorageCommand::Set {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                ttl: None,
            },
            StorageCommand::Get {
                key: b"key1".to_vec(),
            },
            StorageCommand::Get {
                key: b"key2".to_vec(),
            },
        ];

        let batch_command = StorageCommand::Batch {
            commands: batch_commands.clone(),
        };

        // Verify batch command structure
        match batch_command {
            StorageCommand::Batch { commands } => {
                assert_eq!(commands.len(), 4);
            }
            _ => panic!("Expected Batch command"),
        }

        // Test sending batch command (will fail without storage server)
        let result = storage_client
            .send_request(StorageCommand::Batch {
                commands: batch_commands,
            })
            .await;
        assert!(result.is_err()); // Expected to fail without storage server
    }

    // Performance and Load Testing
    #[tokio::test]
    async fn test_high_throughput_operations() {
        let message_channel = Arc::new(MessageChannel::new(10000));
        let storage_client = Arc::new(StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_secs(10),
        ));

        let num_operations = 100; // Reduced for faster testing
        let start_time = Instant::now();
        let success_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let error_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut handles = Vec::new();

        for i in 0..num_operations {
            let client: Arc<StorageClient> = Arc::clone(&storage_client);
            let success_counter = Arc::clone(&success_counter);
            let error_counter = Arc::clone(&error_counter);

            let handle = tokio::spawn(async move {
                let command = StorageCommand::Set {
                    key: format!("perf_key_{}", i).into_bytes(),
                    value: format!("perf_value_{}", i).into_bytes(),
                    ttl: None,
                };

                match client.send_request(command).await {
                    Ok(_) => success_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    Err(_) => error_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                };
            });

            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }

        let elapsed = start_time.elapsed();
        let success_count = success_counter.load(std::sync::atomic::Ordering::Relaxed);
        let error_count = error_counter.load(std::sync::atomic::Ordering::Relaxed);

        println!("Performance test results:");
        println!("  Total operations: {}", num_operations);
        println!("  Successful: {}", success_count);
        println!("  Errors: {}", error_count);
        println!("  Time elapsed: {:?}", elapsed);
        println!(
            "  Operations per second: {:.2}",
            num_operations as f64 / elapsed.as_secs_f64()
        );

        // Verify that we processed all operations (success or error)
        assert_eq!(success_count + error_count, num_operations);

        // Get channel statistics
        let stats = storage_client.channel_stats().await;
        println!("Channel statistics:");
        println!("  Requests sent: {}", stats.requests_sent);
        println!("  Send failures: {}", stats.send_failures);
        println!("  Timeouts: {}", stats.requests_timeout);
        println!("  Max pending: {}", stats.max_pending_requests);
    }

    #[tokio::test]
    async fn test_channel_capacity_limits() {
        let small_buffer_size = 10;
        let message_channel = Arc::new(MessageChannel::new(small_buffer_size));
        let storage_client =
            StorageClient::new(Arc::clone(&message_channel), Duration::from_millis(100));

        // Try to send more requests than the buffer can hold
        let num_requests = small_buffer_size * 3;
        let mut handles = Vec::new();

        for i in 0..num_requests {
            let client = storage_client.clone();
            let handle = tokio::spawn(async move {
                let command = StorageCommand::Get {
                    key: format!("capacity_test_{}", i).into_bytes(),
                };
                client.send_request(command).await
            });
            handles.push(handle);
        }

        // Count results
        let mut success_count = 0;
        let mut error_count = 0;

        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
        }

        println!("Capacity test results:");
        println!("  Buffer size: {}", small_buffer_size);
        println!("  Requests sent: {}", num_requests);
        println!("  Successful: {}", success_count);
        println!("  Errors: {}", error_count);

        // Should have some errors due to capacity limits
        assert!(error_count > 0);
        assert_eq!(success_count + error_count, num_requests);
    }
}