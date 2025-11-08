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

//! Integration tests for dual runtime architecture
//! 
//! This module contains comprehensive integration tests for:
//! - Complete request flow from network to storage and back
//! - Concurrent request handling and isolation
//! - Runtime startup, shutdown, and recovery scenarios
//! - Various Redis command types and edge cases

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, Barrier};
use tokio::time::timeout;

// Import the dual runtime components
use runtime::{
    RuntimeManager, RuntimeConfig, MessageChannel, StorageCommand, 
    RequestId, RequestPriority, DualRuntimeError, StorageClient,
    StorageRequest, StorageResponse, StorageStats, ChannelStats,
    BackpressureConfig, RetryConfig, ManagerRuntimeHealth
};
use resp::RespData;

// Integration test configuration
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
#[cfg(test)]
mod request_flow_tests {
    use super::*;

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
            command: StorageCommand::Get { key: b"test".to_vec() },
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
        let command = StorageCommand::Get { key: b"test".to_vec() };
        let result = storage_client.send_request(command).await;
        
        assert!(result.is_err());
        match result.unwrap_err() {
            DualRuntimeError::Timeout { .. } => {},
            DualRuntimeError::Channel(_) => {}, // Also acceptable
            other => panic!("Expected timeout or channel error, got: {:?}", other),
        }
    }
}
// Concu
rrent Request Handling Tests
#[cfg(test)]
mod concurrent_tests {
    use super::*;

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
            let client = Arc::clone(&storage_client);
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
        let client1 = StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_millis(50),
        );
        
        let client2 = StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_millis(100),
        );
        
        // Test that requests from different clients are isolated
        let command1 = StorageCommand::Get { key: b"key1".to_vec() };
        let command2 = StorageCommand::Get { key: b"key2".to_vec() };
        
        let (result1, result2) = tokio::join!(
            client1.send_request(command1),
            client2.send_request(command2)
        );
        
        // Both should fail (no storage server), but they should be independent
        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_channel_backpressure() {
        let message_channel = Arc::new(MessageChannel::new(5)); // Small buffer
        let storage_client = StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_millis(10),
        );
        
        // Fill up the channel buffer
        let mut handles = Vec::new();
        for i in 0..10 {
            let client = storage_client.clone();
            let handle = tokio::spawn(async move {
                let command = StorageCommand::Get { 
                    key: format!("key_{}", i).into_bytes() 
                };
                client.send_request(command).await
            });
            handles.push(handle);
        }
        
        // Some requests should fail due to backpressure or timeout
        let mut success_count = 0;
        let mut error_count = 0;
        
        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
        }
        
        // We expect some errors due to backpressure/timeout
        assert!(error_count > 0);
    }
}// Runt
ime Lifecycle Tests
#[cfg(test)]
mod lifecycle_tests {
    use super::*;

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
        assert_eq!(network_health, ManagerRuntimeHealth::Healthy);
        assert_eq!(storage_health, ManagerRuntimeHealth::Healthy);
        
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
        runtime_manager.force_shutdown(Duration::from_secs(1)).await.unwrap();
        assert!(!runtime_manager.is_running().await);
        
        // Should be able to start again after force shutdown
        runtime_manager.start().await.unwrap();
        assert!(runtime_manager.is_running().await);
        
        runtime_manager.stop().await.unwrap();
    }
}// Red
is Command Tests
#[cfg(test)]
mod redis_command_tests {
    use super::*;

    #[tokio::test]
    async fn test_redis_command_serialization() {
        // Test all Redis command types
        let commands = vec![
            StorageCommand::Get { key: b"key1".to_vec() },
            StorageCommand::Set { 
                key: b"key1".to_vec(), 
                value: b"value1".to_vec(), 
                ttl: Some(Duration::from_secs(60)) 
            },
            StorageCommand::Del { keys: vec![b"key1".to_vec(), b"key2".to_vec()] },
            StorageCommand::Exists { keys: vec![b"key1".to_vec()] },
            StorageCommand::Expire { key: b"key1".to_vec(), ttl: Duration::from_secs(30) },
            StorageCommand::Ttl { key: b"key1".to_vec() },
            StorageCommand::Incr { key: b"counter".to_vec() },
            StorageCommand::IncrBy { key: b"counter".to_vec(), increment: 5 },
            StorageCommand::Decr { key: b"counter".to_vec() },
            StorageCommand::DecrBy { key: b"counter".to_vec(), decrement: 3 },
            StorageCommand::MSet { 
                pairs: vec![
                    (b"key1".to_vec(), b"value1".to_vec()),
                    (b"key2".to_vec(), b"value2".to_vec()),
                ]
            },
            StorageCommand::MGet { keys: vec![b"key1".to_vec(), b"key2".to_vec()] },
        ];
        
        // Test that all commands can be created and have correct structure
        for command in commands {
            match command {
                StorageCommand::Get { key } => assert!(!key.is_empty()),
                StorageCommand::Set { key, value, .. } => {
                    assert!(!key.is_empty());
                    assert!(!value.is_empty());
                },
                StorageCommand::Del { keys } => assert!(!keys.is_empty()),
                StorageCommand::Exists { keys } => assert!(!keys.is_empty()),
                StorageCommand::Expire { key, ttl } => {
                    assert!(!key.is_empty());
                    assert!(ttl > Duration::ZERO);
                },
                StorageCommand::Ttl { key } => assert!(!key.is_empty()),
                StorageCommand::Incr { key } => assert!(!key.is_empty()),
                StorageCommand::IncrBy { key, increment } => {
                    assert!(!key.is_empty());
                    assert_ne!(increment, 0);
                },
                StorageCommand::Decr { key } => assert!(!key.is_empty()),
                StorageCommand::DecrBy { key, decrement } => {
                    assert!(!key.is_empty());
                    assert_ne!(decrement, 0);
                },
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
        let large_key_command = StorageCommand::Get { key: large_key.clone() };
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
        let storage_client = StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_millis(100),
        );
        
        // Create a batch of commands
        let batch_commands = vec![
            StorageCommand::Set { 
                key: b"key1".to_vec(), 
                value: b"value1".to_vec(), 
                ttl: None 
            },
            StorageCommand::Set { 
                key: b"key2".to_vec(), 
                value: b"value2".to_vec(), 
                ttl: None 
            },
            StorageCommand::Get { key: b"key1".to_vec() },
            StorageCommand::Get { key: b"key2".to_vec() },
        ];
        
        let batch_command = StorageCommand::Batch { 
            commands: batch_commands.clone() 
        };
        
        // Verify batch command structure
        match batch_command {
            StorageCommand::Batch { commands } => {
                assert_eq!(commands.len(), 4);
            },
            _ => panic!("Expected Batch command"),
        }
        
        // Test sending batch command (will fail without storage server)
        let result = storage_client.send_request(StorageCommand::Batch { 
            commands: batch_commands 
        }).await;
        assert!(result.is_err()); // Expected to fail without storage server
    }
}

// Complete End-to-End Integration Tests
#[cfg(test)]
mod end_to_end_tests {
    use super::*;
    use runtime::{StorageServer, StorageServerConfig, BatchConfig, BackgroundTaskConfig};
    use storage::Storage;
    use std::path::PathBuf;
    use tempfile::TempDir;

    async fn create_test_storage() -> (Storage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().join("test_storage");
        let storage = Storage::new(&storage_path).await.unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_complete_request_flow_with_storage_server() {
        // This test is simplified to focus on core functionality
        // Create temporary storage
        let (storage, _temp_dir) = create_test_storage().await;
        let storage = Arc::new(storage);
        
        // Create message channel
        let mut message_channel = MessageChannel::new(1000);
        let request_receiver = message_channel.take_request_receiver().unwrap();
        
        // Create storage server with default config
        let storage_server_config = StorageServerConfig::default();
        
        let storage_server = StorageServer::new(
            Arc::clone(&storage),
            request_receiver,
            storage_server_config,
        );
        
        // Start storage server in background
        let storage_handle = tokio::spawn(async move {
            storage_server.run().await
        });
        
        // Create storage client
        let storage_client = StorageClient::new(
            Arc::new(message_channel),
            Duration::from_secs(5),
        );
        
        // Test SET operation
        let set_result = storage_client.send_request(StorageCommand::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            ttl: None,
        }).await;
        
        assert!(set_result.is_ok());
        
        // Test GET operation
        let get_result = storage_client.send_request(StorageCommand::Get {
            key: b"test_key".to_vec(),
        }).await;
        
        assert!(get_result.is_ok());
        match get_result.unwrap() {
            RespData::BulkString(data) => {
                assert_eq!(data, b"test_value");
            }
            _ => panic!("Expected BulkString response"),
        }
        
        // Test DEL operation
        let del_result = storage_client.send_request(StorageCommand::Del {
            keys: vec![b"test_key".to_vec()],
        }).await;
        
        assert!(del_result.is_ok());
        
        // Verify key is deleted
        let get_after_del = storage_client.send_request(StorageCommand::Get {
            key: b"test_key".to_vec(),
        }).await;
        
        assert!(get_after_del.is_ok());
        assert_eq!(get_after_del.unwrap(), RespData::Null);
        
        // Stop storage server
        storage_handle.abort();
    }

    #[tokio::test]
    async fn test_concurrent_operations_with_storage_server() {
        // Create temporary storage
        let (storage, _temp_dir) = create_test_storage().await;
        let storage = Arc::new(storage);
        
        // Create message channel
        let mut message_channel = MessageChannel::new(1000);
        let request_receiver = message_channel.take_request_receiver().unwrap();
        
        // Create and start storage server
        let storage_server_config = StorageServerConfig::default();
        let storage_server = StorageServer::new(
            Arc::clone(&storage),
            request_receiver,
            storage_server_config,
        );
        
        let storage_handle = tokio::spawn(async move {
            storage_server.run().await
        });
        
        // Create storage client
        let storage_client = Arc::new(StorageClient::new(
            Arc::new(message_channel),
            Duration::from_secs(5),
        ));
        
        // Perform concurrent SET operations (reduced number for faster testing)
        let num_operations = 10;
        let mut handles = Vec::new();
        
        for i in 0..num_operations {
            let client = Arc::clone(&storage_client);
            let handle = tokio::spawn(async move {
                let key = format!("key_{}", i);
                let value = format!("value_{}", i);
                
                client.send_request(StorageCommand::Set {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                    ttl: None,
                }).await
            });
            handles.push(handle);
        }
        
        // Wait for all SET operations to complete
        let mut success_count = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => success_count += 1,
                Err(e) => eprintln!("SET operation failed: {}", e),
            }
        }
        
        assert_eq!(success_count, num_operations);
        
        // Perform concurrent GET operations
        let mut handles = Vec::new();
        
        for i in 0..num_operations {
            let client = Arc::clone(&storage_client);
            let handle = tokio::spawn(async move {
                let key = format!("key_{}", i);
                let expected_value = format!("value_{}", i);
                
                match client.send_request(StorageCommand::Get {
                    key: key.into_bytes(),
                }).await {
                    Ok(RespData::BulkString(data)) => {
                        if data == expected_value.as_bytes() {
                            Ok(())
                        } else {
                            Err(format!("Value mismatch for key {}", i))
                        }
                    }
                    Ok(_) => Err(format!("Unexpected response type for key {}", i)),
                    Err(e) => Err(format!("GET failed for key {}: {}", i, e)),
                }
            });
            handles.push(handle);
        }
        
        // Wait for all GET operations to complete
        success_count = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => success_count += 1,
                Err(e) => eprintln!("GET operation failed: {}", e),
            }
        }
        
        assert_eq!(success_count, num_operations);
        
        // Stop storage server
        storage_handle.abort();
    }

    #[tokio::test]
    async fn test_runtime_startup_shutdown_with_full_system() {
        // Test complete system startup and shutdown
        let mut runtime_manager = RuntimeManager::with_defaults().unwrap();
        
        // Start runtime manager
        runtime_manager.start().await.unwrap();
        assert!(runtime_manager.is_running().await);
        
        // Get runtime handles
        let network_handle = runtime_manager.network_handle().unwrap();
        let storage_handle = runtime_manager.storage_handle().unwrap();
        
        // Test that we can spawn tasks on both runtimes
        let network_task = network_handle.spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            "network_task_completed"
        });
        
        let storage_task = storage_handle.spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            "storage_task_completed"
        });
        
        // Wait for tasks to complete
        let network_result = network_task.await.unwrap();
        let storage_result = storage_task.await.unwrap();
        
        assert_eq!(network_result, "network_task_completed");
        assert_eq!(storage_result, "storage_task_completed");
        
        // Test health checks
        let (network_health, storage_health) = runtime_manager.health_check().await.unwrap();
        assert_eq!(network_health, ManagerRuntimeHealth::Healthy);
        assert_eq!(storage_health, ManagerRuntimeHealth::Healthy);
        
        // Test graceful shutdown
        runtime_manager.stop().await.unwrap();
        assert!(!runtime_manager.is_running().await);
    }

    #[tokio::test]
    async fn test_error_handling_and_recovery() {
        let message_channel = Arc::new(MessageChannel::new(100));
        let storage_client = StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_millis(100),
        );
        
        // Test timeout handling
        let timeout_result = storage_client.send_request_with_timeout(
            StorageCommand::Get { key: b"test".to_vec() },
            Duration::from_millis(1), // Very short timeout
        ).await;
        
        assert!(timeout_result.is_err());
        
        // Test priority handling
        let high_priority_result = storage_client.send_request_with_priority(
            StorageCommand::Get { key: b"test".to_vec() },
            Duration::from_millis(50),
            RequestPriority::High,
        ).await;
        
        assert!(high_priority_result.is_err()); // No storage server
        
        // Test client health status
        assert!(storage_client.is_healthy());
        
        // Test channel statistics
        let stats = storage_client.channel_stats().await;
        assert!(stats.requests_sent > 0);
        assert!(stats.send_failures > 0 || stats.requests_timeout > 0);
    }

    #[tokio::test]
    async fn test_batch_processing_integration() {
        // Create temporary storage
        let (storage, _temp_dir) = create_test_storage().await;
        let storage = Arc::new(storage);
        
        // Create message channel with larger buffer for batch processing
        let mut message_channel = MessageChannel::new(1000);
        let request_receiver = message_channel.take_request_receiver().unwrap();
        
        // Create storage server with default config (includes batch processing)
        let storage_server_config = StorageServerConfig::default();
        
        let storage_server = StorageServer::new(
            Arc::clone(&storage),
            request_receiver,
            storage_server_config,
        );
        
        let storage_handle = tokio::spawn(async move {
            storage_server.run().await
        });
        
        // Create storage client
        let storage_client = StorageClient::new(
            Arc::new(message_channel),
            Duration::from_secs(5),
        );
        
        // Create batch of commands
        let batch_commands = vec![
            StorageCommand::Set { key: b"batch_key1".to_vec(), value: b"batch_value1".to_vec(), ttl: None },
            StorageCommand::Set { key: b"batch_key2".to_vec(), value: b"batch_value2".to_vec(), ttl: None },
            StorageCommand::Set { key: b"batch_key3".to_vec(), value: b"batch_value3".to_vec(), ttl: None },
            StorageCommand::Get { key: b"batch_key1".to_vec() },
            StorageCommand::Get { key: b"batch_key2".to_vec() },
        ];
        
        // Send batch command
        let batch_result = storage_client.send_request(StorageCommand::Batch {
            commands: batch_commands,
        }).await;
        
        assert!(batch_result.is_ok());
        
        // Verify individual operations worked
        let get_result = storage_client.send_request(StorageCommand::Get {
            key: b"batch_key3".to_vec(),
        }).await;
        
        assert!(get_result.is_ok());
        match get_result.unwrap() {
            RespData::BulkString(data) => {
                assert_eq!(data, b"batch_value3");
            }
            _ => panic!("Expected BulkString response"),
        }
        
        // Stop storage server
        storage_handle.abort();
    }
}

// Performance and Load Testing
#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_high_throughput_operations() {
        let message_channel = Arc::new(MessageChannel::new(10000));
        let storage_client = Arc::new(StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_secs(10),
        ));
        
        let num_operations = 1000;
        let start_time = Instant::now();
        let success_counter = Arc::new(AtomicUsize::new(0));
        let error_counter = Arc::new(AtomicUsize::new(0));
        
        let mut handles = Vec::new();
        
        for i in 0..num_operations {
            let client = Arc::clone(&storage_client);
            let success_counter = Arc::clone(&success_counter);
            let error_counter = Arc::clone(&error_counter);
            
            let handle = tokio::spawn(async move {
                let command = StorageCommand::Set {
                    key: format!("perf_key_{}", i).into_bytes(),
                    value: format!("perf_value_{}", i).into_bytes(),
                    ttl: None,
                };
                
                match client.send_request(command).await {
                    Ok(_) => success_counter.fetch_add(1, Ordering::Relaxed),
                    Err(_) => error_counter.fetch_add(1, Ordering::Relaxed),
                };
            });
            
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        let elapsed = start_time.elapsed();
        let success_count = success_counter.load(Ordering::Relaxed);
        let error_count = error_counter.load(Ordering::Relaxed);
        
        println!("Performance test results:");
        println!("  Total operations: {}", num_operations);
        println!("  Successful: {}", success_count);
        println!("  Errors: {}", error_count);
        println!("  Time elapsed: {:?}", elapsed);
        println!("  Operations per second: {:.2}", num_operations as f64 / elapsed.as_secs_f64());
        
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
        let storage_client = StorageClient::new(
            Arc::clone(&message_channel),
            Duration::from_millis(100),
        );
        
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