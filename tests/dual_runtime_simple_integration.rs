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

//! Simple integration tests to verify dual runtime basic functionality

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use runtime::{
    RuntimeManager, MessageChannel, StorageCommand, 
    RequestId, RequestPriority, StorageClient,
    StorageRequest, ManagerRuntimeHealth
};

#[tokio::test]
async fn test_basic_runtime_functionality() {
    // Test basic runtime manager functionality
    let mut runtime_manager = RuntimeManager::with_defaults().unwrap();
    
    // Start runtime manager
    runtime_manager.start().await.unwrap();
    assert!(runtime_manager.is_running().await);
    
    // Test health checks
    let (network_health, storage_health) = runtime_manager.health_check().await.unwrap();
    assert_eq!(network_health, ManagerRuntimeHealth::Healthy);
    assert_eq!(storage_health, ManagerRuntimeHealth::Healthy);
    
    // Stop runtime manager
    runtime_manager.stop().await.unwrap();
    assert!(!runtime_manager.is_running().await);
}

#[tokio::test]
async fn test_message_channel_basic() {
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
        timestamp: std::time::Instant::now(),
        priority: RequestPriority::Normal,
    };
    
    // Send request
    let send_result = sender.send(request).await;
    assert!(send_result.is_ok());
    
    // Receive request
    let received = tokio::time::timeout(Duration::from_millis(100), receiver.recv()).await;
    assert!(received.is_ok());
    assert!(received.unwrap().is_some());
}

#[tokio::test]
async fn test_storage_client_basic() {
    let message_channel = Arc::new(MessageChannel::new(100));
    let storage_client = StorageClient::new(
        Arc::clone(&message_channel),
        Duration::from_millis(100),
    );
    
    // Test that client is healthy
    assert!(storage_client.is_healthy());
    
    // Test timeout handling (should fail since there's no storage server)
    let command = StorageCommand::Get { key: b"test".to_vec() };
    let result = storage_client.send_request(command).await;
    
    assert!(result.is_err());
}

#[tokio::test]
async fn test_storage_commands() {
    // Test that we can create various storage commands
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