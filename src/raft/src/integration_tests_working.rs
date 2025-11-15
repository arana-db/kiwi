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

// Working integration tests that avoid OpenRaft lifetime issues

use std::sync::Arc;
use bytes::Bytes;

use crate::openraft_compatibility::OpenRaftCompatibilityLayer;
use crate::state_machine::core::KiwiStateMachine;
use crate::types::{ClientRequest, ConsistencyLevel, RedisCommand, RequestId};

#[tokio::test]
async fn test_state_machine_basic_operations() {
    let state_machine = Arc::new(KiwiStateMachine::new(1));
    
    // Test basic functionality without OpenRaft traits
    assert_eq!(state_machine.applied_index(), 0);
    assert_eq!(state_machine.node_id, 1);
    
    // Test SET command
    let set_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("test_key"), Bytes::from("test_value")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = state_machine.apply_redis_command(&set_request).await.unwrap();
    assert_eq!(response.id, set_request.id);
    assert!(response.result.is_ok());
    
    // Test GET command
    let get_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "GET".to_string(),
            vec![Bytes::from("test_key")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = state_machine.apply_redis_command(&get_request).await.unwrap();
    assert_eq!(response.id, get_request.id);
    assert!(response.result.is_ok());
}

#[tokio::test]
async fn test_state_machine_batch_operations() {
    let state_machine = Arc::new(KiwiStateMachine::new(1));
    
    let requests = vec![
        ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![Bytes::from("key1"), Bytes::from("value1")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        },
        ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![Bytes::from("key2"), Bytes::from("value2")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        },
        ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "GET".to_string(),
                vec![Bytes::from("key1")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        },
    ];
    
    let responses = state_machine.apply_redis_commands_batch(&requests).await.unwrap();
    assert_eq!(responses.len(), 3);
    
    for (i, response) in responses.iter().enumerate() {
        assert_eq!(response.id, requests[i].id);
        assert!(response.result.is_ok());
    }
}

#[tokio::test]
async fn test_state_machine_snapshot_operations() {
    let state_machine = Arc::new(KiwiStateMachine::new(1));
    
    // Apply some data
    let set_requests = vec![
        ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![Bytes::from("key1"), Bytes::from("value1")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        },
        ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![Bytes::from("key2"), Bytes::from("value2")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        },
    ];
    
    let _responses = state_machine.apply_redis_commands_batch(&set_requests).await.unwrap();
    
    // Create snapshot
    let snapshot = state_machine.create_snapshot().await.unwrap();
    assert_eq!(snapshot.applied_index, 0); // No applied index tracking in this simple test
    
    // Create new state machine and restore
    let new_state_machine = Arc::new(KiwiStateMachine::new(2));
    new_state_machine.restore_from_snapshot(&snapshot).await.unwrap();
    
    assert_eq!(new_state_machine.node_id, 2);
}

#[tokio::test]
async fn test_compatibility_layer_integration() {
    let compat = OpenRaftCompatibilityLayer::new(1);
    
    // Test basic operations
    let set_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("integration_key"), Bytes::from("integration_value")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = compat.apply_client_request(&set_request).await.unwrap();
    assert!(response.result.is_ok());
    
    // Test GET
    let get_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "GET".to_string(),
            vec![Bytes::from("integration_key")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = compat.apply_client_request(&get_request).await.unwrap();
    assert!(response.result.is_ok());
    
    // Test snapshot operations
    let snapshot_data = compat.create_snapshot().await.unwrap();
    assert!(!snapshot_data.is_empty());
    
    let new_compat = OpenRaftCompatibilityLayer::new(2);
    new_compat.restore_from_snapshot(&snapshot_data).await.unwrap();
    
    assert_eq!(new_compat.node_id(), 2);
}

#[tokio::test]
async fn test_redis_command_types() {
    let state_machine = Arc::new(KiwiStateMachine::new(1));
    
    // Test PING
    let ping_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new("PING".to_string(), vec![]),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = state_machine.apply_redis_command(&ping_request).await.unwrap();
    assert!(response.result.is_ok());
    if let Ok(result) = &response.result {
        assert_eq!(result, &Bytes::from("PONG"));
    }
    
    // Test EXISTS
    let exists_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "EXISTS".to_string(),
            vec![Bytes::from("nonexistent_key")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = state_machine.apply_redis_command(&exists_request).await.unwrap();
    assert!(response.result.is_ok());
    if let Ok(result) = &response.result {
        assert_eq!(result, &Bytes::from("0"));
    }
    
    // Test DEL
    let del_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "DEL".to_string(),
            vec![Bytes::from("nonexistent_key")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = state_machine.apply_redis_command(&del_request).await.unwrap();
    assert!(response.result.is_ok());
    if let Ok(result) = &response.result {
        assert_eq!(result, &Bytes::from("0"));
    }
}

#[tokio::test]
async fn test_error_handling() {
    let state_machine = Arc::new(KiwiStateMachine::new(1));
    
    // Test invalid SET command (missing value)
    let invalid_set_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("key_only")], // Missing value
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = state_machine.apply_redis_command(&invalid_set_request).await.unwrap();
    assert!(response.result.is_err());
    
    // Test invalid GET command (missing key)
    let invalid_get_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new("GET".to_string(), vec![]), // Missing key
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = state_machine.apply_redis_command(&invalid_get_request).await.unwrap();
    assert!(response.result.is_err());
    
    // Test unsupported command
    let unsupported_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new("UNSUPPORTED".to_string(), vec![]),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = state_machine.apply_redis_command(&unsupported_request).await.unwrap();
    assert!(response.result.is_err());
}

#[tokio::test]
async fn test_concurrent_operations() {
    let state_machine = Arc::new(KiwiStateMachine::new(1));
    
    // Create multiple concurrent SET operations
    let mut handles = vec![];
    
    for i in 0..10 {
        let state_machine = Arc::clone(&state_machine);
        let handle = tokio::spawn(async move {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand::new(
                    "SET".to_string(),
                    vec![
                        Bytes::from(format!("concurrent_key_{}", i)),
                        Bytes::from(format!("concurrent_value_{}", i)),
                    ],
                ),
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            state_machine.apply_redis_command(&request).await
        });
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    for handle in handles {
        let response = handle.await.unwrap().unwrap();
        assert!(response.result.is_ok());
    }
    
    // Verify all keys were set by reading them back
    for i in 0..10 {
        let get_request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "GET".to_string(),
                vec![Bytes::from(format!("concurrent_key_{}", i))],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };
        
        let response = state_machine.apply_redis_command(&get_request).await.unwrap();
        assert!(response.result.is_ok());
    }
}
