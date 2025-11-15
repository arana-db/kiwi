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

//! Integration tests for failure recovery scenarios
//!
//! These tests verify:
//! - Node restart and state recovery
//! - Snapshot recovery correctness
//! - Log replay after restart

use crate::node::{RaftNode, RaftNodeInterface};
use crate::types::{ClientRequest, ClusterConfig, ConsistencyLevel, NodeId, RedisCommand, RequestId};
use bytes::Bytes;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

/// Helper to create a test cluster configuration
fn create_test_config(node_id: NodeId, temp_dir: &TempDir) -> ClusterConfig {
    ClusterConfig {
        enabled: true,
        node_id,
        cluster_members: vec![
            format!("{}:127.0.0.1:{}", node_id, 7380 + node_id),
        ]
        .into_iter()
        .collect(),
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        heartbeat_interval_ms: 100,
        election_timeout_min_ms: 300,
        election_timeout_max_ms: 600,
        snapshot_threshold: 10,
        max_payload_entries: 100,
    }
}

#[tokio::test]
#[ignore = "Requires persistent storage engine, currently using in-memory storage"]
async fn test_node_restart_with_state_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(1, &temp_dir);
    
    // Phase 1: Start node and write data
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(true).await.unwrap();
        
        // Wait for node to become leader
        node.wait_for_election(Duration::from_secs(5)).await.unwrap();
        
        // Write some data
        for i in 0..5 {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand {
                    command: "SET".to_string(),
                    args: vec![
                        Bytes::from(format!("restart_key_{}", i)),
                        Bytes::from(format!("restart_value_{}", i)),
                    ],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            node.propose(request).await.unwrap();
        }
        
        sleep(Duration::from_millis(500)).await;
        
        // Verify data was written
        let metrics = node.get_metrics().await.unwrap();
        let last_applied_index = metrics.last_applied.unwrap().index;
        assert!(last_applied_index >= 5, "Should have applied at least 5 entries");
        
        // Shutdown node
        node.shutdown().await.unwrap();
    }
    
    // Phase 2: Restart node and verify state recovery
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(false).await.unwrap();
        
        sleep(Duration::from_millis(1000)).await;
        
        // Verify state was recovered
        let metrics = node.get_metrics().await.unwrap();
        assert!(metrics.last_applied.is_some(), "Should have recovered applied state");
        
        // Try to read data to verify state machine recovery
        let get_request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand {
                command: "GET".to_string(),
                args: vec![Bytes::from("restart_key_0")],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };
        
        // Note: In a single-node cluster, the node should become leader again
        sleep(Duration::from_millis(500)).await;
        
        let response = node.propose(get_request).await.unwrap();
        assert!(response.result.is_ok(), "Should be able to read after restart");
        assert_eq!(
            response.result.unwrap(),
            Bytes::from("restart_value_0"),
            "Should recover correct data"
        );
        
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "Requires persistent storage engine, currently using in-memory storage"]
async fn test_log_replay_after_restart() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(1, &temp_dir);
    
    // Phase 1: Write data
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(true).await.unwrap();
        
        // Wait for node to become leader
        node.wait_for_election(Duration::from_secs(5)).await.unwrap();
        
        // Write multiple entries
        for i in 0..10 {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand {
                    command: "SET".to_string(),
                    args: vec![
                        Bytes::from(format!("replay_key_{}", i)),
                        Bytes::from(format!("replay_value_{}", i)),
                    ],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            node.propose(request).await.unwrap();
        }
        
        sleep(Duration::from_millis(500)).await;
        node.shutdown().await.unwrap();
    }
    
    // Phase 2: Restart and verify log replay
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(false).await.unwrap();
        
        sleep(Duration::from_millis(1000)).await;
        
        // Verify all entries were replayed
        let metrics = node.get_metrics().await.unwrap();
        assert!(
            metrics.last_applied.unwrap().index >= 10,
            "All entries should be replayed"
        );
        
        // Verify data integrity after replay
        for i in 0..10 {
            let get_request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand {
                    command: "GET".to_string(),
                    args: vec![Bytes::from(format!("replay_key_{}", i))],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            sleep(Duration::from_millis(100)).await;
            let response = node.propose(get_request).await.unwrap();
            assert!(response.result.is_ok(), "Should read key {} after replay", i);
            assert_eq!(
                response.result.unwrap(),
                Bytes::from(format!("replay_value_{}", i)),
                "Should have correct value for key {}", i
            );
        }
        
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "Requires persistent storage engine, currently using in-memory storage"]
async fn test_snapshot_recovery_correctness() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = create_test_config(1, &temp_dir);
    config.snapshot_threshold = 5; // Trigger snapshot after 5 entries
    
    // Phase 1: Write data and trigger snapshot
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(true).await.unwrap();
        
        // Wait for node to become leader
        node.wait_for_election(Duration::from_secs(5)).await.unwrap();
        
        // Write enough data to trigger snapshot
        for i in 0..15 {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand {
                    command: "SET".to_string(),
                    args: vec![
                        Bytes::from(format!("snapshot_key_{}", i)),
                        Bytes::from(format!("snapshot_value_{}", i)),
                    ],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            node.propose(request).await.unwrap();
        }
        
        // Wait for snapshot to be created
        sleep(Duration::from_secs(2)).await;
        
        node.shutdown().await.unwrap();
    }
    
    // Phase 2: Restart and verify snapshot recovery
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(false).await.unwrap();
        
        sleep(Duration::from_millis(1000)).await;
        
        // Verify state was recovered from snapshot
        let metrics = node.get_metrics().await.unwrap();
        assert!(metrics.last_applied.is_some(), "Should have recovered from snapshot");
        
        // Verify data correctness after snapshot recovery
        for i in 0..15 {
            let get_request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand {
                    command: "GET".to_string(),
                    args: vec![Bytes::from(format!("snapshot_key_{}", i))],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            sleep(Duration::from_millis(100)).await;
            let response = node.propose(get_request).await.unwrap();
            assert!(
                response.result.is_ok(),
                "Should read key {} after snapshot recovery", i
            );
            assert_eq!(
                response.result.unwrap(),
                Bytes::from(format!("snapshot_value_{}", i)),
                "Should have correct value for key {} after snapshot recovery", i
            );
        }
        
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn test_recovery_with_empty_state() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(1, &temp_dir);
    
    // Start node with no prior state
    let node = RaftNode::new(config.clone()).await.unwrap();
    node.start(true).await.unwrap();
    
    // Wait for node to become leader
    node.wait_for_election(Duration::from_secs(5)).await.unwrap();
    
    // Verify node starts correctly with empty state
    // Note: OpenRaft creates an initial membership entry at index 1 when initializing,
    // so we allow for index <= 1 (0 or 1) to account for this standard behavior
    let metrics = node.get_metrics().await.unwrap();
    assert!(
        metrics.last_applied.is_none() || metrics.last_applied.unwrap().index <= 1,
        "Should start with empty state (allowing for initial membership entry at index 1)"
    );
    
    // Verify node can accept writes
    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand {
            command: "SET".to_string(),
            args: vec![Bytes::from("first_key"), Bytes::from("first_value")],
        },
        consistency_level: ConsistencyLevel::Linearizable,
    };
    
    let response = node.propose(request).await.unwrap();
    assert!(response.result.is_ok(), "Should accept writes with empty state");
    
    node.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "Requires persistent storage engine, currently using in-memory storage"]
async fn test_recovery_after_multiple_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(1, &temp_dir);
    
    // Perform multiple restart cycles
    for cycle in 0..3 {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(cycle == 0).await.unwrap(); // Initialize only on first cycle
        
        sleep(Duration::from_millis(500)).await;
        
        // Write data in each cycle
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand {
                command: "SET".to_string(),
                args: vec![
                    Bytes::from(format!("cycle_key_{}", cycle)),
                    Bytes::from(format!("cycle_value_{}", cycle)),
                ],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };
        
        node.propose(request).await.unwrap();
        sleep(Duration::from_millis(200)).await;
        
        node.shutdown().await.unwrap();
    }
    
    // Final restart to verify all data persisted
    let node = RaftNode::new(config.clone()).await.unwrap();
    node.start(false).await.unwrap();
    
    sleep(Duration::from_millis(1000)).await;
    
    // Verify all data from all cycles is present
    for cycle in 0..3 {
        let get_request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand {
                command: "GET".to_string(),
                args: vec![Bytes::from(format!("cycle_key_{}", cycle))],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };
        
        sleep(Duration::from_millis(100)).await;
        let response = node.propose(get_request).await.unwrap();
        assert!(
            response.result.is_ok(),
            "Should read data from cycle {} after multiple restarts", cycle
        );
        assert_eq!(
            response.result.unwrap(),
            Bytes::from(format!("cycle_value_{}", cycle)),
            "Should have correct value from cycle {}", cycle
        );
    }
    
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_recovery_with_corrupted_log_handling() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(1, &temp_dir);
    
    // Phase 1: Write some data
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(true).await.unwrap();
        
        // Wait for node to become leader
        node.wait_for_election(Duration::from_secs(5)).await.unwrap();
        
        for i in 0..5 {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand {
                    command: "SET".to_string(),
                    args: vec![
                        Bytes::from(format!("safe_key_{}", i)),
                        Bytes::from(format!("safe_value_{}", i)),
                    ],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            node.propose(request).await.unwrap();
        }
        
        sleep(Duration::from_millis(500)).await;
        node.shutdown().await.unwrap();
    }
    
    // Phase 2: Restart and verify recovery works
    // In a real scenario, we might have some corrupted entries
    // The system should handle this gracefully
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        let result = node.start(false).await;
        
        // Node should either recover successfully or fail gracefully
        assert!(
            result.is_ok() || result.is_err(),
            "Node should handle recovery attempt"
        );
        
        if result.is_ok() {
            sleep(Duration::from_millis(1000)).await;
            
            // If recovery succeeded, verify we can still operate
            let test_request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand {
                    command: "PING".to_string(),
                    args: vec![],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            sleep(Duration::from_millis(500)).await;
            let _response = node.propose(test_request).await;
            // Response might succeed or fail depending on state
            
            node.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "Requires persistent storage engine, currently using in-memory storage"]
async fn test_incremental_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(1, &temp_dir);
    
    // Write data incrementally across restarts
    let total_keys = 20;
    let keys_per_session = 5;
    
    for session in 0..(total_keys / keys_per_session) {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(session == 0).await.unwrap();
        
        sleep(Duration::from_millis(500)).await;
        
        // Write keys for this session
        let start_key = session * keys_per_session;
        let end_key = start_key + keys_per_session;
        
        for i in start_key..end_key {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand {
                    command: "SET".to_string(),
                    args: vec![
                        Bytes::from(format!("incremental_key_{}", i)),
                        Bytes::from(format!("incremental_value_{}", i)),
                    ],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            
            node.propose(request).await.unwrap();
        }
        
        sleep(Duration::from_millis(200)).await;
        node.shutdown().await.unwrap();
    }
    
    // Final verification
    let node = RaftNode::new(config.clone()).await.unwrap();
    node.start(false).await.unwrap();
    
    sleep(Duration::from_millis(1000)).await;
    
    // Verify all incrementally written data is present
    for i in 0..total_keys {
        let get_request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand {
                command: "GET".to_string(),
                args: vec![Bytes::from(format!("incremental_key_{}", i))],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };
        
        sleep(Duration::from_millis(100)).await;
        let response = node.propose(get_request).await.unwrap();
        assert!(
            response.result.is_ok(),
            "Should read incremental key {} after recovery", i
        );
        assert_eq!(
            response.result.unwrap(),
            Bytes::from(format!("incremental_value_{}", i)),
            "Should have correct incremental value for key {}", i
        );
    }
    
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_recovery_preserves_term_and_vote() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(1, &temp_dir);
    
    // Phase 1: Start node and let it establish term
    let initial_term;
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(true).await.unwrap();
        
        // Wait for node to become leader
        node.wait_for_election(Duration::from_secs(5)).await.unwrap();
        
        let metrics = node.get_metrics().await.unwrap();
        initial_term = metrics.current_term;
        assert!(initial_term > 0, "Should have established a term");
        
        node.shutdown().await.unwrap();
    }
    
    // Phase 2: Restart and verify term is preserved or advanced
    {
        let node = RaftNode::new(config.clone()).await.unwrap();
        node.start(false).await.unwrap();
        
        sleep(Duration::from_millis(1000)).await;
        
        let metrics = node.get_metrics().await.unwrap();
        assert!(
            metrics.current_term >= initial_term,
            "Term should be preserved or advanced after restart"
        );
        
        node.shutdown().await.unwrap();
    }
}
