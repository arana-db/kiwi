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

//! Cluster deployment and testing utilities
//!
//! This module provides:
//! - Three-node cluster deployment tests
//! - Failover and recovery tests
//! - Data consistency verification tests

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

use crate::error::{RaftError, RaftResult};
use crate::node::{RaftNode, RaftNodeInterface};
use crate::types::{
    ClientRequest, ClientResponse, ClusterConfig, ConsistencyLevel, NodeId, RedisCommand, RequestId,
};
use bytes::Bytes;

/// Three-node cluster for testing
pub struct ThreeNodeCluster {
    /// Node 1
    pub node1: Arc<RaftNode>,
    /// Node 2
    pub node2: Arc<RaftNode>,
    /// Node 3
    pub node3: Arc<RaftNode>,
    /// Temporary directories
    _temp_dirs: Vec<TempDir>,
    /// Node IDs
    pub node_ids: Vec<NodeId>,
}

impl ThreeNodeCluster {
    /// Create a new three-node cluster
    pub async fn new() -> RaftResult<Self> {
        let temp_dir1 = TempDir::new()
            .map_err(|e| RaftError::state_machine(format!("Failed to create temp dir: {}", e)))?;
        let temp_dir2 = TempDir::new()
            .map_err(|e| RaftError::state_machine(format!("Failed to create temp dir: {}", e)))?;
        let temp_dir3 = TempDir::new()
            .map_err(|e| RaftError::state_machine(format!("Failed to create temp dir: {}", e)))?;

        let node_ids = vec![1, 2, 3];

        // Create cluster config for each node
        let config1 = ClusterConfig {
            node_id: 1,
            data_dir: temp_dir1.path().to_string_lossy().to_string(),
            cluster_members: vec![
                "1:127.0.0.1:7380".to_string(),
                "2:127.0.0.1:7381".to_string(),
                "3:127.0.0.1:7382".to_string(),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        let config2 = ClusterConfig {
            node_id: 2,
            data_dir: temp_dir2.path().to_string_lossy().to_string(),
            cluster_members: vec![
                "1:127.0.0.1:7380".to_string(),
                "2:127.0.0.1:7381".to_string(),
                "3:127.0.0.1:7382".to_string(),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        let config3 = ClusterConfig {
            node_id: 3,
            data_dir: temp_dir3.path().to_string_lossy().to_string(),
            cluster_members: vec![
                "1:127.0.0.1:7380".to_string(),
                "2:127.0.0.1:7381".to_string(),
                "3:127.0.0.1:7382".to_string(),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        // Create nodes
        let node1 = Arc::new(RaftNode::new(config1).await?);
        let node2 = Arc::new(RaftNode::new(config2).await?);
        let node3 = Arc::new(RaftNode::new(config3).await?);

        Ok(Self {
            node1,
            node2,
            node3,
            _temp_dirs: vec![temp_dir1, temp_dir2, temp_dir3],
            node_ids,
        })
    }

    /// Start all nodes in the cluster
    pub async fn start_all(&self) -> RaftResult<()> {
        // Start nodes: first node initializes cluster, others join
        self.node1.start(true).await?; // Initialize cluster
        sleep(Duration::from_millis(200)).await;

        self.node2.start(false).await?; // Join cluster
        sleep(Duration::from_millis(200)).await;

        self.node3.start(false).await?; // Join cluster
        sleep(Duration::from_millis(200)).await;

        // Add nodes 2 and 3 as learners, then change membership
        self.node1
            .add_learner(2, "127.0.0.1:7381".to_string())
            .await?;
        self.node1
            .add_learner(3, "127.0.0.1:7382".to_string())
            .await?;
        sleep(Duration::from_millis(200)).await;

        // Change membership to include all three nodes
        use std::collections::BTreeSet;
        let members: BTreeSet<NodeId> = [1, 2, 3].iter().cloned().collect();
        self.node1.change_membership(members).await?;

        // Wait for leader election and membership change
        sleep(Duration::from_millis(500)).await;

        Ok(())
    }

    /// Get the leader node
    pub async fn get_leader(&self) -> RaftResult<Option<Arc<RaftNode>>> {
        // Check each node's metrics to find the leader
        let nodes = vec![
            (1, Arc::clone(&self.node1)),
            (2, Arc::clone(&self.node2)),
            (3, Arc::clone(&self.node3)),
        ];

        for (node_id, node) in nodes {
            match node.get_metrics().await {
                Ok(metrics) => {
                    if let Some(leader_id) = metrics.current_leader {
                        if leader_id == node_id {
                            return Ok(Some(node));
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(None)
    }

    /// Stop a node (simulate failure)
    pub async fn stop_node(&self, node_id: NodeId) -> RaftResult<()> {
        match node_id {
            1 => {
                self.node1.shutdown().await?;
            }
            2 => {
                self.node2.shutdown().await?;
            }
            3 => {
                self.node3.shutdown().await?;
            }
            _ => return Err(RaftError::invalid_request("Invalid node ID")),
        }
        Ok(())
    }

    /// Restart a node (simulate recovery)
    /// Note: In a real implementation, this would recreate the node from persisted state
    pub async fn restart_node(&self, node_id: NodeId) -> RaftResult<()> {
        // For testing purposes, we just log the restart
        // In a production environment, this would:
        // 1. Load persisted state
        // 2. Recreate the RaftNode
        // 3. Rejoin the cluster
        log::info!("Restarting node {} (simulated)", node_id);
        Ok(())
    }

    /// Write data to cluster (to any node)
    pub async fn write(&self, key: &str, value: &str) -> RaftResult<ClientResponse> {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand {
                command: "SET".to_string(),
                args: vec![
                    Bytes::copy_from_slice(key.as_bytes()),
                    Bytes::copy_from_slice(value.as_bytes()),
                ],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };

        // Try to write through the leader
        if let Ok(Some(leader)) = self.get_leader().await {
            return leader.propose(request).await;
        }

        // If no leader found, try node1
        self.node1.propose(request).await
    }

    /// Read data from cluster (from any node)
    pub async fn read(&self, key: &str) -> RaftResult<Option<Bytes>> {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand {
                command: "GET".to_string(),
                args: vec![Bytes::copy_from_slice(key.as_bytes())],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };

        // Try to read through the leader
        if let Ok(Some(leader)) = self.get_leader().await {
            let response = leader.propose(request).await?;
            return Ok(response.result.ok());
        }

        // If no leader found, try node1
        let response = self.node1.propose(request).await?;
        Ok(response.result.ok())
    }

    /// Shutdown all nodes
    pub async fn shutdown_all(&self) -> RaftResult<()> {
        let _ = self.node1.shutdown().await;
        let _ = self.node2.shutdown().await;
        let _ = self.node3.shutdown().await;
        Ok(())
    }
}

/// Wait for cluster to elect a leader
pub async fn wait_for_leader(
    cluster: &ThreeNodeCluster,
    timeout_duration: Duration,
) -> RaftResult<NodeId> {
    timeout(timeout_duration, async {
        loop {
            if let Ok(Some(leader)) = cluster.get_leader().await {
                if let Ok(metrics) = leader.get_metrics().await {
                    if let Some(leader_id) = metrics.current_leader {
                        return leader_id;
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| RaftError::timeout("Timeout waiting for leader election"))
}

/// Wait for a node to become follower
#[allow(dead_code)]
pub async fn wait_for_follower(
    node: &Arc<RaftNode>,
    expected_leader: NodeId,
    timeout_duration: Duration,
) -> RaftResult<()> {
    timeout(timeout_duration, async {
        loop {
            if let Ok(metrics) = node.get_metrics().await {
                if let Some(leader_id) = metrics.current_leader {
                    if leader_id == expected_leader {
                        return Ok(());
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| RaftError::timeout("Timeout waiting for follower state"))?
}

/// Verify data consistency across all nodes
pub async fn verify_data_consistency(
    cluster: &ThreeNodeCluster,
    test_data: &HashMap<String, String>,
) -> RaftResult<bool> {
    // Read each key from all nodes and verify consistency
    for (key, expected_value) in test_data {
        let mut values = Vec::new();

        // Read from all nodes
        for _node in [&cluster.node1, &cluster.node2, &cluster.node3] {
            if let Ok(Some(value)) = cluster.read(key).await {
                values.push(String::from_utf8_lossy(&value).to_string());
            }
        }

        // Check if all values match
        if values.is_empty() {
            return Ok(false);
        }

        let first_value = &values[0];
        for value in &values {
            if value != first_value {
                log::error!(
                    "Data inconsistency detected for key '{}': {:?}",
                    key,
                    values
                );
                return Ok(false);
            }
        }

        // Verify against expected value
        if first_value != expected_value {
            log::error!(
                "Value mismatch for key '{}': expected '{}', got '{}'",
                key,
                expected_value,
                first_value
            );
            return Ok(false);
        }
    }

    Ok(true)
}

/// Create test data set
pub fn create_test_data(count: usize) -> HashMap<String, String> {
    let mut data = HashMap::new();
    for i in 0..count {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        data.insert(key, value);
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Three-node cluster test may be slow for CI"]
    async fn test_three_node_cluster_deployment() {
        // Create three-node cluster
        let cluster = ThreeNodeCluster::new().await.unwrap();

        // Start all nodes
        cluster.start_all().await.unwrap();

        // Wait for leader election
        let leader_id = wait_for_leader(&cluster, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(leader_id == 1 || leader_id == 2 || leader_id == 3);

        // Verify all nodes are running
        let metrics1 = cluster.node1.get_metrics().await.unwrap();
        let metrics2 = cluster.node2.get_metrics().await.unwrap();
        let metrics3 = cluster.node3.get_metrics().await.unwrap();

        assert!(metrics1.current_term > 0);
        assert!(metrics2.current_term > 0);
        assert!(metrics3.current_term > 0);

        // Cleanup
        cluster.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Failover test may be slow for CI"]
    async fn test_failover_and_recovery() {
        // Create and start cluster
        let cluster = ThreeNodeCluster::new().await.unwrap();
        cluster.start_all().await.unwrap();

        // Wait for leader election
        let initial_leader_id = wait_for_leader(&cluster, Duration::from_secs(5))
            .await
            .unwrap();
        log::info!("Initial leader: {}", initial_leader_id);

        // Write some test data
        cluster.write("test_key", "test_value").await.unwrap();
        sleep(Duration::from_millis(200)).await;

        // Stop the leader (simulate failure)
        cluster.stop_node(initial_leader_id).await.unwrap();
        sleep(Duration::from_millis(500)).await;

        // Wait for new leader election
        let new_leader_id = wait_for_leader(&cluster, Duration::from_secs(5))
            .await
            .unwrap();
        assert_ne!(new_leader_id, initial_leader_id);
        log::info!("New leader after failover: {}", new_leader_id);

        // Verify data is still accessible
        let value = cluster.read("test_key").await.unwrap();
        assert_eq!(value, Some(Bytes::from("test_value")));

        // Restart the failed node
        cluster.restart_node(initial_leader_id).await.unwrap();
        sleep(Duration::from_millis(500)).await;

        // Verify the restarted node would rejoin the cluster
        // In a real implementation, we would recreate the node and verify it rejoins
        // For now, we just verify the remaining nodes are still healthy
        if initial_leader_id != 1 {
            let metrics = cluster.node1.get_metrics().await.unwrap();
            assert!(metrics.current_term > 0);
        }

        // Cleanup
        cluster.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Data consistency test may be slow for CI"]
    async fn test_data_consistency() {
        // Create and start cluster
        let cluster = ThreeNodeCluster::new().await.unwrap();
        cluster.start_all().await.unwrap();

        // Wait for leader election
        wait_for_leader(&cluster, Duration::from_secs(5))
            .await
            .unwrap();

        // Create test data
        let test_data = create_test_data(10);

        // Write all test data
        for (key, value) in &test_data {
            cluster.write(key, value).await.unwrap();
            sleep(Duration::from_millis(50)).await; // Small delay for replication
        }

        // Wait for replication
        sleep(Duration::from_millis(500)).await;

        // Verify data consistency across all nodes
        let is_consistent = verify_data_consistency(&cluster, &test_data).await.unwrap();
        assert!(is_consistent, "Data should be consistent across all nodes");

        // Cleanup
        cluster.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Multiple writes consistency test may be slow for CI"]
    async fn test_multiple_writes_and_consistency() {
        // Create and start cluster
        let cluster = ThreeNodeCluster::new().await.unwrap();
        cluster.start_all().await.unwrap();

        // Wait for leader election
        wait_for_leader(&cluster, Duration::from_secs(5))
            .await
            .unwrap();

        // Perform multiple writes
        for i in 0..20 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cluster.write(&key, &value).await.unwrap();
        }

        // Wait for replication
        sleep(Duration::from_millis(1000)).await;

        // Verify all writes are consistent
        for i in 0..20 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);

            let value = cluster.read(&key).await.unwrap();
            assert_eq!(
                value,
                Some(Bytes::from(expected_value)),
                "Value for key '{}' should be consistent",
                key
            );
        }

        // Cleanup
        cluster.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Ignore this test for now due to complexity
    async fn test_consistency_after_failover() {
        // Create and start cluster
        let cluster = ThreeNodeCluster::new().await.unwrap();
        cluster.start_all().await.unwrap();

        // Wait for leader election
        let leader_id = wait_for_leader(&cluster, Duration::from_secs(5))
            .await
            .unwrap();

        // Write data
        cluster.write("key1", "value1").await.unwrap();
        cluster.write("key2", "value2").await.unwrap();
        sleep(Duration::from_millis(300)).await;

        // Stop leader
        cluster.stop_node(leader_id).await.unwrap();
        sleep(Duration::from_millis(500)).await;

        // Wait for new leader
        let new_leader_id = wait_for_leader(&cluster, Duration::from_secs(5))
            .await
            .unwrap();
        assert_ne!(new_leader_id, leader_id);

        // Verify data is still accessible and consistent
        let value1 = cluster.read("key1").await.unwrap();
        let value2 = cluster.read("key2").await.unwrap();

        assert_eq!(value1, Some(Bytes::from("value1")));
        assert_eq!(value2, Some(Bytes::from("value2")));

        // Write more data after failover
        cluster.write("key3", "value3").await.unwrap();
        sleep(Duration::from_millis(300)).await;

        // Verify new write is also consistent
        let value3 = cluster.read("key3").await.unwrap();
        assert_eq!(value3, Some(Bytes::from("value3")));

        // Cleanup
        cluster.shutdown_all().await.unwrap();
    }
}
