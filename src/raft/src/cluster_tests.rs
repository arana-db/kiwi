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
    let nodes = vec![
        (cluster.node1.node_id(), Arc::clone(&cluster.node1)),
        (cluster.node2.node_id(), Arc::clone(&cluster.node2)),
        (cluster.node3.node_id(), Arc::clone(&cluster.node3)),
    ];

    for (key, expected_value) in test_data {
        let mut observed_values = Vec::new();

        for (node_id, node) in &nodes {
            match read_key_from_node(node, key).await? {
                Some(value) => observed_values.push((*node_id, value)),
                None => {
                    log::error!("Node {} missing key '{}'", node_id, key);
                    return Ok(false);
                }
            }
        }

        if observed_values.is_empty() {
            log::error!("No values collected for key '{}'", key);
            return Ok(false);
        }

        let first_value = observed_values[0].1.clone();

        for (node_id, value) in &observed_values {
            if value != &first_value {
                let printable: Vec<String> = observed_values
                    .iter()
                    .map(|(id, val)| format!("node {} -> {}", id, String::from_utf8_lossy(val)))
                    .collect();
                log::error!(
                    "Data inconsistency detected for key '{}': node {} differs, values: {:?}",
                    key,
                    node_id,
                    printable
                );
                return Ok(false);
            }
        }

        if first_value.as_ref() != expected_value.as_bytes() {
            let printable: Vec<String> = observed_values
                .iter()
                .map(|(id, val)| format!("node {} -> {}", id, String::from_utf8_lossy(val)))
                .collect();
            log::error!(
                "Value mismatch for key '{}': expected '{}', observed {:?}",
                key,
                expected_value,
                printable
            );
            return Ok(false);
        }
    }

    Ok(true)
}

async fn read_key_from_node(node: &Arc<RaftNode>, key: &str) -> RaftResult<Option<Bytes>> {
    let state_machine = node.state_machine();
    let key_bytes = Bytes::copy_from_slice(key.as_bytes());

    let exists_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand {
            command: "EXISTS".to_string(),
            args: vec![key_bytes.clone()],
        },
        consistency_level: ConsistencyLevel::Eventual,
    };

    let exists_response = state_machine.apply_redis_command(&exists_request).await?;
    let exists_bytes = exists_response.result.map_err(|err| {
        RaftError::state_machine_with_context(
            err,
            format!("EXISTS '{}' on node {}", key, node.node_id()),
        )
    })?;

    let exists = std::str::from_utf8(&exists_bytes)
        .map_err(|err| {
            RaftError::state_machine_with_context(
                format!("Failed to parse EXISTS response: {}", err),
                format!("node {}", node.node_id()),
            )
        })?
        .trim()
        .parse::<u64>()
        .map_err(|err| {
            RaftError::state_machine_with_context(
                format!("Failed to parse EXISTS count: {}", err),
                format!("node {}", node.node_id()),
            )
        })?
        > 0;

    if !exists {
        return Ok(None);
    }

    let get_request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand {
            command: "GET".to_string(),
            args: vec![key_bytes],
        },
        consistency_level: ConsistencyLevel::Eventual,
    };

    let get_response = state_machine.apply_redis_command(&get_request).await?;
    let value = get_response.result.map_err(|err| {
        RaftError::state_machine_with_context(
            err,
            format!("GET '{}' on node {}", key, node.node_id()),
        )
    })?;

    Ok(Some(value))
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
    use env_logger::Env;

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

    fn init_test_logging() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info"))
            .is_test(true)
            .try_init();
    }

    #[tokio::test]
    async fn test_consistency_after_failover() -> RaftResult<()> {
        init_test_logging();

        // 1. Create and start three-node cluster
        let cluster = ThreeNodeCluster::new().await?;
        cluster.start_all().await?;

        // 2. Wait for initial leader election
        let initial_leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await?;
        log::info!("Initial leader elected: node {}", initial_leader_id);

        // 3. Write test data to the cluster
        let test_key1 = "failover_test_key1";
        let test_value1 = "consistent_value1";
        let test_key2 = "failover_test_key2";
        let test_value2 = "consistent_value2";

        cluster.write(test_key1, test_value1).await?;
        cluster.write(test_key2, test_value2).await?;
        
        // Wait for replication to complete
        sleep(Duration::from_millis(500)).await;

        // 4. Verify all nodes have the data before failover
        log::info!("Verifying initial data consistency across all nodes");
        let value1_before = cluster.read(test_key1).await?;
        let value2_before = cluster.read(test_key2).await?;
        
        assert_eq!(
            value1_before,
            Some(Bytes::from(test_value1)),
            "Initial data should be consistent before failover"
        );
        assert_eq!(
            value2_before,
            Some(Bytes::from(test_value2)),
            "Initial data should be consistent before failover"
        );

        // 5. Simulate leader failure
        log::info!("Stopping leader node {}", initial_leader_id);
        cluster.stop_node(initial_leader_id).await?;
        
        // Wait for failure detection
        sleep(Duration::from_secs(1)).await;

        // 6. Wait for new leader election
        let new_leader_id = wait_for_leader(&cluster, Duration::from_secs(10)).await?;
        assert_ne!(
            new_leader_id, initial_leader_id,
            "New leader should be different from failed leader"
        );
        log::info!("New leader elected after failover: node {}", new_leader_id);

        // 7. Verify data consistency after failover on remaining nodes
        log::info!("Verifying data consistency after failover");
        let value1_after = cluster.read(test_key1).await?;
        let value2_after = cluster.read(test_key2).await?;

        assert_eq!(
            value1_after,
            Some(Bytes::from(test_value1)),
            "Data should remain consistent after failover for key '{}'",
            test_key1
        );
        assert_eq!(
            value2_after,
            Some(Bytes::from(test_value2)),
            "Data should remain consistent after failover for key '{}'",
            test_key2
        );

        // 8. Write new data after failover to verify cluster is still operational
        let test_key3 = "post_failover_key";
        let test_value3 = "post_failover_value";
        
        log::info!("Writing new data after failover");
        cluster.write(test_key3, test_value3).await?;
        
        // Wait for replication
        sleep(Duration::from_millis(500)).await;

        // 9. Verify new write is consistent across remaining nodes
        let value3 = cluster.read(test_key3).await?;
        assert_eq!(
            value3,
            Some(Bytes::from(test_value3)),
            "New data written after failover should be consistent"
        );

        // 10. Verify all data is still consistent
        log::info!("Final consistency check");
        let mut test_data = HashMap::new();
        test_data.insert(test_key1.to_string(), test_value1.to_string());
        test_data.insert(test_key2.to_string(), test_value2.to_string());
        test_data.insert(test_key3.to_string(), test_value3.to_string());

        let is_consistent = verify_data_consistency(&cluster, &test_data).await?;
        assert!(
            is_consistent,
            "All data should be consistent across remaining nodes after failover"
        );

        log::info!("Consistency after failover test completed successfully");

        // Cleanup
        cluster.shutdown_all().await?;
        Ok(())
    }
}
