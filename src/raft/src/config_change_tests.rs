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

//! Configuration change tests
//!
//! Tests for adding and removing nodes from the Raft cluster

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

use crate::error::RaftResult;
use crate::node::{RaftNode, RaftNodeInterface};
use crate::types::{ClusterConfig, NodeId};

/// Helper to create a test node
async fn create_test_node(node_id: NodeId, cluster_members: Vec<String>) -> RaftResult<(Arc<RaftNode>, TempDir)> {
    let temp_dir = TempDir::new()
        .map_err(|e| crate::error::RaftError::state_machine(format!("Failed to create temp dir: {}", e)))?;

    let config = ClusterConfig {
        node_id,
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        cluster_members: cluster_members.into_iter().collect(),
        ..Default::default()
    };

    let node = Arc::new(RaftNode::new(config).await?);
    Ok((node, temp_dir))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Config change tests require network setup"]
    async fn test_add_node_to_single_node_cluster() -> RaftResult<()> {
        timeout(Duration::from_secs(10), async {
            // Create initial single-node cluster
            let (node1, _temp1) = create_test_node(
                1,
                vec!["1:127.0.0.1:8001".to_string()],
            ).await?;

            // Start node1 as initial cluster
            node1.start(true).await?;
            // Wait for node1 to become leader
            node1.wait_for_election(Duration::from_secs(5)).await?;

            // Verify node1 is leader
            assert!(node1.is_leader().await, "Node 1 should be leader");

            // Create node2
            let (node2, _temp2) = create_test_node(
                2,
                vec![
                    "1:127.0.0.1:8001".to_string(),
                    "2:127.0.0.1:8002".to_string(),
                ],
            ).await?;

            // Start node2 (not initializing cluster)
            node2.start(false).await?;
            sleep(Duration::from_millis(200)).await;

            // Add node2 to cluster using add_node_safely
            node1.add_node_safely(2, "127.0.0.1:8002".to_string()).await?;
            sleep(Duration::from_millis(500)).await;

            // Verify membership
            let membership = node1.get_membership().await?;
            assert_eq!(membership.len(), 2, "Cluster should have 2 members");
            assert!(membership.contains(&1), "Cluster should contain node 1");
            assert!(membership.contains(&2), "Cluster should contain node 2");

            // Cleanup
            let _ = node1.shutdown().await;
            let _ = node2.shutdown().await;
            Ok(())
        })
        .await
        .map_err(|_| crate::error::RaftError::timeout("Test timed out after 10 seconds"))?
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Config change tests require network setup"]
    async fn test_add_multiple_nodes_sequentially() -> RaftResult<()> {
        timeout(Duration::from_secs(15), async {
            // Create initial single-node cluster
            let (node1, _temp1) = create_test_node(
                1,
                vec!["1:127.0.0.1:8011".to_string()],
            ).await?;

            node1.start(true).await?;
            // Wait for node1 to become leader
            node1.wait_for_election(Duration::from_secs(5)).await?;

            // Create and add node2
            let (node2, _temp2) = create_test_node(
                2,
                vec![
                    "1:127.0.0.1:8011".to_string(),
                    "2:127.0.0.1:8012".to_string(),
                ],
            ).await?;

            node2.start(false).await?;
            sleep(Duration::from_millis(200)).await;

            node1.add_node_safely(2, "127.0.0.1:8012".to_string()).await?;
            sleep(Duration::from_millis(500)).await;

            // Create and add node3
            let (node3, _temp3) = create_test_node(
                3,
                vec![
                    "1:127.0.0.1:8011".to_string(),
                    "2:127.0.0.1:8012".to_string(),
                    "3:127.0.0.1:8013".to_string(),
                ],
            ).await?;

            node3.start(false).await?;
            sleep(Duration::from_millis(200)).await;

            node1.add_node_safely(3, "127.0.0.1:8013".to_string()).await?;
            sleep(Duration::from_millis(500)).await;

            // Verify final membership
            let membership = node1.get_membership().await?;
            assert_eq!(membership.len(), 3, "Cluster should have 3 members");
            assert!(membership.contains(&1), "Cluster should contain node 1");
            assert!(membership.contains(&2), "Cluster should contain node 2");
            assert!(membership.contains(&3), "Cluster should contain node 3");

            // Cleanup
            let _ = node1.shutdown().await;
            let _ = node2.shutdown().await;
            let _ = node3.shutdown().await;
            Ok(())
        })
        .await
        .map_err(|_| crate::error::RaftError::timeout("Test timed out after 15 seconds"))?
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Config change tests require network setup"]
    async fn test_add_node_already_member() -> RaftResult<()> {
        timeout(Duration::from_secs(10), async {
            // Create initial cluster with 2 nodes
            let (node1, _temp1) = create_test_node(
                1,
                vec![
                    "1:127.0.0.1:8021".to_string(),
                    "2:127.0.0.1:8022".to_string(),
                ],
            ).await?;

            let (node2, _temp2) = create_test_node(
                2,
                vec![
                    "1:127.0.0.1:8021".to_string(),
                    "2:127.0.0.1:8022".to_string(),
                ],
            ).await?;

            node1.start(true).await?;
            // Wait for node1 to become leader
            node1.wait_for_election(Duration::from_secs(5)).await?;

            node2.start(false).await?;
            sleep(Duration::from_millis(200)).await;

            node1.add_node_safely(2, "127.0.0.1:8022".to_string()).await?;
            sleep(Duration::from_millis(500)).await;

            // Try to add node2 again (should succeed without error)
            let result = node1.add_node_safely(2, "127.0.0.1:8022".to_string()).await;
            assert!(result.is_ok(), "Adding existing member should succeed gracefully");

            // Verify membership unchanged
            let membership = node1.get_membership().await?;
            assert_eq!(membership.len(), 2, "Cluster should still have 2 members");

            // Cleanup
            let _ = node1.shutdown().await;
            let _ = node2.shutdown().await;
            Ok(())
        })
        .await
        .map_err(|_| crate::error::RaftError::timeout("Test timed out after 10 seconds"))?
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Config change tests require network setup"]
    async fn test_remove_node_from_cluster() -> RaftResult<()> {
        timeout(Duration::from_secs(15), async {
            // Create initial 3-node cluster
            let (node1, _temp1) = create_test_node(
                1,
                vec![
                    "1:127.0.0.1:8031".to_string(),
                    "2:127.0.0.1:8032".to_string(),
                    "3:127.0.0.1:8033".to_string(),
                ],
            ).await?;

            let (node2, _temp2) = create_test_node(
                2,
                vec![
                    "1:127.0.0.1:8031".to_string(),
                    "2:127.0.0.1:8032".to_string(),
                    "3:127.0.0.1:8033".to_string(),
                ],
            ).await?;

            let (node3, _temp3) = create_test_node(
                3,
                vec![
                    "1:127.0.0.1:8031".to_string(),
                    "2:127.0.0.1:8032".to_string(),
                    "3:127.0.0.1:8033".to_string(),
                ],
            ).await?;

            // Start all nodes
            node1.start(true).await?;
            // Wait for node1 to become leader
            node1.wait_for_election(Duration::from_secs(5)).await?;

            node2.start(false).await?;
            node3.start(false).await?;
            sleep(Duration::from_millis(200)).await;

            // Add nodes to cluster
            node1.add_node_safely(2, "127.0.0.1:8032".to_string()).await?;
            sleep(Duration::from_millis(300)).await;
            node1.add_node_safely(3, "127.0.0.1:8033".to_string()).await?;
            sleep(Duration::from_millis(500)).await;

            // Verify initial membership
            let membership_before = node1.get_membership().await?;
            assert_eq!(membership_before.len(), 3, "Cluster should have 3 members");

            // Remove node3
            node1.remove_node_safely(3).await?;
            sleep(Duration::from_millis(500)).await;

            // Verify membership after removal
            let membership_after = node1.get_membership().await?;
            assert_eq!(membership_after.len(), 2, "Cluster should have 2 members after removal");
            assert!(membership_after.contains(&1), "Cluster should still contain node 1");
            assert!(membership_after.contains(&2), "Cluster should still contain node 2");
            assert!(!membership_after.contains(&3), "Cluster should not contain node 3");

            // Cleanup
            let _ = node1.shutdown().await;
            let _ = node2.shutdown().await;
            let _ = node3.shutdown().await;
            Ok(())
        })
        .await
        .map_err(|_| crate::error::RaftError::timeout("Test timed out after 15 seconds"))?
    }

    #[tokio::test]
    async fn test_remove_non_member_node() -> RaftResult<()> {
        // Create single-node cluster
        let (node1, _temp1) = create_test_node(
            1,
            vec!["1:127.0.0.1:8041".to_string()],
        ).await?;

        node1.start(true).await?;
        // Wait for node1 to become leader
        node1.wait_for_election(Duration::from_secs(5)).await?;

        // Try to remove non-existent node (should succeed gracefully)
        let result = node1.remove_node_safely(99).await;
        assert!(result.is_ok(), "Removing non-member should succeed gracefully");

        // Verify membership unchanged
        let membership = node1.get_membership().await?;
        assert_eq!(membership.len(), 1, "Cluster should still have 1 member");

        // Cleanup
        let _ = node1.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_cannot_remove_last_node() -> RaftResult<()> {
        // Create single-node cluster
        let (node1, _temp1) = create_test_node(
            1,
            vec!["1:127.0.0.1:8051".to_string()],
        ).await?;

        node1.start(true).await?;
        // Wait for node1 to become leader
        node1.wait_for_election(Duration::from_secs(5)).await?;

        // Try to remove the only node (should fail)
        let result = node1.remove_node_safely(1).await;
        assert!(result.is_err(), "Should not be able to remove the last node");

        // Verify membership unchanged
        let membership = node1.get_membership().await?;
        assert_eq!(membership.len(), 1, "Cluster should still have 1 member");

        // Cleanup
        let _ = node1.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_add_node_requires_leader() -> RaftResult<()> {
        // Create a follower node (not leader)
        let (node1, _temp1) = create_test_node(
            1,
            vec!["1:127.0.0.1:8061".to_string()],
        ).await?;

        // Start without initializing cluster (will be follower/candidate)
        node1.start(false).await?;
        sleep(Duration::from_millis(300)).await;

        // Try to add a node (should fail because not leader)
        let result = node1.add_node_safely(2, "127.0.0.1:8062".to_string()).await;
        assert!(result.is_err(), "Non-leader should not be able to add nodes");

        // Cleanup
        let _ = node1.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_node_requires_leader() -> RaftResult<()> {
        // Create a follower node (not leader)
        let (node1, _temp1) = create_test_node(
            1,
            vec!["1:127.0.0.1:8071".to_string()],
        ).await?;

        // Start without initializing cluster (will be follower/candidate)
        node1.start(false).await?;
        sleep(Duration::from_millis(300)).await;

        // Try to remove a node (should fail because not leader)
        let result = node1.remove_node_safely(2).await;
        assert!(result.is_err(), "Non-leader should not be able to remove nodes");

        // Cleanup
        let _ = node1.shutdown().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Config change tests require network setup"]
    async fn test_membership_change_with_direct_api() -> RaftResult<()> {
        timeout(Duration::from_secs(10), async {
            // Test using the lower-level change_membership API directly
            let (node1, _temp1) = create_test_node(
                1,
                vec![
                    "1:127.0.0.1:8081".to_string(),
                    "2:127.0.0.1:8082".to_string(),
                ],
            ).await?;

            node1.start(true).await?;
            // Wait for node1 to become leader
            node1.wait_for_election(Duration::from_secs(5)).await?;

            // Create node2 and add as learner first
            let (node2, _temp2) = create_test_node(
                2,
                vec![
                    "1:127.0.0.1:8081".to_string(),
                    "2:127.0.0.1:8082".to_string(),
                ],
            ).await?;

            node2.start(false).await?;
            sleep(Duration::from_millis(200)).await;

            // Add node2 as learner
            node1.add_learner(2, "127.0.0.1:8082".to_string()).await?;
            sleep(Duration::from_millis(300)).await;

            // Verify node2 is a learner
            let learners = node1.get_learners().await?;
            assert!(learners.contains(&2), "Node 2 should be a learner");

            // Change membership to include node2 as voter
            let mut new_members = BTreeSet::new();
            new_members.insert(1);
            new_members.insert(2);
            node1.change_membership(new_members).await?;
            sleep(Duration::from_millis(500)).await;

            // Verify node2 is now a voting member
            let membership = node1.get_membership().await?;
            assert!(membership.contains(&2), "Node 2 should be a voting member");
            assert_eq!(membership.len(), 2, "Cluster should have 2 voting members");

            // Cleanup
            let _ = node1.shutdown().await;
            let _ = node2.shutdown().await;
            Ok(())
        })
        .await
        .map_err(|_| crate::error::RaftError::timeout("Test timed out after 10 seconds"))?
    }
}
