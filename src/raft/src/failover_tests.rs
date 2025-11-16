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

//! Failover and leader election tests
//!
//! This module tests:
//! - Leader election after leader failure (Requirement 8.1)
//! - Request redirection to leader (Requirement 8.3)
//! - Data consistency during failover
//! - Multiple leader failures

use std::time::Duration;
use tokio::time::sleep;

use crate::cluster_tests::{ThreeNodeCluster, wait_for_leader};
use crate::error::{RaftError, RaftResult};
use crate::node::RaftNodeInterface;
use crate::types::{ClientRequest, ConsistencyLevel, RedisCommand, RequestId};
use bytes::Bytes;

/// Test basic leader election after leader failure
///
/// Requirements:
/// - 8.1.1: Leader failure SHALL trigger new election
/// - 8.1.2: New leader SHALL be elected with majority agreement
#[tokio::test]
#[ignore = "Leader election test may be slow for CI"]
async fn test_leader_election_after_failure() -> RaftResult<()> {
    // Create and start three-node cluster
    let cluster = ThreeNodeCluster::new().await?;
    cluster.start_all().await?;

    // Wait for initial leader election
    let initial_leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await?;
    log::info!("Initial leader elected: node {}", initial_leader_id);

    // Verify initial leader is actually the leader
    let initial_leader = match initial_leader_id {
        1 => &cluster.node1,
        2 => &cluster.node2,
        3 => &cluster.node3,
        _ => panic!("Invalid leader ID"),
    };
    assert!(
        initial_leader.is_leader().await,
        "Initial leader should be in leader state"
    );

    // Stop the leader to trigger election
    log::info!("Stopping leader node {}", initial_leader_id);
    cluster.stop_node(initial_leader_id).await?;

    // Wait for failure detection and new election
    sleep(Duration::from_secs(2)).await;

    // Wait for new leader election
    let new_leader_id = wait_for_leader(&cluster, Duration::from_secs(10)).await?;
    log::info!("New leader elected: node {}", new_leader_id);

    // Verify new leader is different from failed leader
    assert_ne!(
        new_leader_id, initial_leader_id,
        "New leader should be different from failed leader"
    );

    // Verify new leader is actually in leader state
    let new_leader = match new_leader_id {
        1 => &cluster.node1,
        2 => &cluster.node2,
        3 => &cluster.node3,
        _ => panic!("Invalid leader ID"),
    };
    assert!(
        new_leader.is_leader().await,
        "New leader should be in leader state"
    );

    // Verify remaining nodes recognize the new leader
    let remaining_nodes: Vec<_> = vec![&cluster.node1, &cluster.node2, &cluster.node3]
        .into_iter()
        .filter(|n| n.node_id() != initial_leader_id)
        .collect();

    for node in remaining_nodes {
        let leader_id = node.get_leader_id().await;
        assert_eq!(
            leader_id,
            Some(new_leader_id),
            "Node {} should recognize node {} as leader",
            node.node_id(),
            new_leader_id
        );
    }

    // Cleanup
    cluster.shutdown_all().await?;
    Ok(())
}

/// Test leader election with multiple failures
///
/// Requirements:
/// - 8.1.1: Leader failure SHALL trigger new election
/// - 8.1.2: New leader SHALL be elected with majority agreement
#[tokio::test]
#[ignore = "Multiple leader failures test may be slow for CI"]
async fn test_multiple_leader_failures() -> RaftResult<()> {
    // Create and start three-node cluster
    let cluster = ThreeNodeCluster::new().await?;
    cluster.start_all().await?;

    // First leader election
    let first_leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await?;
    log::info!("First leader elected: node {}", first_leader_id);

    // Stop first leader
    log::info!("Stopping first leader node {}", first_leader_id);
    cluster.stop_node(first_leader_id).await?;
    sleep(Duration::from_secs(2)).await;

    // Second leader election
    let second_leader_id = wait_for_leader(&cluster, Duration::from_secs(10)).await?;
    log::info!("Second leader elected: node {}", second_leader_id);
    assert_ne!(second_leader_id, first_leader_id);

    // Stop second leader
    log::info!("Stopping second leader node {}", second_leader_id);
    cluster.stop_node(second_leader_id).await?;
    sleep(Duration::from_secs(2)).await;

    // Third leader election (only one node left, should become leader)
    let third_leader_id = wait_for_leader(&cluster, Duration::from_secs(10)).await?;
    log::info!("Third leader elected: node {}", third_leader_id);
    assert_ne!(third_leader_id, first_leader_id);
    assert_ne!(third_leader_id, second_leader_id);

    // Verify the remaining node is the leader
    let remaining_node = match third_leader_id {
        1 => &cluster.node1,
        2 => &cluster.node2,
        3 => &cluster.node3,
        _ => panic!("Invalid leader ID"),
    };
    assert!(remaining_node.is_leader().await);

    // Cleanup
    cluster.shutdown_all().await?;
    Ok(())
}

/// Test that data remains consistent after leader election
///
/// Requirements:
/// - 8.1.4: Data SHALL remain consistent after failover
#[tokio::test]
#[ignore = "Data consistency after election test may be slow for CI"]
async fn test_data_consistency_after_election() -> RaftResult<()> {
    // Create and start three-node cluster
    let cluster = ThreeNodeCluster::new().await?;
    cluster.start_all().await?;

    // Wait for initial leader election
    let initial_leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await?;
    log::info!("Initial leader elected: node {}", initial_leader_id);

    // Write test data before failover
    let test_key = "election_test_key";
    let test_value = "election_test_value";
    cluster.write(test_key, test_value).await?;
    sleep(Duration::from_millis(500)).await;

    // Verify data is written
    let value_before = cluster.read(test_key).await?;
    assert_eq!(value_before, Some(Bytes::from(test_value)));

    // Stop the leader
    log::info!("Stopping leader node {}", initial_leader_id);
    cluster.stop_node(initial_leader_id).await?;
    sleep(Duration::from_secs(2)).await;

    // Wait for new leader election
    let new_leader_id = wait_for_leader(&cluster, Duration::from_secs(10)).await?;
    log::info!("New leader elected: node {}", new_leader_id);

    // Verify data is still accessible after election
    let value_after = cluster.read(test_key).await?;
    assert_eq!(
        value_after,
        Some(Bytes::from(test_value)),
        "Data should remain consistent after leader election"
    );

    // Write new data with new leader
    let new_key = "post_election_key";
    let new_value = "post_election_value";
    cluster.write(new_key, new_value).await?;
    sleep(Duration::from_millis(500)).await;

    // Verify new data is accessible
    let new_data = cluster.read(new_key).await?;
    assert_eq!(new_data, Some(Bytes::from(new_value)));

    // Cleanup
    cluster.shutdown_all().await?;
    Ok(())
}

/// Test leader election timing and term increments
///
/// Requirements:
/// - 8.1.2: New leader SHALL be elected with majority agreement
#[tokio::test]
#[ignore = "Leader election timing test may be slow for CI"]
async fn test_leader_election_timing() -> RaftResult<()> {
    // Create and start three-node cluster
    let cluster = ThreeNodeCluster::new().await?;
    cluster.start_all().await?;

    // Wait for initial leader election
    let initial_leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await?;
    log::info!("Initial leader elected: node {}", initial_leader_id);

    // Get initial term
    let initial_leader = match initial_leader_id {
        1 => &cluster.node1,
        2 => &cluster.node2,
        3 => &cluster.node3,
        _ => panic!("Invalid leader ID"),
    };
    let initial_term = initial_leader.get_current_term().await;
    log::info!("Initial term: {}", initial_term);

    // Stop the leader
    cluster.stop_node(initial_leader_id).await?;
    sleep(Duration::from_secs(2)).await;

    // Wait for new leader election
    let new_leader_id = wait_for_leader(&cluster, Duration::from_secs(10)).await?;
    log::info!("New leader elected: node {}", new_leader_id);

    // Get new term
    let new_leader = match new_leader_id {
        1 => &cluster.node1,
        2 => &cluster.node2,
        3 => &cluster.node3,
        _ => panic!("Invalid leader ID"),
    };
    let new_term = new_leader.get_current_term().await;
    log::info!("New term: {}", new_term);

    // Verify term has incremented
    assert!(
        new_term > initial_term,
        "Term should increment after new election (initial: {}, new: {})",
        initial_term,
        new_term
    );

    // Cleanup
    cluster.shutdown_all().await?;
    Ok(())
}

/// Test request redirection from follower to leader
///
/// Requirements:
/// - 8.3.1: Non-leader nodes SHALL redirect write requests to leader
/// - 8.3.3: Client requests SHALL automatically redirect to new leader
#[tokio::test]
#[ignore = "Request redirection test may be slow for CI"]
async fn test_request_redirection_to_leader() -> RaftResult<()> {
    // Create and start three-node cluster
    let cluster = ThreeNodeCluster::new().await?;
    cluster.start_all().await?;

    // Wait for leader election
    let leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await?;
    log::info!("Leader elected: node {}", leader_id);

    // Find a follower node
    let follower = if leader_id != 1 {
        &cluster.node1
    } else {
        &cluster.node2
    };
    let follower_id = follower.node_id();
    log::info!("Testing with follower: node {}", follower_id);

    // Verify follower is not the leader
    assert!(
        !follower.is_leader().await,
        "Node {} should not be leader",
        follower_id
    );

    // Try to write through follower (should get NotLeader error)
    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand {
            command: "SET".to_string(),
            args: vec![
                Bytes::from("redirect_test_key"),
                Bytes::from("redirect_test_value"),
            ],
        },
        consistency_level: ConsistencyLevel::Linearizable,
    };

    let result = follower.propose(request).await;

    // Should get NotLeader error with leader information
    match result {
        Err(RaftError::NotLeader {
            leader_id: Some(returned_leader_id),
            ..
        }) => {
            assert_eq!(
                returned_leader_id, leader_id,
                "Follower should return correct leader ID"
            );
            log::info!(
                "Follower correctly returned leader ID: {}",
                returned_leader_id
            );
        }
        Err(RaftError::NotLeader {
            leader_id: None, ..
        }) => {
            log::warn!("Follower returned NotLeader but no leader ID");
        }
        Ok(_) => {
            panic!("Write to follower should fail with NotLeader error");
        }
        Err(e) => {
            panic!("Unexpected error: {}", e);
        }
    }

    // Cleanup
    cluster.shutdown_all().await?;
    Ok(())
}

/// Test request redirection after leader failure
///
/// Requirements:
/// - 8.3.3: Client requests SHALL automatically redirect to new leader
/// - 8.3.4: Data SHALL remain consistent after failover
#[tokio::test]
#[ignore = "Request redirection after failover test may be slow for CI"]
async fn test_request_redirection_after_failover() -> RaftResult<()> {
    // Create and start three-node cluster
    let cluster = ThreeNodeCluster::new().await?;
    cluster.start_all().await?;

    // Wait for initial leader election
    let initial_leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await?;
    log::info!("Initial leader elected: node {}", initial_leader_id);

    // Write initial data
    cluster.write("failover_key", "failover_value").await?;
    sleep(Duration::from_millis(500)).await;

    // Stop the leader
    log::info!("Stopping leader node {}", initial_leader_id);
    cluster.stop_node(initial_leader_id).await?;
    sleep(Duration::from_secs(2)).await;

    // Wait for new leader election
    let new_leader_id = wait_for_leader(&cluster, Duration::from_secs(10)).await?;
    log::info!("New leader elected: node {}", new_leader_id);

    // Find a follower (not the new leader)
    let follower = if new_leader_id != 1 && initial_leader_id != 1 {
        &cluster.node1
    } else if new_leader_id != 2 && initial_leader_id != 2 {
        &cluster.node2
    } else {
        &cluster.node3
    };
    let follower_id = follower.node_id();
    log::info!("Testing with follower: node {}", follower_id);

    // Try to write through follower (should get NotLeader error with new leader)
    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand {
            command: "SET".to_string(),
            args: vec![
                Bytes::from("post_failover_key"),
                Bytes::from("post_failover_value"),
            ],
        },
        consistency_level: ConsistencyLevel::Linearizable,
    };

    let result = follower.propose(request).await;

    // Should get NotLeader error with new leader information
    match result {
        Err(RaftError::NotLeader {
            leader_id: Some(returned_leader_id),
            ..
        }) => {
            assert_eq!(
                returned_leader_id, new_leader_id,
                "Follower should return new leader ID after failover"
            );
            log::info!(
                "Follower correctly returned new leader ID: {}",
                returned_leader_id
            );
        }
        Err(RaftError::NotLeader {
            leader_id: None, ..
        }) => {
            log::warn!("Follower returned NotLeader but no leader ID");
        }
        Ok(_) => {
            panic!("Write to follower should fail with NotLeader error");
        }
        Err(e) => {
            panic!("Unexpected error: {}", e);
        }
    }

    // Verify old data is still accessible
    let old_value = cluster.read("failover_key").await?;
    assert_eq!(old_value, Some(Bytes::from("failover_value")));

    // Cleanup
    cluster.shutdown_all().await?;
    Ok(())
}

/// Test that followers correctly track leader changes
///
/// Requirements:
/// - 8.3.3: Client requests SHALL automatically redirect to new leader
#[tokio::test]
#[ignore = "Leader tracking test may be slow for CI"]
async fn test_follower_tracks_leader_changes() -> RaftResult<()> {
    // Create and start three-node cluster
    let cluster = ThreeNodeCluster::new().await?;
    cluster.start_all().await?;

    // Wait for initial leader election
    let initial_leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await?;
    log::info!("Initial leader elected: node {}", initial_leader_id);

    // Verify all nodes recognize the leader
    for node in [&cluster.node1, &cluster.node2, &cluster.node3] {
        let leader_id = node.get_leader_id().await;
        assert_eq!(
            leader_id,
            Some(initial_leader_id),
            "Node {} should recognize node {} as leader",
            node.node_id(),
            initial_leader_id
        );
    }

    // Stop the leader
    cluster.stop_node(initial_leader_id).await?;
    sleep(Duration::from_secs(2)).await;

    // Wait for new leader election
    let new_leader_id = wait_for_leader(&cluster, Duration::from_secs(10)).await?;
    log::info!("New leader elected: node {}", new_leader_id);

    // Verify remaining nodes recognize the new leader
    let remaining_nodes: Vec<_> = vec![&cluster.node1, &cluster.node2, &cluster.node3]
        .into_iter()
        .filter(|n| n.node_id() != initial_leader_id)
        .collect();

    for node in remaining_nodes {
        let leader_id = node.get_leader_id().await;
        assert_eq!(
            leader_id,
            Some(new_leader_id),
            "Node {} should recognize node {} as new leader",
            node.node_id(),
            new_leader_id
        );
    }

    // Cleanup
    cluster.shutdown_all().await?;
    Ok(())
}
