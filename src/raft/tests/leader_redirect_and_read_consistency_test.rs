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

use std::sync::Arc;
use bytes::Bytes;
use raft::{RaftNode, RaftNodeInterface, types::{ClusterConfig, ClientRequest, ConsistencyLevel, RedisCommand}};

#[tokio::test]
async fn test_non_leader_write_redirects_and_linearizable_read_succeeds() {
    // Clean up any existing data directories
    let workspace_root = std::env::var("CARGO_MANIFEST_DIR")
        .map(|p| std::path::PathBuf::from(p).parent().unwrap().parent().unwrap().to_path_buf())
        .unwrap_or_else(|_| std::env::current_dir().unwrap());
    let test_dir1 = workspace_root.join("target/test_data/it_cluster_node1");
    let test_dir2 = workspace_root.join("target/test_data/it_cluster_node2");
    let _ = std::fs::remove_dir_all(&test_dir1);
    let _ = std::fs::remove_dir_all(&test_dir2);

    let mut members = std::collections::BTreeSet::new();
    members.insert("1:127.0.0.1:7379".to_string());
    members.insert("2:127.0.0.1:8380".to_string());

    // Leader node
    let cfg1 = ClusterConfig {
        enabled: true,
        node_id: 1,
        cluster_members: members.clone(),
        data_dir: test_dir1.to_string_lossy().to_string(),
        heartbeat_interval_ms: 200,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 800,
        snapshot_threshold: 1000,
        max_payload_entries: 64,
    };
    let node1 = Arc::new(RaftNode::new(cfg1).await.unwrap());
    node1.start(true).await.unwrap();

    // Wait for node1 to become leader
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Follower node (join later)
    let cfg2 = ClusterConfig {
        enabled: true,
        node_id: 2,
        cluster_members: members.clone(),
        data_dir: test_dir2.to_string_lossy().to_string(),
        heartbeat_interval_ms: 200,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 800,
        snapshot_threshold: 1000,
        max_payload_entries: 64,
    };
    let node2 = Arc::new(RaftNode::new(cfg2).await.unwrap());
    node2.start(false).await.unwrap();

    // Wait for cluster to stabilize
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Leader writes
    let set_req = ClientRequest {
        id: raft::types::RequestId::new(),
        command: RedisCommand::new("SET".to_string(), vec![Bytes::from("redir_key"), Bytes::from("redir_val")]),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    let _ = node1.propose(set_req).await.unwrap();

    // Follower attempts write; expect NotLeader error (handled in network path normally)
    let set_req_follower = ClientRequest {
        id: raft::types::RequestId::new(),
        command: RedisCommand::new("SET".to_string(), vec![Bytes::from("redir_key2"), Bytes::from("v")]),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    let resp = node2.propose(set_req_follower).await.unwrap();
    let err_msg = resp.result.err().unwrap();
    assert!(err_msg.contains("NotLeader") || err_msg.contains("not leader") || err_msg.contains("Not leader"));

    // Linearizable read from leader should succeed
    let get_req = ClientRequest {
        id: raft::types::RequestId::new(),
        command: RedisCommand::new("GET".to_string(), vec![Bytes::from("redir_key")]),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    let resp = node1.propose(get_req).await.unwrap();
    assert_eq!(resp.result.unwrap(), Bytes::from("redir_val"));
}