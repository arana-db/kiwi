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
async fn test_raft_writes_persist_in_redis_engine() {
    // Clean up any existing data directory
    let workspace_root = std::env::var("CARGO_MANIFEST_DIR")
        .map(|p| std::path::PathBuf::from(p).parent().unwrap().parent().unwrap().to_path_buf())
        .unwrap_or_else(|_| std::env::current_dir().unwrap());
    let test_dir = workspace_root.join("target/test_data/it_redis_engine");
    let _ = std::fs::remove_dir_all(&test_dir);

    let mut members = std::collections::BTreeSet::new();
    members.insert("1:127.0.0.1:7379".to_string());

    let cfg = ClusterConfig {
        enabled: true,
        node_id: 1,
        cluster_members: members,
        data_dir: test_dir.to_string_lossy().to_string(),
        heartbeat_interval_ms: 200,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 800,
        snapshot_threshold: 1000,
        max_payload_entries: 64,
    };

    let node = Arc::new(RaftNode::new(cfg.clone()).await.unwrap());
    node.start(true).await.unwrap();

    // Wait for node to become leader
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Propose a SET via Raft
    let set_req = ClientRequest {
        id: raft::types::RequestId::new(),
        command: RedisCommand::new("SET".to_string(), vec![Bytes::from("it_key"), Bytes::from("it_val")]),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    let _ = node.propose(set_req).await.unwrap();

    // Read back via Raft GET
    let get_req = ClientRequest {
        id: raft::types::RequestId::new(),
        command: RedisCommand::new("GET".to_string(), vec![Bytes::from("it_key")]),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    let resp = node.propose(get_req).await.unwrap();
    assert_eq!(resp.result.unwrap(), Bytes::from("it_val"));
}