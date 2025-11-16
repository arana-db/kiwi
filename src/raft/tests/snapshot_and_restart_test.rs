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

use bytes::Bytes;
use raft::{
    RaftNode, RaftNodeInterface,
    types::{ClientRequest, ClusterConfig, ConsistencyLevel, RedisCommand},
};
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn test_restart_persists_data() {
    let mut members = std::collections::BTreeSet::new();
    members.insert("1:127.0.0.1:7379".to_string());

    let workspace_root = std::env::var("CARGO_MANIFEST_DIR")
        .map(|p| {
            std::path::PathBuf::from(p)
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf()
        })
        .unwrap_or_else(|_| std::env::current_dir().unwrap());
    let base_dir = workspace_root.join("target/test_data/it_restart");

    let cfg = ClusterConfig {
        enabled: true,
        node_id: 1,
        cluster_members: members.clone(),
        data_dir: base_dir.to_string_lossy().to_string(),
        heartbeat_interval_ms: 200,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 800,
        snapshot_threshold: 1000,
        max_payload_entries: 64,
    };
    {
        let node = Arc::new(RaftNode::new(cfg.clone()).await.unwrap());
        node.start(true).await.unwrap();

        // Write data
        let set_req = ClientRequest {
            id: raft::types::RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![Bytes::from("restart_key"), Bytes::from("restart_val")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };
        let _ = node.propose(set_req).await.unwrap();
    }

    // Restart node using same data_dir
    {
        let node = Arc::new(RaftNode::new(cfg.clone()).await.unwrap());
        node.start(false).await.unwrap();

        let get_req = ClientRequest {
            id: raft::types::RequestId::new(),
            command: RedisCommand::new("GET".to_string(), vec![Bytes::from("restart_key")]),
            consistency_level: ConsistencyLevel::Linearizable,
        };
        let resp = node.propose(get_req).await.unwrap();
        assert_eq!(resp.result.unwrap(), Bytes::from("restart_val"));
    }
}
