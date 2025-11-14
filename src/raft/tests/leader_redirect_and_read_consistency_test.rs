use std::sync::Arc;
use bytes::Bytes;
use raft::{RaftNode, RaftNodeInterface, types::{ClusterConfig, ClientRequest, ConsistencyLevel, RedisCommand}};

#[tokio::test]
async fn test_non_leader_write_redirects_and_linearizable_read_succeeds() {
    let mut members = std::collections::BTreeSet::new();
    members.insert("1:127.0.0.1:7379".to_string());
    members.insert("2:127.0.0.1:8380".to_string());

    // Leader node
    let cfg1 = ClusterConfig {
        enabled: true,
        node_id: 1,
        cluster_members: members.clone(),
        data_dir: "./target/it_cluster_node1".to_string(),
        heartbeat_interval_ms: 200,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 800,
        snapshot_threshold: 1000,
        max_payload_entries: 64,
    };
    let node1 = Arc::new(RaftNode::new(cfg1).await.unwrap());
    node1.start(true).await.unwrap();

    // Follower node (join later)
    let cfg2 = ClusterConfig {
        enabled: true,
        node_id: 2,
        cluster_members: members.clone(),
        data_dir: "./target/it_cluster_node2".to_string(),
        heartbeat_interval_ms: 200,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 800,
        snapshot_threshold: 1000,
        max_payload_entries: 64,
    };
    let node2 = Arc::new(RaftNode::new(cfg2).await.unwrap());
    node2.start(false).await.unwrap();

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
    let err = node2.propose(set_req_follower).await.err().unwrap();
    let msg = format!("{}", err);
    assert!(msg.contains("NotLeader") || msg.contains("not leader"));

    // Linearizable read from leader should succeed
    let get_req = ClientRequest {
        id: raft::types::RequestId::new(),
        command: RedisCommand::new("GET".to_string(), vec![Bytes::from("redir_key")]),
        consistency_level: ConsistencyLevel::Linearizable,
    };
    let resp = node1.propose(get_req).await.unwrap();
    assert_eq!(resp.result.unwrap(), Bytes::from("redir_val"));
}
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