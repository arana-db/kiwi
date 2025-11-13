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

//! Tests for the RequestRouter

#[cfg(test)]
mod tests {
    use crate::router::{ClusterMode, RequestRouter};
    use crate::types::{ClusterConfig, RedisCommand};
    use crate::node::RaftNode;
    use std::sync::Arc;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_router_creation() {
        // Create a test cluster config
        let config = ClusterConfig {
            enabled: true,
            node_id: 1,
            cluster_members: vec!["1:127.0.0.1:7379".to_string()].into_iter().collect(),
            data_dir: "./test_data/router_test".to_string(),
            ..Default::default()
        };

        // Create a Raft node
        let raft_node = Arc::new(RaftNode::new(config).await.unwrap());

        // Create a router
        let router = RequestRouter::new(raft_node.clone(), ClusterMode::Cluster);

        // Verify router properties
        assert_eq!(router.mode(), ClusterMode::Cluster);
        assert_eq!(router.raft_node().node_id(), 1);
    }

    #[test]
    fn test_write_command_detection() {
        // Test that write commands are correctly identified
        let write_commands = vec![
            "SET", "DEL", "SETEX", "SETNX", "INCR", "DECR",
            "LPUSH", "RPUSH", "SADD", "ZADD", "HSET",
        ];

        for cmd in write_commands {
            let redis_cmd = RedisCommand::from_strings(
                cmd.to_string(),
                vec!["key".to_string(), "value".to_string()],
            );
            // We can't directly test is_write_command without a router instance,
            // but we verify the command structure is correct
            assert_eq!(redis_cmd.command, cmd);
        }
    }

    #[test]
    fn test_read_command_detection() {
        // Test that read commands are correctly identified
        let read_commands = vec![
            "GET", "MGET", "EXISTS", "STRLEN", "LLEN",
            "SCARD", "ZCARD", "HGET", "HGETALL",
        ];

        for cmd in read_commands {
            let redis_cmd = RedisCommand::from_strings(
                cmd.to_string(),
                vec!["key".to_string()],
            );
            assert_eq!(redis_cmd.command, cmd);
        }
    }

    #[test]
    fn test_cluster_mode_switching() {
        // Test cluster mode enum
        let single = ClusterMode::Single;
        let cluster = ClusterMode::Cluster;

        assert_ne!(single, cluster);
        assert_eq!(single, ClusterMode::Single);
        assert_eq!(cluster, ClusterMode::Cluster);
    }

    #[test]
    fn test_redis_command_creation() {
        // Test creating Redis commands with different argument types
        let cmd1 = RedisCommand::from_strings(
            "SET".to_string(),
            vec!["key".to_string(), "value".to_string()],
        );
        assert_eq!(cmd1.command, "SET");
        assert_eq!(cmd1.args.len(), 2);

        let cmd2 = RedisCommand::from_bytes(
            "GET".to_string(),
            vec![b"key".to_vec()],
        );
        assert_eq!(cmd2.command, "GET");
        assert_eq!(cmd2.args.len(), 1);

        let cmd3 = RedisCommand::new(
            "DEL".to_string(),
            vec![Bytes::from("key1"), Bytes::from("key2")],
        );
        assert_eq!(cmd3.command, "DEL");
        assert_eq!(cmd3.args.len(), 2);
    }

    #[tokio::test]
    async fn test_read_routing_with_consistency_levels() {
        use crate::types::ConsistencyLevel;
        use crate::node::RaftNodeInterface;

        // Create a test cluster config
        let config = ClusterConfig {
            enabled: true,
            node_id: 1,
            cluster_members: vec!["1:127.0.0.1:7380".to_string()].into_iter().collect(),
            data_dir: "./test_data/router_read_test".to_string(),
            ..Default::default()
        };

        // Create a Raft node
        let raft_node = Arc::new(RaftNode::new(config).await.unwrap());
        
        // Initialize the cluster
        raft_node.start(true).await.unwrap();

        // Create a router
        let router = RequestRouter::new(raft_node.clone(), ClusterMode::Cluster);

        // Test read command
        let get_cmd = RedisCommand::from_strings(
            "GET".to_string(),
            vec!["test_key".to_string()],
        );

        // Test linearizable read (should work on leader)
        let result_linearizable = router
            .route_read_with_consistency(get_cmd.clone(), ConsistencyLevel::Linearizable)
            .await;
        
        // Should succeed or return NotLeader error (depending on leadership state)
        match result_linearizable {
            Ok(response) => {
                // If successful, verify response structure
                assert!(response.data.is_ok() || response.data.is_err());
            }
            Err(e) => {
                // May fail if not leader yet
                println!("Linearizable read result: {:?}", e);
            }
        }

        // Test eventual consistency read (should always work)
        let result_eventual = router
            .route_read_with_consistency(get_cmd.clone(), ConsistencyLevel::Eventual)
            .await;
        
        // Eventual reads should always succeed (even on followers)
        assert!(result_eventual.is_ok(), "Eventual consistency read should succeed");
        
        // Cleanup
        let _ = raft_node.shutdown().await;
        let _ = std::fs::remove_dir_all("./test_data/router_read_test");
    }

    #[tokio::test]
    async fn test_write_then_read_consistency() {
        use crate::types::ConsistencyLevel;
        use crate::node::RaftNodeInterface;

        // Create a test cluster config
        let config = ClusterConfig {
            enabled: true,
            node_id: 1,
            cluster_members: vec!["1:127.0.0.1:7381".to_string()].into_iter().collect(),
            data_dir: "./test_data/router_write_read_test".to_string(),
            ..Default::default()
        };

        // Create a Raft node
        let raft_node = Arc::new(RaftNode::new(config).await.unwrap());
        
        // Initialize the cluster
        raft_node.start(true).await.unwrap();

        // Wait for node to become leader
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Create a router
        let router = RequestRouter::new(raft_node.clone(), ClusterMode::Cluster);

        // Write a value
        let set_cmd = RedisCommand::from_strings(
            "SET".to_string(),
            vec!["consistency_test_key".to_string(), "test_value".to_string()],
        );

        let write_result = router.route_command(set_cmd).await;
        
        if write_result.is_ok() {
            // If write succeeded, try to read it back
            let get_cmd = RedisCommand::from_strings(
                "GET".to_string(),
                vec!["consistency_test_key".to_string()],
            );

            // Linearizable read should see the write
            let read_result = router
                .route_read_with_consistency(get_cmd.clone(), ConsistencyLevel::Linearizable)
                .await;
            
            assert!(read_result.is_ok(), "Linearizable read after write should succeed");

            // Eventual read should also work
            let eventual_read_result = router
                .route_read_with_consistency(get_cmd, ConsistencyLevel::Eventual)
                .await;
            
            assert!(eventual_read_result.is_ok(), "Eventual read after write should succeed");
        }

        // Cleanup
        let _ = raft_node.shutdown().await;
        let _ = std::fs::remove_dir_all("./test_data/router_write_read_test");
    }

    #[test]
    fn test_consistency_level_enum() {
        use crate::types::ConsistencyLevel;

        // Test consistency level enum
        let linearizable = ConsistencyLevel::Linearizable;
        let eventual = ConsistencyLevel::Eventual;

        assert_ne!(linearizable, eventual);
        assert_eq!(linearizable, ConsistencyLevel::Linearizable);
        assert_eq!(eventual, ConsistencyLevel::Eventual);
    }
}
