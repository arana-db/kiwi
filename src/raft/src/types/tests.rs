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

//! Unit tests for core types

#[cfg(test)]
mod tests {
    use super::super::*;
    use bytes::Bytes;
    use std::collections::BTreeSet;

    #[test]
    fn test_request_id_creation() {
        let id1 = RequestId::new();
        let id2 = RequestId::new();
        
        // IDs should be unique
        assert_ne!(id1, id2);
        assert!(id2.as_u64() > id1.as_u64());
    }

    #[test]
    fn test_request_id_default() {
        let id1 = RequestId::default();
        let id2 = RequestId::default();
        
        // Default should create new unique IDs
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_request_id_display() {
        let id = RequestId::new();
        let display_str = format!("{}", id);
        let expected = id.as_u64().to_string();
        assert_eq!(display_str, expected);
    }

    #[test]
    fn test_request_id_serialization() {
        let original_id = RequestId::new();
        
        // Test JSON serialization
        let serialized = serde_json::to_string(&original_id).unwrap();
        let deserialized: RequestId = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(original_id, deserialized);
        assert_eq!(original_id.as_u64(), deserialized.as_u64());
    }

    #[test]
    fn test_redis_command_creation() {
        let cmd = RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("key"), Bytes::from("value")]
        );
        
        assert_eq!(cmd.command, "SET");
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args[0], Bytes::from("key"));
        assert_eq!(cmd.args[1], Bytes::from("value"));
    }

    #[test]
    fn test_redis_command_from_strings() {
        let cmd = RedisCommand::from_strings(
            "GET".to_string(),
            vec!["key".to_string()]
        );
        
        assert_eq!(cmd.command, "GET");
        assert_eq!(cmd.args.len(), 1);
        assert_eq!(cmd.args[0], Bytes::from("key"));
    }

    #[test]
    fn test_redis_command_from_bytes() {
        let cmd = RedisCommand::from_bytes(
            "DEL".to_string(),
            vec![b"key1".to_vec(), b"key2".to_vec()]
        );
        
        assert_eq!(cmd.command, "DEL");
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args[0], Bytes::from("key1"));
        assert_eq!(cmd.args[1], Bytes::from("key2"));
    }

    #[test]
    fn test_redis_command_serialization() {
        let original_cmd = RedisCommand::new(
            "MSET".to_string(),
            vec![
                Bytes::from("key1"), Bytes::from("value1"),
                Bytes::from("key2"), Bytes::from("value2")
            ]
        );
        
        let serialized = serde_json::to_string(&original_cmd).unwrap();
        let deserialized: RedisCommand = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(original_cmd.command, deserialized.command);
        assert_eq!(original_cmd.args, deserialized.args);
    }

    #[test]
    fn test_client_request_creation() {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new("PING".to_string(), vec![]),
            consistency_level: ConsistencyLevel::Linearizable,
        };
        
        assert_eq!(request.command.command, "PING");
        assert_eq!(request.consistency_level, ConsistencyLevel::Linearizable);
        assert!(request.command.args.is_empty());
    }

    #[test]
    fn test_client_response_success() {
        let id = RequestId::new();
        let data = Bytes::from("OK");
        let response = ClientResponse::success(id, data.clone(), Some(1));
        
        assert_eq!(response.id, id);
        assert_eq!(response.result, Ok(data));
        assert_eq!(response.leader_id, Some(1));
    }

    #[test]
    fn test_client_response_error() {
        let id = RequestId::new();
        let error_msg = "Command failed".to_string();
        let response = ClientResponse::error(id, error_msg.clone(), Some(2));
        
        assert_eq!(response.id, id);
        assert_eq!(response.result, Err(error_msg));
        assert_eq!(response.leader_id, Some(2));
    }

    #[test]
    fn test_consistency_level_default() {
        let default_level = ConsistencyLevel::default();
        assert_eq!(default_level, ConsistencyLevel::Linearizable);
    }

    #[test]
    fn test_consistency_level_serialization() {
        let levels = vec![
            ConsistencyLevel::Linearizable,
            ConsistencyLevel::Eventual,
        ];
        
        for level in levels {
            let serialized = serde_json::to_string(&level).unwrap();
            let deserialized: ConsistencyLevel = serde_json::from_str(&serialized).unwrap();
            assert_eq!(level, deserialized);
        }
    }

    #[test]
    fn test_cluster_config_default() {
        let config = ClusterConfig::default();
        
        assert!(!config.enabled);
        assert_eq!(config.node_id, 1);
        assert!(config.cluster_members.is_empty());
        assert_eq!(config.data_dir, "./raft_data");
        assert_eq!(config.heartbeat_interval_ms, 1000);
        assert_eq!(config.election_timeout_min_ms, 3000);
        assert_eq!(config.election_timeout_max_ms, 6000);
        assert_eq!(config.snapshot_threshold, 1000);
        assert_eq!(config.max_payload_entries, 100);
    }

    #[test]
    fn test_cluster_config_with_members() {
        let mut config = ClusterConfig::default();
        config.enabled = true;
        config.node_id = 2;
        
        let mut members = BTreeSet::new();
        members.insert("1:127.0.0.1:7379".to_string());
        members.insert("2:127.0.0.1:7380".to_string());
        members.insert("3:127.0.0.1:7381".to_string());
        config.cluster_members = members;
        
        assert!(config.enabled);
        assert_eq!(config.node_id, 2);
        assert_eq!(config.cluster_members.len(), 3);
        assert!(config.cluster_members.contains("2:127.0.0.1:7380"));
    }

    #[test]
    fn test_cluster_health_creation() {
        let health = ClusterHealth {
            total_members: 3,
            healthy_members: 3,
            learners: 0,
            partitioned_nodes: 0,
            current_leader: Some(1),
            is_healthy: true,
            last_log_index: 100,
            commit_index: 95,
        };
        
        assert_eq!(health.total_members, 3);
        assert_eq!(health.healthy_members, 3);
        assert!(health.is_healthy);
        assert_eq!(health.current_leader, Some(1));
        assert_eq!(health.last_log_index, 100);
        assert_eq!(health.commit_index, 95);
    }

    #[test]
    fn test_cluster_health_serialization() {
        let health = ClusterHealth {
            total_members: 5,
            healthy_members: 4,
            learners: 1,
            partitioned_nodes: 1,
            current_leader: Some(2),
            is_healthy: false,
            last_log_index: 200,
            commit_index: 180,
        };
        
        let serialized = serde_json::to_string(&health).unwrap();
        let deserialized: ClusterHealth = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(health.total_members, deserialized.total_members);
        assert_eq!(health.healthy_members, deserialized.healthy_members);
        assert_eq!(health.learners, deserialized.learners);
        assert_eq!(health.partitioned_nodes, deserialized.partitioned_nodes);
        assert_eq!(health.current_leader, deserialized.current_leader);
        assert_eq!(health.is_healthy, deserialized.is_healthy);
        assert_eq!(health.last_log_index, deserialized.last_log_index);
        assert_eq!(health.commit_index, deserialized.commit_index);
    }

    #[test]
    fn test_type_config_consistency() {
        // Test that TypeConfig implements required traits
        let _config = TypeConfig::default();
        
        // Test type aliases are properly defined
        let node_id: NodeId = 1;
        let term: Term = 100;
        let log_index: LogIndex = 50;
        
        assert_eq!(node_id, 1);
        assert_eq!(term, 100);
        assert_eq!(log_index, 50);
    }

    #[test]
    fn test_complex_client_request_serialization() {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "HMSET".to_string(),
                vec![
                    Bytes::from("hash_key"),
                    Bytes::from("field1"), Bytes::from("value1"),
                    Bytes::from("field2"), Bytes::from("value2"),
                ]
            ),
            consistency_level: ConsistencyLevel::Eventual,
        };
        
        let serialized = serde_json::to_string(&request).unwrap();
        let deserialized: ClientRequest = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(request.id, deserialized.id);
        assert_eq!(request.command.command, deserialized.command.command);
        assert_eq!(request.command.args, deserialized.command.args);
        assert_eq!(request.consistency_level, deserialized.consistency_level);
    }

    #[test]
    fn test_empty_redis_command() {
        let cmd = RedisCommand::new("PING".to_string(), vec![]);
        
        assert_eq!(cmd.command, "PING");
        assert!(cmd.args.is_empty());
    }

    #[test]
    fn test_redis_command_with_binary_data() {
        let binary_data = vec![0u8, 1, 2, 3, 255];
        let cmd = RedisCommand::from_bytes(
            "SET".to_string(),
            vec![b"binary_key".to_vec(), binary_data.clone()]
        );
        
        assert_eq!(cmd.command, "SET");
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args[0], Bytes::from("binary_key"));
        assert_eq!(cmd.args[1], Bytes::from(binary_data));
    }
}