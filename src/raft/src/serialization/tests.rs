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

//! Unit tests for command serialization

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::placeholder_types::RespData;
    use crate::types::{ClientRequest, ClientResponse, ConsistencyLevel, RedisCommand, RequestId};
    use bytes::Bytes;

    #[test]
    fn test_redis_command_serialization() {
        let original_cmd = RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("key"), Bytes::from("value")],
        );

        let serialized = CommandSerializer::serialize_redis_command(&original_cmd).unwrap();
        let deserialized = CommandSerializer::deserialize_redis_command(&serialized).unwrap();

        assert_eq!(original_cmd.command, deserialized.command);
        assert_eq!(original_cmd.args, deserialized.args);
    }

    #[test]
    fn test_redis_command_serialization_empty_args() {
        let original_cmd = RedisCommand::new("PING".to_string(), vec![]);

        let serialized = CommandSerializer::serialize_redis_command(&original_cmd).unwrap();
        let deserialized = CommandSerializer::deserialize_redis_command(&serialized).unwrap();

        assert_eq!(original_cmd.command, deserialized.command);
        assert!(deserialized.args.is_empty());
    }

    #[test]
    fn test_redis_command_serialization_binary_data() {
        let binary_data = vec![0u8, 1, 2, 3, 255, 128];
        let original_cmd = RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("binary_key"), Bytes::from(binary_data.clone())],
        );

        let serialized = CommandSerializer::serialize_redis_command(&original_cmd).unwrap();
        let deserialized = CommandSerializer::deserialize_redis_command(&serialized).unwrap();

        assert_eq!(original_cmd.command, deserialized.command);
        assert_eq!(original_cmd.args, deserialized.args);
        assert_eq!(deserialized.args[1], Bytes::from(binary_data));
    }

    #[test]
    fn test_resp_data_to_redis_command_array() {
        let resp_data = RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from("SET"))),
            RespData::BulkString(Some(Bytes::from("key"))),
            RespData::BulkString(Some(Bytes::from("value"))),
        ]));

        let redis_cmd = CommandSerializer::resp_data_to_redis_command(&resp_data).unwrap();

        assert_eq!(redis_cmd.command, "SET");
        assert_eq!(redis_cmd.args.len(), 2);
        assert_eq!(redis_cmd.args[0], Bytes::from("key"));
        assert_eq!(redis_cmd.args[1], Bytes::from("value"));
    }

    #[test]
    fn test_resp_data_to_redis_command_inline() {
        let resp_data = RespData::Inline(vec!["GET".to_string(), "key".to_string()]);

        let redis_cmd = CommandSerializer::resp_data_to_redis_command(&resp_data).unwrap();

        assert_eq!(redis_cmd.command, "GET");
        assert_eq!(redis_cmd.args.len(), 1);
        assert_eq!(redis_cmd.args[0], Bytes::from("key"));
    }

    #[test]
    fn test_resp_data_to_redis_command_empty_array() {
        let resp_data = RespData::Array(Some(vec![]));
        let result = CommandSerializer::resp_data_to_redis_command(&resp_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_resp_data_to_redis_command_invalid_format() {
        let resp_data = RespData::SimpleString("PING".to_string());
        let result = CommandSerializer::resp_data_to_redis_command(&resp_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_redis_command_to_resp_data() {
        let cmd = RedisCommand::new(
            "MGET".to_string(),
            vec![Bytes::from("key1"), Bytes::from("key2")],
        );

        let resp_data = CommandSerializer::redis_command_to_resp_data(&cmd);

        match resp_data {
            RespData::Array(Some(parts)) => {
                assert_eq!(parts.len(), 3);
                match &parts[0] {
                    RespData::BulkString(Some(cmd_bytes)) => {
                        assert_eq!(cmd_bytes, &Bytes::from("MGET"));
                    }
                    _ => panic!("Expected BulkString for command"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_create_client_request() {
        let resp_data = RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from("DEL"))),
            RespData::BulkString(Some(Bytes::from("key1"))),
            RespData::BulkString(Some(Bytes::from("key2"))),
        ]));

        let request = CommandSerializer::create_client_request(&resp_data).unwrap();

        assert_eq!(request.command.command, "DEL");
        assert_eq!(request.command.args.len(), 2);
        assert_eq!(request.consistency_level, ConsistencyLevel::Linearizable);
    }

    #[test]
    fn test_create_client_request_with_consistency() {
        let resp_data = RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from("GET"))),
            RespData::BulkString(Some(Bytes::from("key"))),
        ]));

        let request = CommandSerializer::create_client_request_with_consistency(
            &resp_data,
            ConsistencyLevel::Eventual,
        )
        .unwrap();

        assert_eq!(request.command.command, "GET");
        assert_eq!(request.consistency_level, ConsistencyLevel::Eventual);
    }

    #[test]
    fn test_client_request_serialization() {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new("PING".to_string(), vec![]),
            consistency_level: ConsistencyLevel::Eventual,
        };

        let serialized = CommandSerializer::serialize_client_request(&request).unwrap();
        let deserialized = CommandSerializer::deserialize_client_request(&serialized).unwrap();

        assert_eq!(request.id, deserialized.id);
        assert_eq!(request.command.command, deserialized.command.command);
        assert_eq!(request.consistency_level, deserialized.consistency_level);
    }

    #[test]
    fn test_client_response_serialization() {
        let response = ClientResponse::success(RequestId::new(), Bytes::from("OK"), Some(1));

        let serialized = CommandSerializer::serialize_client_response(&response).unwrap();
        let deserialized = CommandSerializer::deserialize_client_response(&serialized).unwrap();

        assert_eq!(response.id, deserialized.id);
        assert_eq!(response.result, deserialized.result);
        assert_eq!(response.leader_id, deserialized.leader_id);
    }

    #[test]
    fn test_client_response_error_serialization() {
        let response =
            ClientResponse::error(RequestId::new(), "Command failed".to_string(), Some(2));

        let serialized = CommandSerializer::serialize_client_response(&response).unwrap();
        let deserialized = CommandSerializer::deserialize_client_response(&serialized).unwrap();

        assert_eq!(response.id, deserialized.id);
        assert_eq!(response.result, deserialized.result);
        assert_eq!(response.leader_id, deserialized.leader_id);
    }

    #[test]
    fn test_estimate_command_size() {
        let small_cmd = RedisCommand::new("PING".to_string(), vec![]);
        let small_size = CommandSerializer::estimate_command_size(&small_cmd);

        let large_cmd = RedisCommand::new(
            "MSET".to_string(),
            vec![
                Bytes::from("key1"),
                Bytes::from("value1"),
                Bytes::from("key2"),
                Bytes::from("value2"),
                Bytes::from("key3"),
                Bytes::from("value3"),
            ],
        );
        let large_size = CommandSerializer::estimate_command_size(&large_cmd);

        assert!(large_size > small_size);
        assert!(small_size >= "PING".len());
        assert!(large_size >= "MSET".len() + 6 * 4); // 6 args with overhead
    }

    #[test]
    fn test_command_classification() {
        // Test read-only commands
        let read_commands = vec![
            "GET", "MGET", "EXISTS", "TTL", "TYPE", "STRLEN", "LLEN", "LINDEX", "SCARD", "ZCARD",
            "HGET", "PING",
        ];

        for cmd_name in read_commands {
            let cmd = RedisCommand::new(cmd_name.to_string(), vec![Bytes::from("key")]);
            assert!(
                CommandSerializer::is_read_only_command(&cmd),
                "Command {} should be read-only",
                cmd_name
            );
            assert!(
                !CommandSerializer::is_write_command(&cmd),
                "Command {} should not be write command",
                cmd_name
            );
        }

        // Test write commands
        let write_commands = vec!["SET", "DEL", "INCR", "LPUSH", "SADD", "ZADD", "HSET"];

        for cmd_name in write_commands {
            let cmd = RedisCommand::new(cmd_name.to_string(), vec![Bytes::from("key")]);
            assert!(
                !CommandSerializer::is_read_only_command(&cmd),
                "Command {} should not be read-only",
                cmd_name
            );
            assert!(
                CommandSerializer::is_write_command(&cmd),
                "Command {} should be write command",
                cmd_name
            );
        }
    }

    #[test]
    fn test_command_classification_case_insensitive() {
        let cmd_lower = RedisCommand::new("get".to_string(), vec![Bytes::from("key")]);
        let cmd_upper = RedisCommand::new("GET".to_string(), vec![Bytes::from("key")]);
        let cmd_mixed = RedisCommand::new("GeT".to_string(), vec![Bytes::from("key")]);

        assert!(CommandSerializer::is_read_only_command(&cmd_lower));
        assert!(CommandSerializer::is_read_only_command(&cmd_upper));
        assert!(CommandSerializer::is_read_only_command(&cmd_mixed));
    }

    #[test]
    fn test_serialized_command_structure() {
        let cmd = RedisCommand::new(
            "HMSET".to_string(),
            vec![
                Bytes::from("hash_key"),
                Bytes::from("field1"),
                Bytes::from("value1"),
                Bytes::from("field2"),
                Bytes::from("value2"),
            ],
        );

        let serialized = CommandSerializer::serialize_redis_command(&cmd).unwrap();

        // Verify that serialization produces valid bincode data
        let deserialized_cmd: SerializedCommand = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized_cmd.command, "HMSET");
        assert_eq!(deserialized_cmd.args.len(), 5);
        assert_eq!(deserialized_cmd.args[0], b"hash_key");
        assert_eq!(deserialized_cmd.args[1], b"field1");
        assert_eq!(deserialized_cmd.args[2], b"value1");
        assert!(deserialized_cmd.metadata.is_empty());
    }

    #[test]
    fn test_invalid_serialization_data() {
        let invalid_data = b"invalid bincode data";
        let result = CommandSerializer::deserialize_redis_command(invalid_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_json_data() {
        let invalid_json = b"{ invalid json }";
        let result = CommandSerializer::deserialize_client_request(invalid_json);
        assert!(result.is_err());

        let result = CommandSerializer::deserialize_client_response(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_command_serialization() {
        let cmd = RedisCommand::new(
            "EVAL".to_string(),
            vec![
                Bytes::from("return redis.call('get', KEYS[1])"),
                Bytes::from("1"),
                Bytes::from("mykey"),
            ],
        );

        let serialized = CommandSerializer::serialize_redis_command(&cmd).unwrap();
        let deserialized = CommandSerializer::deserialize_redis_command(&serialized).unwrap();

        assert_eq!(cmd.command, deserialized.command);
        assert_eq!(cmd.args, deserialized.args);
        assert_eq!(
            deserialized.args[0],
            Bytes::from("return redis.call('get', KEYS[1])")
        );
    }
}
