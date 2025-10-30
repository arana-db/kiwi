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

//! Command serialization for Raft log entries
//!
//! This module provides efficient serialization and deserialization of Redis commands
//! for storage in Raft log entries and network transmission.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use resp::command::{RespCommand, CommandType, Command};
use resp::types::RespData;
use crate::error::{RaftError, RaftResult};
use crate::types::{RedisCommand, ClientRequest, ClientResponse, RequestId, ConsistencyLevel};

/// Serialization format for Redis commands in Raft log entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedCommand {
    /// Command name (e.g., "SET", "GET", "DEL")
    pub command: String,
    /// Command arguments as byte arrays
    pub args: Vec<Vec<u8>>,
    /// Optional metadata for command execution
    pub metadata: HashMap<String, String>,
}

/// Command serializer for efficient encoding/decoding
pub struct CommandSerializer;

impl CommandSerializer {
    /// Serialize a RespCommand to bytes using bincode for efficiency
    pub fn serialize_resp_command(cmd: &RespCommand) -> RaftResult<Vec<u8>> {
        let serialized_cmd = SerializedCommand {
            command: cmd.command_type.to_string(),
            args: cmd.args.iter().map(|b| b.to_vec()).collect(),
            metadata: HashMap::new(),
        };

        bincode::serialize(&serialized_cmd)
            .map_err(|e| RaftError::invalid_request(format!("Serialization failed: {}", e)))
    }

    /// Deserialize bytes to RespCommand using bincode
    pub fn deserialize_resp_command(data: &[u8]) -> RaftResult<RespCommand> {
        let serialized_cmd: SerializedCommand = bincode::deserialize(data)
            .map_err(|e| RaftError::invalid_request(format!("Deserialization failed: {}", e)))?;

        let command_type = CommandType::from_str(&serialized_cmd.command)
            .map_err(|_| RaftError::invalid_request(format!("Invalid command: {}", serialized_cmd.command)))?;

        let args = serialized_cmd.args
            .into_iter()
            .map(Bytes::from)
            .collect();

        Ok(RespCommand::new(command_type, args, false))
    }

    /// Serialize a RedisCommand to bytes
    pub fn serialize_redis_command(cmd: &RedisCommand) -> RaftResult<Vec<u8>> {
        let serialized_cmd = SerializedCommand {
            command: cmd.command.clone(),
            args: cmd.args.iter().map(|b| b.to_vec()).collect(),
            metadata: HashMap::new(),
        };

        bincode::serialize(&serialized_cmd)
            .map_err(|e| RaftError::invalid_request(format!("Serialization failed: {}", e)))
    }

    /// Deserialize bytes to RedisCommand
    pub fn deserialize_redis_command(data: &[u8]) -> RaftResult<RedisCommand> {
        let serialized_cmd: SerializedCommand = bincode::deserialize(data)
            .map_err(|e| RaftError::invalid_request(format!("Deserialization failed: {}", e)))?;

        let args = serialized_cmd.args
            .into_iter()
            .map(Bytes::from)
            .collect();

        Ok(RedisCommand::new(serialized_cmd.command, args))
    }

    /// Convert RespData to RedisCommand for Raft processing
    pub fn resp_data_to_redis_command(data: &RespData) -> RaftResult<RedisCommand> {
        match data {
            RespData::Array(Some(ref array)) if !array.is_empty() => {
                let command_name = array[0].as_string().ok_or_else(|| {
                    RaftError::invalid_request("Command name must be a string")
                })?;

                let args = array
                    .iter()
                    .skip(1)
                    .map(|data| {
                        data.as_bytes().ok_or_else(|| {
                            RaftError::invalid_request("Command argument must be convertible to bytes")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(RedisCommand::new(command_name, args))
            }
            RespData::Inline(parts) if !parts.is_empty() => {
                let command_name = std::str::from_utf8(&parts[0])
                    .map_err(|_| RaftError::invalid_request("Command name must be valid UTF-8"))?
                    .to_string();

                let args = parts.iter().skip(1).cloned().map(Bytes::from).collect();

                Ok(RedisCommand::new(command_name, args))
            }
            _ => Err(RaftError::invalid_request("Invalid command format")),
        }
    }

    /// Convert RedisCommand to RespData for response
    pub fn redis_command_to_resp_data(cmd: &RedisCommand) -> RespData {
        let mut parts = vec![RespData::BulkString(Some(cmd.command.clone().into()))];
        
        for arg in &cmd.args {
            parts.push(RespData::BulkString(Some(arg.clone())));
        }

        RespData::Array(Some(parts))
    }

    /// Create a ClientRequest from RespData with default consistency level
    pub fn create_client_request(data: &RespData) -> RaftResult<ClientRequest> {
        let redis_command = Self::resp_data_to_redis_command(data)?;
        
        Ok(ClientRequest {
            id: RequestId::new(),
            command: redis_command,
            consistency_level: ConsistencyLevel::default(),
        })
    }

    /// Create a ClientRequest with specific consistency level
    pub fn create_client_request_with_consistency(
        data: &RespData,
        consistency: ConsistencyLevel,
    ) -> RaftResult<ClientRequest> {
        let redis_command = Self::resp_data_to_redis_command(data)?;
        
        Ok(ClientRequest {
            id: RequestId::new(),
            command: redis_command,
            consistency_level: consistency,
        })
    }

    /// Serialize ClientRequest for network transmission
    pub fn serialize_client_request(request: &ClientRequest) -> RaftResult<Vec<u8>> {
        serde_json::to_vec(request)
            .map_err(RaftError::Serialization)
    }

    /// Deserialize ClientRequest from network data
    pub fn deserialize_client_request(data: &[u8]) -> RaftResult<ClientRequest> {
        serde_json::from_slice(data)
            .map_err(RaftError::Serialization)
    }

    /// Serialize ClientResponse for network transmission
    pub fn serialize_client_response(response: &ClientResponse) -> RaftResult<Vec<u8>> {
        serde_json::to_vec(response)
            .map_err(RaftError::Serialization)
    }

    /// Deserialize ClientResponse from network data
    pub fn deserialize_client_response(data: &[u8]) -> RaftResult<ClientResponse> {
        serde_json::from_slice(data)
            .map_err(RaftError::Serialization)
    }

    /// Estimate serialized size of a command (for batching optimization)
    pub fn estimate_command_size(cmd: &RedisCommand) -> usize {
        let base_size = cmd.command.len() + 8; // command name + overhead
        let args_size: usize = cmd.args.iter().map(|arg| arg.len() + 4).sum(); // args + length prefixes
        base_size + args_size
    }

    /// Check if command is read-only (for optimization purposes)
    pub fn is_read_only_command(cmd: &RedisCommand) -> bool {
        matches!(cmd.command.to_uppercase().as_str(), 
            "GET" | "MGET" | "EXISTS" | "TTL" | "PTTL" | "TYPE" | 
            "STRLEN" | "GETRANGE" | "GETBIT" | "LLEN" | "LINDEX" | 
            "LRANGE" | "SCARD" | "SISMEMBER" | "SMEMBERS" | "SRANDMEMBER" |
            "ZCARD" | "ZCOUNT" | "ZRANGE" | "ZRANGEBYSCORE" | "ZRANK" |
            "ZREVRANGE" | "ZREVRANGEBYSCORE" | "ZREVRANK" | "ZSCORE" |
            "HGET" | "HMGET" | "HGETALL" | "HEXISTS" | "HKEYS" | "HVALS" |
            "HLEN" | "HSTRLEN" | "PING" | "ECHO" | "INFO"
        )
    }

    /// Check if command modifies data (write operation)
    pub fn is_write_command(cmd: &RedisCommand) -> bool {
        !Self::is_read_only_command(cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resp::command::CommandType;

    #[test]
    fn test_serialize_deserialize_resp_command() {
        let original_cmd = RespCommand::new(
            CommandType::Set,
            vec![Bytes::from("key"), Bytes::from("value")],
            false,
        );

        let serialized = CommandSerializer::serialize_resp_command(&original_cmd).unwrap();
        let deserialized = CommandSerializer::deserialize_resp_command(&serialized).unwrap();

        assert_eq!(original_cmd.command_type, deserialized.command_type);
        assert_eq!(original_cmd.args, deserialized.args);
    }

    #[test]
    fn test_serialize_deserialize_redis_command() {
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
    fn test_resp_data_to_redis_command() {
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
    fn test_create_client_request() {
        let resp_data = RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from("GET"))),
            RespData::BulkString(Some(Bytes::from("key"))),
        ]));

        let request = CommandSerializer::create_client_request(&resp_data).unwrap();
        
        assert_eq!(request.command.command, "GET");
        assert_eq!(request.command.args.len(), 1);
        assert_eq!(request.consistency_level, ConsistencyLevel::Linearizable);
    }

    #[test]
    fn test_command_classification() {
        let read_cmd = RedisCommand::new("GET".to_string(), vec![Bytes::from("key")]);
        let write_cmd = RedisCommand::new("SET".to_string(), vec![Bytes::from("key"), Bytes::from("value")]);

        assert!(CommandSerializer::is_read_only_command(&read_cmd));
        assert!(!CommandSerializer::is_read_only_command(&write_cmd));
        assert!(!CommandSerializer::is_write_command(&read_cmd));
        assert!(CommandSerializer::is_write_command(&write_cmd));
    }

    #[test]
    fn test_estimate_command_size() {
        let cmd = RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("key"), Bytes::from("value")],
        );

        let size = CommandSerializer::estimate_command_size(&cmd);
        assert!(size > 0);
        assert!(size >= cmd.command.len() + cmd.args.iter().map(|a| a.len()).sum::<usize>());
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
}