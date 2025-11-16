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

//! Raft-aware network connection handling
//!
//! This module provides connection processing that routes commands through
//! the Raft consensus layer for cluster mode operations, ensuring strong
//! consistency for write operations and configurable consistency for reads.
//!
//! # Requirements
//! - Requirement 6.1: Network layer SHALL identify read/write operation types
//! - Requirement 6.2: Write operations SHALL route to RaftNode.propose()
//! - Requirement 6.3: Read operations SHALL route based on consistency level

use std::sync::Arc;

use bytes::Bytes;
use client::Client;
use cmd::table::CmdTable;
use executor::CmdExecutor;
use log::{debug, error, warn};
use resp::encode::RespEncoder;
use resp::{Parse, RespData, RespEncode, RespParseResult, RespVersion};
use tokio::select;

use crate::executor_ext::CmdExecutorNetworkExt;
use crate::storage_client::StorageClient;

/// Cluster mode for routing decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMode {
    /// Single node mode - direct storage access
    Single,
    /// Cluster mode - Raft consensus required
    Cluster,
}

/// Process a network connection with Raft-aware routing
///
/// This function routes commands based on cluster mode:
/// - Single mode: Direct storage access via StorageClient
/// - Cluster mode: Route through Raft for writes, configurable for reads
///
/// # Requirements
/// - Requirement 6.1.1: Network layer SHALL identify read/write operation types
/// - Requirement 6.1.2: Write operations SHALL route to RaftNode.propose()
/// - Requirement 6.1.3: Read operations SHALL route based on consistency level
/// - Requirement 6.1.4: Command parsing SHALL maintain Redis protocol compatibility
/// - Requirement 6.1.5: Response format SHALL conform to Redis protocol
pub async fn process_raft_aware_connection(
    client: Arc<Client>,
    _storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    _executor: Arc<CmdExecutor>,
    cluster_mode: ClusterMode,
    raft_router: Option<Arc<raft::RequestRouter>>,
) -> std::io::Result<()> {
    let mut buf = vec![0; 4096];
    let mut resp_parser = resp::RespParse::new(resp::RespVersion::RESP2);

    debug!(
        "Starting Raft-aware connection processing in {:?} mode",
        cluster_mode
    );

    loop {
        select! {
            result = client.read(&mut buf) => {
                match result {
                    Ok(n) => {
                        if n == 0 {
                            debug!("Connection closed by client");
                            return Ok(());
                        }

                        debug!("Received {} bytes from client", n);

                        match resp_parser.parse(Bytes::copy_from_slice(&buf[..n])) {
                            RespParseResult::Complete(data) => {
                                debug!("RESP parsing complete: {:?}", data);

                                if let RespData::Array(Some(params)) = data {
                                    if params.is_empty() {
                                        debug!("Empty command array, continuing");
                                        continue;
                                    }

                                    if let RespData::BulkString(Some(cmd_name)) = &params[0] {
                                        client.set_cmd_name(cmd_name.as_ref());
                                        debug!("Command: {}", String::from_utf8_lossy(cmd_name));
                                    }

                                    let argv = params.iter().map(|p| {
                                        if let RespData::BulkString(Some(d)) = p {
                                            d.to_vec()
                                        } else {
                                            vec![]
                                        }
                                    }).collect::<Vec<Vec<u8>>>();

                                    client.set_argv(&argv);

                                    // Route command based on cluster mode
                                    handle_raft_aware_command(
                                        client.clone(),
                                        _storage_client.clone(),
                                        cmd_table.clone(),
                                        _executor.clone(),
                                        cluster_mode,
                                        raft_router.clone(),
                                    ).await;

                                    // Extract the reply and send it
                                    let response = client.take_reply();
                                    debug!("Sending response: {:?}", response);

                                    let mut encoder = RespEncoder::new(RespVersion::RESP2);
                                    encoder.encode_resp_data(&response);

                                    match client.write(encoder.get_response().as_ref()).await {
                                        Ok(_) => debug!("Response sent successfully"),
                                        Err(e) => {
                                            error!("Write error: {}", e);
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                            RespParseResult::Error(e) => {
                                error!("RESP protocol error: {:?}", e);
                                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                            }
                            RespParseResult::Incomplete => {
                                debug!("Incomplete RESP data, waiting for more");
                                // Not enough data, wait for more
                            }
                        }
                    }
                    Err(e) => {
                        error!("Read error: {:?}", e);
                        return Err(e);
                    }
                }
            }
        }
    }
}

/// Handle a command with Raft-aware routing
///
/// Routes commands based on:
/// 1. Cluster mode (single vs cluster)
/// 2. Command type (read vs write)
/// 3. Consistency requirements
///
/// # Requirements
/// - Requirement 6.1.1: Network layer SHALL identify read/write operation types
/// - Requirement 6.1.2: Write operations SHALL route to RaftNode.propose()
/// - Requirement 6.1.3: Read operations SHALL route based on consistency level
async fn handle_raft_aware_command(
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    cluster_mode: ClusterMode,
    raft_router: Option<Arc<raft::RequestRouter>>,
) {
    let cmd_name = String::from_utf8_lossy(&client.cmd_name()).to_lowercase();
    debug!(
        "Handling Raft-aware command: {} in {:?} mode",
        cmd_name, cluster_mode
    );

    match cluster_mode {
        ClusterMode::Single => {
            // Single mode: direct storage access
            debug!("Single mode: routing to storage directly");
            handle_single_mode_command(client, storage_client, cmd_table, executor).await;
        }
        ClusterMode::Cluster => {
            if let Some(router) = raft_router {
                handle_cluster_mode_command(client, storage_client, cmd_table, executor, router)
                    .await;
            } else {
                error!("Cluster mode enabled but no Raft router available");
                client.set_reply(RespData::Error("CLUSTERDOWN Cluster not configured".into()));
            }
        }
    }
}

/// Handle command in single mode (direct storage access)
async fn handle_single_mode_command(
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
) {
    let cmd_name = String::from_utf8_lossy(&client.cmd_name()).to_lowercase();
    debug!("Handling single mode command: {}", cmd_name);

    if let Some(cmd) = cmd_table.get(&cmd_name) {
        debug!("Command found in table: {}", cmd_name);

        // Create network-aware execution that uses StorageClient
        let network_exec = crate::network_execution::NetworkCmdExecution {
            cmd: cmd.clone(),
            client: client.clone(),
            storage_client: storage_client.clone(),
        };

        // Execute the command using the network-aware executor
        match executor.execute_network(network_exec).await {
            Ok(_) => {
                debug!("Command executed successfully: {}", cmd_name);
            }
            Err(e) => {
                error!("Command execution failed for {}: {}", cmd_name, e);
                let error_response = generate_error_response(&cmd_name, &e.to_string());
                client.set_reply(error_response);
            }
        }
    } else {
        let err_msg = format!("ERR unknown command `{}`", cmd_name);
        warn!("Unknown command: {}", cmd_name);
        client.set_reply(RespData::Error(err_msg.into()));
    }
}

/// Handle command in cluster mode (Raft routing)
///
/// # Requirements
/// - Requirement 6.1.1: Network layer SHALL identify read/write operation types
/// - Requirement 6.1.2: Write operations SHALL route to RaftNode.propose()
/// - Requirement 6.1.3: Read operations SHALL route based on consistency level
async fn handle_cluster_mode_command(
    client: Arc<Client>,
    _storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    _executor: Arc<CmdExecutor>,
    raft_router: Arc<raft::RequestRouter>,
) {
    let cmd_name = String::from_utf8_lossy(&client.cmd_name()).to_lowercase();
    let argv = client.argv();

    debug!("Handling cluster mode command: {}", cmd_name);

    // Check if command exists in table
    if cmd_table.get(&cmd_name).is_none() {
        let err_msg = format!("ERR unknown command `{}`", cmd_name);
        warn!("Unknown command: {}", cmd_name);
        client.set_reply(RespData::Error(err_msg.into()));
        return;
    }

    // Create RedisCommand for Raft routing
    let redis_command = raft::types::RedisCommand::from_bytes(cmd_name.clone(), argv.clone());

    // Route through Raft
    match raft_router.route_command(redis_command).await {
        Ok(response) => {
            debug!("Raft command succeeded: {}", cmd_name);

            // Convert Raft response to RESP data
            match response.data {
                Ok(data) => {
                    let resp_data = parse_raft_response(data);
                    client.set_reply(resp_data);
                }
                Err(error_msg) => {
                    error!("Raft command error: {}", error_msg);
                    client.set_reply(RespData::Error(error_msg.into()));
                }
            }
        }
        Err(raft::error::RaftError::NotLeader { leader_id, .. }) => {
            warn!("Not leader for command {}: {:?}", cmd_name, leader_id);
            let endpoint_opt = raft_router.get_leader_endpoint().await;
            if let Some(endpoint) = endpoint_opt {
                let slot = if argv.len() > 1 {
                    let key = &argv[1];
                    let mut s: u16 = 0;
                    for b in key.iter() {
                        s = s.wrapping_add(*b as u16);
                    }
                    (s % 16384) as usize
                } else {
                    0
                };
                let msg = format!("MOVED {} {}", slot, endpoint);
                client.set_reply(RespData::Error(msg.into()));
            } else {
                client.set_reply(RespData::Error("CLUSTERDOWN No leader available".into()));
            }
        }
        Err(e) => {
            error!("Raft routing error for {}: {}", cmd_name, e);
            let error_response = generate_error_response(&cmd_name, &e.to_string());
            client.set_reply(error_response);
        }
    }
}

/// Parse Raft response bytes into RESP data
fn parse_raft_response(data: bytes::Bytes) -> RespData {
    let mut parser = resp::RespParse::new(RespVersion::RESP2);
    match parser.parse(data) {
        RespParseResult::Complete(resp_data) => resp_data,
        RespParseResult::Error(e) => {
            error!("Failed to parse Raft response: {:?}", e);
            RespData::Error(format!("ERR Failed to parse response: {}", e).into())
        }
        RespParseResult::Incomplete => {
            error!("Incomplete Raft response");
            RespData::Error("ERR Incomplete response from Raft".into())
        }
    }
}

/// Generate error response in RESP format
fn generate_error_response(cmd_name: &str, error: &str) -> RespData {
    let error_message = format!("ERR Command '{}' failed: {}", cmd_name, error);
    RespData::Error(error_message.into())
}

/// Check if a command is a write operation
///
/// # Requirements
/// - Requirement 6.1.1: Network layer SHALL identify read/write operation types
pub fn is_write_command(cmd_name: &str) -> bool {
    let cmd = cmd_name.to_lowercase();
    matches!(
        cmd.as_str(),
        // String commands
        "set" | "setex" | "psetex" | "setnx" | "setrange" | "append" |
        "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" |
        "mset" | "msetnx" |
        // Key commands
        "del" | "unlink" | "expire" | "expireat" | "pexpire" | "pexpireat" |
        "persist" | "rename" | "renamenx" |
        // List commands
        "lpush" | "rpush" | "lpushx" | "rpushx" | "lpop" | "rpop" |
        "lset" | "linsert" | "lrem" | "ltrim" | "rpoplpush" |
        // Set commands
        "sadd" | "srem" | "spop" | "smove" |
        // Sorted set commands
        "zadd" | "zrem" | "zincrby" | "zremrangebyrank" | "zremrangebyscore" |
        "zremrangebylex" | "zpopmin" | "zpopmax" |
        // Hash commands
        "hset" | "hsetnx" | "hmset" | "hdel" | "hincrby" | "hincrbyfloat" |
        // Database commands
        "flushdb" | "flushall" | "swapdb" |
        // Pub/Sub commands (writes to channels)
        "publish" |
        // Transaction commands
        "multi" | "exec" | "discard"
    )
}

/// Check if a command is a read operation
///
/// # Requirements
/// - Requirement 6.1.1: Network layer SHALL identify read/write operation types
pub fn is_read_command(cmd_name: &str) -> bool {
    let cmd = cmd_name.to_lowercase();
    matches!(
        cmd.as_str(),
        // String commands
        "get" | "mget" | "strlen" | "getrange" |
        // Key commands
        "exists" | "ttl" | "pttl" | "type" | "keys" | "scan" |
        // List commands
        "llen" | "lrange" | "lindex" | "lpos" |
        // Set commands
        "smembers" | "sismember" | "scard" | "sinter" | "sunion" | "sdiff" |
        // Sorted set commands
        "zrange" | "zrevrange" | "zrangebyscore" | "zrevrangebyscore" |
        "zrank" | "zrevrank" | "zscore" | "zcard" | "zcount" |
        // Hash commands
        "hget" | "hmget" | "hgetall" | "hkeys" | "hvals" | "hlen" | "hexists" |
        // Server commands
        "ping" | "echo" | "info" | "dbsize"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_mode() {
        assert_eq!(ClusterMode::Single, ClusterMode::Single);
        assert_eq!(ClusterMode::Cluster, ClusterMode::Cluster);
        assert_ne!(ClusterMode::Single, ClusterMode::Cluster);
    }

    #[test]
    fn test_write_command_detection() {
        assert!(is_write_command("SET"));
        assert!(is_write_command("set"));
        assert!(is_write_command("DEL"));
        assert!(is_write_command("INCR"));
        assert!(is_write_command("LPUSH"));
        assert!(is_write_command("SADD"));
        assert!(is_write_command("ZADD"));
        assert!(is_write_command("HSET"));

        assert!(!is_write_command("GET"));
        assert!(!is_write_command("LRANGE"));
        assert!(!is_write_command("SMEMBERS"));
    }

    #[test]
    fn test_read_command_detection() {
        assert!(is_read_command("GET"));
        assert!(is_read_command("get"));
        assert!(is_read_command("MGET"));
        assert!(is_read_command("EXISTS"));
        assert!(is_read_command("LRANGE"));
        assert!(is_read_command("SMEMBERS"));
        assert!(is_read_command("ZRANGE"));
        assert!(is_read_command("HGET"));

        assert!(!is_read_command("SET"));
        assert!(!is_read_command("DEL"));
        assert!(!is_read_command("LPUSH"));
    }

    #[test]
    fn test_command_classification_completeness() {
        // Ensure commands are classified as either read or write
        let test_commands = vec![
            ("SET", true, false),
            ("GET", false, true),
            ("DEL", true, false),
            ("EXISTS", false, true),
            ("INCR", true, false),
            ("LPUSH", true, false),
            ("LRANGE", false, true),
        ];

        for (cmd, should_be_write, should_be_read) in test_commands {
            assert_eq!(is_write_command(cmd), should_be_write, "Command: {}", cmd);
            assert_eq!(is_read_command(cmd), should_be_read, "Command: {}", cmd);
        }
    }
}
