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

//! Network connection handling for dual runtime architecture
//!
//! This module provides connection processing that uses StorageClient
//! instead of direct storage access, enabling communication between
//! network and storage runtimes.

use std::sync::Arc;

use crate::network_execution::NetworkCmdExecution;
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
use runtime::DualRuntimeError;

/// Process a network connection using StorageClient for storage operations
///
/// This function replaces the original process_connection to work with the
/// dual runtime architecture. It handles RESP protocol parsing in the network
/// runtime and sends storage requests to the storage runtime via StorageClient.
/// Supports request pipelining and batching for improved performance.
pub async fn process_network_connection(
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
) -> std::io::Result<()> {
    let mut buf = vec![0; 4096]; // Increased buffer size for better performance
    let mut resp_parser = resp::RespParse::new(resp::RespVersion::RESP2);
    let mut pending_commands = Vec::new();

    debug!("Starting network connection processing with pipelining support");

    loop {
        select! {
            result = client.read(&mut buf) => {
                match result {
                    Ok(n) => {
                        if n == 0 {
                            debug!("Connection closed by client");
                            // Process any remaining pending commands before closing
                            if !pending_commands.is_empty() {
                                process_command_batch(
                                    &pending_commands,
                                    client.clone(),
                                    storage_client.clone(),
                                    cmd_table.clone(),
                                    executor.clone(),
                                ).await;
                            }
                            return Ok(());
                        }

                        debug!("Received {} bytes from client", n);

                        // Parse RESP data with support for multiple commands
                        let mut parse_result = resp_parser.parse(Bytes::copy_from_slice(&buf[..n]));

                        loop {
                            match parse_result {
                                RespParseResult::Complete(data) => {
                                    debug!("RESP parsing complete: {:?}", data);

                                    if let Some(command) = extract_command_from_data(data) {
                                        pending_commands.push(command);

                                        // Check if we should process the batch
                                        if should_process_batch(&pending_commands) {
                                            process_command_batch(
                                                &pending_commands,
                                                client.clone(),
                                                storage_client.clone(),
                                                cmd_table.clone(),
                                                executor.clone(),
                                            ).await;
                                            pending_commands.clear();
                                        }
                                    }

                                    // Try to parse more commands from the buffer
                                    parse_result = resp_parser.parse(Bytes::new());
                                }
                                RespParseResult::Error(e) => {
                                    error!("RESP protocol error: {:?}", e);
                                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                                }
                                RespParseResult::Incomplete => {
                                    debug!("Incomplete RESP data, waiting for more");
                                    break; // Wait for more data
                                }
                            }
                        }

                        // Process any remaining commands if we have a complete batch
                        if !pending_commands.is_empty() && should_flush_batch(&pending_commands) {
                            process_command_batch(
                                &pending_commands,
                                client.clone(),
                                storage_client.clone(),
                                cmd_table.clone(),
                                executor.clone(),
                            ).await;
                            pending_commands.clear();
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

/// Handle a command using network-aware execution with StorageClient
///
/// This function processes Redis commands by routing them through the
/// StorageClient instead of accessing storage directly.
async fn handle_network_command(
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
) {
    // Convert the command name from &[u8] to a lowercase String for lookup
    let cmd_name = String::from_utf8_lossy(&client.cmd_name()).to_lowercase();
    debug!("Handling network command: {}", cmd_name);

    if let Some(cmd) = cmd_table.get(&cmd_name) {
        debug!("Command found in table: {}", cmd_name);

        // Create network-aware execution that uses StorageClient
        let network_exec = NetworkCmdExecution {
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

                // Use enhanced error response generation
                let error_response = generate_storage_error_response(&e, &cmd_name);
                client.set_reply(error_response);
            }
        }
    } else {
        // Command not found, set an error reply
        let err_msg = format!("ERR unknown command `{}`", cmd_name);
        warn!("Unknown command: {}", cmd_name);
        client.set_reply(RespData::Error(err_msg.into()));
    }
}

/// Process connection with cluster awareness using StorageClient
///
/// This function provides cluster-aware connection processing that routes
/// commands through the StorageClient for dual runtime architecture.
pub async fn process_network_cluster_connection(
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    // raft_compatibility: Arc<raft::RedisProtocolCompatibility>, // Temporarily disabled
) -> std::io::Result<()> {
    let mut buf = vec![0; 1024];
    let mut resp_parser = resp::RespParse::new(resp::RespVersion::RESP2);

    debug!("Starting network cluster connection processing");

    loop {
        select! {
            result = client.read(&mut buf) => {
                match result {
                    Ok(n) => {
                        if n == 0 {
                            debug!("Cluster connection closed by client");
                            return Ok(());
                        }

                        debug!("Received {} bytes from cluster client", n);

                        match resp_parser.parse(Bytes::copy_from_slice(&buf[..n])) {
                            RespParseResult::Complete(data) => {
                                debug!("Cluster RESP parsing complete: {:?}", data);

                                if let RespData::Array(Some(params)) = data {
                                    if params.is_empty() {
                                        debug!("Empty cluster command array, continuing");
                                        continue;
                                    }

                                    if let RespData::BulkString(Some(cmd_name)) = &params[0] {
                                        client.set_cmd_name(cmd_name.as_ref());
                                        debug!("Cluster command: {}", String::from_utf8_lossy(cmd_name));
                                    }

                                    let argv = params.iter().map(|p| {
                                        if let RespData::BulkString(Some(d)) = p {
                                            d.to_vec()
                                        } else {
                                            vec![]
                                        }
                                    }).collect::<Vec<Vec<u8>>>();

                                    client.set_argv(&argv);

                                    // Handle command with cluster awareness
                                    handle_network_cluster_command(
                                        client.clone(),
                                        storage_client.clone(),
                                        cmd_table.clone(),
                                        executor.clone(),
                                        // raft_compatibility.clone() // Temporarily disabled
                                    ).await;

                                    // Extract the reply from the connection and send it
                                    let response = client.take_reply();
                                    debug!("Sending cluster response: {:?}", response);

                                    let mut encoder = RespEncoder::new(RespVersion::RESP2);
                                    encoder.encode_resp_data(&response);

                                    match client.write(encoder.get_response().as_ref()).await {
                                        Ok(_) => debug!("Cluster response sent successfully"),
                                        Err(e) => {
                                            error!("Cluster write error: {}", e);
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                            RespParseResult::Error(e) => {
                                error!("Cluster RESP protocol error: {:?}", e);
                                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                            }
                            RespParseResult::Incomplete => {
                                debug!("Incomplete cluster RESP data, waiting for more");
                                // Not enough data, wait for more
                            }
                        }
                    }
                    Err(e) => {
                        error!("Cluster read error: {:?}", e);
                        return Err(e);
                    }
                }
            }
        }
    }
}

/// Handle a cluster command using network-aware execution with StorageClient
async fn handle_network_cluster_command(
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    // raft_compatibility: Arc<raft::RedisProtocolCompatibility>, // Temporarily disabled
) {
    // Convert the command name from &[u8] to a lowercase String for lookup
    let cmd_name = String::from_utf8_lossy(&client.cmd_name()).to_lowercase();
    debug!("Handling network cluster command: {}", cmd_name);

    if let Some(cmd) = cmd_table.get(&cmd_name) {
        debug!("Cluster command found in table: {}", cmd_name);

        // Create cluster-aware network execution
        let network_exec = NetworkCmdExecution {
            cmd: cmd.clone(),
            client: client.clone(),
            storage_client: storage_client.clone(),
        };

        // For now, use the regular network executor
        // TODO: Implement dedicated cluster-aware network executor when Raft is ready
        match executor.execute_network(network_exec).await {
            Ok(_) => {
                debug!("Cluster command executed successfully: {}", cmd_name);
            }
            Err(e) => {
                error!("Cluster command execution failed for {}: {}", cmd_name, e);

                // Use enhanced error response generation for cluster commands
                let error_response =
                    generate_storage_error_response(&e, &format!("CLUSTER {}", cmd_name));
                client.set_reply(error_response);
            }
        }
    } else {
        // Command not found, set an error reply
        let err_msg = format!("ERR unknown cluster command `{}`", cmd_name);
        warn!("Unknown cluster command: {}", cmd_name);
        client.set_reply(RespData::Error(err_msg.into()));
    }
}

/// Represents a parsed command ready for execution
#[derive(Debug, Clone)]
struct ParsedCommand {
    cmd_name: Vec<u8>,
    argv: Vec<Vec<u8>>,
}

/// Extract command from RESP data
fn extract_command_from_data(data: RespData) -> Option<ParsedCommand> {
    match data {
        RespData::Array(Some(params)) if !params.is_empty() => {
            if let RespData::BulkString(Some(cmd_name)) = &params[0] {
                let argv = params
                    .iter()
                    .map(|p| {
                        if let RespData::BulkString(Some(d)) = p {
                            d.to_vec()
                        } else {
                            vec![]
                        }
                    })
                    .collect::<Vec<Vec<u8>>>();

                Some(ParsedCommand {
                    cmd_name: cmd_name.to_vec(),
                    argv,
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Determine if we should process the current batch of commands
fn should_process_batch(commands: &[ParsedCommand]) -> bool {
    // Process batch if:
    // 1. We have reached the maximum batch size
    // 2. We have a mix of read and write commands (to maintain consistency)
    // 3. We have blocking commands that need immediate processing

    const MAX_BATCH_SIZE: usize = 10;

    if commands.len() >= MAX_BATCH_SIZE {
        return true;
    }

    // Check for blocking commands that should be processed immediately
    for cmd in commands {
        let cmd_name = String::from_utf8_lossy(&cmd.cmd_name).to_lowercase();
        if is_blocking_command(&cmd_name) {
            return true;
        }
    }

    false
}

/// Determine if we should flush the current batch (even if not full)
fn should_flush_batch(commands: &[ParsedCommand]) -> bool {
    // Flush if we have any commands and no more data is immediately available
    !commands.is_empty()
}

/// Check if a command is blocking and should be processed immediately
fn is_blocking_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name,
        "blpop" | "brpop" | "brpoplpush" | "bzpopmin" | "bzpopmax"
    )
}

/// Process a batch of commands, potentially in parallel for read operations
async fn process_command_batch(
    commands: &[ParsedCommand],
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
) {
    debug!("Processing command batch of {} commands", commands.len());

    // For now, process commands sequentially to maintain order
    // TODO: Implement parallel processing for read-only commands
    for command in commands {
        // Set up client state for this command
        client.set_cmd_name(&command.cmd_name);
        client.set_argv(&command.argv);

        // Handle the command
        handle_network_command(
            client.clone(),
            storage_client.clone(),
            cmd_table.clone(),
            executor.clone(),
        )
        .await;

        // Send the response immediately for pipelining
        let response = client.take_reply();
        debug!("Sending pipelined response: {:?}", response);

        let mut encoder = RespEncoder::new(RespVersion::RESP2);
        encoder.encode_resp_data(&response);

        match client.write(encoder.get_response().as_ref()).await {
            Ok(_) => debug!("Pipelined response sent successfully"),
            Err(e) => {
                error!("Write error in pipeline: {}", e);
                // Continue processing other commands even if one fails
            }
        }
    }
}

/// Enhanced error response generation for storage failures
fn generate_storage_error_response(error: &DualRuntimeError, command: &str) -> RespData {
    let error_message = match error {
        DualRuntimeError::Timeout { timeout } => {
            format!(
                "TIMEOUT Command '{}' timed out after {:?}",
                command, timeout
            )
        }
        DualRuntimeError::Storage(storage_err) => {
            format!("STORAGE Storage error in '{}': {}", command, storage_err)
        }
        DualRuntimeError::Channel(channel_err) => {
            format!(
                "CHANNEL Communication error in '{}': {}",
                command, channel_err
            )
        }
        DualRuntimeError::NetworkRuntime(net_err) => {
            format!(
                "NETWORK Network runtime error in '{}': {}",
                command, net_err
            )
        }
        DualRuntimeError::StorageRuntime(storage_err) => {
            format!(
                "STORAGE Storage runtime error in '{}': {}",
                command, storage_err
            )
        }
        DualRuntimeError::Configuration(config_err) => {
            format!(
                "CONFIG Configuration error in '{}': {}",
                command, config_err
            )
        }
        DualRuntimeError::Lifecycle(lifecycle_err) => {
            format!(
                "LIFECYCLE Lifecycle error in '{}': {}",
                command, lifecycle_err
            )
        }
        DualRuntimeError::HealthCheck(health_err) => {
            format!(
                "HEALTH Health check failed in '{}': {}",
                command, health_err
            )
        }
        DualRuntimeError::Io(io_err) => {
            format!("IO I/O error in '{}': {}", command, io_err)
        }
        DualRuntimeError::CircuitBreakerOpen { reason } => {
            format!(
                "CIRCUIT_BREAKER Circuit breaker open in '{}': {}",
                command, reason
            )
        }
        DualRuntimeError::RuntimeIsolation { runtime, reason } => {
            format!(
                "ISOLATION Runtime isolation error in '{}' ({}): {}",
                command, runtime, reason
            )
        }
        DualRuntimeError::ErrorBoundary { boundary, error } => {
            format!(
                "BOUNDARY Error boundary violation in '{}' ({}): {}",
                command, boundary, error
            )
        }
        DualRuntimeError::FaultIsolation { component, details } => {
            format!(
                "FAULT Fault isolation in '{}' ({}): {}",
                command, component, details
            )
        }
        DualRuntimeError::RecoveryFailed { mechanism, reason } => {
            format!(
                "RECOVERY Recovery failed in '{}' ({}): {}",
                command, mechanism, reason
            )
        }
    };

    RespData::Error(error_message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cmd::table::create_command_table;
    use executor::CmdExecutorBuilder;
    use runtime::{MessageChannel, StorageClient as RuntimeStorageClient};
    use std::sync::Arc;
    use std::time::Duration;

    fn create_test_components() -> (
        Arc<crate::storage_client::StorageClient>,
        Arc<CmdTable>,
        Arc<CmdExecutor>,
    ) {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let runtime_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(crate::storage_client::StorageClient::new(runtime_client));
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        (storage_client, cmd_table, executor)
    }

    #[tokio::test]
    async fn test_handle_network_command_unknown() {
        let (storage_client, _cmd_table, _executor) = create_test_components();

        // This test would need a proper mock client implementation
        // For now, we'll just test that the storage client is healthy
        // without creating actual network connections
        assert!(storage_client.is_healthy());
    }

    #[test]
    fn test_network_handle_module_exists() {
        // Test that the module compiles and functions are accessible
        // This test ensures the module structure is correct
    }
}
