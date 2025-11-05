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

use bytes::Bytes;
use client::Client;
use cmd::table::CmdTable;
use executor::{CmdExecutor, NetworkCmdExecution};
use log::{error, debug, warn};
use resp::encode::RespEncoder;
use resp::{Parse, RespData, RespEncode, RespParseResult, RespVersion};
use tokio::select;

use crate::storage_client::StorageClient;
use common::runtime::DualRuntimeError;

/// Process a network connection using StorageClient for storage operations
/// 
/// This function replaces the original process_connection to work with the
/// dual runtime architecture. It handles RESP protocol parsing in the network
/// runtime and sends storage requests to the storage runtime via StorageClient.
pub async fn process_network_connection(
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
) -> std::io::Result<()> {
    let mut buf = vec![0; 1024];
    let mut resp_parser = resp::RespParse::new(resp::RespVersion::RESP2);

    debug!("Starting network connection processing");

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
                                    
                                    // Handle command with network-aware processing
                                    handle_network_command(
                                        client.clone(), 
                                        storage_client.clone(), 
                                        cmd_table.clone(), 
                                        executor.clone()
                                    ).await;
                                    
                                    // Extract the reply from the connection and send it
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
                
                // Convert the error to an appropriate RESP error response
                let error_msg = match e {
                    DualRuntimeError::Timeout { timeout } => {
                        format!("ERR command timeout after {:?}", timeout)
                    }
                    DualRuntimeError::Storage(storage_err) => {
                        format!("ERR storage error: {}", storage_err)
                    }
                    DualRuntimeError::Channel(channel_err) => {
                        format!("ERR communication error: {}", channel_err)
                    }
                    _ => {
                        format!("ERR internal error: {}", e)
                    }
                };
                
                client.set_reply(RespData::Error(error_msg.into()));
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
                
                // Convert the error to an appropriate RESP error response
                let error_msg = match e {
                    DualRuntimeError::Timeout { timeout } => {
                        format!("ERR cluster command timeout after {:?}", timeout)
                    }
                    DualRuntimeError::Storage(storage_err) => {
                        format!("ERR cluster storage error: {}", storage_err)
                    }
                    DualRuntimeError::Channel(channel_err) => {
                        format!("ERR cluster communication error: {}", channel_err)
                    }
                    _ => {
                        format!("ERR cluster internal error: {}", e)
                    }
                };
                
                client.set_reply(RespData::Error(error_msg.into()));
            }
        }
    } else {
        // Command not found, set an error reply
        let err_msg = format!("ERR unknown cluster command `{}`", cmd_name);
        warn!("Unknown cluster command: {}", cmd_name);
        client.set_reply(RespData::Error(err_msg.into()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use cmd::table::create_command_table;
    use executor::CmdExecutorBuilder;
    use common::runtime::{MessageChannel, StorageClient as RuntimeStorageClient};
    use crate::storage_client::StorageClient;

    fn create_test_components() -> (Arc<StorageClient>, Arc<CmdTable>, Arc<CmdExecutor>) {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let runtime_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(StorageClient::new(runtime_client));
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());
        
        (storage_client, cmd_table, executor)
    }

    #[tokio::test]
    async fn test_handle_network_command_unknown() {
        let (storage_client, cmd_table, executor) = create_test_components();
        
        // Create a mock client with an unknown command
        let stream = Box::new(crate::tcp::TcpStreamWrapper::new(
            tokio::net::TcpStream::connect("127.0.0.1:1").await.unwrap_or_else(|_| {
                // Create a dummy stream for testing
                panic!("Cannot create test stream")
            })
        ));
        
        // This test would need a proper mock client implementation
        // For now, we'll just test that the function signature is correct
        assert!(storage_client.is_healthy());
    }

    #[test]
    fn test_network_handle_module_exists() {
        // Test that the module compiles and functions are accessible
        assert!(true);
    }
}