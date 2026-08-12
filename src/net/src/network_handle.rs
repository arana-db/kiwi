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
use cmd::CmdFlags;
use cmd::table::CmdTable;
use cmd::vector::admission::VectorAdmissionLimits;
use executor::CmdExecutor;
use log::{debug, error, warn};
use resp::encode::RespEncoder;
use resp::{Parse, RespData, RespEncode, RespParseResult};
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::executor_ext::CmdExecutorNetworkExt;
use crate::storage_client::StorageClient;
use runtime::DualRuntimeError;

pub(crate) fn unauthenticated_buffer_limit_exceeded(
    is_authenticated: bool,
    buffered_len: usize,
    incoming_len: usize,
) -> bool {
    !is_authenticated
        && buffered_len
            .checked_add(incoming_len)
            .is_none_or(|len| len > resp::parse::MAX_UNAUTHENTICATED_BUFFER_SIZE)
}

pub(crate) fn discard_legacy_parsed_command(parser: &mut resp::RespParse) {
    drop(parser.next_command());
}

pub(crate) fn parse_request(parser: &mut resp::RespParse, data: Bytes) -> RespParseResult {
    let result = parser.parse(data);
    if matches!(result, RespParseResult::Complete(_)) {
        discard_legacy_parsed_command(parser);
    }
    result
}

pub(crate) fn parse_client_request(
    parser: &mut resp::RespParse,
    is_authenticated: bool,
    data: Bytes,
) -> std::io::Result<RespParseResult> {
    if unauthenticated_buffer_limit_exceeded(is_authenticated, parser.buffered_len(), data.len()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unauthenticated RESP buffer limit exceeded",
        ));
    }

    Ok(parse_request(parser, data))
}

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
    vector_admission_limits: VectorAdmissionLimits,
    leader_gate: Option<std::sync::Arc<dyn raft::leader_gate::LeaderGate>>,
) -> std::io::Result<()> {
    process_network_connection_until_cancelled(
        client,
        storage_client,
        cmd_table,
        executor,
        vector_admission_limits,
        leader_gate,
        CancellationToken::new(),
    )
    .await
}

/// Process a network connection until cancellation is observed at a safe read
/// boundary. Once a read completes, every command from that read is allowed to
/// reach a terminal result before cancellation is checked again.
pub async fn process_network_connection_until_cancelled(
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    vector_admission_limits: VectorAdmissionLimits,
    leader_gate: Option<std::sync::Arc<dyn raft::leader_gate::LeaderGate>>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let mut buf = vec![0; 4096]; // Increased buffer size for better performance
    let mut resp_parser = resp::RespParse::new(client.resp_version());
    let mut pending_commands = Vec::new();

    debug!("Starting network connection processing with pipelining support");

    loop {
        select! {
            biased;

            _ = shutdown.cancelled() => {
                debug!("Connection cancelled before next read");
                return Ok(());
            }
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
                                    vector_admission_limits,
                                    leader_gate.clone(),
                                    &shutdown,
                                ).await;
                            }
                            return Ok(());
                        }

                        debug!("Received {} bytes from client", n);
                        let mut shutdown_after_current_read = false;

                        // Parse RESP data with support for multiple commands
                        let mut parse_result = parse_client_request(
                            &mut resp_parser,
                            client.is_authenticated(),
                            Bytes::copy_from_slice(&buf[..n]),
                        )?;

                        loop {
                            match parse_result {
                                RespParseResult::Complete(data) => {
                                    debug!("RESP parsing complete: {:?}", data);
                                    if let Some(command) = extract_command_from_data(data) {
                                        pending_commands.push(command);

                                        // Check if we should process the batch
                                        if should_process_batch(&pending_commands) {
                                            shutdown_after_current_read |= process_command_batch(
                                                &pending_commands,
                                                client.clone(),
                                                storage_client.clone(),
                                                cmd_table.clone(),
                                                executor.clone(),
                                                vector_admission_limits,
                                                leader_gate.clone(),
                                                &shutdown,
                                            ).await;
                                            pending_commands.clear();
                                        }
                                    }

                                    // Try to parse more commands from the buffer
                                    parse_result = parse_client_request(
                                        &mut resp_parser,
                                        client.is_authenticated(),
                                        Bytes::new(),
                                    )?;
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
                            shutdown_after_current_read |= process_command_batch(
                                &pending_commands,
                                client.clone(),
                                storage_client.clone(),
                                cmd_table.clone(),
                                executor.clone(),
                                vector_admission_limits,
                                leader_gate.clone(),
                                &shutdown,
                            ).await;
                            pending_commands.clear();
                        }

                        if shutdown_after_current_read || shutdown.is_cancelled() {
                            debug!("Connection cancelled after completing the current socket read");
                            return Ok(());
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
    leader_gate: Option<std::sync::Arc<dyn raft::leader_gate::LeaderGate>>,
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
            leader_gate: leader_gate.clone(),
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

/// Represents a parsed command ready for execution
#[derive(Debug, Clone)]
struct ParsedCommand {
    cmd_name: Bytes,
    argv: Vec<Bytes>,
}

/// Extract command from RESP data
fn extract_command_from_data(data: RespData) -> Option<ParsedCommand> {
    match data {
        RespData::Array(Some(params)) if !params.is_empty() => {
            let mut params = params.into_iter();
            let RespData::BulkString(Some(cmd_name)) = params.next()? else {
                return None;
            };
            let mut argv = Vec::with_capacity(params.len() + 1);
            argv.push(cmd_name.clone());
            argv.extend(params.map(|param| match param {
                RespData::BulkString(Some(argument)) => argument,
                _ => Bytes::new(),
            }));

            Some(ParsedCommand { cmd_name, argv })
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

/// Check if a command is a read operation
pub fn is_read_command(cmd: &str) -> bool {
    let cmd_lower = cmd.to_lowercase();
    matches!(
        cmd_lower.as_str(),
        "get"
            | "mget"
            | "strlen"
            | "getrange"
            | "lindex"
            | "llen"
            | "lrange"
            | "scard"
            | "sismember"
            | "smembers"
            | "zscore"
            | "zrank"
            | "zrange"
            | "hlen"
            | "hexists"
            | "hget"
            | "hgetall"
            | "hkeys"
            | "hvals"
            | "type"
            | "ttl"
            | "pttl"
            | "exists"
            | "ping"
    )
}

/// Check if a command is a write operation
pub fn is_write_command(cmd: &str) -> bool {
    let cmd_lower = cmd.to_lowercase();
    matches!(
        cmd_lower.as_str(),
        "set"
            | "del"
            | "mset"
            | "incr"
            | "decr"
            | "incrby"
            | "decrby"
            | "append"
            | "setrange"
            | "lpush"
            | "rpush"
            | "lpop"
            | "rpop"
            | "lset"
            | "lrem"
            | "ltrim"
            | "sadd"
            | "srem"
            | "spop"
            | "smove"
            | "zadd"
            | "zrem"
            | "zincrby"
            | "zpopmin"
            | "zpopmax"
            | "hset"
            | "hdel"
            | "hincrby"
            | "hincrbyfloat"
            | "expire"
            | "pexpire"
            | "persist"
            | "expireat"
    )
}

/// Process every command in a batch to a terminal result. If shutdown interrupts
/// a response write, subsequent responses are suppressed and `true` is returned.
async fn process_command_batch(
    commands: &[ParsedCommand],
    client: Arc<Client>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    vector_admission_limits: VectorAdmissionLimits,
    leader_gate: Option<std::sync::Arc<dyn raft::leader_gate::LeaderGate>>,
    shutdown: &CancellationToken,
) -> bool {
    debug!("Processing command batch of {} commands", commands.len());
    let mut response_writes_cancelled = false;

    // For now, process commands sequentially to maintain order
    // TODO: Implement parallel processing for read-only commands
    for command in commands {
        let cmd_name = String::from_utf8_lossy(command.cmd_name.as_ref()).to_lowercase();
        let mut admitted = false;

        if let Some(cmd) = cmd_table.get(&cmd_name) {
            if !client.is_authenticated() && !cmd.has_flag(CmdFlags::NO_AUTH) {
                client.set_reply(RespData::Error("NOAUTH Authentication required.".into()));
            } else if !cmd.check_arg(command.argv.len()) {
                client.set_reply(cmd.wrong_arity_reply(cmd_name.as_bytes()));
            } else if let Err(reply) =
                cmd.admit_network_request(&command.argv, vector_admission_limits)
            {
                client.set_reply(reply);
            } else {
                admitted = true;
            }
        } else {
            warn!("Unknown command: {}", cmd_name);
            client.set_reply(RespData::Error(
                format!("ERR unknown command `{}`", cmd_name).into(),
            ));
        }

        if admitted {
            let argv = command
                .argv
                .iter()
                .map(|argument| argument.to_vec())
                .collect::<Vec<Vec<u8>>>();
            client.set_cmd_name(command.cmd_name.as_ref());
            client.set_argv(&argv);
            handle_network_command(
                client.clone(),
                storage_client.clone(),
                cmd_table.clone(),
                executor.clone(),
                leader_gate.clone(),
            )
            .await;
        }

        // Send the response immediately for pipelining
        let response = client.take_reply();
        debug!("Sending pipelined response: {:?}", response);

        let encoder_version = client.resp_version();
        let mut encoder = RespEncoder::new(encoder_version);
        encoder.encode_resp_data(&response);
        let encoded_response = encoder.get_response();

        if response_writes_cancelled {
            continue;
        }

        let write_result = tokio::select! {
            biased;

            _ = shutdown.cancelled() => {
                response_writes_cancelled = true;
                None
            }
            result = client.write(encoded_response.as_ref()) => Some(result),
        };
        let Some(write_result) = write_result else {
            continue;
        };

        match write_result {
            Ok(_) => debug!("Pipelined response sent successfully"),
            Err(e) => {
                error!("Write error in pipeline: {}", e);
                // Continue processing other commands even if one fails
            }
        }
    }

    response_writes_cancelled
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

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use client::StreamTrait;
    use cmd::table::create_command_table;
    use cmd::{Cmd, CmdMeta};
    use executor::CmdExecutorBuilder;
    use runtime::{MessageChannel, StorageClient as RuntimeStorageClient};
    use std::future::pending;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Semaphore;

    struct BlockingWriteStream {
        inbound: Option<Vec<u8>>,
        write_started: Arc<Semaphore>,
    }

    #[async_trait]
    impl StreamTrait for BlockingWriteStream {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
            let Some(inbound) = self.inbound.take() else {
                return pending::<Result<usize, std::io::Error>>().await;
            };
            assert!(
                inbound.len() <= buf.len(),
                "test payload exceeds read buffer"
            );
            buf[..inbound.len()].copy_from_slice(&inbound);
            Ok(inbound.len())
        }

        async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
            self.write_started.add_permits(1);
            pending::<Result<usize, std::io::Error>>().await
        }
    }

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
        let cmd_table = Arc::new(create_command_table(Arc::new(|| None)));
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        (storage_client, cmd_table, executor)
    }

    struct RecordingWriteStream {
        writes: Arc<Mutex<Vec<u8>>>,
    }

    #[async_trait]
    impl StreamTrait for RecordingWriteStream {
        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
            pending::<Result<usize, std::io::Error>>().await
        }

        async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
            self.writes.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
    }

    #[derive(Clone)]
    struct RejectingAdmissionCmd {
        meta: CmdMeta,
        client: Arc<Client>,
        expected_payload_pointer: usize,
    }

    impl Cmd for RejectingAdmissionCmd {
        fn meta(&self) -> &CmdMeta {
            &self.meta
        }

        fn admit_network_request(
            &self,
            argv: &[Bytes],
            _limits: VectorAdmissionLimits,
        ) -> Result<(), RespData> {
            assert_eq!(
                self.client.argv(),
                vec![b"sentinel-before-admission".to_vec()],
                "Client argv was replaced before network admission"
            );
            assert_eq!(
                argv[1].as_ptr() as usize,
                self.expected_payload_pointer,
                "network admission did not receive the original RESP Bytes allocation"
            );
            Err(RespData::Error(
                "ERR injected network admission rejection".into(),
            ))
        }

        fn do_initial(&self, _client: &Client) -> bool {
            panic!("rejected command must not reach do_initial")
        }

        fn do_cmd(&self, _client: &Client, _storage: Arc<storage::storage::Storage>) {
            panic!("rejected command must not reach storage")
        }

        fn clone_box(&self) -> Box<dyn Cmd> {
            Box::new(self.clone())
        }
    }

    fn test_vector_admission_limits() -> VectorAdmissionLimits {
        VectorAdmissionLimits {
            max_dimension: 4096,
            max_element_bytes: 1_048_576,
            max_vector_bytes: 16_777_216,
        }
    }

    #[test]
    fn parsed_command_keeps_bulk_bytes_until_admission() {
        fn assert_bytes(_: &Bytes) {}

        let payload = Bytes::from(vec![b'x'; 32]);
        let payload_pointer = payload.as_ptr();

        let command = extract_command_from_data(RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from_static(b"VADD"))),
            RespData::BulkString(Some(Bytes::from_static(b"vectors"))),
            RespData::BulkString(Some(payload)),
        ])))
        .expect("bulk-string command should parse");

        assert_bytes(&command.cmd_name);
        for argument in &command.argv {
            assert_bytes(argument);
        }
        assert_eq!(command.cmd_name, Bytes::from_static(b"VADD"));
        assert_eq!(command.argv[1], Bytes::from_static(b"vectors"));
        assert_eq!(
            command.argv[2].as_ptr(),
            payload_pointer,
            "parsed bulk payload must retain the RESP Bytes backing allocation"
        );
    }

    #[test]
    fn non_bulk_arguments_preserve_existing_empty_argument_semantics() {
        fn assert_bytes(_: &Bytes) {}

        let command = extract_command_from_data(RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from_static(b"PING"))),
            RespData::Integer(1),
            RespData::BulkString(None),
        ])))
        .expect("bulk-string command name should parse");

        assert_eq!(command.argv.len(), 3);
        assert_bytes(&command.argv[1]);
        assert_bytes(&command.argv[2]);
        assert!(command.argv[1].is_empty());
        assert!(command.argv[2].is_empty());
    }

    #[tokio::test]
    async fn parsed_command_admission_precedes_client_payload_copy() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(Client::new(Box::new(RecordingWriteStream {
            writes: writes.clone(),
        })));
        client.set_argv(&[b"sentinel-before-admission".to_vec()]);

        let payload = Bytes::from(vec![b'x'; 32]);
        let expected_payload_pointer = payload.as_ptr() as usize;
        let command = ParsedCommand {
            cmd_name: Bytes::from_static(b"VPROBE"),
            argv: vec![Bytes::from_static(b"VPROBE"), payload],
        };
        let rejecting_cmd = RejectingAdmissionCmd {
            meta: CmdMeta {
                name: "vprobe".to_string(),
                arity: 2,
                flags: CmdFlags::NO_AUTH,
                ..Default::default()
            },
            client: client.clone(),
            expected_payload_pointer,
        };
        let mut cmd_table = CmdTable::new();
        cmd_table.insert("vprobe".to_string(), Arc::new(rejecting_cmd));
        let (storage_client, _, executor) = create_test_components();

        let response_writes_cancelled = process_command_batch(
            &[command],
            client.clone(),
            storage_client,
            Arc::new(cmd_table),
            executor,
            test_vector_admission_limits(),
            None,
            &CancellationToken::new(),
        )
        .await;

        assert!(!response_writes_cancelled);
        assert_eq!(
            client.argv(),
            vec![b"sentinel-before-admission".to_vec()],
            "rejected request must not cross the Bytes-to-Vec copy boundary"
        );
        assert_eq!(
            writes.lock().unwrap().as_slice(),
            b"-ERR injected network admission rejection\r\n"
        );
    }

    #[tokio::test]
    async fn test_handle_network_command_unknown() {
        let (storage_client, _cmd_table, _executor) = create_test_components();

        // This test would need a proper mock client implementation
        // For now, we'll just test that the storage client is healthy
        // without creating actual network connections
        assert!(storage_client.is_healthy());
    }

    #[tokio::test]
    async fn shutdown_unblocks_response_write_and_finishes_commands_from_same_read() {
        let write_started = Arc::new(Semaphore::new(0));
        let client = Arc::new(Client::new(Box::new(BlockingWriteStream {
            inbound: Some(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n".to_vec()),
            write_started: write_started.clone(),
        })));
        let (storage_client, _cmd_table, executor) = create_test_components();
        let cmd_table = Arc::new(create_command_table(Arc::new(|| {
            Some("secret".to_string())
        })));
        let shutdown = CancellationToken::new();
        let connection_task = tokio::spawn(process_network_connection_until_cancelled(
            client.clone(),
            storage_client,
            cmd_table,
            executor,
            test_vector_admission_limits(),
            None,
            shutdown.clone(),
        ));

        let write_permit = tokio::time::timeout(Duration::from_secs(1), write_started.acquire())
            .await
            .expect("first response write did not start")
            .expect("write-start semaphore closed");
        drop(write_permit);
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(1), connection_task)
            .await
            .expect("connection stayed blocked in response write after shutdown")
            .expect("connection task panicked")
            .expect("connection processing failed");
        assert!(
            client.is_authenticated(),
            "AUTH from the same socket read did not reach a terminal result"
        );
    }

    #[tokio::test]
    async fn shutdown_unblocks_noauth_write_and_finishes_commands_from_same_read() {
        let write_started = Arc::new(Semaphore::new(0));
        let client = Arc::new(Client::new(Box::new(BlockingWriteStream {
            inbound: Some(
                b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n".to_vec(),
            ),
            write_started: write_started.clone(),
        })));
        let (storage_client, _cmd_table, executor) = create_test_components();
        let cmd_table = Arc::new(create_command_table(Arc::new(|| {
            Some("secret".to_string())
        })));
        let shutdown = CancellationToken::new();
        let connection_task = tokio::spawn(process_network_connection_until_cancelled(
            client.clone(),
            storage_client,
            cmd_table,
            executor,
            test_vector_admission_limits(),
            None,
            shutdown.clone(),
        ));

        let write_permit = tokio::time::timeout(Duration::from_secs(1), write_started.acquire())
            .await
            .expect("NOAUTH response write did not start")
            .expect("write-start semaphore closed");
        drop(write_permit);
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(1), connection_task)
            .await
            .expect("connection stayed blocked in NOAUTH response write after shutdown")
            .expect("connection task panicked")
            .expect("connection processing failed");
        assert!(
            client.is_authenticated(),
            "AUTH after the rejected command did not reach a terminal result"
        );
    }

    #[test]
    fn test_network_handle_module_exists() {
        // Test that the module compiles and functions are accessible
        // This test ensures the module structure is correct
    }

    #[test]
    fn unauthenticated_buffer_limit_checks_boundaries_and_overflow() {
        let limit = resp::parse::MAX_UNAUTHENTICATED_BUFFER_SIZE;

        assert!(!unauthenticated_buffer_limit_exceeded(false, limit - 1, 1));
        assert!(unauthenticated_buffer_limit_exceeded(false, limit, 1));
        assert!(unauthenticated_buffer_limit_exceeded(false, usize::MAX, 1));
        assert!(!unauthenticated_buffer_limit_exceeded(true, usize::MAX, 1));
    }

    #[test]
    fn active_consumer_discards_legacy_parsed_command_copy() {
        let mut parser = resp::RespParse::new(resp::RespVersion::RESP2);

        assert!(matches!(
            parse_client_request(
                &mut parser,
                false,
                Bytes::from_static(b"*1\r\n$4\r\nPING\r\n")
            ),
            Ok(RespParseResult::Complete(_))
        ));
        assert!(parser.next_command().is_none());
    }
}
