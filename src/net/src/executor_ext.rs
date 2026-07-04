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

//! Executor extensions for network operations
//!
//! This module provides extensions to CmdExecutor to support network-aware
//! command execution with StorageClient for async storage operations.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cmd::CmdFlags;
use executor::CmdExecutor;
use log::{debug, error};
use resp::RespData;
use runtime::DualRuntimeError;
use storage::storage::Storage;

use crate::network_execution::NetworkCmdExecution;

/// Extension trait for CmdExecutor to support network operations
pub trait CmdExecutorNetworkExt {
    /// Execute a network command using StorageClient for dual runtime architecture
    fn execute_network(
        &self,
        exec: NetworkCmdExecution,
    ) -> Pin<Box<dyn Future<Output = Result<(), DualRuntimeError>> + Send + '_>>;
}

impl CmdExecutorNetworkExt for CmdExecutor {
    fn execute_network(
        &self,
        exec: NetworkCmdExecution,
    ) -> Pin<Box<dyn Future<Output = Result<(), DualRuntimeError>> + Send + '_>> {
        Box::pin(async move {
            let cmd_name = String::from_utf8_lossy(&exec.client.cmd_name()).to_lowercase();
            debug!("Executing network command: {}", cmd_name);

            // Check argument count first
            let argv = exec.client.argv();
            if !exec.cmd.check_arg(argv.len()) {
                let error_msg = format!("ERR wrong number of arguments for '{}' command", cmd_name);
                exec.client.set_reply(RespData::Error(error_msg.into()));
                return Ok(());
            }

            // Cluster-mode leader gate: reject writes on non-leaders before any
            // command-specific setup runs.
            if let Some(gate) = exec.leader_gate.as_ref() {
                if exec.cmd.has_flag(CmdFlags::WRITE) && !gate.is_leader() {
                    // Simplified redirect: Kiwi returns "MOVED <addr>" (no hash slot,
                    // unlike Redis Cluster's "MOVED <slot> <ip:port>"). Clients are
                    // expected to reconnect to the returned leader address directly.
                    let reply = match gate.leader_resp_addr() {
                        Some(addr) => format!("MOVED {addr}"),
                        None => "ERR not leader".to_string(),
                    };
                    exec.client.set_reply(RespData::Error(reply.into()));
                    return Ok(());
                }
            }

            // Execute do_initial if needed
            if !exec.cmd.do_initial(&exec.client) {
                debug!("Command initial check failed for: {}", cmd_name);
                return Ok(());
            }

            // Route connection-local commands locally and send all other
            // storage-backed commands through the generic Execute path.
            match cmd_name.as_str() {
                "ping" | "auth" | "client" => {
                    execute_local_command(&exec).await?;
                }
                _ => {
                    execute_generic_command(&exec).await?;
                }
            }

            Ok(())
        })
    }
}

async fn execute_local_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let storage = Arc::new(Storage::new(1, 0));
    exec.cmd.do_cmd(exec.client.as_ref(), storage);
    Ok(())
}

async fn execute_generic_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let cmd_name = exec.client.cmd_name();
    let argv = exec.client.argv();

    match exec
        .storage_client
        .execute_command(cmd_name.as_slice(), &argv)
        .await
    {
        Ok(response) => exec.client.set_reply(response),
        Err(e) => {
            error!("Generic command execution failed: {}", e);
            let error_msg = format_storage_error("EXECUTE", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }

    Ok(())
}

/// Format storage error for RESP response
fn format_storage_error(command: &str, error: &DualRuntimeError) -> String {
    match error {
        DualRuntimeError::Timeout { timeout } => {
            format!("ERR {} command timeout after {:?}", command, timeout)
        }
        DualRuntimeError::Storage(storage_err) => {
            format!("ERR storage error in {}: {}", command, storage_err)
        }
        DualRuntimeError::Channel(channel_err) => {
            format!("ERR communication error in {}: {}", command, channel_err)
        }
        _ => {
            format!("ERR internal error in {}: {}", command, error)
        }
    }
}
