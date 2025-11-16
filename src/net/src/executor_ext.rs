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
use std::time::Duration;

use executor::CmdExecutor;
use log::{debug, error, warn};
use resp::RespData;
use runtime::DualRuntimeError;

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

            // Execute do_initial if needed
            if !exec.cmd.do_initial(&exec.client) {
                debug!("Command initial check failed for: {}", cmd_name);
                return Ok(());
            }

            // Route command to appropriate async storage operation
            match cmd_name.as_str() {
                "get" => {
                    execute_get_command(&exec).await?;
                }
                "set" => {
                    execute_set_command(&exec).await?;
                }
                "del" => {
                    execute_del_command(&exec).await?;
                }
                "exists" => {
                    execute_exists_command(&exec).await?;
                }
                "expire" => {
                    execute_expire_command(&exec).await?;
                }
                "ttl" => {
                    execute_ttl_command(&exec).await?;
                }
                "incr" => {
                    execute_incr_command(&exec).await?;
                }
                "incrby" => {
                    execute_incrby_command(&exec).await?;
                }
                "decr" => {
                    execute_decr_command(&exec).await?;
                }
                "decrby" => {
                    execute_decrby_command(&exec).await?;
                }
                "mget" => {
                    execute_mget_command(&exec).await?;
                }
                "mset" => {
                    execute_mset_command(&exec).await?;
                }
                "ping" => {
                    execute_ping_command(&exec).await?;
                }
                _ => {
                    // For unsupported commands, return an error
                    let error_msg = format!(
                        "ERR command '{}' not supported in dual runtime mode",
                        cmd_name
                    );
                    warn!("Unsupported network command: {}", cmd_name);
                    exec.client.set_reply(RespData::Error(error_msg.into()));
                }
            }

            Ok(())
        })
    }
}

/// Execute GET command asynchronously
async fn execute_get_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let key = &exec.client.argv()[1];
    debug!("Executing GET for key: {:?}", String::from_utf8_lossy(key));

    match exec.storage_client.get(key).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("GET command failed: {}", e);
            let error_msg = format_storage_error("GET", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute SET command asynchronously
async fn execute_set_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let key = &exec.client.argv()[1];
    let value = &exec.client.argv()[2];
    debug!("Executing SET for key: {:?}", String::from_utf8_lossy(key));

    // TODO: Parse SET options (EX, PX, NX, XX)
    let ttl = None;

    match exec.storage_client.set(key, value, ttl).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("SET command failed: {}", e);
            let error_msg = format_storage_error("SET", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute DEL command asynchronously
async fn execute_del_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let keys: Vec<Vec<u8>> = exec.client.argv()[1..].to_vec();
    debug!("Executing DEL for {} keys", keys.len());

    match exec.storage_client.del(&keys).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("DEL command failed: {}", e);
            let error_msg = format_storage_error("DEL", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute EXISTS command asynchronously
async fn execute_exists_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let keys: Vec<Vec<u8>> = exec.client.argv()[1..].to_vec();
    debug!("Executing EXISTS for {} keys", keys.len());

    match exec.storage_client.exists(&keys).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("EXISTS command failed: {}", e);
            let error_msg = format_storage_error("EXISTS", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute EXPIRE command asynchronously
async fn execute_expire_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let key = &exec.client.argv()[1];
    let argv2 = exec.client.argv()[2].clone();
    let seconds_str = String::from_utf8_lossy(&argv2);

    match seconds_str.parse::<u64>() {
        Ok(seconds) => {
            let ttl = Duration::from_secs(seconds);
            debug!(
                "Executing EXPIRE for key: {:?}, ttl: {:?}",
                String::from_utf8_lossy(key),
                ttl
            );

            match exec.storage_client.expire(key, ttl).await {
                Ok(response) => {
                    exec.client.set_reply(response);
                }
                Err(e) => {
                    error!("EXPIRE command failed: {}", e);
                    let error_msg = format_storage_error("EXPIRE", &e);
                    exec.client.set_reply(RespData::Error(error_msg.into()));
                }
            }
        }
        Err(_) => {
            exec.client.set_reply(RespData::Error(
                "ERR value is not an integer or out of range".into(),
            ));
        }
    }
    Ok(())
}

/// Execute TTL command asynchronously
async fn execute_ttl_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let key = &exec.client.argv()[1];
    debug!("Executing TTL for key: {:?}", String::from_utf8_lossy(key));

    match exec.storage_client.ttl(key).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("TTL command failed: {}", e);
            let error_msg = format_storage_error("TTL", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute INCR command asynchronously
async fn execute_incr_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let key = &exec.client.argv()[1];
    debug!("Executing INCR for key: {:?}", String::from_utf8_lossy(key));

    match exec.storage_client.incr(key).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("INCR command failed: {}", e);
            let error_msg = format_storage_error("INCR", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute INCRBY command asynchronously
async fn execute_incrby_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let key = &exec.client.argv()[1];
    let argv2 = exec.client.argv()[2].clone();
    let increment_str = String::from_utf8_lossy(&argv2);

    match increment_str.parse::<i64>() {
        Ok(increment) => {
            debug!(
                "Executing INCRBY for key: {:?}, increment: {}",
                String::from_utf8_lossy(key),
                increment
            );

            match exec.storage_client.incr_by(key, increment).await {
                Ok(response) => {
                    exec.client.set_reply(response);
                }
                Err(e) => {
                    error!("INCRBY command failed: {}", e);
                    let error_msg = format_storage_error("INCRBY", &e);
                    exec.client.set_reply(RespData::Error(error_msg.into()));
                }
            }
        }
        Err(_) => {
            exec.client.set_reply(RespData::Error(
                "ERR value is not an integer or out of range".into(),
            ));
        }
    }
    Ok(())
}

/// Execute DECR command asynchronously
async fn execute_decr_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let key = &exec.client.argv()[1];
    debug!("Executing DECR for key: {:?}", String::from_utf8_lossy(key));

    match exec.storage_client.decr(key).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("DECR command failed: {}", e);
            let error_msg = format_storage_error("DECR", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute DECRBY command asynchronously
async fn execute_decrby_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let key = &exec.client.argv()[1];
    let argv2 = exec.client.argv()[2].clone();
    let decrement_str = String::from_utf8_lossy(&argv2);

    match decrement_str.parse::<i64>() {
        Ok(decrement) => {
            debug!(
                "Executing DECRBY for key: {:?}, decrement: {}",
                String::from_utf8_lossy(key),
                decrement
            );

            match exec.storage_client.decr_by(key, decrement).await {
                Ok(response) => {
                    exec.client.set_reply(response);
                }
                Err(e) => {
                    error!("DECRBY command failed: {}", e);
                    let error_msg = format_storage_error("DECRBY", &e);
                    exec.client.set_reply(RespData::Error(error_msg.into()));
                }
            }
        }
        Err(_) => {
            exec.client.set_reply(RespData::Error(
                "ERR value is not an integer or out of range".into(),
            ));
        }
    }
    Ok(())
}

/// Execute MGET command asynchronously
async fn execute_mget_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let keys: Vec<Vec<u8>> = exec.client.argv()[1..].to_vec();
    debug!("Executing MGET for {} keys", keys.len());

    match exec.storage_client.mget(&keys).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("MGET command failed: {}", e);
            let error_msg = format_storage_error("MGET", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute MSET command asynchronously
async fn execute_mset_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    let argv = exec.client.argv();

    // MSET requires an even number of arguments (key-value pairs)
    if argv.len() < 3 || !(argv.len() - 1).is_multiple_of(2) {
        exec.client.set_reply(RespData::Error(
            "ERR wrong number of arguments for 'mset' command".into(),
        ));
        return Ok(());
    }

    let mut pairs = Vec::new();
    for i in (1..argv.len()).step_by(2) {
        pairs.push((argv[i].clone(), argv[i + 1].clone()));
    }

    debug!("Executing MSET for {} pairs", pairs.len());

    match exec.storage_client.mset(&pairs).await {
        Ok(response) => {
            exec.client.set_reply(response);
        }
        Err(e) => {
            error!("MSET command failed: {}", e);
            let error_msg = format_storage_error("MSET", &e);
            exec.client.set_reply(RespData::Error(error_msg.into()));
        }
    }
    Ok(())
}

/// Execute PING command asynchronously (doesn't require storage)
async fn execute_ping_command(exec: &NetworkCmdExecution) -> Result<(), DualRuntimeError> {
    debug!("Executing PING command");

    if exec.client.argv().len() == 1 {
        exec.client.set_reply(RespData::SimpleString("PONG".into()));
    } else if exec.client.argv().len() == 2 {
        let arg = exec.client.argv()[1].clone();
        exec.client
            .set_reply(RespData::BulkString(Some(arg.into())));
    } else {
        exec.client.set_reply(RespData::Error(
            "ERR wrong number of arguments for 'ping' command".into(),
        ));
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
