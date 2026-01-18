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

pub mod async_resp_parser;
pub mod buffer;
pub mod executor_ext;
pub mod handle;
pub mod network_execution;
pub mod network_handle;
pub mod network_server;
pub mod optimized_handler;
pub mod pipeline;
pub mod pool;
pub mod storage_client;
pub mod tcp;

// TODO: delete this module
pub mod error;
pub mod unix;

use std::error::Error;

use async_trait::async_trait;

use crate::network_server::NetworkServer;
use crate::storage_client::StorageClient;
use crate::tcp::TcpServer;
use cmd::table::create_command_table;
use executor::CmdExecutorBuilder;
use runtime::RuntimeManager;
use std::sync::Arc;

#[async_trait]
pub trait ServerTrait: Send + Sync + 'static {
    async fn run(&self) -> Result<(), Box<dyn Error>>;
}

pub struct ServerFactory;

impl ServerFactory {
    pub fn create_server(
        protocol: &str,
        addr: Option<String>,
        runtime_manager: &RuntimeManager,
    ) -> Option<Box<dyn ServerTrait>> {
        match protocol.to_lowercase().as_str() {
            "tcp" => match Self::create_network_server(addr, runtime_manager) {
                Ok(server) => Some(Box::new(server) as Box<dyn ServerTrait>),
                Err(e) => {
                    log::error!("Failed to create NetworkServer: {}", e);
                    None
                }
            },
            #[cfg(unix)]
            "unix" => Some(Box::new(unix::UnixServer::new(addr))),
            #[cfg(not(unix))]
            "unix" => None,
            _ => None,
        }
    }

    /// Create a legacy server without dual runtime architecture (for backward compatibility)
    pub fn create_legacy_server(
        protocol: &str,
        addr: Option<String>,
    ) -> Option<Box<dyn ServerTrait>> {
        match protocol.to_lowercase().as_str() {
            "tcp" => TcpServer::new(addr)
                .ok()
                .map(|s| Box::new(s) as Box<dyn ServerTrait>),
            #[cfg(unix)]
            "unix" => Some(Box::new(unix::UnixServer::new(addr))),
            #[cfg(not(unix))]
            "unix" => None,
            _ => None,
        }
    }

    /// Create a NetworkServer with dual runtime architecture
    fn create_network_server(
        addr: Option<String>,
        runtime_manager: &RuntimeManager,
    ) -> Result<NetworkServer, Box<dyn std::error::Error>> {
        // Get the storage client from RuntimeManager
        let runtime_storage_client = runtime_manager.storage_client().map_err(|e| {
            format!(
                "Storage client not initialized. Make sure RuntimeManager::initialize_storage_components() was called first: {}",
                e
            )
        })?;

        // Wrap the runtime storage client in the network-side StorageClient
        let storage_client = Arc::new(StorageClient::new(runtime_storage_client));

        // Create command table and executor
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        Ok(NetworkServer::new(
            addr,
            storage_client,
            cmd_table,
            executor,
        )?)
    }
}
