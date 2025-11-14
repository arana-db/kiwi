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
pub mod raft_network_handle;
pub mod storage_client;
pub mod tcp;

// TODO: delete this module
pub mod error;
pub mod unix;

use std::error::Error;

use async_trait::async_trait;

use crate::network_server::NetworkServer;
use crate::storage_client::StorageClient;
use crate::tcp::{ClusterTcpServer, TcpServer};
use raft::{RequestRouter, RaftNode};
use std::sync::Arc;
use cmd::table::create_command_table;
use executor::CmdExecutorBuilder;
use runtime::{MessageChannel, RuntimeManager, StorageClient as RuntimeStorageClient};

#[async_trait]
pub trait ServerTrait: Send + Sync + 'static {
    async fn run(&self) -> Result<(), Box<dyn Error>>;
}

pub struct ServerFactory;

impl ServerFactory {
    /// Create a server with dual runtime architecture support
    ///
    /// Defaults to single-node mode. Use `create_server_with_mode` for cluster mode.
    pub fn create_server(
        protocol: &str,
        addr: Option<String>,
        runtime_manager: &RuntimeManager,
    ) -> Option<Box<dyn ServerTrait>> {
        Self::create_server_with_mode(
            protocol,
            addr,
            runtime_manager,
            crate::raft_network_handle::ClusterMode::Single,
            None,
        )
    }

    /// Create a server with dual runtime architecture and specified cluster mode
    ///
    /// # Requirements
    /// - Requirement 6.1: Network layer SHALL support mode switching
    pub fn create_server_with_mode(
        protocol: &str,
        addr: Option<String>,
        runtime_manager: &RuntimeManager,
        cluster_mode: crate::raft_network_handle::ClusterMode,
        raft_node_opt: Option<Arc<RaftNode>>,
    ) -> Option<Box<dyn ServerTrait>> {
        match protocol.to_lowercase().as_str() {
            "tcp" => {
                // Create NetworkServer with dual runtime architecture
                match Self::create_network_server_with_mode(addr, runtime_manager, cluster_mode, raft_node_opt) {
                    Ok(server) => Some(Box::new(server) as Box<dyn ServerTrait>),
                    Err(e) => {
                        log::error!("Failed to create NetworkServer: {}", e);
                        None
                    }
                }
            }
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

    /// Create a NetworkServer with dual runtime architecture and specified cluster mode
    fn create_network_server_with_mode(
        addr: Option<String>,
        runtime_manager: &RuntimeManager,
        cluster_mode: crate::raft_network_handle::ClusterMode,
        raft_node_opt: Option<Arc<RaftNode>>,
    ) -> Result<NetworkServer, Box<dyn std::error::Error>> {
        // Create message channel for communication between runtimes
        let message_channel = Arc::new(MessageChannel::new(
            runtime_manager.config().channel_buffer_size,
        ));

        // Create storage client for network-to-storage communication
        let runtime_storage_client = Arc::new(RuntimeStorageClient::new(
            message_channel.clone(),
            runtime_manager.config().request_timeout,
        ));
        let storage_client = Arc::new(StorageClient::new(runtime_storage_client));

        // Create command table and executor
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        // Create NetworkServer with cluster mode
        let mut server = NetworkServer::new(addr, storage_client, cmd_table, executor)?;
        server.set_cluster_mode(cluster_mode);
        if matches!(cluster_mode, crate::raft_network_handle::ClusterMode::Cluster) {
            if let Some(raft_node) = raft_node_opt {
                let router = Arc::new(RequestRouter::new(Arc::clone(&raft_node), raft::ClusterMode::Cluster));
                server.set_raft_router(router);
            } else {
                log::warn!("Cluster mode enabled but RequestRouter is not available at server creation time");
            }
        }

        Ok(server)
    }

    /// Try to construct a RequestRouter from RuntimeManager context if available
    fn initialize_request_router(
        _runtime_manager: &RuntimeManager,
    ) -> Option<Arc<RequestRouter>> {
        // Placeholder: In a complete integration, obtain the RaftNode from a registry
        // and create RequestRouter::new(raft_node, ClusterMode::Cluster)
        None
    }

    pub fn create_cluster_server(
        protocol: &str,
        addr: Option<String>,
        raft_node: Arc<dyn Send + Sync>,
    ) -> Option<Box<dyn ServerTrait>> {
        match protocol.to_lowercase().as_str() {
            "tcp" => ClusterTcpServer::new(addr, raft_node)
                .ok()
                .map(|s| Box::new(s) as Box<dyn ServerTrait>),
            _ => None,
        }
    }
}
