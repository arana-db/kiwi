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

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use client::Client;
use cmd::table::CmdTable;
use executor::CmdExecutor;
use log::{info, warn};
use tokio::net::TcpListener;
use tokio::time::interval;

use crate::ServerTrait;
use crate::pool::{ConnectionPool, PoolConfig};
use crate::storage_client::StorageClient;
use crate::tcp::TcpStreamWrapper;

/// Default pool configuration for network server connection pooling
fn default_network_pool_config() -> PoolConfig {
    PoolConfig {
        max_connections: 1000,
        connection_timeout: Duration::from_secs(30),
        idle_timeout: Duration::from_secs(300),
        min_connections: 10,
    }
}

/// Network connection handler resources that can be pooled
pub struct NetworkResources {
    pub storage_client: Arc<StorageClient>,
    pub cmd_table: Arc<CmdTable>,
    pub executor: Arc<CmdExecutor>,
    pub cluster_mode: crate::raft_network_handle::ClusterMode,
    pub raft_router: Option<Arc<raft::RequestRouter>>,
}

/// NetworkServer replaces TcpServer with dual runtime architecture support
///
/// This server handles network I/O operations in a dedicated runtime and
/// communicates with storage operations through a StorageClient.
///
/// # Requirements
/// - Requirement 6.1: Network layer SHALL identify read/write operation types
/// - Requirement 6.2: Write operations SHALL route to RaftNode.propose()
pub struct NetworkServer {
    /// Address to bind the server to
    addr: String,
    /// Client for communicating with storage runtime
    storage_client: Arc<StorageClient>,
    /// Command table for Redis command lookup
    cmd_table: Arc<CmdTable>,
    /// Command executor for processing commands
    executor: Arc<CmdExecutor>,
    /// Connection pool for managing network resources
    connection_pool: Arc<ConnectionPool<NetworkResources>>,
    /// Cluster mode for routing decisions
    cluster_mode: crate::raft_network_handle::ClusterMode,
    /// Optional raft request router for cluster mode
    raft_router: Option<Arc<raft::RequestRouter>>,
}

impl NetworkServer {
    /// Create a new NetworkServer with the given configuration
    ///
    /// Defaults to single-node mode. Use `with_cluster_mode` to enable cluster mode.
    pub fn new(
        addr: Option<String>,
        storage_client: Arc<StorageClient>,
        cmd_table: Arc<CmdTable>,
        executor: Arc<CmdExecutor>,
    ) -> Result<Self, Box<dyn Error>> {
        let pool_config = default_network_pool_config();

        Ok(Self {
            addr: addr.unwrap_or("127.0.0.1:7379".to_string()),
            storage_client: storage_client.clone(),
            cmd_table: cmd_table.clone(),
            executor: executor.clone(),
            connection_pool: Arc::new(ConnectionPool::new(pool_config)),
            cluster_mode: crate::raft_network_handle::ClusterMode::Single,
            raft_router: None,
        })
    }

    /// Create a NetworkServer with custom pool configuration
    pub fn with_pool_config(
        addr: Option<String>,
        storage_client: Arc<StorageClient>,
        cmd_table: Arc<CmdTable>,
        executor: Arc<CmdExecutor>,
        pool_config: PoolConfig,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            addr: addr.unwrap_or("127.0.0.1:7379".to_string()),
            storage_client: storage_client.clone(),
            cmd_table: cmd_table.clone(),
            executor: executor.clone(),
            connection_pool: Arc::new(ConnectionPool::new(pool_config)),
            cluster_mode: crate::raft_network_handle::ClusterMode::Single,
            raft_router: None,
        })
    }

    /// Set the cluster mode for this server
    ///
    /// # Requirements
    /// - Requirement 6.1: Network layer SHALL support mode switching
    pub fn set_cluster_mode(&mut self, mode: crate::raft_network_handle::ClusterMode) {
        info!("Setting cluster mode to: {:?}", mode);
        self.cluster_mode = mode;
    }
    pub fn set_raft_router(&mut self, router: Arc<raft::RequestRouter>) {
        self.raft_router = Some(router);
    }
    pub fn raft_router(&self) -> Option<Arc<raft::RequestRouter>> {
        self.raft_router.clone()
    }

    /// Get the current cluster mode
    pub fn cluster_mode(&self) -> crate::raft_network_handle::ClusterMode {
        self.cluster_mode
    }

    /// Start background task for connection pool cleanup
    async fn start_pool_cleanup(&self) {
        let pool = self.connection_pool.clone();
        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(60)); // Cleanup every minute

            loop {
                cleanup_interval.tick().await;
                pool.cleanup_idle().await;

                let stats = pool.stats().await;
                if stats.active_connections > 0 || stats.available_connections > 0 {
                    info!(
                        "Network server pool stats - Active: {}, Available: {}, Max: {}",
                        stats.active_connections,
                        stats.available_connections,
                        stats.max_connections
                    );
                }
            }
        });
    }

    /// Get the server address
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Get the storage client
    pub fn storage_client(&self) -> &Arc<StorageClient> {
        &self.storage_client
    }

    /// Get connection pool statistics
    pub async fn pool_stats(&self) -> crate::pool::PoolStats {
        self.connection_pool.stats().await
    }

    /// Check if the server is healthy
    pub fn is_healthy(&self) -> bool {
        self.storage_client.is_healthy()
    }
}

#[async_trait]
impl ServerTrait for NetworkServer {
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.addr).await?;

        info!("NetworkServer listening on: {}", self.addr);

        // Start background cleanup task
        self.start_pool_cleanup().await;

        loop {
            let (socket, client_addr) = listener.accept().await?;

            let pool = self.connection_pool.clone();
            let storage_client = self.storage_client.clone();
            let cmd_table = self.cmd_table.clone();
            let executor = self.executor.clone();

            let cluster_mode = self.cluster_mode;
            let raft_router = self.raft_router.clone();
            
            tokio::spawn(async move {
                // Get or create resources from the pool
                let pooled_resources = match pool
                    .get_connection(|| async {
                        Ok(NetworkResources {
                            storage_client: storage_client.clone(),
                            cmd_table: cmd_table.clone(),
                            executor: executor.clone(),
                            cluster_mode,
                            raft_router: raft_router.clone(),
                        })
                    })
                    .await
                {
                    Ok(resources) => resources,
                    Err(e) => {
                        warn!(
                            "Failed to get network resources from pool for {}: {}",
                            client_addr, e
                        );
                        return;
                    }
                };

                // Create client for this specific connection
                let stream = TcpStreamWrapper::new(socket);
                let client = Arc::new(Client::new(Box::new(stream)));

                // Process the connection using Raft-aware handler
                let result = crate::raft_network_handle::process_raft_aware_connection(
                    client,
                    pooled_resources.inner().storage_client.clone(),
                    pooled_resources.inner().cmd_table.clone(),
                    pooled_resources.inner().executor.clone(),
                    pooled_resources.inner().cluster_mode,
                    pooled_resources.inner().raft_router.clone(),
                )
                .await;

                if let Err(e) = result {
                    warn!(
                        "Network connection processing error for {}: {}",
                        client_addr, e
                    );
                }

                // Return resources to pool
                pool.return_connection(pooled_resources).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cmd::table::create_command_table;
    use executor::CmdExecutorBuilder;
    use runtime::{MessageChannel, StorageClient as RuntimeStorageClient};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_network_server_creation() {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let runtime_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(crate::storage_client::StorageClient::new(runtime_client));
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        let server = NetworkServer::new(
            Some("127.0.0.1:0".to_string()),
            storage_client,
            cmd_table,
            executor,
        );

        assert!(server.is_ok());
        let server = server.unwrap();
        assert_eq!(server.addr(), "127.0.0.1:0");
        assert!(server.is_healthy());
        assert_eq!(server.cluster_mode(), crate::raft_network_handle::ClusterMode::Single);
    }

    #[tokio::test]
    async fn test_network_server_with_custom_pool_config() {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let runtime_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(crate::storage_client::StorageClient::new(runtime_client));
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        let pool_config = PoolConfig {
            max_connections: 500,
            connection_timeout: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(150),
            min_connections: 5,
        };

        let server = NetworkServer::with_pool_config(
            Some("127.0.0.1:0".to_string()),
            storage_client,
            cmd_table,
            executor,
            pool_config,
        );

        assert!(server.is_ok());
        let server = server.unwrap();

        let stats = server.pool_stats().await;
        assert_eq!(stats.max_connections, 500);
    }

    #[tokio::test]
    async fn test_network_server_default_address() {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let runtime_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(crate::storage_client::StorageClient::new(runtime_client));
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        let server = NetworkServer::new(None, storage_client, cmd_table, executor);

        assert!(server.is_ok());
        let server = server.unwrap();
        assert_eq!(server.addr(), "127.0.0.1:7379");
    }

    #[tokio::test]
    async fn test_network_server_cluster_mode_switching() {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let runtime_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(crate::storage_client::StorageClient::new(runtime_client));
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        let mut server = NetworkServer::new(
            Some("127.0.0.1:0".to_string()),
            storage_client,
            cmd_table,
            executor,
        ).unwrap();

        // Default should be single mode
        assert_eq!(server.cluster_mode(), crate::raft_network_handle::ClusterMode::Single);

        // Switch to cluster mode
        server.set_cluster_mode(crate::raft_network_handle::ClusterMode::Cluster);
        assert_eq!(server.cluster_mode(), crate::raft_network_handle::ClusterMode::Cluster);

        // Switch back to single mode
        server.set_cluster_mode(crate::raft_network_handle::ClusterMode::Single);
        assert_eq!(server.cluster_mode(), crate::raft_network_handle::ClusterMode::Single);
    }
}
