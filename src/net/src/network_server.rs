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
use log::{info, warn, error};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::interval;

use crate::ServerTrait;
use crate::pool::{ConnectionPool, PoolConfig};
use crate::tcp::TcpStreamWrapper;
use crate::storage_client::StorageClient;

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
}

/// NetworkServer replaces TcpServer with dual runtime architecture support
/// 
/// This server handles network I/O operations in a dedicated runtime and
/// communicates with storage operations through a StorageClient.
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
}

impl NetworkServer {
    /// Create a new NetworkServer with the given configuration
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
        })
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

            tokio::spawn(async move {
                // Get or create resources from the pool
                let pooled_resources = match pool
                    .get_connection(|| async {
                        Ok(NetworkResources {
                            storage_client: storage_client.clone(),
                            cmd_table: cmd_table.clone(),
                            executor: executor.clone(),
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

                // Process the connection using the new network-aware handler
                let result = crate::network_handle::process_network_connection(
                    client,
                    pooled_resources.inner().storage_client.clone(),
                    pooled_resources.inner().cmd_table.clone(),
                    pooled_resources.inner().executor.clone(),
                )
                .await;

                if let Err(e) = result {
                    warn!("Network connection processing error for {}: {}", client_addr, e);
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
    use std::sync::Arc;
    use std::time::Duration;
    use cmd::table::create_command_table;
    use executor::CmdExecutorBuilder;
    use common::runtime::{MessageChannel, StorageClient};

    #[tokio::test]
    async fn test_network_server_creation() {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let storage_client = Arc::new(StorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
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
    }

    #[tokio::test]
    async fn test_network_server_with_custom_pool_config() {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let storage_client = Arc::new(StorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
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
        let storage_client = Arc::new(StorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let cmd_table = Arc::new(create_command_table());
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        let server = NetworkServer::new(
            None,
            storage_client,
            cmd_table,
            executor,
        );

        assert!(server.is_ok());
        let server = server.unwrap();
        assert_eq!(server.addr(), "127.0.0.1:7379");
    }
}