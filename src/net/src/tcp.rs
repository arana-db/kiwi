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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use client::{Client, StreamTrait};
use cmd::table::{CmdTable, create_command_table};
use executor::{CmdExecutor, CmdExecutorBuilder};
use log::{info, warn};
use storage::options::StorageOptions;
use storage::storage::Storage;
use storage::ClusterStorage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::interval;

use crate::ServerTrait;
use crate::handle::{process_connection, process_cluster_connection};
use crate::pool::{ConnectionPool, PoolConfig};

pub struct TcpStreamWrapper {
    stream: TcpStream,
}

impl TcpStreamWrapper {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl StreamTrait for TcpStreamWrapper {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf).await
    }
    async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        self.stream.write(data).await
    }
}

/// Connection handler resources that can be pooled
pub struct ConnectionResources {
    pub storage: Arc<Storage>,
    pub cmd_table: Arc<CmdTable>,
    pub executor: Arc<CmdExecutor>,
}

/// Cluster connection handler resources
pub struct ClusterConnectionResources {
    pub cluster_storage: Arc<ClusterStorage>,
    pub cmd_table: Arc<CmdTable>,
    pub executor: Arc<CmdExecutor>,
}

pub struct TcpServer {
    addr: String,
    storage: Arc<Storage>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    resource_pool: Arc<ConnectionPool<ConnectionResources>>,
}

impl TcpServer {
    pub fn new(addr: Option<String>) -> Self {
        // TODO: Get storage options from config
        let storage_options = Arc::new(StorageOptions::default());
        let db_path = PathBuf::from("./db");
        let mut storage = Storage::new(1, 0);
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        // Note: Storage::open returns a receiver, and should be called after construction, not in new.
        // The caller should call storage.open(storage_options, db_path) and spawn the bg_task_worker as needed.
        storage.open(storage_options, db_path).unwrap();

        // Configure connection pool
        let pool_config = PoolConfig {
            max_connections: 1000,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300),
            min_connections: 10,
        };

        let storage = Arc::new(storage);
        let cmd_table = Arc::new(create_command_table());

        Self {
            addr: addr.unwrap_or("127.0.0.1:7379".to_string()),
            storage: storage.clone(),
            cmd_table: cmd_table.clone(),
            executor: executor.clone(),
            resource_pool: Arc::new(ConnectionPool::new(pool_config)),
        }
    }

    /// Start background task for resource pool cleanup
    async fn start_pool_cleanup(&self) {
        let pool = self.resource_pool.clone();
        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(60)); // Cleanup every minute
            
            loop {
                cleanup_interval.tick().await;
                pool.cleanup_idle().await;
                
                let stats = pool.stats().await;
                if stats.active_connections > 0 || stats.available_connections > 0 {
                    info!(
                        "Resource pool stats - Active: {}, Available: {}, Max: {}",
                        stats.active_connections, stats.available_connections, stats.max_connections
                    );
                }
            }
        });
    }
}

/// Cluster-aware TCP server that integrates with Raft consensus
pub struct ClusterTcpServer {
    addr: String,
    cluster_storage: Arc<ClusterStorage>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    resource_pool: Arc<ConnectionPool<ClusterConnectionResources>>,
    raft_node: Arc<dyn Send + Sync>,
}

impl ClusterTcpServer {
    pub fn new(addr: Option<String>, raft_node: Arc<dyn Send + Sync>) -> Self {
        // TODO: Get storage options from config
        let storage_options = Arc::new(StorageOptions::default());
        let db_path = PathBuf::from("./db");
        let mut storage = Storage::new(1, 0);
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        // Note: Storage::open returns a receiver, and should be called after construction, not in new.
        // The caller should call storage.open(storage_options, db_path) and spawn the bg_task_worker as needed.
        storage.open(storage_options, db_path).unwrap();

        // Configure connection pool
        let pool_config = PoolConfig {
            max_connections: 1000,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300),
            min_connections: 10,
        };

        let storage = Arc::new(storage);
        let cluster_storage = Arc::new(ClusterStorage::new(storage, raft_node.clone()));
        let cmd_table = Arc::new(create_command_table());

        Self {
            addr: addr.unwrap_or("127.0.0.1:7379".to_string()),
            cluster_storage: cluster_storage.clone(),
            cmd_table: cmd_table.clone(),
            executor: executor.clone(),
            resource_pool: Arc::new(ConnectionPool::new(pool_config)),
            raft_node,
        }
    }

    /// Start background task for resource pool cleanup
    async fn start_pool_cleanup(&self) {
        let pool = self.resource_pool.clone();
        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(60)); // Cleanup every minute
            
            loop {
                cleanup_interval.tick().await;
                pool.cleanup_idle().await;
                
                let stats = pool.stats().await;
                if stats.active_connections > 0 || stats.available_connections > 0 {
                    info!(
                        "Cluster resource pool stats - Active: {}, Available: {}, Max: {}",
                        stats.active_connections, stats.available_connections, stats.max_connections
                    );
                }
            }
        });
    }
}

#[async_trait]
impl ServerTrait for ClusterTcpServer {
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.addr).await?;

        info!("Listening on TCP (cluster mode): {}", self.addr);

        // Start background cleanup task
        self.start_pool_cleanup().await;

        loop {
            let (socket, addr) = listener.accept().await?;

            let pool = self.resource_pool.clone();
            let cluster_storage = self.cluster_storage.clone();
            let cmd_table = self.cmd_table.clone();
            let executor = self.executor.clone();
            let raft_node = self.raft_node.clone();

            tokio::spawn(async move {
                // Get or create resources from the pool
                let pooled_resources = match pool.get_connection(|| async {
                    Ok(ClusterConnectionResources {
                        cluster_storage: cluster_storage.clone(),
                        cmd_table: cmd_table.clone(),
                        executor: executor.clone(),
                    })
                }).await {
                    Ok(resources) => resources,
                    Err(e) => {
                        warn!("Failed to get cluster resources from pool for {}: {}", addr, e);
                        return;
                    }
                };

                // Create client for this specific connection
                let stream = TcpStreamWrapper::new(socket);
                let client = Arc::new(Client::new(Box::new(stream)));

                // Process the connection with cluster awareness
                let result = process_cluster_connection(
                    client,
                    pooled_resources.connection.cluster_storage.local_storage().clone(),
                    pooled_resources.connection.cmd_table.clone(),
                    pooled_resources.connection.executor.clone(),
                    raft_node,
                ).await;

                if let Err(e) = result {
                    warn!("Cluster connection processing error for {}: {}", addr, e);
                }

                // Return resources to pool
                pool.return_connection(pooled_resources).await;
            });
        }
    }
}

#[async_trait]
impl ServerTrait for TcpServer {
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.addr).await?;

        info!("Listening on TCP: {}", self.addr);

        // Start background cleanup task
        self.start_pool_cleanup().await;

        loop {
            let (socket, addr) = listener.accept().await?;

            let pool = self.resource_pool.clone();
            let storage = self.storage.clone();
            let cmd_table = self.cmd_table.clone();
            let executor = self.executor.clone();

            tokio::spawn(async move {
                // Get or create resources from the pool
                let pooled_resources = match pool.get_connection(|| async {
                    Ok(ConnectionResources {
                        storage: storage.clone(),
                        cmd_table: cmd_table.clone(),
                        executor: executor.clone(),
                    })
                }).await {
                    Ok(resources) => resources,
                    Err(e) => {
                        warn!("Failed to get resources from pool for {}: {}", addr, e);
                        return;
                    }
                };

                // Create client for this specific connection
                let stream = TcpStreamWrapper::new(socket);
                let client = Arc::new(Client::new(Box::new(stream)));

                // Process the connection
                let result = process_connection(
                    client,
                    pooled_resources.connection.storage.clone(),
                    pooled_resources.connection.cmd_table.clone(),
                    pooled_resources.connection.executor.clone(),
                ).await;

                if let Err(e) = result {
                    warn!("Connection processing error for {}: {}", addr, e);
                }

                // Return resources to pool
                pool.return_connection(pooled_resources).await;
            });
        }
    }
}
