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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use client::Client;
use cmd::table::CmdTable;
use executor::CmdExecutor;
use log::{info, warn};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

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

fn create_lifecycle_shutdown(shutdown: &CancellationToken) -> CancellationToken {
    shutdown.child_token()
}

const SERVER_STATE_NEW: u8 = 0;
const SERVER_STATE_BINDING: u8 = 1;
const SERVER_STATE_BOUND: u8 = 2;
const SERVER_STATE_RUNNING: u8 = 3;
const SERVER_STATE_TERMINATED: u8 = 4;

struct BindStateGuard<'a> {
    state: &'a AtomicU8,
    committed: bool,
}

impl BindStateGuard<'_> {
    fn commit(&mut self) {
        self.committed = true;
        self.state.store(SERVER_STATE_BOUND, Ordering::Release);
    }
}

impl Drop for BindStateGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.state.store(SERVER_STATE_NEW, Ordering::Release);
        }
    }
}

struct RunStateGuard<'a> {
    state: &'a AtomicU8,
    was_bound: bool,
}

impl Drop for RunStateGuard<'_> {
    fn drop(&mut self) {
        self.state.store(SERVER_STATE_TERMINATED, Ordering::Release);
    }
}

struct AbortOnDropJoinHandle<T> {
    handle: JoinHandle<T>,
}

impl<T> AbortOnDropJoinHandle<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self { handle }
    }

    async fn join(&mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.handle).await
    }
}

impl<T> Drop for AbortOnDropJoinHandle<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Network connection handler resources that can be pooled
pub struct NetworkResources {
    pub storage_client: Arc<StorageClient>,
    pub cmd_table: Arc<CmdTable>,
    pub executor: Arc<CmdExecutor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkLifecycleStats {
    pub accepted_connection_tasks: u64,
    pub completed_connection_tasks: u64,
    pub reaped_connection_tasks: u64,
    pub tracked_connection_tasks: usize,
    pub max_tracked_connection_tasks: usize,
    pub pool_cleanup_running: bool,
    pub pool_cleanup_finished: bool,
}

#[derive(Default)]
struct NetworkLifecycleCounters {
    accepted_connection_tasks: AtomicU64,
    completed_connection_tasks: AtomicU64,
    reaped_connection_tasks: AtomicU64,
    tracked_connection_tasks: AtomicUsize,
    max_tracked_connection_tasks: AtomicUsize,
    pool_cleanup_running: AtomicBool,
    pool_cleanup_finished: AtomicBool,
}

impl NetworkLifecycleCounters {
    fn connection_spawned(&self) {
        self.accepted_connection_tasks
            .fetch_add(1, Ordering::SeqCst);
        let tracked = self.tracked_connection_tasks.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_tracked_connection_tasks
            .fetch_max(tracked, Ordering::SeqCst);
    }

    fn connection_completed(&self) {
        self.completed_connection_tasks
            .fetch_add(1, Ordering::SeqCst);
    }

    fn connection_reaped(&self) {
        self.reaped_connection_tasks.fetch_add(1, Ordering::SeqCst);
        let result = self.tracked_connection_tasks.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |tracked| tracked.checked_sub(1),
        );
        debug_assert!(result.is_ok(), "reaped an untracked connection task");
    }

    fn snapshot(&self) -> NetworkLifecycleStats {
        NetworkLifecycleStats {
            accepted_connection_tasks: self.accepted_connection_tasks.load(Ordering::SeqCst),
            completed_connection_tasks: self.completed_connection_tasks.load(Ordering::SeqCst),
            reaped_connection_tasks: self.reaped_connection_tasks.load(Ordering::SeqCst),
            tracked_connection_tasks: self.tracked_connection_tasks.load(Ordering::SeqCst),
            max_tracked_connection_tasks: self.max_tracked_connection_tasks.load(Ordering::SeqCst),
            pool_cleanup_running: self.pool_cleanup_running.load(Ordering::SeqCst),
            pool_cleanup_finished: self.pool_cleanup_finished.load(Ordering::SeqCst),
        }
    }
}

struct ConnectionTaskCompletion {
    lifecycle: Arc<NetworkLifecycleCounters>,
}

impl Drop for ConnectionTaskCompletion {
    fn drop(&mut self) {
        self.lifecycle.connection_completed();
    }
}

struct PoolCleanupCompletion {
    lifecycle: Arc<NetworkLifecycleCounters>,
}

impl Drop for PoolCleanupCompletion {
    fn drop(&mut self) {
        self.lifecycle
            .pool_cleanup_running
            .store(false, Ordering::SeqCst);
        self.lifecycle
            .pool_cleanup_finished
            .store(true, Ordering::SeqCst);
    }
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
    /// Authentication password; when set, clients must AUTH before running commands
    requirepass: Option<String>,
    /// Optional leadership gate for cluster-mode write rejection
    leader_gate: Option<std::sync::Arc<dyn raft::leader_gate::LeaderGate>>,
    /// Pre-bound listener for tests that need an ephemeral port.
    listener: Mutex<Option<TcpListener>>,
    lifecycle: Arc<NetworkLifecycleCounters>,
    lifecycle_state: AtomicU8,
}

impl NetworkServer {
    /// Create a new NetworkServer with the given configuration
    ///
    /// Defaults to single-node mode.
    pub fn new(
        addr: Option<String>,
        storage_client: Arc<StorageClient>,
        cmd_table: Arc<CmdTable>,
        executor: Arc<CmdExecutor>,
        requirepass: Option<String>,
        leader_gate: Option<std::sync::Arc<dyn raft::leader_gate::LeaderGate>>,
    ) -> Result<Self, Box<dyn Error>> {
        let pool_config = default_network_pool_config();

        Ok(Self {
            addr: addr.unwrap_or("127.0.0.1:7379".to_string()),
            storage_client: storage_client.clone(),
            cmd_table: cmd_table.clone(),
            executor: executor.clone(),
            connection_pool: Arc::new(ConnectionPool::new(pool_config)),
            requirepass,
            leader_gate,
            listener: Mutex::new(None),
            lifecycle: Arc::new(NetworkLifecycleCounters::default()),
            lifecycle_state: AtomicU8::new(SERVER_STATE_NEW),
        })
    }

    /// Create a NetworkServer with custom pool configuration
    pub fn with_pool_config(
        addr: Option<String>,
        storage_client: Arc<StorageClient>,
        cmd_table: Arc<CmdTable>,
        executor: Arc<CmdExecutor>,
        pool_config: PoolConfig,
        requirepass: Option<String>,
        leader_gate: Option<std::sync::Arc<dyn raft::leader_gate::LeaderGate>>,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            addr: addr.unwrap_or("127.0.0.1:7379".to_string()),
            storage_client: storage_client.clone(),
            cmd_table: cmd_table.clone(),
            executor: executor.clone(),
            connection_pool: Arc::new(ConnectionPool::new(pool_config)),
            requirepass,
            leader_gate,
            listener: Mutex::new(None),
            lifecycle: Arc::new(NetworkLifecycleCounters::default()),
            lifecycle_state: AtomicU8::new(SERVER_STATE_NEW),
        })
    }

    /// Start a cancellable background task for connection pool cleanup.
    fn start_pool_cleanup(&self, shutdown: CancellationToken) -> JoinHandle<()> {
        let pool = self.connection_pool.clone();
        let lifecycle = self.lifecycle.clone();
        lifecycle
            .pool_cleanup_finished
            .store(false, Ordering::SeqCst);
        lifecycle.pool_cleanup_running.store(true, Ordering::SeqCst);

        tokio::spawn(async move {
            let _completion = PoolCleanupCompletion {
                lifecycle: lifecycle.clone(),
            };
            let mut cleanup_interval = interval(Duration::from_secs(60)); // Cleanup every minute

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown.cancelled() => break,
                    _ = cleanup_interval.tick() => {
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
                }
            }
        })
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

    /// Return lifecycle counters used to verify that completed connection tasks
    /// are reaped while the server remains online.
    pub fn lifecycle_stats(&self) -> NetworkLifecycleStats {
        self.lifecycle.snapshot()
    }

    /// Check if the server is healthy
    pub fn is_healthy(&self) -> bool {
        self.storage_client.is_healthy()
    }

    /// Bind the server early and return the resolved socket address.
    ///
    /// This allows tests to use an OS-assigned ephemeral port (`127.0.0.1:0`)
    /// while still querying the actual bound address. If `run()` is called
    /// without calling `bind()` first, it binds lazily as before.
    pub async fn bind(&self) -> Result<SocketAddr, Box<dyn Error>> {
        self.lifecycle_state
            .compare_exchange(
                SERVER_STATE_NEW,
                SERVER_STATE_BINDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|state| {
                let message = match state {
                    SERVER_STATE_BINDING => "network server bind already in progress",
                    SERVER_STATE_BOUND => "network server already bound",
                    SERVER_STATE_RUNNING => "network server already running",
                    SERVER_STATE_TERMINATED => "network server already terminated",
                    _ => "network server has an invalid lifecycle state",
                };
                std::io::Error::other(message)
            })?;
        let mut state_guard = BindStateGuard {
            state: &self.lifecycle_state,
            committed: false,
        };
        let listener = TcpListener::bind(&self.addr).await?;
        let addr = listener.local_addr()?;
        *self
            .listener
            .lock()
            .expect("network server listener lock poisoned") = Some(listener);
        state_guard.commit();
        Ok(addr)
    }

    fn begin_run(&self) -> Result<RunStateGuard<'_>, Box<dyn Error>> {
        loop {
            let state = self.lifecycle_state.load(Ordering::Acquire);
            let was_bound = match state {
                SERVER_STATE_NEW => false,
                SERVER_STATE_BOUND => true,
                SERVER_STATE_BINDING => {
                    return Err(Box::new(std::io::Error::other(
                        "network server bind already in progress",
                    )));
                }
                SERVER_STATE_RUNNING => {
                    return Err(Box::new(std::io::Error::other(
                        "network server already running",
                    )));
                }
                SERVER_STATE_TERMINATED => {
                    return Err(Box::new(std::io::Error::other(
                        "network server already terminated",
                    )));
                }
                _ => {
                    return Err(Box::new(std::io::Error::other(
                        "network server has an invalid lifecycle state",
                    )));
                }
            };

            if self
                .lifecycle_state
                .compare_exchange(
                    state,
                    SERVER_STATE_RUNNING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(RunStateGuard {
                    state: &self.lifecycle_state,
                    was_bound,
                });
            }
        }
    }

    /// Run the network server until cancellation and wait for every owned task
    /// to finish before returning.
    pub async fn run_until_cancelled(
        &self,
        shutdown: CancellationToken,
    ) -> Result<(), Box<dyn Error>> {
        let run_state = self.begin_run()?;
        let listener = {
            let mut guard = self
                .listener
                .lock()
                .expect("network server listener lock poisoned");
            guard.take()
        };
        let listener = match (listener, run_state.was_bound) {
            (Some(listener), _) => listener,
            (None, false) => TcpListener::bind(&self.addr).await?,
            (None, true) => {
                return Err(Box::new(std::io::Error::other(
                    "network server bound listener is missing",
                )));
            }
        };

        info!("NetworkServer listening on: {}", listener.local_addr()?);

        let lifecycle_shutdown = create_lifecycle_shutdown(&shutdown);
        let mut cleanup_task =
            AbortOnDropJoinHandle::new(self.start_pool_cleanup(lifecycle_shutdown.child_token()));
        let mut connection_tasks = JoinSet::new();
        let mut server_error = None;

        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => break,
                join_result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    if let Some(join_result) = join_result {
                        self.lifecycle.connection_reaped();
                        if let Err(error) = join_result {
                            server_error = Some(std::io::Error::other(format!(
                                "network connection task failed: {error}"
                            )));
                            break;
                        }
                    }
                }
                accept_result = listener.accept() => {
                    let (socket, client_addr) = match accept_result {
                        Ok(connection) => connection,
                        Err(error) => {
                            server_error = Some(error);
                            break;
                        }
                    };

                    let pool = self.connection_pool.clone();
                    let storage_client = self.storage_client.clone();
                    let cmd_table = self.cmd_table.clone();
                    let executor = self.executor.clone();
                    let requirepass = self.requirepass.clone();
                    let leader_gate = self.leader_gate.clone();
                    let connection_shutdown = lifecycle_shutdown.child_token();
                    let lifecycle = self.lifecycle.clone();
                    lifecycle.connection_spawned();

                    connection_tasks.spawn(async move {
                        let _completion = ConnectionTaskCompletion {
                            lifecycle: lifecycle.clone(),
                        };

                        let pooled_resources = tokio::select! {
                            biased;

                            _ = connection_shutdown.cancelled() => return,
                            result = pool.get_connection(|| async {
                                Ok(NetworkResources {
                                    storage_client: storage_client.clone(),
                                    cmd_table: cmd_table.clone(),
                                    executor: executor.clone(),
                                })
                            }) => match result {
                                Ok(resources) => resources,
                                Err(error) => {
                                    warn!(
                                        "Failed to get network resources from pool for {}: {}",
                                        client_addr, error
                                    );
                                    return;
                                }
                            },
                        };

                        let stream = TcpStreamWrapper::new(socket);
                        let client = Arc::new(Client::new(Box::new(stream)));
                        if requirepass.is_none() {
                            client.set_authenticated(true);
                        }

                        let result =
                            crate::handle::process_connection_with_storage_client_until_cancelled(
                            client,
                            pooled_resources.inner().storage_client.clone(),
                            pooled_resources.inner().cmd_table.clone(),
                            pooled_resources.inner().executor.clone(),
                            leader_gate,
                            connection_shutdown,
                        )
                        .await;
                        if let Err(error) = result {
                            warn!(
                                "Network connection processing error for {}: {}",
                                client_addr, error
                            );
                        }

                        pool.return_connection(pooled_resources).await;
                    });
                }
            }
        }

        drop(listener);
        lifecycle_shutdown.cancel();

        while let Some(join_result) = connection_tasks.join_next().await {
            self.lifecycle.connection_reaped();
            if let Err(error) = join_result {
                if server_error.is_none() {
                    server_error = Some(std::io::Error::other(format!(
                        "network connection task failed during shutdown: {error}"
                    )));
                }
            }
        }

        if let Err(error) = cleanup_task.join().await {
            if server_error.is_none() {
                server_error = Some(std::io::Error::other(format!(
                    "network pool cleanup task failed: {error}"
                )));
            }
        }

        self.connection_pool.clear_idle().await;
        let pool_stats = self.connection_pool.stats().await;
        if (pool_stats.active_connections != 0 || pool_stats.available_connections != 0)
            && server_error.is_none()
        {
            server_error = Some(std::io::Error::other(format!(
                "network pool not empty after shutdown: active={}, available={}",
                pool_stats.active_connections, pool_stats.available_connections
            )));
        }

        match server_error {
            Some(error) => Err(Box::new(error)),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl ServerTrait for NetworkServer {
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        self.run_until_cancelled(CancellationToken::new()).await
    }
}

#[allow(clippy::unwrap_used)]
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
        let cmd_table = Arc::new(create_command_table(Arc::new(|| None)));
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        let server = NetworkServer::new(
            Some("127.0.0.1:0".to_string()),
            storage_client,
            cmd_table,
            executor,
            None,
            None,
        );

        assert!(server.is_ok());
        let server = server.unwrap();
        assert_eq!(server.addr(), "127.0.0.1:0");
        assert!(server.is_healthy());
    }

    #[tokio::test]
    async fn test_network_server_with_custom_pool_config() {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let runtime_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(crate::storage_client::StorageClient::new(runtime_client));
        let cmd_table = Arc::new(create_command_table(Arc::new(|| None)));
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
            None,
            None,
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
        let cmd_table = Arc::new(create_command_table(Arc::new(|| None)));
        let executor = Arc::new(CmdExecutorBuilder::new().build());

        let server = NetworkServer::new(None, storage_client, cmd_table, executor, None, None);

        assert!(server.is_ok());
        let server = server.unwrap();
        assert_eq!(server.addr(), "127.0.0.1:7379");
    }

    #[test]
    fn lifecycle_shutdown_is_cancelled_synchronously_with_parent() {
        let shutdown = CancellationToken::new();
        let lifecycle_shutdown = create_lifecycle_shutdown(&shutdown);

        shutdown.cancel();

        assert!(lifecycle_shutdown.is_cancelled());
    }
}
