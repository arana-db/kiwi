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

use log::{debug, error, info};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

use crate::error_logging::{ErrorLogger, RuntimeContext};
use crate::{DualRuntimeError, MessageChannel, RuntimeConfig, StorageClient};

/// Health status of a runtime
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeHealth {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

/// Statistics for runtime monitoring
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    pub active_tasks: usize,
    pub total_tasks_spawned: u64,
    pub uptime: Duration,
    pub health: RuntimeHealth,
    pub last_health_check: Option<Instant>,
}

/// Lifecycle state of the RuntimeManager
#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleState {
    Created,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed(String),
}

/// Manages dual tokio runtimes for network and storage operations
pub struct RuntimeManager {
    config: RuntimeConfig,
    network_runtime: Option<Runtime>,
    storage_runtime: Option<Runtime>,
    network_stats: Arc<RwLock<RuntimeStats>>,
    storage_stats: Arc<RwLock<RuntimeStats>>,
    state: Arc<RwLock<LifecycleState>>,
    start_time: Option<Instant>,
    shutdown_notify: Arc<Notify>,
    health_check_handle: Option<JoinHandle<()>>,
    _task_counter: Arc<AtomicU64>,
    error_logger: Option<Arc<ErrorLogger>>,
    /// Message channel for communication between network and storage runtimes
    /// The receiver is extracted during start() and passed to storage server
    message_channel: Option<MessageChannel>,
    /// Storage client for network runtime to send requests to storage runtime
    storage_client: Option<Arc<StorageClient>>,
}

impl RuntimeManager {
    /// Create a new RuntimeManager with the given configuration
    pub fn new(config: RuntimeConfig) -> Result<Self, DualRuntimeError> {
        // Validate configuration
        config.validate().map_err(DualRuntimeError::configuration)?;

        info!("Creating RuntimeManager with config: {:?}", config);

        // Create message channel for communication between runtimes
        // We'll extract the receiver during start() and pass it to storage server
        let message_channel = MessageChannel::new(config.channel_buffer_size);

        Ok(Self {
            config,
            network_runtime: None,
            storage_runtime: None,
            network_stats: Arc::new(RwLock::new(RuntimeStats {
                active_tasks: 0,
                total_tasks_spawned: 0,
                uptime: Duration::ZERO,
                health: RuntimeHealth::Healthy,
                last_health_check: None,
            })),
            storage_stats: Arc::new(RwLock::new(RuntimeStats {
                active_tasks: 0,
                total_tasks_spawned: 0,
                uptime: Duration::ZERO,
                health: RuntimeHealth::Healthy,
                last_health_check: None,
            })),
            state: Arc::new(RwLock::new(LifecycleState::Created)),
            start_time: None,
            shutdown_notify: Arc::new(Notify::new()),
            health_check_handle: None,
            _task_counter: Arc::new(AtomicU64::new(0)),
            error_logger: crate::error_logging::get_global_error_logger(),
            message_channel: Some(message_channel),
            storage_client: None,
        })
    }

    /// Create a RuntimeManager with default configuration
    pub fn with_defaults() -> Result<Self, DualRuntimeError> {
        Self::new(RuntimeConfig::default())
    }

    /// Start both runtimes
    pub async fn start(&mut self) -> Result<(), DualRuntimeError> {
        let current_state = self.state.read().await.clone();
        match current_state {
            LifecycleState::Running => {
                return Err(DualRuntimeError::lifecycle(
                    "RuntimeManager is already running".to_string(),
                ));
            }
            LifecycleState::Starting => {
                return Err(DualRuntimeError::lifecycle(
                    "RuntimeManager is already starting".to_string(),
                ));
            }
            LifecycleState::Stopping => {
                return Err(DualRuntimeError::lifecycle(
                    "RuntimeManager is currently stopping".to_string(),
                ));
            }
            _ => {}
        }

        // Set state to starting
        *self.state.write().await = LifecycleState::Starting;

        info!("Starting RuntimeManager...");

        // Create network runtime
        let network_runtime = match self.create_network_runtime() {
            Ok(runtime) => {
                info!(
                    "Network runtime created with {} threads",
                    self.config.network_threads
                );
                runtime
            }
            Err(e) => {
                error!("Failed to create network runtime: {}", e);

                // Log the error
                if let Some(ref logger) = self.error_logger {
                    let logger = Arc::clone(logger);
                    let error = e.clone();
                    tokio::spawn(async move {
                        logger.log_simple_error(error, RuntimeContext::Main).await;
                    });
                }

                *self.state.write().await =
                    LifecycleState::Failed(format!("Network runtime creation failed: {}", e));
                return Err(e);
            }
        };

        // Create storage runtime
        let storage_runtime = match self.create_storage_runtime() {
            Ok(runtime) => {
                info!(
                    "Storage runtime created with {} threads",
                    self.config.storage_threads
                );
                runtime
            }
            Err(e) => {
                error!("Failed to create storage runtime: {}", e);

                // Log the error
                if let Some(ref logger) = self.error_logger {
                    let logger = Arc::clone(logger);
                    let error = e.clone();
                    tokio::spawn(async move {
                        logger.log_simple_error(error, RuntimeContext::Main).await;
                    });
                }

                *self.state.write().await =
                    LifecycleState::Failed(format!("Storage runtime creation failed: {}", e));
                return Err(e);
            }
        };

        self.network_runtime = Some(network_runtime);
        self.storage_runtime = Some(storage_runtime);
        self.start_time = Some(Instant::now());

        // Note: The message channel and storage client will be initialized
        // when the storage server is started. The receiver must be extracted
        // before creating the storage client.
        info!("Runtimes created. Storage server initialization required to complete setup.");

        // Start health monitoring
        self.start_health_monitoring().await;

        // Set state to running
        *self.state.write().await = LifecycleState::Running;

        info!("RuntimeManager started successfully");
        Ok(())
    }

    /// Stop both runtimes gracefully
    pub async fn stop(&mut self) -> Result<(), DualRuntimeError> {
        let current_state = self.state.read().await.clone();
        match current_state {
            LifecycleState::Stopped => return Ok(()),
            LifecycleState::Stopping => {
                // Wait for shutdown to complete
                self.shutdown_notify.notified().await;
                return Ok(());
            }
            LifecycleState::Created | LifecycleState::Failed(_) => return Ok(()),
            _ => {}
        }

        info!("Stopping RuntimeManager...");

        // Set state to stopping
        *self.state.write().await = LifecycleState::Stopping;

        // Stop health monitoring
        if let Some(handle) = self.health_check_handle.take() {
            handle.abort();
            debug!("Health monitoring stopped");
        }

        // Shutdown storage runtime first (to complete pending operations)
        if let Some(storage_runtime) = self.storage_runtime.take() {
            info!("Shutting down storage runtime...");
            // Use a separate thread to avoid blocking in async context
            let handle = std::thread::spawn(move || {
                storage_runtime.shutdown_timeout(Duration::from_secs(30));
            });
            let _ = handle.join();
            info!("Storage runtime stopped");
        }

        // Then shutdown network runtime
        if let Some(network_runtime) = self.network_runtime.take() {
            info!("Shutting down network runtime...");
            // Use a separate thread to avoid blocking in async context
            let handle = std::thread::spawn(move || {
                network_runtime.shutdown_timeout(Duration::from_secs(10));
            });
            let _ = handle.join();
            info!("Network runtime stopped");
        }

        self.start_time = None;
        *self.state.write().await = LifecycleState::Stopped;

        // Notify any waiting shutdown operations
        self.shutdown_notify.notify_waiters();

        info!("RuntimeManager stopped successfully");
        Ok(())
    }

    /// Check if the RuntimeManager is running
    pub async fn is_running(&self) -> bool {
        matches!(*self.state.read().await, LifecycleState::Running)
    }

    /// Get the current lifecycle state
    pub async fn state(&self) -> LifecycleState {
        self.state.read().await.clone()
    }

    /// Wait for the RuntimeManager to reach a specific state
    pub async fn wait_for_state(
        &self,
        target_state: LifecycleState,
        timeout: Duration,
    ) -> Result<(), DualRuntimeError> {
        let start = Instant::now();

        loop {
            let current_state = self.state.read().await.clone();
            if std::mem::discriminant(&current_state) == std::mem::discriminant(&target_state) {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(DualRuntimeError::timeout(timeout));
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Get a handle to the network runtime
    pub fn network_handle(&self) -> Result<tokio::runtime::Handle, DualRuntimeError> {
        self.network_runtime
            .as_ref()
            .map(|rt| rt.handle().clone())
            .ok_or_else(|| DualRuntimeError::lifecycle("Network runtime not started".to_string()))
    }

    /// Get a handle to the storage runtime
    pub fn storage_handle(&self) -> Result<tokio::runtime::Handle, DualRuntimeError> {
        self.storage_runtime
            .as_ref()
            .map(|rt| rt.handle().clone())
            .ok_or_else(|| DualRuntimeError::lifecycle("Storage runtime not started".to_string()))
    }

    /// Perform health checks on both runtimes
    pub async fn health_check(&self) -> Result<(RuntimeHealth, RuntimeHealth), DualRuntimeError> {
        if !self.is_running().await {
            return Ok((
                RuntimeHealth::Unhealthy("Runtime not started".to_string()),
                RuntimeHealth::Unhealthy("Runtime not started".to_string()),
            ));
        }

        let network_health = self.check_network_health().await;
        let storage_health = self.check_storage_health().await;

        // Update stats with health check results
        {
            let mut network_stats = self.network_stats.write().await;
            network_stats.health = network_health.clone();
            network_stats.last_health_check = Some(Instant::now());
        }

        {
            let mut storage_stats = self.storage_stats.write().await;
            storage_stats.health = storage_health.clone();
            storage_stats.last_health_check = Some(Instant::now());
        }

        Ok((network_health, storage_health))
    }

    /// Force a graceful shutdown with timeout
    pub async fn force_shutdown(&mut self, timeout: Duration) -> Result<(), DualRuntimeError> {
        info!("Force shutdown requested with timeout: {:?}", timeout);

        let shutdown_future = self.stop();

        match tokio::time::timeout(timeout, shutdown_future).await {
            Ok(result) => result,
            Err(_) => {
                error!("Graceful shutdown timed out, forcing immediate shutdown");
                *self.state.write().await = LifecycleState::Failed("Shutdown timeout".to_string());

                // Force drop runtimes
                self.network_runtime = None;
                self.storage_runtime = None;

                Err(DualRuntimeError::timeout(timeout))
            }
        }
    }

    /// Get runtime statistics
    pub async fn get_stats(&self) -> (RuntimeStats, RuntimeStats) {
        let mut network_stats = self.network_stats.read().await.clone();
        let mut storage_stats = self.storage_stats.read().await.clone();

        // Update uptime
        if let Some(start_time) = self.start_time {
            let uptime = start_time.elapsed();
            network_stats.uptime = uptime;
            storage_stats.uptime = uptime;
        }

        (network_stats, storage_stats)
    }

    /// Get the current configuration
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// Get the storage client for sending requests from network to storage runtime
    /// This should only be called after the RuntimeManager has been started
    pub fn storage_client(&self) -> Result<Arc<StorageClient>, DualRuntimeError> {
        self.storage_client.as_ref().map(Arc::clone).ok_or_else(|| {
            DualRuntimeError::lifecycle(
                "Storage client not initialized. RuntimeManager must be started first.".to_string(),
            )
        })
    }

    /// Initialize storage components by extracting the receiver and creating the storage client
    /// This must be called before the storage server is started and before the storage client is used
    /// Returns the receiver that should be passed to the storage server
    pub fn initialize_storage_components(
        &mut self,
    ) -> Result<tokio::sync::mpsc::Receiver<crate::message::StorageRequest>, DualRuntimeError> {
        // Extract the message channel
        let mut channel = self.message_channel.take().ok_or_else(|| {
            DualRuntimeError::lifecycle(
                "Message channel already consumed or not initialized".to_string(),
            )
        })?;

        // Extract the receiver for the storage server
        let receiver = channel.take_request_receiver().ok_or_else(|| {
            DualRuntimeError::channel("Failed to extract request receiver".to_string())
        })?;

        // Wrap the channel (now without receiver) in Arc for the storage client
        let message_channel_arc = Arc::new(channel);

        // Create the storage client
        let storage_client = if let Some(ref logger) = self.error_logger {
            Arc::new(StorageClient::with_error_logger(
                Arc::clone(&message_channel_arc),
                self.config.request_timeout,
                Arc::clone(logger),
            ))
        } else {
            Arc::new(StorageClient::new(
                Arc::clone(&message_channel_arc),
                self.config.request_timeout,
            ))
        };

        self.storage_client = Some(storage_client);

        info!("Storage components initialized: client created, receiver extracted");

        Ok(receiver)
    }

    /// Create the network runtime with optimized settings for I/O operations
    fn create_network_runtime(&self) -> Result<Runtime, DualRuntimeError> {
        Builder::new_multi_thread()
            .worker_threads(self.config.network_threads)
            .thread_name("kiwi-network")
            .thread_stack_size(2 * 1024 * 1024) // 2MB stack
            .enable_all()
            .build()
            .map_err(|e| {
                DualRuntimeError::network_runtime(format!(
                    "Failed to create network runtime: {}",
                    e
                ))
            })
    }

    /// Create the storage runtime with optimized settings for CPU-intensive operations
    fn create_storage_runtime(&self) -> Result<Runtime, DualRuntimeError> {
        Builder::new_multi_thread()
            .worker_threads(self.config.storage_threads)
            .thread_name("kiwi-storage")
            .thread_stack_size(4 * 1024 * 1024) // 4MB stack for RocksDB operations
            .enable_all()
            .build()
            .map_err(|e| {
                DualRuntimeError::storage_runtime(format!(
                    "Failed to create storage runtime: {}",
                    e
                ))
            })
    }

    /// Check network runtime health
    async fn check_network_health(&self) -> RuntimeHealth {
        match self.network_handle() {
            Ok(handle) => {
                // Try to spawn a simple task to verify runtime is responsive
                let result = handle.spawn(async { "health_check" }).await;
                match result {
                    Ok(_) => RuntimeHealth::Healthy,
                    Err(e) => {
                        RuntimeHealth::Degraded(format!("Network runtime task failed: {}", e))
                    }
                }
            }
            Err(e) => RuntimeHealth::Unhealthy(format!("Network runtime unavailable: {}", e)),
        }
    }

    /// Check storage runtime health
    async fn check_storage_health(&self) -> RuntimeHealth {
        match self.storage_handle() {
            Ok(handle) => {
                // Try to spawn a simple task to verify runtime is responsive
                let result = handle.spawn(async { "health_check" }).await;
                match result {
                    Ok(_) => RuntimeHealth::Healthy,
                    Err(e) => {
                        RuntimeHealth::Degraded(format!("Storage runtime task failed: {}", e))
                    }
                }
            }
            Err(e) => RuntimeHealth::Unhealthy(format!("Storage runtime unavailable: {}", e)),
        }
    }

    /// Start background health monitoring
    async fn start_health_monitoring(&mut self) {
        let network_stats = Arc::clone(&self.network_stats);
        let storage_stats = Arc::clone(&self.storage_stats);
        let state = Arc::clone(&self.state);
        let network_handle = self.network_handle().ok();
        let storage_handle = self.storage_handle().ok();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));

            loop {
                interval.tick().await;

                // Check if we should continue monitoring
                let current_state = state.read().await.clone();
                if !matches!(current_state, LifecycleState::Running) {
                    debug!(
                        "Health monitoring stopped due to state change: {:?}",
                        current_state
                    );
                    break;
                }

                // Perform health checks
                if let Some(ref handle) = network_handle {
                    let health = match handle.spawn(async { "health_check" }).await {
                        Ok(_) => RuntimeHealth::Healthy,
                        Err(e) => {
                            RuntimeHealth::Degraded(format!("Network runtime task failed: {}", e))
                        }
                    };

                    let mut stats = network_stats.write().await;
                    stats.health = health;
                    stats.last_health_check = Some(Instant::now());
                }

                if let Some(ref handle) = storage_handle {
                    let health = match handle.spawn(async { "health_check" }).await {
                        Ok(_) => RuntimeHealth::Healthy,
                        Err(e) => {
                            RuntimeHealth::Degraded(format!("Storage runtime task failed: {}", e))
                        }
                    };

                    let mut stats = storage_stats.write().await;
                    stats.health = health;
                    stats.last_health_check = Some(Instant::now());
                }

                debug!("Health monitoring cycle completed");
            }
        });

        self.health_check_handle = Some(handle);
        debug!("Health monitoring started");
    }
}

impl Drop for RuntimeManager {
    fn drop(&mut self) {
        // Note: We can't use async methods in Drop, so we do a best-effort cleanup
        if let Some(handle) = self.health_check_handle.take() {
            handle.abort();
        }

        // For runtimes, we need to be careful about dropping them in async context
        // We'll use a different approach - spawn a blocking task to handle shutdown
        if self.network_runtime.is_some() || self.storage_runtime.is_some() {
            // Try to shutdown gracefully if possible
            if let Some(storage_runtime) = self.storage_runtime.take() {
                std::thread::spawn(move || {
                    storage_runtime.shutdown_timeout(Duration::from_secs(5));
                });
            }

            if let Some(network_runtime) = self.network_runtime.take() {
                std::thread::spawn(move || {
                    network_runtime.shutdown_timeout(Duration::from_secs(5));
                });
            }
        }

        debug!("RuntimeManager dropped - resources cleaned up");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_runtime_manager_lifecycle() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = RuntimeManager::with_defaults().unwrap();

            // Initially not running
            assert!(!manager.is_running().await);
            assert_eq!(manager.state().await, LifecycleState::Created);

            // Start the manager
            manager.start().await.unwrap();
            assert!(manager.is_running().await);
            assert_eq!(manager.state().await, LifecycleState::Running);

            // Should be able to get handles
            assert!(manager.network_handle().is_ok());
            assert!(manager.storage_handle().is_ok());

            // Health check should pass
            let (network_health, storage_health) = manager.health_check().await.unwrap();
            assert_eq!(network_health, RuntimeHealth::Healthy);
            assert_eq!(storage_health, RuntimeHealth::Healthy);

            // Stop the manager
            manager.stop().await.unwrap();
            assert!(!manager.is_running().await);
            assert_eq!(manager.state().await, LifecycleState::Stopped);
        });
    }

    #[test]
    fn test_runtime_manager_with_custom_config() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = RuntimeConfig::new(
                2,
                4,
                1000,
                Duration::from_secs(10),
                50,
                Duration::from_millis(5),
            )
            .unwrap();

            let mut manager = RuntimeManager::new(config).unwrap();
            manager.start().await.unwrap();

            assert!(manager.is_running().await);
            assert_eq!(manager.config().network_threads, 2);
            assert_eq!(manager.config().storage_threads, 4);

            manager.stop().await.unwrap();
        });
    }

    #[test]
    fn test_runtime_manager_state_transitions() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut manager = RuntimeManager::with_defaults().unwrap();

            // Test state transitions
            assert_eq!(manager.state().await, LifecycleState::Created);

            manager.start().await.unwrap();
            assert_eq!(manager.state().await, LifecycleState::Running);

            manager.stop().await.unwrap();
            assert_eq!(manager.state().await, LifecycleState::Stopped);
        });
    }

    #[tokio::test]
    async fn test_runtime_manager_wait_for_state() {
        let manager = RuntimeManager::with_defaults().unwrap();

        // This should timeout since manager is not started
        let result = manager
            .wait_for_state(LifecycleState::Running, Duration::from_millis(50))
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_runtime_manager_invalid_config() {
        let result = RuntimeConfig::new(
            0,
            4,
            1000,
            Duration::from_secs(10),
            50,
            Duration::from_millis(5),
        );

        assert!(result.is_err());
    }
}
