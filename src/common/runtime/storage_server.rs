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

//! Storage server for dedicated storage processing in the storage runtime

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use client::{Client, StreamTrait};
use cmd::table::{CmdTable, create_command_table};
use log::{debug, error, info, warn};
use tokio::sync::mpsc;

use resp::RespData;
use storage::error::SystemSnafu;
use storage::storage::Storage;

use crate::error::DualRuntimeError;
use crate::global_storage::GlobalStorage;
use crate::message::{
    NoopStorageStatsCollector, RequestPriority, StorageCommand, StorageRequest, StorageResponse,
    StorageStatsCollector,
};
use crate::metrics::StorageMetricsTracker;

static STORAGE_COMMAND_TABLE: OnceLock<CmdTable> = OnceLock::new();

struct RuntimeCommandStream;

#[async_trait::async_trait]
impl StreamTrait for RuntimeCommandStream {
    async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
        Ok(0)
    }

    async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
        Ok(0)
    }
}

/// Pause controller for coordinating with StorageServer during snapshot installation.
/// This struct holds the necessary Arc fields to control pause/resume without owning StorageServer.
#[derive(Clone)]
pub struct StorageServerPauseController {
    paused: Arc<AtomicBool>,
    pending_count: Arc<AtomicU64>,
    pause_notify: Arc<tokio::sync::Notify>,
}

impl StorageServerPauseController {
    /// Create a new pause controller with default state (not paused)
    pub fn new() -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            pending_count: Arc::new(AtomicU64::new(0)),
            pause_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Request pause: wait for all pending requests to complete.
    pub async fn request_pause(&self) {
        log::info!("StorageServerPauseController: requesting pause for snapshot installation");
        self.paused.store(true, Ordering::SeqCst);

        // Wait for all pending requests to complete
        while self.pending_count.load(Ordering::SeqCst) > 0 {
            self.pause_notify.notified().await;
        }
        log::info!("StorageServerPauseController: pause complete, all pending requests finished");
    }

    /// Resume: allow new requests to proceed.
    pub fn resume(&self) {
        log::info!("StorageServerPauseController: resuming after snapshot installation");
        self.paused.store(false, Ordering::SeqCst);
        self.pause_notify.notify_waiters();
    }

    /// Get the paused flag Arc (for StorageServer initialization)
    pub fn paused_arc(&self) -> Arc<AtomicBool> {
        self.paused.clone()
    }

    /// Get the pending count Arc (for StorageServer initialization)
    pub fn pending_count_arc(&self) -> Arc<AtomicU64> {
        self.pending_count.clone()
    }

    /// Get the pause notify Arc (for StorageServer initialization)
    pub fn pause_notify_arc(&self) -> Arc<tokio::sync::Notify> {
        self.pause_notify.clone()
    }
}

impl Default for StorageServerPauseController {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage server that processes storage requests in the dedicated storage runtime
pub struct StorageServer {
    /// Global storage wrapper for hot-swapping during snapshot installation
    global_storage: GlobalStorage,
    /// Receiver for storage requests from the network runtime
    request_receiver: Option<mpsc::Receiver<StorageRequest>>,
    /// Batch processor for optimizing storage operations
    batch_processor: Option<BatchProcessor>,
    /// Background task manager for RocksDB maintenance
    background_task_manager: Option<BackgroundTaskManager>,
    /// Metrics tracker for storage operations
    metrics_tracker: Option<Arc<StorageMetricsTracker>>,
    /// Server configuration
    config: StorageServerConfig,

    // --- Pause mechanism for snapshot installation ---
    /// Pause flag: when true, new requests wait for resume
    paused: Arc<AtomicBool>,
    /// Count of pending requests being processed
    pending_count: Arc<AtomicU64>,
    /// Notify for waiting paused requests
    pause_notify: Arc<tokio::sync::Notify>,
}

/// Configuration for the storage server
#[derive(Debug, Clone)]
pub struct StorageServerConfig {
    /// Maximum number of requests to process in a single batch
    pub max_batch_size: usize,
    /// Maximum time to wait before processing a partial batch
    pub batch_timeout_ms: u64,
    /// Number of worker tasks for processing requests
    pub worker_count: usize,
    /// Whether to enable request batching
    pub enable_batching: bool,
    /// Whether to enable background task management
    pub enable_background_tasks: bool,
}

impl Default for StorageServerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout_ms: 10,
            worker_count: 4,
            enable_batching: true,
            enable_background_tasks: true,
        }
    }
}

impl StorageServer {
    /// Create a new storage server with the given GlobalStorage and request receiver
    pub fn new(
        global_storage: GlobalStorage,
        request_receiver: mpsc::Receiver<StorageRequest>,
    ) -> Self {
        Self::with_config(
            global_storage,
            request_receiver,
            StorageServerConfig::default(),
        )
    }

    /// Create a new storage server with a pause controller
    pub fn with_pause_controller(
        global_storage: GlobalStorage,
        request_receiver: mpsc::Receiver<StorageRequest>,
        pause_controller: StorageServerPauseController,
    ) -> Self {
        let mut server = Self::with_config(
            global_storage,
            request_receiver,
            StorageServerConfig::default(),
        );
        server.paused = pause_controller.paused_arc();
        server.pending_count = pause_controller.pending_count_arc();
        server.pause_notify = pause_controller.pause_notify_arc();
        server
    }

    /// Create a new storage server with metrics tracking
    pub fn with_metrics(
        global_storage: GlobalStorage,
        request_receiver: mpsc::Receiver<StorageRequest>,
        metrics_tracker: Arc<StorageMetricsTracker>,
    ) -> Self {
        let mut server = Self::with_config(
            global_storage,
            request_receiver,
            StorageServerConfig::default(),
        );
        server.metrics_tracker = Some(metrics_tracker);
        server
    }

    /// Create a new storage server with custom configuration
    pub fn with_config(
        global_storage: GlobalStorage,
        request_receiver: mpsc::Receiver<StorageRequest>,
        config: StorageServerConfig,
    ) -> Self {
        let batch_processor = if config.enable_batching {
            Some(BatchProcessor::new(
                config.max_batch_size,
                config.batch_timeout_ms,
            ))
        } else {
            None
        };

        let background_task_manager = if config.enable_background_tasks {
            Some(BackgroundTaskManager::new(global_storage.load()))
        } else {
            None
        };

        Self {
            global_storage,
            request_receiver: Some(request_receiver),
            batch_processor,
            background_task_manager,
            metrics_tracker: None,
            config,
            paused: Arc::new(AtomicBool::new(false)),
            pending_count: Arc::new(AtomicU64::new(0)),
            pause_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Request pause: wait for all pending requests to complete.
    /// Called by KiwiStateMachine during install_snapshot.
    pub async fn request_pause(&self) {
        info!("StorageServer: requesting pause for snapshot installation");
        self.paused.store(true, Ordering::SeqCst);

        // Wait for all pending requests to complete
        while self.pending_count.load(Ordering::SeqCst) > 0 {
            self.pause_notify.notified().await;
        }
        info!("StorageServer: pause complete, all pending requests finished");
    }

    /// Resume: allow new requests to proceed.
    /// Called by KiwiStateMachine after snapshot installation completes.
    pub fn resume(&self) {
        info!("StorageServer: resuming after snapshot installation");
        self.paused.store(false, Ordering::SeqCst);
        self.pause_notify.notify_waiters();
    }

    /// Start the storage server and begin processing requests
    pub async fn run(mut self) -> Result<(), DualRuntimeError> {
        info!("Starting storage server with config: {:?}", self.config);

        let request_receiver = self
            .request_receiver
            .take()
            .ok_or_else(|| DualRuntimeError::storage_runtime("Request receiver already taken"))?;

        // Start background task manager if enabled
        if let Some(background_task_manager) = self.background_task_manager.take() {
            tokio::spawn(async move {
                if let Err(e) = background_task_manager.run().await {
                    error!("Background task manager failed: {}", e);
                }
            });
        }

        // Start request processing
        if self.config.enable_batching {
            self.run_with_batching(request_receiver).await
        } else {
            self.run_without_batching(request_receiver).await
        }
    }

    /// Run the server with batching enabled
    async fn run_with_batching(
        &self,
        mut request_receiver: mpsc::Receiver<StorageRequest>,
    ) -> Result<(), DualRuntimeError> {
        info!("Storage server running with batching enabled");

        let mut batch_processor = self
            .batch_processor
            .as_ref()
            .ok_or_else(|| DualRuntimeError::storage_runtime("Batch processor not initialized"))?
            .clone();

        loop {
            tokio::select! {
                // Receive new requests
                request = request_receiver.recv() => {
                    match request {
                        Some(request) => {
                            debug!("Received storage request: {:?}", request.id);

                            // Add request to batch processor
                            if let Err(e) = batch_processor.add_request(request).await {
                                error!("Failed to add request to batch processor: {}", e);
                            }
                        }
                        None => {
                            info!("Request receiver closed, shutting down storage server");
                            break;
                        }
                    }
                }

                // Process batched requests
                batch = batch_processor.get_next_batch() => {
                    if !batch.is_empty() {
                        debug!("Processing batch of {} requests", batch.len());
                        self.process_request_batch(batch).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Run the server without batching (process requests individually)
    async fn run_without_batching(
        &self,
        mut request_receiver: mpsc::Receiver<StorageRequest>,
    ) -> Result<(), DualRuntimeError> {
        info!("Storage server running without batching");

        while let Some(request) = request_receiver.recv().await {
            debug!("Received storage request: {:?}", request.id);

            // Check if paused - wait for resume
            while self.paused.load(Ordering::SeqCst) {
                debug!("Storage server paused, waiting for resume");
                self.pause_notify.notified().await;
            }

            // Increment pending count
            self.pending_count.fetch_add(1, Ordering::SeqCst);

            // Process request immediately - load current storage
            let global_storage = self.global_storage.clone();
            let metrics_tracker = self.metrics_tracker.clone();
            let paused = self.paused.clone();
            let pending_count = self.pending_count.clone();
            let pause_notify = self.pause_notify.clone();

            tokio::spawn(async move {
                let storage = global_storage.load();
                Self::process_single_request(storage, request, metrics_tracker).await;

                // Decrement pending count
                pending_count.fetch_sub(1, Ordering::SeqCst);

                // If paused and all requests complete, notify
                if paused.load(Ordering::SeqCst) && pending_count.load(Ordering::SeqCst) == 0 {
                    pause_notify.notify_waiters();
                }
            });
        }

        info!("Request receiver closed, shutting down storage server");
        Ok(())
    }

    /// Process a batch of storage requests
    async fn process_request_batch(&self, batch: Vec<StorageRequest>) {
        let batch_size = batch.len();
        let batch_start_time = Instant::now();

        debug!("Processing batch of {} requests", batch_size);

        // Increment pending count for all requests in batch
        self.pending_count
            .fetch_add(batch_size as u64, Ordering::SeqCst);

        // Process requests in parallel within the batch
        let mut handles = Vec::new();

        for request in batch {
            // Load current storage for each request
            let storage = self.global_storage.load();
            let metrics_tracker = self.metrics_tracker.clone();
            let paused = self.paused.clone();
            let pending_count = self.pending_count.clone();
            let pause_notify = self.pause_notify.clone();

            let handle = tokio::spawn(async move {
                Self::process_single_request(storage, request, metrics_tracker).await;

                // Decrement pending count
                pending_count.fetch_sub(1, Ordering::SeqCst);

                // If paused and all requests complete, notify
                if paused.load(Ordering::SeqCst) && pending_count.load(Ordering::SeqCst) == 0 {
                    pause_notify.notify_waiters();
                }
            });
            handles.push(handle);
        }

        // Wait for all requests in the batch to complete
        for handle in handles {
            if let Err(e) = handle.await {
                error!("Failed to process request in batch: {}", e);
            }
        }

        // Record batch processing metrics
        let batch_processing_time = batch_start_time.elapsed();
        if let Some(ref _tracker) = self.metrics_tracker {
            // TODO: Fix type annotation issue
            // tracker.record_batch_processed(batch_size, batch_processing_time).await;
        }

        debug!(
            "Completed batch of {} requests in {:?}",
            batch_size, batch_processing_time
        );
    }

    /// Process a single storage request
    async fn process_single_request(
        storage: Arc<Storage>,
        request: StorageRequest,
        metrics_tracker: Option<Arc<StorageMetricsTracker>>,
    ) {
        let start_time = Instant::now();
        let request_id = request.id;

        debug!(
            "Processing storage request {} with command: {:?}",
            request_id, request.command
        );

        // Record operation started
        if let Some(ref _tracker) = metrics_tracker {
            // TODO: Fix type annotation issue
            // tracker.record_operation_started();
        }

        // TODO(storage-stats): Pass this request-local collector through
        // `Cmd::execute` and into the storage crate so stats are recorded by
        // actual storage operations instead of inferred from command arguments.
        let stats_collector = NoopStorageStatsCollector;

        // Route the request based on command type
        let result = Self::execute_storage_command(&storage, &request.command).await;

        let execution_time = start_time.elapsed();
        debug!(
            "Storage request {} completed in {:?}",
            request_id, execution_time
        );

        // Record operation completed
        if let Some(ref _tracker) = metrics_tracker {
            match result {
                Ok(_) => {
                    // TODO: Fix type annotation issue
                    // tracker.record_operation_success(execution_time).await
                }
                Err(_) => {
                    // TODO: Fix type annotation issue
                    // tracker.record_operation_failure(execution_time).await
                }
            }
        }

        let response = StorageResponse {
            id: request_id,
            result,
            execution_time,
            storage_stats: stats_collector.finish(),
        };

        // Send response back to network runtime
        if request.response_channel.send(response).is_err() {
            warn!(
                "Failed to send response for request {}: receiver dropped",
                request_id
            );
        }
    }

    /// Execute a storage command and return the result
    async fn execute_storage_command(
        storage: &Arc<Storage>,
        command: &StorageCommand,
    ) -> Result<RespData, storage::error::Error> {
        match command {
            StorageCommand::Execute { cmd_name, argv } => {
                Self::handle_execute_command(storage, cmd_name, argv).await
            }
            StorageCommand::Batch { commands } => {
                let mut results = Vec::with_capacity(commands.len());

                for command in commands {
                    let result = Box::pin(Self::execute_storage_command(storage, command)).await?;
                    results.push(result);
                }

                Ok(RespData::Array(Some(results)))
            }
        }
    }
}

/// Batch processor for optimizing storage operations
#[derive(Debug, Clone)]
pub struct BatchProcessor {
    max_batch_size: usize,
    batch_timeout_ms: u64,
    pending_requests: Arc<tokio::sync::Mutex<BatchState>>,
    config: BatchConfig,
}

/// Configuration for batch processing optimization
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Enable priority-based batching
    pub enable_priority_batching: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enable_priority_batching: true,
        }
    }
}

/// Internal state for batch processing
#[derive(Debug)]
struct BatchState {
    /// All pending requests
    requests: Vec<StorageRequest>,
    /// Last batch processing time
    last_batch_time: Instant,
}

impl BatchProcessor {
    /// Create a new batch processor with default configuration
    pub fn new(max_batch_size: usize, batch_timeout_ms: u64) -> Self {
        Self::with_config(max_batch_size, batch_timeout_ms, BatchConfig::default())
    }

    /// Create a new batch processor with custom configuration
    pub fn with_config(max_batch_size: usize, batch_timeout_ms: u64, config: BatchConfig) -> Self {
        let batch_state = BatchState {
            requests: Vec::new(),
            last_batch_time: Instant::now(),
        };

        Self {
            max_batch_size,
            batch_timeout_ms,
            pending_requests: Arc::new(tokio::sync::Mutex::new(batch_state)),
            config,
        }
    }

    /// Add a request to the batch
    pub async fn add_request(&mut self, request: StorageRequest) -> Result<(), DualRuntimeError> {
        let mut state = self.pending_requests.lock().await;

        // Add to main request list
        state.requests.push(request);

        Ok(())
    }

    /// Get the next batch of requests to process
    pub async fn get_next_batch(&mut self) -> Vec<StorageRequest> {
        let timeout = tokio::time::Duration::from_millis(self.batch_timeout_ms);

        tokio::select! {
            _ = tokio::time::sleep(timeout) => {
                // Timeout reached, return whatever we have
                self.extract_batch_by_timeout().await
            }
            _ = self.wait_for_batch_condition() => {
                // Batch condition met, return optimized batch
                self.extract_optimized_batch().await
            }
        }
    }

    /// Extract batch when timeout is reached
    async fn extract_batch_by_timeout(&self) -> Vec<StorageRequest> {
        let mut state = self.pending_requests.lock().await;

        if state.requests.is_empty() {
            return Vec::new();
        }

        // Take all requests on timeout
        let batch = std::mem::take(&mut state.requests);
        state.last_batch_time = Instant::now();

        debug!("Extracted batch of {} requests due to timeout", batch.len());
        batch
    }

    /// Extract optimized batch based on configuration
    async fn extract_optimized_batch(&self) -> Vec<StorageRequest> {
        let mut state = self.pending_requests.lock().await;

        if state.requests.is_empty() {
            return Vec::new();
        }

        let batch = if self.config.enable_priority_batching {
            self.extract_priority_batch(&mut state)
        } else {
            // Simple FIFO batch
            let batch_size = self.max_batch_size.min(state.requests.len());
            state.requests.drain(0..batch_size).collect()
        };

        state.last_batch_time = Instant::now();
        debug!("Extracted optimized batch of {} requests", batch.len());
        batch
    }

    /// Extract batch prioritizing high-priority requests
    fn extract_priority_batch(&self, state: &mut BatchState) -> Vec<StorageRequest> {
        // Sort by priority (highest first) and timestamp (oldest first for same priority)
        state.requests.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        let batch_size = self.max_batch_size.min(state.requests.len());
        let batch = state.requests.drain(0..batch_size).collect();

        batch
    }

    /// Wait for batch condition to be met (size or efficiency threshold)
    async fn wait_for_batch_condition(&self) {
        loop {
            {
                let state = self.pending_requests.lock().await;

                // Check if we have enough requests
                if state.requests.len() >= self.max_batch_size {
                    break;
                }

                // Check if we have high priority requests that should be processed quickly
                if self.config.enable_priority_batching && self.has_high_priority_requests(&state) {
                    break;
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
    }

    /// Check if there are high priority requests that should be processed quickly
    fn has_high_priority_requests(&self, state: &BatchState) -> bool {
        state
            .requests
            .iter()
            .any(|req| req.priority >= RequestPriority::High)
    }

    /// Get current batch statistics
    pub async fn get_batch_stats(&self) -> BatchStats {
        let state = self.pending_requests.lock().await;

        BatchStats {
            pending_requests: state.requests.len(),
            grouped_operations: 0,
            time_since_last_batch: state.last_batch_time.elapsed(),
        }
    }
}

/// Statistics about batch processing
#[derive(Debug, Clone)]
pub struct BatchStats {
    pub pending_requests: usize,
    pub grouped_operations: usize,
    pub time_since_last_batch: std::time::Duration,
}

/// Background task manager for RocksDB maintenance
pub struct BackgroundTaskManager {
    storage: Arc<Storage>,
    config: BackgroundTaskConfig,
    stats: Arc<tokio::sync::Mutex<BackgroundTaskStats>>,
    shutdown_signal: Arc<tokio::sync::Notify>,
    metrics_tracker: Option<Arc<StorageMetricsTracker>>,
}

/// Configuration for background task management
#[derive(Debug, Clone)]
pub struct BackgroundTaskConfig {
    /// Interval for compaction monitoring (seconds)
    pub compaction_check_interval: u64,
    /// Interval for flush monitoring (seconds)
    pub flush_check_interval: u64,
    /// Interval for statistics collection (seconds)
    pub stats_collection_interval: u64,
    /// Threshold for triggering compaction (number of files)
    pub compaction_trigger_threshold: usize,
    /// Enable automatic compaction triggering
    pub enable_auto_compaction: bool,
    /// Enable flush monitoring
    pub enable_flush_monitoring: bool,
    /// Enable statistics collection
    pub enable_stats_collection: bool,
}

impl Default for BackgroundTaskConfig {
    fn default() -> Self {
        Self {
            compaction_check_interval: 30,
            flush_check_interval: 10,
            stats_collection_interval: 60,
            compaction_trigger_threshold: 10,
            enable_auto_compaction: true,
            enable_flush_monitoring: true,
            enable_stats_collection: true,
        }
    }
}

/// Statistics about background task operations
#[derive(Debug, Clone, Default)]
pub struct BackgroundTaskStats {
    /// Number of compaction operations triggered
    pub compactions_triggered: u64,
    /// Number of flush operations detected
    pub flushes_detected: u64,
    /// Number of statistics collections performed
    pub stats_collections: u64,
    /// Last compaction check time
    pub last_compaction_check: Option<Instant>,
    /// Last flush check time
    pub last_flush_check: Option<Instant>,
    /// Last statistics collection time
    pub last_stats_collection: Option<Instant>,
    /// Current RocksDB statistics
    pub rocksdb_stats: RocksDbStats,
}

/// RocksDB statistics
#[derive(Debug, Clone, Default)]
pub struct RocksDbStats {
    /// Number of keys in database
    pub total_keys: u64,
    /// Total size of database in bytes
    pub total_size_bytes: u64,
    /// Number of SST files
    pub sst_file_count: u64,
    /// Number of memtables
    pub memtable_count: u64,
    /// Current compaction status
    pub compaction_pending: bool,
    /// Current flush status
    pub flush_pending: bool,
    /// Write stall status
    pub write_stall_active: bool,
}

impl BackgroundTaskManager {
    /// Create a new background task manager with default configuration
    pub fn new(storage: Arc<Storage>) -> Self {
        Self::with_config(storage, BackgroundTaskConfig::default())
    }

    /// Create a new background task manager with custom configuration
    pub fn with_config(storage: Arc<Storage>, config: BackgroundTaskConfig) -> Self {
        Self {
            storage,
            config,
            stats: Arc::new(tokio::sync::Mutex::new(BackgroundTaskStats::default())),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            metrics_tracker: None,
        }
    }

    /// Create a new background task manager with metrics tracking
    pub fn with_metrics(
        storage: Arc<Storage>,
        config: BackgroundTaskConfig,
        metrics_tracker: Arc<StorageMetricsTracker>,
    ) -> Self {
        Self {
            storage,
            config,
            stats: Arc::new(tokio::sync::Mutex::new(BackgroundTaskStats::default())),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            metrics_tracker: Some(metrics_tracker),
        }
    }

    /// Run the background task manager
    pub async fn run(self) -> Result<(), DualRuntimeError> {
        info!(
            "Starting background task manager with config: {:?}",
            self.config
        );

        let mut handles = Vec::new();

        // Start compaction monitoring if enabled
        if self.config.enable_auto_compaction {
            let handle = {
                let storage = Arc::clone(&self.storage);
                let config = self.config.clone();
                let stats = Arc::clone(&self.stats);
                let shutdown = Arc::clone(&self.shutdown_signal);
                let metrics_tracker = self.metrics_tracker.clone();
                tokio::spawn(async move {
                    Self::monitor_compaction(storage, config, stats, shutdown, metrics_tracker)
                        .await;
                })
            };
            handles.push(("compaction", handle));
        }

        // Start flush monitoring if enabled
        if self.config.enable_flush_monitoring {
            let handle = {
                let storage = Arc::clone(&self.storage);
                let config = self.config.clone();
                let stats = Arc::clone(&self.stats);
                let shutdown = Arc::clone(&self.shutdown_signal);
                let metrics_tracker = self.metrics_tracker.clone();
                tokio::spawn(async move {
                    Self::monitor_flush(storage, config, stats, shutdown, metrics_tracker).await;
                })
            };
            handles.push(("flush", handle));
        }

        // Start statistics collection if enabled
        if self.config.enable_stats_collection {
            let handle = {
                let storage = Arc::clone(&self.storage);
                let config = self.config.clone();
                let stats = Arc::clone(&self.stats);
                let shutdown = Arc::clone(&self.shutdown_signal);
                let metrics_tracker = self.metrics_tracker.clone();
                tokio::spawn(async move {
                    Self::collect_statistics(storage, config, stats, shutdown, metrics_tracker)
                        .await;
                })
            };
            handles.push(("statistics", handle));
        }

        // Wait for shutdown signal or handle task failures
        tokio::select! {
            _ = self.shutdown_signal.notified() => {
                info!("Background task manager received shutdown signal");
            }
            _ = async {
                // Wait for any task to complete (which would indicate an error)
                for (name, handle) in handles {
                    if let Err(e) = handle.await {
                        error!("Background task '{}' failed: {}", name, e);
                        return;
                    }
                }
            } => {
                info!("All background tasks completed");
            }
        }

        Ok(())
    }

    /// Shutdown the background task manager
    pub async fn shutdown(&self) {
        info!("Shutting down background task manager");
        self.shutdown_signal.notify_waiters();
    }

    /// Get current background task statistics
    pub async fn get_stats(&self) -> BackgroundTaskStats {
        self.stats.lock().await.clone()
    }

    /// Monitor RocksDB compaction operations
    async fn monitor_compaction(
        storage: Arc<Storage>,
        config: BackgroundTaskConfig,
        stats: Arc<tokio::sync::Mutex<BackgroundTaskStats>>,
        shutdown: Arc<tokio::sync::Notify>,
        metrics_tracker: Option<Arc<StorageMetricsTracker>>,
    ) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            config.compaction_check_interval,
        ));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("Monitoring compaction status");

                    // Update last check time
                    {
                        let mut stats_guard = stats.lock().await;
                        stats_guard.last_compaction_check = Some(Instant::now());
                    }

                    // Check if compaction is needed
                    if let Ok(needs_compaction) = Self::check_compaction_needed(&storage, config.compaction_trigger_threshold).await {
                        if needs_compaction {
                            info!("Triggering background compaction");

                            // Record compaction started
                            if let Some(ref _tracker) = metrics_tracker {
                                // TODO: Fix type annotation issue
                                // tracker.record_compaction_started().await;
                            }

                            let compaction_start = Instant::now();
                            if let Err(e) = storage.compact_all(false).await {
                                warn!("Failed to trigger background compaction: {}", e);
                            } else {
                                let _compaction_duration = compaction_start.elapsed();

                                // Update statistics
                                let mut stats_guard = stats.lock().await;
                                stats_guard.compactions_triggered += 1;

                                // Record compaction completed
                                if let Some(ref _tracker) = metrics_tracker {
                                    // TODO: Fix type annotation issue
                                    // tracker.record_compaction_completed(0, compaction_duration).await; // TODO: Get actual bytes compacted
                                }
                            }
                        }
                    }
                }
                _ = shutdown.notified() => {
                    info!("Compaction monitoring task shutting down");
                    break;
                }
            }
        }
    }

    /// Monitor RocksDB flush operations
    async fn monitor_flush(
        storage: Arc<Storage>,
        config: BackgroundTaskConfig,
        stats: Arc<tokio::sync::Mutex<BackgroundTaskStats>>,
        shutdown: Arc<tokio::sync::Notify>,
        metrics_tracker: Option<Arc<StorageMetricsTracker>>,
    ) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            config.flush_check_interval,
        ));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("Monitoring flush status");

                    // Update last check time
                    {
                        let mut stats_guard = stats.lock().await;
                        stats_guard.last_flush_check = Some(Instant::now());
                    }

                    // Check flush status for each storage instance
                    for (i, instance) in storage.insts.iter().enumerate() {
                        if let Ok(flush_pending) = Self::check_flush_status(instance).await {
                            if flush_pending {
                                debug!("Flush pending detected on storage instance {}", i);

                                // Record flush started
                                if let Some(ref _tracker) = metrics_tracker {
                                    // TODO: Fix type annotation issue
                                    // tracker.record_flush_started().await;
                                }

                                // Update statistics
                                let mut stats_guard = stats.lock().await;
                                stats_guard.flushes_detected += 1;

                                // Record flush completed (simulated)
                                if let Some(ref _tracker) = metrics_tracker {
                                    // TODO: Fix type annotation issue
                                    // tracker.record_flush_completed(0, Duration::from_millis(10)).await; // TODO: Get actual flush metrics
                                }
                            }
                        }
                    }
                }
                _ = shutdown.notified() => {
                    info!("Flush monitoring task shutting down");
                    break;
                }
            }
        }
    }

    /// Collect storage statistics
    async fn collect_statistics(
        storage: Arc<Storage>,
        config: BackgroundTaskConfig,
        stats: Arc<tokio::sync::Mutex<BackgroundTaskStats>>,
        shutdown: Arc<tokio::sync::Notify>,
        metrics_tracker: Option<Arc<StorageMetricsTracker>>,
    ) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            config.stats_collection_interval,
        ));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("Collecting storage statistics");

                    // Collect RocksDB statistics
                    let rocksdb_stats = Self::collect_rocksdb_stats(&storage).await;

                    // Update statistics
                    {
                        let mut stats_guard = stats.lock().await;
                        stats_guard.last_stats_collection = Some(Instant::now());
                        stats_guard.stats_collections += 1;
                        stats_guard.rocksdb_stats = rocksdb_stats.clone();
                    }

                    // Update metrics tracker if available
                    if let Some(ref _tracker) = metrics_tracker {
                        // TODO: Fix type annotation issue
                        // tracker.update_rocksdb_metrics(
                        //     rocksdb_stats.total_keys,
                        //     rocksdb_stats.total_size_bytes,
                        //     rocksdb_stats.sst_file_count,
                        //     rocksdb_stats.memtable_count,
                        //     rocksdb_stats.write_stall_active,
                        //     0.0, // TODO: Implement cache hit rate calculation
                        //     0.0, // TODO: Implement bloom filter effectiveness calculation
                        // ).await;
                    }

                    // Log key metrics
                    let stats_guard = stats.lock().await;
                    info!(
                        "Storage stats - Keys: {}, Size: {} MB, SST files: {}, Compactions: {}, Flushes: {}",
                        stats_guard.rocksdb_stats.total_keys,
                        stats_guard.rocksdb_stats.total_size_bytes / (1024 * 1024),
                        stats_guard.rocksdb_stats.sst_file_count,
                        stats_guard.compactions_triggered,
                        stats_guard.flushes_detected
                    );
                }
                _ = shutdown.notified() => {
                    info!("Statistics collection task shutting down");
                    break;
                }
            }
        }
    }

    /// Check if compaction is needed based on various metrics
    async fn check_compaction_needed(
        storage: &Arc<Storage>,
        threshold: usize,
    ) -> Result<bool, DualRuntimeError> {
        // Check each storage instance
        for instance in &storage.insts {
            // Get number of SST files (placeholder - would use actual RocksDB property)
            if let Ok(sst_count) = instance.get_property("rocksdb.num-files-at-level0") {
                if sst_count as usize > threshold {
                    return Ok(true);
                }
            }

            // Check for write stalls
            if let Ok(write_stall) = instance.get_property("rocksdb.actual-delayed-write-rate") {
                if write_stall > 0 {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Check flush status for a storage instance
    async fn check_flush_status(instance: &Arc<storage::Redis>) -> Result<bool, DualRuntimeError> {
        // Check if flush is pending (placeholder - would use actual RocksDB property)
        match instance.get_property("rocksdb.mem-table-flush-pending") {
            Ok(flush_pending) => Ok(flush_pending > 0),
            Err(_) => Ok(false),
        }
    }

    /// Collect comprehensive RocksDB statistics
    async fn collect_rocksdb_stats(storage: &Arc<Storage>) -> RocksDbStats {
        let mut stats = RocksDbStats::default();

        // Aggregate statistics from all storage instances
        for instance in &storage.insts {
            // Collect various RocksDB properties
            if let Ok(keys) = instance.get_property("rocksdb.estimate-num-keys") {
                stats.total_keys += keys;
            }

            if let Ok(size) = instance.get_property("rocksdb.total-sst-files-size") {
                stats.total_size_bytes += size;
            }

            if let Ok(sst_files) = instance.get_property("rocksdb.num-files-at-level0") {
                stats.sst_file_count += sst_files;
            }

            if let Ok(memtables) = instance.get_property("rocksdb.num-immutable-mem-table") {
                stats.memtable_count += memtables;
            }

            // Check compaction status
            if let Ok(compaction) = instance.get_property("rocksdb.compaction-pending") {
                if compaction > 0 {
                    stats.compaction_pending = true;
                }
            }

            // Check flush status
            if let Ok(flush) = instance.get_property("rocksdb.mem-table-flush-pending") {
                if flush > 0 {
                    stats.flush_pending = true;
                }
            }

            // Check write stall status
            if let Ok(stall) = instance.get_property("rocksdb.actual-delayed-write-rate") {
                if stall > 0 {
                    stats.write_stall_active = true;
                }
            }
        }

        stats
    }
}

impl StorageServer {
    /// Execute an arbitrary Redis command through the shared command table.
    async fn handle_execute_command(
        storage: &Arc<Storage>,
        cmd_name: &[u8],
        argv: &[Vec<u8>],
    ) -> Result<RespData, storage::error::Error> {
        let command_name = String::from_utf8_lossy(cmd_name).to_lowercase();
        let cmd_table =
            STORAGE_COMMAND_TABLE.get_or_init(|| create_command_table(Arc::new(|| None)));
        let Some(command) = cmd_table.get(command_name.as_str()) else {
            return SystemSnafu {
                message: format!(
                    "command '{}' not supported in storage runtime",
                    command_name
                ),
            }
            .fail();
        };

        let client = Client::new(Box::new(RuntimeCommandStream));
        client.set_cmd_name(cmd_name);
        client.set_argv(argv);
        client.set_authenticated(true);

        command.execute(&client, Arc::clone(storage));
        Ok(client.take_reply())
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use storage::{StorageOptions, safe_cleanup_test_db, unique_test_db_path};

    fn opened_test_storage() -> (Arc<Storage>, std::path::PathBuf) {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);

        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let _receiver = storage
            .open(options, &db_path)
            .expect("test storage should open");

        (Arc::new(storage), db_path)
    }

    #[test]
    fn test_storage_server_config_default() {
        let config = StorageServerConfig::default();

        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.batch_timeout_ms, 10);
        assert_eq!(config.worker_count, 4);
        assert!(config.enable_batching);
        assert!(config.enable_background_tasks);
    }

    #[test]
    fn test_batch_processor_creation() {
        let processor = BatchProcessor::new(50, 20);

        assert_eq!(processor.max_batch_size, 50);
        assert_eq!(processor.batch_timeout_ms, 20);
    }

    #[tokio::test]
    async fn test_execute_storage_command_uses_command_table_for_strings_hashes_and_delete() {
        let (storage, db_path) = opened_test_storage();

        let set = StorageCommand::Execute {
            cmd_name: b"set".to_vec(),
            argv: vec![b"set".to_vec(), b"k".to_vec(), b"v".to_vec()],
        };
        assert_eq!(
            StorageServer::execute_storage_command(&storage, &set)
                .await
                .expect("SET should execute"),
            RespData::SimpleString("OK".into())
        );

        let get = StorageCommand::Execute {
            cmd_name: b"get".to_vec(),
            argv: vec![b"get".to_vec(), b"k".to_vec()],
        };
        assert_eq!(
            StorageServer::execute_storage_command(&storage, &get)
                .await
                .expect("GET should execute"),
            RespData::BulkString(Some(b"v".to_vec().into()))
        );

        let hset = StorageCommand::Execute {
            cmd_name: b"hset".to_vec(),
            argv: vec![
                b"hset".to_vec(),
                b"h".to_vec(),
                b"field".to_vec(),
                b"value".to_vec(),
            ],
        };
        assert_eq!(
            StorageServer::execute_storage_command(&storage, &hset)
                .await
                .expect("HSET should execute through the command table"),
            RespData::Integer(1)
        );

        let hget = StorageCommand::Execute {
            cmd_name: b"hget".to_vec(),
            argv: vec![b"hget".to_vec(), b"h".to_vec(), b"field".to_vec()],
        };
        assert_eq!(
            StorageServer::execute_storage_command(&storage, &hget)
                .await
                .expect("HGET should execute through the command table"),
            RespData::BulkString(Some(b"value".to_vec().into()))
        );

        let hdel = StorageCommand::Execute {
            cmd_name: b"hdel".to_vec(),
            argv: vec![b"hdel".to_vec(), b"h".to_vec(), b"field".to_vec()],
        };
        assert_eq!(
            StorageServer::execute_storage_command(&storage, &hdel)
                .await
                .expect("HDEL should execute through the command table"),
            RespData::Integer(1)
        );

        let del = StorageCommand::Execute {
            cmd_name: b"del".to_vec(),
            argv: vec![b"del".to_vec(), b"k".to_vec()],
        };
        assert_eq!(
            StorageServer::execute_storage_command(&storage, &del)
                .await
                .expect("DEL should execute"),
            RespData::Integer(1)
        );

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }
}
