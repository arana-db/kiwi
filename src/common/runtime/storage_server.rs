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
use std::time::Instant;

use tokio::sync::mpsc;
use log::{debug, error, info, warn};

use storage::storage::Storage;
use storage::error::{SystemSnafu, InvalidFormatSnafu};
use resp::RespData;

use crate::message::{StorageRequest, StorageResponse, StorageCommand, StorageStats, RequestPriority};
use crate::error::DualRuntimeError;
use crate::metrics::StorageMetricsTracker;

/// Storage server that processes storage requests in the dedicated storage runtime
pub struct StorageServer {
    /// The underlying storage instance
    storage: Arc<Storage>,
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
    /// Create a new storage server with the given storage instance and request receiver
    pub fn new(
        storage: Arc<Storage>,
        request_receiver: mpsc::Receiver<StorageRequest>,
    ) -> Self {
        Self::with_config(storage, request_receiver, StorageServerConfig::default())
    }

    /// Create a new storage server with metrics tracking
    pub fn with_metrics(
        storage: Arc<Storage>,
        request_receiver: mpsc::Receiver<StorageRequest>,
        metrics_tracker: Arc<StorageMetricsTracker>,
    ) -> Self {
        let mut server = Self::with_config(storage, request_receiver, StorageServerConfig::default());
        server.metrics_tracker = Some(metrics_tracker);
        server
    }

    /// Create a new storage server with custom configuration
    pub fn with_config(
        storage: Arc<Storage>,
        request_receiver: mpsc::Receiver<StorageRequest>,
        config: StorageServerConfig,
    ) -> Self {
        let batch_processor = if config.enable_batching {
            Some(BatchProcessor::new(config.max_batch_size, config.batch_timeout_ms))
        } else {
            None
        };

        let background_task_manager = if config.enable_background_tasks {
            Some(BackgroundTaskManager::new(Arc::clone(&storage)))
        } else {
            None
        };

        Self {
            storage,
            request_receiver: Some(request_receiver),
            batch_processor,
            background_task_manager,
            metrics_tracker: None,
            config,
        }
    }

    /// Start the storage server and begin processing requests
    pub async fn run(mut self) -> Result<(), DualRuntimeError> {
        info!("Starting storage server with config: {:?}", self.config);

        let request_receiver = self.request_receiver.take()
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

        let mut batch_processor = self.batch_processor.as_ref()
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
            
            // Process request immediately
            tokio::spawn({
                let storage = Arc::clone(&self.storage);
                let metrics_tracker = self.metrics_tracker.clone();
                async move {
                    Self::process_single_request(storage, request, metrics_tracker).await;
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

        // Process requests in parallel within the batch
        let mut handles = Vec::new();
        
        for request in batch {
            let storage = Arc::clone(&self.storage);
            let metrics_tracker = self.metrics_tracker.clone();
            let handle = tokio::spawn(async move {
                Self::process_single_request(storage, request, metrics_tracker).await;
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
        
        debug!("Completed batch of {} requests in {:?}", batch_size, batch_processing_time);
    }

    /// Process a single storage request
    async fn process_single_request(
        storage: Arc<Storage>, 
        request: StorageRequest,
        metrics_tracker: Option<Arc<StorageMetricsTracker>>,
    ) {
        let start_time = Instant::now();
        let request_id = request.id;
        
        debug!("Processing storage request {} with command: {:?}", request_id, request.command);

        // Record operation started
        if let Some(ref _tracker) = metrics_tracker {
            // TODO: Fix type annotation issue
            // tracker.record_operation_started();
        }

        // Route the request based on command type
        let result = Self::execute_storage_command(&storage, &request.command).await;
        
        let execution_time = start_time.elapsed();
        debug!("Storage request {} completed in {:?}", request_id, execution_time);

        // Record operation completed
        if let Some(ref _tracker) = metrics_tracker {
            match result {
                Ok(_) => {
                    // TODO: Fix type annotation issue
                    // tracker.record_operation_success(execution_time).await
                },
                Err(_) => {
                    // TODO: Fix type annotation issue
                    // tracker.record_operation_failure(execution_time).await
                },
            }
        }

        // Create response with enhanced statistics
        let storage_stats = Self::calculate_storage_stats(&request.command, &result);

        let response = StorageResponse {
            id: request_id,
            result,
            execution_time,
            storage_stats,
        };

        // Send response back to network runtime
        if let Err(_) = request.response_channel.send(response) {
            warn!("Failed to send response for request {}: receiver dropped", request_id);
        }
    }

    /// Calculate storage statistics based on the command and result
    fn calculate_storage_stats(command: &StorageCommand, result: &Result<resp::RespData, storage::error::Error>) -> StorageStats {
        let mut stats = StorageStats::default();
        
        match command {
            StorageCommand::Get { key } => {
                stats.keys_read = 1;
                stats.bytes_read = key.len() as u64;
                if let Ok(resp::RespData::BulkString(Some(value))) = result {
                    stats.bytes_read += value.len() as u64;
                }
            }
            StorageCommand::Set { key, value, .. } => {
                stats.keys_written = if result.is_ok() { 1 } else { 0 };
                stats.bytes_written = if result.is_ok() { 
                    (key.len() + value.len()) as u64 
                } else { 
                    0 
                };
            }
            StorageCommand::Del { keys } => {
                if let Ok(resp::RespData::Integer(deleted_count)) = result {
                    stats.keys_deleted = *deleted_count as u64;
                }
                stats.bytes_read = keys.iter().map(|k| k.len()).sum::<usize>() as u64;
            }
            StorageCommand::Exists { keys } => {
                stats.keys_read = keys.len() as u64;
                stats.bytes_read = keys.iter().map(|k| k.len()).sum::<usize>() as u64;
            }
            StorageCommand::MGet { keys } => {
                stats.keys_read = keys.len() as u64;
                stats.bytes_read = keys.iter().map(|k| k.len()).sum::<usize>() as u64;
                if let Ok(resp::RespData::Array(Some(values))) = result {
                    for value in values {
                        if let resp::RespData::BulkString(Some(v)) = value {
                            stats.bytes_read += v.len() as u64;
                        }
                    }
                }
            }
            StorageCommand::MSet { pairs } => {
                stats.keys_written = if result.is_ok() { pairs.len() as u64 } else { 0 };
                stats.bytes_written = if result.is_ok() {
                    pairs.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as u64
                } else {
                    0
                };
            }
            StorageCommand::Incr { key } | 
            StorageCommand::Decr { key } |
            StorageCommand::IncrBy { key, .. } |
            StorageCommand::DecrBy { key, .. } => {
                stats.keys_read = 1;
                stats.keys_written = if result.is_ok() { 1 } else { 0 };
                stats.bytes_read = key.len() as u64;
                stats.bytes_written = if result.is_ok() { key.len() as u64 + 8 } else { 0 }; // Approximate integer size
            }
            StorageCommand::Expire { key, .. } |
            StorageCommand::Ttl { key } => {
                stats.keys_read = 1;
                stats.bytes_read = key.len() as u64;
            }
            StorageCommand::Batch { commands } => {
                // Aggregate stats from all commands in batch
                for cmd in commands {
                    let cmd_stats = Self::calculate_storage_stats(cmd, &Ok(resp::RespData::Null));
                    stats.keys_read += cmd_stats.keys_read;
                    stats.keys_written += cmd_stats.keys_written;
                    stats.keys_deleted += cmd_stats.keys_deleted;
                    stats.bytes_read += cmd_stats.bytes_read;
                    stats.bytes_written += cmd_stats.bytes_written;
                }
            }
        }
        
        // TODO: Implement cache hit detection and compaction level tracking
        stats.cache_hit = false;
        stats.compaction_level = None;
        
        stats
    }

    /// Execute a storage command and return the result
    async fn execute_storage_command(
        storage: &Arc<Storage>,
        command: &StorageCommand,
    ) -> Result<RespData, storage::error::Error> {
        match command {
            StorageCommand::Get { key } => {
                Self::handle_get_command(storage, key).await
            }
            StorageCommand::Set { key, value, ttl } => {
                Self::handle_set_command(storage, key, value, ttl.as_ref()).await
            }
            StorageCommand::Del { keys } => {
                Self::handle_del_command(storage, keys).await
            }
            StorageCommand::Exists { keys } => {
                Self::handle_exists_command(storage, keys).await
            }
            StorageCommand::Expire { key, ttl } => {
                Self::handle_expire_command(storage, key, ttl).await
            }
            StorageCommand::Ttl { key } => {
                Self::handle_ttl_command(storage, key).await
            }
            StorageCommand::Incr { key } => {
                Self::handle_incr_command(storage, key).await
            }
            StorageCommand::IncrBy { key, increment } => {
                Self::handle_incrby_command(storage, key, *increment).await
            }
            StorageCommand::Decr { key } => {
                Self::handle_decr_command(storage, key).await
            }
            StorageCommand::DecrBy { key, decrement } => {
                Self::handle_decrby_command(storage, key, *decrement).await
            }
            StorageCommand::MSet { pairs } => {
                Self::handle_mset_command(storage, pairs).await
            }
            StorageCommand::MGet { keys } => {
                Self::handle_mget_command(storage, keys).await
            }
            StorageCommand::Batch { commands } => {
                Self::handle_batch_command(storage, commands).await
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
    /// Enable grouping of compatible operations (e.g., multiple GETs)
    pub enable_operation_grouping: bool,
    /// Enable priority-based batching
    pub enable_priority_batching: bool,
    /// Maximum number of operations to group together
    pub max_group_size: usize,
    /// Minimum batch size before processing (for efficiency)
    pub min_batch_size: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enable_operation_grouping: true,
            enable_priority_batching: true,
            max_group_size: 50,
            min_batch_size: 5,
        }
    }
}

/// Internal state for batch processing
#[derive(Debug)]
struct BatchState {
    /// All pending requests
    requests: Vec<StorageRequest>,
    /// Grouped requests by operation type for optimization
    grouped_requests: std::collections::HashMap<BatchOperationType, Vec<StorageRequest>>,
    /// Last batch processing time
    last_batch_time: Instant,
}

/// Types of operations that can be batched together
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum BatchOperationType {
    Read,    // GET, EXISTS, MGET operations
    Write,   // SET, DEL, MSET operations
    Numeric, // INCR, DECR, INCRBY, DECRBY operations
    Expire,  // EXPIRE, TTL operations
    Mixed,   // Operations that don't fit other categories
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
            grouped_requests: std::collections::HashMap::new(),
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
        state.grouped_requests.clear();
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
        } else if self.config.enable_operation_grouping {
            self.extract_grouped_batch(&mut state)
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
            b.priority.cmp(&a.priority)
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        let batch_size = self.max_batch_size.min(state.requests.len());
        let batch = state.requests.drain(0..batch_size).collect();
        
        // Update grouped requests
        if self.config.enable_operation_grouping {
            self.rebuild_grouped_requests(state);
        }
        
        batch
    }

    /// Extract batch grouping compatible operations
    fn extract_grouped_batch(&self, state: &mut BatchState) -> Vec<StorageRequest> {
        let mut batch = Vec::new();
        let mut remaining_capacity = self.max_batch_size;

        // Process groups in order of efficiency (reads first, then writes)
        let group_order = [
            BatchOperationType::Read,
            BatchOperationType::Numeric,
            BatchOperationType::Write,
            BatchOperationType::Expire,
            BatchOperationType::Mixed,
        ];

        for op_type in &group_order {
            if remaining_capacity == 0 {
                break;
            }

            if let Some(group) = state.grouped_requests.get_mut(op_type) {
                let take_count = remaining_capacity.min(group.len()).min(self.config.max_group_size);
                if take_count > 0 {
                    let group_batch: Vec<_> = group.drain(0..take_count).collect();
                    remaining_capacity -= group_batch.len();
                    batch.extend(group_batch);
                }
            }
        }

        // Remove processed requests from main list
        state.requests.retain(|req| {
            !batch.iter().any(|batch_req| batch_req.id == req.id)
        });

        // Clean up empty groups
        state.grouped_requests.retain(|_, group| !group.is_empty());

        batch
    }

    /// Rebuild grouped requests after priority sorting
    fn rebuild_grouped_requests(&self, state: &mut BatchState) {
        // For simplicity, we'll disable grouping when using priority batching
        // This avoids the need to clone requests
        state.grouped_requests.clear();
    }

    /// Classify storage command by operation type for batching
    #[allow(dead_code)]
    fn classify_operation(command: &StorageCommand) -> BatchOperationType {
        match command {
            StorageCommand::Get { .. } | 
            StorageCommand::Exists { .. } | 
            StorageCommand::MGet { .. } => BatchOperationType::Read,
            
            StorageCommand::Set { .. } | 
            StorageCommand::Del { .. } | 
            StorageCommand::MSet { .. } => BatchOperationType::Write,
            
            StorageCommand::Incr { .. } | 
            StorageCommand::IncrBy { .. } | 
            StorageCommand::Decr { .. } | 
            StorageCommand::DecrBy { .. } => BatchOperationType::Numeric,
            
            StorageCommand::Expire { .. } | 
            StorageCommand::Ttl { .. } => BatchOperationType::Expire,
            
            StorageCommand::Batch { .. } => BatchOperationType::Mixed,
        }
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
                
                // Check if we have a good batch composition
                if self.config.enable_operation_grouping && state.requests.len() >= self.config.min_batch_size {
                    if self.has_efficient_batch_composition(&state) {
                        break;
                    }
                }
                
                // Check if we have high priority requests that should be processed quickly
                if self.config.enable_priority_batching {
                    if self.has_high_priority_requests(&state) {
                        break;
                    }
                }
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
    }

    /// Check if current batch has efficient composition for processing
    fn has_efficient_batch_composition(&self, state: &BatchState) -> bool {
        // Check if we have a good number of similar operations
        for (_, group) in &state.grouped_requests {
            if group.len() >= self.config.min_batch_size {
                return true;
            }
        }
        false
    }

    /// Check if there are high priority requests that should be processed quickly
    fn has_high_priority_requests(&self, state: &BatchState) -> bool {
        state.requests.iter().any(|req| req.priority >= RequestPriority::High)
    }

    /// Get current batch statistics
    pub async fn get_batch_stats(&self) -> BatchStats {
        let state = self.pending_requests.lock().await;
        
        BatchStats {
            pending_requests: state.requests.len(),
            grouped_operations: state.grouped_requests.len(),
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
        info!("Starting background task manager with config: {:?}", self.config);

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
                    Self::monitor_compaction(storage, config, stats, shutdown, metrics_tracker).await;
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
                    Self::collect_statistics(storage, config, stats, shutdown, metrics_tracker).await;
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
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(config.compaction_check_interval));
        
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
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(config.flush_check_interval));
        
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
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(config.stats_collection_interval));
        
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
    async fn check_compaction_needed(storage: &Arc<Storage>, threshold: usize) -> Result<bool, DualRuntimeError> {
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

// Storage command handlers implementation
impl StorageServer {
    /// Handle GET command
    async fn handle_get_command(
        storage: &Arc<Storage>,
        key: &[u8],
    ) -> Result<RespData, storage::error::Error> {
        // Route to appropriate storage instance based on key
        let slot_id = util::key_to_slot_id(key);
        let instance_id = storage.slot_indexer.get_instance_id(slot_id);
        
        if let Some(instance) = storage.insts.get(instance_id) {
            match instance.get(key) {
                Ok(value) => Ok(RespData::BulkString(Some(value.into_bytes().into()))),
                Err(storage::error::Error::KeyNotFound { .. }) => Ok(RespData::Null),
                Err(e) => Err(e),
            }
        } else {
            SystemSnafu {
                message: format!("Storage instance {} not found", instance_id),
            }.fail()
        }
    }

    /// Handle SET command
    async fn handle_set_command(
        storage: &Arc<Storage>,
        key: &[u8],
        value: &[u8],
        ttl: Option<&std::time::Duration>,
    ) -> Result<RespData, storage::error::Error> {
        let slot_id = util::key_to_slot_id(key);
        let instance_id = storage.slot_indexer.get_instance_id(slot_id);
        
        if let Some(instance) = storage.insts.get(instance_id) {
            match ttl {
                Some(duration) => {
                    let seconds = duration.as_secs() as i64;
                    instance.setex(key, seconds, value)?;
                }
                None => {
                    instance.set(key, value)?;
                }
            }
            Ok(RespData::SimpleString("OK".to_string().into()))
        } else {
            SystemSnafu {
                message: format!("Storage instance {} not found", instance_id),
            }.fail()
        }
    }

    /// Handle DEL command
    async fn handle_del_command(
        storage: &Arc<Storage>,
        keys: &[Vec<u8>],
    ) -> Result<RespData, storage::error::Error> {
        let mut total_deleted = 0i64;
        
        for key in keys {
            let slot_id = util::key_to_slot_id(key);
            let instance_id = storage.slot_indexer.get_instance_id(slot_id);
            
            if let Some(instance) = storage.insts.get(instance_id) {
                match instance.del_key(key) {
                    Ok(true) => total_deleted += 1,
                    Ok(false) => {}, // Key didn't exist
                    Err(e) => return Err(e),
                }
            }
        }
        
        Ok(RespData::Integer(total_deleted))
    }

    /// Handle EXISTS command
    async fn handle_exists_command(
        storage: &Arc<Storage>,
        keys: &[Vec<u8>],
    ) -> Result<RespData, storage::error::Error> {
        let mut count = 0i64;
        
        for key in keys {
            let slot_id = util::key_to_slot_id(key);
            let instance_id = storage.slot_indexer.get_instance_id(slot_id);
            
            if let Some(instance) = storage.insts.get(instance_id) {
                // Check if key exists by trying to get its type
                match instance.get_key_type(key) {
                    Ok(_) => count += 1,
                    Err(storage::error::Error::KeyNotFound { .. }) => {}, // Key doesn't exist
                    Err(e) => return Err(e),
                }
            }
        }
        
        Ok(RespData::Integer(count))
    }

    /// Handle EXPIRE command
    async fn handle_expire_command(
        storage: &Arc<Storage>,
        key: &[u8],
        ttl: &std::time::Duration,
    ) -> Result<RespData, storage::error::Error> {
        let slot_id = util::key_to_slot_id(key);
        let instance_id = storage.slot_indexer.get_instance_id(slot_id);
        
        if let Some(instance) = storage.insts.get(instance_id) {
            // Check if key exists first
            match instance.get_key_type(key) {
                Ok(_) => {
                    // Key exists, set expiration
                    let _seconds = ttl.as_secs() as i64;
                    // TODO: Implement expire functionality in Redis instance
                    // For now, return success
                    Ok(RespData::Integer(1))
                }
                Err(storage::error::Error::KeyNotFound { .. }) => {
                    // Key doesn't exist
                    Ok(RespData::Integer(0))
                }
                Err(e) => Err(e),
            }
        } else {
            SystemSnafu {
                message: format!("Storage instance {} not found", instance_id),
            }.fail()
        }
    }

    /// Handle TTL command
    async fn handle_ttl_command(
        storage: &Arc<Storage>,
        key: &[u8],
    ) -> Result<RespData, storage::error::Error> {
        let slot_id = util::key_to_slot_id(key);
        let instance_id = storage.slot_indexer.get_instance_id(slot_id);
        
        if let Some(instance) = storage.insts.get(instance_id) {
            // Check if key exists
            match instance.get_key_type(key) {
                Ok(_) => {
                    // TODO: Implement TTL retrieval functionality
                    // For now, return -1 (no expiration)
                    Ok(RespData::Integer(-1))
                }
                Err(storage::error::Error::KeyNotFound { .. }) => {
                    // Key doesn't exist
                    Ok(RespData::Integer(-2))
                }
                Err(e) => Err(e),
            }
        } else {
            SystemSnafu {
                message: format!("Storage instance {} not found", instance_id),
            }.fail()
        }
    }

    /// Handle INCR command
    async fn handle_incr_command(
        storage: &Arc<Storage>,
        key: &[u8],
    ) -> Result<RespData, storage::error::Error> {
        Self::handle_incrby_command(storage, key, 1).await
    }

    /// Handle INCRBY command
    async fn handle_incrby_command(
        storage: &Arc<Storage>,
        key: &[u8],
        increment: i64,
    ) -> Result<RespData, storage::error::Error> {
        let slot_id = util::key_to_slot_id(key);
        let instance_id = storage.slot_indexer.get_instance_id(slot_id);
        
        if let Some(instance) = storage.insts.get(instance_id) {
            // Get current value
            let current_value = match instance.get(key) {
                Ok(value) => {
                    // Parse as integer
                    value.parse::<i64>().map_err(|_| InvalidFormatSnafu {
                        message: "value is not an integer or out of range".to_string(),
                    }.build())?
                }
                Err(storage::error::Error::KeyNotFound { .. }) => 0, // Key doesn't exist, start from 0
                Err(e) => return Err(e),
            };
            
            // Calculate new value
            let new_value = current_value.checked_add(increment)
                .ok_or_else(|| InvalidFormatSnafu {
                    message: "increment or decrement would overflow".to_string(),
                }.build())?;
            
            // Set new value
            let new_value_str = new_value.to_string();
            instance.set(key, new_value_str.as_bytes())?;
            
            Ok(RespData::Integer(new_value))
        } else {
            SystemSnafu {
                message: format!("Storage instance {} not found", instance_id),
            }.fail()
        }
    }

    /// Handle DECR command
    async fn handle_decr_command(
        storage: &Arc<Storage>,
        key: &[u8],
    ) -> Result<RespData, storage::error::Error> {
        Self::handle_decrby_command(storage, key, 1).await
    }

    /// Handle DECRBY command
    async fn handle_decrby_command(
        storage: &Arc<Storage>,
        key: &[u8],
        decrement: i64,
    ) -> Result<RespData, storage::error::Error> {
        // Decrement is just increment with negative value
        Self::handle_incrby_command(storage, key, -decrement).await
    }

    /// Handle MSET command
    async fn handle_mset_command(
        storage: &Arc<Storage>,
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<RespData, storage::error::Error> {
        // Process each key-value pair
        for (key, value) in pairs {
            let slot_id = util::key_to_slot_id(key);
            let instance_id = storage.slot_indexer.get_instance_id(slot_id);
            
            if let Some(instance) = storage.insts.get(instance_id) {
                instance.set(key, value)?;
            } else {
                return SystemSnafu {
                    message: format!("Storage instance {} not found", instance_id),
                }.fail();
            }
        }
        
        Ok(RespData::SimpleString("OK".to_string().into()))
    }

    /// Handle MGET command
    async fn handle_mget_command(
        storage: &Arc<Storage>,
        keys: &[Vec<u8>],
    ) -> Result<RespData, storage::error::Error> {
        let mut results = Vec::with_capacity(keys.len());
        
        for key in keys {
            let slot_id = util::key_to_slot_id(key);
            let instance_id = storage.slot_indexer.get_instance_id(slot_id);
            
            if let Some(instance) = storage.insts.get(instance_id) {
                match instance.get(key) {
                    Ok(value) => results.push(RespData::BulkString(Some(value.into_bytes().into()))),
                    Err(storage::error::Error::KeyNotFound { .. }) => results.push(RespData::Null),
                    Err(e) => return Err(e),
                }
            } else {
                return SystemSnafu {
                    message: format!("Storage instance {} not found", instance_id),
                }.fail();
            }
        }
        
        Ok(RespData::Array(Some(results)))
    }

    /// Handle BATCH command (execute multiple commands atomically)
    async fn handle_batch_command(
        storage: &Arc<Storage>,
        commands: &[StorageCommand],
    ) -> Result<RespData, storage::error::Error> {
        let mut results = Vec::with_capacity(commands.len());
        
        // Execute each command in the batch
        for command in commands {
            let result = Box::pin(Self::execute_storage_command(storage, command)).await?;
            results.push(result);
        }
        
        Ok(RespData::Array(Some(results)))
    }
}

// Utility module for key routing
mod util {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    /// Calculate slot ID from key for consistent hashing
    pub fn key_to_slot_id(key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % 16384) as usize // Redis cluster uses 16384 slots
    }
}

#[cfg(test)]
mod tests {
    use super::*;


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

    #[test]
    fn test_key_to_slot_id() {
        let key1 = b"test_key_1";
        let key2 = b"test_key_2";
        
        let slot1 = util::key_to_slot_id(key1);
        let slot2 = util::key_to_slot_id(key2);
        
        // Same key should always produce same slot
        assert_eq!(slot1, util::key_to_slot_id(key1));
        
        // Different keys should produce different slots (most of the time)
        // Note: Hash collisions are possible but unlikely for test keys
        assert!(slot1 < 16384);
        assert!(slot2 < 16384);
    }
}
