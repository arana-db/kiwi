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

//! Performance optimization components for Raft consensus

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::time::timeout;

use crate::error::{RaftError, RaftResult};
use crate::types::{ClientRequest, ClientResponse, NodeId, RequestId};

/// Configuration for batching operations
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of requests to batch together
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing a batch
    pub max_batch_delay: Duration,
    /// Maximum memory usage for batching (in bytes)
    pub max_batch_memory: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_batch_delay: Duration::from_millis(10),
            max_batch_memory: 1024 * 1024, // 1MB
        }
    }
}

/// Batch of client requests for processing
#[derive(Debug)]
pub struct RequestBatch {
    pub requests: Vec<ClientRequest>,
    pub created_at: Instant,
    pub estimated_size: usize,
}

impl RequestBatch {
    pub fn new() -> Self {
        Self {
            requests: Vec::new(),
            created_at: Instant::now(),
            estimated_size: 0,
        }
    }

    pub fn add_request(&mut self, request: ClientRequest) {
        // Estimate memory usage (rough approximation)
        let size_estimate = std::mem::size_of::<ClientRequest>() 
            + request.command.command.len()
            + request.command.args.iter().map(|arg| arg.len()).sum::<usize>();
        
        self.estimated_size += size_estimate;
        self.requests.push(request);
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn len(&self) -> usize {
        self.requests.len()
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Request batcher for improving throughput
pub struct RequestBatcher {
    config: BatchConfig,
    pending_batch: Arc<Mutex<RequestBatch>>,
    flush_notify: Arc<Notify>,
    is_running: Arc<RwLock<bool>>,
}

impl RequestBatcher {
    /// Create a new request batcher
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            pending_batch: Arc::new(Mutex::new(RequestBatch::new())),
            flush_notify: Arc::new(Notify::new()),
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the batcher background task
    pub async fn start(&self) -> RaftResult<()> {
        let mut running = self.is_running.write().await;
        if *running {
            return Err(RaftError::invalid_state("Batcher already running"));
        }
        *running = true;

        let pending_batch = Arc::clone(&self.pending_batch);
        let flush_notify = Arc::clone(&self.flush_notify);
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);

        tokio::spawn(async move {
            let mut flush_interval = tokio::time::interval(config.max_batch_delay);
            flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            while *is_running.read().await {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        // Time-based flush
                        let mut batch = pending_batch.lock().await;
                        if !batch.is_empty() {
                            flush_notify.notify_waiters();
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        // Check for size-based flush
                        let batch = pending_batch.lock().await;
                        if batch.len() >= config.max_batch_size || 
                           batch.estimated_size >= config.max_batch_memory {
                            drop(batch);
                            flush_notify.notify_waiters();
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Stop the batcher
    pub async fn stop(&self) {
        let mut running = self.is_running.write().await;
        *running = false;
        self.flush_notify.notify_waiters();
    }

    /// Add a request to the current batch
    pub async fn add_request(&self, request: ClientRequest) -> RaftResult<()> {
        let mut batch = self.pending_batch.lock().await;
        batch.add_request(request);

        // Check if we should flush immediately
        if batch.len() >= self.config.max_batch_size || 
           batch.estimated_size >= self.config.max_batch_memory {
            self.flush_notify.notify_waiters();
        }

        Ok(())
    }

    /// Wait for and retrieve the next batch
    pub async fn next_batch(&self) -> RaftResult<RequestBatch> {
        loop {
            // Wait for flush notification
            self.flush_notify.notified().await;

            let mut current_batch = self.pending_batch.lock().await;
            if !current_batch.is_empty() {
                let batch = std::mem::replace(&mut *current_batch, RequestBatch::new());
                return Ok(batch);
            }
        }
    }

    /// Get current batch statistics
    pub async fn get_stats(&self) -> BatchStats {
        let batch = self.pending_batch.lock().await;
        BatchStats {
            pending_requests: batch.len(),
            pending_size: batch.estimated_size,
            batch_age: batch.age(),
        }
    }
}

/// Statistics for batch operations
#[derive(Debug, Clone)]
pub struct BatchStats {
    pub pending_requests: usize,
    pub pending_size: usize,
    pub batch_age: Duration,
}

/// Pipeline configuration for log replication
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum number of in-flight requests per follower
    pub max_inflight_requests: usize,
    /// Timeout for individual pipeline requests
    pub request_timeout: Duration,
    /// Maximum pipeline depth
    pub max_pipeline_depth: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_inflight_requests: 10,
            request_timeout: Duration::from_millis(100),
            max_pipeline_depth: 50,
        }
    }
}

/// In-flight request tracking for pipelining
#[derive(Debug)]
struct InFlightRequest {
    request_id: RequestId,
    sent_at: Instant,
    timeout: Duration,
}

/// Pipeline manager for log replication optimization
pub struct ReplicationPipeline {
    config: PipelineConfig,
    /// In-flight requests per follower
    inflight_requests: Arc<RwLock<std::collections::HashMap<NodeId, VecDeque<InFlightRequest>>>>,
    /// Pipeline statistics
    stats: Arc<RwLock<PipelineStats>>,
}

impl ReplicationPipeline {
    /// Create a new replication pipeline
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            config,
            inflight_requests: Arc::new(RwLock::new(std::collections::HashMap::new())),
            stats: Arc::new(RwLock::new(PipelineStats::default())),
        }
    }

    /// Check if we can send more requests to a follower
    pub async fn can_send_to_follower(&self, follower_id: NodeId) -> bool {
        let inflight = self.inflight_requests.read().await;
        let follower_queue = inflight.get(&follower_id);
        
        match follower_queue {
            Some(queue) => queue.len() < self.config.max_inflight_requests,
            None => true,
        }
    }

    /// Track a new in-flight request
    pub async fn track_request(&self, follower_id: NodeId, request_id: RequestId) -> RaftResult<()> {
        let mut inflight = self.inflight_requests.write().await;
        let follower_queue = inflight.entry(follower_id).or_insert_with(VecDeque::new);
        
        if follower_queue.len() >= self.config.max_inflight_requests {
            return Err(RaftError::resource_exhausted("Too many in-flight requests"));
        }

        follower_queue.push_back(InFlightRequest {
            request_id,
            sent_at: Instant::now(),
            timeout: self.config.request_timeout,
        });

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_requests_sent += 1;
        stats.current_inflight_requests += 1;

        Ok(())
    }

    /// Complete an in-flight request
    pub async fn complete_request(&self, follower_id: NodeId, request_id: RequestId) -> RaftResult<Duration> {
        let mut inflight = self.inflight_requests.write().await;
        let follower_queue = inflight.get_mut(&follower_id);
        
        if let Some(queue) = follower_queue {
            if let Some(pos) = queue.iter().position(|req| req.request_id == request_id) {
                let request = queue.remove(pos).unwrap();
                let latency = request.sent_at.elapsed();
                
                // Update stats
                let mut stats = self.stats.write().await;
                stats.total_requests_completed += 1;
                stats.current_inflight_requests -= 1;
                stats.total_latency += latency;
                stats.max_latency = stats.max_latency.max(latency);
                
                return Ok(latency);
            }
        }

        Err(RaftError::not_found("Request not found in pipeline"))
    }

    /// Handle request timeout
    pub async fn handle_timeout(&self, follower_id: NodeId, request_id: RequestId) -> RaftResult<()> {
        let mut inflight = self.inflight_requests.write().await;
        let follower_queue = inflight.get_mut(&follower_id);
        
        if let Some(queue) = follower_queue {
            if let Some(pos) = queue.iter().position(|req| req.request_id == request_id) {
                queue.remove(pos);
                
                // Update stats
                let mut stats = self.stats.write().await;
                stats.total_timeouts += 1;
                stats.current_inflight_requests -= 1;
                
                return Ok(());
            }
        }

        Err(RaftError::not_found("Request not found in pipeline"))
    }

    /// Clean up expired requests
    pub async fn cleanup_expired_requests(&self) -> RaftResult<Vec<(NodeId, RequestId)>> {
        let mut inflight = self.inflight_requests.write().await;
        let mut expired_requests = Vec::new();
        let now = Instant::now();

        for (follower_id, queue) in inflight.iter_mut() {
            while let Some(request) = queue.front() {
                if now.duration_since(request.sent_at) > request.timeout {
                    let expired = queue.pop_front().unwrap();
                    expired_requests.push((*follower_id, expired.request_id));
                    
                    // Update stats
                    let mut stats = self.stats.write().await;
                    stats.total_timeouts += 1;
                    stats.current_inflight_requests -= 1;
                } else {
                    break;
                }
            }
        }

        Ok(expired_requests)
    }

    /// Get pipeline statistics
    pub async fn get_stats(&self) -> PipelineStats {
        self.stats.read().await.clone()
    }

    /// Reset pipeline statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = PipelineStats::default();
    }
}

/// Statistics for pipeline operations
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    pub total_requests_sent: u64,
    pub total_requests_completed: u64,
    pub total_timeouts: u64,
    pub current_inflight_requests: usize,
    pub total_latency: Duration,
    pub max_latency: Duration,
}

impl PipelineStats {
    /// Calculate average latency
    pub fn average_latency(&self) -> Duration {
        if self.total_requests_completed > 0 {
            self.total_latency / self.total_requests_completed as u32
        } else {
            Duration::ZERO
        }
    }

    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_requests_sent > 0 {
            self.total_requests_completed as f64 / self.total_requests_sent as f64
        } else {
            0.0
        }
    }
}
// Read optimization strategies for Raft followers
pub mod read_optimization {
    use super::*;
    use crate::types::{ConsistencyLevel, NodeId};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Configuration for read optimizations
    #[derive(Debug, Clone)]
    pub struct ReadOptimizationConfig {
        /// Maximum staleness allowed for eventual consistency reads
        pub max_staleness: Duration,
        /// Leadership confirmation timeout for linearizable reads
        pub leadership_timeout: Duration,
        /// Cache size for read-through optimization
        pub read_cache_size: usize,
        /// Enable follower reads for eventual consistency
        pub enable_follower_reads: bool,
    }

    impl Default for ReadOptimizationConfig {
        fn default() -> Self {
            Self {
                max_staleness: Duration::from_millis(100),
                leadership_timeout: Duration::from_millis(50),
                read_cache_size: 10000,
                enable_follower_reads: true,
            }
        }
    }

    /// Leadership lease for optimizing linearizable reads
    #[derive(Debug)]
    pub struct LeadershipLease {
        /// Last confirmed leadership timestamp
        last_confirmed: AtomicU64,
        /// Lease duration
        lease_duration: Duration,
        /// Node ID of the leader
        leader_id: NodeId,
    }

    impl LeadershipLease {
        pub fn new(leader_id: NodeId, lease_duration: Duration) -> Self {
            Self {
                last_confirmed: AtomicU64::new(0),
                lease_duration,
                leader_id,
            }
        }

        /// Confirm leadership at current time
        pub fn confirm_leadership(&self) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            self.last_confirmed.store(now, Ordering::Release);
        }

        /// Check if leadership is still valid
        pub fn is_leadership_valid(&self) -> bool {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let last_confirmed = self.last_confirmed.load(Ordering::Acquire);
            
            now.saturating_sub(last_confirmed) < self.lease_duration.as_millis() as u64
        }

        /// Get time since last confirmation
        pub fn time_since_confirmation(&self) -> Duration {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let last_confirmed = self.last_confirmed.load(Ordering::Acquire);
            
            Duration::from_millis(now.saturating_sub(last_confirmed))
        }
    }

    /// Read optimization manager
    pub struct ReadOptimizer {
        config: ReadOptimizationConfig,
        leadership_lease: Option<Arc<LeadershipLease>>,
        read_stats: Arc<RwLock<ReadStats>>,
        /// Cache for frequently accessed keys (simple LRU-like)
        read_cache: Arc<RwLock<std::collections::HashMap<String, CachedValue>>>,
    }

    #[derive(Debug, Clone)]
    struct CachedValue {
        value: bytes::Bytes,
        cached_at: Instant,
        log_index: u64,
    }

    impl ReadOptimizer {
        /// Create a new read optimizer
        pub fn new(config: ReadOptimizationConfig) -> Self {
            Self {
                config,
                leadership_lease: None,
                read_stats: Arc::new(RwLock::new(ReadStats::default())),
                read_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
            }
        }

        /// Set leadership lease for this node
        pub fn set_leadership_lease(&mut self, leader_id: NodeId, lease_duration: Duration) {
            self.leadership_lease = Some(Arc::new(LeadershipLease::new(leader_id, lease_duration)));
        }

        /// Clear leadership lease
        pub fn clear_leadership_lease(&mut self) {
            self.leadership_lease = None;
        }

        /// Determine if a read can be served locally
        pub async fn can_serve_read_locally(
            &self,
            consistency_level: ConsistencyLevel,
            current_log_index: u64,
        ) -> RaftResult<bool> {
            match consistency_level {
                ConsistencyLevel::Linearizable => {
                    // For linearizable reads, we need confirmed leadership
                    if let Some(ref lease) = self.leadership_lease {
                        Ok(lease.is_leadership_valid())
                    } else {
                        // Followers cannot serve linearizable reads without leadership confirmation
                        Ok(false)
                    }
                }
                ConsistencyLevel::Eventual => {
                    // For eventual consistency, check if we're not too far behind
                    if !self.config.enable_follower_reads {
                        return Ok(false);
                    }
                    
                    // This is a simplified check - in practice, we'd compare with leader's log index
                    Ok(true)
                }
            }
        }

        /// Confirm leadership for linearizable reads
        pub async fn confirm_leadership(&self) -> RaftResult<()> {
            if let Some(ref lease) = self.leadership_lease {
                lease.confirm_leadership();
                
                let mut stats = self.read_stats.write().await;
                stats.leadership_confirmations += 1;
                
                Ok(())
            } else {
                Err(RaftError::not_leader("No leadership lease available"))
            }
        }

        /// Cache a read result
        pub async fn cache_read_result(
            &self,
            key: String,
            value: bytes::Bytes,
            log_index: u64,
        ) -> RaftResult<()> {
            let mut cache = self.read_cache.write().await;
            
            // Simple cache size management
            if cache.len() >= self.config.read_cache_size {
                // Remove oldest entry (simplified LRU)
                if let Some(oldest_key) = cache
                    .iter()
                    .min_by_key(|(_, v)| v.cached_at)
                    .map(|(k, _)| k.clone())
                {
                    cache.remove(&oldest_key);
                }
            }

            cache.insert(key, CachedValue {
                value,
                cached_at: Instant::now(),
                log_index,
            });

            Ok(())
        }

        /// Try to serve read from cache
        pub async fn try_cached_read(
            &self,
            key: &str,
            current_log_index: u64,
            consistency_level: ConsistencyLevel,
        ) -> Option<bytes::Bytes> {
            let cache = self.read_cache.read().await;
            
            if let Some(cached) = cache.get(key) {
                match consistency_level {
                    ConsistencyLevel::Linearizable => {
                        // For linearizable reads, cache must be current
                        if cached.log_index >= current_log_index {
                            return Some(cached.value.clone());
                        }
                    }
                    ConsistencyLevel::Eventual => {
                        // For eventual consistency, allow some staleness
                        if cached.cached_at.elapsed() <= self.config.max_staleness {
                            return Some(cached.value.clone());
                        }
                    }
                }
            }

            None
        }

        /// Update read statistics
        pub async fn record_read_operation(
            &self,
            consistency_level: ConsistencyLevel,
            served_locally: bool,
            cache_hit: bool,
            latency: Duration,
        ) {
            let mut stats = self.read_stats.write().await;
            
            stats.total_reads += 1;
            stats.total_read_latency += latency;
            stats.max_read_latency = stats.max_read_latency.max(latency);
            
            match consistency_level {
                ConsistencyLevel::Linearizable => stats.linearizable_reads += 1,
                ConsistencyLevel::Eventual => stats.eventual_reads += 1,
            }
            
            if served_locally {
                stats.local_reads += 1;
            } else {
                stats.forwarded_reads += 1;
            }
            
            if cache_hit {
                stats.cache_hits += 1;
            } else {
                stats.cache_misses += 1;
            }
        }

        /// Get read optimization statistics
        pub async fn get_stats(&self) -> ReadStats {
            self.read_stats.read().await.clone()
        }

        /// Clear read cache
        pub async fn clear_cache(&self) {
            let mut cache = self.read_cache.write().await;
            cache.clear();
        }

        /// Get cache statistics
        pub async fn get_cache_stats(&self) -> CacheStats {
            let cache = self.read_cache.read().await;
            CacheStats {
                size: cache.len(),
                capacity: self.config.read_cache_size,
                hit_rate: {
                    let stats = self.read_stats.read().await;
                    if stats.cache_hits + stats.cache_misses > 0 {
                        stats.cache_hits as f64 / (stats.cache_hits + stats.cache_misses) as f64
                    } else {
                        0.0
                    }
                },
            }
        }
    }

    /// Statistics for read operations
    #[derive(Debug, Clone, Default)]
    pub struct ReadStats {
        pub total_reads: u64,
        pub linearizable_reads: u64,
        pub eventual_reads: u64,
        pub local_reads: u64,
        pub forwarded_reads: u64,
        pub cache_hits: u64,
        pub cache_misses: u64,
        pub leadership_confirmations: u64,
        pub total_read_latency: Duration,
        pub max_read_latency: Duration,
    }

    impl ReadStats {
        /// Calculate average read latency
        pub fn average_read_latency(&self) -> Duration {
            if self.total_reads > 0 {
                self.total_read_latency / self.total_reads as u32
            } else {
                Duration::ZERO
            }
        }

        /// Calculate local read percentage
        pub fn local_read_percentage(&self) -> f64 {
            if self.total_reads > 0 {
                self.local_reads as f64 / self.total_reads as f64 * 100.0
            } else {
                0.0
            }
        }

        /// Calculate cache hit rate
        pub fn cache_hit_rate(&self) -> f64 {
            let total_cache_ops = self.cache_hits + self.cache_misses;
            if total_cache_ops > 0 {
                self.cache_hits as f64 / total_cache_ops as f64 * 100.0
            } else {
                0.0
            }
        }
    }

    /// Cache statistics
    #[derive(Debug, Clone)]
    pub struct CacheStats {
        pub size: usize,
        pub capacity: usize,
        pub hit_rate: f64,
    }
}

// Memory and resource management for Raft operations
pub mod resource_management {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{Semaphore, RwLock};

    /// Configuration for resource management
    #[derive(Debug, Clone)]
    pub struct ResourceConfig {
        /// Maximum memory usage for Raft operations (in bytes)
        pub max_memory_usage: usize,
        /// Maximum number of concurrent operations
        pub max_concurrent_operations: usize,
        /// Memory pressure threshold (percentage)
        pub memory_pressure_threshold: f64,
        /// Cleanup interval for resource management
        pub cleanup_interval: Duration,
        /// Maximum log entry cache size
        pub max_log_cache_size: usize,
        /// Maximum snapshot memory usage
        pub max_snapshot_memory: usize,
    }

    impl Default for ResourceConfig {
        fn default() -> Self {
            Self {
                max_memory_usage: 512 * 1024 * 1024, // 512MB
                max_concurrent_operations: 1000,
                memory_pressure_threshold: 0.8, // 80%
                cleanup_interval: Duration::from_secs(30),
                max_log_cache_size: 10000,
                max_snapshot_memory: 128 * 1024 * 1024, // 128MB
            }
        }
    }

    /// Memory tracker for Raft operations
    pub struct MemoryTracker {
        /// Current memory usage
        current_usage: AtomicUsize,
        /// Maximum allowed memory usage
        max_usage: usize,
        /// Memory usage by category
        usage_by_category: Arc<RwLock<std::collections::HashMap<String, usize>>>,
    }

    impl MemoryTracker {
        pub fn new(max_usage: usize) -> Self {
            Self {
                current_usage: AtomicUsize::new(0),
                max_usage,
                usage_by_category: Arc::new(RwLock::new(std::collections::HashMap::new())),
            }
        }

        /// Allocate memory for a category
        pub async fn allocate(&self, category: &str, size: usize) -> RaftResult<MemoryAllocation> {
            let current = self.current_usage.load(Ordering::Acquire);
            
            if current + size > self.max_usage {
                return Err(RaftError::resource_exhausted(
                    format!("Memory allocation would exceed limit: {} + {} > {}", 
                           current, size, self.max_usage)
                ));
            }

            // Update current usage
            self.current_usage.fetch_add(size, Ordering::AcqRel);

            // Update category usage
            let mut usage_by_category = self.usage_by_category.write().await;
            *usage_by_category.entry(category.to_string()).or_insert(0) += size;

            Ok(MemoryAllocation {
                tracker: self,
                category: category.to_string(),
                size,
            })
        }

        /// Get current memory usage
        pub fn current_usage(&self) -> usize {
            self.current_usage.load(Ordering::Acquire)
        }

        /// Get memory usage percentage
        pub fn usage_percentage(&self) -> f64 {
            self.current_usage() as f64 / self.max_usage as f64
        }

        /// Check if under memory pressure
        pub fn is_under_pressure(&self, threshold: f64) -> bool {
            self.usage_percentage() > threshold
        }

        /// Get usage by category
        pub async fn get_usage_by_category(&self) -> std::collections::HashMap<String, usize> {
            self.usage_by_category.read().await.clone()
        }

        /// Internal method to deallocate memory
        async fn deallocate(&self, category: &str, size: usize) {
            self.current_usage.fetch_sub(size, Ordering::AcqRel);
            
            let mut usage_by_category = self.usage_by_category.write().await;
            if let Some(category_usage) = usage_by_category.get_mut(category) {
                *category_usage = category_usage.saturating_sub(size);
                if *category_usage == 0 {
                    usage_by_category.remove(category);
                }
            }
        }
    }

    /// RAII memory allocation guard
    pub struct MemoryAllocation<'a> {
        tracker: &'a MemoryTracker,
        category: String,
        size: usize,
    }

    impl<'a> Drop for MemoryAllocation<'a> {
        fn drop(&mut self) {
            // Use blocking call in drop - not ideal but necessary
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    self.tracker.deallocate(&self.category, self.size).await;
                });
            });
        }
    }

    /// Resource manager for Raft operations
    pub struct ResourceManager {
        config: ResourceConfig,
        memory_tracker: Arc<MemoryTracker>,
        operation_semaphore: Arc<Semaphore>,
        resource_stats: Arc<RwLock<ResourceStats>>,
        cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    }

    impl ResourceManager {
        /// Create a new resource manager
        pub fn new(config: ResourceConfig) -> Self {
            let memory_tracker = Arc::new(MemoryTracker::new(config.max_memory_usage));
            let operation_semaphore = Arc::new(Semaphore::new(config.max_concurrent_operations));

            Self {
                config,
                memory_tracker,
                operation_semaphore,
                resource_stats: Arc::new(RwLock::new(ResourceStats::default())),
                cleanup_handle: None,
            }
        }

        /// Start the resource manager
        pub async fn start(&mut self) -> RaftResult<()> {
            if self.cleanup_handle.is_some() {
                return Err(RaftError::invalid_state("Resource manager already started"));
            }

            let memory_tracker = Arc::clone(&self.memory_tracker);
            let resource_stats = Arc::clone(&self.resource_stats);
            let cleanup_interval = self.config.cleanup_interval;
            let memory_threshold = self.config.memory_pressure_threshold;

            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(cleanup_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    interval.tick().await;

                    // Update resource statistics
                    let mut stats = resource_stats.write().await;
                    stats.current_memory_usage = memory_tracker.current_usage();
                    stats.memory_usage_percentage = memory_tracker.usage_percentage();
                    stats.is_under_memory_pressure = memory_tracker.is_under_pressure(memory_threshold);
                    stats.memory_by_category = memory_tracker.get_usage_by_category().await;

                    // Log memory pressure warnings
                    if stats.is_under_memory_pressure {
                        log::warn!(
                            "Raft resource manager under memory pressure: {:.1}% usage",
                            stats.memory_usage_percentage * 100.0
                        );
                    }
                }
            });

            self.cleanup_handle = Some(handle);
            Ok(())
        }

        /// Stop the resource manager
        pub async fn stop(&mut self) {
            if let Some(handle) = self.cleanup_handle.take() {
                handle.abort();
            }
        }

        /// Acquire memory for an operation
        pub async fn acquire_memory(&self, category: &str, size: usize) -> RaftResult<MemoryAllocation> {
            // Check if we're under memory pressure
            if self.memory_tracker.is_under_pressure(self.config.memory_pressure_threshold) {
                let mut stats = self.resource_stats.write().await;
                stats.memory_pressure_events += 1;
                
                return Err(RaftError::resource_exhausted("System under memory pressure"));
            }

            self.memory_tracker.allocate(category, size).await
        }

        /// Acquire operation permit
        pub async fn acquire_operation_permit(&self) -> RaftResult<tokio::sync::SemaphorePermit> {
            match timeout(Duration::from_millis(100), self.operation_semaphore.acquire()).await {
                Ok(Ok(permit)) => {
                    let mut stats = self.resource_stats.write().await;
                    stats.active_operations += 1;
                    Ok(permit)
                }
                Ok(Err(_)) => Err(RaftError::resource_exhausted("Semaphore closed")),
                Err(_) => {
                    let mut stats = self.resource_stats.write().await;
                    stats.operation_timeouts += 1;
                    Err(RaftError::timeout("Operation permit acquisition timeout"))
                }
            }
        }

        /// Check if system can handle new operations
        pub async fn can_accept_operations(&self) -> bool {
            !self.memory_tracker.is_under_pressure(self.config.memory_pressure_threshold) &&
            self.operation_semaphore.available_permits() > 0
        }

        /// Get current resource statistics
        pub async fn get_stats(&self) -> ResourceStats {
            self.resource_stats.read().await.clone()
        }

        /// Force cleanup of resources
        pub async fn force_cleanup(&self) -> RaftResult<CleanupResult> {
            let mut cleanup_result = CleanupResult::default();

            // This is a simplified cleanup - in practice, you'd implement
            // more sophisticated cleanup strategies
            let current_usage = self.memory_tracker.current_usage();
            let usage_by_category = self.memory_tracker.get_usage_by_category().await;

            cleanup_result.memory_freed = 0; // Would implement actual cleanup
            cleanup_result.operations_cancelled = 0; // Would implement actual cancellation
            cleanup_result.categories_cleaned = usage_by_category.keys().len();

            log::info!(
                "Resource cleanup completed: {} bytes freed, {} operations cancelled",
                cleanup_result.memory_freed,
                cleanup_result.operations_cancelled
            );

            Ok(cleanup_result)
        }

        /// Get memory tracker reference
        pub fn memory_tracker(&self) -> Arc<MemoryTracker> {
            Arc::clone(&self.memory_tracker)
        }
    }

    /// Statistics for resource management
    #[derive(Debug, Clone, Default)]
    pub struct ResourceStats {
        pub current_memory_usage: usize,
        pub memory_usage_percentage: f64,
        pub is_under_memory_pressure: bool,
        pub memory_pressure_events: u64,
        pub active_operations: usize,
        pub operation_timeouts: u64,
        pub memory_by_category: std::collections::HashMap<String, usize>,
    }

    /// Result of cleanup operations
    #[derive(Debug, Clone, Default)]
    pub struct CleanupResult {
        pub memory_freed: usize,
        pub operations_cancelled: usize,
        pub categories_cleaned: usize,
    }

    /// Bounded queue for managing resource-intensive operations
    pub struct BoundedQueue<T> {
        queue: Arc<RwLock<VecDeque<T>>>,
        max_size: usize,
        current_memory: Arc<AtomicUsize>,
        max_memory: usize,
    }

    impl<T> BoundedQueue<T> 
    where 
        T: Send + Sync,
    {
        pub fn new(max_size: usize, max_memory: usize) -> Self {
            Self {
                queue: Arc::new(RwLock::new(VecDeque::new())),
                max_size,
                current_memory: Arc::new(AtomicUsize::new(0)),
                max_memory,
            }
        }

        /// Try to enqueue an item
        pub async fn try_enqueue(&self, item: T, item_size: usize) -> RaftResult<()> {
            let mut queue = self.queue.write().await;
            
            if queue.len() >= self.max_size {
                return Err(RaftError::resource_exhausted("Queue size limit exceeded"));
            }

            let current_memory = self.current_memory.load(Ordering::Acquire);
            if current_memory + item_size > self.max_memory {
                return Err(RaftError::resource_exhausted("Queue memory limit exceeded"));
            }

            queue.push_back(item);
            self.current_memory.fetch_add(item_size, Ordering::AcqRel);
            
            Ok(())
        }

        /// Dequeue an item
        pub async fn dequeue(&self, item_size: usize) -> Option<T> {
            let mut queue = self.queue.write().await;
            if let Some(item) = queue.pop_front() {
                self.current_memory.fetch_sub(item_size, Ordering::AcqRel);
                Some(item)
            } else {
                None
            }
        }

        /// Get queue statistics
        pub async fn stats(&self) -> QueueStats {
            let queue = self.queue.read().await;
            QueueStats {
                size: queue.len(),
                max_size: self.max_size,
                memory_usage: self.current_memory.load(Ordering::Acquire),
                max_memory: self.max_memory,
            }
        }
    }

    /// Statistics for bounded queues
    #[derive(Debug, Clone)]
    pub struct QueueStats {
        pub size: usize,
        pub max_size: usize,
        pub memory_usage: usize,
        pub max_memory: usize,
    }
}