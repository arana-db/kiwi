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

//! Message channel communication system for dual runtime architecture
//! 
//! This module provides the data structures and communication mechanisms
//! for passing storage requests between the network and storage runtimes.

use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

use resp::RespData;
use storage::error::Error as StorageError;

/// Unique identifier for tracking storage requests across runtime boundaries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(Uuid);

impl RequestId {
    /// Create a new unique request ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Get the inner UUID value
    pub fn inner(&self) -> Uuid {
        self.0
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Storage commands that can be executed in the storage runtime
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageCommand {
    /// Get a value by key
    Get { 
        key: Vec<u8> 
    },
    /// Set a key-value pair with optional TTL
    Set { 
        key: Vec<u8>, 
        value: Vec<u8>, 
        ttl: Option<Duration> 
    },
    /// Delete one or more keys
    Del { 
        keys: Vec<Vec<u8>> 
    },
    /// Check if keys exist
    Exists { 
        keys: Vec<Vec<u8>> 
    },
    /// Set expiration time for a key
    Expire { 
        key: Vec<u8>, 
        ttl: Duration 
    },
    /// Get time to live for a key
    Ttl { 
        key: Vec<u8> 
    },
    /// Increment a numeric value
    Incr { 
        key: Vec<u8> 
    },
    /// Increment by a specific amount
    IncrBy { 
        key: Vec<u8>, 
        increment: i64 
    },
    /// Decrement a numeric value
    Decr { 
        key: Vec<u8> 
    },
    /// Decrement by a specific amount
    DecrBy { 
        key: Vec<u8>, 
        decrement: i64 
    },
    /// Multiple set operations
    MSet { 
        pairs: Vec<(Vec<u8>, Vec<u8>)> 
    },
    /// Multiple get operations
    MGet { 
        keys: Vec<Vec<u8>> 
    },
    /// Batch multiple commands together
    Batch { 
        commands: Vec<StorageCommand> 
    },
}

/// Statistics about storage operations for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    /// Number of keys read during the operation
    pub keys_read: u64,
    /// Number of keys written during the operation
    pub keys_written: u64,
    /// Number of keys deleted during the operation
    pub keys_deleted: u64,
    /// Size of data read in bytes
    pub bytes_read: u64,
    /// Size of data written in bytes
    pub bytes_written: u64,
    /// Whether the operation hit the cache
    pub cache_hit: bool,
    /// RocksDB compaction level accessed
    pub compaction_level: Option<u32>,
}

impl Default for StorageStats {
    fn default() -> Self {
        Self {
            keys_read: 0,
            keys_written: 0,
            keys_deleted: 0,
            bytes_read: 0,
            bytes_written: 0,
            cache_hit: false,
            compaction_level: None,
        }
    }
}

/// Request sent from network runtime to storage runtime
#[derive(Debug)]
pub struct StorageRequest {
    /// Unique identifier for this request
    pub id: RequestId,
    /// The storage command to execute
    pub command: StorageCommand,
    /// Channel to send the response back
    pub response_channel: oneshot::Sender<StorageResponse>,
    /// Request timeout duration
    pub timeout: Duration,
    /// Timestamp when the request was created
    pub timestamp: Instant,
    /// Priority level for request processing
    pub priority: RequestPriority,
}

/// Priority levels for storage request processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RequestPriority {
    /// Low priority requests (background operations)
    Low = 0,
    /// Normal priority requests (regular client operations)
    Normal = 1,
    /// High priority requests (critical operations)
    High = 2,
    /// Critical priority requests (system operations)
    Critical = 3,
}

impl Default for RequestPriority {
    fn default() -> Self {
        RequestPriority::Normal
    }
}

/// Response sent from storage runtime back to network runtime
#[derive(Debug)]
pub struct StorageResponse {
    /// Request ID this response corresponds to
    pub id: RequestId,
    /// Result of the storage operation
    pub result: Result<RespData, StorageError>,
    /// Time taken to execute the storage operation
    pub execution_time: Duration,
    /// Statistics about the storage operation
    pub storage_stats: StorageStats,
}

/// Message channel for communication between network and storage runtimes
pub struct MessageChannel {
    /// Sender for storage requests (used by network runtime)
    request_sender: mpsc::Sender<StorageRequest>,
    /// Receiver for storage requests (used by storage runtime)
    request_receiver: Option<mpsc::Receiver<StorageRequest>>,
    /// Buffer size for the request channel
    buffer_size: usize,
    /// Channel statistics for monitoring
    stats: Arc<Mutex<ChannelStats>>,
    /// Configuration for backpressure handling
    backpressure_config: BackpressureConfig,
}

/// Configuration for backpressure handling
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Threshold percentage (0-100) at which backpressure kicks in
    pub threshold_percent: u8,
    /// Maximum time to wait when channel is full before giving up
    pub max_wait_time: Duration,
    /// Whether to drop oldest requests when channel is full
    pub drop_oldest_on_full: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            threshold_percent: 80,
            max_wait_time: Duration::from_millis(100),
            drop_oldest_on_full: false,
        }
    }
}

/// Configuration for retry logic
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: usize,
    /// Base delay between retries
    pub base_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Whether to add jitter to retry delays
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }
}

/// Detailed error information for failed requests
#[derive(Debug, Clone)]
pub struct RequestError {
    /// The original request ID
    pub request_id: RequestId,
    /// The error that occurred
    pub error: String,
    /// Number of retry attempts made
    pub retry_attempts: usize,
    /// Total time spent on the request
    pub total_time: Duration,
    /// Timestamp when the error occurred
    pub timestamp: Instant,
}

/// Circuit breaker for handling repeated failures
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    /// Number of consecutive failures before opening
    failure_threshold: usize,
    /// Time to wait before attempting to close the circuit
    recovery_timeout: Duration,
    /// Current state of the circuit breaker
    state: CircuitBreakerState,
    /// Number of consecutive failures
    failure_count: usize,
    /// Timestamp when the circuit was opened
    opened_at: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitBreakerState {
    Closed,  // Normal operation
    Open,    // Failing fast
    HalfOpen, // Testing if service has recovered
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(failure_threshold: usize, recovery_timeout: Duration) -> Self {
        Self {
            failure_threshold,
            recovery_timeout,
            state: CircuitBreakerState::Closed,
            failure_count: 0,
            opened_at: None,
        }
    }

    /// Check if a request should be allowed through
    pub fn should_allow_request(&mut self) -> bool {
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                if let Some(opened_at) = self.opened_at {
                    if opened_at.elapsed() >= self.recovery_timeout {
                        self.state = CircuitBreakerState::HalfOpen;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitBreakerState::HalfOpen => true,
        }
    }

    /// Record a successful request
    pub fn record_success(&mut self) {
        self.failure_count = 0;
        self.state = CircuitBreakerState::Closed;
        self.opened_at = None;
    }

    /// Record a failed request
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        if self.failure_count >= self.failure_threshold {
            self.state = CircuitBreakerState::Open;
            self.opened_at = Some(Instant::now());
        }
    }

    /// Get the current state
    pub fn state(&self) -> &CircuitBreakerState {
        &self.state
    }
}

/// Statistics for monitoring channel health and performance
#[derive(Debug, Clone, Default)]
pub struct ChannelStats {
    /// Total number of requests sent
    pub requests_sent: u64,
    /// Total number of requests received
    pub requests_received: u64,
    /// Total number of responses sent
    pub responses_sent: u64,
    /// Total number of requests that timed out
    pub requests_timeout: u64,
    /// Total number of channel send failures
    pub send_failures: u64,
    /// Current number of pending requests
    pub pending_requests: u64,
    /// Maximum pending requests seen
    pub max_pending_requests: u64,
    /// Number of times backpressure was applied
    pub backpressure_events: u64,
    /// Average request processing time
    pub avg_processing_time: Duration,
}

impl MessageChannel {
    /// Create a new message channel with the specified buffer size
    pub fn new(buffer_size: usize) -> Self {
        Self::with_backpressure_config(buffer_size, BackpressureConfig::default())
    }

    /// Create a new message channel with custom backpressure configuration
    pub fn with_backpressure_config(buffer_size: usize, backpressure_config: BackpressureConfig) -> Self {
        let (request_sender, request_receiver) = mpsc::channel(buffer_size);
        
        Self {
            request_sender,
            request_receiver: Some(request_receiver),
            buffer_size,
            stats: Arc::new(Mutex::new(ChannelStats::default())),
            backpressure_config,
        }
    }

    /// Get the request sender (for network runtime)
    pub fn request_sender(&self) -> mpsc::Sender<StorageRequest> {
        self.request_sender.clone()
    }

    /// Take the request receiver (for storage runtime)
    /// This can only be called once as the receiver is moved
    pub fn take_request_receiver(&mut self) -> Option<mpsc::Receiver<StorageRequest>> {
        self.request_receiver.take()
    }

    /// Get the buffer size of the channel
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get current channel statistics
    pub async fn stats(&self) -> ChannelStats {
        self.stats.lock().await.clone()
    }

    /// Check if the channel is healthy (not closed and within capacity)
    pub fn is_healthy(&self) -> bool {
        !self.request_sender.is_closed()
    }

    /// Get the current number of pending requests in the channel
    pub fn pending_requests(&self) -> usize {
        self.request_sender.capacity() - self.request_sender.max_capacity()
    }

    /// Check if the channel is experiencing backpressure
    pub fn has_backpressure(&self) -> bool {
        let threshold = (self.buffer_size * self.backpressure_config.threshold_percent as usize) / 100;
        self.pending_requests() >= threshold
    }

    /// Get the backpressure configuration
    pub fn backpressure_config(&self) -> &BackpressureConfig {
        &self.backpressure_config
    }

    /// Update statistics when a request is sent
    pub async fn record_request_sent(&self) {
        let mut stats = self.stats.lock().await;
        stats.requests_sent += 1;
        stats.pending_requests += 1;
        stats.max_pending_requests = stats.max_pending_requests.max(stats.pending_requests);
        
        if self.has_backpressure() {
            stats.backpressure_events += 1;
        }
    }

    /// Update statistics when a request is received
    pub async fn record_request_received(&self) {
        let mut stats = self.stats.lock().await;
        stats.requests_received += 1;
    }

    /// Update statistics when a response is sent
    pub async fn record_response_sent(&self, processing_time: Duration) {
        let mut stats = self.stats.lock().await;
        stats.responses_sent += 1;
        stats.pending_requests = stats.pending_requests.saturating_sub(1);
        
        // Update average processing time using exponential moving average
        if stats.avg_processing_time.is_zero() {
            stats.avg_processing_time = processing_time;
        } else {
            let alpha = 0.1; // Smoothing factor
            let current_nanos = stats.avg_processing_time.as_nanos() as f64;
            let new_nanos = processing_time.as_nanos() as f64;
            let updated_nanos = (alpha * new_nanos + (1.0 - alpha) * current_nanos) as u64;
            stats.avg_processing_time = Duration::from_nanos(updated_nanos);
        }
    }

    /// Update statistics when a request times out
    pub async fn record_timeout(&self) {
        let mut stats = self.stats.lock().await;
        stats.requests_timeout += 1;
        stats.pending_requests = stats.pending_requests.saturating_sub(1);
    }

    /// Update statistics when a send operation fails
    pub async fn record_send_failure(&self) {
        let mut stats = self.stats.lock().await;
        stats.send_failures += 1;
    }
}

/// Storage client for sending requests from network runtime to storage runtime
pub struct StorageClient {
    /// Channel for sending storage requests
    message_channel: Arc<MessageChannel>,
    /// Map of pending requests waiting for responses
    pending_requests: Arc<Mutex<HashMap<RequestId, oneshot::Receiver<StorageResponse>>>>,
    /// Default timeout for storage requests
    default_timeout: Duration,
    /// Retry configuration
    retry_config: RetryConfig,
    /// Circuit breaker for handling repeated failures
    circuit_breaker: Arc<Mutex<CircuitBreaker>>,
}

impl StorageClient {
    /// Create a new storage client with the given message channel
    pub fn new(message_channel: Arc<MessageChannel>, default_timeout: Duration) -> Self {
        Self::with_retry_config(message_channel, default_timeout, RetryConfig::default())
    }

    /// Create a new storage client with custom retry configuration
    pub fn with_retry_config(
        message_channel: Arc<MessageChannel>, 
        default_timeout: Duration,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            message_channel,
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            default_timeout,
            retry_config,
            circuit_breaker: Arc::new(Mutex::new(CircuitBreaker::new(5, Duration::from_secs(30)))),
        }
    }

    /// Send a storage request and wait for the response
    pub async fn send_request(&self, command: StorageCommand) -> Result<RespData, crate::error::DualRuntimeError> {
        self.send_request_with_timeout(command, self.default_timeout).await
    }

    /// Send a storage request with a custom timeout
    pub async fn send_request_with_timeout(
        &self, 
        command: StorageCommand, 
        timeout: Duration
    ) -> Result<RespData, crate::error::DualRuntimeError> {
        self.send_request_with_priority(command, timeout, RequestPriority::Normal).await
    }

    /// Send a storage request with custom timeout and priority
    pub async fn send_request_with_priority(
        &self,
        command: StorageCommand,
        timeout: Duration,
        priority: RequestPriority,
    ) -> Result<RespData, crate::error::DualRuntimeError> {
        let start_time = Instant::now();
        let mut last_error = None;

        // Check circuit breaker
        {
            let mut circuit_breaker = self.circuit_breaker.lock().await;
            if !circuit_breaker.should_allow_request() {
                return Err(crate::error::DualRuntimeError::Channel(
                    "Circuit breaker is open - too many failures".to_string()
                ));
            }
        }

        for attempt in 0..=self.retry_config.max_retries {
            // Check if we have enough time left for this attempt
            let elapsed = start_time.elapsed();
            if elapsed >= timeout {
                break;
            }
            let remaining_timeout = timeout - elapsed;

            match self.try_send_request(command.clone(), remaining_timeout, priority).await {
                Ok(data) => {
                    // Success - record in circuit breaker and return
                    let mut circuit_breaker = self.circuit_breaker.lock().await;
                    circuit_breaker.record_success();
                    return Ok(data);
                }
                Err(err) => {
                    last_error = Some(err);
                    
                    // Don't retry on certain errors
                    if let Some(ref error) = last_error {
                        match error {
                            crate::error::DualRuntimeError::Storage(_) => {
                                // Storage errors are not retryable
                                break;
                            }
                            _ => {
                                // Other errors might be retryable
                            }
                        }
                    }

                    // If this is not the last attempt, wait before retrying
                    if attempt < self.retry_config.max_retries {
                        let delay = self.calculate_retry_delay(attempt);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        // All retries failed - record failure in circuit breaker
        {
            let mut circuit_breaker = self.circuit_breaker.lock().await;
            circuit_breaker.record_failure();
        }

        // Return the last error
        Err(last_error.unwrap_or_else(|| {
            crate::error::DualRuntimeError::Channel(
                "Request failed after all retry attempts".to_string()
            )
        }))
    }

    /// Try to send a single request without retry logic
    async fn try_send_request(
        &self,
        command: StorageCommand,
        timeout: Duration,
        priority: RequestPriority,
    ) -> Result<RespData, crate::error::DualRuntimeError> {
        let request_id = RequestId::new();
        let (response_sender, response_receiver) = oneshot::channel();

        let request = StorageRequest {
            id: request_id,
            command,
            response_channel: response_sender,
            timeout,
            timestamp: Instant::now(),
            priority,
        };

        // Store the response receiver for tracking
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(request_id, response_receiver);
        }

        // Try to send the request with backpressure handling
        let send_result = if self.message_channel.has_backpressure() {
            // Apply backpressure handling
            tokio::time::timeout(
                self.message_channel.backpressure_config.max_wait_time,
                self.message_channel.request_sender.send(request)
            ).await
        } else {
            // Send immediately
            Ok(self.message_channel.request_sender.send(request).await)
        };

        match send_result {
            Ok(Ok(())) => {
                // Request sent successfully, record statistics
                self.message_channel.record_request_sent().await;

                // Wait for response with timeout
                let response_receiver = {
                    let mut pending = self.pending_requests.lock().await;
                    pending.remove(&request_id)
                        .ok_or_else(|| crate::error::DualRuntimeError::Channel(
                            "Response receiver not found".to_string()
                        ))?
                };

                match tokio::time::timeout(timeout, response_receiver).await {
                    Ok(Ok(response)) => {
                        match response.result {
                            Ok(data) => Ok(data),
                            Err(storage_err) => Err(crate::error::DualRuntimeError::Storage(storage_err)),
                        }
                    }
                    Ok(Err(_)) => {
                        // Response channel was closed
                        Err(crate::error::DualRuntimeError::Channel(
                            "Response channel closed".to_string()
                        ))
                    }
                    Err(_) => {
                        // Timeout occurred
                        self.message_channel.record_timeout().await;
                        Err(crate::error::DualRuntimeError::Timeout { timeout })
                    }
                }
            }
            Ok(Err(_)) => {
                // Channel send failed
                self.message_channel.record_send_failure().await;
                // Clean up pending request
                let mut pending = self.pending_requests.lock().await;
                pending.remove(&request_id);
                Err(crate::error::DualRuntimeError::Channel(
                    "Failed to send request to storage runtime".to_string()
                ))
            }
            Err(_) => {
                // Timeout during backpressure handling
                self.message_channel.record_send_failure().await;
                // Clean up pending request
                let mut pending = self.pending_requests.lock().await;
                pending.remove(&request_id);
                Err(crate::error::DualRuntimeError::Channel(
                    "Request send timeout due to backpressure".to_string()
                ))
            }
        }
    }

    /// Calculate the delay for the next retry attempt using exponential backoff
    fn calculate_retry_delay(&self, attempt: usize) -> Duration {
        let base_delay_ms = self.retry_config.base_delay.as_millis() as f64;
        let delay_ms = base_delay_ms * self.retry_config.backoff_multiplier.powi(attempt as i32);
        let delay_ms = delay_ms.min(self.retry_config.max_delay.as_millis() as f64);

        let mut delay = Duration::from_millis(delay_ms as u64);

        // Add jitter if enabled
        if self.retry_config.jitter {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            
            let mut hasher = DefaultHasher::new();
            std::thread::current().id().hash(&mut hasher);
            Instant::now().hash(&mut hasher);
            let jitter_factor = (hasher.finish() % 100) as f64 / 100.0; // 0.0 to 0.99
            
            let jitter_ms = (delay.as_millis() as f64 * jitter_factor * 0.1) as u64; // Up to 10% jitter
            delay += Duration::from_millis(jitter_ms);
        }

        delay
    }

    /// Get the number of pending requests
    pub async fn pending_request_count(&self) -> usize {
        self.pending_requests.lock().await.len()
    }

    /// Check if the storage client is healthy
    pub fn is_healthy(&self) -> bool {
        self.message_channel.is_healthy()
    }

    /// Get channel statistics
    pub async fn channel_stats(&self) -> ChannelStats {
        self.message_channel.stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_creation() {
        let id1 = RequestId::new();
        let id2 = RequestId::new();
        
        assert_ne!(id1, id2);
        assert_ne!(id1.inner(), id2.inner());
    }

    #[test]
    fn test_request_id_display() {
        let id = RequestId::new();
        let display_str = format!("{}", id);
        let uuid_str = format!("{}", id.inner());
        
        assert_eq!(display_str, uuid_str);
    }

    #[test]
    fn test_request_priority_ordering() {
        assert!(RequestPriority::Critical > RequestPriority::High);
        assert!(RequestPriority::High > RequestPriority::Normal);
        assert!(RequestPriority::Normal > RequestPriority::Low);
    }

    #[test]
    fn test_storage_stats_default() {
        let stats = StorageStats::default();
        
        assert_eq!(stats.keys_read, 0);
        assert_eq!(stats.keys_written, 0);
        assert_eq!(stats.keys_deleted, 0);
        assert_eq!(stats.bytes_read, 0);
        assert_eq!(stats.bytes_written, 0);
        assert!(!stats.cache_hit);
        assert_eq!(stats.compaction_level, None);
    }

    #[tokio::test]
    async fn test_message_channel_creation() {
        let channel = MessageChannel::new(1000);
        
        assert_eq!(channel.buffer_size(), 1000);
        assert!(channel.is_healthy());
        assert!(!channel.has_backpressure());
    }

    #[tokio::test]
    async fn test_message_channel_with_backpressure_config() {
        let config = BackpressureConfig {
            threshold_percent: 90,
            max_wait_time: Duration::from_millis(200),
            drop_oldest_on_full: true,
        };
        let channel = MessageChannel::with_backpressure_config(100, config.clone());
        
        assert_eq!(channel.buffer_size(), 100);
        assert_eq!(channel.backpressure_config().threshold_percent, 90);
        assert_eq!(channel.backpressure_config().max_wait_time, Duration::from_millis(200));
        assert!(channel.backpressure_config().drop_oldest_on_full);
    }

    #[tokio::test]
    async fn test_message_channel_stats() {
        let channel = MessageChannel::new(100);
        let stats = channel.stats().await;
        
        assert_eq!(stats.requests_sent, 0);
        assert_eq!(stats.requests_received, 0);
        assert_eq!(stats.responses_sent, 0);
        assert_eq!(stats.requests_timeout, 0);
        assert_eq!(stats.send_failures, 0);
        assert_eq!(stats.pending_requests, 0);
    }

    #[tokio::test]
    async fn test_channel_statistics_recording() {
        let channel = MessageChannel::new(100);
        
        // Record some operations
        channel.record_request_sent().await;
        channel.record_request_received().await;
        channel.record_response_sent(Duration::from_millis(10)).await;
        
        let stats = channel.stats().await;
        assert_eq!(stats.requests_sent, 1);
        assert_eq!(stats.requests_received, 1);
        assert_eq!(stats.responses_sent, 1);
        assert_eq!(stats.pending_requests, 0);
        assert_eq!(stats.avg_processing_time, Duration::from_millis(10));
    }

    #[test]
    fn test_storage_command_serialization() {
        let cmd = StorageCommand::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        };
        
        // Test that the command can be serialized and deserialized
        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: StorageCommand = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            StorageCommand::Set { key, value, ttl } => {
                assert_eq!(key, b"test_key");
                assert_eq!(value, b"test_value");
                assert_eq!(ttl, Some(Duration::from_secs(60)));
            }
            _ => panic!("Unexpected command type"),
        }
    }

    #[tokio::test]
    async fn test_storage_client_creation() {
        let channel = Arc::new(MessageChannel::new(100));
        let client = StorageClient::new(channel.clone(), Duration::from_secs(30));
        
        assert!(client.is_healthy());
        assert_eq!(client.pending_request_count().await, 0);
    }

    #[tokio::test]
    async fn test_backpressure_config_default() {
        let config = BackpressureConfig::default();
        
        assert_eq!(config.threshold_percent, 80);
        assert_eq!(config.max_wait_time, Duration::from_millis(100));
        assert!(!config.drop_oldest_on_full);
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay, Duration::from_millis(10));
        assert_eq!(config.max_delay, Duration::from_secs(1));
        assert_eq!(config.backoff_multiplier, 2.0);
        assert!(config.jitter);
    }

    #[test]
    fn test_circuit_breaker() {
        let mut circuit_breaker = CircuitBreaker::new(3, Duration::from_secs(10));
        
        // Initially closed
        assert!(circuit_breaker.should_allow_request());
        
        // Record failures
        circuit_breaker.record_failure();
        assert!(circuit_breaker.should_allow_request());
        
        circuit_breaker.record_failure();
        assert!(circuit_breaker.should_allow_request());
        
        circuit_breaker.record_failure();
        // Should now be open
        assert!(!circuit_breaker.should_allow_request());
        
        // Record success should close it
        circuit_breaker.record_success();
        assert!(circuit_breaker.should_allow_request());
    }

    #[tokio::test]
    async fn test_storage_client_with_retry_config() {
        let channel = Arc::new(MessageChannel::new(100));
        let retry_config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(500),
            backoff_multiplier: 1.5,
            jitter: false,
        };
        let client = StorageClient::with_retry_config(
            channel.clone(), 
            Duration::from_secs(30), 
            retry_config
        );
        
        assert!(client.is_healthy());
        assert_eq!(client.pending_request_count().await, 0);
    }
}