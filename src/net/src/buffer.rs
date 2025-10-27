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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use log::debug;
use tokio::sync::Mutex;
use tokio::time::interval;

/// Configuration for buffer management
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Initial buffer size for new buffers
    pub initial_buffer_size: usize,
    /// Maximum buffer size before splitting
    pub max_buffer_size: usize,
    /// Maximum number of buffers to keep in pool
    pub max_pool_size: usize,
    /// Minimum number of buffers to keep in pool
    pub min_pool_size: usize,
    /// Time after which unused buffers are cleaned up
    pub cleanup_interval: Duration,
    /// Maximum idle time before buffer is removed from pool
    pub max_idle_time: Duration,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            initial_buffer_size: 8192,      // 8KB initial size
            max_buffer_size: 1024 * 1024,   // 1MB max size
            max_pool_size: 100,
            min_pool_size: 10,
            cleanup_interval: Duration::from_secs(60),
            max_idle_time: Duration::from_secs(300),
        }
    }
}

/// A pooled buffer with metadata
#[derive(Debug)]
pub struct PooledBuffer {
    /// The actual buffer
    pub buffer: BytesMut,
    /// When this buffer was last used
    pub last_used: Instant,
    /// Unique identifier for this buffer
    pub id: u64,
    /// Original capacity when buffer was created
    pub original_capacity: usize,
}

impl PooledBuffer {
    pub fn new(capacity: usize, id: u64) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
            last_used: Instant::now(),
            id,
            original_capacity: capacity,
        }
    }

    pub fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    pub fn is_idle(&self, max_idle_time: Duration) -> bool {
        self.last_used.elapsed() > max_idle_time
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.touch();
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Check if buffer has grown too large and should be replaced
    pub fn should_replace(&self, max_size: usize) -> bool {
        self.buffer.capacity() > max_size
    }
}

/// Buffer manager for efficient memory usage
pub struct BufferManager {
    config: BufferConfig,
    /// Pool of available buffers
    buffer_pool: Arc<Mutex<VecDeque<PooledBuffer>>>,
    /// Buffer counter for unique IDs
    buffer_counter: Arc<Mutex<u64>>,
    /// Statistics
    stats: Arc<Mutex<BufferStats>>,
}

impl BufferManager {
    pub fn new(config: BufferConfig) -> Self {
        let manager = Self {
            config: config.clone(),
            buffer_pool: Arc::new(Mutex::new(VecDeque::new())),
            buffer_counter: Arc::new(Mutex::new(0)),
            stats: Arc::new(Mutex::new(BufferStats::default())),
        };

        // Start cleanup task
        manager.start_cleanup_task();

        // Pre-populate pool with minimum buffers in a background task
        // We'll use a tokio handle to spawn it if available
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            manager_clone.populate_initial_buffers().await;
        });

        manager
    }
    
    /// Create a new BufferManager and wait for initial population
    pub async fn new_with_init(config: BufferConfig) -> Self {
        let manager = Self {
            config: config.clone(),
            buffer_pool: Arc::new(Mutex::new(VecDeque::new())),
            buffer_counter: Arc::new(Mutex::new(0)),
            stats: Arc::new(Mutex::new(BufferStats::default())),
        };

        // Pre-populate pool synchronously
        manager.populate_initial_buffers().await;
        
        // Start cleanup task after population
        manager.start_cleanup_task();

        manager
    }

    /// Get a buffer from the pool or create a new one
    pub async fn get_buffer(&self) -> PooledBuffer {
        let mut pool = self.buffer_pool.lock().await;
        let mut stats = self.stats.lock().await;

        // Try to get an existing buffer from pool
        while let Some(mut buffer) = pool.pop_front() {
            // Check if buffer is still good to use
            if !buffer.should_replace(self.config.max_buffer_size) {
                buffer.reset();
                stats.pool_hits += 1;
                return buffer;
            }
            // Buffer too large, let it drop and try next
            stats.oversized_dropped += 1;
        }

        // No suitable buffer in pool, create new one
        let mut counter = self.buffer_counter.lock().await;
        *counter += 1;
        let id = *counter;
        drop(counter);

        stats.pool_misses += 1;
        stats.buffers_created += 1;

        PooledBuffer::new(self.config.initial_buffer_size, id)
    }

    /// Return a buffer to the pool
    pub async fn return_buffer(&self, mut buffer: PooledBuffer) {
        let mut pool = self.buffer_pool.lock().await;
        let mut stats = self.stats.lock().await;

        // Don't return oversized buffers to pool
        if buffer.should_replace(self.config.max_buffer_size) {
            stats.oversized_dropped += 1;
            return;
        }

        // Don't exceed max pool size
        if pool.len() >= self.config.max_pool_size {
            stats.pool_full_drops += 1;
            return;
        }

        buffer.reset();
        pool.push_back(buffer);
        stats.buffers_returned += 1;
    }

    /// Create a zero-copy slice from buffer
    pub fn create_bytes_slice(&self, buffer: &PooledBuffer, start: usize, len: usize) -> Option<Bytes> {
        if start + len <= buffer.len() {
            Some(Bytes::copy_from_slice(&buffer.buffer[start..start + len]))
        } else {
            None
        }
    }

    /// Efficiently copy data between buffers
    pub fn copy_data(&self, src: &PooledBuffer, dst: &mut PooledBuffer, src_start: usize, len: usize) -> bool {
        if src_start + len <= src.len() {
            dst.buffer.extend_from_slice(&src.buffer[src_start..src_start + len]);
            true
        } else {
            false
        }
    }

    /// Get buffer statistics
    pub async fn stats(&self) -> BufferStats {
        let stats = self.stats.lock().await;
        let pool = self.buffer_pool.lock().await;
        
        BufferStats {
            pool_size: pool.len(),
            pool_hits: stats.pool_hits,
            pool_misses: stats.pool_misses,
            buffers_created: stats.buffers_created,
            buffers_returned: stats.buffers_returned,
            oversized_dropped: stats.oversized_dropped,
            pool_full_drops: stats.pool_full_drops,
            cleanups_performed: stats.cleanups_performed,
        }
    }

    /// Start background cleanup task
    fn start_cleanup_task(&self) {
        let pool = self.buffer_pool.clone();
        let stats = self.stats.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut cleanup_interval = interval(config.cleanup_interval);
            
            loop {
                cleanup_interval.tick().await;
                
                let mut pool_guard = pool.lock().await;
                let mut stats_guard = stats.lock().await;
                
                let _original_size = pool_guard.len();
                let mut cleaned_count = 0;
                
                // Keep only non-idle buffers, but maintain minimum pool size
                let mut kept_buffers = VecDeque::new();
                
                while let Some(buffer) = pool_guard.pop_front() {
                    if buffer.is_idle(config.max_idle_time) && 
                       (kept_buffers.len() + pool_guard.len()) > config.min_pool_size {
                        cleaned_count += 1;
                        // Buffer will be dropped
                    } else {
                        kept_buffers.push_back(buffer);
                    }
                }
                
                *pool_guard = kept_buffers;
                stats_guard.cleanups_performed += 1;
                
                if cleaned_count > 0 {
                    debug!("Buffer cleanup: removed {} idle buffers, {} remaining", 
                           cleaned_count, pool_guard.len());
                }
            }
        });
    }

    /// Populate pool with initial buffers
    async fn populate_initial_buffers(&self) {
        let mut pool = self.buffer_pool.lock().await;
        let mut counter = self.buffer_counter.lock().await;
        let mut stats = self.stats.lock().await;
        
        for _ in 0..self.config.min_pool_size {
            *counter += 1;
            let buffer = PooledBuffer::new(self.config.initial_buffer_size, *counter);
            pool.push_back(buffer);
            stats.buffers_created += 1;
        }
        
        debug!("Pre-populated buffer pool with {} buffers", self.config.min_pool_size);
    }
}

impl Clone for BufferManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            buffer_pool: self.buffer_pool.clone(),
            buffer_counter: self.buffer_counter.clone(),
            stats: self.stats.clone(),
        }
    }
}

/// Buffer pool statistics
#[derive(Debug, Clone, Default)]
pub struct BufferStats {
    pub pool_size: usize,
    pub pool_hits: u64,
    pub pool_misses: u64,
    pub buffers_created: u64,
    pub buffers_returned: u64,
    pub oversized_dropped: u64,
    pub pool_full_drops: u64,
    pub cleanups_performed: u64,
}

/// Optimized buffer reader for streaming data
pub struct BufferedReader {
    buffer_manager: BufferManager,
    current_buffer: Option<PooledBuffer>,
    read_position: usize,
}

impl BufferedReader {
    pub fn new(buffer_manager: BufferManager) -> Self {
        Self {
            buffer_manager,
            current_buffer: None,
            read_position: 0,
        }
    }

    /// Read data into buffer, returning number of bytes read
    pub async fn read_data(&mut self, data: &[u8]) -> usize {
        if self.current_buffer.is_none() {
            self.current_buffer = Some(self.buffer_manager.get_buffer().await);
            self.read_position = 0;
        }

        if let Some(ref mut buffer) = self.current_buffer {
            let available_space = buffer.buffer.capacity() - buffer.buffer.len();
            let bytes_to_copy = std::cmp::min(data.len(), available_space);
            
            buffer.buffer.extend_from_slice(&data[..bytes_to_copy]);
            buffer.touch();
            
            bytes_to_copy
        } else {
            0
        }
    }

    /// Get available data as bytes slice
    pub fn available_data(&self) -> Option<&[u8]> {
        if let Some(ref buffer) = self.current_buffer {
            if self.read_position < buffer.len() {
                Some(&buffer.buffer[self.read_position..])
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Consume bytes from buffer
    pub fn consume(&mut self, bytes: usize) {
        if let Some(ref buffer) = self.current_buffer {
            self.read_position = std::cmp::min(self.read_position + bytes, buffer.len());
        }
    }

    /// Reset buffer for reuse
    pub async fn reset(&mut self) {
        if let Some(buffer) = self.current_buffer.take() {
            self.buffer_manager.return_buffer(buffer).await;
        }
        self.read_position = 0;
    }

    /// Check if buffer needs to be reset due to fragmentation
    pub fn needs_compaction(&self) -> bool {
        if let Some(ref buffer) = self.current_buffer {
            // Reset if we've consumed more than half the buffer
            self.read_position > buffer.len() / 2
        } else {
            false
        }
    }

    /// Compact buffer by moving unread data to beginning
    pub async fn compact(&mut self) {
        if let Some(mut buffer) = self.current_buffer.take() {
            if self.read_position > 0 && self.read_position < buffer.len() {
                // Move unread data to beginning
                let unread_len = buffer.len() - self.read_position;
                buffer.buffer.copy_within(self.read_position.., 0);
                buffer.buffer.truncate(unread_len);
                self.read_position = 0;
                buffer.touch();
                self.current_buffer = Some(buffer);
            } else {
                // Return buffer and get a fresh one
                self.buffer_manager.return_buffer(buffer).await;
                self.current_buffer = Some(self.buffer_manager.get_buffer().await);
                self.read_position = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_buffer_manager_basic() {
        let config = BufferConfig {
            initial_buffer_size: 1024,
            max_pool_size: 5,
            min_pool_size: 2,
            ..Default::default()
        };
        
        let manager = BufferManager::new(config);
        
        // Get a buffer
        let buffer1 = manager.get_buffer().await;
        assert_eq!(buffer1.capacity(), 1024);
        assert!(buffer1.is_empty());
        
        // Return buffer
        manager.return_buffer(buffer1).await;
        
        // Get another buffer - should reuse
        let buffer2 = manager.get_buffer().await;
        assert_eq!(buffer2.id, 1); // Should be the same buffer
    }

    #[tokio::test]
    async fn test_buffer_stats() {
        let config = BufferConfig {
            max_pool_size: 2,
            min_pool_size: 1,
            ..Default::default()
        };
        
        let manager = BufferManager::new_with_init(config).await;
        
        let stats = manager.stats().await;
        assert!(stats.buffers_created > 0);
    }

    #[tokio::test]
    async fn test_buffered_reader() {
        let config = BufferConfig::default();
        let manager = BufferManager::new(config);
        let mut reader = BufferedReader::new(manager);
        
        // Read some data
        let data = b"Hello, World!";
        let bytes_read = reader.read_data(data).await;
        assert_eq!(bytes_read, data.len());
        
        // Check available data
        let available = reader.available_data().unwrap();
        assert_eq!(available, data);
        
        // Consume some bytes
        reader.consume(5);
        let remaining = reader.available_data().unwrap();
        assert_eq!(remaining, b", World!");
    }

    #[tokio::test]
    async fn test_zero_copy_slice() {
        let config = BufferConfig::default();
        let manager = BufferManager::new(config);
        let mut buffer = manager.get_buffer().await;
        
        // Add some data
        buffer.buffer.extend_from_slice(b"Hello, World!");
        
        // Create zero-copy slice
        let slice = manager.create_bytes_slice(&buffer, 0, 5).unwrap();
        assert_eq!(slice.as_ref(), b"Hello");
        
        // Original buffer still has all data
        assert_eq!(buffer.buffer.as_ref(), b"Hello, World!");
    }
}