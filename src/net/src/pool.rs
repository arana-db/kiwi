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

use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;

/// Configuration for connection pool
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_connections: usize,
    /// Maximum time to wait for a connection from the pool
    pub connection_timeout: Duration,
    /// Maximum idle time before a connection is closed
    pub idle_timeout: Duration,
    /// Minimum number of connections to keep in the pool
    pub min_connections: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300), // 5 minutes
            min_connections: 10,
        }
    }
}

/// Represents a pooled connection with metadata
#[derive(Debug)]
pub struct PooledConnection<T> {
    /// The actual connection
    pub connection: T,
    /// When this connection was last used
    pub last_used: Instant,
    /// Unique identifier for this connection
    pub id: u64,
}

impl<T> PooledConnection<T> {
    pub fn new(connection: T, id: u64) -> Self {
        Self {
            connection,
            last_used: Instant::now(),
            id,
        }
    }

    pub fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    pub fn is_idle(&self, idle_timeout: Duration) -> bool {
        self.last_used.elapsed() > idle_timeout
    }
}

/// Generic connection pool implementation
pub struct ConnectionPool<T> {
    /// Pool configuration
    config: PoolConfig,
    /// Available connections
    available: Arc<Mutex<VecDeque<PooledConnection<T>>>>,
    /// Semaphore to limit concurrent connections
    semaphore: Arc<Semaphore>,
    /// Connection counter for unique IDs
    connection_counter: Arc<Mutex<u64>>,
}

impl<T> ConnectionPool<T>
where
    T: Send + 'static,
{
    pub fn new(config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));
        
        Self {
            config,
            available: Arc::new(Mutex::new(VecDeque::new())),
            semaphore,
            connection_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Get a connection from the pool or create a new one
    pub async fn get_connection<F, Fut>(&self, create_fn: F) -> Result<PooledConnection<T>, PoolError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
    {
        // Try to acquire a permit from the semaphore
        let _permit = timeout(self.config.connection_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| PoolError::Timeout)?
            .map_err(|_| PoolError::Closed)?;

        // Try to get an existing connection from the pool
        {
            let mut available = self.available.lock().await;
            
            // Remove idle connections
            let _now = Instant::now();
            while let Some(conn) = available.front() {
                if conn.is_idle(self.config.idle_timeout) {
                    available.pop_front();
                } else {
                    break;
                }
            }

            // Return an available connection if one exists
            if let Some(mut conn) = available.pop_front() {
                conn.touch();
                return Ok(conn);
            }
        }

        // Create a new connection
        let connection = create_fn().await.map_err(PoolError::CreateFailed)?;
        
        let mut counter = self.connection_counter.lock().await;
        *counter += 1;
        let id = *counter;
        
        Ok(PooledConnection::new(connection, id))
    }

    /// Return a connection to the pool
    pub async fn return_connection(&self, mut connection: PooledConnection<T>) {
        connection.touch();
        
        let mut available = self.available.lock().await;
        
        // Only return to pool if we haven't exceeded max connections
        if available.len() < self.config.max_connections {
            available.push_back(connection);
        }
        // Connection will be dropped if pool is full
    }

    /// Get current pool statistics
    pub async fn stats(&self) -> PoolStats {
        let available = self.available.lock().await;
        let available_count = available.len();
        let total_permits = self.semaphore.available_permits();
        
        PoolStats {
            available_connections: available_count,
            active_connections: self.config.max_connections - total_permits,
            max_connections: self.config.max_connections,
        }
    }

    /// Clean up idle connections
    pub async fn cleanup_idle(&self) {
        let mut available = self.available.lock().await;
        let _original_len = available.len();
        
        // Keep only non-idle connections, but maintain minimum
        let mut kept = VecDeque::new();
        let mut removed_count = 0;
        
        while let Some(conn) = available.pop_front() {
            if conn.is_idle(self.config.idle_timeout) && 
               (kept.len() + available.len()) > self.config.min_connections {
                removed_count += 1;
                // Connection will be dropped
            } else {
                kept.push_back(conn);
            }
        }
        
        *available = kept;
        
        if removed_count > 0 {
            log::debug!("Cleaned up {} idle connections from pool", removed_count);
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub available_connections: usize,
    pub active_connections: usize,
    pub max_connections: usize,
}

/// Pool-related errors
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("Timeout waiting for connection")]
    Timeout,
    #[error("Connection pool is closed")]
    Closed,
    #[error("Failed to create connection: {0}")]
    CreateFailed(Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[derive(Debug)]
    struct MockConnection {
        id: u32,
    }

    #[tokio::test]
    async fn test_connection_pool_basic() {
        let config = PoolConfig {
            max_connections: 2,
            connection_timeout: Duration::from_millis(100),
            idle_timeout: Duration::from_secs(1),
            min_connections: 1,
        };
        
        let pool = ConnectionPool::new(config);
        
        // Get first connection
        let conn1 = pool.get_connection(|| async {
            Ok(MockConnection { id: 1 })
        }).await.unwrap();
        
        assert_eq!(conn1.id, 1);
        
        // Return connection to pool
        pool.return_connection(conn1).await;
        
        // Get connection again - should reuse
        let conn2 = pool.get_connection(|| async {
            Ok(MockConnection { id: 2 })
        }).await.unwrap();
        
        // Should reuse the first connection
        assert_eq!(conn2.id, 1);
    }

    #[tokio::test]
    async fn test_connection_pool_idle_cleanup() {
        let config = PoolConfig {
            max_connections: 5,
            connection_timeout: Duration::from_millis(100),
            idle_timeout: Duration::from_millis(50), // Very short for testing
            min_connections: 1,
        };
        
        let pool = ConnectionPool::new(config);
        
        // Create and return a connection
        let conn = pool.get_connection(|| async {
            Ok(MockConnection { id: 1 })
        }).await.unwrap();
        
        pool.return_connection(conn).await;
        
        // Wait for idle timeout
        sleep(Duration::from_millis(100)).await;
        
        // Cleanup idle connections
        pool.cleanup_idle().await;
        
        // Get a new connection - should create new one since old was cleaned up
        let new_conn = pool.get_connection(|| async {
            Ok(MockConnection { id: 2 })
        }).await.unwrap();
        
        assert_eq!(new_conn.id, 2);
    }

    #[tokio::test]
    async fn test_connection_pool_stats() {
        let config = PoolConfig {
            max_connections: 3,
            connection_timeout: Duration::from_millis(100),
            idle_timeout: Duration::from_secs(1),
            min_connections: 1,
        };
        
        let pool = ConnectionPool::new(config);
        
        let initial_stats = pool.stats().await;
        assert_eq!(initial_stats.available_connections, 0);
        assert_eq!(initial_stats.active_connections, 0);
        assert_eq!(initial_stats.max_connections, 3);
        
        // Get a connection
        let _conn = pool.get_connection(|| async {
            Ok(MockConnection { id: 1 })
        }).await.unwrap();
        
        let active_stats = pool.stats().await;
        assert_eq!(active_stats.active_connections, 1);
    }
}