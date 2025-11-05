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

//! StorageClient for network-side storage communication
//! 
//! This module provides the StorageClient implementation that allows the network
//! runtime to communicate with the storage runtime through async message passing.

use std::sync::Arc;
use std::time::Duration;

use log::debug;
use resp::RespData;
use runtime::{
    StorageClient as RuntimeStorageClient, StorageCommand, RequestPriority, DualRuntimeError
};

/// Network-side storage client that provides Redis command methods
/// 
/// This client wraps the runtime StorageClient and provides convenient
/// methods for executing Redis commands asynchronously.
pub struct StorageClient {
    /// The underlying runtime storage client
    inner: Arc<RuntimeStorageClient>,
}

impl StorageClient {
    /// Create a new StorageClient wrapping the runtime client
    pub fn new(runtime_client: Arc<RuntimeStorageClient>) -> Self {
        Self {
            inner: runtime_client,
        }
    }

    /// Get the underlying runtime client
    pub fn inner(&self) -> &Arc<RuntimeStorageClient> {
        &self.inner
    }

    /// Check if the storage client is healthy
    pub fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }

    /// Get the number of pending requests
    pub async fn pending_request_count(&self) -> usize {
        self.inner.pending_request_count().await
    }

    /// Get channel statistics
    pub async fn channel_stats(&self) -> runtime::ChannelStats {
        self.inner.channel_stats().await
    }

    // Redis command implementations

    /// GET command - retrieve a value by key
    pub async fn get(&self, key: &[u8]) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::get - key: {:?}", String::from_utf8_lossy(key));
        
        let command = StorageCommand::Get {
            key: key.to_vec(),
        };
        
        self.inner.send_request(command).await
    }

    /// SET command - set a key-value pair with optional TTL
    pub async fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::set - key: {:?}, value_len: {}, ttl: {:?}", 
               String::from_utf8_lossy(key), value.len(), ttl);
        
        let command = StorageCommand::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            ttl,
        };
        
        self.inner.send_request(command).await
    }

    /// DEL command - delete one or more keys
    pub async fn del(&self, keys: &[Vec<u8>]) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::del - keys: {:?}", 
               keys.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>());
        
        let command = StorageCommand::Del {
            keys: keys.to_vec(),
        };
        
        self.inner.send_request(command).await
    }

    /// EXISTS command - check if keys exist
    pub async fn exists(&self, keys: &[Vec<u8>]) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::exists - keys: {:?}", 
               keys.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>());
        
        let command = StorageCommand::Exists {
            keys: keys.to_vec(),
        };
        
        self.inner.send_request(command).await
    }

    /// EXPIRE command - set expiration time for a key
    pub async fn expire(&self, key: &[u8], ttl: Duration) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::expire - key: {:?}, ttl: {:?}", 
               String::from_utf8_lossy(key), ttl);
        
        let command = StorageCommand::Expire {
            key: key.to_vec(),
            ttl,
        };
        
        self.inner.send_request(command).await
    }

    /// TTL command - get time to live for a key
    pub async fn ttl(&self, key: &[u8]) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::ttl - key: {:?}", String::from_utf8_lossy(key));
        
        let command = StorageCommand::Ttl {
            key: key.to_vec(),
        };
        
        self.inner.send_request(command).await
    }

    /// INCR command - increment a numeric value
    pub async fn incr(&self, key: &[u8]) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::incr - key: {:?}", String::from_utf8_lossy(key));
        
        let command = StorageCommand::Incr {
            key: key.to_vec(),
        };
        
        self.inner.send_request(command).await
    }

    /// INCRBY command - increment by a specific amount
    pub async fn incr_by(&self, key: &[u8], increment: i64) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::incr_by - key: {:?}, increment: {}", 
               String::from_utf8_lossy(key), increment);
        
        let command = StorageCommand::IncrBy {
            key: key.to_vec(),
            increment,
        };
        
        self.inner.send_request(command).await
    }

    /// DECR command - decrement a numeric value
    pub async fn decr(&self, key: &[u8]) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::decr - key: {:?}", String::from_utf8_lossy(key));
        
        let command = StorageCommand::Decr {
            key: key.to_vec(),
        };
        
        self.inner.send_request(command).await
    }

    /// DECRBY command - decrement by a specific amount
    pub async fn decr_by(&self, key: &[u8], decrement: i64) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::decr_by - key: {:?}, decrement: {}", 
               String::from_utf8_lossy(key), decrement);
        
        let command = StorageCommand::DecrBy {
            key: key.to_vec(),
            decrement,
        };
        
        self.inner.send_request(command).await
    }

    /// MSET command - multiple set operations
    pub async fn mset(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::mset - pairs: {}", pairs.len());
        
        let command = StorageCommand::MSet {
            pairs: pairs.to_vec(),
        };
        
        self.inner.send_request(command).await
    }

    /// MGET command - multiple get operations
    pub async fn mget(&self, keys: &[Vec<u8>]) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::mget - keys: {:?}", 
               keys.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>());
        
        let command = StorageCommand::MGet {
            keys: keys.to_vec(),
        };
        
        self.inner.send_request(command).await
    }

    // Advanced methods with custom timeout and priority

    /// Execute a command with custom timeout
    pub async fn execute_with_timeout(
        &self, 
        command: StorageCommand, 
        timeout: Duration
    ) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::execute_with_timeout - timeout: {:?}", timeout);
        
        self.inner.send_request_with_timeout(command, timeout).await
    }

    /// Execute a command with custom timeout and priority
    pub async fn execute_with_priority(
        &self,
        command: StorageCommand,
        timeout: Duration,
        priority: RequestPriority,
    ) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::execute_with_priority - timeout: {:?}, priority: {:?}", 
               timeout, priority);
        
        self.inner.send_request_with_priority(command, timeout, priority).await
    }

    /// Execute a batch of commands with optimized batching
    pub async fn execute_batch(&self, commands: Vec<StorageCommand>) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::execute_batch - commands: {}", commands.len());
        
        // For small batches, execute individually for better latency
        if commands.len() <= 3 {
            let mut results = Vec::new();
            for command in commands {
                match self.inner.send_request(command).await {
                    Ok(result) => results.push(result),
                    Err(e) => return Err(e),
                }
            }
            // Return array of results
            let resp_results: Vec<RespData> = results;
            return Ok(RespData::Array(Some(resp_results)));
        }
        
        // For larger batches, use the batch command
        let batch_command = StorageCommand::Batch { commands };
        self.inner.send_request(batch_command).await
    }

    /// Execute commands with request pipelining support
    pub async fn execute_pipelined(&self, commands: Vec<StorageCommand>) -> Result<Vec<RespData>, DualRuntimeError> {
        debug!("StorageClient::execute_pipelined - commands: {}", commands.len());
        
        // Send all requests concurrently for better throughput
        let mut futures = Vec::new();
        for command in commands {
            futures.push(self.inner.send_request(command));
        }
        
        // Wait for all responses
        let mut results = Vec::new();
        for future in futures {
            match future.await {
                Ok(result) => results.push(result),
                Err(e) => {
                    // For pipelined requests, we continue processing other commands
                    // and return an error response for the failed command
                    let error_resp = RespData::Error(format!("ERR pipelined command failed: {}", e).into());
                    results.push(error_resp);
                }
            }
        }
        
        Ok(results)
    }

    /// Execute a high-priority command (for critical operations)
    pub async fn execute_critical(&self, command: StorageCommand) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::execute_critical");
        
        self.inner.send_request_with_priority(
            command, 
            Duration::from_secs(10), // Shorter timeout for critical operations
            RequestPriority::Critical
        ).await
    }

    /// Execute a low-priority command (for background operations)
    pub async fn execute_background(&self, command: StorageCommand) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::execute_background");
        
        self.inner.send_request_with_priority(
            command, 
            Duration::from_secs(60), // Longer timeout for background operations
            RequestPriority::Low
        ).await
    }
}

impl Clone for StorageClient {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use runtime::{MessageChannel, StorageClient as RuntimeStorageClient};

    fn create_test_storage_client() -> StorageClient {
        let message_channel = Arc::new(MessageChannel::new(1000));
        let runtime_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        StorageClient::new(runtime_client)
    }

    #[tokio::test]
    async fn test_storage_client_creation() {
        let client = create_test_storage_client();
        
        assert!(client.is_healthy());
        assert_eq!(client.pending_request_count().await, 0);
    }

    #[tokio::test]
    async fn test_storage_client_clone() {
        let client = create_test_storage_client();
        let cloned_client = client.clone();
        
        assert!(cloned_client.is_healthy());
        assert_eq!(cloned_client.pending_request_count().await, 0);
    }

    #[tokio::test]
    async fn test_storage_client_channel_stats() {
        let client = create_test_storage_client();
        let stats = client.channel_stats().await;
        
        assert_eq!(stats.requests_sent, 0);
        assert_eq!(stats.pending_requests, 0);
    }

    #[test]
    fn test_storage_command_creation() {
        // Test that we can create various storage commands
        let _get_cmd = StorageCommand::Get { key: b"test".to_vec() };
        let _set_cmd = StorageCommand::Set { 
            key: b"test".to_vec(), 
            value: b"value".to_vec(), 
            ttl: None 
        };
        let _del_cmd = StorageCommand::Del { keys: vec![b"test".to_vec()] };
        let _exists_cmd = StorageCommand::Exists { keys: vec![b"test".to_vec()] };
    }

    #[test]
    fn test_request_priority_levels() {
        // Test that priority levels are ordered correctly
        assert!(RequestPriority::Critical > RequestPriority::High);
        assert!(RequestPriority::High > RequestPriority::Normal);
        assert!(RequestPriority::Normal > RequestPriority::Low);
    }
}