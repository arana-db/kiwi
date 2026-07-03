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
    DualRuntimeError, RequestPriority, StorageClient as RuntimeStorageClient, StorageCommand,
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

    /// Execute an arbitrary Redis command through the storage runtime command table
    pub async fn execute_command(
        &self,
        cmd_name: &[u8],
        argv: &[Vec<u8>],
    ) -> Result<RespData, DualRuntimeError> {
        debug!(
            "StorageClient::execute_command - cmd: {:?}, argc: {}",
            String::from_utf8_lossy(cmd_name),
            argv.len()
        );

        let command = StorageCommand::Execute {
            cmd_name: cmd_name.to_vec(),
            argv: argv.to_vec(),
        };

        self.inner.send_request(command).await
    }

    // Advanced methods with custom timeout and priority

    /// Execute a command with custom timeout
    pub async fn execute_with_timeout(
        &self,
        command: StorageCommand,
        timeout: Duration,
    ) -> Result<RespData, DualRuntimeError> {
        debug!(
            "StorageClient::execute_with_timeout - timeout: {:?}",
            timeout
        );

        self.inner.send_request_with_timeout(command, timeout).await
    }

    /// Execute a command with custom timeout and priority
    pub async fn execute_with_priority(
        &self,
        command: StorageCommand,
        timeout: Duration,
        priority: RequestPriority,
    ) -> Result<RespData, DualRuntimeError> {
        debug!(
            "StorageClient::execute_with_priority - timeout: {:?}, priority: {:?}",
            timeout, priority
        );

        self.inner
            .send_request_with_priority(command, timeout, priority)
            .await
    }

    /// Execute a batch of commands with optimized batching
    pub async fn execute_batch(
        &self,
        commands: Vec<StorageCommand>,
    ) -> Result<RespData, DualRuntimeError> {
        debug!(
            "StorageClient::execute_batch - commands: {}",
            commands.len()
        );

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
    pub async fn execute_pipelined(
        &self,
        commands: Vec<StorageCommand>,
    ) -> Result<Vec<RespData>, DualRuntimeError> {
        debug!(
            "StorageClient::execute_pipelined - commands: {}",
            commands.len()
        );

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
                    let error_resp =
                        RespData::Error(format!("ERR pipelined command failed: {}", e).into());
                    results.push(error_resp);
                }
            }
        }

        Ok(results)
    }

    /// Execute a high-priority command (for critical operations)
    pub async fn execute_critical(
        &self,
        command: StorageCommand,
    ) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::execute_critical");

        self.inner
            .send_request_with_priority(
                command,
                Duration::from_secs(10), // Shorter timeout for critical operations
                RequestPriority::Critical,
            )
            .await
    }

    /// Execute a low-priority command (for background operations)
    pub async fn execute_background(
        &self,
        command: StorageCommand,
    ) -> Result<RespData, DualRuntimeError> {
        debug!("StorageClient::execute_background");

        self.inner
            .send_request_with_priority(
                command,
                Duration::from_secs(60), // Longer timeout for background operations
                RequestPriority::Low,
            )
            .await
    }
}

impl Clone for StorageClient {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use runtime::{MessageChannel, StorageClient as RuntimeStorageClient};
    use std::sync::Arc;
    use std::time::Duration;

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
        let command = StorageCommand::Execute {
            cmd_name: b"get".to_vec(),
            argv: vec![b"get".to_vec(), b"test".to_vec()],
        };
        assert!(matches!(command, StorageCommand::Execute { .. }));
    }

    #[test]
    fn test_request_priority_levels() {
        // Test that priority levels are ordered correctly
        assert!(RequestPriority::Critical > RequestPriority::High);
        assert!(RequestPriority::High > RequestPriority::Normal);
        assert!(RequestPriority::Normal > RequestPriority::Low);
    }
}
