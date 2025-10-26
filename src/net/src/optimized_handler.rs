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

use std::sync::Arc;

use bytes::Bytes;
use client::Client;
use cmd::table::CmdTable;
use executor::CmdExecutor;
use log::{error, warn};
use resp::encode::RespEncoder;
use resp::{Parse, RespData, RespEncode, RespParseResult, RespVersion};
use storage::storage::Storage;
use tokio::select;

use crate::buffer::{BufferManager, BufferedReader};
use crate::pipeline::{CommandPipeline, PipelineConfig};
use crate::pool::{ConnectionPool, PoolConfig, PooledConnection};

/// Configuration for optimized connection handling
#[derive(Debug, Clone)]
pub struct OptimizedHandlerConfig {
    pub pool_config: PoolConfig,
    pub pipeline_config: PipelineConfig,
    pub enable_pipelining: bool,
    pub enable_buffer_pooling: bool,
    pub max_request_size: usize,
}

impl Default for OptimizedHandlerConfig {
    fn default() -> Self {
        Self {
            pool_config: PoolConfig::default(),
            pipeline_config: PipelineConfig::default(),
            enable_pipelining: true,
            enable_buffer_pooling: true,
            max_request_size: 1024 * 1024, // 1MB max request
        }
    }
}

/// Connection resources that can be pooled
pub struct ConnectionResources {
    pub storage: Arc<Storage>,
    pub cmd_table: Arc<CmdTable>,
    pub executor: Arc<CmdExecutor>,
    pub pipeline: Option<Arc<CommandPipeline>>,
}

/// Optimized connection handler with pooling, pipelining, and buffer management
pub struct OptimizedConnectionHandler {
    config: OptimizedHandlerConfig,
    resource_pool: Arc<ConnectionPool<ConnectionResources>>,
    buffer_manager: BufferManager,
}

impl OptimizedConnectionHandler {
    pub fn new(
        config: OptimizedHandlerConfig,
        _storage: Arc<Storage>,
        _cmd_table: Arc<CmdTable>,
        _executor: Arc<CmdExecutor>,
    ) -> Self {
        let resource_pool = Arc::new(ConnectionPool::new(config.pool_config.clone()));
        let buffer_manager = BufferManager::new(crate::buffer::BufferConfig::default());

        Self {
            config,
            resource_pool,
            buffer_manager,
        }
    }

    /// Process a connection with all optimizations
    pub async fn process_connection(&self, client: Arc<Client>) -> std::io::Result<()> {
        // Get resources from pool
        let pooled_resources = match self.get_pooled_resources().await {
            Ok(resources) => resources,
            Err(e) => {
                error!("Failed to get resources from pool: {}", e);
                return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
            }
        };

        let result = if self.config.enable_pipelining {
            self.process_with_pipeline(client, &pooled_resources).await
        } else {
            self.process_without_pipeline(client, &pooled_resources).await
        };

        // Return resources to pool
        self.resource_pool.return_connection(pooled_resources).await;

        result
    }

    /// Get resources from pool or create new ones
    async fn get_pooled_resources(&self) -> Result<PooledConnection<ConnectionResources>, crate::pool::PoolError> {
        let storage = Arc::new(Storage::new(1, 0)); // This should come from config
        let cmd_table = Arc::new(cmd::table::create_command_table());
        let executor = Arc::new(executor::CmdExecutorBuilder::new().build());

        self.resource_pool.get_connection(|| async {
            let pipeline = if self.config.enable_pipelining {
                Some(Arc::new(CommandPipeline::new(
                    self.config.pipeline_config.clone(),
                    storage.clone(),
                    cmd_table.clone(),
                    executor.clone(),
                )))
            } else {
                None
            };

            Ok(ConnectionResources {
                storage,
                cmd_table,
                executor,
                pipeline,
            })
        }).await
    }

    /// Process connection with pipeline optimization
    async fn process_with_pipeline(
        &self,
        client: Arc<Client>,
        resources: &PooledConnection<ConnectionResources>,
    ) -> std::io::Result<()> {
        let pipeline = resources.connection.pipeline.as_ref()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "Pipeline not available"))?;

        let _buffered_reader = BufferedReader::new(self.buffer_manager.clone());
        let mut resp_parser = resp::RespParse::new(RespVersion::RESP2);

        loop {
            // Read data using optimized buffer management
            let mut read_buffer = if self.config.enable_buffer_pooling {
                self.buffer_manager.get_buffer().await
            } else {
                crate::buffer::PooledBuffer::new(8192, 0)
            };

            select! {
                result = client.read(read_buffer.buffer.as_mut()) => {
                    match result {
                        Ok(n) => {
                            if n == 0 { 
                                if self.config.enable_buffer_pooling {
                                    self.buffer_manager.return_buffer(read_buffer).await;
                                }
                                return Ok(()); 
                            }

                            // Process data with pipeline
                            match resp_parser.parse(Bytes::copy_from_slice(&read_buffer.buffer[..n])) {
                                RespParseResult::Complete(data) => {
                                    // Submit to pipeline for processing
                                    match pipeline.submit_command(data, client.clone()).await {
                                        Ok(response) => {
                                            // Send response
                                            let mut encoder = RespEncoder::new(RespVersion::RESP2);
                                            encoder.encode_resp_data(&response);
                                            if let Err(e) = client.write(encoder.get_response().as_ref()).await {
                                                error!("Write error: {}", e);
                                                if self.config.enable_buffer_pooling {
                                                    self.buffer_manager.return_buffer(read_buffer).await;
                                                }
                                                return Err(e);
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Pipeline processing error: {}", e);
                                            let error_response = RespData::Error(format!("ERR {}", e).into());
                                            let mut encoder = RespEncoder::new(RespVersion::RESP2);
                                            encoder.encode_resp_data(&error_response);
                                            let _ = client.write(encoder.get_response().as_ref()).await;
                                        }
                                    }
                                }
                                RespParseResult::Error(e) => {
                                    error!("Protocol error: {:?}", e);
                                    if self.config.enable_buffer_pooling {
                                        self.buffer_manager.return_buffer(read_buffer).await;
                                    }
                                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                                }
                                RespParseResult::Incomplete => {
                                    // Not enough data, continue reading
                                }
                            }

                            if self.config.enable_buffer_pooling {
                                self.buffer_manager.return_buffer(read_buffer).await;
                            }
                        }
                        Err(e) => {
                            error!("Read error: {:?}", e);
                            if self.config.enable_buffer_pooling {
                                self.buffer_manager.return_buffer(read_buffer).await;
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Process connection without pipeline (fallback)
    async fn process_without_pipeline(
        &self,
        client: Arc<Client>,
        resources: &PooledConnection<ConnectionResources>,
    ) -> std::io::Result<()> {
        // Use the original processing logic but with buffer optimization
        let mut read_buffer = if self.config.enable_buffer_pooling {
            self.buffer_manager.get_buffer().await
        } else {
            crate::buffer::PooledBuffer::new(8192, 0)
        };

        let mut resp_parser = resp::RespParse::new(RespVersion::RESP2);

        loop {
            select! {
                result = client.read(read_buffer.buffer.as_mut()) => {
                    match result {
                        Ok(n) => {
                            if n == 0 { 
                                if self.config.enable_buffer_pooling {
                                    self.buffer_manager.return_buffer(read_buffer).await;
                                }
                                return Ok(()); 
                            }

                            match resp_parser.parse(Bytes::copy_from_slice(&read_buffer.buffer[..n])) {
                                RespParseResult::Complete(data) => {
                                    if let RespData::Array(Some(params)) = data {
                                        if params.is_empty() { continue; }

                                        if let RespData::BulkString(Some(cmd_name)) = &params[0] {
                                            client.set_cmd_name(cmd_name.as_ref());
                                        }
                                        let argv = params.iter().map(|p| if let RespData::BulkString(Some(d)) = p { d.to_vec() } else { vec![] }).collect::<Vec<Vec<u8>>>();
                                        client.set_argv(&argv);
                                        
                                        // Execute command directly
                                        self.handle_command_direct(
                                            client.clone(),
                                            resources.connection.storage.clone(),
                                            resources.connection.cmd_table.clone(),
                                            resources.connection.executor.clone(),
                                        ).await;
                                        
                                        // Send response
                                        let response = client.take_reply();
                                        let mut encoder = RespEncoder::new(RespVersion::RESP2);
                                        encoder.encode_resp_data(&response);
                                        if let Err(e) = client.write(encoder.get_response().as_ref()).await {
                                            error!("Write error: {}", e);
                                            if self.config.enable_buffer_pooling {
                                                self.buffer_manager.return_buffer(read_buffer).await;
                                            }
                                            return Err(e);
                                        }
                                    }
                                }
                                RespParseResult::Error(e) => {
                                    error!("Protocol error: {:?}", e);
                                    if self.config.enable_buffer_pooling {
                                        self.buffer_manager.return_buffer(read_buffer).await;
                                    }
                                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                                }
                                RespParseResult::Incomplete => {
                                    // Not enough data, wait for more
                                }
                            }
                        }
                        Err(e) => {
                            error!("Read error: {:?}", e);
                            if self.config.enable_buffer_pooling {
                                self.buffer_manager.return_buffer(read_buffer).await;
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Handle command directly (without pipeline)
    async fn handle_command_direct(
        &self,
        client: Arc<Client>,
        storage: Arc<Storage>,
        cmd_table: Arc<CmdTable>,
        executor: Arc<CmdExecutor>,
    ) {
        let cmd_name = String::from_utf8_lossy(&client.cmd_name()).to_lowercase();

        if let Some(cmd) = cmd_table.get(&cmd_name) {
            let exec = executor::CmdExecution {
                cmd: cmd.clone(),
                client: client.clone(),
                storage,
            };
            executor.execute(exec).await;
        } else {
            let err_msg = format!("ERR unknown command `{cmd_name}`");
            client.set_reply(RespData::Error(err_msg.into()));
        }
    }

    /// Get handler statistics
    pub async fn stats(&self) -> OptimizedHandlerStats {
        let pool_stats = self.resource_pool.stats().await;
        let buffer_stats = self.buffer_manager.stats().await;

        OptimizedHandlerStats {
            pool_stats,
            buffer_stats,
        }
    }
}

/// Combined statistics for the optimized handler
#[derive(Debug, Clone)]
pub struct OptimizedHandlerStats {
    pub pool_stats: crate::pool::PoolStats,
    pub buffer_stats: crate::buffer::BufferStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_optimized_handler_config() {
        let config = OptimizedHandlerConfig::default();
        assert!(config.enable_pipelining);
        assert!(config.enable_buffer_pooling);
        assert_eq!(config.max_request_size, 1024 * 1024);
    }
}