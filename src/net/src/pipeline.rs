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
use std::time::{Duration, Instant};

use client::Client;
use cmd::table::CmdTable;
use executor::{CmdExecution, CmdExecutor};
use log::{debug, warn};
use resp::RespData;
use storage::storage::Storage;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::timeout;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// Configuration for pipeline processing
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum number of commands to batch together
    pub max_batch_size: usize,
    /// Maximum time to wait before processing a partial batch
    pub batch_timeout: Duration,
    /// Maximum number of concurrent pipeline processors
    pub max_concurrent_pipelines: usize,
    /// Channel buffer size for command queuing
    pub command_queue_size: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            max_concurrent_pipelines: 10,
            command_queue_size: 1000,
        }
    }
}

/// Represents a command in the pipeline with its context
pub struct PipelineCommand {
    /// The parsed command data
    pub data: RespData,
    /// Client context for this command
    pub client: Arc<Client>,
    /// Timestamp when command was received
    pub received_at: Instant,
    /// Response sender
    pub response_tx: oneshot::Sender<RespData>,
}

/// A batch of commands to be processed together
pub struct CommandBatch {
    pub commands: Vec<PipelineCommand>,
    pub batch_id: u64,
    pub created_at: Instant,
}

impl CommandBatch {
    pub fn new(batch_id: u64) -> Self {
        Self {
            commands: Vec::new(),
            batch_id,
            created_at: Instant::now(),
        }
    }

    pub fn add_command(&mut self, command: PipelineCommand) {
        self.commands.push(command);
    }

    pub fn is_full(&self, max_size: usize) -> bool {
        self.commands.len() >= max_size
    }

    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.created_at.elapsed() > timeout
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

fn bounded_command_channel<T>(capacity: usize) -> (mpsc::Sender<T>, mpsc::Receiver<T>, usize) {
    let capacity = capacity.max(1);
    let (sender, receiver) = mpsc::channel(capacity);
    (sender, receiver, capacity)
}

/// Pipeline processor for batching and executing commands
pub struct CommandPipeline {
    config: PipelineConfig,
    storage: Arc<Storage>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    command_tx: mpsc::Sender<PipelineCommand>,
    semaphore: Arc<Semaphore>,
    batch_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl CommandPipeline {
    pub fn new(
        mut config: PipelineConfig,
        storage: Arc<Storage>,
        cmd_table: Arc<CmdTable>,
        executor: Arc<CmdExecutor>,
    ) -> Self {
        // Bounded channel enforces backpressure: when the queue is full,
        // senders await instead of growing the queue unboundedly.
        // Guard against 0 to avoid unintentional stalls.
        let (command_tx, command_rx, queue_capacity) =
            bounded_command_channel(config.command_queue_size);
        config.command_queue_size = queue_capacity;
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_pipelines));

        let pipeline = Self {
            config: config.clone(),
            storage,
            cmd_table,
            executor,
            command_tx,
            semaphore,
            batch_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };

        // Start the batch processor
        pipeline.start_batch_processor(command_rx);

        pipeline
    }

    /// Submit a command for pipeline processing
    pub async fn submit_command(
        &self,
        data: RespData,
        client: Arc<Client>,
    ) -> Result<RespData, PipelineError> {
        let (response_tx, response_rx) = oneshot::channel();

        let command = PipelineCommand {
            data,
            client,
            received_at: Instant::now(),
            response_tx,
        };

        send_and_receive(&self.command_tx, command, response_rx, COMMAND_TIMEOUT).await
    }

    /// Start the background batch processor
    fn start_batch_processor(&self, mut command_rx: mpsc::Receiver<PipelineCommand>) {
        let config = self.config.clone();
        let storage = self.storage.clone();
        let cmd_table = self.cmd_table.clone();
        let executor = self.executor.clone();
        let semaphore = self.semaphore.clone();
        let batch_counter = Arc::clone(&self.batch_counter);

        tokio::spawn(async move {
            let mut current_batch: Option<CommandBatch> = None;
            let mut batch_timer = tokio::time::interval(config.batch_timeout);
            batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    // Receive new command
                    command = command_rx.recv() => {
                        match command {
                            Some(cmd) => {
                                // Initialize batch if needed
                                if current_batch.is_none() {
                                    let batch_id = batch_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    current_batch = Some(CommandBatch::new(batch_id));
                                }

                                // Add command to current batch
                                if let Some(ref mut batch) = current_batch {
                                    batch.add_command(cmd);

                                    // Process batch if full
                                    if batch.is_full(config.max_batch_size) {
                                        let batch_to_process = current_batch.take().expect("batch was just checked Some");
                                        Self::process_batch(
                                            batch_to_process,
                                            storage.clone(),
                                            cmd_table.clone(),
                                            executor.clone(),
                                            semaphore.clone(),
                                        ).await;
                                    }
                                }
                            }
                            None => {
                                // Channel closed, process remaining batch and exit
                                if let Some(batch) = current_batch.take()
                                    && !batch.is_empty()
                                {
                                    Self::process_batch(
                                        batch,
                                        storage.clone(),
                                        cmd_table.clone(),
                                        executor.clone(),
                                        semaphore.clone(),
                                    ).await;
                                }
                                break;
                            }
                        }
                    }

                    // Batch timeout - process partial batch
                    _ = batch_timer.tick() => {
                        let pending_batch = current_batch.take();
                        if let Some(batch) = pending_batch {
                            if !batch.is_empty() && batch.is_expired(config.batch_timeout) {
                                Self::process_batch(
                                    batch,
                                    storage.clone(),
                                    cmd_table.clone(),
                                    executor.clone(),
                                    semaphore.clone(),
                                ).await;
                            } else if !batch.is_empty() {
                                // Put batch back if not expired
                                current_batch = Some(batch);
                            }
                        }
                    }
                }
            }
        });
    }

    /// Process a batch of commands
    async fn process_batch(
        batch: CommandBatch,
        storage: Arc<Storage>,
        cmd_table: Arc<CmdTable>,
        executor: Arc<CmdExecutor>,
        semaphore: Arc<Semaphore>,
    ) {
        let batch_size = batch.len();
        let batch_id = batch.batch_id;

        debug!("Processing batch {} with {} commands", batch_id, batch_size);

        // Acquire semaphore permit for concurrency control
        let _permit = match semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                warn!("Failed to acquire semaphore for batch {}", batch_id);
                // Send errors to all commands in batch
                for command in batch.commands {
                    let _ = command
                        .response_tx
                        .send(RespData::Error("ERR server overloaded".into()));
                }
                return;
            }
        };

        // Process commands concurrently within the batch
        let mut handles = Vec::with_capacity(batch_size);

        for command in batch.commands {
            let storage = storage.clone();
            let cmd_table = cmd_table.clone();
            let executor = executor.clone();

            let handle = tokio::spawn(async move {
                let response = Self::execute_command(
                    command.data,
                    command.client,
                    storage,
                    cmd_table,
                    executor,
                )
                .await;
                let _ = command.response_tx.send(response);
            });

            handles.push(handle);
        }

        // Wait for all commands in batch to complete
        for handle in handles {
            let join_result = handle.await;
            if let Err(e) = join_result {
                warn!("Command execution failed in batch {}: {}", batch_id, e);
            }
        }

        debug!("Completed batch {} with {} commands", batch_id, batch_size);
    }

    /// Execute a single command
    async fn execute_command(
        data: RespData,
        client: Arc<Client>,
        storage: Arc<Storage>,
        cmd_table: Arc<CmdTable>,
        executor: Arc<CmdExecutor>,
    ) -> RespData {
        // Parse command from RespData
        if let RespData::Array(Some(params)) = data {
            if params.is_empty() {
                return RespData::Error("ERR empty command".into());
            }

            if let RespData::BulkString(Some(cmd_name)) = &params[0] {
                client.set_cmd_name(cmd_name.as_ref());
            }

            let argv = params
                .iter()
                .map(|p| {
                    if let RespData::BulkString(Some(d)) = p {
                        d.to_vec()
                    } else {
                        vec![]
                    }
                })
                .collect::<Vec<Vec<u8>>>();

            client.set_argv(&argv);

            // Execute command
            let cmd_name = String::from_utf8_lossy(&client.cmd_name()).to_lowercase();

            if let Some(cmd) = cmd_table.get(&cmd_name) {
                let exec = CmdExecution {
                    cmd: cmd.clone(),
                    client: client.clone(),
                    storage,
                };
                executor.execute(exec).await;
                client.take_reply()
            } else {
                let err_msg = format!("ERR unknown command `{cmd_name}`");
                RespData::Error(err_msg.into())
            }
        } else {
            RespData::Error("ERR invalid command format".into())
        }
    }

    /// Get pipeline statistics
    pub fn stats(&self) -> PipelineStats {
        PipelineStats {
            available_permits: self.semaphore.available_permits(),
            max_concurrent_pipelines: self.config.max_concurrent_pipelines,
            // The channel is bounded by config.command_queue_size.
            queue_capacity: self.config.command_queue_size,
        }
    }
}

/// Pipeline statistics
#[derive(Debug, Clone)]
pub struct PipelineStats {
    pub available_permits: usize,
    pub max_concurrent_pipelines: usize,
    pub queue_capacity: usize,
}

/// Pipeline-related errors
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("Pipeline channel closed")]
    ChannelClosed,
    #[error("Response channel closed")]
    ResponseChannelClosed,
    #[error("Command execution timeout")]
    Timeout,
}

async fn send_and_receive(
    command_tx: &mpsc::Sender<PipelineCommand>,
    command: PipelineCommand,
    response_rx: oneshot::Receiver<RespData>,
    request_timeout: Duration,
) -> Result<RespData, PipelineError> {
    timeout(request_timeout, async {
        command_tx
            .send(command)
            .await
            .map_err(|_| PipelineError::ChannelClosed)?;
        response_rx
            .await
            .map_err(|_| PipelineError::ResponseChannelClosed)
    })
    .await
    .map_err(|_| PipelineError::Timeout)?
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::{advance, sleep};

    struct TestStream;

    #[async_trait::async_trait]
    impl client::StreamTrait for TestStream {
        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }

        async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }
    }

    fn pipeline_command(response_tx: oneshot::Sender<RespData>) -> PipelineCommand {
        PipelineCommand {
            data: RespData::Array(Some(Vec::new())),
            client: Arc::new(Client::new(Box::new(TestStream))),
            received_at: Instant::now(),
            response_tx,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn queue_wait_and_response_share_single_deadline() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (filler_response_tx, _filler_response_rx) = oneshot::channel();
        command_tx
            .send(pipeline_command(filler_response_tx))
            .await
            .expect("filler command must fit in the queue");

        let (target_response_tx, target_response_rx) = oneshot::channel();
        let target_command = pipeline_command(target_response_tx);
        let submit_task = tokio::spawn(async move {
            send_and_receive(
                &command_tx,
                target_command,
                target_response_rx,
                Duration::from_secs(30),
            )
            .await
        });
        tokio::task::yield_now().await;

        advance(Duration::from_secs(20)).await;
        let filler_command = command_rx
            .recv()
            .await
            .expect("filler command must remain queued");
        drop(filler_command);
        tokio::task::yield_now().await;

        advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            submit_task.is_finished(),
            "queue wait and response wait must share one deadline"
        );
        let result = submit_task.await.expect("submission task must not panic");
        assert!(matches!(result, Err(PipelineError::Timeout)));

        let target_command = command_rx
            .recv()
            .await
            .expect("target command must have entered the queue");
        assert!(
            target_command
                .response_tx
                .send(RespData::SimpleString("late".into()))
                .is_err(),
            "timing out must close the target response receiver"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn submit_command_uses_single_30_second_deadline_across_admission_and_response() {
        let client = Arc::new(Client::new(Box::new(TestStream)));
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let pipeline = Arc::new(CommandPipeline {
            config: PipelineConfig {
                command_queue_size: 1,
                ..PipelineConfig::default()
            },
            storage: Arc::new(Storage::new(1, 0)),
            cmd_table: Arc::new(CmdTable::new()),
            executor: Arc::new(CmdExecutor::new(0, 1)),
            command_tx: command_tx.clone(),
            semaphore: Arc::new(Semaphore::new(1)),
            batch_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        });

        let (blocker_response_tx, _blocker_response_rx) = oneshot::channel();
        command_tx
            .send(PipelineCommand {
                data: RespData::Null,
                client: Arc::clone(&client),
                received_at: Instant::now(),
                response_tx: blocker_response_tx,
            })
            .await
            .expect("filler command must fit in the queue");

        let submit_task = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let client = Arc::clone(&client);
            async move { pipeline.submit_command(RespData::Null, client).await }
        });
        tokio::task::yield_now().await;

        advance(Duration::from_secs(20)).await;
        let blocker = command_rx
            .recv()
            .await
            .expect("filler command must remain queued");
        drop(blocker);
        tokio::task::yield_now().await;

        let admitted_command = command_rx
            .recv()
            .await
            .expect("submitted command must enter the queue");
        advance(Duration::from_secs(9)).await;
        tokio::task::yield_now().await;
        assert!(
            !submit_task.is_finished(),
            "deadline fired before 30 seconds"
        );

        advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert!(
            submit_task.is_finished(),
            "admission and response exceeded the shared 30-second deadline"
        );
        assert!(matches!(
            submit_task.await.expect("submission task must not panic"),
            Err(PipelineError::Timeout)
        ));
        drop(admitted_command);
    }

    #[tokio::test]
    async fn zero_command_queue_size_reports_effective_capacity() {
        let config = PipelineConfig {
            command_queue_size: 0,
            ..Default::default()
        };
        let pipeline = CommandPipeline::new(
            config,
            Arc::new(Storage::new(1, 0)),
            Arc::new(CmdTable::new()),
            Arc::new(CmdExecutor::new(1, 1)),
        );

        assert_eq!(pipeline.command_tx.max_capacity(), 1);
        assert_eq!(pipeline.stats().queue_capacity, 1);
    }

    #[tokio::test]
    async fn test_pipeline_basic() {
        // This test would require proper mock implementations
        // For now, we'll just test the configuration
        let config = PipelineConfig::default();
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.batch_timeout, Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_command_batch() {
        let batch = CommandBatch::new(1);
        assert!(batch.is_empty());
        assert!(!batch.is_full(10));

        // Test batch expiration
        let config = PipelineConfig {
            batch_timeout: Duration::from_millis(1),
            ..Default::default()
        };

        sleep(Duration::from_millis(2)).await;
        assert!(batch.is_expired(config.batch_timeout));
    }

    #[test]
    fn command_channel_is_bounded_and_normalizes_zero_capacity() {
        let (sender, _receiver, capacity) = bounded_command_channel::<u8>(0);

        assert_eq!(capacity, 1);
        assert!(sender.try_send(1).is_ok());
        assert!(matches!(
            sender.try_send(2),
            Err(mpsc::error::TrySendError::Full(2))
        ));
    }
}
