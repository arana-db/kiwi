/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::Arc;

use log::{info, warn};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use client::Client;
use cmd::Cmd;
use storage::storage::Storage;

pub struct CmdExecution {
    cmd: Arc<dyn Cmd>,
    client: Arc<Client>,
    storage: Arc<Storage>,
}

/// [`CmdExecutor`] accepts the command & command args parsed from server, and
/// execute them in a task pool.
pub struct CmdExecutor {
    /// Sender for submitting tasks to the worker pool
    work_tx: async_channel::Sender<CmdExecution>,
    /// Worker task handles
    workers: Vec<JoinHandle<()>>,
    /// Cancellation token for the executor
    cancellation_token: CancellationToken,
}

impl CmdExecutor {
    /// Creates a new `CmdExecutor` with a specified number of worker tasks
    pub fn new(worker_count: usize, channel_size: usize) -> Self {
        let cancellation_token = CancellationToken::new();
        let (work_tx, work_rx) = async_channel::bounded::<CmdExecution>(channel_size);

        let mut workers = Vec::new();

        // Spawn workers
        for worker_id in 0..worker_count {
            let task_rx_clone = work_rx.clone();
            let worker = tokio::spawn(Self::run_worker(
                worker_id,
                task_rx_clone,
                cancellation_token.clone(),
            ));
            workers.push(worker);
        }

        Self {
            work_tx,
            workers,
            cancellation_token,
        }
    }

    pub async fn execute(&self, work: CmdExecution) {
        match self.work_tx.send(work).await {
            Ok(_) => (),
            Err(e) => {
                warn!("Failed to send work to worker: {e:?}");
            }
        }
    }

    pub async fn close(&mut self) {
        self.cancellation_token.cancel();

        // Wait for all workers to complete
        for worker in &mut self.workers {
            let _ = worker.await;
        }

        info!("CmdExecutor closed");
    }

    async fn do_execute_once(work: CmdExecution) {
        let cmd = work.cmd;
        let client = work.client;
        let storage = work.storage;
        cmd.execute(client.as_ref(), storage);
    }

    async fn run_worker(
        worker_id: usize,
        work_rx: async_channel::Receiver<CmdExecution>,
        cancellation_token: CancellationToken,
    ) {
        info!("Worker {worker_id} started");
        loop {
            tokio::select! {
                work = work_rx.recv() => {
                    match work {
                        Ok(work) => {
                            Self::do_execute_once(work).await;
                        }
                        Err(err) => {
                            warn!("Worker {worker_id} channel unexpectly closed, shutting down: {err:?}");
                            break;
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    // Cancellation requested, worker should exit
                    info!("Worker {worker_id} received cancellation signal, shutting down...");
                    break;
                }
            }
        }
    }
}
