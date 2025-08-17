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

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use resp::{RespData, RespError};

/// Task message sent to worker tasks
type TaskMessage = Box<dyn FnOnce() -> Result<RespData, RespError> + Send + 'static>;

/// [`CmdExecutor`] accepts the command & command args parsed from server, and
/// execute them in a task pool.
pub struct CmdExecutor {
    /// Sender for submitting tasks to the worker pool
    task_tx: async_channel::Sender<TaskMessage>,
    /// Worker task handles
    workers: Vec<JoinHandle<()>>,
    /// Cancellation token for the executor
    cancellation_token: CancellationToken,
}

impl CmdExecutor {
    /// Creates a new `CmdExecutor` with a specified number of worker tasks
    pub fn new(worker_count: usize) -> Self {
        let cancellation_token = CancellationToken::new();
        let (task_tx, task_rx) = async_channel::bounded::<TaskMessage>(1000);

        let mut workers = Vec::new();

        // Spawn workers
        for _ in 0..worker_count {
            let task_rx_clone = task_rx.clone();
            let worker = tokio::spawn(Self::run_worker(task_rx_clone, cancellation_token.clone()));
            workers.push(worker);
        }

        Self {
            task_tx,
            workers,
            cancellation_token,
        }
    }

    async fn run_worker(
        task_rx: async_channel::Receiver<TaskMessage>,
        cancellation_token: CancellationToken,
    ) {
        loop {
            let task = task_rx.recv().await;

            match task {
                Ok(task_fn) => {
                    // Execute the task
                    let _result = task_fn();
                }
                Err(_) => {
                    // Channel closed, worker should exit
                    break;
                }
            }
        }
    }
}
