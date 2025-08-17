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
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use resp::{RespData, RespError};

/// Task message sent to worker tasks
type TaskMessage = Box<dyn FnOnce() -> Result<RespData, RespError> + Send + 'static>;

/// [`CmdExecutor`] accepts the command & command args parsed from server, and
/// execute them in a task pool.
pub struct CmdExecutor {
    /// Sender for submitting tasks to the worker pool
    task_tx: async_channel::Sender<TaskMessage>,
    /// workers size
    workers_size: usize,
    /// Worker task handles
    _workers: Vec<JoinHandle<()>>,
}

impl CmdExecutor {
    /// Creates a new `CmdExecutor` with a specified number of worker tasks
    pub fn new(worker_count: usize) -> Self {
        let (task_tx, task_rx) = async_channel::bounded::<TaskMessage>(1000);
        let task_rx = Arc::new(tokio::sync::Mutex::new(task_rx));
        
        let mut workers = Vec::new();
        
        // Spawn worker tasks
        for _ in 0..worker_count {
            let task_rx_clone = task_rx.clone();
            let worker = tokio::spawn(async move {
                loop {
                    let task = {
                        let mut task_rx = task_rx_clone.lock().await;
                        task_rx.recv().await
                    };
                    
                    match task {
                        Some(task_fn) => {
                            // Execute the task
                            let _result = task_fn();
                            // Note: In a real implementation, you'd want to handle the result
                            // and send it back through a channel or store it somewhere
                        }
                        None => {
                            // Channel closed, worker should exit
                            break;
                        }
                    }
                }
            });
            workers.push(worker);
        }
        
        Self {
            task_sender,
            _workers: workers,
        }
    }

    /// Submits a task to the worker pool
    pub async fn submit_task<F>(&self, task_fn: F) -> Result<(), mpsc::error::SendError<TaskMessage>>
    where
        F: FnOnce() -> Result<RespData, RespError> + Send + 'static,
    {
        self.task_sender.send(Box::new(task_fn)).await
    }

    /// Submits a task and returns a future that resolves to the result
    pub async fn submit_task_with_result<F>(&self, task_fn: F) -> Result<RespData, RespError>
    where
        F: FnOnce() -> Result<RespData, RespError> + Send + 'static,
    {
        let (result_sender, result_receiver) = oneshot::channel();
        
        let wrapped_task = move || {
            let result = task_fn();
            let _ = result_sender.send(result);
            result
        };
        
        self.task_sender.send(Box::new(wrapped_task)).await
            .map_err(|_| RespError::UnknownError("Failed to submit task".to_string()))?;
        
        result_receiver.await
            .map_err(|_| RespError::UnknownError("Task result channel closed".to_string()))?
    }

    /// Executes a simple command task (helper method)
    pub async fn execute_command<F>(&self, command_fn: F) -> Result<RespData, RespError>
    where
        F: FnOnce() -> Result<RespData, RespError> + Send + 'static,
    {
        self.submit_task_with_result(command_fn).await
    }
}

impl Default for CmdExecutor {
    fn default() -> Self {
        Self::new()
    }
}
