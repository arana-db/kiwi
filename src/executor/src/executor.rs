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

use log::{error, info, warn};
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use client::Client;
use cmd::Cmd;
use resp::RespData;
use storage::storage::Storage;

pub struct CmdExecution {
    pub cmd: Arc<dyn Cmd>,
    // TODO(flaneur2020): it might be good to have a CmdContext to place the command
    // args, key, etc. to limit the scope of the command execution. Client is an
    // object that able to be accessed in different threads, we can consider to
    // reduce the responsibility of the client object if we considers to split
    // command execution in a standalone tokio runtime.
    pub client: Arc<Client>,
    // TODO(flaneur2020): storage might be better to be owned by CmdExecutor, if we
    // plans to put execution in a seperate tokio runtime.
    pub storage: Arc<Storage>,
}

/// [`CmdExecutionWork`] is the work item sent to the worker pool. It notifies the
/// caller when the work is finished.
struct CmdExecutionWork {
    exec: CmdExecution,
    done: oneshot::Sender<()>,
}

/// [`CmdExecutor`] accepts the command & command args parsed from server, and
/// execute them in a task pool.
pub struct CmdExecutor {
    /// Sender for submitting tasks to the worker pool
    work_tx: async_channel::Sender<CmdExecutionWork>,
    /// Worker task handles
    workers: Vec<JoinHandle<()>>,
    /// Cancellation token for the executor
    cancellation_token: CancellationToken,
}

impl CmdExecutor {
    /// Creates a new `CmdExecutor` with a specified number of worker tasks
    pub fn new(worker_count: usize, channel_size: usize) -> Self {
        let cancellation_token = CancellationToken::new();
        let (work_tx, work_rx) = async_channel::bounded::<CmdExecutionWork>(channel_size);

        let mut workers = Vec::new();

        // Spawn workers
        for worker_id in 0..worker_count {
            let work_rx_clone = work_rx.clone();
            let worker = tokio::spawn(Self::run_worker(
                worker_id,
                work_rx_clone,
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

    pub async fn execute(&self, exec: CmdExecution) {
        let (done_tx, done_rx) = oneshot::channel();
        let work = CmdExecutionWork {
            exec,
            done: done_tx,
        };

        // send the work to the worker pool
        match self.work_tx.send(work).await {
            Ok(_) => {}
            Err(async_channel::SendError(work)) => {
                error!("Failed to send work to worker; executor likely closed");
                work.exec
                    .client
                    .set_reply(RespData::Error("ERR executor unavailable".into()));
                // Unblock the waiter
                let _ = work.done.send(());
            }
        }

        // TODO: add a timeout for waiting
        let _ = done_rx.await;
    }

    pub async fn close(&mut self) {
        self.cancellation_token.cancel();

        // Wait for all workers to complete
        for worker in &mut self.workers {
            let _ = worker.await;
        }

        info!("CmdExecutor closed");
    }

    async fn do_execute_once(work: CmdExecutionWork) {
        let exec = work.exec;

        // TODO: we may consider pass the cancellation_token to cmd.execute to
        // allow having a graceful shutdown in a big command processing logic.
        exec.cmd.execute(exec.client.as_ref(), exec.storage);

        // notify the work has finished to the caller
        let _ = work.done.send(());
    }

    async fn run_worker(
        worker_id: usize,
        work_rx: async_channel::Receiver<CmdExecutionWork>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use cmd::get::GetCmd;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_cmd_executor_basic_functionality() {
        // Create a real GetCmd
        let get_cmd = Arc::new(GetCmd::new());

        // Create a simple client
        let client = Arc::new(Client::new(Box::new(TestStream::new())));
        client.set_cmd_name(b"get");
        client.set_argv(&[b"get".to_vec(), b"test_key".to_vec()]);

        // Create storage
        let storage = Arc::new(Storage::new(1, 0));

        // Create executor with 1 worker
        let mut executor = CmdExecutor::new(1, 5);

        // Create command execution
        let cmd_execution = CmdExecution {
            cmd: get_cmd,
            client,
            storage,
        };

        // Execute the command
        executor.execute(cmd_execution).await;

        // Test graceful shutdown
        executor.close().await;
    }

    // Simple test stream implementation
    struct TestStream;

    impl TestStream {
        fn new() -> Self {
            Self
        }
    }

    #[async_trait::async_trait]
    impl client::StreamTrait for TestStream {
        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }

        async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }
    }
}
