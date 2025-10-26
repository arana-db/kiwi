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

use client::Client;
use cmd::Cmd;
use log::{error, info, warn};
use resp::RespData;
use storage::storage::Storage;
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::CmdExecution;

/// Cluster-aware command execution that routes through Raft consensus
pub struct ClusterCmdExecution {
    pub cmd: Arc<dyn Cmd>,
    pub client: Arc<Client>,
    pub storage: Arc<Storage>,
    pub raft_node: Arc<dyn Send + Sync>,
}

/// Work item for cluster command execution
struct ClusterCmdExecutionWork {
    exec: ClusterCmdExecution,
    done: oneshot::Sender<()>,
}

/// Cluster-aware command executor that integrates with Raft consensus
pub struct ClusterCmdExecutor {
    /// Sender for submitting tasks to the worker pool
    work_tx: async_channel::Sender<ClusterCmdExecutionWork>,
    /// Worker task handles
    workers: Vec<JoinHandle<()>>,
    /// Cancellation token for the executor
    cancellation_token: CancellationToken,
}

impl ClusterCmdExecutor {
    /// Creates a new cluster-aware command executor
    pub fn new(worker_count: usize, channel_size: usize) -> Self {
        let cancellation_token = CancellationToken::new();
        let (work_tx, work_rx) = async_channel::bounded::<ClusterCmdExecutionWork>(channel_size);

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

    pub async fn execute(&self, exec: ClusterCmdExecution) {
        let (done_tx, done_rx) = oneshot::channel();
        let work = ClusterCmdExecutionWork {
            exec,
            done: done_tx,
        };

        if self.cancellation_token.is_cancelled() {
            error!("execute failed, ClusterCmdExecutor is closed");
            work.exec
                .client
                .set_reply(RespData::Error("ERR cluster executor unavailable".into()));
            return;
        }

        // Send the work to the worker pool
        match self.work_tx.send(work).await {
            Ok(_) => {}
            Err(async_channel::SendError(_)) => {
                panic!("Failed to send work to worker; cluster executor likely closed");
            }
        }

        // Wait for completion
        let _ = done_rx.await;
    }

    pub async fn close(&mut self) {
        self.cancellation_token.cancel();

        // Wait for all workers to complete
        for worker in &mut self.workers {
            let _ = worker.await;
        }

        info!("ClusterCmdExecutor closed");
    }

    async fn do_execute_once(work: ClusterCmdExecutionWork) {
        let exec = work.exec;
        
        // Determine if this is a write command that needs Raft consensus
        let cmd_name = String::from_utf8_lossy(&exec.client.cmd_name()).to_lowercase();
        let is_write_command = Self::is_write_command(&cmd_name);
        
        if is_write_command {
            // TODO: Route through Raft consensus for write commands
            // For now, execute directly (will be enhanced when Raft is fully implemented)
            warn!("Write command '{}' executed directly - Raft integration pending", cmd_name);
            exec.cmd.execute(exec.client.as_ref(), exec.storage);
        } else {
            // Read commands can be executed directly on local storage
            exec.cmd.execute(exec.client.as_ref(), exec.storage);
        }

        // Notify completion
        let _ = work.done.send(());
    }

    /// Determine if a command is a write operation that requires Raft consensus
    fn is_write_command(cmd_name: &str) -> bool {
        matches!(cmd_name, 
            "set" | "del" | "expire" | "expireat" | "persist" | "rename" | "renamenx" |
            "lpush" | "rpush" | "lpop" | "rpop" | "lset" | "lrem" | "ltrim" |
            "sadd" | "srem" | "spop" | "smove" |
            "zadd" | "zrem" | "zincrby" | "zremrangebyrank" | "zremrangebyscore" |
            "hset" | "hdel" | "hincrby" | "hincrbyfloat" |
            "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" |
            "append" | "setrange" | "setex" | "setnx" | "mset" | "msetnx" |
            "flushdb" | "flushall"
        )
    }

    async fn run_worker(
        worker_id: usize,
        work_rx: async_channel::Receiver<ClusterCmdExecutionWork>,
        cancellation_token: CancellationToken,
    ) {
        info!("Cluster worker {worker_id} started");
        loop {
            tokio::select! {
                work = work_rx.recv() => {
                    match work {
                        Ok(work) => {
                            Self::do_execute_once(work).await;
                        }
                        Err(err) => {
                            warn!("Cluster worker {worker_id} channel unexpectedly closed, shutting down: {err:?}");
                            break;
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    info!("Cluster worker {worker_id} received cancellation signal, shutting down...");
                    break;
                }
            }
        }
    }
}

/// Convert regular CmdExecution to ClusterCmdExecution
impl From<(CmdExecution, Arc<dyn Send + Sync>)> for ClusterCmdExecution {
    fn from((exec, raft_node): (CmdExecution, Arc<dyn Send + Sync>)) -> Self {
        Self {
            cmd: exec.cmd,
            client: exec.client,
            storage: exec.storage,
            raft_node,
        }
    }
}