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
use raft::{RaftNode, RedisProtocolCompatibility, RedisCommand};
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
    pub raft_compatibility: Arc<RedisProtocolCompatibility>,
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
            Ok(_) => {
                // Wait for completion
                let _ = done_rx.await;
            }
            Err(async_channel::SendError(work)) => {
                // Executor has been closed; set error response
                log::error!("Failed to send work to worker; cluster executor is closed");
                work.exec.client.set_reply(RespData::Error(
                    "ERR cluster executor is closed".into()
                ));
            }
        }
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
        
        // Convert client command to RedisCommand format
        let cmd_name = String::from_utf8_lossy(&exec.client.cmd_name()).to_string();
        let args: Vec<Vec<u8>> = exec.client.argv().iter().skip(1).cloned().collect();
        let redis_command = RedisCommand::new(cmd_name.clone(), args);
        
        // Process command through Raft-aware Redis protocol compatibility layer
        match exec.raft_compatibility.process_redis_command(&exec.client, redis_command).await {
            Ok(response) => {
                exec.client.set_reply(response);
            }
            Err(e) => {
                error!("Cluster command execution failed: {}", e);
                exec.client.set_reply(RespData::Error(format!("ERR {}", e).into()));
            }
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
impl From<(CmdExecution, Arc<RedisProtocolCompatibility>)> for ClusterCmdExecution {
    fn from((exec, raft_compatibility): (CmdExecution, Arc<RedisProtocolCompatibility>)) -> Self {
        Self {
            cmd: exec.cmd,
            client: exec.client,
            storage: exec.storage,
            raft_compatibility,
        }
    }
}