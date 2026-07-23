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

        if self.cancellation_token.is_cancelled() {
            error!("execute failed, CmdExecutor is closed");
            work.exec
                .client
                .set_reply(RespData::Error("ERR executor unavailable".into()));
            return;
        }

        // send the work to the worker pool
        match self.work_tx.send(work).await {
            Ok(_) => {}
            Err(async_channel::SendError(_)) => {
                // this should not happen, because the only case when all the workers
                // has been closed is when the executor is closed. and we've already
                // checked the cancellation_token.
                panic!("Failed to send work to worker; executor likely closed");
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
        let storage = exec.storage;
        if exec.cmd.requires_exclusive_storage_access() {
            let _guard = storage.acquire_exclusive_command_access().await;
            exec.cmd.execute(exec.client.as_ref(), Arc::clone(&storage));
        } else {
            let _guard = storage.acquire_shared_command_access().await;
            exec.cmd.execute(exec.client.as_ref(), Arc::clone(&storage));
        }

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

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    use cmd::{AclCategory, CmdFlags, CmdMeta};

    use super::*;

    #[derive(Clone)]
    struct ProbeCmd {
        meta: CmdMeta,
        executed: Arc<AtomicBool>,
    }

    impl ProbeCmd {
        fn new(executed: Arc<AtomicBool>) -> Self {
            Self {
                meta: CmdMeta {
                    name: "probe".to_string(),
                    arity: 1,
                    flags: CmdFlags::READONLY,
                    acl_category: AclCategory::READ,
                    ..Default::default()
                },
                executed,
            }
        }
    }

    impl Cmd for ProbeCmd {
        fn meta(&self) -> &CmdMeta {
            &self.meta
        }

        fn do_initial(&self, _client: &Client) -> bool {
            true
        }

        fn do_cmd(&self, _client: &Client, _storage: Arc<Storage>) {
            self.executed.store(true, Ordering::SeqCst);
        }

        fn clone_box(&self) -> Box<dyn Cmd> {
            Box::new(self.clone())
        }
    }

    #[derive(Clone)]
    struct BlockingExclusiveCmd {
        meta: CmdMeta,
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        control: Arc<(Mutex<bool>, Condvar)>,
    }

    impl BlockingExclusiveCmd {
        fn new(entered: oneshot::Sender<()>, control: Arc<(Mutex<bool>, Condvar)>) -> Self {
            Self {
                meta: CmdMeta {
                    name: "blocking-exclusive".to_string(),
                    arity: 1,
                    flags: CmdFlags::WRITE | CmdFlags::STORAGE_EXCLUSIVE,
                    acl_category: AclCategory::WRITE,
                    ..Default::default()
                },
                entered: Arc::new(Mutex::new(Some(entered))),
                control,
            }
        }
    }

    impl Cmd for BlockingExclusiveCmd {
        fn meta(&self) -> &CmdMeta {
            &self.meta
        }

        fn do_initial(&self, _client: &Client) -> bool {
            true
        }

        fn do_cmd(&self, _client: &Client, _storage: Arc<Storage>) {
            if let Some(entered) = self
                .entered
                .lock()
                .expect("entered sender mutex should not be poisoned")
                .take()
            {
                let _ = entered.send(());
            }

            let (blocked, wake) = self.control.as_ref();
            let mut blocked = blocked
                .lock()
                .expect("blocking control mutex should not be poisoned");
            while *blocked {
                blocked = wake
                    .wait(blocked)
                    .expect("blocking control mutex should not be poisoned");
            }
        }

        fn clone_box(&self) -> Box<dyn Cmd> {
            Box::new(self.clone())
        }
    }

    #[derive(Clone)]
    struct WaitingSharedCmd {
        meta: CmdMeta,
        attempts: Arc<AtomicUsize>,
        attempt_notify: Arc<tokio::sync::Notify>,
        executed: Arc<AtomicUsize>,
    }

    impl WaitingSharedCmd {
        fn new(
            attempts: Arc<AtomicUsize>,
            attempt_notify: Arc<tokio::sync::Notify>,
            executed: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                meta: CmdMeta {
                    name: "waiting-shared".to_string(),
                    arity: 1,
                    flags: CmdFlags::READONLY,
                    acl_category: AclCategory::READ,
                    ..Default::default()
                },
                attempts,
                attempt_notify,
                executed,
            }
        }
    }

    impl Cmd for WaitingSharedCmd {
        fn meta(&self) -> &CmdMeta {
            &self.meta
        }

        fn do_initial(&self, _client: &Client) -> bool {
            true
        }

        fn do_cmd(&self, _client: &Client, _storage: Arc<Storage>) {
            self.executed.fetch_add(1, Ordering::SeqCst);
        }

        fn clone_box(&self) -> Box<dyn Cmd> {
            Box::new(self.clone())
        }

        fn requires_exclusive_storage_access(&self) -> bool {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.attempt_notify.notify_one();
            false
        }
    }

    #[tokio::test]
    async fn test_cmd_executor_basic_functionality() {
        let executed = Arc::new(AtomicBool::new(false));
        let get_cmd = Arc::new(ProbeCmd::new(Arc::clone(&executed)));

        // Create a simple client
        let client = Arc::new(Client::new(Box::new(TestStream::new())));
        client.set_cmd_name(b"probe");
        client.set_argv(&[b"probe".to_vec()]);

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
        assert!(executed.load(Ordering::SeqCst));

        // Test graceful shutdown
        executor.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_routes_commands_through_shared_storage_access() {
        let storage = Arc::new(Storage::new(1, 0));
        let executed = Arc::new(AtomicBool::new(false));
        let command = Arc::new(ProbeCmd::new(Arc::clone(&executed)));
        let client = Arc::new(Client::new(Box::new(TestStream::new())));
        client.set_cmd_name(b"probe");
        client.set_argv(&[b"probe".to_vec()]);

        let exclusive = storage.acquire_exclusive_command_access().await;

        let mut executor = CmdExecutor::new(1, 5);
        let execution = executor.execute(CmdExecution {
            cmd: command,
            client,
            storage,
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), execution)
                .await
                .is_err(),
            "shared command should wait behind held exclusive access"
        );
        assert!(!executed.load(Ordering::SeqCst));

        drop(exclusive);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !executed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("command should execute after exclusive access exits");

        executor.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiting_for_command_gate_does_not_starve_runtime_workers() {
        const WAITING_COMMANDS: usize = 4;

        let storage = Arc::new(Storage::new(1, 0));
        let exclusive_executor = Arc::new(CmdExecutor::new(1, 2));
        let control = Arc::new((Mutex::new(true), Condvar::new()));
        let (exclusive_entered_tx, exclusive_entered_rx) = oneshot::channel();
        let exclusive_cmd = Arc::new(BlockingExclusiveCmd::new(
            exclusive_entered_tx,
            Arc::clone(&control),
        ));
        let exclusive_client = Arc::new(Client::new(Box::new(TestStream::new())));
        exclusive_client.set_cmd_name(b"blocking-exclusive");
        exclusive_client.set_argv(&[b"blocking-exclusive".to_vec()]);

        let exclusive_executor_for_task = Arc::clone(&exclusive_executor);
        let exclusive_storage = Arc::clone(&storage);
        let exclusive_task = tokio::spawn(async move {
            exclusive_executor_for_task
                .execute(CmdExecution {
                    cmd: exclusive_cmd,
                    client: exclusive_client,
                    storage: exclusive_storage,
                })
                .await;
        });
        exclusive_entered_rx
            .await
            .expect("exclusive command should enter while holding the gate");

        // Create shared workers from the runtime worker that remains available.
        // A synchronous gate wait would block this worker; an async wait yields it.
        let shared_executor = Arc::new(CmdExecutor::new(WAITING_COMMANDS, 16));

        // This native watchdog prevents a broken synchronous gate from hanging
        // the test forever after both Tokio workers become blocked.
        let watchdog_control = Arc::clone(&control);
        let watchdog = thread::spawn(move || {
            let (blocked, wake) = watchdog_control.as_ref();
            let blocked = blocked
                .lock()
                .expect("watchdog control mutex should not be poisoned");
            let (mut blocked, _) = wake
                .wait_timeout_while(blocked, Duration::from_secs(1), |blocked| *blocked)
                .expect("watchdog control mutex should not be poisoned");
            if *blocked {
                *blocked = false;
                wake.notify_all();
            }
        });

        let attempts = Arc::new(AtomicUsize::new(0));
        let executed = Arc::new(AtomicUsize::new(0));
        let attempt_notify = Arc::new(tokio::sync::Notify::new());
        let (heartbeat_tx, heartbeat_rx) = mpsc::channel();
        let heartbeat_notify = Arc::clone(&attempt_notify);
        let heartbeat = tokio::spawn(async move {
            heartbeat_notify.notified().await;
            tokio::task::yield_now().await;
            heartbeat_tx
                .send(())
                .expect("heartbeat receiver should remain alive");
        });

        let mut waiting_tasks = Vec::new();
        for _ in 0..WAITING_COMMANDS {
            let command = Arc::new(WaitingSharedCmd::new(
                Arc::clone(&attempts),
                Arc::clone(&attempt_notify),
                Arc::clone(&executed),
            ));
            let client = Arc::new(Client::new(Box::new(TestStream::new())));
            client.set_cmd_name(b"waiting-shared");
            client.set_argv(&[b"waiting-shared".to_vec()]);
            let waiting_executor = Arc::clone(&shared_executor);
            let waiting_storage = Arc::clone(&storage);
            waiting_tasks.push(tokio::spawn(async move {
                waiting_executor
                    .execute(CmdExecution {
                        cmd: command,
                        client,
                        storage: waiting_storage,
                    })
                    .await;
            }));
        }

        let heartbeat_result = tokio::task::spawn_blocking(move || {
            heartbeat_rx.recv_timeout(Duration::from_millis(250))
        })
        .await
        .expect("heartbeat waiter should not panic");
        assert!(
            heartbeat_result.is_ok(),
            "gate waiters must yield so an unrelated heartbeat can run"
        );
        tokio::time::timeout(Duration::from_millis(250), async {
            while attempts.load(Ordering::SeqCst) < WAITING_COMMANDS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all shared commands should reach the async gate");
        assert_eq!(executed.load(Ordering::SeqCst), 0);

        let (blocked, wake) = control.as_ref();
        *blocked
            .lock()
            .expect("blocking control mutex should not be poisoned") = false;
        wake.notify_all();

        exclusive_task
            .await
            .expect("exclusive command task should finish");
        for task in waiting_tasks {
            task.await.expect("shared command task should finish");
        }
        heartbeat.await.expect("heartbeat task should finish");
        watchdog.join().expect("watchdog thread should finish");
        assert_eq!(executed.load(Ordering::SeqCst), WAITING_COMMANDS);

        match Arc::try_unwrap(exclusive_executor) {
            Ok(mut executor) => executor.close().await,
            Err(_) => panic!("all exclusive executor owners should be released"),
        }
        match Arc::try_unwrap(shared_executor) {
            Ok(mut executor) => executor.close().await,
            Err(_) => panic!("all shared executor owners should be released"),
        }
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
