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

#![allow(clippy::unwrap_used)]

//! End-to-end regression tests for the unified storage command dispatch path.
//!
//! These tests start a full dual-runtime stack (real `RuntimeManager`, RocksDB
//! `Storage`, `StorageServer`, and `NetworkServer`) and drive it over TCP with
//! raw RESP2 frames. They exercise the complete chain:
//!
//! ```text
//! process_network_connection -> execute_network -> StorageClient::execute_command
//! -> StorageServer::execute_storage_command
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use client::Client;
use cmd::{AclCategory, Cmd, CmdFlags, CmdMeta};
use net::network_server::NetworkServer;
use net::storage_client::StorageClient;
use resp::{
    Parse, RespData, RespEncode, RespParse, RespParseResult, RespVersion, encode::RespEncoder,
};
use runtime::{
    GlobalStorage, RuntimeConfig, RuntimeManager, StorageServer, StorageServerConfig,
    storage_server::initialize_storage_command_table,
};
use storage::{StorageOptions, safe_cleanup_test_db, storage::Storage, unique_test_db_path};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct PanickingPingCmd {
    meta: CmdMeta,
}

impl PanickingPingCmd {
    fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "ping".to_string(),
                arity: -1,
                flags: CmdFlags::READONLY | CmdFlags::FAST | CmdFlags::NO_AUTH,
                acl_category: AclCategory::FAST | AclCategory::CONNECTION,
                ..Default::default()
            },
        }
    }
}

impl Cmd for PanickingPingCmd {
    fn meta(&self) -> &CmdMeta {
        &self.meta
    }

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, _client: &Client, _storage: Arc<Storage>) {
        panic!("simulated connection task panic");
    }

    fn clone_box(&self) -> Box<dyn Cmd> {
        Box::new(self.clone())
    }
}

/// A full dual-runtime test stack with a bound TCP endpoint.
struct TestServer {
    addr: SocketAddr,
    runtime_manager: RuntimeManager,
    db_path: PathBuf,
    storage_client: Arc<StorageClient>,
    network_server: Arc<NetworkServer>,
    network_shutdown: CancellationToken,
    network_task: Option<JoinHandle<Result<(), String>>>,
}

impl TestServer {
    /// Start the network and storage runtimes, open a real storage DB, and bind
    /// a `NetworkServer` to an ephemeral port.
    async fn start(requirepass: Option<String>) -> Self {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);

        // Open a real RocksDB-backed storage instance.
        let mut storage = Storage::new(1, 0);
        let options = Arc::new(StorageOptions::default());
        let bg_receiver = storage.open(options, &db_path).expect("open storage");
        tokio::spawn(async move {
            let mut rx = bg_receiver;
            while rx.recv().await.is_some() {}
        });

        // Start dedicated network and storage runtimes.
        let runtime_config = RuntimeConfig::new(
            1,                        // network_threads
            1,                        // storage_threads
            1000,                     // channel_buffer_size
            Duration::from_secs(10),  // request_timeout
            10,                       // batch_size
            Duration::from_millis(1), // batch_timeout
        )
        .expect("valid runtime config");

        let mut runtime_manager = RuntimeManager::new(runtime_config).expect("runtime manager");
        runtime_manager.start().await.expect("start runtimes");

        let request_receiver = runtime_manager
            .initialize_storage_components()
            .expect("init storage components");
        let network_handle = runtime_manager.network_handle().expect("network handle");
        let storage_handle = runtime_manager.storage_handle().expect("storage handle");
        let runtime_storage_client = runtime_manager.storage_client().expect("storage client");

        // Network-side client, command table, and executor.
        let net_storage_client = Arc::new(StorageClient::new(runtime_storage_client.clone()));
        let requirepass_for_provider = requirepass.clone();
        let cmd_table = Arc::new(cmd::table::create_command_table(Arc::new(move || {
            requirepass_for_provider.clone()
        })));
        let executor = Arc::new(executor::CmdExecutorBuilder::new().build());

        // Bind the network server to an ephemeral port before spawning it.
        let network_server = Arc::new(
            NetworkServer::new(
                Some("127.0.0.1:0".to_string()),
                net_storage_client.clone(),
                cmd_table,
                executor,
                requirepass,
                None,
            )
            .expect("network server"),
        );
        let addr = network_server.bind().await.expect("bind network server");

        let network_shutdown = CancellationToken::new();
        let shutdown_for_task = network_shutdown.clone();
        let server_clone = network_server.clone();
        let network_task = network_handle.spawn(async move {
            server_clone
                .run_until_cancelled(shutdown_for_task)
                .await
                .map_err(|error| error.to_string())
        });

        // Start the storage server on the storage runtime.
        let global_storage = GlobalStorage::new(storage);
        storage_handle.spawn(async move {
            initialize_storage_command_table(Arc::new(|| None));
            let config = StorageServerConfig {
                enable_batching: false,
                enable_background_tasks: false,
                ..StorageServerConfig::default()
            };
            let storage_server =
                StorageServer::with_config(global_storage, request_receiver, config);
            let _ = storage_server.run().await;
        });

        wait_for_server(addr).await;

        Self {
            addr,
            runtime_manager,
            db_path,
            storage_client: net_storage_client,
            network_server,
            network_shutdown,
            network_task: Some(network_task),
        }
    }

    async fn stop_network(&mut self) -> Result<(), String> {
        self.network_shutdown.cancel();
        self.wait_for_network_stop().await
    }

    async fn wait_for_network_stop(&mut self) -> Result<(), String> {
        let Some(network_task) = self.network_task.take() else {
            return Ok(());
        };

        match tokio::time::timeout(Duration::from_secs(1), network_task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(format!("network task join failed: {error}")),
            Err(_) => Err("network task did not stop within one second".to_string()),
        }
    }

    /// Stop the runtimes and clean up the temporary storage directory.
    async fn shutdown(mut self) {
        self.stop_network().await.expect("stop network server");
        let _ = tokio::time::timeout(Duration::from_secs(5), self.runtime_manager.stop()).await;
        safe_cleanup_test_db(&self.db_path);
    }
}

/// Poll until the server accepts TCP connections, with a timeout.
async fn wait_for_server(addr: SocketAddr) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server did not become connectable at {}", addr);
}

/// Encode a Redis command as a RESP2 array of bulk strings.
fn encode_command(args: &[&str]) -> Bytes {
    let binary_args: Vec<&[u8]> = args.iter().map(|arg| arg.as_bytes()).collect();
    encode_binary_command(&binary_args)
}

/// Encode a Redis command with binary-safe bulk string arguments.
fn encode_binary_command(args: &[&[u8]]) -> Bytes {
    let mut encoder = RespEncoder::new(RespVersion::RESP2);
    encoder.append_array_len(args.len() as i64);
    for arg in args {
        encoder.append_bulk_string(arg);
    }
    encoder.get_response()
}

/// Read and parse a single RESP frame from the stream within a bounded timeout.
async fn read_response_with_version(
    stream: &mut tokio::net::TcpStream,
    version: RespVersion,
) -> RespData {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut parser = RespParse::new(version);
        let mut buf = vec![0u8; 4096];
        loop {
            let n = stream.read(&mut buf).await.expect("read from server");
            if n == 0 {
                panic!("server closed connection before responding");
            }
            match parser.parse(Bytes::copy_from_slice(&buf[..n])) {
                RespParseResult::Complete(data) => return data,
                RespParseResult::Incomplete => continue,
                RespParseResult::Error(e) => panic!("RESP parse error: {:?}", e),
            }
        }
    })
    .await
    .expect("timed out waiting for RESP response")
}

/// Send a command and return its parsed RESP response.
async fn send_command(stream: &mut tokio::net::TcpStream, args: &[&str]) -> RespData {
    send_command_with_version(stream, args, RespVersion::RESP2).await
}

/// Send a command with binary-safe arguments and return its parsed RESP2 response.
async fn send_binary_command(stream: &mut tokio::net::TcpStream, args: &[&[u8]]) -> RespData {
    stream
        .write_all(encode_binary_command(args).as_ref())
        .await
        .expect("write to server");
    read_response_with_version(stream, RespVersion::RESP2).await
}

/// Send a command and parse its response using the specified protocol version.
async fn send_command_with_version(
    stream: &mut tokio::net::TcpStream,
    args: &[&str],
    version: RespVersion,
) -> RespData {
    stream
        .write_all(encode_command(args).as_ref())
        .await
        .expect("write to server");
    read_response_with_version(stream, version).await
}

/// Send a command whose response is a single RESP line and return its exact wire bytes.
async fn send_command_and_read_line(stream: &mut tokio::net::TcpStream, args: &[&str]) -> Bytes {
    stream
        .write_all(encode_command(args).as_ref())
        .await
        .expect("write to server");

    tokio::time::timeout(Duration::from_secs(5), async {
        let mut response = Vec::with_capacity(16);
        loop {
            response.push(stream.read_u8().await.expect("read from server"));
            if response.ends_with(b"\r\n") {
                return Bytes::from(response);
            }
            assert!(response.len() < 256, "unexpectedly long RESP line");
        }
    })
    .await
    .expect("timed out waiting for RESP line")
}

async fn wait_for_connection_tasks_reaped(server: &NetworkServer, minimum_accepted: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let stats = server.lifecycle_stats();
        if stats.accepted_connection_tasks >= minimum_accepted
            && stats.completed_connection_tasks == stats.accepted_connection_tasks
            && stats.reaped_connection_tasks == stats.accepted_connection_tasks
            && stats.tracked_connection_tasks == 0
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "connection tasks did not drain: {stats:?}"
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_idle_pool_entry(server: &NetworkServer) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let stats = server.pool_stats().await;
        if stats.active_connections == 0 && stats.available_connections > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "network resource did not return to pool: {stats:?}"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn network_server_cancel_stops_listener_and_existing_connection() {
    let mut server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let reply = send_command(&mut stream, &["PING"]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"PONG")));

    let key = "cancelled-connection-must-not-write";
    server.network_shutdown.cancel();
    let command = encode_command(&["SET", key, "unexpected"]);
    let _ = stream.write_all(command.as_ref()).await;
    server
        .wait_for_network_stop()
        .await
        .expect("cancel network server");

    let reply = server
        .storage_client
        .execute_command(b"GET", &[b"GET".to_vec(), key.as_bytes().to_vec()])
        .await
        .expect("read directly through storage client");
    assert_eq!(
        reply,
        RespData::BulkString(None),
        "the keep-alive connection executed SET after cancellation"
    );

    assert!(
        tokio::net::TcpStream::connect(server.addr).await.is_err(),
        "listener still accepted connections after shutdown completed"
    );

    let mut byte = [0u8; 1];
    match tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("existing connection returned a response after cancellation"),
        Err(_) => panic!("existing connection remained open after cancellation"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn network_server_reaps_short_lived_connection_tasks_while_running() {
    const WAVE_SIZE: u64 = 8;
    const WAVES: u64 = 8;

    let server = TestServer::start(None).await;
    wait_for_connection_tasks_reaped(&server.network_server, 1).await;
    let baseline = server.network_server.lifecycle_stats();

    for wave in 1..=WAVES {
        for _ in 0..WAVE_SIZE {
            let mut stream = tokio::net::TcpStream::connect(server.addr)
                .await
                .expect("connect short-lived client");
            let reply = send_command(&mut stream, &["PING"]).await;
            assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"PONG")));
            stream
                .shutdown()
                .await
                .expect("shutdown short-lived client");
        }

        wait_for_connection_tasks_reaped(
            &server.network_server,
            baseline.accepted_connection_tasks + wave * WAVE_SIZE,
        )
        .await;
    }

    let stats = server.network_server.lifecycle_stats();
    assert_eq!(stats.tracked_connection_tasks, 0);
    assert_eq!(
        stats.reaped_connection_tasks,
        stats.accepted_connection_tasks
    );
    assert!(stats.reaped_connection_tasks >= baseline.reaped_connection_tasks + WAVE_SIZE * WAVES);
    assert!(
        stats.max_tracked_connection_tasks <= WAVE_SIZE as usize,
        "tracked task high-water mark followed historical connections: {stats:?}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn network_server_protocol_error_only_closes_the_bad_connection() {
    let server = TestServer::start(None).await;
    let mut malformed = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect malformed client");
    malformed
        .write_all(b"$not-a-number\r\n")
        .await
        .expect("write malformed RESP");
    let mut byte = [0u8; 1];
    match tokio::time::timeout(Duration::from_secs(1), malformed.read(&mut byte)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("malformed connection unexpectedly received a response"),
        Err(_) => panic!("malformed connection was not closed"),
    }

    let mut healthy = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect healthy client after protocol error");
    assert_eq!(
        send_command(&mut healthy, &["PING"]).await,
        RespData::SimpleString(Bytes::from_static(b"PONG"))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn network_server_continues_after_connection_task_panic() {
    let message_channel = Arc::new(runtime::MessageChannel::new(16));
    let runtime_storage_client = Arc::new(runtime::StorageClient::new(
        message_channel,
        Duration::from_secs(1),
    ));
    let mut cmd_table = cmd::table::create_command_table(Arc::new(|| Some("secret".to_string())));
    cmd_table.insert("ping".to_string(), Arc::new(PanickingPingCmd::new()));
    let server = Arc::new(
        NetworkServer::new(
            Some("127.0.0.1:0".to_string()),
            Arc::new(StorageClient::new(runtime_storage_client)),
            Arc::new(cmd_table),
            Arc::new(executor::CmdExecutorBuilder::new().build()),
            Some("secret".to_string()),
            None,
        )
        .expect("network server"),
    );
    let addr = server.bind().await.expect("bind network server");
    let shutdown = CancellationToken::new();
    let server_for_task = server.clone();
    let shutdown_for_task = shutdown.clone();
    let network_task = tokio::spawn(async move {
        server_for_task
            .run_until_cancelled(shutdown_for_task)
            .await
            .map_err(|error| error.to_string())
    });

    let mut panicking_client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect panicking client");
    panicking_client
        .write_all(encode_command(&["PING"]).as_ref())
        .await
        .expect("send panicking command");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let stats = server.lifecycle_stats();
        if stats.failed_connection_tasks == 1
            && stats.reaped_connection_tasks >= 1
            && stats.tracked_connection_tasks == 0
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "panicking connection task was not recorded and reaped: {stats:?}"
        );
        tokio::task::yield_now().await;
    }
    assert!(
        !network_task.is_finished(),
        "one connection panic stopped the network server"
    );

    let mut healthy_client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect healthy client after panic");
    assert_eq!(
        send_command(&mut healthy_client, &["AUTH", "secret"]).await,
        RespData::SimpleString(Bytes::from_static(b"OK"))
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), network_task)
        .await
        .expect("network shutdown timeout")
        .expect("network task join")
        .expect("network server shutdown");

    let stats = server.lifecycle_stats();
    assert_eq!(stats.failed_connection_tasks, 1);
    assert_eq!(
        stats.accepted_connection_tasks,
        stats.completed_connection_tasks
    );
    assert_eq!(
        stats.accepted_connection_tasks,
        stats.reaped_connection_tasks
    );
    assert_eq!(stats.tracked_connection_tasks, 0);
}

#[tokio::test]
async fn network_server_shutdown_joins_cleanup_clears_pool_and_releases_channel() {
    let mut message_channel = runtime::MessageChannel::new(16);
    let mut request_receiver = message_channel
        .take_request_receiver()
        .expect("take request receiver");
    let message_channel = Arc::new(message_channel);
    let runtime_storage_client = Arc::new(runtime::StorageClient::new(
        message_channel.clone(),
        Duration::from_secs(1),
    ));
    let storage_client = Arc::new(StorageClient::new(runtime_storage_client.clone()));
    let cmd_table = Arc::new(cmd::table::create_command_table(Arc::new(|| None)));
    let executor = Arc::new(executor::CmdExecutorBuilder::new().build());
    let server = Arc::new(
        NetworkServer::new(
            Some("127.0.0.1:0".to_string()),
            storage_client.clone(),
            cmd_table,
            executor,
            None,
            None,
        )
        .expect("network server"),
    );
    let addr = server.bind().await.expect("bind network server");
    let shutdown = CancellationToken::new();
    let shutdown_for_task = shutdown.clone();
    let server_for_task = server.clone();
    let network_task = tokio::spawn(async move {
        server_for_task
            .run_until_cancelled(shutdown_for_task)
            .await
            .map_err(|error| error.to_string())
    });

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to server");
    assert_eq!(
        send_command(&mut stream, &["PING"]).await,
        RespData::SimpleString(Bytes::from_static(b"PONG"))
    );
    stream.shutdown().await.expect("shutdown client");
    wait_for_idle_pool_entry(&server).await;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), network_task)
        .await
        .expect("network shutdown timeout")
        .expect("network task join")
        .expect("network server shutdown");

    let pool_stats = server.pool_stats().await;
    assert_eq!(pool_stats.active_connections, 0);
    assert_eq!(pool_stats.available_connections, 0);
    let lifecycle_stats = server.lifecycle_stats();
    assert!(!lifecycle_stats.pool_cleanup_running);
    assert!(lifecycle_stats.pool_cleanup_finished);

    drop(server);
    drop(storage_client);
    drop(runtime_storage_client);
    drop(message_channel);

    assert!(
        tokio::time::timeout(Duration::from_secs(1), request_receiver.recv())
            .await
            .expect("request receiver EOF timeout")
            .is_none(),
        "request receiver remained open after the minimal ownership chain was dropped"
    );
}

#[tokio::test]
async fn network_server_abort_releases_cleanup_pool_and_storage_sender() {
    let mut message_channel = runtime::MessageChannel::new(16);
    let mut request_receiver = message_channel
        .take_request_receiver()
        .expect("take request receiver");
    let message_channel = Arc::new(message_channel);
    let runtime_storage_client = Arc::new(runtime::StorageClient::new(
        message_channel.clone(),
        Duration::from_secs(1),
    ));
    let storage_client = Arc::new(StorageClient::new(runtime_storage_client.clone()));
    let server = Arc::new(
        NetworkServer::new(
            Some("127.0.0.1:0".to_string()),
            storage_client.clone(),
            Arc::new(cmd::table::create_command_table(Arc::new(|| None))),
            Arc::new(executor::CmdExecutorBuilder::new().build()),
            None,
            None,
        )
        .expect("network server"),
    );
    let addr = server.bind().await.expect("bind network server");
    let server_for_task = server.clone();
    let network_task = tokio::spawn(async move {
        server_for_task
            .run_until_cancelled(CancellationToken::new())
            .await
            .map_err(|error| error.to_string())
    });

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to server");
    assert_eq!(
        send_command(&mut stream, &["PING"]).await,
        RespData::SimpleString(Bytes::from_static(b"PONG"))
    );
    stream.shutdown().await.expect("shutdown client");
    wait_for_idle_pool_entry(&server).await;

    network_task.abort();
    let join_error = network_task
        .await
        .expect_err("aborted network task joined cleanly");
    assert!(join_error.is_cancelled());

    let repeated_error = server
        .run_until_cancelled(CancellationToken::new())
        .await
        .expect_err("aborted server ran again");
    assert!(repeated_error.to_string().contains("already terminated"));
    assert!(server.bind().await.is_err(), "aborted server rebound");

    drop(server);
    drop(storage_client);
    drop(runtime_storage_client);
    drop(message_channel);

    assert!(
        tokio::time::timeout(Duration::from_secs(1), request_receiver.recv())
            .await
            .expect("request receiver EOF timeout after abort")
            .is_none(),
        "detached cleanup task retained a pooled storage sender after abort"
    );
}

#[tokio::test]
async fn network_server_waits_for_inflight_storage_request_before_shutdown() {
    let message_channel = Arc::new(runtime::MessageChannel::new(1));
    let request_sender = message_channel.request_sender();
    let (response_sender, _response_receiver) = tokio::sync::oneshot::channel();
    request_sender
        .send(runtime::StorageRequest::new(
            runtime::RequestId::new(),
            runtime::StorageCommand::Execute {
                cmd_name: b"GET".to_vec(),
                argv: vec![b"GET".to_vec(), b"channel-saturation".to_vec()],
            },
            response_sender,
            Duration::from_secs(1),
            runtime::RequestPriority::Normal,
        ))
        .await
        .expect("saturate storage request channel");

    let runtime_storage_client = Arc::new(runtime::StorageClient::new(
        message_channel.clone(),
        Duration::from_millis(250),
    ));
    let storage_client = Arc::new(StorageClient::new(runtime_storage_client.clone()));
    let server = Arc::new(
        NetworkServer::new(
            Some("127.0.0.1:0".to_string()),
            storage_client,
            Arc::new(cmd::table::create_command_table(Arc::new(|| None))),
            Arc::new(executor::CmdExecutorBuilder::new().build()),
            None,
            None,
        )
        .expect("network server"),
    );
    let addr = server.bind().await.expect("bind network server");
    let shutdown = CancellationToken::new();
    let shutdown_for_task = shutdown.clone();
    let server_for_task = server.clone();
    let network_task = tokio::spawn(async move {
        server_for_task
            .run_until_cancelled(shutdown_for_task)
            .await
            .map_err(|error| error.to_string())
    });

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to server");
    assert_eq!(
        send_command(&mut stream, &["PING"]).await,
        RespData::SimpleString(Bytes::from_static(b"PONG"))
    );
    stream
        .write_all(encode_command(&["SET", "inflight", "value"]).as_ref())
        .await
        .expect("start storage command");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while runtime_storage_client.pending_request_count().await == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "storage command never entered pending state"
        );
        tokio::task::yield_now().await;
    }

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), network_task)
        .await
        .expect("network shutdown timeout")
        .expect("network task join")
        .expect("network server shutdown");
    assert_eq!(
        runtime_storage_client.pending_request_count().await,
        0,
        "cancellation dropped an in-flight storage future and leaked pending state"
    );
}

#[tokio::test]
async fn network_server_rejects_concurrent_and_repeated_run() {
    let message_channel = Arc::new(runtime::MessageChannel::new(16));
    let runtime_storage_client = Arc::new(runtime::StorageClient::new(
        message_channel,
        Duration::from_secs(1),
    ));
    let server = Arc::new(
        NetworkServer::new(
            Some("127.0.0.1:0".to_string()),
            Arc::new(StorageClient::new(runtime_storage_client)),
            Arc::new(cmd::table::create_command_table(Arc::new(|| None))),
            Arc::new(executor::CmdExecutorBuilder::new().build()),
            None,
            None,
        )
        .expect("network server"),
    );
    let addr = server.bind().await.expect("bind network server");
    let shutdown = CancellationToken::new();
    let shutdown_for_task = shutdown.clone();
    let server_for_task = server.clone();
    let first_run = tokio::spawn(async move {
        server_for_task
            .run_until_cancelled(shutdown_for_task)
            .await
            .map_err(|error| error.to_string())
    });

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to first run");
    assert_eq!(
        send_command(&mut stream, &["PING"]).await,
        RespData::SimpleString(Bytes::from_static(b"PONG"))
    );

    let concurrent_error = tokio::time::timeout(
        Duration::from_millis(100),
        server.run_until_cancelled(CancellationToken::new()),
    )
    .await
    .expect("concurrent run did not fail fast")
    .expect_err("concurrent run unexpectedly succeeded");
    assert!(concurrent_error.to_string().contains("already running"));

    assert_eq!(
        send_command(&mut stream, &["PING"]).await,
        RespData::SimpleString(Bytes::from_static(b"PONG")),
        "failed concurrent run disrupted the active server"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), first_run)
        .await
        .expect("first run shutdown timeout")
        .expect("first run join")
        .expect("first run shutdown");

    let repeated_error = tokio::time::timeout(
        Duration::from_millis(100),
        server.run_until_cancelled(CancellationToken::new()),
    )
    .await
    .expect("repeated run did not fail fast")
    .expect_err("repeated run unexpectedly succeeded");
    assert!(repeated_error.to_string().contains("already terminated"));
    assert!(server.bind().await.is_err(), "terminated server rebound");
}

#[tokio::test]
async fn network_server_rejects_duplicate_bind() {
    let message_channel = Arc::new(runtime::MessageChannel::new(16));
    let runtime_storage_client = Arc::new(runtime::StorageClient::new(
        message_channel,
        Duration::from_secs(1),
    ));
    let server = NetworkServer::new(
        Some("127.0.0.1:0".to_string()),
        Arc::new(StorageClient::new(runtime_storage_client)),
        Arc::new(cmd::table::create_command_table(Arc::new(|| None))),
        Arc::new(executor::CmdExecutorBuilder::new().build()),
        None,
        None,
    )
    .expect("network server");

    server.bind().await.expect("first bind");
    let error = server.bind().await.expect_err("duplicate bind succeeded");
    assert!(error.to_string().contains("already bound"));
}

#[tokio::test]
async fn storage_command_e2e_set_get_round_trip() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let reply = send_command(&mut stream, &["SET", "kiwi_key", "kiwi_value"]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"OK")));

    let reply = send_command(&mut stream, &["GET", "kiwi_key"]).await;
    assert_eq!(
        reply,
        RespData::BulkString(Some(Bytes::from_static(b"kiwi_value")))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn storage_command_e2e_wrong_number_of_arguments_returns_resp_error() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let reply = send_command(&mut stream, &["SET", "only_key"]).await;
    assert!(
        matches!(reply, RespData::Error(_)),
        "expected RESP error, got {:?}",
        reply
    );
    let text = reply.as_string().expect("error string");
    assert!(
        text.contains("wrong number of arguments"),
        "unexpected error: {}",
        text
    );

    server.shutdown().await;
}

#[tokio::test]
async fn storage_command_e2e_auth_requirepass_flow() {
    let server = TestServer::start(Some("secret".to_string())).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    // Unauthenticated non-NO_AUTH command is rejected.
    let reply = send_command(&mut stream, &["GET", "x"]).await;
    assert!(
        matches!(reply, RespData::Error(_)),
        "expected NOAUTH error, got {:?}",
        reply
    );
    let text = reply.as_string().expect("error string");
    assert!(
        text.contains("NOAUTH"),
        "expected NOAUTH error, got: {}",
        text
    );

    // Wrong password fails.
    let reply = send_command(&mut stream, &["AUTH", "wrong"]).await;
    assert!(
        matches!(reply, RespData::Error(_)),
        "expected WRONGPASS error, got {:?}",
        reply
    );
    let text = reply.as_string().expect("error string");
    assert!(
        text.contains("WRONGPASS"),
        "expected WRONGPASS error, got: {}",
        text
    );

    // Correct password authenticates the connection.
    let reply = send_command(&mut stream, &["AUTH", "secret"]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"OK")));

    // Subsequent commands traverse the generic storage path.
    let reply = send_command(&mut stream, &["SET", "x", "1"]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"OK")));

    let reply = send_command(&mut stream, &["GET", "x"]).await;
    assert_eq!(reply, RespData::BulkString(Some(Bytes::from_static(b"1"))));

    server.shutdown().await;
}

#[tokio::test]
async fn storage_command_e2e_ping_and_client_are_local() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let before = server.storage_client.channel_stats().await;

    let reply = send_command(&mut stream, &["PING"]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"PONG")));

    let reply = send_command(&mut stream, &["CLIENT", "SETNAME", "test_client"]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"OK")));

    let reply = send_command(&mut stream, &["CLIENT", "GETNAME"]).await;
    assert_eq!(
        reply,
        RespData::BulkString(Some(Bytes::from_static(b"test_client")))
    );

    let after = server.storage_client.channel_stats().await;
    assert_eq!(
        after.requests_sent, before.requests_sent,
        "local commands must not go through the storage channel"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn storage_command_e2e_generic_storage_commands_use_storage_path() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let before = server.storage_client.channel_stats().await;

    let reply = send_command(&mut stream, &["MSET", "a", "1", "b", "2"]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"OK")));

    let reply = send_command(&mut stream, &["MGET", "a", "b"]).await;
    assert_eq!(
        reply,
        RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from_static(b"1"))),
            RespData::BulkString(Some(Bytes::from_static(b"2"))),
        ]))
    );

    let reply = send_command(&mut stream, &["DEL", "a"]).await;
    assert_eq!(reply, RespData::Integer(1));

    let reply = send_command(&mut stream, &["EXPIRE", "b", "10"]).await;
    assert_eq!(reply, RespData::Integer(1));

    let after = server.storage_client.channel_stats().await;
    assert!(
        after.requests_sent >= before.requests_sent + 4,
        "generic commands should traverse the storage channel"
    );

    server.shutdown().await;
}

// Regression for issue #349: GET/MGET must return stored bytes unchanged over RESP.
#[tokio::test]
async fn storage_command_e2e_get_and_mget_preserve_binary_values() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let first_value = [0, 1, 2, 3, 255];
    let second_value = [255, 0, 254];

    let reply = send_binary_command(&mut stream, &[b"SET", b"binary:first", &first_value]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"OK")));
    let reply = send_binary_command(&mut stream, &[b"SET", b"binary:second", &second_value]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"OK")));

    let reply = send_binary_command(&mut stream, &[b"GET", b"binary:first"]).await;
    let RespData::BulkString(Some(actual)) = reply else {
        panic!("expected bulk string reply, got {reply:?}");
    };
    assert_eq!(
        actual.as_ref(),
        first_value,
        "GET returned different bytes: {actual:02x?}"
    );

    let reply = send_binary_command(
        &mut stream,
        &[
            b"MGET",
            b"binary:first",
            b"binary:missing",
            b"binary:second",
        ],
    )
    .await;
    assert_eq!(
        reply,
        RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::copy_from_slice(&first_value))),
            RespData::BulkString(None),
            RespData::BulkString(Some(Bytes::copy_from_slice(&second_value))),
        ]))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn storage_command_e2e_resp3_legacy_nulls_use_null_type() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let reply = send_command_with_version(&mut stream, &["HELLO", "3"], RespVersion::RESP3).await;
    assert!(
        matches!(reply, RespData::Map(_)),
        "expected RESP3 HELLO map, got {:?}",
        reply
    );

    let reply = send_command_with_version(
        &mut stream,
        &["LINDEX", "missing-list", "0"],
        RespVersion::RESP3,
    )
    .await;
    assert_eq!(reply, RespData::Null);

    let reply = send_command_with_version(
        &mut stream,
        &["MGET", "missing-key-1", "missing-key-2"],
        RespVersion::RESP3,
    )
    .await;
    assert_eq!(
        reply,
        RespData::Array(Some(vec![RespData::Null, RespData::Null]))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn storage_command_e2e_rank_nulls_follow_negotiated_wire_protocol() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    for command in ["ZRANK", "ZREVRANK"] {
        let reply =
            send_command_and_read_line(&mut stream, &[command, "missing-zset", "member"]).await;
        assert_eq!(reply, Bytes::from_static(b"$-1\r\n"), "{command}");
    }

    let reply = send_command_with_version(&mut stream, &["HELLO", "3"], RespVersion::RESP3).await;
    assert!(
        matches!(reply, RespData::Map(_)),
        "expected RESP3 HELLO map, got {:?}",
        reply
    );

    for command in ["ZRANK", "ZREVRANK"] {
        let reply =
            send_command_and_read_line(&mut stream, &[command, "missing-zset", "member"]).await;
        assert_eq!(reply, Bytes::from_static(b"_\r\n"), "{command}");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn storage_command_e2e_smismember_matches_redis_semantics() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let reply = send_command(&mut stream, &["SADD", "members", "a", "b"]).await;
    assert_eq!(reply, RespData::Integer(2));

    let reply = send_command(
        &mut stream,
        &["SMISMEMBER", "members", "a", "missing", "b", "a"],
    )
    .await;
    assert_eq!(
        reply,
        RespData::Array(Some(vec![
            RespData::Integer(1),
            RespData::Integer(0),
            RespData::Integer(1),
            RespData::Integer(1),
        ]))
    );

    let reply = send_command(&mut stream, &["SMISMEMBER", "missing-set", "a", "b"]).await;
    assert_eq!(
        reply,
        RespData::Array(Some(vec![RespData::Integer(0), RespData::Integer(0)]))
    );

    let binary_member = [0xff, 0, 0xfe];
    let reply = send_binary_command(
        &mut stream,
        &[b"SADD", b"binary-members", &binary_member, b""],
    )
    .await;
    assert_eq!(reply, RespData::Integer(2));

    let reply = send_binary_command(
        &mut stream,
        &[
            b"SMISMEMBER",
            b"binary-members",
            &binary_member,
            b"absent",
            b"",
        ],
    )
    .await;
    assert_eq!(
        reply,
        RespData::Array(Some(vec![
            RespData::Integer(1),
            RespData::Integer(0),
            RespData::Integer(1),
        ]))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn storage_command_e2e_smismember_preserves_redis_errors() {
    let server = TestServer::start(None).await;
    let mut stream = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("connect to server");

    let reply = send_command(&mut stream, &["SET", "not-a-set", "value"]).await;
    assert_eq!(reply, RespData::SimpleString(Bytes::from_static(b"OK")));

    let reply =
        send_command_and_read_line(&mut stream, &["SMISMEMBER", "not-a-set", "member"]).await;
    assert_eq!(
        reply,
        Bytes::from_static(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        )
    );

    let reply = send_command(&mut stream, &["SMISMEMBER", "members"]).await;
    assert_eq!(
        reply,
        RespData::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'smismember' command"
        ))
    );

    server.shutdown().await;
}
