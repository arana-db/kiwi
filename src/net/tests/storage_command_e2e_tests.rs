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
use net::{ServerTrait, network_server::NetworkServer, storage_client::StorageClient};
use resp::{
    Parse, RespData, RespEncode, RespParse, RespParseResult, RespVersion, encode::RespEncoder,
};
use runtime::{
    GlobalStorage, RuntimeConfig, RuntimeManager, StorageServer, StorageServerConfig,
    storage_server::initialize_storage_command_table,
};
use storage::{StorageOptions, safe_cleanup_test_db, storage::Storage, unique_test_db_path};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A full dual-runtime test stack with a bound TCP endpoint.
struct TestServer {
    addr: SocketAddr,
    runtime_manager: RuntimeManager,
    db_path: PathBuf,
    storage_client: Arc<StorageClient>,
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

        let server_clone = network_server.clone();
        network_handle.spawn(async move {
            let _ = server_clone.run().await;
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
        }
    }

    /// Stop the runtimes and clean up the temporary storage directory.
    async fn shutdown(mut self) {
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
            assert!(response.len() < 64, "unexpectedly long RESP line");
        }
    })
    .await
    .expect("timed out waiting for RESP line")
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
