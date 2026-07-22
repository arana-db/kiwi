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

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

use clap::Parser;
use conf::config::Config;
use log::{debug, error, info, warn};
use runtime::{
    DualRuntimeError, GlobalStorage, RuntimeManager,
    StorageAccessPermit as RuntimeStorageAccessPermit, StorageServer, StorageServerPauseController,
};
use storage::StorageOptions;
use storage::storage::Storage;

use raft::node::{RaftApp, RaftConfig, create_raft_node};
use raft::raft_proto;
use raft::state_machine::{PauseController, StorageAccessPermit};

struct PauseControllerWrapper(StorageServerPauseController);
struct PausePermitWrapper {
    _permit: RuntimeStorageAccessPermit,
}

impl StorageAccessPermit for PausePermitWrapper {}

impl PauseController for PauseControllerWrapper {
    fn request_pause(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(self.0.request_pause())
    }

    fn enter(
        self: Arc<Self>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>,
    > {
        Box::pin(async move {
            Box::new(PausePermitWrapper {
                _permit: self.0.enter().await,
            }) as Box<dyn StorageAccessPermit>
        })
    }

    fn resume(&self) {
        self.0.resume();
    }
}

#[derive(Parser)]
#[command(name = "kiwi")]
#[command(about = "A Redis-compatible key-value database built in Rust")]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Args {
    #[arg(short, long)]
    config: Option<String>,

    #[arg(long)]
    single_node: bool,

    #[arg(long)]
    init_cluster: bool,

    #[arg(long)]
    sample_config: bool,

    #[arg(long)]
    full_sample_config: bool,
}

fn main() -> std::io::Result<()> {
    env_logger::init();

    let args = Args::parse();

    if args.full_sample_config {
        print!("{}", Config::full_sample_config());
        return Ok(());
    }

    if args.sample_config {
        print!("{}", Config::sample_config());
        return Ok(());
    }

    let config = if let Some(config_path) = args.config {
        Config::load(&config_path).map_err(|_e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to load config file '{}': {}", config_path, _e),
            )
        })?
    } else {
        Config::default()
    };

    let addr = format!("{}:{}", config.binding, config.port);
    let protocol = "tcp";

    let runtime_config = config.runtime.clone();
    info!(
        "Creating RuntimeManager with {} network threads and {} storage threads",
        runtime_config.network_threads, runtime_config.storage_threads
    );

    let mut runtime_manager = RuntimeManager::new(runtime_config)
        .map_err(|e| std::io::Error::other(format!("Failed to create RuntimeManager: {}", e)))?;

    let basic_rt = tokio::runtime::Runtime::new()
        .map_err(|e| std::io::Error::other(format!("Failed to create basic runtime: {}", e)))?;

    basic_rt.block_on(async {
        if let Err(e) = runtime_manager.start().await {
            error!("Failed to start RuntimeManager: {}", e);
            return Err(std::io::Error::other(format!(
                "Failed to start RuntimeManager: {}",
                e
            )));
        }
        Ok(())
    })?;

    info!("RuntimeManager started successfully");

    let network_handle = runtime_manager
        .network_handle()
        .map_err(|e| std::io::Error::other(format!("Failed to get network handle: {}", e)))?;
    let storage_handle = runtime_manager
        .storage_handle()
        .map_err(|e| std::io::Error::other(format!("Failed to get storage handle: {}", e)))?;

    let storage_receiver = runtime_manager
        .initialize_storage_components()
        .map_err(|e| {
            std::io::Error::other(format!("Failed to initialize storage components: {}", e))
        })?;

    let result = network_handle.block_on(async {
        let storage = initialize_storage(&config)
            .await
            .map_err(|e| std::io::Error::other(format!("Failed to initialize storage: {}", e)))?;

        info!("Storage components initialized, starting storage server...");

        let pause_controller = StorageServerPauseController::new();
        let pause_controller_for_raft = pause_controller.clone();

        let storage_for_server = storage.clone();
        let requirepass_for_storage_server = config.requirepass.clone();
        storage_handle.spawn(async move {
            info!("Initializing storage server...");
            match initialize_storage_server(
                storage_receiver,
                storage_for_server,
                pause_controller,
                requirepass_for_storage_server,
            )
            .await
            {
                Ok(_) => {
                    error!("Storage server exited unexpectedly - this should never happen!");
                }
                Err(e) => {
                    error!("Storage server failed: {}", e);
                }
            }
        });

        std::thread::sleep(std::time::Duration::from_millis(100));

        info!("Storage server started in background");
        info!("Starting Kiwi server in single-node mode on {}", addr);

        match start_server(
            protocol,
            &addr,
            &mut runtime_manager,
            &storage,
            &config,
            pause_controller_for_raft,
        )
        .await
        {
            Ok(_) => info!("Server started successfully"),
            Err(e) => {
                error!("Failed to start server: {}", e);
                return Err(e);
            }
        }

        info!("Press Ctrl+C to stop.");
        tokio::signal::ctrl_c().await.map_err(|e| {
            std::io::Error::other(format!("Failed to listen for shutdown signal: {}", e))
        })?;
        info!("Received shutdown signal, stopping...");

        Ok(())
    });

    if let Err(e) = network_handle.block_on(runtime_manager.stop()) {
        warn!("Error during RuntimeManager shutdown: {}", e);
    }

    result
}

async fn initialize_storage(config: &Config) -> Result<GlobalStorage, DualRuntimeError> {
    info!("Initializing storage...");

    let storage_options = Arc::new(StorageOptions::from_config(config));
    let data_dir = PathBuf::from(&config.data_dir);

    let mut storage = Storage::new(config.db_instance_num, 0);

    info!("Opening storage at path: {:?}", data_dir);
    let bg_task_receiver = storage
        .open(storage_options, &data_dir)
        .map_err(|e| DualRuntimeError::storage_runtime(format!("Failed to open storage: {}", e)))?;
    info!("Storage opened successfully");

    tokio::spawn(async move {
        let mut receiver = bg_task_receiver;
        while let Some(_task) = receiver.recv().await {
            debug!("Processing background task");
        }
        info!("Background task receiver closed");
    });

    Ok(GlobalStorage::new(storage))
}

async fn initialize_storage_server(
    request_receiver: tokio::sync::mpsc::Receiver<runtime::StorageRequest>,
    global_storage: GlobalStorage,
    pause_controller: StorageServerPauseController,
    requirepass: Option<String>,
) -> Result<(), DualRuntimeError> {
    info!("Initializing storage server...");

    // Initialize the storage-runtime command table with the same password
    // provider used by the network runtime, so AUTH behaves consistently.
    runtime::initialize_storage_command_table(Arc::new(move || requirepass.clone()));

    let storage_server =
        StorageServer::with_pause_controller(global_storage, request_receiver, pause_controller);

    info!("Storage server created, starting processing...");
    storage_server.run().await?;

    Ok(())
}

async fn start_server(
    protocol: &str,
    addr: &str,
    runtime_manager: &mut RuntimeManager,
    global_storage: &GlobalStorage,
    config: &Config,
    pause_controller: StorageServerPauseController,
) -> std::io::Result<()> {
    use conf::raft_type::{Binlog, BinlogResponse};
    use tokio::sync::{mpsc, oneshot};

    // Cluster mode: stand up Raft, wire the append-log bridge, expose a leader gate.
    let leader_gate: Option<Arc<dyn raft::leader_gate::LeaderGate>> = if let Some(raft_config) =
        &config.raft
    {
        let raft_config = RaftConfig {
            node_id: raft_config.node_id,
            raft_addr: raft_config.raft_addr.clone(),
            resp_addr: raft_config.resp_addr.clone(),
            data_dir: PathBuf::from(&raft_config.data_dir),
            db_path: PathBuf::from(&config.data_dir),
            heartbeat_interval: raft_config.heartbeat_interval_ms.unwrap_or(200),
            election_timeout_min: raft_config.election_timeout_min_ms.unwrap_or(500),
            election_timeout_max: raft_config.election_timeout_max_ms.unwrap_or(1500),
            ..RaftConfig::default()
        };

        let storage_swap = global_storage.arc_swap();
        let pause_controller_wrapper = Arc::new(PauseControllerWrapper(pause_controller));
        let append_log_fn_holder = Arc::new(OnceLock::new());

        let raft_app = create_raft_node(
            raft_config,
            storage_swap,
            pause_controller_wrapper,
            Some(append_log_fn_holder.clone()),
        )
        .await
        .map_err(|e| std::io::Error::other(format!("Failed to create Raft node: {}", e)))?;

        // Bridge: storage runtime -> (channel) -> network runtime drain task -> client_write.
        let (log_tx, mut log_rx) =
            mpsc::unbounded_channel::<(Binlog, oneshot::Sender<Result<BinlogResponse, String>>)>();

        // NOTE: this drains serially — one Raft consensus round-trip at a time.
        // Acceptable for now; a throughput follow-up may spawn per-message or add
        // backpressure via a bounded channel.
        let raft_for_drain = raft_app.clone();
        tokio::spawn(async move {
            while let Some((binlog, resp_tx)) = log_rx.recv().await {
                let result = raft_for_drain
                    .client_write(binlog)
                    .await
                    .map_err(|e| e.to_string());
                let _ = resp_tx.send(result);
            }
            warn!("Raft append-log drain task exited: channel closed");
        });

        // append_log_fn is invoked synchronously from BinlogBatch::commit, which
        // runs inside a tokio task on the (multi-threaded) storage runtime. A bare
        // blocking_recv would panic ("Cannot block ... within an asynchronous
        // execution context"), so we wrap it in block_in_place; the drain task that
        // resolves the oneshot runs on the separate network runtime.
        let append_log_fn: storage::AppendLogFn = Arc::new(move |binlog| {
            let (tx, rx) = oneshot::channel();
            log_tx
                .send((binlog, tx))
                .map_err(|_| "raft log channel closed".to_string())?;
            tokio::task::block_in_place(|| rx.blocking_recv())
                .map_err(|_| "raft response channel closed".to_string())?
        });
        let _ = append_log_fn_holder.set(append_log_fn.clone());
        global_storage.load().set_append_log_fn(append_log_fn);

        let raft_addr = raft_app.raft_addr.clone();
        let grpc_addr = raft_addr.parse::<std::net::SocketAddr>().map_err(|e| {
            std::io::Error::other(format!("Invalid Raft address '{}': {}", raft_addr, e))
        })?;

        let (core_svc, admin_svc, client_svc, metrics_svc) =
            RaftApp::create_grpc_services(raft_app.clone());

        info!("Starting Raft gRPC server on {}", raft_addr);

        let reflect_svc = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(raft_proto::FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|e| {
                std::io::Error::other(format!("Failed to create reflection service: {}", e))
            })?;

        let grpc_listener = tokio::net::TcpListener::bind(grpc_addr)
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to bind Raft gRPC server on {}: {}",
                    grpc_addr, e
                ))
            })?;

        tokio::spawn(async move {
            use tonic::transport::Server;

            let incoming = tokio_stream::wrappers::TcpListenerStream::new(grpc_listener);

            info!("Raft gRPC server listening on {}", grpc_addr);

            if let Err(e) = Server::builder()
                .add_service(reflect_svc)
                .add_service(core_svc)
                .add_service(admin_svc)
                .add_service(client_svc)
                .add_service(metrics_svc)
                .serve_with_incoming(incoming)
                .await
            {
                let error_message = e.to_string();
                error!("Raft gRPC server error: {}", error_message);
            }
        });

        Some(raft_app as Arc<dyn raft::leader_gate::LeaderGate>)
    } else {
        None
    };

    if let Some(server) = net::ServerFactory::create_server(
        protocol,
        Some(addr.to_string()),
        runtime_manager,
        config.requirepass.clone(),
        leader_gate,
    ) {
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!("Redis server error: {}", e);
            }
        });
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "Failed to create server for protocol '{}' on address '{}'",
            protocol, addr
        )))
    }
}
