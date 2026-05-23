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

use clap::Parser;
use conf::config::Config;
use log::{debug, error, info, warn};
use runtime::{
    DualRuntimeError, GlobalStorage, RuntimeConfig, RuntimeManager, StorageServer,
    StorageServerPauseController,
};
use std::path::PathBuf;
use std::sync::Arc;
use storage::StorageOptions;
use storage::storage::Storage;

use raft::node::{RaftApp, RaftConfig, create_raft_node};
use raft::raft_proto;
use raft::state_machine::PauseController;

struct PauseControllerWrapper(StorageServerPauseController);

impl PauseController for PauseControllerWrapper {
    fn request_pause(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(self.0.request_pause())
    }

    fn resume(&self) {
        self.0.resume();
    }
}

#[derive(Parser)]
#[command(name = "kiwi-server")]
#[command(about = "A Redis-compatible key-value database built in Rust")]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Args {
    #[arg(short, long)]
    config: Option<String>,

    #[arg(long)]
    single_node: bool,

    #[arg(long)]
    init_cluster: bool,
}

fn main() -> std::io::Result<()> {
    env_logger::init();

    let args = Args::parse();
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

    let runtime_config = RuntimeConfig::default();
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
        storage_handle.spawn(async move {
            info!("Initializing storage server...");
            match initialize_storage_server(storage_receiver, storage_for_server, pause_controller)
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
    let db_path = PathBuf::from(&config.db_path);

    let mut storage = Storage::new(1, 0);

    info!("Opening storage at path: {:?}", db_path);
    let bg_task_receiver = storage
        .open(storage_options, &db_path)
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
) -> Result<(), DualRuntimeError> {
    info!("Initializing storage server...");

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
    if let Some(server) = net::ServerFactory::create_server(
        protocol,
        Some(addr.to_string()),
        runtime_manager,
        config.requirepass.clone(),
    ) {
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!("Redis server error: {}", e);
            }
        });

        if let Some(raft_config) = &config.raft {
            let raft_config = RaftConfig {
                node_id: raft_config.node_id,
                raft_addr: raft_config.raft_addr.clone(),
                resp_addr: raft_config.resp_addr.clone(),
                data_dir: PathBuf::from(&raft_config.data_dir),
                db_path: PathBuf::from(&config.db_path),
                heartbeat_interval: raft_config.heartbeat_interval_ms.unwrap_or(200),
                election_timeout_min: raft_config.election_timeout_min_ms.unwrap_or(500),
                election_timeout_max: raft_config.election_timeout_max_ms.unwrap_or(1500),
                use_memory_log_store: raft_config.use_memory_log_store,
                ..RaftConfig::default()
            };

            let storage_swap = global_storage.arc_swap();
            let pause_controller_wrapper = Arc::new(PauseControllerWrapper(pause_controller));

            let raft_app =
                create_raft_node(raft_config, storage_swap, Some(pause_controller_wrapper))
                    .await
                    .map_err(|e| {
                        std::io::Error::other(format!("Failed to create Raft node: {}", e))
                    })?;

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
        }

        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "Failed to create server for protocol '{}' on address '{}'",
            protocol, addr
        )))
    }
}
