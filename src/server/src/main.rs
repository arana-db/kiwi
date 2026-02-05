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
// ServerFactory is used via net::ServerFactory::create_server
use runtime::{DualRuntimeError, RuntimeConfig, RuntimeManager, StorageServer};
use std::path::PathBuf;
use std::sync::Arc;
use storage::StorageOptions;
use storage::storage::Storage;

use raft::node::{RaftConfig, create_raft_node, RaftApp};

/// Kiwi - A Redis-compatible key-value database built in Rust
#[derive(Parser)]
#[command(name = "kiwi-server")]
#[command(about = "A Redis-compatible key-value database built in Rust")]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Args {
    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,

    /// Force single-node mode (no-op; cluster mode is removed)
    #[arg(long)]
    single_node: bool,

    /// Initialize a new cluster (no-op; cluster mode is removed)
    #[arg(long)]
    init_cluster: bool,
}

fn main() -> std::io::Result<()> {
    // init logger
    // set env RUST_LOG=level to control
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

    // Create runtime configuration based on system capabilities
    let runtime_config = RuntimeConfig::default();
    info!(
        "Creating RuntimeManager with {} network threads and {} storage threads",
        runtime_config.network_threads, runtime_config.storage_threads
    );

    // Create and start RuntimeManager
    let mut runtime_manager = RuntimeManager::new(runtime_config)
        .map_err(|e| std::io::Error::other(format!("Failed to create RuntimeManager: {}", e)))?;

    // Start the RuntimeManager first
    // We need to use a basic runtime to start the RuntimeManager
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

    // Get runtime handles after starting
    let network_handle = runtime_manager
        .network_handle()
        .map_err(|e| std::io::Error::other(format!("Failed to get network handle: {}", e)))?;
    let storage_handle = runtime_manager
        .storage_handle()
        .map_err(|e| std::io::Error::other(format!("Failed to get storage handle: {}", e)))?;

    // Initialize storage components and get the receiver for the storage server
    let storage_receiver = runtime_manager
        .initialize_storage_components()
        .map_err(|e| {
            std::io::Error::other(format!("Failed to initialize storage components: {}", e))
        })?;

    // Use the network runtime to run the main server logic
    let result = network_handle.block_on(async {
        let storage = initialize_storage()
            .await
            .map_err(|e| std::io::Error::other(format!("Failed to initialize storage: {}", e)))?;

        info!("Storage components initialized, starting storage server...");

        // Initialize storage server in storage runtime (runs in background)

        let storage_for_server = storage.clone();
        storage_handle.spawn(async move {
            info!("Initializing storage server...");
            match initialize_storage_server(storage_receiver, storage_for_server).await {
                Ok(_) => {
                    error!("Storage server exited unexpectedly - this should never happen!");
                }
                Err(e) => {
                    error!("Storage server failed: {}", e);
                }
            }
        });

        // Store the handle to monitor the task (optional)
        // You could add this to RuntimeManager to track the storage server task

        // Give storage server a moment to initialize
        std::thread::sleep(std::time::Duration::from_millis(100));

        info!("Storage server started in background");

        // NOTE: Raft/cluster mode has been removed from this repo.
        // We always run in single-node mode.
        info!("Starting Kiwi server in single-node mode on {}", addr);

        match start_server(protocol, &addr, &mut runtime_manager, &storage, &config).await {
            Ok(_) => info!("Server started successfully"),
            Err(e) => {
                error!("Failed to start server: {}", e);
                return Err(e);
            }
        }

        // Block until Ctrl+C so the process does not exit immediately
        info!("Press Ctrl+C to stop.");
        tokio::signal::ctrl_c()
            .await
            .map_err(|e| std::io::Error::other(format!("Failed to listen for shutdown signal: {}", e)))?;
        info!("Received shutdown signal, stopping...");

        Ok(())
    });

    // Shutdown the runtime manager
    if let Err(e) = network_handle.block_on(runtime_manager.stop()) {
        warn!("Error during RuntimeManager shutdown: {}", e);
    }

    result
}

async fn initialize_storage() -> Result<Arc<Storage>, DualRuntimeError> {
    info!("Initializing storage...");

    let storage_options = Arc::new(StorageOptions::default());
    let db_path = PathBuf::from("./db");

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

    Ok(Arc::new(storage))
}

/// Initialize the storage server in the storage runtime.
/// Uses the already-opened storage from `initialize_storage()` to avoid opening RocksDB twice.
async fn initialize_storage_server(
    request_receiver: tokio::sync::mpsc::Receiver<runtime::StorageRequest>,
    storage: Arc<Storage>,
) -> Result<(), DualRuntimeError> {
    info!("Initializing storage server...");

    // Create and start storage server with the shared storage (already opened in initialize_storage)
    let storage_server = StorageServer::new(storage, request_receiver);

    info!("Storage server created, starting processing...");

    // Start the storage server (this will run indefinitely)
    storage_server.run().await?;

    Ok(())
}

async fn start_server(
    protocol: &str,
    addr: &str,
    runtime_manager: &mut RuntimeManager,
    storage: &Arc<Storage>,
    config: &Config,
) -> std::io::Result<()> {
    if let Some(server) =
        net::ServerFactory::create_server(protocol, Some(addr.to_string()), runtime_manager)
    {
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
                ..Default::default()
            };

            let raft_app = create_raft_node(raft_config, storage.clone())
                .await
                .map_err(|e| std::io::Error::other(format!("Failed to create Raft node: {}", e)))?;

            let raft_addr = raft_app.raft_addr.clone();
            let grpc_addr = raft_addr.parse::<std::net::SocketAddr>().map_err(|e| {
                std::io::Error::other(format!("Invalid Raft address '{}': {}", raft_addr, e))
            })?;

            // 创建所有 gRPC 服务
            let (core_svc, admin_svc, client_svc, metrics_svc) = RaftApp::create_grpc_services(raft_app.clone());

            info!("Starting Raft gRPC server on {}", raft_addr);

            // 启动 gRPC 服务器
            tokio::spawn(async move {
                use tonic::transport::Server;
                Server::builder()
                    .add_service(core_svc)
                    .add_service(admin_svc)
                    .add_service(client_svc)
                    .add_service(metrics_svc)
                    .serve(grpc_addr)
                    .await
                    .unwrap();
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
