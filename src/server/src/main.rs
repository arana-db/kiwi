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

    info!("Storage components initialized, starting storage server...");

    // Initialize storage server in storage runtime (runs in background)

    storage_handle.spawn(async move {
        info!("Initializing storage server...");
        match initialize_storage_server(storage_receiver).await {
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

    // Use the network runtime to run the main server logic
    let result = network_handle.block_on(async {
        info!("Storage server started in background");

        // NOTE: Raft/cluster mode has been removed from this repo.
        // We always run in single-node mode.
        info!("Starting Kiwi server in single-node mode on {}", addr);

        match start_server(protocol, &addr, &mut runtime_manager).await {
            Ok(_) => info!("Server started successfully"),
            Err(e) => {
                error!("Failed to start server: {}", e);
                return Err(e);
            }
        }

        Ok(())
    });

    // Shutdown the runtime manager
    if let Err(e) = network_handle.block_on(runtime_manager.stop()) {
        warn!("Error during RuntimeManager shutdown: {}", e);
    }

    result
}

/// Initialize the storage server in the storage runtime
async fn initialize_storage_server(
    request_receiver: tokio::sync::mpsc::Receiver<runtime::StorageRequest>,
) -> Result<(), DualRuntimeError> {
    info!("Initializing storage server...");

    // Create storage options and path
    let storage_options = Arc::new(StorageOptions::default());
    let db_path = PathBuf::from("./db");

    // Create storage instance (not yet opened)
    let mut storage = Storage::new(1, 0); // Single instance, db_id 0

    // Open storage to initialize actual RocksDB instances
    info!("Opening storage at path: {:?}", db_path);
    let bg_task_receiver = storage
        .open(storage_options, &db_path)
        .map_err(|e| DualRuntimeError::storage_runtime(format!("Failed to open storage: {}", e)))?;
    info!("Storage opened successfully");

    // Start background task handler for storage maintenance
    tokio::spawn(async move {
        let mut receiver = bg_task_receiver;
        while let Some(_task) = receiver.recv().await {
            debug!("Processing background task");
            // Background tasks are handled by Storage internally
        }
        info!("Background task receiver closed");
    });

    // Wrap storage in Arc for sharing
    let storage = Arc::new(storage);

    // Create and start storage server with the receiver
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
) -> std::io::Result<()> {
    if let Some(server) =
        net::ServerFactory::create_server(protocol, Some(addr.to_string()), runtime_manager)
    {
        server.run().await.map_err(|e| {
            std::io::Error::other(format!(
                "Failed to start the server on {}: {}. Please check the server configuration and ensure the address is available.",
                addr, e
            ))
        })
    } else {
        Err(std::io::Error::other(format!(
            "Failed to create server for protocol '{}' on address '{}'",
            protocol, addr
        )))
    }
}
