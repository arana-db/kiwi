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
use log::{error, info, warn};
// ServerFactory is used via net::ServerFactory::create_server_with_mode
use runtime::{DualRuntimeError, RuntimeConfig, RuntimeManager, StorageServer};
use std::sync::Arc;
use storage::storage::Storage;

/// Convert conf::config::ClusterConfig to raft::types::ClusterConfig
/// 
/// This provides a unified entry point for cluster configuration conversion,
/// following the pattern used in kiwi-cpp where configuration is mapped
/// at the server initialization layer.
fn convert_cluster_config(config: &conf::config::ClusterConfig) -> raft::types::ClusterConfig {
    raft::types::ClusterConfig {
        enabled: config.enabled,
        node_id: config.node_id,
        cluster_members: config.cluster_members.clone(),
        data_dir: config.data_dir.clone(),
        heartbeat_interval_ms: config.heartbeat_interval_ms,
        election_timeout_min_ms: config.election_timeout_min_ms,
        election_timeout_max_ms: config.election_timeout_max_ms,
        snapshot_threshold: config.snapshot_threshold,
        max_payload_entries: config.max_payload_entries,
    }
}

/// Kiwi - A Redis-compatible key-value database built in Rust
#[derive(Parser)]
#[command(name = "kiwi-server")]
#[command(about = "A Redis-compatible key-value database built in Rust")]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Args {
    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,

    /// Force single-node mode (disable cluster even if configured)
    #[arg(long)]
    single_node: bool,

    /// Initialize a new cluster (only for the first node)
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
    let mut runtime_manager = RuntimeManager::new(runtime_config).map_err(|e| {
        std::io::Error::other(
            format!("Failed to create RuntimeManager: {}", e),
        )
    })?;

    // Start the RuntimeManager first
    // We need to use a basic runtime to start the RuntimeManager
    let basic_rt = tokio::runtime::Runtime::new().map_err(|e| {
        std::io::Error::other(
            format!("Failed to create basic runtime: {}", e),
        )
    })?;

    basic_rt.block_on(async {
        if let Err(e) = runtime_manager.start().await {
            error!("Failed to start RuntimeManager: {}", e);
            return Err(std::io::Error::other(
                format!("Failed to start RuntimeManager: {}", e),
            ));
        }
        Ok(())
    })?;

    info!("RuntimeManager started successfully");

    // Get runtime handles after starting
    let network_handle = runtime_manager.network_handle().map_err(|e| {
        std::io::Error::other(
            format!("Failed to get network handle: {}", e),
        )
    })?;
    let storage_handle = runtime_manager.storage_handle().map_err(|e| {
        std::io::Error::other(
            format!("Failed to get storage handle: {}", e),
        )
    })?;

    // Initialize storage components and get the receiver for the storage server
    let storage_receiver = runtime_manager
        .initialize_storage_components()
        .map_err(|e| {
            std::io::Error::other(
                format!("Failed to initialize storage components: {}", e),
            )
        })?;

    info!("Storage components initialized, starting storage server...");

    // Initialize storage server in storage runtime
    let storage_server_result = storage_handle.spawn(async move {
        initialize_storage_server(storage_receiver).await
    });

    // Use the network runtime to run the main server logic
    let result = network_handle.block_on(async {
        // Wait for storage server initialization
        match storage_server_result.await {
            Ok(Ok(_)) => info!("Storage server initialized successfully"),
            Ok(Err(e)) => {
                error!("Failed to initialize storage server: {}", e);
                return Err(std::io::Error::other(
                    format!("Failed to initialize storage server: {}", e),
                ));
            }
            Err(e) => {
                error!("Storage server initialization task failed: {}", e);
                return Err(std::io::Error::other(
                    format!("Storage server initialization task failed: {}", e),
                ));
            }
        }

        // Determine if we should run in cluster mode using the new method
        let cluster_mode = config.should_run_cluster_mode(args.single_node);

        if cluster_mode {
            info!("Starting Kiwi server in cluster mode");

            // Get cluster information
            let cluster_info = config.get_cluster_info();
            info!("Node ID: {}", cluster_info.node_id);
            info!("Cluster members: {:?}", config.cluster.cluster_members);
            info!("Self endpoint: {:?}", cluster_info.self_endpoint);
            info!("Peer endpoints: {:?}", cluster_info.peer_endpoints);

            // Validate cluster configuration for startup
            if let Err(e) = config.validate_cluster_startup(args.init_cluster) {
                error!("Cluster configuration validation failed: {}", e);
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e));
            }

            // Check if cluster configuration is valid
            if !cluster_info.is_valid_cluster {
                error!("Invalid cluster configuration detected");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid cluster configuration. Please check node ID and cluster members.",
                ));
            }

            // Create Raft cluster configuration from the validated config
            let raft_cluster_config = convert_cluster_config(&config.cluster);

            // Initialize Raft node
            info!("Initializing Raft node with configuration: {:?}", raft_cluster_config);
            let raft_node = match raft::RaftNode::new(raft_cluster_config).await {
                Ok(node) => {
                    info!("Raft node initialized successfully");
                    Arc::new(node)
                },
                Err(e) => {
                    error!("Failed to initialize Raft node: {}", e);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to initialize Raft node: {}", e)
                    ));
                }
            };

            // Start Raft node
            info!("Starting Raft node (init_cluster: {})", args.init_cluster);
            if let Err(e) = raft_node.start(args.init_cluster).await {
                error!("Failed to start Raft node: {}", e);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to start Raft node: {}", e)
                ));
            }

            info!("Raft node started successfully");

            // Start server in cluster mode
            match start_server_with_mode(protocol, &addr, &mut runtime_manager, true).await {
                Ok(_) => info!("Server started successfully in cluster mode"),
                Err(e) => {
                    error!("Failed to start server: {}", e);
                    return Err(e);
                }
            }
        } else {
            if config.cluster.enabled {
                warn!("Cluster mode disabled by --single-node flag");
            }
            info!("Starting Kiwi server in single-node mode on {}", addr);

            match start_server_with_mode(protocol, &addr, &mut runtime_manager, false).await {
                Ok(_) => info!("Server started successfully"),
                Err(e) => {
                    error!("Failed to start server: {}", e);
                    return Err(e);
                }
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

    // Create storage instance
    let storage = Arc::new(Storage::new(1, 0)); // Single instance, db_id 0

    // Create and start storage server with the receiver
    let storage_server = StorageServer::new(storage, request_receiver);

    info!("Storage server created, starting processing...");

    // Start the storage server (this will run indefinitely)
    storage_server.run().await?;

    Ok(())
}

/// Start the server with proper error handling and cluster mode support
///
/// # Requirements
/// - Requirement 6.1: Network layer SHALL support mode switching
async fn start_server_with_mode(
    protocol: &str,
    addr: &str,
    runtime_manager: &mut RuntimeManager,
    cluster_mode: bool,
) -> std::io::Result<()> {
    let mode = if cluster_mode {
        net::raft_network_handle::ClusterMode::Cluster
    } else {
        net::raft_network_handle::ClusterMode::Single
    };

    info!("Starting server in {:?} mode", mode);

    if let Some(server) =
        net::ServerFactory::create_server_with_mode(
            protocol,
            Some(addr.to_string()),
            runtime_manager,
            mode,
        )
    {
        server.run().await.map_err(|e| {
            std::io::Error::other(format!("Failed to start the server on {}: {}. Please check the server configuration and ensure the address is available.", addr, e))
        })
    } else {
        Err(std::io::Error::other(format!(
            "Failed to create server for protocol '{}' on address '{}'",
            protocol, addr
        )))
    }
}
