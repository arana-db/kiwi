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
use log::{info, warn, error};
use net::ServerFactory;
// use raft::{RaftNode, ClusterConfig}; // TODO: Re-enable when Raft module is fixed
// use std::sync::Arc; // TODO: Re-enable when Raft module is fixed

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

#[tokio::main]
async fn main() -> std::io::Result<()> {
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
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                e
            ));
        }

        // Check if cluster configuration is valid
        if !cluster_info.is_valid_cluster {
            error!("Invalid cluster configuration detected");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid cluster configuration. Please check node ID and cluster members."
            ));
        }
        
        // TODO: Re-enable Raft initialization when module is fixed
        /*
        // Create Raft cluster configuration from the validated config
        let raft_cluster_config = config.cluster.clone();
        
        // Initialize Raft node
        info!("Initializing Raft node with configuration: {:?}", raft_cluster_config);
        let raft_node = match RaftNode::new(raft_cluster_config).await {
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
        */
        
        // info!("Raft node started successfully");
        
        // Cluster mode temporarily disabled - falling back to single-node mode
        warn!("Cluster mode temporarily disabled due to Raft module compilation issues");
        warn!("Falling back to single-node mode");
        info!("Starting server in single-node mode on {}", addr);
        
        match start_server(protocol, &addr).await {
            Ok(_) => info!("Server started successfully"),
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
        
        match start_server(protocol, &addr).await {
            Ok(_) => info!("Server started successfully"),
            Err(e) => {
                error!("Failed to start server: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Start the server with proper error handling
async fn start_server(protocol: &str, addr: &str) -> std::io::Result<()> {
    if let Some(server) = ServerFactory::create_server(protocol, Some(addr.to_string())) {
        server.run().await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to start the server on {}: {}. Please check the server configuration and ensure the address is available.", addr, e)
            )
        })
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to create server for protocol '{}' on address '{}'", protocol, addr)
        ))
    }
}
