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
use log::{info, warn};
use net::ServerFactory;
use raft::{RaftNode, ClusterConfig};
use std::sync::Arc;

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

    // Determine if we should run in cluster mode
    let cluster_mode = config.cluster.enabled && !args.single_node;
    
    if cluster_mode {
        info!("Starting Kiwi server in cluster mode");
        info!("Node ID: {}", config.cluster.node_id);
        info!("Cluster members: {:?}", config.cluster.cluster_members);
        
        // Validate cluster configuration
        if config.cluster.cluster_members.is_empty() && !args.init_cluster {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cluster mode enabled but no cluster members specified. Use --init-cluster for the first node."
            ));
        }
        
        // Convert config cluster to raft cluster config
        let raft_cluster_config = ClusterConfig {
            enabled: config.cluster.enabled,
            node_id: config.cluster.node_id,
            cluster_members: config.cluster.cluster_members.clone(),
            data_dir: config.cluster.data_dir.clone(),
            heartbeat_interval_ms: config.cluster.heartbeat_interval_ms,
            election_timeout_min_ms: config.cluster.election_timeout_min_ms,
            election_timeout_max_ms: config.cluster.election_timeout_max_ms,
            snapshot_threshold: config.cluster.snapshot_threshold,
            max_payload_entries: config.cluster.max_payload_entries,
        };
        
        // Initialize Raft node
        let raft_node = match RaftNode::new(raft_cluster_config).await {
            Ok(node) => Arc::new(node),
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to initialize Raft node: {}", e)
                ));
            }
        };
        
        // Start Raft node
        if let Err(e) = raft_node.start(args.init_cluster).await {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to start Raft node: {}", e)
            ));
        }
        
        info!("Raft node started successfully");
        
        // Create cluster-aware server
        info!("tcp listener listen on {addr} (cluster mode)");
        if let Some(server) = ServerFactory::create_cluster_server(protocol, Option::from(addr), raft_node) {
            server.run().await.expect("Failed to start the cluster server. Please check the server configuration and ensure the address is available.");
        } else {
            return Err(std::io::Error::other("cluster server unavailable"));
        }
    } else {
        if config.cluster.enabled {
            warn!("Cluster mode disabled by --single-node flag");
        }
        info!("Starting Kiwi server in single-node mode");
        
        info!("tcp listener listen on {addr}");
        if let Some(server) = ServerFactory::create_server(protocol, Option::from(addr)) {
            server.run().await.expect("Failed to start the server. Please check the server configuration and ensure the address is available.");
        } else {
            return Err(std::io::Error::other("server unavailable"));
        }
    }

    Ok(())
}
