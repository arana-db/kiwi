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

use log::{info, error};
use net::ServerFactory;
use conf::config::Config;
use std::env;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // init logger
    // set env RUST_LOG=level to control
    env_logger::init();

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        error!("Usage: {} <config_file>", args[0]);
        error!("Example: {} /path/to/kiwi.conf", args[0]);
        return Err(std::io::Error::other("Missing configuration file argument"));
    }
    
    let config_path = &args[1];
    
    // Load configuration from Redis-style config file
    let config = match Config::load(config_path) {
        Ok(config) => {
            info!("Configuration loaded successfully from {}", config_path);
            config
        }
        Err(e) => {
            error!("Failed to load configuration from {}: {}", config_path, e);
            error!("Using default configuration");
            Config::default()
        }
    };

    let addr = config.get_listen_address();
    let protocol = "tcp";

    info!("Kiwi Redis-compatible server starting...");
    info!("Listen address: {}", addr);
    info!("Redis compatible mode: {}", config.redis_compatible_mode);
    info!("Memory limit: {} bytes", config.memory);
    
    if let Some(server) = ServerFactory::create_server(protocol, Option::from(addr)) {
        server.run().await.expect("Failed to start the server. Please check the server configuration and ensure the address is available.");
    } else {
        return Err(std::io::Error::other("server unavailable"));
    }

    Ok(())
}
