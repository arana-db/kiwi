/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use log::info;
use net::ServerFactory;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // init logger
    // set env RUST_LOG=level to control
    env_logger::init();

    let addr = String::from("127.0.0.1:9221");
    let protocol = "tcp";

    info!("tcp listener listen on {addr}");
    if let Some(server) = ServerFactory::create_server(protocol, Option::from(addr)) {
        server.start().await.expect("Failed to start the server. Please check the server configuration and ensure the address is available.");
    } else {
        return Err(std::io::Error::other("server unavailable"));
    }

    Ok(())
}