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

pub mod handle;
pub mod tcp;

// TODO: delete this module
pub mod error;
pub mod unix;

use crate::tcp::TcpServer;
use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait ServerTrait: Send + Sync + 'static {
    async fn run(&self) -> Result<(), Box<dyn Error>>;
}

pub struct ServerFactory;

impl ServerFactory {
    pub fn create_server(protocol: &str, addr: Option<String>) -> Option<Box<dyn ServerTrait>> {
        match protocol.to_lowercase().as_str() {
            "tcp" => Some(Box::new(TcpServer::new(addr))),
            #[cfg(unix)]
            "unix" => Some(Box::new(unix::UnixServer::new(addr))),
            #[cfg(not(unix))]
            "unix" => None,
            _ => None,
        }
    }
}
