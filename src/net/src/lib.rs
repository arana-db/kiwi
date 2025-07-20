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

mod error;
pub mod handle;
mod resp;
mod tcp;
mod unix;

use crate::tcp::TcpServer;
use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait ServerTrait: Send + Sync + 'static {
    async fn start(&self) -> Result<(), Box<dyn Error>>;
}

#[async_trait]
pub trait StreamTrait: Send + Sync {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error>;
    async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error>;
}

pub struct Client {
    stream: Box<dyn StreamTrait>,
}

impl Client {
    pub fn new(stream: Box<dyn StreamTrait>) -> Self {
        Self { stream }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf).await
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        self.stream.write(data).await
    }
}

pub struct ServerFactory;

impl ServerFactory {
    pub fn create_server(protocol: &str, addr: Option<String>) -> Option<Box<dyn ServerTrait>> {
        match protocol.to_lowercase().as_str() {
            "tcp" => Some(Box::new(TcpServer::new(addr))),
            "unix" => Some(Box::new(unix::UnixServer::new(addr))),
            _ => None,
        }
    }
}
