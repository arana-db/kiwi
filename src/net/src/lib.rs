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

mod base_cmd;
pub mod cmd_group_client;
pub mod cmd_kv;
mod cmd_table;
mod constant;
mod error;
pub mod handle;
mod resp;
pub mod tcp;
mod unix;

use crate::resp::RespProtocol;
use crate::tcp::TcpServer;
use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait ServerTrait: Send + Sync + 'static {
    async fn run(&self) -> Result<(), Box<dyn Error>>;
}

#[async_trait]
pub trait StreamTrait: Send + Sync {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error>;
    async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error>;
}

pub struct Client {
    stream: Box<dyn StreamTrait>,
    // TODO: use &[Vec<u8>], need lifetime.
    argv: Vec<Vec<u8>>,
    // Client name.
    name: Vec<u8>,
    cmd_name: Vec<u8>,
    key: Vec<u8>,
    reply: RespProtocol,
}

impl Client {
    pub fn new(stream: Box<dyn StreamTrait>) -> Self {
        Self {
            stream,
            argv: Vec::default(),
            name: Vec::default(),
            cmd_name: Vec::default(),
            key: Vec::default(),
            reply: RespProtocol::new(),
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf).await
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        self.stream.write(data).await
    }

    pub fn set_argv(&mut self, argv: &[Vec<u8>]) {
        self.argv = argv.to_vec()
    }

    pub fn argv(&self) -> &[Vec<u8>] {
        &self.argv
    }

    pub fn set_name(&mut self, name: &[u8]) {
        self.name = name.to_vec()
    }

    pub fn name(&self) -> &[u8] {
        &self.name
    }

    pub fn set_cmd_name(&mut self, name: &[u8]) {
        self.cmd_name = name.to_vec()
    }

    pub fn cmd_name(&self) -> &[u8] {
        &self.cmd_name
    }

    pub fn set_key(&mut self, key: &[u8]) {
        self.key = key.to_vec()
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn reply_mut(&mut self) -> &mut RespProtocol {
        &mut self.reply
    }

    pub fn take_reply(&mut self) -> RespProtocol {
        std::mem::replace(&mut self.reply, RespProtocol::new())
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
