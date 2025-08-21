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

use std::sync::Arc;

use async_trait::async_trait;
use resp::RespData;
use tokio::sync::Mutex;

#[async_trait]
pub trait StreamTrait: Send + Sync {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error>;
    async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error>;
}

pub struct Client {
    // using tokio::sync::Mutex may has risks to pass this Client object across
    // tokio runtimes. we may require to figure out how to make a refactor to
    // avoid passing Client across runtimes.
    stream: Mutex<Box<dyn StreamTrait>>,
    ctx: parking_lot::Mutex<ClientContext>,
}

struct ClientContext {
    // TODO: use &[Vec<u8>], need lifetime.
    argv: Vec<Vec<u8>>,
    // Client name.
    name: Arc<Vec<u8>>,
    cmd_name: Arc<Vec<u8>>,
    key: Vec<u8>,
    reply: RespData,
}

impl Client {
    pub fn new(stream: Box<dyn StreamTrait>) -> Self {
        Self {
            stream: Mutex::new(stream),
            ctx: parking_lot::Mutex::new(ClientContext {
                argv: Vec::default(),
                name: Arc::new(Vec::default()),
                cmd_name: Arc::new(Vec::default()),
                key: Vec::default(),
                reply: RespData::default(),
            }),
        }
    }

    pub async fn read(&self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let mut stream = self.stream.lock().await;
        stream.read(buf).await
    }

    pub async fn write(&self, data: &[u8]) -> Result<usize, std::io::Error> {
        let mut stream = self.stream.lock().await;
        stream.write(data).await
    }

    pub fn set_argv(&self, argv: &[Vec<u8>]) {
        let mut ctx = self.ctx.lock();
        ctx.argv = argv.to_vec();
    }

    pub fn argv(&self) -> Vec<Vec<u8>> {
        let ctx = self.ctx.lock();
        ctx.argv.clone()
    }

    pub fn set_name(&self, name: &[u8]) {
        let mut ctx = self.ctx.lock();
        ctx.name = Arc::new(name.to_vec());
    }

    pub fn name(&self) -> Arc<Vec<u8>> {
        let ctx = self.ctx.lock();
        ctx.name.clone()
    }

    pub fn set_cmd_name(&self, name: &[u8]) {
        let mut ctx = self.ctx.lock();
        ctx.cmd_name = Arc::new(name.to_vec());
    }

    pub fn cmd_name(&self) -> Arc<Vec<u8>> {
        let ctx = self.ctx.lock();
        ctx.cmd_name.clone()
    }

    pub fn set_key(&self, key: &[u8]) {
        let mut ctx = self.ctx.lock();
        ctx.key = key.to_vec();
    }

    pub fn key(&self) -> Vec<u8> {
        let ctx = self.ctx.lock();
        ctx.key.clone()
    }

    pub fn set_reply(&self, reply: RespData) {
        let mut ctx = self.ctx.lock();
        ctx.reply = reply;
    }

    pub fn take_reply(&self) -> RespData {
        let mut ctx = self.ctx.lock();
        std::mem::take(&mut ctx.reply)
    }
}
