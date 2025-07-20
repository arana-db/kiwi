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

use crate::handle::process_connection;
use crate::{Client, ServerTrait, StreamTrait};
use async_trait::async_trait;
use log::info;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct TcpStreamWrapper {
    stream: TcpStream,
}

impl TcpStreamWrapper {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl StreamTrait for TcpStreamWrapper {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf).await
    }
    async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        self.stream.write(data).await
    }
}

pub struct TcpServer {
    addr: String,
}

impl TcpServer {
    pub fn new(addr: Option<String>) -> Self {
        let addr = addr.unwrap_or_else(|| "127.0.0.1:8080".to_string());
        Self { addr }
    }
}

#[async_trait]
impl ServerTrait for TcpServer {
    async fn start(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.addr).await?;

        info!("Listening on TCP: {}", self.addr);

        loop {
            let (socket, _) = listener.accept().await?;

            let s = TcpStreamWrapper::new(socket);

            let mut client = Client::new(Box::new(s));

            tokio::spawn(async move {
                process_connection(&mut client).await.unwrap();
            });
        }
    }
}
