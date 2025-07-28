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
use crate::{Connection, ServerTrait, StreamTrait};
use async_trait::async_trait;
use log::info;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use storage::options::StorageOptions;
use storage::storage::Storage;
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
    storage: Arc<Storage>,
}

impl TcpServer {
    /// addr is 127.0.0.1:9221
    pub fn new(addr: Option<String>) -> Self {
        let storage_options = Arc::new(StorageOptions::default());
        let db_path = PathBuf::from("./db");
        let mut storage = Storage::new(1, 0);
        // Note: Storage::open returns a receiver, and should be called after construction, not in new.
        // The caller should call storage.open(storage_options, db_path) and spawn the bg_task_worker as needed.
        storage.open(storage_options, db_path).unwrap();
        Self {
            addr: addr.unwrap_or("127.0.0.1:9221".to_string()),
            storage: Arc::new(storage),
        }
    }
}

#[async_trait]
impl ServerTrait for TcpServer {
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.addr).await?;

        info!("Listening on TCP: {}", self.addr);

        loop {
            let (socket, _) = listener.accept().await?;

            let s = TcpStreamWrapper::new(socket);

            let mut connection = Connection::new(Box::new(s));

            let storage = self.storage.clone();

            tokio::spawn(async move {
                process_connection(&mut connection, storage).await.unwrap();
            });
        }
    }
}
