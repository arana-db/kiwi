//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#[cfg(unix)]
use crate::handle::process_connection;
use crate::ServerTrait;
#[cfg(unix)]
use crate::{Client, StreamTrait};
use async_trait::async_trait;
#[cfg(unix)]
use log::info;
use std::error::Error;
#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

#[cfg(unix)]
pub struct UnixStreamWrapper {
    stream: UnixStream,
}

#[cfg(unix)]
impl UnixStreamWrapper {
    pub fn new(stream: UnixStream) -> Self {
        Self { stream }
    }
}

#[cfg(unix)]
#[async_trait]
impl StreamTrait for UnixStreamWrapper {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf).await
    }
    async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        self.stream.write(data).await
    }
}

pub struct UnixServer {
    #[cfg(unix)]
    path: String,
    #[cfg(not(unix))]
    _path: String, 
}

impl UnixServer {
    pub fn new(path: Option<String>) -> Self {
        let path = path.unwrap_or_else(|| "/tmp/sagedb.sock".to_string());
        #[cfg(unix)]
        {
            Self { path }
        }
        #[cfg(not(unix))]
        {
            Self { _path: path }
        }
    }
}

#[cfg(unix)]
#[async_trait]
impl ServerTrait for UnixServer {
    async fn start(&self) -> Result<(), Box<dyn Error>> {
        let _ = std::fs::remove_file(&self.path);
        let listener = UnixListener::bind(&self.path)?;
        info!("Listening on Unix Socket: {}", self.path);

        loop {
            let (socket, _) = listener.accept().await?;

            let s = UnixStreamWrapper::new(socket);

            let mut client = Client::new(Box::new(s));

            tokio::spawn(async move { process_connection(&mut client).await.unwrap() });
        }
    }
}

#[cfg(not(unix))]
#[async_trait]
impl ServerTrait for UnixServer {
    async fn start(&self) -> Result<(), Box<dyn Error>> {
        Err("Unix sockets are not supported on this platform".into())
    }
}
