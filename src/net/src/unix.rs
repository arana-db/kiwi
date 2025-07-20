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

use crate::ServerTrait;
use async_trait::async_trait;
use std::error::Error;

#[allow(dead_code)]
pub struct UnixServer {
    path: String,
}

impl UnixServer {
    pub fn new(path: Option<String>) -> Self {
        let path = path.unwrap_or_else(|| "/tmp/sagedb.sock".to_string());
        Self { path }
    }
}

#[cfg(unix)]
mod unix_impl {
    use super::*;
    use crate::handle::process_connection;
    use crate::{Client, StreamTrait};
    use log::{error, info};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{UnixListener, UnixStream};

    pub struct UnixStreamWrapper {
        stream: UnixStream,
    }

    impl UnixStreamWrapper {
        pub fn new(stream: UnixStream) -> Self {
            Self { stream }
        }
    }

    #[async_trait]
    impl StreamTrait for UnixStreamWrapper {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
            self.stream.read(buf).await
        }
        async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
            self.stream.write(data).await
        }
    }

    #[async_trait]
    impl ServerTrait for UnixServer {
        async fn start(&self) -> Result<(), Box<dyn Error>> {
            if let Err(e) = std::fs::remove_file(&self.path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }

            let listener = UnixListener::bind(&self.path)?;
            info!("Listening on Unix Socket: {}", self.path);

            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        let s = UnixStreamWrapper::new(socket);
                        let mut client = Client::new(Box::new(s));
                        tokio::spawn(async move {
                            if let Err(e) = process_connection(&mut client).await {
                                error!("Connection processing failed: {e:?}");
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {e:?}");
                    }
                }
            }
        }
    }
}

#[cfg(not(unix))]
mod unix_impl {
    use super::*;

    #[async_trait]
    impl ServerTrait for UnixServer {
        async fn start(&self) -> Result<(), Box<dyn Error>> {
            Err("Unix sockets are not supported on this platform".into())
        }
    }
}
