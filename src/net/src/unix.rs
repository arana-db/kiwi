// Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Unix-domain socket support for Kiwi-rs.
//!
//! This module is **only** compiled on Unix platforms. Windows builds will
//! simply skip this file, ensuring cross-platform compilation without the
//! need for extra Cargo features.

#![cfg(unix)]
#![cfg_attr(not(unix), allow(unused_imports, dead_code))]

use log::error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

/// Start a Unix-domain socket server at the supplied `path`.
///
/// This is a very small echo implementation, mirroring the behaviour of
/// the TCP handler in `handle.rs`. Production code will most likely share
/// common request-processing logic between the TCP and Unix handlers.
#[allow(dead_code)]
#[cfg(unix)]
pub async fn serve(path: &str) -> std::io::Result<()> {
    // Remove any existing socket at the path to avoid bind errors.
    let _ = std::fs::remove_file(path);

    let listener = UnixListener::bind(path)?;

    loop {
        let (socket, _addr) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = process_connection(socket).await {
                error!("unix socket error: {e}");
            }
        });
    }
}

#[cfg(unix)]
async fn process_connection(mut socket: UnixStream) -> std::io::Result<()> {
    let mut buffer = [0u8; 1024];

    loop {
        let n = match socket.read(&mut buffer).await {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) => {
                error!("socket read error: {e}");
                return Err(e);
            }
        };

        if let Err(e) = socket.write_all(&buffer[..n]).await {
            error!("socket write error: {e}");
            return Err(e);
        }
    }
} 