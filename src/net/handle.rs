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

use log::error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn process_connection(mut socket: TcpStream) -> std::io::Result<()> {
    // TODO: add handle command logic

    let mut buffer = [0; 1024];

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
