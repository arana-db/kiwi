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

use kiwi_rs::net;
use log::{error, info};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // init logger
    env_logger::init();

    let addr = "127.0.0.1:9221";
    let listener = TcpListener::bind(addr).await?;

    info!("tcp listener listen on {addr}");
    loop {
        let (socket, addr) = listener.accept().await?;
        info!("new connection: {addr}");

        tokio::spawn(async move {
            if let Err(e) = net::handle::process_connection(socket).await {
                error!("handle connection error: {e}");
            }
        });
    }
}
