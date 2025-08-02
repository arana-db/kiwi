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

use crate::resp::{Protocol, RespProtocol};
use crate::Client;
use log::{error, info};
use std::sync::Arc;
use storage::storage::Storage;
use tokio::select;

/// Processes an incoming TCP connection.
///
/// This function reads data from the provided `TcpStream`, processes it, and writes the response back to the stream.
/// It operates in a loop until the connection is closed or an error occurs.
///
/// # Arguments
///
/// * `socket` - A `TcpStream` representing the connection to process.
///
/// # Returns
///
/// A `std::io::Result` indicating success or failure.
///
pub async fn process_connection(socket: &mut Client, storage: Arc<Storage>) -> std::io::Result<()> {
    let mut buf = vec![0; 1024];

    let mut prot = RespProtocol::new();

    loop {
        select! {
            result = socket.read(&mut buf) => {
                match result {
                    Ok(n) => {
                        if n == 0 {
                            return Ok(());
                        }

                        match prot.parse(&buf[..n]) {
                            Ok(true) => {
                                let args = prot.take_args();
                                let response = handle_command(&args, storage.clone()).await;
                                match socket.write(&response.serialize()).await {
                                    Ok(_) => (),
                                    Err(e) => error!("Write error: {e}"),
                                }
                            }
                            Ok(false) => (),  // Data is incomplete, continue to read in a loop
                            Err(e) => {  // Protocol error
                                error!("Protocol error: {e:?}");
                                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Protocol error: {e:?}");
                        return Err(e);
                    }
                }
            }
        }
    }
}

async fn handle_command(args: &Vec<Vec<u8>>, storage: Arc<Storage>) -> RespProtocol {
    let mut resp = RespProtocol::new();
    if args.is_empty() {
        resp.push_bulk_string("Empty command".to_string());
        return resp;
    }
    info!("handle_command: {args:?}");

    match args[0].as_slice() {
        b"set" if args.len() == 3 => {
            let key = &args[1];
            let value = &args[2];
            match storage.set(key, value) {
                Ok(_) => resp.push_bulk_string("OK".to_string()),
                Err(e) => resp.push_bulk_string(format!("ERR: {e}")),
            }
        }
        b"get" if args.len() == 2 => {
            let key = &args[1];
            match storage.get(key) {
                Ok(val) => resp.push_bulk_string(val),
                Err(e) => resp.push_bulk_string(format!("ERR: {e}")),
            }
        }
        b"ping" => {
            resp.push_bulk_string("PONG".to_string());
        }
        _ => {
            resp.push_bulk_string("Unknown or invalid command".to_string());
        }
    }

    resp
}
