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

use crate::client::Client;
use crate::cmd::table::CmdTable;
use crate::resp::{Protocol, RespProtocol};
use log::error;
use std::sync::Arc;
use storage::storage::Storage;
use tokio::select;

pub async fn process_connection(
    client: &mut Client,
    storage: Arc<Storage>,
    commands: Arc<CmdTable>,
) -> std::io::Result<()> {
    let mut buf = vec![0; 1024];
    let mut resp_parser = RespProtocol::new();

    loop {
        select! {
            result = client.read(&mut buf) => {
                match result {
                    Ok(n) => {
                        if n == 0 { return Ok(()); }

                        match resp_parser.parse(&buf[..n]) {
                            Ok(true) => {
                                let params = resp_parser.take_params();
                                if params.is_empty() { continue; }

                                client.set_cmd_name(&params[0]);
                                client.set_argv(&params);

                                handle_command(client, storage.clone(), commands.clone()).await;

                                // Extract the reply from the connection and send it
                                let response = client.take_reply();
                                match client.write(&response.serialize()).await {
                                    Ok(_) => (),
                                    Err(e) => error!("Write error: {e}"),
                                }
                            }
                            // Other match branches remain unchanged
                            Ok(false) => (),
                            Err(e) => {
                                error!("Protocol error: {e:?}");
                                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Read error: {e:?}");
                        return Err(e);
                    }
                }
            }
        }
    }
}

async fn handle_command(client: &mut Client, storage: Arc<Storage>, cmd_table: Arc<CmdTable>) {
    // Convert the command name from &[u8] to a lowercase String for lookup
    let cmd_name = String::from_utf8_lossy(client.cmd_name()).to_lowercase();

    if let Some(cmd) = cmd_table.get(&cmd_name) {
        // Clone a command object for this specific request
        let mut cmd_clone = cmd.clone_box();

        cmd_clone.execute(client, storage);
    } else {
        // Command not found, set an error reply
        let err_msg = format!("ERR unknown command `{cmd_name}`");
        client.reply_mut().push_bulk_string(err_msg);
    }
}
