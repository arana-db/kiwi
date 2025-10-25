// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct PsetexCmd {
    meta: CmdMeta,
}

impl PsetexCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "psetex".to_string(),
                arity: 4, // PSETEX key milliseconds value
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::STRING | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for PsetexCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// PSETEX key milliseconds value
    ///
    /// Set key to hold string value and set key to timeout after a given number of milliseconds.
    /// This command is atomic. It is exactly equivalent to executing the following commands:
    /// SET key value
    /// PEXPIRE key milliseconds
    ///
    /// # Time Complexity
    /// O(1)
    ///
    /// # Returns
    /// Simple string reply: OK if SET was executed correctly
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = client.key();

        // Parse milliseconds parameter
        let milliseconds = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        // Validate milliseconds - must be positive
        if milliseconds <= 0 {
            client.set_reply(RespData::Error("ERR invalid expire time in psetex".into()));
            return;
        }

        // Check TTL upper limit to prevent overflow
        if milliseconds > i64::MAX / 1_000 {
            client.set_reply(RespData::Error("ERR invalid expire time in psetex".into()));
            return;
        }

        let value = &argv[3];

        let result = storage.psetex(&key, milliseconds, value);

        match result {
            Ok(()) => {
                client.set_reply(RespData::SimpleString("OK".into()));
            }
            Err(e) => match e {
                storage::error::Error::RedisErr { ref message, .. } => {
                    // RedisErr already contains the formatted message
                    client.set_reply(RespData::Error(message.clone().into()));
                }
                _ => {
                    client.set_reply(RespData::Error(format!("ERR {e}").into()));
                }
            },
        }
    }
}
