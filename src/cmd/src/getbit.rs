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
pub struct GetbitCmd {
    meta: CmdMeta,
}

impl GetbitCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "getbit".to_string(),
                arity: 3, // GETBIT key offset
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::STRING | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for GetbitCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// GETBIT key offset
    ///
    /// Returns the bit value at offset in the string value stored at key.
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        if argv.len() != 3 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'getbit' command"
                    .to_string()
                    .into(),
            ));
            return false;
        }

        // Use the key for routing in distributed setup
        let key = argv[1].clone();
        client.set_key(&key);

        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = client.key();

        // Parse offset
        let offset: i64 = match String::from_utf8_lossy(&argv[2]).parse() {
            Ok(offset) => offset,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR bit offset is not an integer or out of range".into(),
                ));
                return;
            }
        };

        match storage.getbit(&key, offset) {
            Ok(bit_value) => {
                client.set_reply(RespData::Integer(bit_value));
            }
            Err(e) => match e {
                storage::error::Error::RedisErr { ref message, .. }
                    if message.starts_with("WRONGTYPE") =>
                {
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
