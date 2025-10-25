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

/// BITOP operation destkey key [key ...]
///
/// Perform bitwise operations between strings.
#[derive(Clone, Default)]
pub struct BitopCmd {
    meta: CmdMeta,
}

impl BitopCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "bitop".to_string(),
                arity: -4, // BITOP operation destkey key [key ...]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::STRING | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for BitopCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();

        // Check if the number of arguments is valid
        if argv.len() < 4 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'bitop' command"
                    .to_string()
                    .into(),
            ));
            return false;
        }

        // Use the destination key for routing in distributed setup
        let dest_key = argv[2].clone();
        client.set_key(&dest_key);

        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        // Validate minimum number of arguments
        if argv.len() < 4 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'bitop' command"
                    .to_string()
                    .into(),
            ));
            return;
        }

        // Parse operation
        let operation = String::from_utf8_lossy(&argv[1]).to_string();

        // Parse destination key
        let dest_key = client.key();

        // Parse source keys
        let src_keys: Vec<&[u8]> = argv[3..].iter().map(|key| key.as_slice()).collect();

        match storage.bitop(&operation, &dest_key, &src_keys) {
            Ok(size) => {
                client.set_reply(RespData::Integer(size));
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
