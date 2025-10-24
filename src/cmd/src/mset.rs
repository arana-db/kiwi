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
pub struct MsetCmd {
    meta: CmdMeta,
}

impl MsetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "mset".to_string(),
                arity: -3, // MSET key value [key value ...] (at least 1 key-value pair)
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::STRING | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for MsetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// MSET key value [key value ...]
    ///
    /// Sets the given keys to their respective values.
    /// MSET replaces existing values with new values, just like regular SET.
    /// MSET is atomic, so all given keys are set at once. It is not possible
    /// for clients to see that some of the keys were updated while others are unchanged.
    ///
    /// # Time Complexity
    /// O(N) where N is the number of keys to set
    ///
    /// # Returns
    /// Simple string reply: always OK since MSET can't fail
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();

        // Check if the number of arguments is valid (must be odd: command + key-value pairs)
        // MSET key1 value1 key2 value2 ... means argv.len() must be odd (>= 3)
        if argv.len() < 3 || argv.len().is_multiple_of(2) {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'mset' command"
                    .to_string()
                    .into(),
            ));
            return false;
        }

        // Use the first key for routing in distributed setup
        if argv.len() > 1 {
            let key = argv[1].clone();
            client.set_key(&key);
        }

        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        // Collect all key-value pairs using chunks_exact for cleaner code
        // Skip argv[0] which is the command name
        let kvs: Vec<(Vec<u8>, Vec<u8>)> = argv[1..]
            .chunks_exact(2)
            .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
            .collect();

        match storage.mset(&kvs) {
            Ok(_) => client.set_reply(RespData::SimpleString("OK".to_string().into())),
            Err(e) => client.set_reply(RespData::Error(format!("ERR {e}").into())),
        }
    }
}
