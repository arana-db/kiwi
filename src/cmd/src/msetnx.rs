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
pub struct MsetnxCmd {
    meta: CmdMeta,
}

impl MsetnxCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "msetnx".to_string(),
                arity: -3, // MSETNX key value [key value ...] (at least 1 key-value pair)
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::STRING | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for MsetnxCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// MSETNX key value [key value ...]
    ///
    /// Sets the given keys to their respective values, only if all keys don't exist.
    /// MSETNX is atomic, so either all keys are set or no keys are set.
    ///
    /// # Time Complexity
    /// O(N) where N is the number of keys to set
    ///
    /// # Returns
    /// Integer reply: 1 if the all the keys were set, 0 if no key was set (at least one key already existed)
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();

        // len must be odd (cmd + pairs), and at least 3
        if argv.len() < 3 || argv.len().is_multiple_of(2) {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'msetnx' command"
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

        match storage.msetnx(&kvs) {
            Ok(true) => client.set_reply(RespData::Integer(1)),
            Ok(false) => client.set_reply(RespData::Integer(0)),
            Err(e) => client.set_reply(RespData::Error(format!("ERR {e}").into())),
        }
    }
}
