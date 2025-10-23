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
pub struct MgetCmd {
    meta: CmdMeta,
}

impl MgetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "mget".to_string(),
                arity: -2, // MGET key [key ...] (at least 1 key)
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::STRING | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for MgetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// MGET key [key ...]
    ///
    /// Returns the values of all specified keys. For every key that does not hold
    /// a string value or does not exist, the special value nil is returned.
    ///
    /// # Time Complexity
    /// O(N) where N is the number of keys to retrieve
    ///
    /// # Returns
    /// Array reply: list of values at the specified keys
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        // Use the first key for routing in distributed setup
        // Note: In a true multi-instance distributed environment,
        // the storage layer will handle cross-instance key retrieval
        if argv.len() > 1 {
            let key = argv[1].clone();
            client.set_key(&key);
        }
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        // Collect all keys (skip argv[0] which is the command name)
        let keys: Vec<Vec<u8>> = argv.iter().skip(1).cloned().collect();

        let result = storage.mget(&keys);

        match result {
            Ok(values) => {
                // Convert Option<String> to RespData
                let resp_array: Vec<RespData> = values
                    .into_iter()
                    .map(|opt_val| match opt_val {
                        Some(val) => RespData::BulkString(Some(val.into_bytes().into())),
                        None => RespData::BulkString(None),
                    })
                    .collect();

                client.set_reply(RespData::Array(Some(resp_array)));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}
