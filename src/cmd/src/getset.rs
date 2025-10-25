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
pub struct GetsetCmd {
    meta: CmdMeta,
}

impl GetsetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "getset".to_string(),
                arity: 3, // GETSET key value
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::STRING | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for GetsetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// GETSET key value
    ///
    /// Atomically sets key to value and returns the old value stored at key.
    /// Returns an error when key exists but does not hold a string value.
    ///
    /// # Time Complexity
    /// O(1)
    ///
    /// # Returns
    /// Bulk string reply: the old value stored at key, or nil when key did not exist
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = client.key();
        let value = &argv[2];

        let result = storage.getset(&key, value);

        match result {
            Ok(old_value) => {
                // Return the old value if it existed, or nil if it didn't
                let reply = match old_value {
                    Some(val) => RespData::BulkString(Some(val.into_bytes().into())),
                    None => RespData::BulkString(None),
                };
                client.set_reply(reply);
            }
            Err(e) => match e {
                storage::error::Error::RedisErr { ref message, .. } => {
                    client.set_reply(RespData::Error(message.clone().into()));
                }
                _ => {
                    client.set_reply(RespData::Error(format!("ERR {e}").into()));
                }
            },
        }
    }
}
