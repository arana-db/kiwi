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
pub struct DelCmd {
    meta: CmdMeta,
}

impl DelCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "del".to_string(),
                arity: -2, // DEL key [key ...]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::KEYSPACE | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for DelCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// DEL key [key ...]
    ///
    /// Removes the specified keys. A key is ignored if it does not exist.
    ///
    /// # Returns
    /// * Integer reply: the number of keys that were removed
    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        // Skip command name, collect all key arguments
        let keys: Vec<Vec<u8>> = argv[1..].to_vec();

        if keys.is_empty() {
            client.set_reply(RespData::Integer(0));
            return;
        }

        match storage.del(&keys) {
            Ok(count) => {
                client.set_reply(RespData::Integer(count));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}
