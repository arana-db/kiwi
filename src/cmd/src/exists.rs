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
pub struct ExistsCmd {
    meta: CmdMeta,
}

impl ExistsCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "exists".to_string(),
                arity: -2, // EXISTS key [key ...]
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::KEYSPACE | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ExistsCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// EXISTS key [key ...]
    ///
    /// Returns if key exists. Since Redis 3.0.3 it is possible to specify multiple keys instead of a single one.
    /// In such a case, it returns the total number of keys existing among the ones specified as arguments.
    /// Keys mentioned multiple times and existing are counted multiple times.
    ///
    /// # Returns
    /// * Integer reply: the number of keys existing among the ones specified as arguments
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

        match storage.exists(&keys) {
            Ok(count) => {
                client.set_reply(RespData::Integer(count));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}