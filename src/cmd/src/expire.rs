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
pub struct ExpireCmd {
    meta: CmdMeta,
}

impl ExpireCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "expire".to_string(),
                arity: 3, // EXPIRE key seconds
                flags: CmdFlags::WRITE | CmdFlags::FAST,
                acl_category: AclCategory::KEYSPACE | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ExpireCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// EXPIRE key seconds
    ///
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
    /// A key with an associated timeout is often said to be volatile in Redis terminology.
    ///
    /// # Returns
    /// * Integer reply: 1 if the timeout was set, 0 if key does not exist
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = client.key();

        // Parse seconds parameter
        let seconds = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        match storage.expire(&key, seconds) {
            Ok(success) => {
                client.set_reply(RespData::Integer(if success { 1 } else { 0 }));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}