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

pub type RequirepassProvider = Arc<dyn Fn() -> Option<String> + Send + Sync>;

#[derive(Clone)]
pub struct AuthCmd {
    meta: CmdMeta,
    requirepass_provider: RequirepassProvider,
}

impl Default for AuthCmd {
    fn default() -> Self {
        Self {
            meta: CmdMeta {
                name: "auth".to_string(),
                arity: -2,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION,
                ..Default::default()
            },
            requirepass_provider: Arc::new(|| None),
        }
    }
}

impl AuthCmd {
    pub fn new(provider: RequirepassProvider) -> Self {
        Self {
            meta: CmdMeta {
                name: "auth".to_string(),
                arity: -2,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION,
                ..Default::default()
            },
            requirepass_provider: provider,
        }
    }
}

impl Cmd for AuthCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        let argv = client.argv();
        match argv.len() {
            2 => {
                let password = String::from_utf8_lossy(&argv[1]);
                if let Some(requirepass) = (self.requirepass_provider)() {
                    if password == requirepass {
                        client.set_authenticated(true);
                        client.set_reply(RespData::SimpleString("OK".into()));
                    } else {
                        client.set_reply(RespData::Error(
                            "WRONGPASS invalid username-password pair or user is disabled.".into(),
                        ));
                    }
                } else {
                    client.set_reply(RespData::Error(
                        "ERR AUTH called without any password configured".into(),
                    ));
                }
            }
            3 => {
                // AUTH <user> <pass> — ACL authentication reserved for future
                client.set_reply(RespData::Error(
                    "ERR ACL authentication is not supported".into(),
                ));
            }
            _ => {
                client.set_reply(RespData::Error(
                    "ERR wrong number of arguments for 'auth' command".into(),
                ));
            }
        }
    }
}
