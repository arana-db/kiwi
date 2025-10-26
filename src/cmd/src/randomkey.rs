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
pub struct RandomkeyCmd {
    meta: CmdMeta,
}

impl RandomkeyCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "randomkey".to_string(),
                arity: 1, // RANDOMKEY
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::KEYSPACE | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for RandomkeyCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// RANDOMKEY
    ///
    /// Return a random key from the currently selected database.
    ///
    /// # Returns
    /// * Bulk string reply: the random key, or nil when the database is empty
    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        match storage.randomkey() {
            Ok(Some(key)) => {
                client.set_reply(RespData::BulkString(Some(key.into_bytes().into())));
            }
            Ok(None) => {
                client.set_reply(RespData::BulkString(None));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}