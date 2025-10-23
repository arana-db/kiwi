// Copyright (c) 2024-present, arana-db Community. All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
                arity: -3, // MSET key value [key value ...] (at least one pair)
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
    /// Sets the given keys to their respective values. MSET replaces existing values
    /// with new values, just like regular SET. MSET is atomic, so all given keys are
    /// set at once. It is not possible for clients to see that some of the keys were
    /// updated while others are unchanged.
    ///
    /// # Time Complexity
    /// O(N) where N is the number of keys to set
    ///
    /// # Returns
    /// Simple string reply: always OK since MSET can't fail
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();

        if (argv.len() - 1) % 2 != 0 {
            client.set_reply(
                RespData::Error(
                    "ERR wrong number of arguments for 'mset' command"
                        .to_string()
                        .into(),
                ),
            );
            return false;
        }

        if argv.len() > 1 {
            let key = argv[1].clone();
            client.set_key(&key);
        }

        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        let mut kvs = Vec::with_capacity((argv.len().saturating_sub(1)) / 2);
        let mut iter = argv.iter().skip(1);

        while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            kvs.push((key.clone(), value.clone()));
        }

        match storage.mset(&kvs) {
            Ok(()) => client.set_reply(RespData::SimpleString("OK".to_string().into())),
            Err(e) => client.set_reply(RespData::Error(format!("ERR {e}").into())),
        }
    }
}
