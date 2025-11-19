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
pub struct ZcardCmd {
    meta: CmdMeta,
}

impl ZcardCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "zcard".to_string(),
                arity: 2, // ZCARD key
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::READ | AclCategory::SORTEDSET,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ZcardCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();

        let key = argv[1].clone();
        client.set_key(&key);

        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();
        let result = storage.zcard(&key);
        match result {
            Ok(cardinality) => {
                client.set_reply(RespData::Integer(cardinality as i64));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {}", e).into()));
            }
        }
    }
}
