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

use crate::{impl_cmd_clone_box, impl_cmd_meta, AclCategory, Cmd, CmdFlags, CmdMeta};

#[derive(Clone, Default)]
pub struct HLenCmd {
    meta: CmdMeta,
}

impl HLenCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "hlen".to_string(),
                arity: 2, // HLEN key
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::READ | AclCategory::HASH,
                ..Default::default()
            },
        }
    }
}

impl Cmd for HLenCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = &argv[1];
        
        match storage.hlen(key) {
            Ok(len) => {
                client.set_reply(RespData::Integer(len as i64));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {}", e).into()));
            }
        }
    }
}

