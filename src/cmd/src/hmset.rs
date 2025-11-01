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

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct HMSetCmd {
    meta: CmdMeta,
}

impl HMSetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "hmset".to_string(),
                arity: -4, // At least 4 args: HMSET key field value [field value ...]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::WRITE | AclCategory::HASH,
                ..Default::default()
            },
        }
    }
}

impl Cmd for HMSetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        if argv.len() < 4 || !(argv.len() - 2).is_multiple_of(2) {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'hmset' command".into(),
            ));
            return false;
        }
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = &argv[1];

        let mut field_values = Vec::new();
        for i in (2..argv.len()).step_by(2) {
            field_values.push((argv[i].clone(), argv[i + 1].clone()));
        }

        match storage.hmset(key, &field_values) {
            Ok(_) => {
                client.set_reply(RespData::SimpleString("OK".into()));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {}", e).into()));
            }
        }
    }
}
