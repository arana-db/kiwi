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

use bytes::Bytes;
use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct HGetAllCmd {
    meta: CmdMeta,
}

impl HGetAllCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "hgetall".to_string(),
                arity: 2, // HGETALL key
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::READ | AclCategory::HASH,
                ..Default::default()
            },
        }
    }
}

impl Cmd for HGetAllCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = &argv[1];

        match storage.hgetall(key) {
            Ok(field_values) => {
                let mut resp_data = Vec::new();
                for (field, value) in field_values {
                    resp_data.push(RespData::BulkString(Some(Bytes::from(field))));
                    resp_data.push(RespData::BulkString(Some(Bytes::from(value))));
                }
                client.set_reply(RespData::Array(Some(resp_data)));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {}", e).into()));
            }
        }
    }
}
