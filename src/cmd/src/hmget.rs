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

use crate::{impl_cmd_clone_box, impl_cmd_meta, AclCategory, Cmd, CmdFlags, CmdMeta};

#[derive(Clone, Default)]
pub struct HMGetCmd {
    meta: CmdMeta,
}

impl HMGetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "hmget".to_string(),
                arity: -3, // At least 3 args: HMGET key field [field ...]
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::READ | AclCategory::HASH,
                ..Default::default()
            },
        }
    }
}

impl Cmd for HMGetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = &argv[1];
        let fields: Vec<Vec<u8>> = argv[2..].to_vec();
        
        match storage.hmget(key, &fields) {
            Ok(values) => {
                let resp_values: Vec<RespData> = values
                    .into_iter()
                    .map(|v| match v {
                        Some(value) => RespData::BulkString(Some(Bytes::from(value))),
                        None => RespData::BulkString(None),
                    })
                    .collect();
                client.set_reply(RespData::Array(Some(resp_values)));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {}", e).into()));
            }
        }
    }
}

