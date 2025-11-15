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

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};
use client::Client;
use resp::RespData;
use storage::storage::Storage;

#[derive(Clone, Default)]
pub struct ZremrangebyscoreCmd {
    meta: CmdMeta,
}

impl ZremrangebyscoreCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "zremrangebyscore".to_string(),
                arity: 4, // ZREMRANGEBYSCORE key min max
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::WRITE | AclCategory::SORTEDSET,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ZremrangebyscoreCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = client.key();

        let min_score = match String::from_utf8_lossy(&argv[2]).parse::<f64>() {
            Ok(s) => s,
            Err(_) => {
                client.set_reply(RespData::Error("ERR min or max is not a float".into()));
                return;
            }
        };

        let max_score = match String::from_utf8_lossy(&argv[3]).parse::<f64>() {
            Ok(s) => s,
            Err(_) => {
                client.set_reply(RespData::Error("ERR min or max is not a float".into()));
                return;
            }
        };

        let result = storage.zremrangebyscore(&key, min_score, max_score);

        match result {
            Ok(count) => client.set_reply(RespData::Integer(count as i64)),
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}
