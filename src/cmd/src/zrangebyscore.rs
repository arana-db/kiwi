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
pub struct ZrangebyscoreCmd {
    meta: CmdMeta,
}

impl ZrangebyscoreCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "zrangebyscore".to_string(),
                arity: -4, // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::READ | AclCategory::SORTEDSET,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ZrangebyscoreCmd {
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

        let mut with_scores = false;
        let mut offset = None;
        let mut count = None;

        let mut i = 4;
        while i < argv.len() {
            if argv[i].eq_ignore_ascii_case(b"WITHSCORES") {
                with_scores = true;
                i += 1;
            } else if argv[i].eq_ignore_ascii_case(b"LIMIT") {
                if i + 2 >= argv.len() {
                    client.set_reply(RespData::Error("ERR syntax error".into()));
                    return;
                }
                match String::from_utf8_lossy(&argv[i + 1]).parse::<i64>() {
                    Ok(o) => offset = Some(o),
                    Err(_) => {
                        client.set_reply(RespData::Error(
                            "ERR value is not an integer or out of range".into(),
                        ));
                        return;
                    }
                }
                match String::from_utf8_lossy(&argv[i + 2]).parse::<i64>() {
                    Ok(c) => count = Some(c),
                    Err(_) => {
                        client.set_reply(RespData::Error(
                            "ERR value is not an integer or out of range".into(),
                        ));
                        return;
                    }
                }
                i += 3;
            } else {
                client.set_reply(RespData::Error("ERR syntax error".into()));
                return;
            }
        }

        let result = storage.zrangebyscore(&key, min_score, max_score, with_scores, offset, count);

        match result {
            Ok(members) => {
                let resp_array: Vec<RespData> = members
                    .into_iter()
                    .map(|m| RespData::BulkString(Some(m.into())))
                    .collect();
                client.set_reply(RespData::Array(Some(resp_array)));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}
