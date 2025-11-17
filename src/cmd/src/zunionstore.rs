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
pub struct ZunionstoreCmd {
    meta: CmdMeta,
}

impl ZunionstoreCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "zunionstore".to_string(),
                arity: -4, // ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::WRITE | AclCategory::SORTEDSET | AclCategory::SLOW,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ZunionstoreCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let destination = argv[1].clone();
        client.set_key(&destination);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        if argv.len() < 3 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'zunionstore' command"
                    .to_string()
                    .into(),
            ));
            return;
        }

        let destination = &argv[1];

        // Parse numkeys
        let numkeys_str = String::from_utf8_lossy(&argv[2]);
        let numkeys = match numkeys_str.parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => {
                client.set_reply(RespData::Error(
                    "ERR numkeys should be greater than 0".to_string().into(),
                ));
                return;
            }
        };

        if argv.len() < 3 + numkeys {
            client.set_reply(RespData::Error("ERR syntax error".to_string().into()));
            return;
        }

        // Extract keys
        let keys: Vec<Vec<u8>> = argv[3..3 + numkeys].to_vec();

        // Parse optional WEIGHTS and AGGREGATE
        let mut weights: Vec<f64> = Vec::new();
        let mut aggregate = "SUM".to_string();
        let mut idx = 3 + numkeys;

        while idx < argv.len() {
            let option = String::from_utf8_lossy(&argv[idx]).to_uppercase();

            match option.as_str() {
                "WEIGHTS" => {
                    idx += 1;
                    if idx + numkeys > argv.len() {
                        client.set_reply(RespData::Error("ERR syntax error".to_string().into()));
                        return;
                    }

                    for i in 0..numkeys {
                        let weight_str = String::from_utf8_lossy(&argv[idx + i]);
                        match weight_str.parse::<f64>() {
                            Ok(w) => weights.push(w),
                            Err(_) => {
                                client.set_reply(RespData::Error(
                                    "ERR weight value is not a float".to_string().into(),
                                ));
                                return;
                            }
                        }
                    }
                    idx += numkeys;
                }
                "AGGREGATE" => {
                    idx += 1;
                    if idx >= argv.len() {
                        client.set_reply(RespData::Error("ERR syntax error".to_string().into()));
                        return;
                    }

                    aggregate = String::from_utf8_lossy(&argv[idx]).to_uppercase();
                    if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
                        client.set_reply(RespData::Error("ERR syntax error".to_string().into()));
                        return;
                    }
                    idx += 1;
                }
                _ => {
                    client.set_reply(RespData::Error("ERR syntax error".to_string().into()));
                    return;
                }
            }
        }

        let result = storage.zunionstore(destination, &keys, &weights, &aggregate);

        match result {
            Ok(count) => {
                client.set_reply(RespData::Integer(count as i64));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}
