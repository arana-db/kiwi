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
pub struct ZcountCmd {
    meta: CmdMeta,
}

impl ZcountCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "zcount".to_string(),
                arity: 4, // ZCOUNT key min max
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::READ | AclCategory::SORTEDSET,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ZcountCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();

        // Validate argument count
        if argv.len() != 4 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'zcount' command"
                    .to_string()
                    .into(),
            ));
            return false;
        }

        let key = argv[1].clone();
        client.set_key(&key);

        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = &argv[1];
        let min_str = &argv[2];
        let max_str = &argv[3];

        // Parse min and max scores
        let min = match String::from_utf8_lossy(min_str).parse::<f64>() {
            Ok(score) => score,
            Err(err_msg) => {
                client.set_reply(RespData::Error(format!("{}", err_msg).into()));
                return;
            }
        };

        let max = match String::from_utf8_lossy(max_str).parse::<f64>() {
            Ok(score) => score,
            Err(err_msg) => {
                client.set_reply(RespData::Error(format!("{}", err_msg).into()));
                return;
            }
        };

        // Perform the ZCOUNT operation
        match storage.zcount(key, min, max) {
            Ok(count) => {
                client.set_reply(RespData::Integer(count as i64));
            }
            Err(err_msg) => {
                client.set_reply(RespData::Error(format!("{}", err_msg).into()));
            }
        }
    }
}
