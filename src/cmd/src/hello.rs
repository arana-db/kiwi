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
use resp::{CommandType, RespCommand, RespData};
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct HelloCmd {
    meta: CmdMeta,
}

impl HelloCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "hello".to_string(),
                arity: -1,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION | AclCategory::FAST,
                ..Default::default()
            },
        }
    }
}

impl Cmd for HelloCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        let argv = client.argv();
        let command = RespCommand {
            command_type: CommandType::Hello,
            args: argv.into_iter().skip(1).map(Bytes::from).collect(),
            is_pipeline: false,
        };

        match client.handle_hello(&command) {
            Ok(response) => client.set_reply(response),
            Err(err) => client.set_reply(RespData::Error(format!("ERR {err}").into())),
        }
    }
}
