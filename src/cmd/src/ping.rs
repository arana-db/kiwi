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

use bytes::Bytes;
use std::sync::Arc;

use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct PingCmd {
    meta: CmdMeta,
}

impl PingCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "ping".to_string(),
                arity: -1,
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::FAST | AclCategory::CONNECTION,
                ..Default::default()
            },
        }
    }
}

impl Cmd for PingCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        if client.argv().len() == 1 {
            client.set_reply(RespData::SimpleString("PONG".into()));
        } else if client.argv().len() == 2 {
            let arg = client.argv()[1].clone();
            client.set_reply(RespData::BulkString(Some(Bytes::from(arg))));
        } else {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'ping' command".into(),
            ));
        }
    }
}
