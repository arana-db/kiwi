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
pub struct FlushallCmd {
    meta: CmdMeta,
}

impl FlushallCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "flushall".to_string(),
                arity: 1, // FLUSHALL
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::KEYSPACE | AclCategory::WRITE | AclCategory::DANGEROUS,
                ..Default::default()
            },
        }
    }
}

impl Cmd for FlushallCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// FLUSHALL
    ///
    /// Delete all the keys of all the existing databases, not just the currently selected one.
    /// This command never fails.
    ///
    /// # Returns
    /// * Simple string reply: OK
    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        match storage.flushall() {
            Ok(()) => {
                client.set_reply(RespData::SimpleString("OK".into()));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}