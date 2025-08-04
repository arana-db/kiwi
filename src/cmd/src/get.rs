/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::{impl_cmd_clone_box, impl_cmd_meta};
use crate::{Cmd, CmdFlags, CmdMeta};
use client::Client;
use resp::RespData;
use std::sync::Arc;
use storage::storage::Storage;

#[derive(Clone, Default)]
pub struct GetCmd {
    meta: CmdMeta,
}

impl GetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "get".to_string(),
                arity: 2, // GET key
                flags: CmdFlags::READONLY,
                ..Default::default()
            },
        }
    }
}

impl Cmd for GetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&mut self, client: &mut Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&mut self, client: &mut Client, storage: Arc<Storage>) {
        let key = client.key();
        let result = storage.get(key);

        match result {
            Ok(value) => {
                *client.reply_mut() = RespData::BulkString(Some(value.into()));
            }
            Err(e) => match e {
                storage::error::Error::KeyNotFound { .. } => {
                    *client.reply_mut() = RespData::BulkString(None);
                }
                _ => {
                    *client.reply_mut() = RespData::Error(format!("ERR {e}").into());
                }
            },
        }
    }
}
