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
pub struct SetCmd {
    meta: CmdMeta,
}

impl SetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "set".to_string(),
                arity: 3, // SET key value
                flags: CmdFlags::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// SET key value
    fn do_initial(&self, client: &mut Client) -> bool {
        // TODO: support xx, nx, ex, px
        let argv = client.argv();

        let key = argv[1].clone();
        client.set_key(&key);

        true
    }

    fn do_cmd(&self, client: &mut Client, storage: Arc<Storage>) {
        let key = client.key();
        let value = &client.argv()[2];

        let result = storage.set(&key, value);

        match result {
            Ok(_) => {
                *client.reply_mut() = RespData::SimpleString("OK".to_string().into());
            }
            Err(e) => {
                *client.reply_mut() = RespData::Error(format!("ERR {e}").into());
            }
        }
    }
}
