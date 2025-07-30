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

use crate::base_cmd::{BaseCmd, CmdFlags, CmdMeta};
use crate::client::Client;
use crate::resp::Protocol;
use crate::{impl_base_cmd_clone_box, impl_base_cmd_meta};
use std::sync::Arc;
use storage::storage::Storage;

#[derive(Clone, Default)]
pub struct SetCmd {
    meta: CmdMeta,
    value: Vec<u8>,
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
            ..Default::default()
        }
    }
}

impl BaseCmd for SetCmd {
    impl_base_cmd_meta!();
    impl_base_cmd_clone_box!();

    /// SET key value
    fn do_initial(&mut self, client: &mut Client) -> bool {
        // TODO: support xx, nx, ex, px
        let argv = client.argv();

        self.value = argv[2].clone();

        let key = argv[1].clone();
        client.set_key(&key);

        true
    }

    fn do_cmd(&mut self, client: &mut Client, storage: Arc<Storage>) {
        let key = client.key();
        let value = &self.value;

        let result = storage.set(key, value);

        let resp = client.reply_mut();
        match result {
            Ok(_) => resp.push_bulk_string("OK".to_string()),
            Err(e) => resp.push_bulk_string(format!("ERR: {e}")),
        }
    }
}
