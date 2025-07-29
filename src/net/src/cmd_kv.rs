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

use crate::base_cmd::{BaseCmd, CmdMeta};
use crate::resp::{Protocol, RespProtocol};
use crate::Connection;
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
            },
        }
    }
}

impl BaseCmd for SetCmd {
    fn meta(&self) -> &CmdMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut CmdMeta {
        &mut self.meta
    }

    fn clone_box(&self) -> Box<dyn BaseCmd> {
        Box::new(self.clone())
    }

    /// SET key value
    fn do_initial(&mut self, connection: &mut Connection) -> bool {
        // TODO: support xx, nx, ex, px

        let key = connection.argv[1].clone();
        connection.set_key(&key);

        self.value = connection.argv[2].clone();

        true
    }

    fn do_cmd(&mut self, connection: &mut Connection, storage: Arc<Storage>) {
        let mut resp = RespProtocol::new();
        match storage.set(connection.key(), &self.value) {
            Ok(_) => resp.push_bulk_string("OK".to_string()),
            Err(e) => resp.push_bulk_string(format!("ERR: {e}")),
        }
    }
}
