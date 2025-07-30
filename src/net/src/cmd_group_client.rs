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

#![allow(unused)] // For beginning only.

use crate::base_cmd::{AclCategory, BaseCmd, BaseCmdGroup, CmdFlags, CmdMeta};
use crate::resp::{Protocol, RespProtocol};
use crate::Client;
use std::sync::Arc;
use storage::storage::Storage;

pub fn new_client_cmd() -> BaseCmdGroup {
    let mut client_cmd = BaseCmdGroup::new(
        "client".to_string(),
        -2,
        CmdFlags::ADMIN | CmdFlags::READONLY,
        AclCategory::ADMIN,
    );

    client_cmd.add_sub_cmd(Box::new(CmdClientGetname::new()));
    client_cmd.add_sub_cmd(Box::new(CmdClientSetname::new()));

    client_cmd
}

#[derive(Clone, Default)]
pub struct CmdClientGetname {
    meta: CmdMeta,
}

impl CmdClientGetname {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "getname".to_string(),
                arity: 2,
                flags: CmdFlags::ADMIN | CmdFlags::READONLY,
                acl_category: AclCategory::ADMIN,
                ..Default::default()
            },
        }
    }
}

impl BaseCmd for CmdClientGetname {
    fn meta(&self) -> &CmdMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut CmdMeta {
        &mut self.meta
    }

    fn clone_box(&self) -> Box<dyn BaseCmd> {
        Box::new(self.clone())
    }

    fn do_initial(&mut self, _client: &mut Client) -> bool {
        true
    }

    fn do_cmd(&mut self, client: &mut Client, _storage: Arc<Storage>) {
        let name = String::from_utf8_lossy(client.name()).to_string();
        client.reply_mut().push_bulk_string(name);
    }
}

// --- CLIENT SETNAME ---
#[derive(Clone, Default)]
pub struct CmdClientSetname {
    meta: CmdMeta,
}

impl CmdClientSetname {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "setname".to_string(),
                arity: 3,
                flags: CmdFlags::ADMIN | CmdFlags::WRITE,
                acl_category: AclCategory::ADMIN,
                ..Default::default()
            },
        }
    }
}

impl BaseCmd for CmdClientSetname {
    fn meta(&self) -> &CmdMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut CmdMeta {
        &mut self.meta
    }

    fn clone_box(&self) -> Box<dyn BaseCmd> {
        Box::new(self.clone())
    }

    fn do_initial(&mut self, _client: &mut Client) -> bool {
        true
    }

    fn do_cmd(&mut self, client: &mut Client, _storage: Arc<Storage>) {
        let new_name = client.argv()[2].clone();
        client.set_name(&new_name);
        client.reply_mut().push_bulk_string("OK".to_string());
    }
}
