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

use crate::Client;
use bitflags::bitflags;
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use storage::storage::Storage;

bitflags! {
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct CmdFlags: u32 {
        const WRITE              = 1 << 0;  // May modify the dataset
        const READONLY           = 1 << 1;  // Doesn't modify the dataset
        const MODULE             = 1 << 2;  // Implemented by a module
        const ADMIN              = 1 << 3;  // Administrative command
        const PUBSUB             = 1 << 4;  // Pub/Sub related command
        const NOSCRIPT           = 1 << 5;  // Not allowed in Lua scripts
        const BLOCKING           = 1 << 6;  // May block the server
        const SKIP_MONITOR       = 1 << 7;  // Don't propagate to MONITOR
        const SKIP_SLOWLOG       = 1 << 8;  // Don't log to slowlog
        const FAST               = 1 << 9;  // Tagged as fast by developer
        const NO_AUTH            = 1 << 10; // Skip ACL checks
        const MAY_REPLICATE      = 1 << 11; // May replicate even if writes are disabled
        const PROTECTED          = 1 << 12; // Don't accept in scripts
        const MODULE_NO_CLUSTER  = 1 << 13; // No cluster mode support
        const NO_MULTI           = 1 << 14; // Cannot be pipelined
        const EXCLUSIVE          = 1 << 15; // May change Storage pointer
        const RAFT               = 1 << 16; // raft
    }
}

bitflags! {
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct AclCategory: u32 {
        const KEYSPACE      = 1 << 0;
        const READ          = 1 << 1;
        const WRITE         = 1 << 2;
        const SET           = 1 << 3;
        const SORTEDSET     = 1 << 4;
        const LIST          = 1 << 5;
        const HASH          = 1 << 6;
        const STRING        = 1 << 7;
        const BITMAP        = 1 << 8;
        const HYPERLOGLOG   = 1 << 9;
        const GEO           = 1 << 10;
        const STREAM        = 1 << 11;
        const PUBSUB        = 1 << 12;
        const ADMIN         = 1 << 13;
        const FAST          = 1 << 14;
        const SLOW          = 1 << 15;
        const BLOCKING      = 1 << 16;
        const DANGEROUS     = 1 << 17;
        const CONNECTION    = 1 << 18;
        const TRANSACTION   = 1 << 19;
        const SCRIPTING     = 1 << 20;
        const RAFT          = 1 << 21;
    }
}

#[derive(Debug, Clone, Default)]
pub struct CmdMeta {
    pub name: String,
    pub arity: i16,
    pub flags: CmdFlags,
    pub acl_category: AclCategory,
    pub cmd_id: u32,
}

pub trait BaseCmd: Send + Sync {
    /// return cmd meta
    fn meta(&self) -> &CmdMeta;

    /// return mut cmd meta
    fn meta_mut(&mut self) -> &mut CmdMeta;

    fn do_initial(&mut self, connection: &mut Client) -> bool;

    fn do_cmd(&mut self, connection: &mut Client, storage: Arc<Storage>);

    fn clone_box(&self) -> Box<dyn BaseCmd>;

    fn execute(&mut self, connection: &mut Client, storage: Arc<Storage>) {
        debug!("excute command: {:?}", connection.cmd_name());
        if self.do_initial(connection) {
            self.do_cmd(connection, storage);
        }
    }

    fn name(&self) -> &str {
        &self.meta().name
    }

    fn check_arg(&self, num: usize) -> bool {
        let arity = self.meta().arity;
        if arity > 0 {
            num == -arity as usize
        } else {
            num >= -arity as usize
        }
    }

    fn has_flag(&self, flag: CmdFlags) -> bool {
        self.meta().flags.contains(flag)
    }

    fn set_flag(&mut self, flag: CmdFlags) {
        self.meta_mut().flags.insert(flag);
    }

    fn reset_flag(&mut self, flag: CmdFlags) {
        self.meta_mut().flags.remove(flag);
    }

    fn acl_category(&self) -> AclCategory {
        self.meta().acl_category
    }

    fn has_sub_command(&self) -> bool {
        false
    }

    fn get_sub_cmd(&self, _cmd_name: &str) -> Option<&dyn BaseCmd> {
        None
    }
}

#[derive(Default)]
pub struct BaseCmdGroup {
    meta: CmdMeta,
    sub_cmds: HashMap<String, Box<dyn BaseCmd>>,
}

impl Clone for BaseCmdGroup {
    fn clone(&self) -> Self {
        let mut new_group = BaseCmdGroup {
            meta: self.meta.clone(), // CmdMeta 可以自动派生 Clone
            sub_cmds: HashMap::new(),
        };

        for (name, cmd) in &self.sub_cmds {
            // 错误的地方：不能用 cmd.clone()
            // 正确的做法：调用我们定义的 clone_box() 方法
            new_group.sub_cmds.insert(name.clone(), cmd.clone_box());
        }

        new_group
    }
}

impl BaseCmdGroup {
    pub fn new(name: String, arity: i16, flags: CmdFlags, acl_category: AclCategory) -> Self {
        Self {
            meta: CmdMeta {
                name,
                arity,
                flags,
                acl_category,
                ..Default::default()
            },
            sub_cmds: HashMap::new(),
        }
    }

    pub fn add_sub_cmd(&mut self, cmd: Box<dyn BaseCmd>) {
        let name = cmd.name().to_lowercase();
        self.sub_cmds.insert(name, cmd);
    }
}

impl BaseCmd for BaseCmdGroup {
    fn meta(&self) -> &CmdMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut CmdMeta {
        &mut self.meta
    }

    fn do_initial(&mut self, _connection: &mut Client) -> bool {
        true
    }

    fn do_cmd(&mut self, _connection: &mut Client, _storage: Arc<Storage>) {}

    fn clone_box(&self) -> Box<dyn BaseCmd> {
        let mut cloned_group = BaseCmdGroup::new(
            self.meta.name.clone(),
            self.meta.arity,
            self.meta.flags,
            self.meta.acl_category,
        );
        for (name, cmd) in &self.sub_cmds {
            cloned_group.sub_cmds.insert(name.clone(), cmd.clone_box());
        }
        Box::new(cloned_group)
    }

    fn has_sub_command(&self) -> bool {
        true
    }

    fn get_sub_cmd(&self, cmd_name: &str) -> Option<&(dyn BaseCmd + 'static)> {
        self.sub_cmds.get(cmd_name).map(|cmd| cmd.as_ref())
    }
}
