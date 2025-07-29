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

use crate::Connection;
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
    name: String,
    arity: i16,
    flags: CmdFlags,
    acl_category: AclCategory,
    cmd_id: u32,
}

pub trait BaseCmd: Send + Sync {
    /// return cmd meta
    fn meta(&self) -> &CmdMeta;

    /// return mut cmd meta
    fn meta_mut(&mut self) -> &mut CmdMeta;

    fn do_initial(&mut self, connection: &mut Connection) -> bool;

    fn do_cmd(&mut self, connection: &mut Connection, storage: Arc<Storage>);

    fn clone_box(&self) -> Box<dyn BaseCmd>;

    fn execute(&mut self, connection: &mut Connection, storage: Arc<Storage>) {
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

/// `BaseCmdGroup` 是一个拥有子命令的命令（例如 `CONFIG`、`CLIENT`）。
/// 它本身也实现了 `BaseCmd` trait。
pub struct BaseCmdGroup {
    meta: CmdMeta,
    // BTreeMap 对应 C++ 的 std::map (有序)
    // Box<dyn BaseCmd> 是一个 Trait Object，等价于 C++ 的 unique_ptr<BaseCmd>
    sub_cmds: HashMap<String, Box<dyn BaseCmd>>,
}

impl BaseCmdGroup {
    pub fn new(name: String, flags: CmdFlags) -> Self {
        Self {
            meta: CmdMeta {
                name,
                arity: -2, // 默认组命令至少需要一个子命令
                flags,
                acl_category: AclCategory::empty(),
                cmd_id: 0,
            },
            sub_cmds: HashMap::new(),
        }
    }

    pub fn add_sub_cmd(&mut self, cmd: Box<dyn BaseCmd>) {
        self.sub_cmds.insert(cmd.name().to_lowercase(), cmd);
    }
}

impl BaseCmd for BaseCmdGroup {
    fn meta(&self) -> &CmdMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut CmdMeta {
        &mut self.meta
    }

    fn clone_box(&self) -> Box<dyn BaseCmd> {
        Box::new(self.clone())
    }

    /// 组命令本身不执行，所以这是一个空实现。
    fn do_cmd(&mut self, _connection: &mut Connection, storage: Arc<Storage>) {}

    fn do_initial(&mut self, _connection: &mut Connection) -> bool {
        // 实际逻辑会在这里解析子命令并委托给它
        true
    }

    fn has_sub_command(&self) -> bool {
        true
    }

    fn get_sub_cmd(&self, cmd_name: &str) -> Option<&dyn BaseCmd> {
        self.sub_cmds
            .get(&cmd_name.to_lowercase())
            .map(|cmd| &**cmd)
    }
}
