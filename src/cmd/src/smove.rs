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
pub struct SmoveCmd {
    meta: CmdMeta,
}

impl SmoveCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "smove".to_string(),
                arity: 4, // SMOVE source destination member
                flags: CmdFlags::WRITE | CmdFlags::FAST,
                acl_category: AclCategory::SET | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SmoveCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let source_key = argv[1].clone();
        client.set_key(&source_key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let source = &argv[1];
        let destination = &argv[2];
        let member = &argv[3];

        let result = storage.smove(source, destination, member);

        match result {
            Ok(moved) => {
                client.set_reply(RespData::Integer(if moved { 1 } else { 0 }));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smove_cmd_meta() {
        let cmd = SmoveCmd::new();
        assert_eq!(cmd.name(), "smove");
        assert_eq!(cmd.meta().arity, 4); // SMOVE source destination member
        assert!(cmd.has_flag(CmdFlags::WRITE));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::READONLY));
    }

    #[test]
    fn test_smove_cmd_clone() {
        let cmd = SmoveCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_smove_acl_category() {
        let cmd = SmoveCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::SET));
        assert!(cmd.acl_category().contains(AclCategory::WRITE));
    }

    #[test]
    fn test_smove_argument_validation() {
        let cmd = SmoveCmd::new();

        // Valid argument count
        assert!(cmd.check_arg(4)); // SMOVE source destination member

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Missing all arguments
        assert!(!cmd.check_arg(2)); // Missing destination and member
        assert!(!cmd.check_arg(3)); // Missing member
        assert!(!cmd.check_arg(5)); // Too many arguments
        assert!(!cmd.check_arg(0)); // No arguments
    }
}
