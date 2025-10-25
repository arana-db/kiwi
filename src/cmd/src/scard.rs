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
pub struct ScardCmd {
    meta: CmdMeta,
}

impl ScardCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "scard".to_string(),
                arity: 2, // SCARD key
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::SET | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ScardCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        let result = storage.scard(&key);

        match result {
            Ok(count) => {
                client.set_reply(RespData::Integer(count as i64));
            }
            Err(e) => {
                // Handle key not found as 0 (Redis behavior)
                if e.to_string().contains("key not found") {
                    client.set_reply(RespData::Integer(0));
                } else {
                    client.set_reply(RespData::Error(format!("ERR {e}").into()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scard_cmd_meta() {
        let cmd = ScardCmd::new();
        assert_eq!(cmd.name(), "scard");
        assert_eq!(cmd.meta().arity, 2); // SCARD key
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_scard_cmd_clone() {
        let cmd = ScardCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_scard_acl_category() {
        let cmd = ScardCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::SET));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_scard_argument_validation() {
        let cmd = ScardCmd::new();

        // Valid argument count (2: command + key)
        assert!(cmd.check_arg(2));

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Missing key
        assert!(!cmd.check_arg(3)); // Too many arguments
        assert!(!cmd.check_arg(0)); // No arguments
    }
}
