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
pub struct SmismemberCmd {
    meta: CmdMeta,
}

impl SmismemberCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "smismember".to_string(),
                arity: -3, // SMISMEMBER key member [member ...]
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::SET | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SmismemberCmd {
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
        let argv = client.argv();

        // One reply element per requested member, preserving input order.
        // `sismember` returns Ok(false) for a missing or expired key, so a
        // non-existent set yields all zeros; it only errors on a wrong-type
        // key, in which case the whole command reports that error (matching
        // Redis, which rejects SMISMEMBER on a non-set key).
        let mut replies = Vec::with_capacity(argv.len().saturating_sub(2));
        for member in &argv[2..] {
            match storage.sismember(&key, member) {
                Ok(is_member) => {
                    replies.push(RespData::Integer(if is_member { 1 } else { 0 }));
                }
                Err(e) => {
                    client.set_reply(RespData::Error(format!("ERR {e}").into()));
                    return;
                }
            }
        }

        client.set_reply(RespData::Array(Some(replies)));
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smismember_cmd_meta() {
        let cmd = SmismemberCmd::new();
        assert_eq!(cmd.name(), "smismember");
        assert_eq!(cmd.meta().arity, -3); // SMISMEMBER key member [member ...]
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_smismember_cmd_clone() {
        let cmd = SmismemberCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_smismember_acl_category() {
        let cmd = SmismemberCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::SET));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_smismember_argument_validation() {
        let cmd = SmismemberCmd::new();

        // Valid: command + key + one or more members
        assert!(cmd.check_arg(3)); // SMISMEMBER key member
        assert!(cmd.check_arg(4)); // SMISMEMBER key member member
        assert!(cmd.check_arg(10));

        // Invalid: missing key and/or member
        assert!(!cmd.check_arg(2)); // Missing member
        assert!(!cmd.check_arg(1)); // Missing key and member
        assert!(!cmd.check_arg(0)); // No arguments
    }
}
