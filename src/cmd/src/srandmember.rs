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
pub struct SrandmemberCmd {
    meta: CmdMeta,
}

impl SrandmemberCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "srandmember".to_string(),
                arity: -2, // SRANDMEMBER key [count] - accepts 2 or more args
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::SET | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SrandmemberCmd {
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

        // Validate argument count (should be 2 or 3)
        if argv.len() > 3 {
            client.set_reply(RespData::Error(
                format!(
                    "ERR wrong number of arguments for '{}' command",
                    String::from_utf8_lossy(client.cmd_name().as_slice()),
                )
                .into(),
            ));
            return;
        }

        // Parse optional count parameter
        let count = if argv.len() > 2 {
            match String::from_utf8_lossy(&argv[2]).parse::<i32>() {
                Ok(c) => Some(c),
                Err(_) => {
                    client.set_reply(RespData::Error(
                        "ERR value is not an integer or out of range"
                            .to_string()
                            .into(),
                    ));
                    return;
                }
            }
        } else {
            None
        };

        let result = storage.srandmember(&key, count);

        match result {
            Ok(members) => {
                if count.is_none() {
                    // Single member case - return as bulk string or nil
                    if members.is_empty() {
                        client.set_reply(RespData::BulkString(None));
                    } else {
                        client.set_reply(RespData::BulkString(Some(
                            members[0].clone().into_bytes().into(),
                        )));
                    }
                } else {
                    // Multiple members case - return as array
                    let resp_members: Vec<RespData> = members
                        .into_iter()
                        .map(|member| RespData::BulkString(Some(member.into_bytes().into())))
                        .collect();
                    client.set_reply(RespData::Array(Some(resp_members)));
                }
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
    fn test_srandmember_cmd_meta() {
        let cmd = SrandmemberCmd::new();
        assert_eq!(cmd.name(), "srandmember");
        assert_eq!(cmd.meta().arity, -2); // SRANDMEMBER key [count]
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_srandmember_cmd_clone() {
        let cmd = SrandmemberCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_srandmember_acl_category() {
        let cmd = SrandmemberCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::SET));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_srandmember_argument_validation() {
        let cmd = SrandmemberCmd::new();

        // Valid argument counts (arity -2 means 2 or more)
        assert!(cmd.check_arg(2)); // SRANDMEMBER key
        assert!(cmd.check_arg(3)); // SRANDMEMBER key count
        assert!(cmd.check_arg(4)); // More args allowed by arity, but handled in do_cmd

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Missing key
        assert!(!cmd.check_arg(0)); // No arguments
    }
}
