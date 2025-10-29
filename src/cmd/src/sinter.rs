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
pub struct SinterCmd {
    meta: CmdMeta,
}

impl SinterCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "sinter".to_string(),
                arity: -2, // SINTER key [key ...]
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::SET | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SinterCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        if argv.len() < 2 {
            return false;
        }

        // Set the first key as the primary key for locking
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        // Convert arguments to byte slices
        let keys: Vec<&[u8]> = argv[1..].iter().map(|k| k.as_slice()).collect();

        let result = storage.sinter(&keys);

        match result {
            Ok(members) => {
                // Convert Vec<String> to Vec<RespData>
                let resp_members: Vec<RespData> = members
                    .into_iter()
                    .map(|member| RespData::BulkString(Some(member.into_bytes().into())))
                    .collect();

                client.set_reply(RespData::Array(Some(resp_members)));
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
    fn test_sinter_cmd_meta() {
        let cmd = SinterCmd::new();
        assert_eq!(cmd.name(), "sinter");
        assert_eq!(cmd.meta().arity, -2); // SINTER key [key ...]
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_sinter_cmd_clone() {
        let cmd = SinterCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_sinter_acl_category() {
        let cmd = SinterCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::SET));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_sinter_argument_validation() {
        let cmd = SinterCmd::new();

        // Valid argument counts (2 or more: command + key1 [+ key2 ...])
        assert!(cmd.check_arg(2)); // SINTER key1
        assert!(cmd.check_arg(3)); // SINTER key1 key2
        assert!(cmd.check_arg(4)); // SINTER key1 key2 key3

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Missing key
        assert!(!cmd.check_arg(0)); // No arguments
    }
}
