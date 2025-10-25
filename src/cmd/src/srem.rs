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
pub struct SremCmd {
    meta: CmdMeta,
}

impl SremCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "srem".to_string(),
                arity: -3, // SREM key member [member ...]
                flags: CmdFlags::WRITE | CmdFlags::FAST,
                acl_category: AclCategory::SET | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SremCmd {
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

        // Extract all members from argv[2..]
        let members: Vec<&[u8]> = argv[2..].iter().map(|v| v.as_slice()).collect();

        let result = storage.srem(&key, &members);

        match result {
            Ok(removed) => {
                client.set_reply(RespData::Integer(removed as i64));
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
    fn test_srem_cmd_meta() {
        let cmd = SremCmd::new();
        assert_eq!(cmd.name(), "srem");
        assert_eq!(cmd.meta().arity, -3); // SREM key member [member ...]
        assert!(cmd.has_flag(CmdFlags::WRITE));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::READONLY));
    }

    #[test]
    fn test_srem_cmd_clone() {
        let cmd = SremCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_srem_acl_category() {
        let cmd = SremCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::SET));
        assert!(cmd.acl_category().contains(AclCategory::WRITE));
    }

    #[test]
    fn test_srem_argument_validation() {
        let cmd = SremCmd::new();

        // Valid argument counts (arity -3 means 3 or more)
        assert!(cmd.check_arg(3)); // SREM key member
        assert!(cmd.check_arg(4)); // SREM key member1 member2
        assert!(cmd.check_arg(5)); // SREM key member1 member2 member3

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Missing key and members
        assert!(!cmd.check_arg(2)); // Missing members
        assert!(!cmd.check_arg(0)); // No arguments
    }
}
