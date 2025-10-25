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
pub struct SunionstoreCmd {
    meta: CmdMeta,
}

impl SunionstoreCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "sunionstore".to_string(),
                arity: -3, // SUNIONSTORE destination key [key ...]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::SET | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SunionstoreCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        if argv.len() < 3 {
            return false;
        }

        // Set the destination key as the primary key for locking
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        // First argument is destination, rest are source keys
        let destination = argv[1].as_slice();
        let source_keys: Vec<&[u8]> = argv[2..].iter().map(|k| k.as_slice()).collect();

        let result = storage.sunionstore(destination, &source_keys);

        match result {
            Ok(count) => {
                client.set_reply(RespData::Integer(count as i64));
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
    fn test_sunionstore_cmd_meta() {
        let cmd = SunionstoreCmd::new();
        assert_eq!(cmd.name(), "sunionstore");
        assert_eq!(cmd.meta().arity, -3);
        assert!(cmd.has_flag(CmdFlags::WRITE));
        assert!(!cmd.has_flag(CmdFlags::READONLY));
    }

    #[test]
    fn test_sunionstore_cmd_clone() {
        let cmd = SunionstoreCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_sunionstore_acl_category() {
        let cmd = SunionstoreCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::SET));
        assert!(cmd.acl_category().contains(AclCategory::WRITE));
    }

    #[test]
    fn test_sunionstore_argument_validation() {
        let cmd = SunionstoreCmd::new();

        // Valid argument counts
        assert!(cmd.check_arg(3)); // SUNIONSTORE dest key1
        assert!(cmd.check_arg(4)); // SUNIONSTORE dest key1 key2

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Missing destination and keys
        assert!(!cmd.check_arg(2)); // Missing source key
        assert!(!cmd.check_arg(0)); // No arguments
    }
}
