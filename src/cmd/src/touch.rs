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
pub struct TouchCmd {
    meta: CmdMeta,
}

impl TouchCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "touch".to_string(),
                arity: -2, // TOUCH key [key ...]
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::KEYSPACE | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for TouchCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// TOUCH key [key ...]
    ///
    /// Returns the number of the specified keys that exist. A key mentioned
    /// multiple times and existing is counted once per mention, matching
    /// Redis. With the cache disabled this is a pure existence count and has
    /// no last-access side effect.
    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        // Skip command name, collect all key arguments.
        let keys: Vec<Vec<u8>> = argv[1..].to_vec();

        if keys.is_empty() {
            client.set_reply(RespData::Integer(0));
            return;
        }

        match storage.exists(&keys) {
            Ok(count) => {
                client.set_reply(RespData::Integer(count));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_touch_cmd_meta() {
        let cmd = TouchCmd::new();
        assert_eq!(cmd.name(), "touch");
        assert_eq!(cmd.meta().arity, -2); // TOUCH key [key ...]
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_touch_cmd_clone() {
        let cmd = TouchCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_touch_acl_category() {
        let cmd = TouchCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::KEYSPACE));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_touch_argument_validation() {
        let cmd = TouchCmd::new();

        // Valid: command + one or more keys
        assert!(cmd.check_arg(2)); // TOUCH key
        assert!(cmd.check_arg(3)); // TOUCH key key
        assert!(cmd.check_arg(10));

        // Invalid: missing key
        assert!(!cmd.check_arg(1)); // No key
        assert!(!cmd.check_arg(0)); // No arguments
    }
}
