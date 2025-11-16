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
pub struct ZscoreCmd {
    meta: CmdMeta,
}

impl ZscoreCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "zscore".to_string(),
                arity: 3, // ZSCORE key member
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::READ | AclCategory::SORTEDSET,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ZscoreCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// ZSCORE key member
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();

        // Validate argument count (must be exactly 3: command + key + member)
        if argv.len() != 3 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'zscore' command"
                    .to_string()
                    .into(),
            ));
            return false;
        }

        let key = argv[1].clone();
        client.set_key(&key);

        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = client.key();

        // Get member
        let member = &argv[2];

        // Execute ZSCORE
        let result = storage.zscore(&key, member);

        match result {
            Ok(Some(score_bytes)) => {
                client.set_reply(RespData::BulkString(Some(score_bytes.into())));
            }
            Ok(None) => {
                client.set_reply(RespData::BulkString(None));
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
    fn test_zscore_cmd_meta() {
        let cmd = ZscoreCmd::new();
        assert_eq!(cmd.name(), "zscore");
        assert_eq!(cmd.meta().arity, 3);
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_zscore_cmd_check_arg() {
        let cmd = ZscoreCmd::new();

        // Valid argument count
        assert!(cmd.check_arg(3)); // ZSCORE key member

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Too few
        assert!(!cmd.check_arg(2)); // Too few
        assert!(!cmd.check_arg(4)); // Too many
    }

    #[test]
    fn test_zscore_cmd_clone() {
        let cmd = ZscoreCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_zscore_acl_category() {
        let cmd = ZscoreCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::READ));
        assert!(cmd.acl_category().contains(AclCategory::SORTEDSET));
    }
}
