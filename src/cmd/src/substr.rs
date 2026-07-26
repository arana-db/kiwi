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

/// `SUBSTR key start end` is the deprecated alias of `GETRANGE`, kept for
/// compatibility with clients written before Redis 2.0. It shares the exact
/// same semantics and storage path as `GETRANGE`.
#[derive(Clone, Default)]
pub struct SubstrCmd {
    meta: CmdMeta,
}

impl SubstrCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "substr".to_string(),
                arity: 4, // SUBSTR key start end
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::STRING | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SubstrCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// SUBSTR key start end
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();
        let argv = client.argv();

        // Parse start offset
        let start = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        // Parse end offset
        let end = match String::from_utf8_lossy(&argv[3]).parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        let result = storage.getrange(&key, start, end);

        match result {
            Ok(substring) => {
                client.set_reply(RespData::BulkString(Some(substring.into())));
            }
            Err(e) => match e {
                storage::error::Error::RedisErr { ref message, .. }
                    if message.starts_with("WRONGTYPE") =>
                {
                    // RedisErr already contains the formatted message
                    client.set_reply(RespData::Error(message.clone().into()));
                }
                _ => {
                    client.set_reply(RespData::Error(format!("ERR {e}").into()));
                }
            },
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substr_cmd_meta() {
        let cmd = SubstrCmd::new();
        assert_eq!(cmd.name(), "substr");
        assert_eq!(cmd.meta().arity, 4); // SUBSTR key start end
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_substr_cmd_clone() {
        let cmd = SubstrCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_substr_acl_category() {
        let cmd = SubstrCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::STRING));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_substr_argument_validation() {
        let cmd = SubstrCmd::new();

        // Valid: command + key + start + end
        assert!(cmd.check_arg(4));

        // Invalid argument counts
        assert!(!cmd.check_arg(3)); // Missing end
        assert!(!cmd.check_arg(5)); // Too many arguments
        assert!(!cmd.check_arg(1));
        assert!(!cmd.check_arg(0));
    }
}
