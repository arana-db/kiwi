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
pub struct SaddCmd {
    meta: CmdMeta,
}

impl SaddCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "sadd".to_string(),
                arity: -3, // SADD key member [member ...]
                flags: CmdFlags::WRITE | CmdFlags::FAST,
                acl_category: AclCategory::SET,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SaddCmd {
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

        let result = storage.sadd(&key, &members);

        match result {
            Ok(added) => {
                client.set_reply(RespData::Integer(added as i64));
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
    fn test_sadd_cmd_meta() {
        let cmd = SaddCmd::new();
        assert_eq!(cmd.name(), "sadd");
        assert_eq!(cmd.meta().arity, -3); // SADD key member [member ...]
        assert!(cmd.has_flag(CmdFlags::WRITE));
        // ACL categories
        assert!(cmd.acl_category().contains(AclCategory::SET));
    }

    #[test]
    fn test_sadd_cmd_clone() {
        let cmd = SaddCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }
}
