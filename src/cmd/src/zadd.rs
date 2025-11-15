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

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};
use client::Client;
use resp::RespData;
use std::sync::Arc;
use storage::ZsetScoreMember;
use storage::storage::Storage;

#[derive(Clone, Default)]
pub struct ZaddCmd {
    meta: CmdMeta,
}

impl ZaddCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "zadd".to_string(),
                arity: -4, // ZADD key score member [score member ...]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::WRITE | AclCategory::SORTEDSET,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ZaddCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// ZADD key score member [score member ...]
    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();

        // Validate argument count (must be odd: command + key + pairs of score/member)
        if argv.len() < 4 || !argv.len().is_multiple_of(2) {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'zadd' command"
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

        // Parse score-member pairs
        let mut score_members = Vec::new();
        let mut i = 2;

        while i + 1 < argv.len() {
            // Parse score
            let score_str = String::from_utf8_lossy(&argv[i]);
            let score = match score_str.parse::<f64>() {
                Ok(s) => {
                    // Check for valid float (not NaN or infinite)
                    if s.is_nan() || s.is_infinite() {
                        client.set_reply(RespData::Error(
                            "ERR value is not a valid float".to_string().into(),
                        ));
                        return;
                    }
                    s
                }
                Err(_) => {
                    client.set_reply(RespData::Error(
                        "ERR value is not a valid float".to_string().into(),
                    ));
                    return;
                }
            };

            // Get member
            let member = argv[i + 1].clone();

            score_members.push(ZsetScoreMember::new(score, member));
            i += 2;
        }

        // Execute ZADD
        let result = storage.zadd(&key, &score_members);

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
    fn test_zadd_cmd_meta() {
        let cmd = ZaddCmd::new();
        assert_eq!(cmd.name(), "zadd");
        assert_eq!(cmd.meta().arity, -4);
        assert!(cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_zadd_cmd_check_arg() {
        let cmd = ZaddCmd::new();

        // Valid argument counts (even number >= 4)
        assert!(cmd.check_arg(4)); // ZADD key score member
        assert!(cmd.check_arg(6)); // ZADD key score1 member1 score2 member2
        assert!(cmd.check_arg(8)); // ZADD key score1 member1 score2 member2 score3 member3

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Too few
        assert!(!cmd.check_arg(2)); // Too few
        assert!(!cmd.check_arg(3)); // Too few, odd number
    }
}
