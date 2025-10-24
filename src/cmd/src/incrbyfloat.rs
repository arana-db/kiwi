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

use crate::{Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct IncrbyFloatCmd {
    meta: CmdMeta,
}

// https://redis.io/commands/incrbyfloat/
impl IncrbyFloatCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "incrbyfloat".to_string(),
                arity: 3, // INCRBYFLOAT key increment
                flags: CmdFlags::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for IncrbyFloatCmd {
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
        let value = &client.argv()[2];

        let incr: f64 = match String::from_utf8_lossy(value).to_string().parse() {
            Ok(v) => v,
            Err(_) => {
                client.set_reply(RespData::Error("ERR value is not a valid float".into()));
                return;
            }
        };

        // Check for NaN or infinity in the increment
        if incr.is_nan() || incr.is_infinite() {
            client.set_reply(RespData::Error(
                "ERR increment would produce NaN or Infinity".into(),
            ));
            return;
        }

        let result = storage.incr_decr_float(&key, incr);
        match result {
            Ok(v) => {
                // Format as integer notation if no fractional part, otherwise standard float
                let formatted = if v.fract() == 0.0 && v.abs() < (i64::MAX as f64) {
                    format!("{:.0}", v)
                } else {
                    format!("{}", v)
                };
                client.set_reply(RespData::BulkString(Some(formatted.into())));
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
    fn test_incrbyfloat_cmd_meta() {
        let cmd = IncrbyFloatCmd::new();
        assert_eq!(cmd.meta().name, "incrbyfloat");
        assert_eq!(cmd.meta().arity, 3);
        assert!(cmd.meta().flags.contains(CmdFlags::WRITE));
    }

    #[test]
    fn test_incrbyfloat_cmd_check_arg() {
        let cmd = IncrbyFloatCmd::new();

        // Correct number of arguments
        assert!(cmd.check_arg(3));

        // Wrong number of arguments
        assert!(!cmd.check_arg(2));
        assert!(!cmd.check_arg(4));
        assert!(!cmd.check_arg(1));
    }

    #[test]
    fn test_incrbyfloat_cmd_name() {
        let cmd = IncrbyFloatCmd::new();
        assert_eq!(cmd.name(), "incrbyfloat");
    }

    #[test]
    fn test_incrbyfloat_cmd_has_flag() {
        let cmd = IncrbyFloatCmd::new();
        assert!(cmd.has_flag(CmdFlags::WRITE));
        assert!(!cmd.has_flag(CmdFlags::READONLY));
    }

    #[test]
    fn test_incrbyfloat_cmd_clone() {
        let cmd = IncrbyFloatCmd::new();
        let cloned_cmd = cmd.clone_box();
        assert_eq!(cloned_cmd.name(), cmd.name());
        assert_eq!(cloned_cmd.meta().arity, cmd.meta().arity);
    }

    // Note: Integration tests with actual storage and client would require
    // more complex setup and are better suited for integration test files.
    // The core logic is tested in the storage layer tests.
}
