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

/// Parsed `SCAN` arguments. `SCAN` extends the shared cursor/`MATCH`/`COUNT`
/// grammar with an optional `TYPE`, so it uses its own parser rather than the
/// shared [`crate::scan_options`] one.
struct ScanArgs {
    cursor: u64,
    pattern: Option<Vec<u8>>,
    count: Option<usize>,
    type_filter: Option<Vec<u8>>,
}

fn parse_scan_args(cursor: &[u8], options: &[Vec<u8>]) -> Result<ScanArgs, &'static str> {
    let cursor = std::str::from_utf8(cursor)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or("ERR invalid cursor")?;

    let mut pattern = None;
    let mut count = None;
    let mut type_filter = None;
    let mut index = 0;

    while index < options.len() {
        if options[index].eq_ignore_ascii_case(b"MATCH") {
            let value = options.get(index + 1).ok_or("ERR syntax error")?;
            pattern = Some(value.clone());
            index += 2;
        } else if options[index].eq_ignore_ascii_case(b"COUNT") {
            let value = options.get(index + 1).ok_or("ERR syntax error")?;
            let parsed = std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or("ERR value is not an integer or out of range")?;
            if parsed < 1 {
                return Err("ERR syntax error");
            }
            count = Some(
                usize::try_from(parsed)
                    .map_err(|_| "ERR value is not an integer or out of range")?,
            );
            index += 2;
        } else if options[index].eq_ignore_ascii_case(b"TYPE") {
            let value = options.get(index + 1).ok_or("ERR syntax error")?;
            type_filter = Some(value.clone());
            index += 2;
        } else {
            return Err("ERR syntax error");
        }
    }

    Ok(ScanArgs {
        cursor,
        pattern,
        count,
        type_filter,
    })
}

#[derive(Clone, Default)]
pub struct ScanCmd {
    meta: CmdMeta,
}

impl ScanCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "scan".to_string(),
                arity: -2, // SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::KEYSPACE | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ScanCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        let args = match parse_scan_args(&argv[1], &argv[2..]) {
            Ok(args) => args,
            Err(error) => {
                client.set_reply(RespData::Error(error.into()));
                return;
            }
        };

        let result = storage.scan(
            args.cursor,
            args.count.unwrap_or(10),
            args.type_filter.as_deref(),
            args.pattern.as_deref().unwrap_or(b"*"),
        );

        match result {
            Ok((next_cursor, keys)) => {
                let resp_keys: Vec<RespData> = keys
                    .into_iter()
                    .map(|key| RespData::BulkString(Some(key.into())))
                    .collect();

                // Reply shape: [next_cursor, [key1, key2, ...]]
                let response = vec![
                    RespData::BulkString(Some(next_cursor.to_string().into_bytes().into())),
                    RespData::Array(Some(resp_keys)),
                ];

                client.set_reply(RespData::Array(Some(response)));
            }
            Err(storage::error::Error::RedisErr { message, .. }) => {
                client.set_reply(RespData::Error(message.into()));
            }
            Err(e) => client.set_reply(RespData::Error(format!("ERR {e}").into())),
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_cmd_meta() {
        let cmd = ScanCmd::new();
        assert_eq!(cmd.name(), "scan");
        assert_eq!(cmd.meta().arity, -2);
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_scan_cmd_clone() {
        let cmd = ScanCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_scan_acl_category() {
        let cmd = ScanCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::KEYSPACE));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_scan_argument_validation() {
        let cmd = ScanCmd::new();
        assert!(cmd.check_arg(2)); // SCAN cursor
        assert!(cmd.check_arg(4)); // SCAN cursor MATCH pattern
        assert!(!cmd.check_arg(1)); // missing cursor
        assert!(!cmd.check_arg(0));
    }

    #[test]
    fn test_parse_cursor_and_options() {
        let opts = vec![
            b"MATCH".to_vec(),
            b"user:*".to_vec(),
            b"COUNT".to_vec(),
            b"50".to_vec(),
            b"TYPE".to_vec(),
            b"hash".to_vec(),
        ];
        let args = parse_scan_args(b"12", &opts).unwrap();
        assert_eq!(args.cursor, 12);
        assert_eq!(args.pattern.as_deref(), Some(b"user:*".as_slice()));
        assert_eq!(args.count, Some(50));
        assert_eq!(args.type_filter.as_deref(), Some(b"hash".as_slice()));
    }

    #[test]
    fn test_parse_option_names_are_case_insensitive() {
        let opts = vec![
            b"match".to_vec(),
            b"*".to_vec(),
            b"Count".to_vec(),
            b"7".to_vec(),
        ];
        let args = parse_scan_args(b"0", &opts).unwrap();
        assert_eq!(args.pattern.as_deref(), Some(b"*".as_slice()));
        assert_eq!(args.count, Some(7));
        assert_eq!(args.type_filter, None);
    }

    #[test]
    fn test_parse_invalid_cursor() {
        assert_eq!(
            parse_scan_args(b"notanumber", &[]).err(),
            Some("ERR invalid cursor")
        );
    }

    #[test]
    fn test_parse_count_must_be_positive() {
        let opts = vec![b"COUNT".to_vec(), b"0".to_vec()];
        assert_eq!(parse_scan_args(b"0", &opts).err(), Some("ERR syntax error"));

        let opts = vec![b"COUNT".to_vec(), b"-1".to_vec()];
        assert_eq!(parse_scan_args(b"0", &opts).err(), Some("ERR syntax error"));
    }

    #[test]
    fn test_parse_count_uses_redis_signed_integer_range() {
        let opts = vec![b"COUNT".to_vec(), i64::MAX.to_string().into_bytes()];
        assert_eq!(
            parse_scan_args(b"0", &opts).unwrap().count,
            Some(i64::MAX as usize)
        );

        let opts = vec![b"COUNT".to_vec(), b"9223372036854775808".to_vec()];
        assert_eq!(
            parse_scan_args(b"0", &opts).err(),
            Some("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn test_parse_unknown_option_is_syntax_error() {
        let opts = vec![b"WEIRD".to_vec()];
        assert_eq!(parse_scan_args(b"0", &opts).err(), Some("ERR syntax error"));
    }

    #[test]
    fn test_parse_dangling_option_value_is_syntax_error() {
        let opts = vec![b"MATCH".to_vec()];
        assert_eq!(parse_scan_args(b"0", &opts).err(), Some("ERR syntax error"));
    }
}
