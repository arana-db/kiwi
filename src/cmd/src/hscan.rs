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

use crate::scan_options::parse_scan_options;
use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct HScanCmd {
    meta: CmdMeta,
}

impl HScanCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "hscan".to_string(),
                arity: -3, // HSCAN key cursor [MATCH pattern] [COUNT count]
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::HASH | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for HScanCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        if argv.len() < 3 {
            return false;
        }

        // Set the key for locking
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        let key = argv[1].as_slice();
        let options = match parse_scan_options(&argv[2], &argv[3..]) {
            Ok(options) => options,
            Err(error) => {
                client.set_reply(RespData::Error(error.into()));
                return;
            }
        };

        let result = storage.hscan(
            key,
            options.cursor,
            options.pattern.as_deref(),
            options.count,
        );

        match result {
            Ok((next_cursor, fields)) => {
                // Convert field-value pairs to RespData
                // Each field and its value become separate elements in the array
                let mut resp_fields = Vec::new();
                for (field, value) in fields {
                    resp_fields.push(RespData::BulkString(Some(field.into())));
                    resp_fields.push(RespData::BulkString(Some(value.into())));
                }

                // Return [next_cursor, [field1, value1, field2, value2, ...]]
                let response = vec![
                    RespData::BulkString(Some(next_cursor.to_string().into_bytes().into())),
                    RespData::Array(Some(resp_fields)),
                ];

                client.set_reply(RespData::Array(Some(response)));
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
    use client::StreamTrait;
    use storage::{StorageOptions, safe_cleanup_test_db, unique_test_db_path};

    use super::*;

    struct TestStream;

    #[async_trait::async_trait]
    impl StreamTrait for TestStream {
        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }

        async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }
    }

    #[test]
    fn test_hscan_cmd_meta() {
        let cmd = HScanCmd::new();
        assert_eq!(cmd.name(), "hscan");
        assert_eq!(cmd.meta().arity, -3); // HSCAN key cursor [MATCH pattern] [COUNT count]
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_hscan_cmd_clone() {
        let cmd = HScanCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_hscan_acl_category() {
        let cmd = HScanCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::HASH));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_hscan_argument_validation() {
        let cmd = HScanCmd::new();

        // Valid argument counts
        assert!(cmd.check_arg(3)); // HSCAN key cursor
        assert!(cmd.check_arg(5)); // HSCAN key cursor MATCH pattern
        assert!(cmd.check_arg(7)); // HSCAN key cursor MATCH pattern COUNT count

        // Invalid argument counts
        assert!(!cmd.check_arg(1)); // Missing key and cursor
        assert!(!cmd.check_arg(2)); // Missing cursor
        assert!(!cmd.check_arg(0)); // No arguments
    }

    #[tokio::test]
    async fn hscan_command_preserves_binary_field_and_value_bytes() {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), &db_path)
            .unwrap();
        storage.hset(b"binary_hash", b"\xff", b"\xfe").unwrap();
        let storage = Arc::new(storage);
        let client = Client::new(Box::new(TestStream));
        client.set_argv(&[
            b"hscan".to_vec(),
            b"binary_hash".to_vec(),
            b"0".to_vec(),
            b"match".to_vec(),
            b"?".to_vec(),
        ]);

        HScanCmd::new().do_cmd(&client, Arc::clone(&storage));

        assert_eq!(
            client.take_reply(),
            RespData::Array(Some(vec![
                RespData::BulkString(Some(b"0".to_vec().into())),
                RespData::Array(Some(vec![
                    RespData::BulkString(Some(vec![0xff].into())),
                    RespData::BulkString(Some(vec![0xfe].into())),
                ])),
            ]))
        );

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }

    #[tokio::test]
    async fn hscan_command_matches_empty_field_with_repeated_star() {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), &db_path)
            .unwrap();
        storage.hset(b"empty_field_hash", b"", b"value").unwrap();
        let storage = Arc::new(storage);
        let client = Client::new(Box::new(TestStream));
        client.set_argv(&[
            b"hscan".to_vec(),
            b"empty_field_hash".to_vec(),
            b"0".to_vec(),
            b"match".to_vec(),
            b"**".to_vec(),
        ]);

        HScanCmd::new().do_cmd(&client, Arc::clone(&storage));

        assert_eq!(
            client.take_reply(),
            RespData::Array(Some(vec![
                RespData::BulkString(Some(b"0".to_vec().into())),
                RespData::Array(Some(vec![
                    RespData::BulkString(Some(Vec::new().into())),
                    RespData::BulkString(Some(b"value".to_vec().into())),
                ])),
            ]))
        );

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }
}
