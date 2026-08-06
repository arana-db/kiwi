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
use storage::storage::Storage;

use crate::getrange::execute_getrange;
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
        execute_getrange(client, storage);
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use client::StreamTrait;
    use resp::{RespData, RespVersion, encode::RespEncode, encode::RespEncoder};
    use storage::{StorageOptions, safe_cleanup_test_db, unique_test_db_path};

    use super::*;
    use crate::auth::no_requirepass_provider;
    use crate::table::create_command_table;

    struct TestStream {
        _marker: u8,
    }

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

    #[tokio::test]
    async fn substr_returns_the_redis_raw_bulk_reply_for_extreme_negative_end() {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), &db_path)
            .unwrap();
        storage.set(b"key", b"Hello World").unwrap();
        let storage = Arc::new(storage);
        let client = Client::new(Box::new(TestStream { _marker: 0 }));
        client.set_cmd_name(b"substr");
        client.set_argv(&[
            b"substr".to_vec(),
            b"key".to_vec(),
            b"0".to_vec(),
            b"-100".to_vec(),
        ]);

        let command_table = create_command_table(no_requirepass_provider());
        command_table
            .get("substr")
            .expect("SUBSTR should be publicly registered")
            .execute(&client, Arc::clone(&storage));

        let reply = client.take_reply();
        assert_eq!(reply, RespData::BulkString(Some(b"H".to_vec().into())));
        for version in [RespVersion::RESP2, RespVersion::RESP3] {
            let mut encoder = RespEncoder::new(version);
            encoder.encode_resp_data(&reply);
            assert_eq!(encoder.get_response().as_ref(), b"$1\r\nH\r\n");
        }

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }

    #[tokio::test]
    async fn substr_preserves_wrongtype_error_prefix() {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), &db_path)
            .unwrap();
        storage.hset(b"hash", b"field", b"value").unwrap();
        let storage = Arc::new(storage);
        let client = Client::new(Box::new(TestStream { _marker: 0 }));
        client.set_cmd_name(b"substr");
        client.set_argv(&[
            b"substr".to_vec(),
            b"hash".to_vec(),
            b"0".to_vec(),
            b"-1".to_vec(),
        ]);

        SubstrCmd::new().execute(&client, Arc::clone(&storage));

        let reply = client.take_reply();
        let RespData::Error(message) = &reply else {
            panic!("expected WRONGTYPE error, got {reply:?}");
        };
        assert!(message.starts_with(b"WRONGTYPE"));
        let mut encoder = RespEncoder::new(RespVersion::RESP2);
        encoder.encode_resp_data(&reply);
        assert!(encoder.get_response().starts_with(b"-WRONGTYPE"));

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }

    #[tokio::test]
    async fn substr_preserves_binary_values_and_returns_empty_for_missing_keys() {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), &db_path)
            .unwrap();
        storage.set(b"binary", b"\xff\x00").unwrap();
        let storage = Arc::new(storage);
        let client = Client::new(Box::new(TestStream { _marker: 0 }));

        for (key, expected) in [
            (
                b"binary".as_slice(),
                RespData::BulkString(Some(vec![0xff].into())),
            ),
            (
                b"missing".as_slice(),
                RespData::BulkString(Some(Vec::new().into())),
            ),
        ] {
            client.set_cmd_name(b"substr");
            client.set_argv(&[
                b"substr".to_vec(),
                key.to_vec(),
                b"0".to_vec(),
                b"0".to_vec(),
            ]);
            SubstrCmd::new().execute(&client, Arc::clone(&storage));
            assert_eq!(client.take_reply(), expected);
        }

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }

    #[test]
    fn substr_rejects_out_of_range_integer_arguments_before_storage_access() {
        let client = Client::new(Box::new(TestStream { _marker: 0 }));
        client.set_cmd_name(b"substr");
        client.set_argv(&[
            b"substr".to_vec(),
            b"key".to_vec(),
            b"9223372036854775808".to_vec(),
            b"0".to_vec(),
        ]);

        SubstrCmd::new().execute(&client, Arc::new(Storage::new(1, 0)));

        assert_eq!(
            client.take_reply(),
            RespData::Error("ERR value is not an integer or out of range".into())
        );
    }
}
