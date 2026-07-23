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
pub struct KeysCmd {
    meta: CmdMeta,
}

impl KeysCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "keys".to_string(),
                arity: 2, // KEYS pattern
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::KEYSPACE | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for KeysCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// KEYS pattern
    ///
    /// Returns all keys matching pattern.
    /// Warning: This command should only be used in production environments with extreme care.
    /// It may ruin performance when it is executed against large databases.
    ///
    /// # Returns
    /// * Array reply: list of keys matching pattern
    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();

        match storage.keys(&argv[1]) {
            Ok(keys) => {
                let resp_keys: Vec<RespData> = keys
                    .into_iter()
                    .map(|key| RespData::BulkString(Some(key.into())))
                    .collect();
                client.set_reply(RespData::Array(Some(resp_keys)));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use client::{Client, StreamTrait};
    use resp::RespData;
    use storage::{StorageOptions, safe_cleanup_test_db, storage::Storage, unique_test_db_path};

    use super::KeysCmd;
    use crate::Cmd;

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

    #[tokio::test]
    async fn keys_command_preserves_binary_keys_and_matches_bytes() {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), &db_path)
            .unwrap();
        let utf8_key = "é".as_bytes().to_vec();
        let invalid_key_a = vec![0xff];
        let invalid_key_b = vec![0xfe];
        storage.set(&utf8_key, b"utf8").unwrap();
        storage.set(&invalid_key_a, b"invalid-a").unwrap();
        storage.set(&invalid_key_b, b"invalid-b").unwrap();
        let storage = Arc::new(storage);
        let client = Client::new(Box::new(TestStream));
        let command = KeysCmd::new();

        client.set_argv(&[b"keys".to_vec(), b"?".to_vec()]);
        command.do_cmd(&client, Arc::clone(&storage));
        let RespData::Array(Some(one_byte_keys)) = client.take_reply() else {
            panic!("KEYS should return an array");
        };
        let mut one_byte_keys = one_byte_keys
            .into_iter()
            .map(|reply| match reply {
                RespData::BulkString(Some(key)) => key.to_vec(),
                other => panic!("unexpected KEYS element: {other:?}"),
            })
            .collect::<Vec<_>>();
        one_byte_keys.sort();
        assert_eq!(one_byte_keys, vec![invalid_key_b, invalid_key_a]);

        client.set_argv(&[b"keys".to_vec(), b"??".to_vec()]);
        command.do_cmd(&client, Arc::clone(&storage));
        assert_eq!(
            client.take_reply(),
            RespData::Array(Some(vec![RespData::BulkString(Some(utf8_key.into()))]))
        );

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }
}
