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

use async_trait::async_trait;
use bytes::Bytes;
use client::{Client, StreamTrait};
use resp::RespData;
use storage::{StorageOptions, safe_cleanup_test_db, storage::Storage, unique_test_db_path};

struct TestStream;

#[async_trait]
impl StreamTrait for TestStream {
    async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
        Ok(0)
    }

    async fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        Ok(data.len())
    }
}

fn f32_bytes(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn open_storage() -> (Arc<Storage>, std::path::PathBuf) {
    let db_path = unique_test_db_path();
    safe_cleanup_test_db(&db_path);

    let mut storage = Storage::new(1, 0);
    let _receiver = storage
        .open(Arc::new(StorageOptions::default()), &db_path)
        .unwrap();

    (Arc::new(storage), db_path)
}

fn execute_command(storage: Arc<Storage>, args: Vec<Vec<u8>>) -> RespData {
    let table = cmd::table::create_command_table(Arc::new(|| None));
    let cmd_name = String::from_utf8_lossy(&args[0]).to_lowercase();
    let command = table
        .get(&cmd_name)
        .unwrap_or_else(|| panic!("missing command {cmd_name}"));

    let client = Client::new(Box::new(TestStream));
    client.set_authenticated(true);
    client.set_cmd_name(&args[0]);
    client.set_argv(&args);

    command.execute(&client, storage);
    client.take_reply()
}

#[test]
fn command_table_registers_ft_commands() {
    let table = cmd::table::create_command_table(Arc::new(|| None));

    assert!(table.contains_key("ft.create"));
    assert!(table.contains_key("ft.search"));
}

#[tokio::test]
async fn ft_create_hset_ft_search_knn_round_trip() {
    let (storage, db_path) = open_storage();

    let create_reply = execute_command(
        storage.clone(),
        vec![
            b"FT.CREATE".to_vec(),
            b"idx".to_vec(),
            b"ON".to_vec(),
            b"HASH".to_vec(),
            b"PREFIX".to_vec(),
            b"1".to_vec(),
            b"doc:".to_vec(),
            b"SCHEMA".to_vec(),
            b"emb".to_vec(),
            b"VECTOR".to_vec(),
            b"FLAT".to_vec(),
            b"6".to_vec(),
            b"TYPE".to_vec(),
            b"FLOAT32".to_vec(),
            b"DIM".to_vec(),
            b"3".to_vec(),
            b"DISTANCE_METRIC".to_vec(),
            b"L2".to_vec(),
        ],
    );
    assert_eq!(
        create_reply,
        RespData::SimpleString(Bytes::from_static(b"OK"))
    );

    assert_eq!(
        execute_command(
            storage.clone(),
            vec![
                b"HSET".to_vec(),
                b"doc:1".to_vec(),
                b"emb".to_vec(),
                f32_bytes(&[1.0, 0.0, 0.0]),
                b"name".to_vec(),
                b"a".to_vec(),
            ],
        ),
        RespData::Integer(2)
    );
    assert_eq!(
        execute_command(
            storage.clone(),
            vec![
                b"HSET".to_vec(),
                b"doc:2".to_vec(),
                b"emb".to_vec(),
                f32_bytes(&[0.0, 1.0, 0.0]),
                b"name".to_vec(),
                b"b".to_vec(),
            ],
        ),
        RespData::Integer(2)
    );

    let search_reply = execute_command(
        storage.clone(),
        vec![
            b"FT.SEARCH".to_vec(),
            b"idx".to_vec(),
            b"*=>[KNN 2 @emb $q]".to_vec(),
            b"PARAMS".to_vec(),
            b"2".to_vec(),
            b"q".to_vec(),
            f32_bytes(&[1.0, 0.0, 0.0]),
            b"RETURN".to_vec(),
            b"1".to_vec(),
            b"name".to_vec(),
            b"LIMIT".to_vec(),
            b"0".to_vec(),
            b"2".to_vec(),
        ],
    );

    assert_eq!(
        search_reply,
        RespData::Array(Some(vec![
            RespData::Integer(2),
            RespData::BulkString(Some(Bytes::from_static(b"doc:1"))),
            RespData::Array(Some(vec![
                RespData::BulkString(Some(Bytes::from_static(b"name"))),
                RespData::BulkString(Some(Bytes::from_static(b"a"))),
            ])),
            RespData::BulkString(Some(Bytes::from_static(b"doc:2"))),
            RespData::Array(Some(vec![
                RespData::BulkString(Some(Bytes::from_static(b"name"))),
                RespData::BulkString(Some(Bytes::from_static(b"b"))),
            ])),
        ]))
    );

    safe_cleanup_test_db(&db_path);
}

#[tokio::test]
async fn ft_search_supports_nocontent() {
    let (storage, db_path) = open_storage();

    let _ = execute_command(
        storage.clone(),
        vec![
            b"FT.CREATE".to_vec(),
            b"idx".to_vec(),
            b"ON".to_vec(),
            b"HASH".to_vec(),
            b"PREFIX".to_vec(),
            b"1".to_vec(),
            b"doc:".to_vec(),
            b"SCHEMA".to_vec(),
            b"emb".to_vec(),
            b"VECTOR".to_vec(),
            b"FLAT".to_vec(),
            b"6".to_vec(),
            b"TYPE".to_vec(),
            b"FLOAT32".to_vec(),
            b"DIM".to_vec(),
            b"3".to_vec(),
            b"DISTANCE_METRIC".to_vec(),
            b"L2".to_vec(),
        ],
    );
    let _ = execute_command(
        storage.clone(),
        vec![
            b"HSET".to_vec(),
            b"doc:1".to_vec(),
            b"emb".to_vec(),
            f32_bytes(&[1.0, 0.0, 0.0]),
        ],
    );

    let search_reply = execute_command(
        storage,
        vec![
            b"FT.SEARCH".to_vec(),
            b"idx".to_vec(),
            b"*=>[KNN 1 @emb $q]".to_vec(),
            b"PARAMS".to_vec(),
            b"2".to_vec(),
            b"q".to_vec(),
            f32_bytes(&[1.0, 0.0, 0.0]),
            b"NOCONTENT".to_vec(),
        ],
    );

    assert_eq!(
        search_reply,
        RespData::Array(Some(vec![
            RespData::Integer(1),
            RespData::BulkString(Some(Bytes::from_static(b"doc:1"))),
        ]))
    );

    safe_cleanup_test_db(&db_path);
}
