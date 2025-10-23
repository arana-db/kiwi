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

use resp::{RespData, RespVersion, decode_many, encode_many, new_decoder, new_encoder};

#[test]
fn map_with_standard_resp_types() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Map(vec![
        (RespData::SimpleString("key1".into()), RespData::Integer(42)),
        (
            RespData::BulkString(Some("key2".into())),
            RespData::SimpleString("value2".into()),
        ),
    ]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);

    assert_eq!(out.len(), 1, "Expected 1 result");
    match out[0].as_ref().unwrap() {
        RespData::Map(entries) => {
            assert_eq!(entries.len(), 2, "Expected 2 map entries");
            // Verify first entry
            match &entries[0] {
                (RespData::SimpleString(k), RespData::Integer(v)) => {
                    assert_eq!(k.as_ref(), b"key1");
                    assert_eq!(*v, 42);
                }
                _ => panic!("Expected (SimpleString, Integer) for first entry"),
            }
            // Verify second entry
            match &entries[1] {
                (RespData::BulkString(Some(k)), RespData::SimpleString(v)) => {
                    assert_eq!(k.as_ref(), b"key2");
                    assert_eq!(v.as_ref(), b"value2");
                }
                _ => panic!("Expected (BulkString, SimpleString) for second entry"),
            }
        }
        other => panic!("Expected Map, got {:?}", other),
    }
}

#[test]
fn set_with_standard_resp_types() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Set(vec![
        RespData::SimpleString("item1".into()),
        RespData::Integer(123),
        RespData::BulkString(Some("item3".into())),
    ]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);

    assert_eq!(out.len(), 1, "Expected 1 result");
    match out[0].as_ref().unwrap() {
        RespData::Set(items) => {
            assert_eq!(items.len(), 3, "Expected 3 set items");
            // Check that all expected types are present
            let mut found_simple = false;
            let mut found_integer = false;
            let mut found_bulk = false;

            for item in items {
                match item {
                    RespData::SimpleString(s) if s.as_ref() == b"item1" => found_simple = true,
                    RespData::Integer(i) if *i == 123 => found_integer = true,
                    RespData::BulkString(Some(s)) if s.as_ref() == b"item3" => found_bulk = true,
                    _ => {}
                }
            }

            assert!(found_simple, "Expected SimpleString 'item1' in set");
            assert!(found_integer, "Expected Integer 123 in set");
            assert!(found_bulk, "Expected BulkString 'item3' in set");
        }
        other => panic!("Expected Set, got {:?}", other),
    }
}

#[test]
fn push_with_standard_resp_types() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Push(vec![
        RespData::SimpleString("PUSH".into()),
        RespData::Integer(1),
        RespData::BulkString(Some("message".into())),
    ]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);

    assert_eq!(out.len(), 1, "Expected 1 result");
    match out[0].as_ref().unwrap() {
        RespData::Push(items) => {
            assert_eq!(items.len(), 3, "Expected 3 push items");
            // Verify each item
            match &items[0] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"PUSH"),
                _ => panic!("Expected SimpleString 'PUSH' as first item"),
            }
            match &items[1] {
                RespData::Integer(i) => assert_eq!(*i, 1),
                _ => panic!("Expected Integer 1 as second item"),
            }
            match &items[2] {
                RespData::BulkString(Some(s)) => assert_eq!(s.as_ref(), b"message"),
                _ => panic!("Expected BulkString 'message' as third item"),
            }
        }
        other => panic!("Expected Push, got {:?}", other),
    }
}

#[test]
fn map_with_mixed_types() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Map(vec![
        (
            RespData::SimpleString("key".into()),
            RespData::Boolean(true),
        ),
        (
            RespData::Integer(1),
            RespData::BulkString(Some("value".into())),
        ),
        (RespData::Null, RespData::Double(3.14)),
    ]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);

    assert_eq!(out.len(), 1, "Expected 1 result");
    match out[0].as_ref().unwrap() {
        RespData::Map(entries) => {
            assert_eq!(entries.len(), 3, "Expected 3 map entries");
        }
        other => panic!("Expected Map, got {:?}", other),
    }
}
