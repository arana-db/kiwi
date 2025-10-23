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

use bytes::Bytes;
use resp::{RespData, RespVersion, decode_many, new_decoder, new_encoder};

#[test]
fn resp3_decodes_standard_resp_types() {
    // Test simple string
    let mut dec1 = new_decoder(RespVersion::RESP3);
    let out1 = decode_many(&mut *dec1, Bytes::from("+OK\r\n"));
    assert_eq!(out1.len(), 1);
    match out1[0].as_ref().unwrap() {
        RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"OK"),
        other => panic!("Expected SimpleString, got {:?}", other),
    }

    // Test integer
    let mut dec2 = new_decoder(RespVersion::RESP3);
    let out2 = decode_many(&mut *dec2, Bytes::from(":42\r\n"));
    assert_eq!(out2.len(), 1);
    match out2[0].as_ref().unwrap() {
        RespData::Integer(n) => assert_eq!(*n, 42),
        other => panic!("Expected Integer, got {:?}", other),
    }

    // Test bulk string
    let mut dec3 = new_decoder(RespVersion::RESP3);
    let out3 = decode_many(&mut *dec3, Bytes::from("$5\r\nhello\r\n"));
    assert_eq!(out3.len(), 1);
    match out3[0].as_ref().unwrap() {
        RespData::BulkString(Some(s)) => assert_eq!(s.as_ref(), b"hello"),
        other => panic!("Expected BulkString, got {:?}", other),
    }

    // Test array
    let mut dec4 = new_decoder(RespVersion::RESP3);
    let out4 = decode_many(&mut *dec4, Bytes::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"));
    assert_eq!(out4.len(), 1);
    match out4[0].as_ref().unwrap() {
        RespData::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                RespData::BulkString(Some(s)) => assert_eq!(s.as_ref(), b"foo"),
                other => panic!("Expected BulkString 'foo', got {:?}", other),
            }
            match &items[1] {
                RespData::BulkString(Some(s)) => assert_eq!(s.as_ref(), b"bar"),
                other => panic!("Expected BulkString 'bar', got {:?}", other),
            }
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

#[test]
fn resp3_encodes_standard_resp_types() {
    let mut enc = new_encoder(RespVersion::RESP3);

    // Test simple string
    let data1 = RespData::SimpleString("OK".into());
    let bytes1 = enc.encode_one(&data1).unwrap();
    assert_eq!(bytes1.as_ref(), b"+OK\r\n");

    // Test integer
    let data2 = RespData::Integer(42);
    let bytes2 = enc.encode_one(&data2).unwrap();
    assert_eq!(bytes2.as_ref(), b":42\r\n");

    // Test bulk string
    let data3 = RespData::BulkString(Some("hello".into()));
    let bytes3 = enc.encode_one(&data3).unwrap();
    assert_eq!(bytes3.as_ref(), b"$5\r\nhello\r\n");

    // Test array
    let data4 = RespData::Array(Some(vec![
        RespData::BulkString(Some("foo".into())),
        RespData::BulkString(Some("bar".into())),
    ]));
    let bytes4 = enc.encode_one(&data4).unwrap();
    assert_eq!(bytes4.as_ref(), b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
}

#[test]
fn resp3_mixed_resp2_and_resp3_types() {
    let mut dec = new_decoder(RespVersion::RESP3);

    // Mix RESP2 and RESP3 types in one input
    let input = Bytes::from("+OK\r\n#t\r\n:42\r\n_\r\n");
    let out = decode_many(&mut *dec, input);

    assert_eq!(out.len(), 4);

    // Simple string
    match out[0].as_ref().unwrap() {
        RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"OK"),
        other => panic!("Expected SimpleString, got {:?}", other),
    }

    // Boolean
    match out[1].as_ref().unwrap() {
        RespData::Boolean(true) => {}
        other => panic!("Expected Boolean(true), got {:?}", other),
    }

    // Integer
    match out[2].as_ref().unwrap() {
        RespData::Integer(n) => assert_eq!(*n, 42),
        other => panic!("Expected Integer, got {:?}", other),
    }

    // Null
    match out[3].as_ref().unwrap() {
        RespData::Null => {}
        other => panic!("Expected Null, got {:?}", other),
    }
}
