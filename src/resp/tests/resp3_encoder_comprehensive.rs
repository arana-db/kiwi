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
use resp::{RespData, RespVersion, new_encoder};

#[test]
fn resp3_encoder_all_types() {
    let mut enc = new_encoder(RespVersion::RESP3);
    
    // Test all RESP3 types
    let tests = vec![
        (RespData::Null, b"_\r\n" as &[u8]),
        (RespData::Boolean(true), b"#t\r\n" as &[u8]),
        (RespData::Boolean(false), b"#f\r\n" as &[u8]),
        (RespData::Double(3.14), b",3.14\r\n" as &[u8]),
        (RespData::Double(f64::INFINITY), b",inf\r\n" as &[u8]),
        (RespData::Double(f64::NEG_INFINITY), b",-inf\r\n" as &[u8]),
        (RespData::Double(f64::NAN), b",nan\r\n" as &[u8]),
        (RespData::BulkError(Bytes::from("ERR something")), b"!13\r\nERR something\r\n" as &[u8]),
        (RespData::VerbatimString { format: *b"txt", data: Bytes::from("hello") }, b"=9\r\ntxt:hello\r\n" as &[u8]),
        (RespData::BigNumber("12345678901234567890".into()), b"(12345678901234567890\r\n" as &[u8]),
    ];
    
    for (data, expected) in tests {
        let result = enc.encode_one(&data);
        assert!(result.is_ok(), "Failed to encode {:?}", data);
        let bytes = result.unwrap();
        assert_eq!(bytes.as_ref(), expected, "Wrong encoding for {:?}", data);
    }
}

#[test]
fn resp3_encoder_standard_resp_types() {
    let mut enc = new_encoder(RespVersion::RESP3);
    
    // Test standard RESP types
    let tests = vec![
        (RespData::SimpleString("OK".into()), b"+OK\r\n" as &[u8]),
        (RespData::Error("ERR something".into()), b"-ERR something\r\n" as &[u8]),
        (RespData::Integer(42), b":42\r\n" as &[u8]),
        (RespData::BulkString(Some("hello".into())), b"$5\r\nhello\r\n" as &[u8]),
        (RespData::BulkString(None), b"$-1\r\n" as &[u8]),
        (RespData::Array(Some(vec![RespData::SimpleString("OK".into())])), b"*1\r\n+OK\r\n" as &[u8]),
        (RespData::Array(None), b"*-1\r\n" as &[u8]),
        (RespData::Inline(vec!["PING".into()]), b"PING\r\n" as &[u8]),
    ];
    
    for (data, expected) in tests {
        let result = enc.encode_one(&data);
        assert!(result.is_ok(), "Failed to encode {:?}", data);
        let bytes = result.unwrap();
        assert_eq!(bytes.as_ref(), expected, "Wrong encoding for {:?}", data);
    }
}

#[test]
fn resp3_encoder_collections() {
    let mut enc = new_encoder(RespVersion::RESP3);
    
    // Test collection types
    let set_data = RespData::Set(vec![
        RespData::Boolean(true),
        RespData::Null,
        RespData::Double(2.5),
    ]);
    let set_result = enc.encode_one(&set_data);
    assert!(set_result.is_ok());
    let set_bytes = set_result.unwrap();
    assert!(set_bytes.starts_with(b"~3\r\n"));
    
    let map_data = RespData::Map(vec![
        (RespData::Boolean(true), RespData::Double(1.0)),
        (RespData::Null, RespData::BulkError(Bytes::from("ERR x"))),
    ]);
    let map_result = enc.encode_one(&map_data);
    assert!(map_result.is_ok());
    let map_bytes = map_result.unwrap();
    assert!(map_bytes.starts_with(b"%2\r\n"));
    
    let push_data = RespData::Push(vec![
        RespData::Boolean(false),
        RespData::Double(3.14),
    ]);
    let push_result = enc.encode_one(&push_data);
    assert!(push_result.is_ok());
    let push_bytes = push_result.unwrap();
    assert!(push_bytes.starts_with(b">2\r\n"));
}
