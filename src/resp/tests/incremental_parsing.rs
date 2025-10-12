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
use resp::{RespData, RespVersion, new_decoder};

#[test]
fn incremental_array_parsing() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: "*2\r\n+foo\r\n"
    decoder.push("*2\r\n+foo\r\n".into());
    let result = decoder.next();
    assert!(result.is_none(), "Should need more data for complete array");

    // Second chunk: "+bar\r\n"
    decoder.push("+bar\r\n".into());
    let result = decoder.next();
    assert!(result.is_some(), "Should have complete array now");

    match result.unwrap() {
        Ok(RespData::Array(Some(items))) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"foo"),
                _ => panic!("Expected SimpleString 'foo'"),
            }
            match &items[1] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"bar"),
                _ => panic!("Expected SimpleString 'bar'"),
            }
        }
        other => panic!("Expected Array with 2 items, got {:?}", other),
    }
}

#[test]
fn incremental_map_parsing() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: "%1\r\n+key\r\n"
    decoder.push("%1\r\n+key\r\n".into());
    let result = decoder.next();
    assert!(result.is_none(), "Should need more data for complete map");

    // Second chunk: "+value\r\n"
    decoder.push("+value\r\n".into());
    let result = decoder.next();
    assert!(result.is_some(), "Should have complete map now");

    match result.unwrap() {
        Ok(RespData::Map(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                (RespData::SimpleString(k), RespData::SimpleString(v)) => {
                    assert_eq!(k.as_ref(), b"key");
                    assert_eq!(v.as_ref(), b"value");
                }
                _ => panic!("Expected (SimpleString, SimpleString) pair"),
            }
        }
        other => panic!("Expected Map with 1 pair, got {:?}", other),
    }
}

#[test]
fn incremental_set_parsing() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: "~2\r\n+item1\r\n"
    decoder.push("~2\r\n+item1\r\n".into());
    let result = decoder.next();
    assert!(result.is_none(), "Should need more data for complete set");

    // Second chunk: "+item2\r\n"
    decoder.push("+item2\r\n".into());
    let result = decoder.next();
    assert!(result.is_some(), "Should have complete set now");

    match result.unwrap() {
        Ok(RespData::Set(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"item1"),
                _ => panic!("Expected SimpleString 'item1'"),
            }
            match &items[1] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"item2"),
                _ => panic!("Expected SimpleString 'item2'"),
            }
        }
        other => panic!("Expected Set with 2 items, got {:?}", other),
    }
}

#[test]
fn incremental_push_parsing() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: ">2\r\n+msg1\r\n"
    decoder.push(">2\r\n+msg1\r\n".into());
    let result = decoder.next();
    assert!(result.is_none(), "Should need more data for complete push");

    // Second chunk: "+msg2\r\n"
    decoder.push("+msg2\r\n".into());
    let result = decoder.next();
    assert!(result.is_some(), "Should have complete push now");

    match result.unwrap() {
        Ok(RespData::Push(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"msg1"),
                _ => panic!("Expected SimpleString 'msg1'"),
            }
            match &items[1] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"msg2"),
                _ => panic!("Expected SimpleString 'msg2'"),
            }
        }
        other => panic!("Expected Push with 2 items, got {:?}", other),
    }
}

#[test]
fn multiple_incremental_messages() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: "*1\r\n+hello\r\n*1\r\n"
    decoder.push("*1\r\n+hello\r\n*1\r\n".into());
    let result = decoder.next();
    assert!(result.is_some(), "Should have first complete array");

    match result.unwrap() {
        Ok(RespData::Array(Some(items))) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"hello"),
                _ => panic!("Expected SimpleString 'hello'"),
            }
        }
        other => panic!("Expected Array with 1 item, got {:?}", other),
    }

    // Second chunk: "+world\r\n"
    decoder.push("+world\r\n".into());
    let result = decoder.next();
    assert!(result.is_some(), "Should have second complete array");

    match result.unwrap() {
        Ok(RespData::Array(Some(items))) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"world"),
                _ => panic!("Expected SimpleString 'world'"),
            }
        }
        other => panic!("Expected Array with 1 item, got {:?}", other),
    }
}
