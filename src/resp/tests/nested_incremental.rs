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

use resp::{RespData, RespVersion, new_decoder};

#[test]
fn nested_array_incremental() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: outer array with incomplete inner array
    decoder.push("*2\r\n*1\r\n".into());
    let result1 = decoder.next();
    assert!(result1.is_none(), "Should need more data for nested array");

    // Second chunk: complete inner array + second element
    decoder.push("+foo\r\n+bar\r\n".into());
    let result2 = decoder.next();

    match result2 {
        Some(Ok(RespData::Array(Some(items)))) => {
            assert_eq!(items.len(), 2, "Outer array should have 2 elements");
            match &items[0] {
                RespData::Array(Some(inner)) => {
                    assert_eq!(inner.len(), 1);
                    match &inner[0] {
                        RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"foo"),
                        _ => panic!("Inner array should contain SimpleString 'foo'"),
                    }
                }
                _ => panic!("First element should be an array"),
            }
            match &items[1] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"bar"),
                _ => panic!("Second element should be SimpleString 'bar'"),
            }
        }
        other => panic!("Expected nested array, got {:?}", other),
    }
}

#[test]
fn deeply_nested_arrays() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: deeply nested arrays
    decoder.push("*2\r\n*1\r\n*1\r\n".into());
    let result1 = decoder.next();
    assert!(
        result1.is_none(),
        "Should need more data for deeply nested arrays"
    );

    // Second chunk: complete the deepest level
    decoder.push("+deep\r\n".into());
    let result2 = decoder.next();
    println!("After second chunk: {:?}", result2);
    assert!(result2.is_none(), "Should still need more data");

    // Third chunk: complete middle level
    decoder.push("+middle\r\n".into());
    let result3 = decoder.next();
    println!("After third chunk: {:?}", result3);

    // The parsing should be complete after the third chunk
    match result3 {
        Some(Ok(RespData::Array(Some(items)))) => {
            assert_eq!(items.len(), 2);

            // First element: [["deep"]]
            match &items[0] {
                RespData::Array(Some(level1)) => {
                    assert_eq!(level1.len(), 1);
                    match &level1[0] {
                        RespData::Array(Some(level2)) => {
                            assert_eq!(level2.len(), 1);
                            match &level2[0] {
                                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"deep"),
                                _ => panic!("Deepest level should contain 'deep'"),
                            }
                        }
                        _ => panic!("Level 1 should contain an array"),
                    }
                }
                _ => panic!("First element should be an array"),
            }

            // Second element: "middle" (not "outer" as originally expected)
            match &items[1] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"middle"),
                _ => panic!("Second element should be 'middle'"),
            }
        }
        other => panic!("Expected deeply nested array, got {:?}", other),
    }
}

#[test]
fn nested_map_incremental() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: outer map with incomplete inner map
    decoder.push("%1\r\n+key1\r\n%1\r\n".into());
    let result1 = decoder.next();
    assert!(result1.is_none(), "Should need more data for nested map");

    // Second chunk: complete inner map + outer map value
    decoder.push("+inner_key\r\n+inner_value\r\n+outer_value\r\n".into());
    let result2 = decoder.next();

    match result2 {
        Some(Ok(RespData::Map(items))) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                (RespData::SimpleString(key), RespData::Map(inner_map)) => {
                    assert_eq!(key.as_ref(), b"key1");
                    assert_eq!(inner_map.len(), 1);
                    match &inner_map[0] {
                        (RespData::SimpleString(ik), RespData::SimpleString(iv)) => {
                            assert_eq!(ik.as_ref(), b"inner_key");
                            assert_eq!(iv.as_ref(), b"inner_value");
                        }
                        _ => panic!("Inner map should contain key-value pair"),
                    }
                }
                _ => panic!("Outer map should contain key1 -> inner map"),
            }
        }
        other => panic!("Expected nested map, got {:?}", other),
    }
}

#[test]
fn mixed_nested_collections() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: array containing map containing set
    decoder.push("*1\r\n%1\r\n+map_key\r\n~1\r\n".into());
    let result1 = decoder.next();
    assert!(
        result1.is_none(),
        "Should need more data for mixed nested collections"
    );

    // Second chunk: complete the set + map value
    decoder.push("+set_item\r\n+map_value\r\n".into());
    let result2 = decoder.next();

    match result2 {
        Some(Ok(RespData::Array(Some(items)))) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                RespData::Map(map_items) => {
                    assert_eq!(map_items.len(), 1);
                    match &map_items[0] {
                        (RespData::SimpleString(key), RespData::Set(set_items)) => {
                            assert_eq!(key.as_ref(), b"map_key");
                            assert_eq!(set_items.len(), 1);
                            match &set_items[0] {
                                RespData::SimpleString(si) => {
                                    assert_eq!(si.as_ref(), b"set_item");
                                }
                                _ => panic!("Set should contain 'set_item'"),
                            }
                        }
                        _ => panic!("Map should contain map_key -> set"),
                    }
                }
                _ => panic!("Array should contain a map"),
            }
        }
        other => panic!("Expected mixed nested collections, got {:?}", other),
    }
}

#[test]
fn multiple_nested_messages() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // First chunk: two nested messages
    decoder.push("*1\r\n*1\r\n+first\r\n*1\r\n".into());
    let result1 = decoder.next();
    println!("After first chunk: {:?}", result1);
    assert!(result1.is_some(), "Should have first complete message");

    // Second chunk: complete second message
    decoder.push("+second\r\n".into());
    let result2 = decoder.next();
    assert!(result2.is_some(), "Should have second complete message");

    // Verify first message
    match result1.unwrap() {
        Ok(RespData::Array(Some(items))) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                RespData::Array(Some(inner)) => {
                    assert_eq!(inner.len(), 1);
                    match &inner[0] {
                        RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"first"),
                        _ => panic!("First message should contain 'first'"),
                    }
                }
                _ => panic!("First message should be nested array"),
            }
        }
        other => panic!("Expected first message to be array, got {:?}", other),
    }

    // Verify second message
    match result2.unwrap() {
        Ok(RespData::Array(Some(items))) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"second"),
                _ => panic!("Second message should contain 'second'"),
            }
        }
        other => panic!("Expected second message to be array, got {:?}", other),
    }
}
