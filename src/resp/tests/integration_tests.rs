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
use resp::{
    encode::{RespEncode, RespEncoder},
    negotiation::ProtocolNegotiator,
    parse::{Parse, RespParse, RespParseResult},
    types::{RespData, RespVersion},
    command::{CommandType, RespCommand},
};

#[test]
fn test_resp3_round_trip_null() {
    let original_data = RespData::Null;
    
    // Encode
    let mut encoder = RespEncoder::new(RespVersion::RESP3);
    encoder.encode_resp_data(&original_data);
    let encoded = encoder.get_response();
    
    // Parse
    let mut parser = RespParse::new(RespVersion::RESP3);
    let result = parser.parse(encoded);
    
    if let RespParseResult::Complete(parsed_data) = result {
        assert_eq!(parsed_data, original_data);
    } else {
        panic!("Failed to parse encoded data");
    }
}

#[test]
fn test_resp3_round_trip_boolean() {
    for &bool_val in &[true, false] {
        let original_data = RespData::Boolean(bool_val);
        
        // Encode
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&original_data);
        let encoded = encoder.get_response();
        
        // Parse
        let mut parser = RespParse::new(RespVersion::RESP3);
        let result = parser.parse(encoded);
        
        if let RespParseResult::Complete(parsed_data) = result {
            assert_eq!(parsed_data, original_data);
        } else {
            panic!("Failed to parse encoded boolean: {}", bool_val);
        }
    }
}

#[test]
fn test_resp3_round_trip_double() {
    let test_values = vec![3.14159, -2.5, 0.0, 1.23e-4, 1.23e10];
    
    for &double_val in &test_values {
        let original_data = RespData::Double(double_val);
        
        // Encode
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&original_data);
        let encoded = encoder.get_response();
        
        // Parse
        let mut parser = RespParse::new(RespVersion::RESP3);
        let result = parser.parse(encoded);
        
        if let RespParseResult::Complete(parsed_data) = result {
            assert_eq!(parsed_data, original_data);
        } else {
            panic!("Failed to parse encoded double: {}", double_val);
        }
    }
}

#[test]
fn test_resp3_round_trip_map() {
    let original_data = RespData::Map(vec![
        (RespData::SimpleString(Bytes::from("key1")), RespData::Integer(42)),
        (RespData::SimpleString(Bytes::from("key2")), RespData::Boolean(true)),
        (RespData::SimpleString(Bytes::from("key3")), RespData::Double(3.14)),
    ]);
    
    // Encode
    let mut encoder = RespEncoder::new(RespVersion::RESP3);
    encoder.encode_resp_data(&original_data);
    let encoded = encoder.get_response();
    
    // Parse
    let mut parser = RespParse::new(RespVersion::RESP3);
    let result = parser.parse(encoded);
    
    if let RespParseResult::Complete(parsed_data) = result {
        assert_eq!(parsed_data, original_data);
    } else {
        panic!("Failed to parse encoded map");
    }
}

#[test]
fn test_resp3_round_trip_set() {
    let original_data = RespData::Set(vec![
        RespData::SimpleString(Bytes::from("apple")),
        RespData::SimpleString(Bytes::from("banana")),
        RespData::Integer(123),
        RespData::Boolean(false),
    ]);
    
    // Encode
    let mut encoder = RespEncoder::new(RespVersion::RESP3);
    encoder.encode_resp_data(&original_data);
    let encoded = encoder.get_response();
    
    // Parse
    let mut parser = RespParse::new(RespVersion::RESP3);
    let result = parser.parse(encoded);
    
    if let RespParseResult::Complete(parsed_data) = result {
        assert_eq!(parsed_data, original_data);
    } else {
        panic!("Failed to parse encoded set");
    }
}

#[test]
fn test_resp3_round_trip_verbatim_string() {
    let original_data = RespData::VerbatimString {
        format: Bytes::from("txt"),
        data: Bytes::from("Hello, RESP3 World!"),
    };
    
    // Encode
    let mut encoder = RespEncoder::new(RespVersion::RESP3);
    encoder.encode_resp_data(&original_data);
    let encoded = encoder.get_response();
    
    // Parse
    let mut parser = RespParse::new(RespVersion::RESP3);
    let result = parser.parse(encoded);
    
    if let RespParseResult::Complete(parsed_data) = result {
        assert_eq!(parsed_data, original_data);
    } else {
        panic!("Failed to parse encoded verbatim string");
    }
}

#[test]
fn test_resp3_backward_compatibility() {
    // Test that RESP2 data works correctly with RESP3 parser/encoder
    let resp2_data = vec![
        RespData::SimpleString(Bytes::from("OK")),
        RespData::Error(Bytes::from("ERR something went wrong")),
        RespData::Integer(42),
        RespData::BulkString(Some(Bytes::from("hello world"))),
        RespData::BulkString(None),
        RespData::Array(Some(vec![
            RespData::SimpleString(Bytes::from("PING")),
            RespData::BulkString(Some(Bytes::from("test"))),
        ])),
    ];
    
    for original_data in resp2_data {
        // Encode with RESP3 encoder
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&original_data);
        let encoded = encoder.get_response();
        
        // Parse with RESP3 parser
        let mut parser = RespParse::new(RespVersion::RESP3);
        let result = parser.parse(encoded);
        
        if let RespParseResult::Complete(parsed_data) = result {
            assert_eq!(parsed_data, original_data);
        } else {
            panic!("Failed backward compatibility test for: {:?}", original_data);
        }
    }
}

#[test]
fn test_protocol_negotiation_resp2() {
    let mut negotiator = ProtocolNegotiator::new();
    let command = RespCommand::new(
        CommandType::Hello,
        vec![Bytes::from("2")],
        false,
    );
    
    let response = negotiator.handle_hello(&command).unwrap();
    assert_eq!(negotiator.current_version(), RespVersion::RESP2);
    
    // Should return array format for RESP2
    match response {
        RespData::Array(Some(items)) => {
            assert!(items.len() >= 12); // At least basic server info
            // Check that proto field is 2
            let proto_index = items.iter().position(|item| {
                item.as_string() == Some("proto".to_string())
            }).expect("proto field not found");
            assert_eq!(items[proto_index + 1], RespData::Integer(2));
        }
        _ => panic!("Expected array response for RESP2 HELLO"),
    }
}

#[test]
fn test_protocol_negotiation_resp3() {
    let mut negotiator = ProtocolNegotiator::new();
    let command = RespCommand::new(
        CommandType::Hello,
        vec![Bytes::from("3")],
        false,
    );
    
    let response = negotiator.handle_hello(&command).unwrap();
    assert_eq!(negotiator.current_version(), RespVersion::RESP3);
    
    // Should return map format for RESP3
    match response {
        RespData::Map(pairs) => {
            assert!(pairs.len() >= 6); // At least basic server info
            // Check that proto field is 3
            let proto_pair = pairs.iter().find(|(key, _)| {
                key.as_string() == Some("proto".to_string())
            }).expect("proto field not found");
            assert_eq!(proto_pair.1, RespData::Integer(3));
        }
        _ => panic!("Expected map response for RESP3 HELLO"),
    }
}

#[test]
fn test_protocol_negotiation_with_auth() {
    let mut negotiator = ProtocolNegotiator::new();
    let command = RespCommand::new(
        CommandType::Hello,
        vec![
            Bytes::from("3"),
            Bytes::from("AUTH"),
            Bytes::from("username"),
            Bytes::from("password"),
        ],
        false,
    );
    
    let _response = negotiator.handle_hello(&command).unwrap();
    assert_eq!(negotiator.current_version(), RespVersion::RESP3);
    assert!(negotiator.client_capabilities().contains_key("auth"));
}

#[test]
fn test_protocol_negotiation_with_setname() {
    let mut negotiator = ProtocolNegotiator::new();
    let command = RespCommand::new(
        CommandType::Hello,
        vec![
            Bytes::from("2"),
            Bytes::from("SETNAME"),
            Bytes::from("my-client"),
        ],
        false,
    );
    
    let _response = negotiator.handle_hello(&command).unwrap();
    assert_eq!(negotiator.current_version(), RespVersion::RESP2);
    assert_eq!(
        negotiator.client_capabilities().get("client_name"),
        Some(&"my-client".to_string())
    );
}

#[test]
fn test_resp3_to_resp2_conversion() {
    let negotiator = ProtocolNegotiator::new();
    
    // Test various RESP3 types conversion to RESP2
    let test_cases = vec![
        (RespData::Null, RespData::BulkString(None)),
        (RespData::Boolean(true), RespData::Integer(1)),
        (RespData::Boolean(false), RespData::Integer(0)),
        (RespData::Double(3.14), RespData::BulkString(Some(Bytes::from("3.14")))),
        (RespData::BigNumber(Bytes::from("999")), RespData::BulkString(Some(Bytes::from("999")))),
        (RespData::BulkError(Bytes::from("ERR")), RespData::Error(Bytes::from("ERR"))),
        (
            RespData::VerbatimString {
                format: Bytes::from("txt"),
                data: Bytes::from("hello"),
            },
            RespData::BulkString(Some(Bytes::from("hello"))),
        ),
    ];
    
    for (resp3_data, expected_resp2) in test_cases {
        let converted = negotiator.convert_to_resp2(&resp3_data);
        assert_eq!(converted, expected_resp2, "Failed to convert {:?}", resp3_data);
    }
}

#[test]
fn test_resp3_map_to_resp2_array_conversion() {
    let negotiator = ProtocolNegotiator::new();
    
    let map_data = RespData::Map(vec![
        (RespData::SimpleString(Bytes::from("key1")), RespData::Integer(1)),
        (RespData::SimpleString(Bytes::from("key2")), RespData::Boolean(true)),
    ]);
    
    let converted = negotiator.convert_to_resp2(&map_data);
    
    match converted {
        RespData::Array(Some(items)) => {
            assert_eq!(items.len(), 4); // 2 key-value pairs = 4 items
            assert_eq!(items[0], RespData::SimpleString(Bytes::from("key1")));
            assert_eq!(items[1], RespData::Integer(1));
            assert_eq!(items[2], RespData::SimpleString(Bytes::from("key2")));
            assert_eq!(items[3], RespData::Integer(1)); // Boolean true -> Integer 1
        }
        _ => panic!("Expected array conversion for map"),
    }
}

#[test]
fn test_feature_support_detection() {
    let mut negotiator = ProtocolNegotiator::new();
    
    // Initially RESP2 - no RESP3 features
    assert!(!negotiator.supports_feature("maps"));
    assert!(!negotiator.supports_feature("sets"));
    assert!(!negotiator.supports_feature("booleans"));
    
    // Switch to RESP3
    let command = RespCommand::new(
        CommandType::Hello,
        vec![Bytes::from("3")],
        false,
    );
    negotiator.handle_hello(&command).unwrap();
    
    // Now supports RESP3 features
    assert!(negotiator.supports_feature("maps"));
    assert!(negotiator.supports_feature("sets"));
    assert!(negotiator.supports_feature("booleans"));
    assert!(negotiator.supports_feature("doubles"));
    assert!(negotiator.supports_feature("big_numbers"));
    assert!(negotiator.supports_feature("verbatim_strings"));
    assert!(negotiator.supports_feature("push_messages"));
    
    // Unknown features should return false
    assert!(!negotiator.supports_feature("unknown_feature"));
}

#[test]
fn test_auto_version_detection() {
    let mut parser = RespParse::new(RespVersion::RESP2);
    
    // Start with RESP2
    assert_eq!(parser.version(), RespVersion::RESP2);
    
    // Parse RESP2 data - should stay RESP2
    let result = parser.parse(Bytes::from("+OK\r\n"));
    assert_eq!(parser.version(), RespVersion::RESP2);
    assert!(matches!(result, RespParseResult::Complete(_)));
    
    // Reset and parse RESP3 data - should auto-detect and switch
    parser.reset();
    let result = parser.parse(Bytes::from("#t\r\n"));
    assert_eq!(parser.version(), RespVersion::RESP3);
    assert!(matches!(result, RespParseResult::Complete(RespData::Boolean(true))));
}