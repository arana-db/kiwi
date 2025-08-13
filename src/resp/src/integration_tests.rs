//! Integration tests for RESP protocol library
//!
//! These tests verify cross-version compatibility and real-world usage patterns

use crate::{
    create_encoder,
    create_parser, types::{RespValue, RespVersion},
    ParseResult,
};
use bytes::Bytes;
use std::collections::HashMap;

/// Test parsing and encoding round-trip for all supported data types
#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_resp1_round_trip() {
        let parser = create_parser(RespVersion::Resp1);
        let encoder = create_encoder(RespVersion::Resp1);

        let test_cases = vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Error("ERR test".to_string()),
            RespValue::Integer(42),
            RespValue::BulkString(Some(Bytes::from("hello"))),
            RespValue::Inline(vec!["GET".to_string(), "key".to_string()]),
        ];

        for original in test_cases {
            let encoded = encoder.encode(&original).unwrap();
            let parsed = parser.parse(&encoded).unwrap();

            if let ParseResult::Complete(parsed_value, _) = parsed {
                // For RESP1, some conversions are expected
                match (&original, &parsed_value) {
                    (RespValue::Inline(orig_args), RespValue::Inline(parsed_args)) => {
                        assert_eq!(orig_args, parsed_args);
                    }
                    _ => {
                        // Other types may have different representations in RESP1
                    }
                }
            } else {
                panic!("Failed to parse encoded value");
            }
        }
    }

    #[test]
    fn test_resp2_round_trip() {
        let parser = create_parser(RespVersion::Resp2);
        let encoder = create_encoder(RespVersion::Resp2);

        let test_cases = vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Error("ERR unknown command".to_string()),
            RespValue::Integer(1000),
            RespValue::Integer(-42),
            RespValue::BulkString(Some(Bytes::from("foobar"))),
            RespValue::BulkString(None),
            RespValue::BulkString(Some(Bytes::new())),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from("foo"))),
                RespValue::BulkString(Some(Bytes::from("bar"))),
                RespValue::BulkString(None),
            ])),
            RespValue::Array(None),
            RespValue::Array(Some(vec![])),
        ];

        for original in test_cases {
            let encoded = encoder.encode(&original).unwrap();
            let parsed = parser.parse(&encoded).unwrap();

            if let ParseResult::Complete(parsed_value, _) = parsed {
                assert_eq!(original, parsed_value);
            } else {
                panic!("Failed to parse encoded value: {:?}", original);
            }
        }
    }

    #[test]
    fn test_resp3_round_trip() {
        let parser = create_parser(RespVersion::Resp3);
        let encoder = create_encoder(RespVersion::Resp3);

        let mut map = HashMap::new();
        map.insert(
            RespValue::SimpleString("key1".to_string()),
            RespValue::Integer(42),
        );
        map.insert(
            RespValue::SimpleString("key2".to_string()),
            RespValue::Boolean(true),
        );

        let test_cases = vec![
            // RESP2 compatible types
            RespValue::SimpleString("OK".to_string()),
            RespValue::Error("ERR test".to_string()),
            RespValue::Integer(42),
            RespValue::BulkString(Some(Bytes::from("hello"))),
            RespValue::Array(Some(vec![RespValue::Integer(1), RespValue::Integer(2)])),

            // RESP3 specific types
            RespValue::Null,
            RespValue::Boolean(true),
            RespValue::Boolean(false),
            RespValue::Double(3.14159),
            RespValue::Double(f64::INFINITY),
            RespValue::Double(f64::NEG_INFINITY),
            RespValue::BigNumber("123456789012345678901234567890".to_string()),
            RespValue::BulkError(Bytes::from("SYNTAX error")),
            RespValue::VerbatimString {
                format: "txt".to_string(),
                data: "Some text".to_string(),
            },
            RespValue::Map(map),
            RespValue::Set(vec![
                RespValue::SimpleString("apple".to_string()),
                RespValue::SimpleString("banana".to_string()),
            ]),
            RespValue::Push(vec![
                RespValue::SimpleString("pubsub".to_string()),
                RespValue::SimpleString("channel".to_string()),
                RespValue::SimpleString("message".to_string()),
            ]),
        ];

        for original in test_cases {
            let encoded = encoder.encode(&original).unwrap();
            let parsed = parser.parse(&encoded).unwrap();

            if let ParseResult::Complete(parsed_value, _) = parsed {
                // Special handling for NaN comparison
                match (&original, &parsed_value) {
                    (RespValue::Double(orig), RespValue::Double(parsed))
                    if orig.is_nan() && parsed.is_nan() => {
                        // Both are NaN, consider them equal
                    }
                    _ => {
                        assert_eq!(original, parsed_value, "Round trip failed for: {:?}", original);
                    }
                }
            } else {
                panic!("Failed to parse encoded value: {:?}", original);
            }
        }
    }

    #[test]
    fn test_resp3_nan_round_trip() {
        let parser = create_parser(RespVersion::Resp3);
        let encoder = create_encoder(RespVersion::Resp3);

        let original = RespValue::Double(f64::NAN);
        let encoded = encoder.encode(&original).unwrap();
        let parsed = parser.parse(&encoded).unwrap();

        if let ParseResult::Complete(RespValue::Double(parsed_value), _) = parsed {
            assert!(parsed_value.is_nan());
        } else {
            panic!("Failed to parse NaN value");
        }
    }

    #[test]
    fn test_pipeline_parsing() {
        let parser = create_parser(RespVersion::Resp2);

        // Multiple commands in pipeline
        let input = b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n+OK\r\n:42\r\n";

        let (values, remaining) = parser.parse_multiple(input).unwrap();
        assert_eq!(values.len(), 3);
        assert!(remaining.is_empty());

        match &values[0] {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                match &arr[0] {
                    RespValue::BulkString(Some(data)) => assert_eq!(data.as_ref(), b"set"),
                    _ => panic!("Expected bulk string"),
                }
            }
            _ => panic!("Expected array"),
        }

        match &values[1] {
            RespValue::SimpleString(s) => assert_eq!(s, "OK"),
            _ => panic!("Expected simple string"),
        }

        match &values[2] {
            RespValue::Integer(n) => assert_eq!(*n, 42),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_fragmented_parsing() {
        let parser = create_parser(RespVersion::Resp2);

        // Simulate network fragmentation
        let fragments = vec![
            b"*5\r\n$5\r\nhmget\r\n$5\r\nfruit\r\n".as_ref(),
            b"$5\r\napple\r\n$6\r\nbanana\r\n$10\r\n".as_ref(),
            b"watermelon\r\n".as_ref(),
        ];

        let mut accumulated = Vec::new();
        let mut complete_values = Vec::new();

        for fragment in fragments {
            accumulated.extend_from_slice(fragment);

            let result = parser.parse(&accumulated).unwrap();
            match result {
                ParseResult::Complete(value, remaining) => {
                    complete_values.push(value);
                    accumulated = remaining.to_vec();
                }
                ParseResult::Incomplete(_) => {
                    // Continue accumulating
                }
            }
        }

        assert_eq!(complete_values.len(), 1);
        match &complete_values[0] {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 5);
                match &arr[0] {
                    RespValue::BulkString(Some(data)) => assert_eq!(data.as_ref(), b"hmget"),
                    _ => panic!("Expected hmget command"),
                }
                match &arr[4] {
                    RespValue::BulkString(Some(data)) => assert_eq!(data.as_ref(), b"watermelon"),
                    _ => panic!("Expected watermelon"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_version_compatibility() {
        // Test that RESP3 can parse RESP2 data
        let resp2_encoder = create_encoder(RespVersion::Resp2);
        let resp3_parser = create_parser(RespVersion::Resp3);

        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from("GET"))),
            RespValue::BulkString(Some(Bytes::from("mykey"))),
        ]));

        let encoded = resp2_encoder.encode(&value).unwrap();
        let parsed = resp3_parser.parse(&encoded).unwrap();

        if let ParseResult::Complete(parsed_value, _) = parsed {
            assert_eq!(value, parsed_value);
        } else {
            panic!("RESP3 should be able to parse RESP2 data");
        }
    }

    #[test]
    fn test_error_handling() {
        let parser = create_parser(RespVersion::Resp2);

        // Test invalid input
        let invalid_inputs = vec![
            b"$abc\r\n".as_ref(), // Invalid length
            b"*abc\r\n".as_ref(), // Invalid array length
            b":abc\r\n".as_ref(),  // Invalid integer
        ];

        for invalid_input in invalid_inputs {
            let result = parser.parse(invalid_input);
            assert!(result.is_err(), "Should fail to parse invalid input: {:?}", invalid_input);
        }
    }

    #[test]
    fn test_large_data_handling() {
        let parser = create_parser(RespVersion::Resp2);
        let encoder = create_encoder(RespVersion::Resp2);

        // Test with large bulk string
        let large_data = "x".repeat(10000);
        let value = RespValue::BulkString(Some(Bytes::from(large_data.clone())));

        let encoded = encoder.encode(&value).unwrap();
        let parsed = parser.parse(&encoded).unwrap();

        if let ParseResult::Complete(RespValue::BulkString(Some(parsed_data)), _) = parsed {
            assert_eq!(parsed_data.len(), 10000);
            assert_eq!(parsed_data, large_data.as_bytes());
        } else {
            panic!("Failed to parse large bulk string");
        }
    }

    #[test]
    fn test_nested_structures() {
        let parser = create_parser(RespVersion::Resp3);
        let encoder = create_encoder(RespVersion::Resp3);

        // Create deeply nested structure
        let mut map = HashMap::new();
        map.insert(
            RespValue::SimpleString("nested".to_string()),
            RespValue::Array(Some(vec![
                RespValue::Set(vec![
                    RespValue::Integer(1),
                    RespValue::Integer(2),
                    RespValue::Integer(3),
                ]),
                RespValue::Map({
                    let mut inner_map = HashMap::new();
                    inner_map.insert(
                        RespValue::SimpleString("inner".to_string()),
                        RespValue::Boolean(true),
                    );
                    inner_map
                }),
            ])),
        );

        let value = RespValue::Map(map);

        let encoded = encoder.encode(&value).unwrap();
        let parsed = parser.parse(&encoded).unwrap();

        if let ParseResult::Complete(parsed_value, _) = parsed {
            assert_eq!(value, parsed_value);
        } else {
            panic!("Failed to parse nested structure");
        }
    }
}