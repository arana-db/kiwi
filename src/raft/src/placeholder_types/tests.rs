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

//! Unit tests for placeholder types

#[cfg(test)]
mod tests {
    use super::super::*;
    use bytes::Bytes;

    #[test]
    fn test_resp_data_as_string() {
        let simple_string = RespData::SimpleString("hello".to_string());
        assert_eq!(simple_string.as_string(), Some("hello".to_string()));

        let bulk_string = RespData::BulkString(Some(Bytes::from("world")));
        assert_eq!(bulk_string.as_string(), Some("world".to_string()));

        let null_bulk_string = RespData::BulkString(None);
        assert_eq!(null_bulk_string.as_string(), None);

        let integer = RespData::Integer(42);
        assert_eq!(integer.as_string(), None);

        let error = RespData::Error("error message".to_string());
        assert_eq!(error.as_string(), None);
    }

    #[test]
    fn test_resp_data_as_bytes() {
        let bulk_string = RespData::BulkString(Some(Bytes::from("test")));
        assert_eq!(bulk_string.as_bytes(), Some(Bytes::from("test")));

        let simple_string = RespData::SimpleString("hello".to_string());
        assert_eq!(simple_string.as_bytes(), Some(Bytes::from("hello")));

        let null_bulk_string = RespData::BulkString(None);
        assert_eq!(null_bulk_string.as_bytes(), None);

        let integer = RespData::Integer(123);
        assert_eq!(integer.as_bytes(), None);

        let array = RespData::Array(Some(vec![]));
        assert_eq!(array.as_bytes(), None);
    }

    #[test]
    fn test_resp_data_serialization() {
        let test_cases = vec![
            RespData::SimpleString("OK".to_string()),
            RespData::Error("ERR something went wrong".to_string()),
            RespData::Integer(42),
            RespData::BulkString(Some(Bytes::from("hello world"))),
            RespData::BulkString(None),
            RespData::Array(Some(vec![
                RespData::SimpleString("first".to_string()),
                RespData::Integer(123),
            ])),
            RespData::Array(None),
            RespData::Inline(vec!["GET".to_string(), "key".to_string()]),
        ];

        for test_case in test_cases {
            let serialized = serde_json::to_string(&test_case).unwrap();
            let deserialized: RespData = serde_json::from_str(&serialized).unwrap();

            // Compare the variants since RespData doesn't implement PartialEq
            match (&test_case, &deserialized) {
                (RespData::SimpleString(a), RespData::SimpleString(b)) => assert_eq!(a, b),
                (RespData::Error(a), RespData::Error(b)) => assert_eq!(a, b),
                (RespData::Integer(a), RespData::Integer(b)) => assert_eq!(a, b),
                (RespData::BulkString(a), RespData::BulkString(b)) => assert_eq!(a, b),
                (RespData::Array(a), RespData::Array(b)) => match (a, b) {
                    (Some(arr_a), Some(arr_b)) => assert_eq!(arr_a.len(), arr_b.len()),
                    (None, None) => {}
                    _ => panic!("Array variants don't match"),
                },
                (RespData::Inline(a), RespData::Inline(b)) => assert_eq!(a, b),
                _ => panic!("Variants don't match"),
            }
        }
    }

    #[test]
    fn test_resp_data_binary_data() {
        let binary_data = vec![0u8, 1, 2, 3, 255, 128];
        let bulk_string = RespData::BulkString(Some(Bytes::from(binary_data.clone())));

        let bytes = bulk_string.as_bytes().unwrap();
        assert_eq!(bytes.to_vec(), binary_data);

        // Test that as_string handles binary data gracefully
        let string_result = bulk_string.as_string();
        assert!(string_result.is_some()); // Should not panic, even with invalid UTF-8
    }

    #[test]
    fn test_client_creation() {
        let client = Client {
            id: "test_client_123".to_string(),
        };

        assert_eq!(client.id, "test_client_123");
    }

    #[test]
    fn test_client_debug() {
        let client = Client {
            id: "debug_test".to_string(),
        };

        let debug_string = format!("{:?}", client);
        assert!(debug_string.contains("Client"));
        assert!(debug_string.contains("debug_test"));
    }

    #[test]
    fn test_rocksdb_engine_creation() {
        let engine = RocksdbEngine {};

        // Test that the struct can be created and debugged
        let debug_string = format!("{:?}", engine);
        assert!(debug_string.contains("RocksdbEngine"));
    }

    #[test]
    fn test_resp_command_creation() {
        let cmd = RespCommand::new(
            CommandType::Set,
            vec![Bytes::from("key"), Bytes::from("value")],
        );

        assert!(matches!(cmd.command_type, CommandType::Set));
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args[0], Bytes::from("key"));
        assert_eq!(cmd.args[1], Bytes::from("value"));
    }

    #[test]
    fn test_resp_command_debug() {
        let cmd = RespCommand::new(CommandType::Get, vec![Bytes::from("test_key")]);

        let debug_string = format!("{:?}", cmd);
        assert!(debug_string.contains("RespCommand"));
        assert!(debug_string.contains("Get"));
    }

    #[test]
    fn test_command_type_display() {
        assert_eq!(format!("{}", CommandType::Set), "SET");
        assert_eq!(format!("{}", CommandType::Get), "GET");
        assert_eq!(format!("{}", CommandType::Del), "DEL");
    }

    #[test]
    fn test_command_type_from_str() {
        assert!(matches!(CommandType::from_str("SET"), Ok(CommandType::Set)));
        assert!(matches!(CommandType::from_str("set"), Ok(CommandType::Set)));
        assert!(matches!(CommandType::from_str("Set"), Ok(CommandType::Set)));

        assert!(matches!(CommandType::from_str("GET"), Ok(CommandType::Get)));
        assert!(matches!(CommandType::from_str("get"), Ok(CommandType::Get)));

        assert!(matches!(CommandType::from_str("DEL"), Ok(CommandType::Del)));
        assert!(matches!(CommandType::from_str("del"), Ok(CommandType::Del)));

        assert!(CommandType::from_str("INVALID").is_err());
        assert!(CommandType::from_str("").is_err());
    }

    #[test]
    fn test_command_type_case_insensitive() {
        let test_cases = vec![
            ("SET", CommandType::Set),
            ("set", CommandType::Set),
            ("Set", CommandType::Set),
            ("sEt", CommandType::Set),
            ("GET", CommandType::Get),
            ("get", CommandType::Get),
            ("GeT", CommandType::Get),
            ("DEL", CommandType::Del),
            ("del", CommandType::Del),
            ("DeL", CommandType::Del),
        ];

        for (input, expected) in test_cases {
            let result = CommandType::from_str(input).unwrap();
            assert!(matches!(
                (result, expected),
                (CommandType::Set, CommandType::Set)
                    | (CommandType::Get, CommandType::Get)
                    | (CommandType::Del, CommandType::Del)
            ));
        }
    }

    #[test]
    fn test_command_type_debug() {
        let set_cmd = CommandType::Set;
        let debug_string = format!("{:?}", set_cmd);
        assert!(debug_string.contains("Set"));

        let get_cmd = CommandType::Get;
        let debug_string = format!("{:?}", get_cmd);
        assert!(debug_string.contains("Get"));

        let del_cmd = CommandType::Del;
        let debug_string = format!("{:?}", del_cmd);
        assert!(debug_string.contains("Del"));
    }

    #[test]
    fn test_resp_data_nested_array() {
        let nested_array = RespData::Array(Some(vec![
            RespData::Array(Some(vec![
                RespData::SimpleString("nested".to_string()),
                RespData::Integer(1),
            ])),
            RespData::BulkString(Some(Bytes::from("top_level"))),
        ]));

        // Test serialization of nested structures
        let serialized = serde_json::to_string(&nested_array).unwrap();
        let deserialized: RespData = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            RespData::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                match &items[0] {
                    RespData::Array(Some(nested_items)) => {
                        assert_eq!(nested_items.len(), 2);
                    }
                    _ => panic!("Expected nested array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_resp_data_empty_collections() {
        let empty_array = RespData::Array(Some(vec![]));
        let empty_inline = RespData::Inline(vec![]);

        match empty_array {
            RespData::Array(Some(items)) => assert!(items.is_empty()),
            _ => panic!("Expected empty array"),
        }

        match empty_inline {
            RespData::Inline(items) => assert!(items.is_empty()),
            _ => panic!("Expected empty inline"),
        }
    }

    #[test]
    fn test_resp_command_empty_args() {
        let cmd = RespCommand::new(CommandType::Get, vec![]);

        assert!(matches!(cmd.command_type, CommandType::Get));
        assert!(cmd.args.is_empty());
    }

    #[test]
    fn test_resp_command_multiple_args() {
        let args = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let cmd = RespCommand::new(CommandType::Del, args.clone());

        assert!(matches!(cmd.command_type, CommandType::Del));
        assert_eq!(cmd.args.len(), 3);
        assert_eq!(cmd.args, args);
    }
}
