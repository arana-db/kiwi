use crate::{
    encoder::{utils, EncodeResult, RespEncoder},
    types::{RespValue, RespVersion}
    ,
};
use bytes::BytesMut;

/// RESP3 protocol encoder
///
/// Implements the enhanced RESP3 protocol encoding with support for all RESP2 types plus:
/// - Null values (_)
/// - Boolean values (#)
/// - Double values (,)
/// - Big numbers (()
/// - Bulk errors (!)
/// - Verbatim strings (=)
/// - Maps (%)
/// - Sets (~)
/// - Push messages (>)
pub struct Resp3Encoder;

impl Resp3Encoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Resp3Encoder {
    fn default() -> Self {
        Self::new()
    }
}

impl RespEncoder for Resp3Encoder {
    fn encode(&self, value: &RespValue) -> EncodeResult {
        utils::validate_version_support(value, RespVersion::Resp3)?;

        let mut buffer = BytesMut::new();

        match value {
            // RESP2 compatible types
            RespValue::SimpleString(s) => {
                utils::encode_simple_string(&mut buffer, s);
            }
            RespValue::Error(e) => {
                utils::encode_error(&mut buffer, e);
            }
            RespValue::Integer(i) => {
                utils::encode_integer(&mut buffer, *i);
            }
            RespValue::BulkString(data) => {
                utils::encode_bulk_string(&mut buffer, data.as_ref().map(|b| b.as_ref()));
            }
            RespValue::Array(arr) => {
                match arr {
                    Some(elements) => {
                        utils::encode_array_header(&mut buffer, Some(elements.len()));
                        for element in elements {
                            let encoded_element = self.encode(element)?;
                            buffer.extend_from_slice(&encoded_element);
                        }
                    }
                    None => {
                        utils::encode_array_header(&mut buffer, None);
                    }
                }
            }
            RespValue::Inline(args) => {
                // Encode inline as array for standard RESP3
                utils::encode_array_header(&mut buffer, Some(args.len()));
                for arg in args {
                    let arg_value = RespValue::BulkString(Some(bytes::Bytes::copy_from_slice(arg.as_bytes())));
                    let encoded_arg = self.encode(&arg_value)?;
                    buffer.extend_from_slice(&encoded_arg);
                }
            }

            // RESP3 specific types
            RespValue::Null => {
                utils::resp3::encode_null(&mut buffer);
            }
            RespValue::Boolean(b) => {
                utils::resp3::encode_boolean(&mut buffer, *b);
            }
            RespValue::Double(d) => {
                utils::resp3::encode_double(&mut buffer, *d);
            }
            RespValue::BigNumber(n) => {
                utils::resp3::encode_big_number(&mut buffer, n);
            }
            RespValue::BulkError(data) => {
                utils::resp3::encode_bulk_error(&mut buffer, data);
            }
            RespValue::VerbatimString { format, data } => {
                utils::resp3::encode_verbatim_string(&mut buffer, format, data);
            }
            RespValue::Map(map) => {
                utils::resp3::encode_map_header(&mut buffer, map.len());
                for (key, value) in map {
                    let encoded_key = self.encode(key)?;
                    let encoded_value = self.encode(value)?;
                    buffer.extend_from_slice(&encoded_key);
                    buffer.extend_from_slice(&encoded_value);
                }
            }
            RespValue::Set(elements) => {
                utils::resp3::encode_set_header(&mut buffer, elements.len());
                for element in elements {
                    let encoded_element = self.encode(element)?;
                    buffer.extend_from_slice(&encoded_element);
                }
            }
            RespValue::Push(elements) => {
                utils::resp3::encode_push_header(&mut buffer, elements.len());
                for element in elements {
                    let encoded_element = self.encode(element)?;
                    buffer.extend_from_slice(&encoded_element);
                }
            }
        }

        Ok(buffer)
    }

    fn version(&self) -> RespVersion {
        RespVersion::Resp3
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    #[test]
    fn test_encode_null() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Null;

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"_\r\n");
    }

    #[test]
    fn test_encode_boolean_true() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Boolean(true);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"#t\r\n");
    }

    #[test]
    fn test_encode_boolean_false() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Boolean(false);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"#f\r\n");
    }

    #[test]
    fn test_encode_double() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Double(3.14159);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b",3.14159\r\n");
    }

    #[test]
    fn test_encode_double_infinity() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Double(f64::INFINITY);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b",inf\r\n");
    }

    #[test]
    fn test_encode_double_negative_infinity() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Double(f64::NEG_INFINITY);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b",-inf\r\n");
    }

    #[test]
    fn test_encode_double_nan() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Double(f64::NAN);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b",nan\r\n");
    }

    #[test]
    fn test_encode_big_number() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::BigNumber("123456789012345678901234567890".to_string());

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"(123456789012345678901234567890\r\n");
    }

    #[test]
    fn test_encode_bulk_error() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::BulkError(Bytes::from("SYNTAX invalid syntax"));

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"!21\r\nSYNTAX invalid syntax\r\n");
    }

    #[test]
    fn test_encode_verbatim_string() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::VerbatimString {
            format: "txt".to_string(),
            data: "Some string".to_string(),
        };

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"=15\r\ntxt:Some string\r\n");
    }

    #[test]
    fn test_encode_map() {
        let encoder = Resp3Encoder::new();
        let mut map = HashMap::new();
        map.insert(
            RespValue::SimpleString("first".to_string()),
            RespValue::Integer(1),
        );
        map.insert(
            RespValue::SimpleString("second".to_string()),
            RespValue::Integer(2),
        );
        let value = RespValue::Map(map);

        let result = encoder.encode(&value).unwrap();

        // Since HashMap iteration order is not guaranteed, we need to check both possible orders
        let expected1 = b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";
        let expected2 = b"%2\r\n+second\r\n:2\r\n+first\r\n:1\r\n";

        assert!(result.as_ref() == expected1 || result.as_ref() == expected2);
    }

    #[test]
    fn test_encode_set() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Set(vec![
            RespValue::SimpleString("apple".to_string()),
            RespValue::SimpleString("orange".to_string()),
            RespValue::SimpleString("banana".to_string()),
        ]);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"~3\r\n+apple\r\n+orange\r\n+banana\r\n");
    }

    #[test]
    fn test_encode_push() {
        let encoder = Resp3Encoder::new();
        let value = RespValue::Push(vec![
            RespValue::SimpleString("pubsub".to_string()),
            RespValue::SimpleString("message".to_string()),
        ]);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b">2\r\n+pubsub\r\n+message\r\n");
    }

    #[test]
    fn test_encode_resp2_compatibility() {
        let encoder = Resp3Encoder::new();

        // Test that all RESP2 types can be encoded by RESP3 encoder
        let values = vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Error("ERR message".to_string()),
            RespValue::Integer(42),
            RespValue::BulkString(Some(Bytes::from("hello"))),
            RespValue::Array(Some(vec![RespValue::Integer(1), RespValue::Integer(2)])),
        ];

        for value in values {
            let result = encoder.encode(&value);
            assert!(result.is_ok(), "Failed to encode RESP2 compatible value: {:?}", value);
        }
    }

    #[test]
    fn test_encode_complex_nested_structure() {
        let encoder = Resp3Encoder::new();

        let mut map = HashMap::new();
        map.insert(
            RespValue::SimpleString("numbers".to_string()),
            RespValue::Set(vec![RespValue::Integer(1), RespValue::Integer(2), RespValue::Integer(3)]),
        );
        map.insert(
            RespValue::SimpleString("info".to_string()),
            RespValue::VerbatimString {
                format: "txt".to_string(),
                data: "some info".to_string(),
            },
        );

        let value = RespValue::Array(Some(vec![
            RespValue::Map(map),
            RespValue::Null,
            RespValue::Boolean(true),
            RespValue::Double(3.14),
        ]));

        let result = encoder.encode(&value);
        assert!(result.is_ok());

        // Just verify it encodes without error for complex nested structures
        let encoded = result.unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_multiple() {
        let encoder = Resp3Encoder::new();
        let values = vec![
            RespValue::Null,
            RespValue::Boolean(true),
            RespValue::Double(3.14),
            RespValue::SimpleString("OK".to_string()),
        ];

        let result = encoder.encode_multiple(&values).unwrap();
        let expected = b"_\r\n#t\r\n,3.14\r\n+OK\r\n";
        assert_eq!(result.as_ref(), expected);
    }
}