use crate::{
    encoder::{utils, EncodeResult, RespEncoder},
    types::{RespValue, RespVersion},
    RespError,
};
use bytes::BytesMut;

/// RESP2 protocol encoder
///
/// Implements the standard RESP2 protocol encoding with support for:
/// - Simple Strings (+)
/// - Errors (-)
/// - Integers (:)
/// - Bulk Strings ($)
/// - Arrays (*)
/// - Inline commands (for compatibility)
pub struct Resp2Encoder;

impl Resp2Encoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Resp2Encoder {
    fn default() -> Self {
        Self::new()
    }
}

impl RespEncoder for Resp2Encoder {
    fn encode(&self, value: &RespValue) -> EncodeResult {
        utils::validate_version_support(value, RespVersion::Resp2)?;

        let mut buffer = BytesMut::new();

        match value {
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
                // Encode inline as array for standard RESP2
                utils::encode_array_header(&mut buffer, Some(args.len()));
                for arg in args {
                    let arg_value = RespValue::BulkString(Some(bytes::Bytes::copy_from_slice(arg.as_bytes())));
                    let encoded_arg = self.encode(&arg_value)?;
                    buffer.extend_from_slice(&encoded_arg);
                }
            }
            _ => {
                return Err(RespError::UnsupportedDataType);
            }
        }

        Ok(buffer)
    }

    fn version(&self) -> RespVersion {
        RespVersion::Resp2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_encode_simple_string() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::SimpleString("OK".to_string());

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Error("ERR unknown command".to_string());

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Integer(1000);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b":1000\r\n");
    }

    #[test]
    fn test_encode_negative_integer() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Integer(-42);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b":-42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::BulkString(Some(Bytes::from("foobar")));

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"$6\r\nfoobar\r\n");
    }

    #[test]
    fn test_encode_null_bulk_string() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::BulkString(None);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"$-1\r\n");
    }

    #[test]
    fn test_encode_empty_bulk_string() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::BulkString(Some(Bytes::new()));

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"$0\r\n\r\n");
    }

    #[test]
    fn test_encode_array() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from("foo"))),
            RespValue::BulkString(Some(Bytes::from("bar"))),
            RespValue::BulkString(None),
        ]));

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$-1\r\n");
    }

    #[test]
    fn test_encode_null_array() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Array(None);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"*-1\r\n");
    }

    #[test]
    fn test_encode_empty_array() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Array(Some(vec![]));

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"*0\r\n");
    }

    #[test]
    fn test_encode_nested_array() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Array(Some(vec![
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from("foo"))),
                RespValue::BulkString(Some(Bytes::from("bar"))),
            ])),
            RespValue::Integer(42),
        ]));

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n");
    }

    #[test]
    fn test_encode_inline_as_array() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Inline(vec!["GET".to_string(), "key".to_string()]);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }

    #[test]
    fn test_encode_multiple() {
        let encoder = Resp2Encoder::new();
        let values = vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Integer(42),
            RespValue::BulkString(Some(Bytes::from("hello"))),
        ];

        let result = encoder.encode_multiple(&values).unwrap();
        assert_eq!(result.as_ref(), b"+OK\r\n:42\r\n$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_unsupported_type() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Null; // RESP3 type not supported in RESP2

        let result = encoder.encode(&value);
        assert!(result.is_err());
        match result.unwrap_err() {
            RespError::UnsupportedDataType => {}
            _ => panic!("Expected UnsupportedDataType error"),
        }
    }

    #[test]
    fn test_encode_complex_structure() {
        let encoder = Resp2Encoder::new();
        let value = RespValue::Array(Some(vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from("key1"))),
                RespValue::BulkString(Some(Bytes::from("value1"))),
                RespValue::BulkString(Some(Bytes::from("key2"))),
                RespValue::BulkString(Some(Bytes::from("value2"))),
            ])),
            RespValue::Integer(2),
        ]));

        let result = encoder.encode(&value).unwrap();
        let expected = b"*3\r\n+OK\r\n*4\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n:2\r\n";
        assert_eq!(result.as_ref(), expected);
    }
}