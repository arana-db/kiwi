use crate::{
    encoder::{utils, EncodeResult, RespEncoder},
    types::{RespValue, RespVersion},
    RespError,
};
use bytes::BytesMut;

/// RESP1 protocol encoder
///
/// RESP1 encoding is simplified and primarily handles inline responses
/// and basic string outputs.
pub struct Resp1Encoder;

impl Resp1Encoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Resp1Encoder {
    fn default() -> Self {
        Self::new()
    }
}

impl RespEncoder for Resp1Encoder {
    fn encode(&self, value: &RespValue) -> EncodeResult {
        utils::validate_version_support(value, RespVersion::Resp1)?;

        let mut buffer = BytesMut::new();

        match value {
            RespValue::SimpleString(s) => {
                buffer.extend_from_slice(s.as_bytes());
                utils::encode_crlf(&mut buffer);
            }
            RespValue::Error(e) => {
                buffer.extend_from_slice(e.as_bytes());
                utils::encode_crlf(&mut buffer);
            }
            RespValue::Integer(i) => {
                buffer.extend_from_slice(i.to_string().as_bytes());
                utils::encode_crlf(&mut buffer);
            }
            RespValue::BulkString(Some(data)) => {
                buffer.extend_from_slice(data);
                utils::encode_crlf(&mut buffer);
            }
            RespValue::BulkString(None) => {
                // In RESP1, null is represented as empty response
                utils::encode_crlf(&mut buffer);
            }
            RespValue::Array(Some(arr)) => {
                // In RESP1, arrays are typically represented as space-separated values
                for (i, item) in arr.iter().enumerate() {
                    if i > 0 {
                        buffer.extend_from_slice(b" ");
                    }
                    match item {
                        RespValue::SimpleString(s) => buffer.extend_from_slice(s.as_bytes()),
                        RespValue::Integer(n) => buffer.extend_from_slice(n.to_string().as_bytes()),
                        RespValue::BulkString(Some(data)) => buffer.extend_from_slice(data),
                        _ => {
                            return Err(RespError::UnsupportedDataType);
                        }
                    }
                }
                utils::encode_crlf(&mut buffer);
            }
            RespValue::Array(None) => {
                // Null array represented as empty response
                utils::encode_crlf(&mut buffer);
            }
            RespValue::Inline(args) => {
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        buffer.extend_from_slice(b" ");
                    }
                    buffer.extend_from_slice(arg.as_bytes());
                }
                utils::encode_crlf(&mut buffer);
            }
            _ => {
                return Err(RespError::UnsupportedDataType);
            }
        }

        Ok(buffer)
    }

    fn version(&self) -> RespVersion {
        RespVersion::Resp1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_encode_simple_string() {
        let encoder = Resp1Encoder::new();
        let value = RespValue::SimpleString("OK".to_string());

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let encoder = Resp1Encoder::new();
        let value = RespValue::Error("Error message".to_string());

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"Error message\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let encoder = Resp1Encoder::new();
        let value = RespValue::Integer(42);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let encoder = Resp1Encoder::new();
        let value = RespValue::BulkString(Some(Bytes::from("hello")));

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"hello\r\n");
    }

    #[test]
    fn test_encode_null_bulk_string() {
        let encoder = Resp1Encoder::new();
        let value = RespValue::BulkString(None);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"\r\n");
    }

    #[test]
    fn test_encode_array() {
        let encoder = Resp1Encoder::new();
        let value = RespValue::Array(Some(vec![
            RespValue::SimpleString("first".to_string()),
            RespValue::SimpleString("second".to_string()),
        ]));

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"first second\r\n");
    }

    #[test]
    fn test_encode_inline_command() {
        let encoder = Resp1Encoder::new();
        let value = RespValue::Inline(vec!["GET".to_string(), "key".to_string()]);

        let result = encoder.encode(&value).unwrap();
        assert_eq!(result.as_ref(), b"GET key\r\n");
    }

    #[test]
    fn test_encode_multiple() {
        let encoder = Resp1Encoder::new();
        let values = vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Integer(42),
        ];

        let result = encoder.encode_multiple(&values).unwrap();
        assert_eq!(result.as_ref(), b"OK\r\n42\r\n");
    }
}