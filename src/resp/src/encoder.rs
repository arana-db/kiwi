use crate::{RespError, RespValue, Result};
use bytes::{BufMut, BytesMut};

/// Result of an encoding operation
pub type EncodeResult = Result<BytesMut>;

/// Trait for RESP protocol encoders
pub trait RespEncoder: Send + Sync {
    /// Encode a single RESP value to bytes
    fn encode(&self, value: &RespValue) -> EncodeResult;

    /// Encode multiple RESP values (pipeline) to bytes
    fn encode_multiple(&self, values: &[RespValue]) -> EncodeResult {
        let mut buffer = BytesMut::new();
        for value in values {
            let encoded = self.encode(value)?;
            buffer.extend_from_slice(&encoded);
        }
        Ok(buffer)
    }

    /// Get the RESP version this encoder supports
    fn version(&self) -> crate::types::RespVersion;
}

/// Common encoding utilities used across different RESP versions
pub mod utils {
    use super::*;

    /// Encode CRLF line terminator
    pub fn encode_crlf(buffer: &mut BytesMut) {
        buffer.extend_from_slice(b"\r\n");
    }

    /// Encode a line with CRLF termination
    pub fn encode_line(buffer: &mut BytesMut, content: &[u8]) {
        buffer.extend_from_slice(content);
        encode_crlf(buffer);
    }

    /// Encode simple string
    pub fn encode_simple_string(buffer: &mut BytesMut, value: &str) {
        buffer.put_u8(b'+');
        buffer.extend_from_slice(value.as_bytes());
        encode_crlf(buffer);
    }

    /// Encode error
    pub fn encode_error(buffer: &mut BytesMut, value: &str) {
        buffer.put_u8(b'-');
        buffer.extend_from_slice(value.as_bytes());
        encode_crlf(buffer);
    }

    /// Encode integer
    pub fn encode_integer(buffer: &mut BytesMut, value: i64) {
        buffer.put_u8(b':');
        buffer.extend_from_slice(value.to_string().as_bytes());
        encode_crlf(buffer);
    }

    /// Encode bulk string
    pub fn encode_bulk_string(buffer: &mut BytesMut, value: Option<&[u8]>) {
        buffer.put_u8(b'$');
        match value {
            Some(data) => {
                buffer.extend_from_slice(data.len().to_string().as_bytes());
                encode_crlf(buffer);
                buffer.extend_from_slice(data);
                encode_crlf(buffer);
            }
            None => {
                buffer.extend_from_slice(b"-1");
                encode_crlf(buffer);
            }
        }
    }

    /// Encode array header
    pub fn encode_array_header(buffer: &mut BytesMut, length: Option<usize>) {
        buffer.put_u8(b'*');
        match length {
            Some(len) => {
                buffer.extend_from_slice(len.to_string().as_bytes());
            }
            None => {
                buffer.extend_from_slice(b"-1");
            }
        }
        encode_crlf(buffer);
    }

    /// RESP3 specific encoders
    pub mod resp3 {
        use super::*;

        /// Encode null value
        pub fn encode_null(buffer: &mut BytesMut) {
            buffer.put_u8(b'_');
            encode_crlf(buffer);
        }

        /// Encode boolean value
        pub fn encode_boolean(buffer: &mut BytesMut, value: bool) {
            buffer.put_u8(b'#');
            if value {
                buffer.put_u8(b't');
            } else {
                buffer.put_u8(b'f');
            }
            encode_crlf(buffer);
        }

        /// Encode double value
        pub fn encode_double(buffer: &mut BytesMut, value: f64) {
            buffer.put_u8(b',');
            if value.is_infinite() {
                if value.is_sign_positive() {
                    buffer.extend_from_slice(b"inf");
                } else {
                    buffer.extend_from_slice(b"-inf");
                }
            } else if value.is_nan() {
                buffer.extend_from_slice(b"nan");
            } else {
                buffer.extend_from_slice(value.to_string().as_bytes());
            }
            encode_crlf(buffer);
        }

        /// Encode big number
        pub fn encode_big_number(buffer: &mut BytesMut, value: &str) {
            buffer.put_u8(b'(');
            buffer.extend_from_slice(value.as_bytes());
            encode_crlf(buffer);
        }

        /// Encode bulk error
        pub fn encode_bulk_error(buffer: &mut BytesMut, value: &[u8]) {
            buffer.put_u8(b'!');
            buffer.extend_from_slice(value.len().to_string().as_bytes());
            encode_crlf(buffer);
            buffer.extend_from_slice(value);
            encode_crlf(buffer);
        }

        /// Encode verbatim string
        pub fn encode_verbatim_string(buffer: &mut BytesMut, format: &str, data: &str) {
            buffer.put_u8(b'=');
            let full_content = format!("{}:{}", format, data);
            buffer.extend_from_slice(full_content.len().to_string().as_bytes());
            encode_crlf(buffer);
            buffer.extend_from_slice(full_content.as_bytes());
            encode_crlf(buffer);
        }

        /// Encode map header
        pub fn encode_map_header(buffer: &mut BytesMut, length: usize) {
            buffer.put_u8(b'%');
            buffer.extend_from_slice(length.to_string().as_bytes());
            encode_crlf(buffer);
        }

        /// Encode set header
        pub fn encode_set_header(buffer: &mut BytesMut, length: usize) {
            buffer.put_u8(b'~');
            buffer.extend_from_slice(length.to_string().as_bytes());
            encode_crlf(buffer);
        }

        /// Encode push header
        pub fn encode_push_header(buffer: &mut BytesMut, length: usize) {
            buffer.put_u8(b'>');
            buffer.extend_from_slice(length.to_string().as_bytes());
            encode_crlf(buffer);
        }
    }

    /// Validate that a value is supported in the given version
    pub fn validate_version_support(value: &RespValue, version: crate::types::RespVersion) -> Result<()> {
        if !value.is_supported_in_version(version) {
            return Err(RespError::UnsupportedDataType);
        }
        Ok(())
    }
}