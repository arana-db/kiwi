use bytes::Bytes;
use std::collections::HashMap;

/// RESP protocol versions supported by this library
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespVersion {
    Resp1,
    Resp2,
    Resp3,
}

/// Unified representation of all RESP data types across different versions
#[derive(Debug, Clone, PartialEq, Default)]
pub enum RespValue {
    // Common types across all versions
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>), // None represents null
    Array(Option<Vec<RespValue>>), // None represents null array

    // RESP3 specific types
    #[default]
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(String),
    BulkError(Bytes),
    VerbatimString { format: String, data: String },
    Map(HashMap<RespValue, RespValue>),
    Set(Vec<RespValue>),
    Push(Vec<RespValue>),

    // RESP1 inline command (fallback parsing)
    Inline(Vec<String>),
}

impl RespValue {
    /// Check if this value type is supported in the given RESP version
    pub fn is_supported_in_version(&self, version: RespVersion) -> bool {
        match (self, version) {
            // All versions support these basic types
            (RespValue::SimpleString(_), _) => true,
            (RespValue::Error(_), _) => true,
            (RespValue::Integer(_), _) => true,
            (RespValue::BulkString(_), _) => true,
            (RespValue::Array(_), _) => true,

            // RESP1 supports inline commands
            (RespValue::Inline(_), RespVersion::Resp1) => true,
            (RespValue::Inline(_), RespVersion::Resp2) => true, // RESP2 also supports inline for compatibility

            // RESP3 specific types
            (RespValue::Null, RespVersion::Resp3) => true,
            (RespValue::Boolean(_), RespVersion::Resp3) => true,
            (RespValue::Double(_), RespVersion::Resp3) => true,
            (RespValue::BigNumber(_), RespVersion::Resp3) => true,
            (RespValue::BulkError(_), RespVersion::Resp3) => true,
            (RespValue::VerbatimString { .. }, RespVersion::Resp3) => true,
            (RespValue::Map(_), RespVersion::Resp3) => true,
            (RespValue::Set(_), RespVersion::Resp3) => true,
            (RespValue::Push(_), RespVersion::Resp3) => true,

            _ => false,
        }
    }

    /// Convert a byte slice to a string, handling encoding errors
    pub fn bytes_to_string(bytes: &[u8]) -> Result<String, crate::error::RespError> {
        std::str::from_utf8(bytes)
            .map(|s| s.to_string())
            .map_err(|source| crate::error::RespError::Utf8Error { source })
    }

    /// Get the type prefix for RESP2/RESP3 encoding
    pub fn type_prefix(&self, version: RespVersion) -> Option<u8> {
        match (self, version) {
            (RespValue::SimpleString(_), _) => Some(b'+'),
            (RespValue::Error(_), _) => Some(b'-'),
            (RespValue::Integer(_), _) => Some(b':'),
            (RespValue::BulkString(_), _) => Some(b'$'),
            (RespValue::Array(_), _) => Some(b'*'),

            // RESP3 specific prefixes
            (RespValue::Null, RespVersion::Resp3) => Some(b'_'),
            (RespValue::Boolean(_), RespVersion::Resp3) => Some(b'#'),
            (RespValue::Double(_), RespVersion::Resp3) => Some(b','),
            (RespValue::BigNumber(_), RespVersion::Resp3) => Some(b'('),
            (RespValue::BulkError(_), RespVersion::Resp3) => Some(b'!'),
            (RespValue::VerbatimString { .. }, RespVersion::Resp3) => Some(b'='),
            (RespValue::Map(_), RespVersion::Resp3) => Some(b'%'),
            (RespValue::Set(_), RespVersion::Resp3) => Some(b'~'),
            (RespValue::Push(_), RespVersion::Resp3) => Some(b'>'),

            _ => None,
        }
    }
}

impl Eq for RespValue {}

impl std::hash::Hash for RespValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            RespValue::SimpleString(s) => {
                0u8.hash(state);
                s.hash(state);
            }
            RespValue::Error(s) => {
                1u8.hash(state);
                s.hash(state);
            }
            RespValue::Integer(i) => {
                2u8.hash(state);
                i.hash(state);
            }
            RespValue::BulkString(opt) => {
                3u8.hash(state);
                opt.hash(state);
            }
            RespValue::Array(opt) => {
                4u8.hash(state);
                opt.hash(state);
            }
            RespValue::Null => {
                5u8.hash(state);
            }
            RespValue::Boolean(b) => {
                6u8.hash(state);
                b.hash(state);
            }
            RespValue::Double(f) => {
                7u8.hash(state);
                f.to_bits().hash(state);
            }
            RespValue::BigNumber(s) => {
                8u8.hash(state);
                s.hash(state);
            }
            RespValue::BulkError(b) => {
                9u8.hash(state);
                b.hash(state);
            }
            RespValue::VerbatimString { format, data } => {
                10u8.hash(state);
                format.hash(state);
                data.hash(state);
            }
            RespValue::Map(m) => {
                11u8.hash(state);
                // Note: HashMap iteration order is not deterministic, but for hashing
                // we need a consistent order. This is a simplification.
                m.len().hash(state);
            }
            RespValue::Set(v) => {
                12u8.hash(state);
                v.hash(state);
            }
            RespValue::Push(v) => {
                13u8.hash(state);
                v.hash(state);
            }
            RespValue::Inline(v) => {
                14u8.hash(state);
                v.hash(state);
            }
        }
    }
}