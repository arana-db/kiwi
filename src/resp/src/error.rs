use thiserror::Error;

/// Result type used throughout the RESP library
pub type Result<T> = std::result::Result<T, RespError>;

/// Errors that can occur during RESP protocol operations
#[derive(Error, Debug, Clone, PartialEq)]
pub enum RespError {
    #[error("Invalid protocol format: {message}")]
    InvalidFormat { message: String },

    #[error("Incomplete data: need {needed} more bytes")]
    IncompleteData { needed: usize },

    #[error("Invalid integer format: {value}")]
    InvalidInteger { value: String },

    #[error("Invalid bulk string length: {length}")]
    InvalidBulkStringLength { length: i64 },

    #[error("Invalid array length: {length}")]
    InvalidArrayLength { length: i64 },

    #[error("Unsupported data type for RESP version")]
    UnsupportedDataType,

    #[error("Protocol version not supported: {version}")]
    UnsupportedVersion { version: String },

    #[error("UTF-8 encoding error: {source}")]
    Utf8Error { source: std::str::Utf8Error },

    #[error("IO error: {message}")]
    IoError { message: String },
}