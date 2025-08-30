//! RESP Protocol Library
//!
//! A comprehensive implementation of Redis Serialization Protocol (RESP) 
//! supporting versions 1, 2, and 3. This library provides efficient parsing 
//! and encoding capabilities while maintaining compatibility across different 
//! protocol versions.

pub mod error;
pub mod types;
pub mod parser;
pub mod encoder;

pub mod resp1;
pub mod resp2;
pub mod resp3;

#[cfg(test)]
mod integration_tests;

pub use encoder::{EncodeResult, RespEncoder};
pub use error::{RespError, Result};
pub use parser::{ParseResult, RespParser};
pub use types::{RespValue, RespVersion};

// Version-specific re-exports
pub use resp1::{Resp1Encoder, Resp1Parser};
pub use resp2::{Resp2Encoder, Resp2Parser};
pub use resp3::{Resp3Encoder, Resp3Parser};

/// Create a parser for the specified RESP version
pub fn create_parser(version: RespVersion) -> Box<dyn RespParser> {
    match version {
        RespVersion::Resp1 => Box::new(Resp1Parser::new()),
        RespVersion::Resp2 => Box::new(Resp2Parser::new()),
        RespVersion::Resp3 => Box::new(Resp3Parser::new()),
    }
}

/// Create an encoder for the specified RESP version
pub fn create_encoder(version: RespVersion) -> Box<dyn RespEncoder> {
    match version {
        RespVersion::Resp1 => Box::new(Resp1Encoder::new()),
        RespVersion::Resp2 => Box::new(Resp2Encoder::new()),
        RespVersion::Resp3 => Box::new(Resp3Encoder::new()),
    }
}