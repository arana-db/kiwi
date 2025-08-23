//! RESP2 protocol implementation
//!
//! RESP2 is the standard Redis Serialization Protocol that became the 
//! default in Redis 2.0. It supports Simple Strings, Errors, Integers,
//! Bulk Strings, Arrays, and inline commands for backwards compatibility.

mod parser;
mod encoder;

pub use encoder::Resp2Encoder;
pub use parser::Resp2Parser;
