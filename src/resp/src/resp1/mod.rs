//! RESP1 protocol implementation
//!
//! RESP1 was the preliminary version of the Redis Serialization Protocol.
//! It primarily supports inline commands and basic string responses.

mod parser;
mod encoder;

pub use encoder::Resp1Encoder;
pub use parser::Resp1Parser;
