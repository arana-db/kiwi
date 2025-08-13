//! RESP3 protocol implementation
//!
//! RESP3 is the enhanced Redis Serialization Protocol introduced in Redis 6.0.
//! It's a superset of RESP2 that adds new data types and features:
//! - Null values (_)
//! - Boolean values (#)
//! - Double values (,)
//! - Big numbers (()
//! - Bulk errors (!)
//! - Verbatim strings (=)
//! - Maps (%)
//! - Sets (~)
//! - Push messages (>)
//! - Out-of-band messaging support

mod parser;
mod encoder;

pub use encoder::Resp3Encoder;
pub use parser::Resp3Parser;
