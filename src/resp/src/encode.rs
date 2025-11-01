// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::TryFrom;
use std::fmt::Write;

use bytes::{Bytes, BytesMut};

use crate::{
    CRLF,
    error::RespError,
    types::{RespData, RespVersion},
};

#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmdRes {
    None = 0,
    Ok,
    Pong,
    SyntaxErr,
    InvalidInt,
    InvalidBitInt,
    InvalidBitOffsetInt,
    InvalidFloat,
    OverFlow,
    NotFound,
    OutOfRange,
    InvalidPwd,
    NoneBgsave,
    PurgeExist,
    InvalidParameter,
    WrongNum,
    InvalidIndex,
    InvalidDbType,
    InvalidDB,
    InconsistentHashTag,
    ErrOther,
    ErrMoved,
    ErrClusterDown,
    UnknownCmd,
    UnknownSubCmd,
    IncrByOverFlow,
    InvalidCursor,
    WrongLeader,
    MultiKey,
    NoAuth,
}

impl TryFrom<i8> for CmdRes {
    type Error = RespError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CmdRes::None),
            1 => Ok(CmdRes::Ok),
            2 => Ok(CmdRes::Pong),
            3 => Ok(CmdRes::SyntaxErr),
            4 => Ok(CmdRes::InvalidInt),
            5 => Ok(CmdRes::InvalidBitInt),
            6 => Ok(CmdRes::InvalidBitOffsetInt),
            7 => Ok(CmdRes::InvalidFloat),
            8 => Ok(CmdRes::OverFlow),
            9 => Ok(CmdRes::NotFound),
            10 => Ok(CmdRes::OutOfRange),
            11 => Ok(CmdRes::InvalidPwd),
            12 => Ok(CmdRes::NoneBgsave),
            13 => Ok(CmdRes::PurgeExist),
            14 => Ok(CmdRes::InvalidParameter),
            15 => Ok(CmdRes::WrongNum),
            16 => Ok(CmdRes::InvalidIndex),
            17 => Ok(CmdRes::InvalidDbType),
            18 => Ok(CmdRes::InvalidDB),
            19 => Ok(CmdRes::InconsistentHashTag),
            20 => Ok(CmdRes::ErrOther),
            21 => Ok(CmdRes::ErrMoved),
            22 => Ok(CmdRes::ErrClusterDown),
            23 => Ok(CmdRes::UnknownCmd),
            24 => Ok(CmdRes::UnknownSubCmd),
            25 => Ok(CmdRes::IncrByOverFlow),
            26 => Ok(CmdRes::InvalidCursor),
            27 => Ok(CmdRes::WrongLeader),
            28 => Ok(CmdRes::MultiKey),
            29 => Ok(CmdRes::NoAuth),
            _ => Err(RespError::InvalidData(format!(
                "Invalid CmdRes value: {value}"
            ))),
        }
    }
}

pub trait RespEncode {
    fn set_res(&mut self, res: CmdRes, content: &str) -> &mut Self;

    fn append_array_len(&mut self, len: i64) -> &mut Self;

    fn append_integer(&mut self, value: i64) -> &mut Self;

    fn append_string_raw(&mut self, value: &str) -> &mut Self;

    fn append_simple_string(&mut self, value: &str) -> &mut Self;

    fn append_bulk_string(&mut self, value: &[u8]) -> &mut Self;

    fn append_string(&mut self, value: &str) -> &mut Self;

    fn append_string_vec(&mut self, values: &[String]) -> &mut Self;

    fn set_line_string(&mut self, value: &str) -> &mut Self;

    fn clear(&mut self) -> &mut Self;

    fn get_response(&self) -> Bytes;

    fn encode_resp_data(&mut self, data: &RespData) -> &mut Self;

    // RESP3 specific methods
    fn append_null(&mut self) -> &mut Self;

    fn append_boolean(&mut self, value: bool) -> &mut Self;

    fn append_double(&mut self, value: f64) -> &mut Self;

    fn append_big_number(&mut self, value: &str) -> &mut Self;

    fn append_bulk_error(&mut self, value: &[u8]) -> &mut Self;

    fn append_verbatim_string(&mut self, format: &str, data: &[u8]) -> &mut Self;

    fn append_map(&mut self, pairs: &[(RespData, RespData)]) -> &mut Self;

    fn append_set(&mut self, items: &[RespData]) -> &mut Self;

    fn append_push(&mut self, items: &[RespData]) -> &mut Self;
}

pub struct RespEncoder {
    buffer: BytesMut,

    res: CmdRes,

    version: RespVersion,
}

impl Default for RespEncoder {
    fn default() -> Self {
        Self::new(RespVersion::default())
    }
}

impl RespEncoder {
    pub fn new(version: RespVersion) -> Self {
        Self {
            buffer: BytesMut::new(),
            res: CmdRes::None,
            version,
        }
    }

    pub fn set_version(&mut self, version: RespVersion) -> &mut Self {
        self.version = version;
        self
    }

    pub fn version(&self) -> RespVersion {
        self.version
    }

    pub fn is_resp3(&self) -> bool {
        matches!(self.version, RespVersion::RESP3)
    }

    fn append_crlf(&mut self) -> &mut Self {
        self.buffer.extend_from_slice(CRLF.as_bytes());
        self
    }

    fn set_bulk_string_len(&mut self, len: i64) -> &mut Self {
        let _ = write!(self.buffer, "${len}");
        self.append_crlf()
    }

    fn set_array_len(&mut self, len: i64) -> &mut Self {
        let _ = write!(self.buffer, "*{len}");
        self.append_crlf()
    }
}

impl RespEncode for RespEncoder {
    fn set_res(&mut self, res: CmdRes, content: &str) -> &mut Self {
        self.res = res;
        self.buffer.clear();

        match res {
            CmdRes::None => {}
            CmdRes::Ok => {
                self.set_line_string("+OK");
            }
            CmdRes::Pong => {
                self.set_line_string("+PONG");
            }
            CmdRes::SyntaxErr => {
                let _ = write!(self.buffer, "-ERR syntax error command '{content}'{CRLF}");
            }
            CmdRes::UnknownCmd => {
                let _ = write!(self.buffer, "-ERR unknown command '{content}'{CRLF}");
            }
            CmdRes::UnknownSubCmd => {
                let _ = write!(self.buffer, "-ERR unknown sub command '{content}'{CRLF}");
            }
            CmdRes::InvalidInt => {
                self.set_line_string("-ERR value is not an integer or out of range");
            }
            CmdRes::InvalidBitInt => {
                self.set_line_string("-ERR bit is not an integer or out of range");
            }
            CmdRes::InvalidBitOffsetInt => {
                self.set_line_string("-ERR bit offset is not an integer or out of range");
            }
            CmdRes::InvalidFloat => {
                self.set_line_string("-ERR value is not a valid float");
            }
            CmdRes::OverFlow => {
                self.set_line_string("-ERR increment or decrement would overflow");
            }
            CmdRes::NotFound => {
                self.set_line_string("-ERR no such key");
            }
            CmdRes::OutOfRange => {
                self.set_line_string("-ERR index out of range");
            }
            CmdRes::InvalidPwd => {
                self.set_line_string("-ERR invalid password");
            }
            CmdRes::NoneBgsave => {
                self.set_line_string("-ERR No BGSave Works now");
            }
            CmdRes::PurgeExist => {
                self.set_line_string("-ERR binlog already in purging...");
            }
            CmdRes::InvalidParameter => {
                self.set_line_string("-ERR Invalid Argument");
            }
            CmdRes::WrongNum => {
                let _ = write!(
                    self.buffer,
                    "-ERR wrong number of arguments for '{content}' command{CRLF}",
                );
            }
            CmdRes::InvalidIndex => {
                let _ = write!(self.buffer, "-ERR invalid DB index for '{content}'{CRLF}");
            }
            CmdRes::InvalidDbType => {
                let _ = write!(self.buffer, "-ERR invalid DB for '{content}'{CRLF}");
            }
            CmdRes::InconsistentHashTag => {
                self.set_line_string("-ERR parameters hashtag is inconsistent");
            }
            CmdRes::InvalidDB => {
                let _ = write!(self.buffer, "-ERR invalid DB for '{content}'{CRLF}");
            }
            CmdRes::ErrOther => {
                let _ = write!(self.buffer, "-ERR {content}{CRLF}");
            }
            CmdRes::ErrMoved => {
                let _ = write!(self.buffer, "-MOVED {content}{CRLF}");
            }
            CmdRes::ErrClusterDown => {
                let _ = write!(self.buffer, "-CLUSTERDOWN {content}{CRLF}");
            }
            CmdRes::IncrByOverFlow => {
                let _ = write!(
                    self.buffer,
                    "-ERR increment would produce NaN or Infinity {content}{CRLF}",
                );
            }
            CmdRes::InvalidCursor => {
                self.set_line_string("-ERR invalid cursor");
            }
            CmdRes::WrongLeader => {
                let _ = write!(self.buffer, "-ERR wrong leader {content}{CRLF}");
            }
            CmdRes::MultiKey => {
                let _ = write!(
                    self.buffer,
                    "-WRONGTYPE Operation against a key holding the wrong kind of value {content}{CRLF}",
                );
            }
            CmdRes::NoAuth => {
                self.set_line_string("-NOAUTH Authentication required");
            }
        };

        self
    }

    fn append_array_len(&mut self, len: i64) -> &mut Self {
        self.set_array_len(len)
    }

    fn append_integer(&mut self, value: i64) -> &mut Self {
        let _ = write!(self.buffer, ":{value}");
        self.append_crlf()
    }

    fn append_string_raw(&mut self, value: &str) -> &mut Self {
        self.buffer.extend_from_slice(value.as_bytes());
        self
    }

    fn append_simple_string(&mut self, value: &str) -> &mut Self {
        let _ = write!(self.buffer, "+{value}");
        self.append_crlf()
    }

    fn append_bulk_string(&mut self, value: &[u8]) -> &mut Self {
        self.set_bulk_string_len(value.len() as i64);
        self.buffer.extend_from_slice(value);
        self.append_crlf()
    }

    fn append_string(&mut self, value: &str) -> &mut Self {
        self.append_bulk_string(value.as_bytes())
    }

    fn append_string_vec(&mut self, values: &[String]) -> &mut Self {
        self.append_array_len(values.len() as i64);
        for value in values {
            self.append_string(value);
        }
        self
    }

    fn set_line_string(&mut self, value: &str) -> &mut Self {
        self.buffer.clear();
        self.buffer.extend_from_slice(value.as_bytes());
        self.append_crlf()
    }

    fn clear(&mut self) -> &mut Self {
        self.buffer.clear();
        self.res = CmdRes::None;
        self
    }

    fn get_response(&self) -> Bytes {
        self.buffer.clone().freeze()
    }

    fn encode_resp_data(&mut self, data: &RespData) -> &mut Self {
        match data {
            RespData::SimpleString(bytes) => {
                self.buffer.extend_from_slice(b"+");
                self.buffer.extend_from_slice(bytes);
                self.append_crlf()
            }
            RespData::Error(bytes) => {
                self.buffer.extend_from_slice(b"-");
                self.buffer.extend_from_slice(bytes);
                self.append_crlf()
            }
            RespData::Integer(num) => self.append_integer(*num),
            RespData::BulkString(Some(bytes)) => self.append_bulk_string(bytes),
            RespData::BulkString(None) => self.set_bulk_string_len(-1),
            RespData::Array(Some(array)) => {
                self.append_array_len(array.len() as i64);
                for item in array {
                    self.encode_resp_data(item);
                }
                self
            }
            RespData::Array(None) => self.set_array_len(-1),
            RespData::Inline(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        self.buffer.extend_from_slice(b" ");
                    }
                    self.buffer.extend_from_slice(part);
                }
                self.append_crlf()
            }
            // RESP3 types
            RespData::Null => {
                self.buffer.extend_from_slice(b"_");
                self.append_crlf()
            }
            RespData::Boolean(b) => {
                self.buffer.extend_from_slice(b"#");
                self.buffer.extend_from_slice(if *b { b"t" } else { b"f" });
                self.append_crlf()
            }
            RespData::Double(d) => {
                if d.is_nan() {
                    self.buffer.extend_from_slice(b",nan");
                } else if d.is_infinite() {
                    self.buffer.extend_from_slice(if d.is_sign_negative() {
                        b",-inf"
                    } else {
                        b",inf"
                    });
                } else {
                    let _ = write!(self.buffer, ",{}", d);
                }
                self.append_crlf()
            }
            RespData::BigNumber(bytes) => {
                self.buffer.extend_from_slice(b"(");
                self.buffer.extend_from_slice(bytes);
                self.append_crlf()
            }
            RespData::BulkError(bytes) => {
                let _ = write!(self.buffer, "!{}", bytes.len());
                self.append_crlf();
                self.buffer.extend_from_slice(bytes);
                self.append_crlf()
            }
            RespData::VerbatimString { format, data } => {
                if format.len() != 3 {
                    panic!(
                        "RESP3 VerbatimString format must be exactly 3 bytes, got {}",
                        format.len()
                    );
                }
                let total_len = format.len() + 1 + data.len(); // format + ':' + data
                let _ = write!(self.buffer, "={total_len}");
                self.append_crlf();
                self.buffer.extend_from_slice(format);
                self.buffer.extend_from_slice(b":");
                self.buffer.extend_from_slice(data);
                self.append_crlf()
            }
            RespData::Map(pairs) => {
                let _ = write!(self.buffer, "%{}", pairs.len());
                self.append_crlf();
                for (key, value) in pairs {
                    self.encode_resp_data(key);
                    self.encode_resp_data(value);
                }
                self
            }
            RespData::Set(items) => {
                let _ = write!(self.buffer, "~{}", items.len());
                self.append_crlf();
                for item in items {
                    self.encode_resp_data(item);
                }
                self
            }
            RespData::Push(items) => {
                let _ = write!(self.buffer, ">{}", items.len());
                self.append_crlf();
                for item in items {
                    self.encode_resp_data(item);
                }
                self
            }
        }
    }

    // RESP3 specific methods
    fn append_null(&mut self) -> &mut Self {
        self.buffer.extend_from_slice(b"_");
        self.append_crlf()
    }

    fn append_boolean(&mut self, value: bool) -> &mut Self {
        self.buffer.extend_from_slice(b"#");
        self.buffer
            .extend_from_slice(if value { b"t" } else { b"f" });
        self.append_crlf()
    }

    fn append_double(&mut self, value: f64) -> &mut Self {
        if value.is_nan() {
            self.buffer.extend_from_slice(b",nan");
        } else if value.is_infinite() {
            self.buffer.extend_from_slice(if value.is_sign_negative() {
                b",-inf"
            } else {
                b",inf"
            });
        } else {
            let _ = write!(self.buffer, ",{}", value);
        }
        self.append_crlf()
    }

    fn append_big_number(&mut self, value: &str) -> &mut Self {
        self.buffer.extend_from_slice(b"(");
        self.buffer.extend_from_slice(value.as_bytes());
        self.append_crlf()
    }

    fn append_bulk_error(&mut self, value: &[u8]) -> &mut Self {
        let _ = write!(self.buffer, "!{}", value.len());
        self.append_crlf();
        self.buffer.extend_from_slice(value);
        self.append_crlf()
    }

    fn append_verbatim_string(&mut self, format: &str, data: &[u8]) -> &mut Self {
        if format.len() != 3 {
            panic!(
                "RESP3 VerbatimString format must be exactly 3 bytes, got {}",
                format.len()
            );
        }
        let total_len = format.len() + 1 + data.len(); // format + ':' + data
        let _ = write!(self.buffer, "={total_len}");
        self.append_crlf();
        self.buffer.extend_from_slice(format.as_bytes());
        self.buffer.extend_from_slice(b":");
        self.buffer.extend_from_slice(data);
        self.append_crlf()
    }

    fn append_map(&mut self, pairs: &[(RespData, RespData)]) -> &mut Self {
        let _ = write!(self.buffer, "%{}", pairs.len());
        self.append_crlf();
        for (key, value) in pairs {
            self.encode_resp_data(key);
            self.encode_resp_data(value);
        }
        self
    }

    fn append_set(&mut self, items: &[RespData]) -> &mut Self {
        let _ = write!(self.buffer, "~{}", items.len());
        self.append_crlf();
        for item in items {
            self.encode_resp_data(item);
        }
        self
    }

    fn append_push(&mut self, items: &[RespData]) -> &mut Self {
        let _ = write!(self.buffer, ">{}", items.len());
        self.append_crlf();
        for item in items {
            self.encode_resp_data(item);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RespData, RespVersion};

    #[test]
    fn test_encode_resp3_null() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::Null);
        assert_eq!(encoder.get_response(), Bytes::from("_\r\n"));
    }

    #[test]
    fn test_encode_resp3_boolean_true() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::Boolean(true));
        assert_eq!(encoder.get_response(), Bytes::from("#t\r\n"));
    }

    #[test]
    fn test_encode_resp3_boolean_false() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::Boolean(false));
        assert_eq!(encoder.get_response(), Bytes::from("#f\r\n"));
    }

    #[test]
    fn test_encode_resp3_double() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        #[allow(clippy::approx_constant)]
        encoder.encode_resp_data(&RespData::Double(3.14159));
        assert_eq!(encoder.get_response(), Bytes::from(",3.14159\r\n"));
    }

    #[test]
    fn test_encode_resp3_big_number() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::BigNumber(Bytes::from(
            "123456789012345678901234567890",
        )));
        assert_eq!(
            encoder.get_response(),
            Bytes::from("(123456789012345678901234567890\r\n")
        );
    }

    #[test]
    fn test_encode_resp3_bulk_error() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::BulkError(Bytes::from("SYNTAX invalid syntax")));
        assert_eq!(
            encoder.get_response(),
            Bytes::from("!21\r\nSYNTAX invalid syntax\r\n")
        );
    }

    #[test]
    fn test_encode_resp3_verbatim_string() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::VerbatimString {
            format: Bytes::from("txt"),
            data: Bytes::from("Some string"),
        });
        assert_eq!(
            encoder.get_response(),
            Bytes::from("=15\r\ntxt:Some string\r\n")
        );
    }

    #[test]
    fn test_encode_resp3_map() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::Map(vec![
            (
                RespData::SimpleString(Bytes::from("first")),
                RespData::Integer(1),
            ),
            (
                RespData::SimpleString(Bytes::from("second")),
                RespData::Integer(2),
            ),
        ]));
        assert_eq!(
            encoder.get_response(),
            Bytes::from("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n")
        );
    }

    #[test]
    fn test_encode_resp3_set() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::Set(vec![
            RespData::SimpleString(Bytes::from("orange")),
            RespData::SimpleString(Bytes::from("apple")),
            RespData::Boolean(true),
        ]));
        assert_eq!(
            encoder.get_response(),
            Bytes::from("~3\r\n+orange\r\n+apple\r\n#t\r\n")
        );
    }

    #[test]
    fn test_encode_resp3_push() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);
        encoder.encode_resp_data(&RespData::Push(vec![
            RespData::SimpleString(Bytes::from("pubsub")),
            RespData::SimpleString(Bytes::from("message")),
        ]));
        assert_eq!(
            encoder.get_response(),
            Bytes::from(">2\r\n+pubsub\r\n+message\r\n")
        );
    }

    #[test]
    fn test_resp3_convenience_methods() {
        let mut encoder = RespEncoder::new(RespVersion::RESP3);

        encoder.clear().append_null();
        assert_eq!(encoder.get_response(), Bytes::from("_\r\n"));

        encoder.clear().append_boolean(true);
        assert_eq!(encoder.get_response(), Bytes::from("#t\r\n"));

        #[allow(clippy::approx_constant)]
        encoder.clear().append_double(2.718);
        assert_eq!(encoder.get_response(), Bytes::from(",2.718\r\n"));

        encoder.clear().append_big_number("999999999999999999999");
        assert_eq!(
            encoder.get_response(),
            Bytes::from("(999999999999999999999\r\n")
        );

        encoder
            .clear()
            .append_bulk_error(b"ERR something went wrong");
        assert_eq!(
            encoder.get_response(),
            Bytes::from("!24\r\nERR something went wrong\r\n")
        );

        encoder
            .clear()
            .append_verbatim_string("txt", b"Hello World");
        assert_eq!(
            encoder.get_response(),
            Bytes::from("=15\r\ntxt:Hello World\r\n")
        );
    }

    #[test]
    fn test_backward_compatibility() {
        // Test that RESP2 data still works with RESP3 encoder
        let mut encoder = RespEncoder::new(RespVersion::RESP3);

        encoder.encode_resp_data(&RespData::SimpleString(Bytes::from("OK")));
        assert_eq!(encoder.get_response(), Bytes::from("+OK\r\n"));

        encoder.clear().encode_resp_data(&RespData::Integer(42));
        assert_eq!(encoder.get_response(), Bytes::from(":42\r\n"));

        encoder
            .clear()
            .encode_resp_data(&RespData::BulkString(Some(Bytes::from("hello"))));
        assert_eq!(encoder.get_response(), Bytes::from("$5\r\nhello\r\n"));
    }

    #[test]
    fn test_resp3_boolean_with_resp2_encoder() {
        // Test current behavior: RESP3 Boolean encodes as RESP3 format even with RESP2 encoder
        // This produces invalid RESP2 output (#t\r\n is not valid RESP2)
        // TODO: Either fail with error or auto-convert Boolean(true) -> Integer(1) ":1\r\n"
        let mut encoder = RespEncoder::new(RespVersion::RESP2);
        encoder.encode_resp_data(&RespData::Boolean(true));
        let result = encoder.get_response();
        // Current implementation produces RESP3 format regardless of encoder version
        assert_eq!(result, Bytes::from("#t\r\n"));
    }

    #[test]
    fn test_resp3_null_with_resp2_encoder() {
        // Test current behavior: RESP3 Null encodes as RESP3 format even with RESP2 encoder
        // This produces invalid RESP2 output (_\r\n is not valid RESP2)
        // TODO: Auto-convert Null -> BulkString(None) "$-1\r\n" for RESP2 compatibility
        let mut encoder = RespEncoder::new(RespVersion::RESP2);
        encoder.encode_resp_data(&RespData::Null);
        let result = encoder.get_response();
        // Current implementation produces RESP3 format regardless of encoder version
        assert_eq!(result, Bytes::from("_\r\n"));
    }

    #[test]
    fn test_resp3_types_behavior() {
        // Document current behavior: RESP3 types encode regardless of version
        // Future improvement: Add version-aware encoding or fail early
        let mut encoder = RespEncoder::new(RespVersion::RESP2);

        // Double
        #[allow(clippy::approx_constant)]
        encoder.clear().encode_resp_data(&RespData::Double(3.14));
        assert_eq!(encoder.get_response(), Bytes::from(",3.14\r\n"));

        // Map
        encoder.clear().encode_resp_data(&RespData::Map(vec![]));
        assert_eq!(encoder.get_response(), Bytes::from("%0\r\n"));
    }
}
