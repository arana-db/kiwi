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
        }
    }
}
