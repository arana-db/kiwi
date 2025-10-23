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

use std::fmt;

use bytes::Bytes;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RespVersion {
    RESP1,
    #[default]
    RESP2,
    RESP3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Inline,
    // RESP3 additions (used for inspection only)
    Null,
    Boolean,
    Double,
    BulkError,
    VerbatimString,
    BigNumber,
    Map,
    Set,
    Push,
}

impl RespType {
    pub fn from_prefix(byte: u8) -> Option<Self> {
        match byte {
            b'+' => Some(RespType::SimpleString),
            b'-' => Some(RespType::Error),
            b':' => Some(RespType::Integer),
            b'$' => Some(RespType::BulkString),
            b'*' => Some(RespType::Array),
            b'_' => Some(RespType::Null),
            b'#' => Some(RespType::Boolean),
            b',' => Some(RespType::Double),
            b'!' => Some(RespType::BulkError),
            b'=' => Some(RespType::VerbatimString),
            b'(' => Some(RespType::BigNumber),
            b'%' => Some(RespType::Map),
            b'~' => Some(RespType::Set),
            b'>' => Some(RespType::Push),
            _ => None,
        }
    }

    pub fn prefix_byte(&self) -> Option<u8> {
        match self {
            RespType::SimpleString => Some(b'+'),
            RespType::Error => Some(b'-'),
            RespType::Integer => Some(b':'),
            RespType::BulkString => Some(b'$'),
            RespType::Array => Some(b'*'),
            RespType::Inline => None,
            RespType::Null => Some(b'_'),
            RespType::Boolean => Some(b'#'),
            RespType::Double => Some(b','),
            RespType::BulkError => Some(b'!'),
            RespType::VerbatimString => Some(b'='),
            RespType::BigNumber => Some(b'('),
            RespType::Map => Some(b'%'),
            RespType::Set => Some(b'~'),
            RespType::Push => Some(b'>'),
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum RespData {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespData>>),
    Inline(Vec<Bytes>),
    // RESP3 additions (subset; full coverage to be added gradually)
    Null,
    Boolean(bool),
    Double(f64),
    BulkError(Bytes),
    VerbatimString { format: [u8; 3], data: Bytes },
    BigNumber(String),
    Map(Vec<(RespData, RespData)>),
    Set(Vec<RespData>),
    Push(Vec<RespData>),
}

impl Default for RespData {
    fn default() -> Self {
        RespData::BulkString(None)
    }
}

impl RespData {
    pub fn get_type(&self) -> RespType {
        match self {
            RespData::SimpleString(_) => RespType::SimpleString,
            RespData::Error(_) => RespType::Error,
            RespData::Integer(_) => RespType::Integer,
            RespData::BulkString(_) => RespType::BulkString,
            RespData::Array(_) => RespType::Array,
            RespData::Inline(_) => RespType::Inline,
            RespData::Null => RespType::Null,
            RespData::Boolean(_) => RespType::Boolean,
            RespData::Double(_) => RespType::Double,
            RespData::BulkError(_) => RespType::BulkError,
            RespData::VerbatimString { .. } => RespType::VerbatimString,
            RespData::BigNumber(_) => RespType::BigNumber,
            RespData::Map(_) => RespType::Map,
            RespData::Set(_) => RespType::Set,
            RespData::Push(_) => RespType::Push,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            RespData::SimpleString(bytes) => String::from_utf8(bytes.to_vec()).ok(),
            RespData::Error(bytes) => String::from_utf8(bytes.to_vec()).ok(),
            RespData::Integer(num) => Some(num.to_string()),
            RespData::BulkString(Some(bytes)) => String::from_utf8(bytes.to_vec()).ok(),
            RespData::BulkString(None) => None,
            RespData::Inline(parts) if !parts.is_empty() => {
                String::from_utf8(parts[0].to_vec()).ok()
            }
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<Bytes> {
        match self {
            RespData::SimpleString(bytes) => Some(bytes.clone()),
            RespData::Error(bytes) => Some(bytes.clone()),
            RespData::Integer(num) => Some(Bytes::from(num.to_string())),
            RespData::BulkString(Some(bytes)) => Some(bytes.clone()),
            RespData::BulkString(None) => None,
            RespData::Inline(parts) if !parts.is_empty() => Some(parts[0].clone()),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespData::Integer(num) => Some(*num),
            RespData::SimpleString(bytes) => {
                std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok())
            }
            RespData::BulkString(Some(bytes)) => {
                std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }
}

impl fmt::Debug for RespData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespData::SimpleString(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    write!(f, "SimpleString(\"{s}\")")
                } else {
                    write!(f, "SimpleString({bytes:?})")
                }
            }
            RespData::Error(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    write!(f, "Error(\"{s}\")")
                } else {
                    write!(f, "Error({bytes:?})")
                }
            }
            RespData::Integer(num) => write!(f, "Integer({num})"),
            RespData::BulkString(Some(bytes)) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    write!(f, "BulkString(\"{s}\")")
                } else {
                    write!(f, "BulkString({bytes:?})")
                }
            }
            RespData::BulkString(None) => write!(f, "BulkString(nil)"),
            RespData::Array(Some(array)) => write!(f, "Array({array:?})"),
            RespData::Array(None) => write!(f, "Array(nil)"),
            RespData::Inline(parts) => {
                write!(f, "Inline(")?;
                let parts_str: Vec<_> = parts
                    .iter()
                    .filter_map(|b| std::str::from_utf8(b).ok())
                    .collect();
                write!(f, "{parts_str:?}")?;
                write!(f, ")")
            }
            RespData::Null => write!(f, "Null"),
            RespData::Boolean(b) => write!(f, "Boolean({b})"),
            RespData::Double(d) => write!(f, "Double({d})"),
            RespData::BulkError(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    write!(f, "BulkError(\"{s}\")")
                } else {
                    write!(f, "BulkError({bytes:?})")
                }
            }
            RespData::VerbatimString { format, data } => {
                if let Ok(s) = std::str::from_utf8(data) {
                    let fmt = std::str::from_utf8(&format[..]).unwrap_or("???");
                    write!(f, "VerbatimString({fmt}:{s})")
                } else {
                    write!(f, "VerbatimString({:?}:{:?})", format, data)
                }
            }
            RespData::BigNumber(s) => write!(f, "BigNumber({s})"),
            RespData::Map(entries) => write!(f, "Map(len={})", entries.len()),
            RespData::Set(items) => write!(f, "Set(len={})", items.len()),
            RespData::Push(items) => write!(f, "Push(len={})", items.len()),
        }
    }
}
