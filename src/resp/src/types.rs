/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use bytes::Bytes;
use std::fmt;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RespVersion {
    RESP1,
    #[default]
    RESP2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Inline,
}

impl RespType {
    pub fn from_prefix(byte: u8) -> Option<Self> {
        match byte {
            b'+' => Some(RespType::SimpleString),
            b'-' => Some(RespType::Error),
            b':' => Some(RespType::Integer),
            b'$' => Some(RespType::BulkString),
            b'*' => Some(RespType::Array),
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
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum RespData {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespData>>),
    Inline(Vec<Bytes>),
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
        }
    }
}
