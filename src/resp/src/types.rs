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
    // RESP3 types
    Null,
    Boolean,
    Double,
    BigNumber,
    BulkError,
    VerbatimString,
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
            // RESP3 types
            b'_' => Some(RespType::Null),
            b'#' => Some(RespType::Boolean),
            b',' => Some(RespType::Double),
            b'(' => Some(RespType::BigNumber),
            b'!' => Some(RespType::BulkError),
            b'=' => Some(RespType::VerbatimString),
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
            // RESP3 types
            RespType::Null => Some(b'_'),
            RespType::Boolean => Some(b'#'),
            RespType::Double => Some(b','),
            RespType::BigNumber => Some(b'('),
            RespType::BulkError => Some(b'!'),
            RespType::VerbatimString => Some(b'='),
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
    // RESP3 types
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(Bytes),
    BulkError(Bytes),
    VerbatimString { format: Bytes, data: Bytes },
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
            // RESP3 types
            RespData::Null => RespType::Null,
            RespData::Boolean(_) => RespType::Boolean,
            RespData::Double(_) => RespType::Double,
            RespData::BigNumber(_) => RespType::BigNumber,
            RespData::BulkError(_) => RespType::BulkError,
            RespData::VerbatimString { .. } => RespType::VerbatimString,
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
            // RESP3 types
            RespData::Null => None,
            RespData::Boolean(b) => Some(b.to_string()),
            RespData::Double(d) => Some(d.to_string()),
            RespData::BigNumber(bytes) => String::from_utf8(bytes.to_vec()).ok(),
            RespData::BulkError(bytes) => String::from_utf8(bytes.to_vec()).ok(),
            RespData::VerbatimString { data, .. } => String::from_utf8(data.to_vec()).ok(),
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
            // RESP3 types
            RespData::Null => None,
            RespData::Boolean(b) => Some(Bytes::from(b.to_string())),
            RespData::Double(d) => Some(Bytes::from(d.to_string())),
            RespData::BigNumber(bytes) => Some(bytes.clone()),
            RespData::BulkError(bytes) => Some(bytes.clone()),
            RespData::VerbatimString { data, .. } => Some(data.clone()),
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
            // RESP3 types
            RespData::Boolean(b) => Some(if *b { 1 } else { 0 }),
            RespData::Double(d) => Some(*d as i64),
            RespData::BigNumber(bytes) => {
                std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }

    /// Convert to boolean (RESP3 specific)
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            RespData::Boolean(b) => Some(*b),
            RespData::Integer(num) => Some(*num != 0),
            RespData::SimpleString(bytes) => {
                match std::str::from_utf8(bytes).ok()?.to_lowercase().as_str() {
                    "true" | "t" | "1" => Some(true),
                    "false" | "f" | "0" => Some(false),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Convert to double (RESP3 specific)
    pub fn as_double(&self) -> Option<f64> {
        match self {
            RespData::Double(d) => Some(*d),
            RespData::Integer(num) => Some(*num as f64),
            RespData::SimpleString(bytes) => {
                std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok())
            }
            RespData::BulkString(Some(bytes)) => {
                std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }

    /// Check if this is a null value
    pub fn is_null(&self) -> bool {
        matches!(
            self,
            RespData::Null | RespData::BulkString(None) | RespData::Array(None)
        )
    }

    // Convenience constructors for RESP3 types
    pub fn null() -> Self {
        RespData::Null
    }

    pub fn boolean(value: bool) -> Self {
        RespData::Boolean(value)
    }

    pub fn double(value: f64) -> Self {
        RespData::Double(value)
    }

    pub fn big_number<T: AsRef<str>>(value: T) -> Self {
        RespData::BigNumber(Bytes::from(value.as_ref().to_string()))
    }

    pub fn bulk_error<T: AsRef<[u8]>>(value: T) -> Self {
        RespData::BulkError(Bytes::copy_from_slice(value.as_ref()))
    }

    pub fn verbatim_string<F: AsRef<str>, D: AsRef<[u8]>>(format: F, data: D) -> Self {
        RespData::VerbatimString {
            format: Bytes::from(format.as_ref().to_string()),
            data: Bytes::copy_from_slice(data.as_ref()),
        }
    }

    pub fn map(pairs: Vec<(RespData, RespData)>) -> Self {
        RespData::Map(pairs)
    }

    pub fn set(items: Vec<RespData>) -> Self {
        RespData::Set(items)
    }

    pub fn push(items: Vec<RespData>) -> Self {
        RespData::Push(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resp3_convenience_constructors() {
        assert_eq!(RespData::null(), RespData::Null);
        assert_eq!(RespData::boolean(true), RespData::Boolean(true));
        assert_eq!(RespData::boolean(false), RespData::Boolean(false));
        assert_eq!(RespData::double(3.14), RespData::Double(3.14));

        let big_num = RespData::big_number("123456789");
        assert_eq!(big_num, RespData::BigNumber(Bytes::from("123456789")));

        let bulk_err = RespData::bulk_error("ERROR");
        assert_eq!(bulk_err, RespData::BulkError(Bytes::from("ERROR")));

        let verbatim = RespData::verbatim_string("txt", "Hello");
        assert_eq!(
            verbatim,
            RespData::VerbatimString {
                format: Bytes::from("txt"),
                data: Bytes::from("Hello"),
            }
        );
    }

    #[test]
    fn test_resp3_type_detection() {
        assert_eq!(RespData::Null.get_type(), RespType::Null);
        assert_eq!(RespData::Boolean(true).get_type(), RespType::Boolean);
        assert_eq!(RespData::Double(1.0).get_type(), RespType::Double);
        assert_eq!(
            RespData::BigNumber(Bytes::from("123")).get_type(),
            RespType::BigNumber
        );
        assert_eq!(
            RespData::BulkError(Bytes::from("ERR")).get_type(),
            RespType::BulkError
        );
        assert_eq!(
            RespData::VerbatimString {
                format: Bytes::from("txt"),
                data: Bytes::from("test")
            }
            .get_type(),
            RespType::VerbatimString
        );
        assert_eq!(RespData::Map(vec![]).get_type(), RespType::Map);
        assert_eq!(RespData::Set(vec![]).get_type(), RespType::Set);
        assert_eq!(RespData::Push(vec![]).get_type(), RespType::Push);
    }

    #[test]
    fn test_resp3_conversions() {
        // Test as_boolean
        assert_eq!(RespData::Boolean(true).as_boolean(), Some(true));
        assert_eq!(RespData::Boolean(false).as_boolean(), Some(false));
        assert_eq!(RespData::Integer(1).as_boolean(), Some(true));
        assert_eq!(RespData::Integer(0).as_boolean(), Some(false));
        assert_eq!(
            RespData::SimpleString(Bytes::from("true")).as_boolean(),
            Some(true)
        );
        assert_eq!(
            RespData::SimpleString(Bytes::from("false")).as_boolean(),
            Some(false)
        );

        // Test as_double
        assert_eq!(RespData::Double(3.14).as_double(), Some(3.14));
        assert_eq!(RespData::Integer(42).as_double(), Some(42.0));
        assert_eq!(
            RespData::SimpleString(Bytes::from("2.5")).as_double(),
            Some(2.5)
        );

        // Test as_integer with RESP3 types
        assert_eq!(RespData::Boolean(true).as_integer(), Some(1));
        assert_eq!(RespData::Boolean(false).as_integer(), Some(0));
        assert_eq!(RespData::Double(3.7).as_integer(), Some(3));
        assert_eq!(
            RespData::BigNumber(Bytes::from("999")).as_integer(),
            Some(999)
        );
    }

    #[test]
    fn test_resp3_string_conversions() {
        assert_eq!(RespData::Null.as_string(), None);
        assert_eq!(
            RespData::Boolean(true).as_string(),
            Some("true".to_string())
        );
        assert_eq!(
            RespData::Boolean(false).as_string(),
            Some("false".to_string())
        );
        assert_eq!(RespData::Double(3.14).as_string(), Some("3.14".to_string()));
        assert_eq!(
            RespData::BigNumber(Bytes::from("123")).as_string(),
            Some("123".to_string())
        );
        assert_eq!(
            RespData::BulkError(Bytes::from("ERR")).as_string(),
            Some("ERR".to_string())
        );
        assert_eq!(
            RespData::VerbatimString {
                format: Bytes::from("txt"),
                data: Bytes::from("hello")
            }
            .as_string(),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_resp3_bytes_conversions() {
        assert_eq!(RespData::Null.as_bytes(), None);
        assert_eq!(
            RespData::Boolean(true).as_bytes(),
            Some(Bytes::from("true"))
        );
        assert_eq!(RespData::Double(2.5).as_bytes(), Some(Bytes::from("2.5")));
        assert_eq!(
            RespData::BigNumber(Bytes::from("456")).as_bytes(),
            Some(Bytes::from("456"))
        );
        assert_eq!(
            RespData::BulkError(Bytes::from("ERROR")).as_bytes(),
            Some(Bytes::from("ERROR"))
        );
        assert_eq!(
            RespData::VerbatimString {
                format: Bytes::from("txt"),
                data: Bytes::from("world")
            }
            .as_bytes(),
            Some(Bytes::from("world"))
        );
    }

    #[test]
    fn test_is_null() {
        assert!(RespData::Null.is_null());
        assert!(RespData::BulkString(None).is_null());
        assert!(RespData::Array(None).is_null());
        assert!(!RespData::Boolean(false).is_null());
        assert!(!RespData::Integer(0).is_null());
        assert!(!RespData::SimpleString(Bytes::from("")).is_null());
    }

    #[test]
    fn test_resp_type_prefix_bytes() {
        assert_eq!(RespType::Null.prefix_byte(), Some(b'_'));
        assert_eq!(RespType::Boolean.prefix_byte(), Some(b'#'));
        assert_eq!(RespType::Double.prefix_byte(), Some(b','));
        assert_eq!(RespType::BigNumber.prefix_byte(), Some(b'('));
        assert_eq!(RespType::BulkError.prefix_byte(), Some(b'!'));
        assert_eq!(RespType::VerbatimString.prefix_byte(), Some(b'='));
        assert_eq!(RespType::Map.prefix_byte(), Some(b'%'));
        assert_eq!(RespType::Set.prefix_byte(), Some(b'~'));
        assert_eq!(RespType::Push.prefix_byte(), Some(b'>'));
    }

    #[test]
    fn test_resp_type_from_prefix() {
        assert_eq!(RespType::from_prefix(b'_'), Some(RespType::Null));
        assert_eq!(RespType::from_prefix(b'#'), Some(RespType::Boolean));
        assert_eq!(RespType::from_prefix(b','), Some(RespType::Double));
        assert_eq!(RespType::from_prefix(b'('), Some(RespType::BigNumber));
        assert_eq!(RespType::from_prefix(b'!'), Some(RespType::BulkError));
        assert_eq!(RespType::from_prefix(b'='), Some(RespType::VerbatimString));
        assert_eq!(RespType::from_prefix(b'%'), Some(RespType::Map));
        assert_eq!(RespType::from_prefix(b'~'), Some(RespType::Set));
        assert_eq!(RespType::from_prefix(b'>'), Some(RespType::Push));
        assert_eq!(RespType::from_prefix(b'@'), None); // Invalid prefix
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
            // RESP3 types
            RespData::Null => write!(f, "Null"),
            RespData::Boolean(b) => write!(f, "Boolean({b})"),
            RespData::Double(d) => write!(f, "Double({d})"),
            RespData::BigNumber(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    write!(f, "BigNumber(\"{s}\")")
                } else {
                    write!(f, "BigNumber({bytes:?})")
                }
            }
            RespData::BulkError(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    write!(f, "BulkError(\"{s}\")")
                } else {
                    write!(f, "BulkError({bytes:?})")
                }
            }
            RespData::VerbatimString { format, data } => {
                let format_str = std::str::from_utf8(format).unwrap_or("<invalid>");
                let data_str = std::str::from_utf8(data).unwrap_or("<invalid>");
                write!(
                    f,
                    "VerbatimString(format: \"{format_str}\", data: \"{data_str}\")"
                )
            }
            RespData::Map(pairs) => write!(f, "Map({pairs:?})"),
            RespData::Set(items) => write!(f, "Set({items:?})"),
            RespData::Push(items) => write!(f, "Push({items:?})"),
        }
    }
}
