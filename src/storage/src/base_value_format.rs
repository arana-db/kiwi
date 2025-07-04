// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//  of patent rights can be found in the PATENTS file in the same directory.

use crate::error::{Error, InvalidFormatSnafu, Result};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;
use snafu::OptionExt;
use std::ops::Range;

/// TODO: remove allow dead code
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String = 0,
    Hash = 1,
    Set = 2,
    List = 3,
    ZSet = 4,
    None = 5,
    All = 6,
}

// TODO: use unified Result
impl TryFrom<u8> for DataType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(DataType::String),
            1 => Ok(DataType::Hash),
            2 => Ok(DataType::Set),
            3 => Ok(DataType::List),
            4 => Ok(DataType::ZSet),
            5 => Ok(DataType::None),
            6 => Ok(DataType::All),
            _ => InvalidFormatSnafu {
                message: format!("Invalid data type byte: {value}"),
            }
            .fail(),
        }
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
pub const DATA_TYPE_STRINGS: [&str; 7] = ["string", "hash", "set", "list", "zset", "none", "all"];
/// TODO: remove allow dead code
#[allow(dead_code)]
pub const DATA_TYPE_TAG: [char; 7] = ['k', 'h', 's', 'l', 'z', 'n', 'a'];

/// TODO: remove allow dead code
#[allow(dead_code)]
pub fn data_type_to_string(data_type: DataType) -> &'static str {
    DATA_TYPE_STRINGS[data_type as usize]
}

/// TODO: remove allow dead code
#[allow(dead_code)]
pub fn data_type_to_tag(data_type: DataType) -> char {
    DATA_TYPE_TAG[data_type as usize]
}

/// TODO: remove allow dead code
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InternalValue {
    pub data_type: DataType,
    pub user_value: Bytes,
    pub version: u64,
    pub etime: u64,
    pub ctime: u64,
    pub reserve: [u8; 16],
}

impl InternalValue {
    pub fn new<T>(data_type: DataType, user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            data_type,
            user_value: user_value.into(),
            version: 0,
            etime: 0,
            ctime: Utc::now().timestamp_micros() as u64,
            reserve: [0; 16],
        }
    }

    pub fn set_etime(&mut self, etime: u64) {
        self.etime = etime
    }

    pub fn set_ctime(&mut self, ctime: u64) {
        self.ctime = ctime
    }

    pub fn set_version(&mut self, version: u64) {
        self.version = version
    }

    pub fn set_relative_etime(&mut self, ttl: u64) -> Result<()> {
        let current_micros = Utc::now().timestamp_micros() as u64;
        self.etime = current_micros
            .checked_add(ttl)
            .context(InvalidFormatSnafu {
                message: "Timestamp overflow when calculating relative etime".to_string(),
            })?;
        Ok(())
    }
}

/// This macro is used to forward the base function to the structure
/// so that it can call the function directly（string_value.set_etime()） without calling it like "string_value.base.user_value()"
#[macro_export]
macro_rules! delegate_internal_value {
    ($struct_name:ident) => {
        impl $struct_name {
            #[allow(dead_code)]
            pub fn set_etime(&mut self, etime: u64) {
                self.inner.set_etime(etime);
            }

            #[allow(dead_code)]
            pub fn set_ctime(&mut self, ctime: u64) {
                self.inner.set_ctime(ctime);
            }

            #[allow(dead_code)]
            pub fn set_version(&mut self, version: u64) {
                self.inner.set_version(version);
            }

            #[allow(dead_code)]
            pub fn set_relative_etime(&mut self, ttl: u64) -> Result<()> {
                self.inner.set_relative_etime(ttl)
            }
        }
    };
}

/// TODO: remove allow dead code
#[allow(dead_code)]
pub struct ParsedInternalValue {
    pub value: BytesMut,
    pub data_type: DataType,
    /// When used to represent MetaValue, the 'user_value' field is decoded to 'count' or 'len'.
    pub user_value_range: Range<usize>,
    pub reserve_range: Range<usize>,
    pub version: u64,
    pub ctime: u64,
    pub etime: u64,
}

#[allow(dead_code)]
impl ParsedInternalValue {
    pub fn new(
        value: BytesMut,
        data_type: DataType,
        user_value_range: Range<usize>,
        reserve_range: Range<usize>,
        version: u64,
        ctime: u64,
        etime: u64,
    ) -> Self {
        Self {
            value,
            data_type,
            user_value_range,
            reserve_range,
            version,
            ctime,
            etime,
        }
    }

    /// When used to represent MetaValue, this function will not be called
    pub fn user_value(&self) -> BytesMut {
        let slice = &self.value[self.user_value_range.clone()];
        let mut bytes_mut = BytesMut::with_capacity(slice.len());
        bytes_mut.put_slice(slice);
        bytes_mut
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn ctime(&self) -> u64 {
        self.ctime
    }

    pub fn etime(&self) -> u64 {
        self.etime
    }

    pub fn is_permanent_survival(&self) -> bool {
        self.etime == 0
    }

    pub fn is_stale(&self) -> bool {
        if self.etime == 0 {
            return false;
        }
        let current_micros = Utc::now().timestamp_micros() as u64;
        self.etime < current_micros
    }

    pub fn is_valid(&self) -> bool {
        !self.is_stale()
    }
}

/// This macro is used to forward the base function to the structure
/// so that it can call the function directly（parsed_value.user_value()） without calling it like "string.base.user_value()"
#[macro_export]
macro_rules! delegate_parsed_value {
    ($struct_name:ident) => {
        impl $struct_name {
            #[allow(dead_code)]
            pub fn etime(&self) -> u64 {
                self.inner.etime()
            }

            #[allow(dead_code)]
            pub fn ctime(&self) -> u64 {
                self.inner.ctime()
            }

            #[allow(dead_code)]
            pub fn is_stale(&self) -> bool {
                self.inner.is_stale()
            }

            #[allow(dead_code)]
            pub fn is_permanent_survival(&self) -> bool {
                self.inner.is_permanent_survival()
            }

            #[allow(dead_code)]
            pub fn user_value(&self) -> BytesMut {
                self.inner.user_value()
            }

            #[allow(dead_code)]
            pub fn version(&self) -> u64 {
                self.inner.version()
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_to_string() {
        assert_eq!(data_type_to_string(DataType::String), "string");
        assert_eq!(data_type_to_string(DataType::Hash), "hash");
        assert_eq!(data_type_to_string(DataType::Set), "set");
        assert_eq!(data_type_to_string(DataType::List), "list");
        assert_eq!(data_type_to_string(DataType::ZSet), "zset");
        assert_eq!(data_type_to_string(DataType::None), "none");
        assert_eq!(data_type_to_string(DataType::All), "all");
    }

    #[test]
    fn test_data_type_to_tag() {
        assert_eq!(data_type_to_tag(DataType::String), 'k');
        assert_eq!(data_type_to_tag(DataType::Hash), 'h');
        assert_eq!(data_type_to_tag(DataType::Set), 's');
        assert_eq!(data_type_to_tag(DataType::List), 'l');
        assert_eq!(data_type_to_tag(DataType::ZSet), 'z');
        assert_eq!(data_type_to_tag(DataType::None), 'n');
        assert_eq!(data_type_to_tag(DataType::All), 'a');
    }
}
