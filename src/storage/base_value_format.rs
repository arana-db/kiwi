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

use bytes::BytesMut;
use rocksdb::CompactionDecision;

use crate::storage::error::{Error, InvalidFormatSnafu, Result};

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
pub trait InternalValue {
    fn encode(&self) -> BytesMut;
    fn set_etime(&mut self, etime: u64);
    fn set_ctime(&mut self, ctime: u64);
    fn set_relative_etime(&mut self, ttl: u64);
}

/// TODO: remove allow dead code
#[allow(dead_code)]
pub trait ParsedInternalValue<'a>: Sized {
    fn new(data: &'a [u8]) -> Result<Self>;
    fn filter_decision(&self, current_time: u64) -> CompactionDecision;
    fn data_type(&self) -> DataType;
    fn user_value(&self) -> &[u8];
    fn version(&self) -> Option<u64> {
        None
    }
    fn ctime(&self) -> u64;
    fn etime(&self) -> u64;
    fn reserve(&self) -> &[u8];
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
