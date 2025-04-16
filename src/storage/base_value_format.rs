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

use crate::kstd::env::now_micros;
use crate::kstd::slice::Slice;

/// TODO: remove allow dead code
#[allow(dead_code)]
pub enum DataType {
    String = 0,
    Hash = 1,
    Set = 2,
    List = 3,
    ZSet = 4,
    None = 5,
    All = 6,
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

pub struct InternalValue {
    pub start: *mut u8,
    pub space: Vec<u8>,

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub typ: DataType,

    pub user_value: Slice,
    pub version: u64,
    pub etime: u64, // expire time
    pub ctime: u64, // create time
    pub reserve: [u8; 16],
}

impl InternalValue {
    pub fn new(typ: DataType, value: &Slice) -> Self {
        let mut internal_value = Self {
            start: std::ptr::null_mut(),
            space: Vec::with_capacity(200),
            typ,
            user_value: value.clone(),
            version: 0,
            etime: 0,
            ctime: now_micros(),
            reserve: [0; 16],
        };

        // Initialize `start` based on internal logic or availability
        internal_value.start = internal_value.space.as_mut_ptr();

        internal_value
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn set_etime(&mut self, etime: u64) {
        self.etime = etime;
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn set_ctime(&mut self, ctime: u64) {
        self.ctime = ctime;
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn set_relative_expire_time(&mut self, ttl: u64) {
        self.etime = now_micros() + ttl;
        println!("self.etime: {}", self.etime);
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

pub struct ParsedInternalValue {
    pub value: String,

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub typ: DataType,

    pub user_value: Slice,
    pub version: u64,
    pub ctime: u64,
    pub etime: u64,
    pub reserve: [u8; 16],
}

impl ParsedInternalValue {
    /// Use this constructor in rocksdb::CompactionFilter::Filter(),
    /// since we use this in Compaction process, all we need to do is parsing
    /// the rocksdb::Slice, so don't need to modify the original value, value_ can be
    /// set to nullptr
    pub fn new_with_slice(_value: &Slice) -> Self {
        Self {
            value: String::default(),
            typ: DataType::None,
            user_value: Slice::default(),
            version: 0,
            etime: 0,
            ctime: 0,
            reserve: [0; 16],
        }
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn user_value(&self) -> Slice {
        self.user_value.clone()
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn etime(&self) -> u64 {
        self.etime
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn set_permanent(&mut self) {
        self.etime = 0;
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn is_permanent(&self) -> bool {
        self.etime == 0
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn is_expired(&self) -> bool {
        if self.is_permanent() {
            return false;
        }
        let unix_time = now_micros();
        self.etime < unix_time
    }

    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn is_valid(&self) -> bool {
        !self.is_expired()
    }
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
