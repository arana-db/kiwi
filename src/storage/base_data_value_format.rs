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
use crate::storage::base_value_format::DataType;
use crate::storage::base_value_format::InternalValue;

use super::{
    coding::{decode_fixed, encode_fixed},
    storage_define::{SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH},
};

/*
 * hash/set/zset/list data value format
 * | value | reserve | ctime |
 * |       |   16B   |   8B  |
 */

/// TODO: remove allow dead code
#[allow(dead_code)]
pub struct BaseDataValue {
    start: *mut u8,
    space: [u8; 200],
    typ: DataType,
    user_value: Slice,
    version: u64,
    etime: u64, // expire time
    ctime: u64, // create time
    reserve: [u8; 16],
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl BaseDataValue {
    const DEFAULT_VALUE_SUFFIX_LEN: usize = SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;

    pub fn new(typ: DataType, value: &Slice) -> Self {
        let mut internal_value = BaseDataValue {
            start: std::ptr::null_mut(),
            space: [0; 200],
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

    pub fn set_etime(&mut self, etime: u64) {
        self.etime = etime;
    }

    pub fn set_ctime(&mut self, ctime: u64) {
        self.ctime = ctime;
    }

    pub fn set_relative_expire_time(&mut self, ttl: u64) {
        self.etime = now_micros() + ttl;
        println!("self.etime: {}", self.etime);
    }

    pub fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

impl InternalValue for BaseDataValue {
    fn encode(&mut self) -> Slice {
        let user_value_size = self.user_value.size();
        // hash/set/zset/list data value format:
        //           |    value      |       reserve         |     ctime       |
        //           |               |         16B           |      8B         |
        let needed = user_value_size + SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;
        let mut offset_ptr: *mut u8;

        if needed <= self.space.len() {
            offset_ptr = self.space.as_mut_ptr();
        } else {
            let new_space = vec![0u8; needed];
            offset_ptr = Box::into_raw(new_space.into_boxed_slice()) as *mut u8;
        }

        self.start = offset_ptr;

        // copy user value
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.user_value.data(),
                offset_ptr,
                self.user_value.size(),
            );
            offset_ptr = offset_ptr.add(self.user_value.size());
        }

        // copy reserve
        unsafe {
            std::ptr::copy(self.reserve.as_ptr(), offset_ptr, self.reserve.len());
            offset_ptr = offset_ptr.add(SUFFIX_RESERVE_LENGTH);
        }

        // copy ctime
        unsafe {
            encode_fixed(offset_ptr, self.ctime);
            let _ = offset_ptr.add(TIMESTAMP_LENGTH);
        }

        Slice::new(self.start, needed)
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
pub struct ParsedBaseDataValue {
    value: String,
    typ: DataType,
    user_value: Slice,
    version: u64,
    ctime: u64,
    etime: u64,
    reserve: [u8; 16],
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl ParsedBaseDataValue {
    const BASEDATAVALUESUFFIXLENGTH: usize = SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;

    pub fn new(value: &Slice) -> Self {
        let mut parsed = Self {
            value: String::default(),
            typ: DataType::None,
            user_value: Slice::default(),
            version: 0,
            etime: 0,
            ctime: 0,
            reserve: [0; 16],
        };

        if value.size() >= Self::BASEDATAVALUESUFFIXLENGTH {
            parsed.user_value =
                Slice::new(value.data(), value.size() - Self::BASEDATAVALUESUFFIXLENGTH);

            // decode reserve
            unsafe {
                let target_ptr = (value.data() as *mut u8).add(parsed.user_value.size());
                std::ptr::copy(parsed.reserve.as_ptr(), target_ptr, SUFFIX_RESERVE_LENGTH);
            }

            // decode ctime
            unsafe {
                let target_ptr =
                    (value.data() as *mut u8).add(parsed.user_value.size() + SUFFIX_RESERVE_LENGTH);
                let length = std::mem::size_of::<u64>();
                let buf = std::slice::from_raw_parts(target_ptr, length);
                parsed.ctime = decode_fixed::<u64>(buf);
            }
        }

        parsed
    }

    pub fn user_value(&self) -> Slice {
        self.user_value.clone()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn etime(&self) -> u64 {
        self.etime
    }

    pub fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    pub fn set_etime(&mut self, etime: u64) {
        self.etime = etime;
    }

    pub fn set_ctime(&mut self, ctime: u64) {
        self.ctime = ctime;
        self.set_ctime_to_value();
    }

    pub fn set_ctime_to_value(&self) {
        let value_size = self.value.len();
        unsafe {
            let dst = self.value.as_ptr().add(value_size - TIMESTAMP_LENGTH);
            encode_fixed(dst as *mut u8, self.ctime);
        }
    }

    pub fn set_reserve_to_value(&self) {
        let value_size = self.value.len();
        unsafe {
            let dst = self
                .value
                .as_ptr()
                .add(value_size - Self::BASEDATAVALUESUFFIXLENGTH) as *mut u8;
            std::ptr::copy(self.reserve.as_ptr(), dst, self.reserve.len());
        }
    }

    pub fn set_permanent(&mut self) {
        self.etime = 0;
    }

    pub fn is_permanent(&self) -> bool {
        self.etime == 0
    }

    pub fn is_expired(&self) -> bool {
        if self.is_permanent() {
            return false;
        }
        let unix_time = now_micros();
        self.etime < unix_time
    }

    pub fn is_valid(&self) -> bool {
        !self.is_expired()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_value_encode_and_decode() {
        let test_value = Slice::new_with_str("test_value");

        let mut value = BaseDataValue::new(DataType::None, &test_value);
        let encoded_data = value.encode();

        let decode_key = ParsedBaseDataValue::new(&encoded_data);

        assert_eq!(decode_key.user_value().as_bytes(), test_value.as_bytes());
    }
}
