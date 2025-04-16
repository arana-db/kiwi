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

use crate::kstd::slice::Slice;
use crate::storage::base_value_format::{DataType, InternalValue, ParsedInternalValue};
use crate::storage::coding::{decode_fixed, encode_fixed};
use crate::storage::storage_define::{SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH};

use std::ops::{Deref, DerefMut};

/*
 * hash/set/zset/list data value format
 * | value | reserve | ctime |
 * |       |   16B   |   8B  |
 */

/// TODO: remove allow dead code
#[allow(dead_code)]
pub struct BaseDataValue {
    internal_value: InternalValue,
}

impl Deref for BaseDataValue {
    type Target = InternalValue;
    fn deref(&self) -> &InternalValue {
        &self.internal_value
    }
}

impl DerefMut for BaseDataValue {
    fn deref_mut(&mut self) -> &mut InternalValue {
        &mut self.internal_value
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl BaseDataValue {
    fn new(user_value: &Slice) -> Self {
        Self {
            internal_value: InternalValue::new(DataType::None, user_value),
        }
    }

    fn encode(&mut self) -> Slice {
        let user_value_size = self.user_value.size();
        // hash/set/zset/list data value format:
        //          |     value      |       reserve         |     ctime       |
        //          |                |         16B           |      8B         |
        let needed = user_value_size + SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;

        if needed > self.space.len() {
            self.space.resize(needed, 0);
        }

        let mut offset_ptr = self.space.as_mut_ptr();
        self.start = offset_ptr;

        unsafe {
            // copy user value
            std::ptr::copy_nonoverlapping(self.user_value.data(), offset_ptr, user_value_size);
            offset_ptr = offset_ptr.add(user_value_size);

            // copy reserve
            std::ptr::copy_nonoverlapping(self.reserve.as_ptr(), offset_ptr, SUFFIX_RESERVE_LENGTH);
            offset_ptr = offset_ptr.add(SUFFIX_RESERVE_LENGTH);

            // copy ctime
            encode_fixed(offset_ptr, self.ctime);
        }

        Slice::new(self.start, needed)
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
pub struct ParsedBaseDataValue {
    parsed_internal_value: ParsedInternalValue,
}

impl Deref for ParsedBaseDataValue {
    type Target = ParsedInternalValue;
    fn deref(&self) -> &ParsedInternalValue {
        &self.parsed_internal_value
    }
}

impl DerefMut for ParsedBaseDataValue {
    fn deref_mut(&mut self) -> &mut ParsedInternalValue {
        &mut self.parsed_internal_value
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl ParsedBaseDataValue {
    const BASEDATAVALUESUFFIXLENGTH: usize = SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;

    pub fn new(value: &Slice) -> Self {
        let mut parsed = ParsedInternalValue::new_with_slice(value);

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
                parsed.ctime = decode_fixed(target_ptr);
            }
        }

        Self {
            parsed_internal_value: parsed,
        }
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

    pub fn strip_suffix(&mut self) {
        let new_len = self.value.len() - Self::BASEDATAVALUESUFFIXLENGTH;
        self.value.truncate(new_len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_value_encode_and_decode() {
        let test_value = Slice::new_with_str("test_value");

        let mut value = BaseDataValue::new(&test_value);
        let encoded_data = value.encode();

        let decode_data = ParsedBaseDataValue::new(&encoded_data);

        assert_eq!(decode_data.user_value().as_bytes(), test_value.as_bytes());
    }
}
