//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::storage::storage_define::{
    ENCODED_KEY_DELIM_SIZE, NEED_TRANSFORM_CHARACTER, PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH,
    decode_user_key, encode_user_key,
};

use crate::kstd::slice::Slice;

//
// used for string data key or hash/zset/set/list's meta key. format:
// | reserve1 | key | reserve2 |
// |    8B    |     |   16B    |
//

/// TODO: remove allow dead code
#[allow(dead_code)]
struct BaseKey {
    start: *mut u8,
    space: [u8; 200],
    reserve1: [u8; 8],
    key: Slice,
    reserve2: [u8; 16],
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl BaseKey {
    pub fn new(key: &Slice) -> Self {
        let mut base_key = BaseKey {
            start: std::ptr::null_mut(),
            space: [0; 200],
            reserve1: [0; 8],
            key: key.clone(),
            reserve2: [0; 16],
        };

        // Initialize `start` based on internal logic or availability
        base_key.start = base_key.space.as_mut_ptr();

        base_key
    }

    fn encode(&mut self) -> &[u8] {
        let meta_size = self.reserve1.len() + self.reserve2.len();
        let zero_num = self.key.count_byte(NEED_TRANSFORM_CHARACTER as u8);
        let encode_key_size = zero_num + ENCODED_KEY_DELIM_SIZE + self.key.size();
        let needed = meta_size + encode_key_size;
        let mut offset_ptr: *mut u8;

        if needed <= self.space.len() {
            offset_ptr = self.space.as_mut_ptr();
        } else {
            let new_space = vec![0u8; needed];
            offset_ptr = Box::into_raw(new_space.into_boxed_slice()) as *mut u8;
        }

        self.start = offset_ptr;
        // copy reserve1
        unsafe { std::ptr::copy(self.reserve1.as_ptr(), self.start, self.reserve1.len()) }

        unsafe {
            offset_ptr = offset_ptr.add(self.reserve1.len());
        }

        // encode user key
        offset_ptr = encode_user_key(&self.key, offset_ptr, zero_num);

        // copy reserve2
        unsafe { std::ptr::copy(self.reserve2.as_ptr(), offset_ptr, self.reserve2.len()) }

        // return encode slice
        unsafe { std::slice::from_raw_parts(self.start, needed) }
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
pub struct ParsedBaseKey {
    key_str: Vec<u8>,
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl ParsedBaseKey {
    pub fn new(encoded_key: &[u8]) -> Self {
        let mut key_str = Vec::new();
        Self::decode(encoded_key, &mut key_str);
        ParsedBaseKey { key_str }
    }

    fn decode(encoded_key: &[u8], key_str: &mut Vec<u8>) {
        if encoded_key.len() > (PREFIX_RESERVE_LENGTH + SUFFIX_RESERVE_LENGTH) {
            let start_idx = PREFIX_RESERVE_LENGTH;
            let end_idx = encoded_key.len() - SUFFIX_RESERVE_LENGTH;
            let data_slice = &encoded_key[start_idx..end_idx];
            decode_user_key(data_slice.as_ptr(), data_slice.len(), key_str);
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key_str
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_key_encode_and_decode() {
        let test_key = Slice::new_with_str("test_key");

        let mut base_key = BaseKey::new(&test_key);
        let encoded_data = base_key.encode();

        // there is no zero, so no need to transform.
        // reserve1 + key.size + delim.size + reserve2
        assert_eq!(
            encoded_data.len(),
            PREFIX_RESERVE_LENGTH
                + test_key.size()
                + ENCODED_KEY_DELIM_SIZE
                + SUFFIX_RESERVE_LENGTH
        );

        let additional_data: Vec<u8> = encoded_data.iter().copied().collect();
        let encoded_str = std::str::from_utf8(&additional_data).unwrap();

        let decode_key = ParsedBaseKey::new(encoded_str.as_bytes());

        assert_eq!(decode_key.key(), test_key.as_bytes());
    }
}
