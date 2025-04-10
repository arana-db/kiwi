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

//
// used for string data key or hash/zset/set/list's meta key. format:
// | reserve1 | key | reserve2 |
// |    8B    |     |   16B    |
//

struct BaseKey {
    start: *mut u8,
    space: [u8; 200],
    reserve1: [u8; 8],
    key: Vec<u8>,
    reserve2: [u8; 16],
}

impl BaseKey {
    pub fn new(key: Vec<u8>) -> Self {
        let mut base_key = BaseKey {
            start: std::ptr::null_mut(),
            space: [0; 200],
            reserve1: [0; 8],
            key,
            reserve2: [0; 16],
        };
        // Initialize `start` based on internal logic or availability
        base_key.start = base_key.space.as_mut_ptr();
        base_key
    }

    fn encode(&mut self) -> &[u8] {
        let meta_size = self.reserve1.len() + self.reserve2.len();
        let nzero = self
            .key
            .iter()
            .filter(|&&x| x == NEED_TRANSFORM_CHARACTER as u8)
            .count();
        let usize = nzero + ENCODED_KEY_DELIM_SIZE + self.key.len();
        let needed = meta_size + usize;
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
        offset_ptr = encode_user_key(self.key.as_slice(), offset_ptr, nzero);

        self.start = offset_ptr;
        // copy reserve2
        unsafe { std::ptr::copy(self.reserve2.as_ptr(), self.start, self.reserve2.len()) }

        // return encode slice
        unsafe { std::slice::from_raw_parts(self.start, needed) }
    }
}

pub struct ParsedBaseKey {
    key_str: Vec<u8>,
}

impl ParsedBaseKey {
    pub fn new(&mut self, key: &str) {
        let ptr = key.as_ptr();
        let end_ptr = unsafe { key.as_ptr().add(key.len()) };
        self.decode(ptr, end_ptr);
    }

    pub fn decode(&mut self, ptr: *const u8, end_ptr: *const u8) {
        unsafe {
            // skip head reserve1_
            let ptr = ptr.add(PREFIX_RESERVE_LENGTH);
            // skip tail reserve2_
            let end_ptr = end_ptr.sub(SUFFIX_RESERVE_LENGTH);

            let len = end_ptr.offset_from(ptr) as usize;

            decode_user_key(ptr, len, &mut self.key_str);
        }
    }

    pub fn key(&self) -> Vec<u8> {
        self.key_str.clone()
    }
}
