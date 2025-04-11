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

use crate::kstd::slice::Slice;

pub const PREFIX_RESERVE_LENGTH: usize = 8;
// const VERSION_LENGTH: usize = 8;
// const SCORE_LENGTH: usize = 8;
pub const SUFFIX_RESERVE_LENGTH: usize = 16;
// const LIST_VALUE_INDEX_LENGTH: usize = 16;

// used to store a fixed-size value for the Type field.
// const TYPE_LENGTH: usize = 1;
// const TIMESTAMP_LENGTH: usize = 8;

// TODO: maybe we can change \u{0000} to \0,
// it will be more readable.
pub const NEED_TRANSFORM_CHARACTER: char = '\u{0000}';
pub const ENCODED_TRANSFORM_CHARACTER: &str = "\u{0000}\u{0001}";
pub const ENCODED_KEY_DELIM: &str = "\u{0000}\u{0000}";
pub const ENCODED_KEY_DELIM_SIZE: usize = 2;

/// Encode user key
///
/// Parameters:
/// - user_key: Original user key data
/// - dst: Destination buffer
/// - nzero: Number of zero bytes contained in the key
///
/// Returns: Pointer to the new encoded location
pub fn encode_user_key(user_key: &Slice, mut dst: *mut u8, nzero: usize) -> *mut u8 {
    // no \u0000 exists in user_key, memcopy user_key directly.
    if nzero == 0 {
        unsafe {
            std::ptr::copy_nonoverlapping(user_key.data(), dst, user_key.size());
            dst = dst.add(user_key.size());
            std::ptr::copy_nonoverlapping(
                ENCODED_KEY_DELIM.as_bytes().as_ptr(),
                dst,
                ENCODED_KEY_DELIM_SIZE,
            );
            dst = dst.add(ENCODED_KEY_DELIM_SIZE);
        }
        return dst;
    }

    // \u0000 exists in user_key, iterate and replace.
    let mut pos = 0;
    let user_data = unsafe { std::slice::from_raw_parts(user_key.data(), user_key.size()) };
    for i in 0..user_key.size() {
        if user_data[i] == NEED_TRANSFORM_CHARACTER as u8 {
            let sub_len = i - pos;
            if sub_len != 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(user_data.as_ptr().add(pos), dst, sub_len);
                    dst = dst.add(sub_len);
                }
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    ENCODED_TRANSFORM_CHARACTER.as_bytes().as_ptr(),
                    dst,
                    ENCODED_TRANSFORM_CHARACTER.len(),
                );
                dst = dst.add(ENCODED_TRANSFORM_CHARACTER.len());
            }
            pos = i + 1;
        }
    }

    // Copy the remaining part
    if pos != user_key.size() {
        unsafe {
            std::ptr::copy_nonoverlapping(user_data.as_ptr().add(pos), dst, user_key.size() - pos);
            dst = dst.add(user_key.size() - pos);
        }
    }

    // add delimiter
    unsafe {
        std::ptr::copy_nonoverlapping(
            ENCODED_KEY_DELIM.as_bytes().as_ptr(),
            dst,
            ENCODED_KEY_DELIM_SIZE,
        );
        dst = dst.add(ENCODED_KEY_DELIM_SIZE);
    }

    dst
}

pub fn decode_user_key(ptr: *const u8, len: usize, user_key: &mut Vec<u8>) -> *const u8 {
    user_key.resize(len - ENCODED_KEY_DELIM_SIZE, 0);
    let mut zero_ahead = false;
    let mut delim_found = false;
    let mut output_idx = 0;
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    let mut ret_ptr = ptr;
    for (idx, &byte) in slice.iter().enumerate() {
        match byte {
            0x00 => {
                delim_found = zero_ahead;
                zero_ahead = true;
            }
            0x01 => {
                user_key[output_idx] = if zero_ahead { 0x00 } else { byte };
                zero_ahead = false;
                output_idx += 1;
            }
            _ => {
                user_key[output_idx] = byte;
                zero_ahead = false;
                output_idx += 1;
            }
        }
        if delim_found {
            user_key.truncate(output_idx);
            ret_ptr = unsafe { ptr.add(idx + 1) };
            break;
        }
    }
    ret_ptr
}
