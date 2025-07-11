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

#![cfg_attr(not(test), allow(dead_code))]

use crate::coding::{decode_fixed, encode_fixed};
use crate::error::Result;
use crate::storage_define::{
    decode_user_key, encode_user_key, ENCODED_KEY_DELIM_SIZE, NEED_TRANSFORM_CHARACTER,
};
use bytes::BytesMut;
use std::mem;

// Constants for fixed-length fields
const RESERVE1_LEN: usize = 8;
const RESERVE2_LEN: usize = 16;
const U64_LEN: usize = 8;

/*
 * 用于 List 数据 key 的格式
 * | reserve1 | key | version | index | reserve2 |
 * |    8B    |     |    8B   |   8B  |   16B    |
 */
pub struct ListsDataKey {
    start: Option<Vec<u8>>,
    space: [u8; 200],
    reserve1: [u8; 8],
    key: Vec<u8>,
    version: u64,
    index: u64,
    reserve2: [u8; 16],
}

impl ListsDataKey {
    pub fn new(key: &[u8], version: u64, index: u64) -> Self {
        Self {
            start: None,
            space: [0; 200],
            reserve1: [0; 8],
            key: key.to_vec(),
            version,
            index,
            reserve2: [0; 16],
        }
    }

    #[inline]
    pub fn encode(&mut self) -> Result<&[u8]> {
        let meta_size = self.reserve1.len() + mem::size_of::<u64>() + self.reserve2.len();
        let mut encoded_size = self.key.len() + mem::size_of::<u64>() + ENCODED_KEY_DELIM_SIZE;
        let nzero = self
            .key
            .iter()
            .filter(|&&c| c == NEED_TRANSFORM_CHARACTER as u8)
            .count();
        encoded_size += nzero;
        let needed = meta_size + encoded_size;

        let dst = if needed <= self.space.len() {
            &mut self.space[..needed]
        } else {
            self.start = Some(vec![0; needed]);
            self.start.as_mut().unwrap()
        };

        let mut offset = 0;

        // reserve1: 8 byte
        dst[offset..offset + self.reserve1.len()].copy_from_slice(&self.reserve1);
        offset += self.reserve1.len();

        // encode user key
        let mut temp_buf = BytesMut::with_capacity(self.key.len() + nzero + ENCODED_KEY_DELIM_SIZE);
        encode_user_key(&self.key, &mut temp_buf)?;
        let encoded_key = temp_buf.as_ref();
        dst[offset..offset + encoded_key.len()].copy_from_slice(encoded_key);
        offset += encoded_key.len();

        // version 8 byte
        let version_slice = &mut dst[offset..offset + mem::size_of::<u64>()];
        encode_fixed(version_slice.as_mut_ptr(), self.version);
        offset += mem::size_of::<u64>();

        // index 8 byte
        let index_slice = &mut dst[offset..offset + mem::size_of::<u64>()];
        encode_fixed(index_slice.as_mut_ptr(), self.index);
        offset += mem::size_of::<u64>();

        // reserve2: 16 byte
        dst[offset..offset + self.reserve2.len()].copy_from_slice(&self.reserve2);

        Ok(if needed <= self.space.len() {
            &self.space[..needed]
        } else {
            self.start.as_ref().unwrap()
        })
    }
}

pub struct ParsedListsDataKey {
    key_str: Vec<u8>,
    reserve1: [u8; 8],
    version: u64,
    index: u64,
    reserve2: [u8; 16],
}

impl ParsedListsDataKey {
    pub fn from_string(key: &str) -> Result<Self> {
        Self::decode(key.as_bytes())
    }

    pub fn from_slice(key: &[u8]) -> Result<Self> {
        Self::decode(key)
    }

    #[inline]
    pub fn decode(key: &[u8]) -> Result<Self> {
        // basic length check using constants for clarity
        let min_len = RESERVE1_LEN + RESERVE2_LEN;
        if key.len() < min_len {
            return Err(crate::error::Error::InvalidFormat {
                message: "Key too short for reserve fields".to_string(),
                location: snafu::location!(),
            });
        }

        // skip head reserve1 and tail reserve2
        let encoded_key_start = RESERVE1_LEN;
        let encoded_key_end = key.len() - RESERVE2_LEN;
        let encoded_key_slice = &key[encoded_key_start..encoded_key_end];

        // find the encoded key delimiter ("\x00\x00") efficiently
        let pos = encoded_key_slice
            .windows(ENCODED_KEY_DELIM_SIZE)
            .position(|window| window == b"\x00\x00")
            .map(|p| p + ENCODED_KEY_DELIM_SIZE)
            .ok_or_else(|| crate::error::Error::InvalidFormat {
                message: "Encoded key delimiter not found".to_string(),
                location: snafu::location!(),
            })?;

        // 解码 user key
        let mut key_str_buf = BytesMut::with_capacity(pos);
        decode_user_key(&encoded_key_slice[..pos], &mut key_str_buf)?;
        let key_str = key_str_buf.to_vec();

        // version & index follow immediately after the encoded key
        let version_offset = encoded_key_start + pos;
        let index_offset = version_offset + U64_LEN;

        // ensure we have enough bytes left for version and index
        if index_offset + U64_LEN > encoded_key_end {
            return Err(crate::error::Error::InvalidFormat {
                message: "Key too short for version/index fields".to_string(),
                location: snafu::location!(),
            });
        }

        let version =
            decode_fixed(key[version_offset..version_offset + U64_LEN].as_ptr() as *mut u8);
        let index = decode_fixed(key[index_offset..index_offset + U64_LEN].as_ptr() as *mut u8);

        // sanity check: we should end exactly before RESERVE2
        if index_offset + U64_LEN != encoded_key_end {
            return Err(crate::error::Error::InvalidFormat {
                message: "Unexpected bytes between index and reserve2".to_string(),
                location: snafu::location!(),
            });
        }

        Ok(Self {
            key_str,
            reserve1: [0; RESERVE1_LEN],
            version,
            index,
            reserve2: [0; RESERVE2_LEN],
        })
    }

    pub fn key(&self) -> &[u8] {
        &self.key_str
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn index(&self) -> u64 {
        self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn test_encode_decode() -> Result<()> {
        let key = b"test\x00key";
        let version = 123;
        let index = 456;

        let mut data_key = ListsDataKey::new(key, version, index);
        let encoded = data_key.encode()?;

        let parsed = ParsedListsDataKey::from_slice(encoded)?;

        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.index(), index);
        Ok(())
    }

    #[test]
    fn test_special_characters() -> Result<()> {
        let key = b"special\x00\x01\x00chars";
        let version = 999;
        let index = 888;

        let mut data_key = ListsDataKey::new(key, version, index);
        let encoded = data_key.encode()?;
        let parsed = ParsedListsDataKey::from_slice(encoded)?;

        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.index(), index);
        Ok(())
    }

    #[test]
    fn test_empty_key() -> Result<()> {
        let key = b"";
        let version = 0;
        let index = 0;

        let mut data_key = ListsDataKey::new(key, version, index);
        let encoded = data_key.encode()?;
        let parsed = ParsedListsDataKey::from_slice(encoded)?;

        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.index(), index);
        Ok(())
    }

    #[test]
    fn test_invalid_encoding() {
        let invalid_data = b"invalid\x00\x02data";
        let result = ParsedListsDataKey::from_slice(invalid_data);
        assert!(matches!(result, Err(Error::InvalidFormat { .. })));
    }
}
