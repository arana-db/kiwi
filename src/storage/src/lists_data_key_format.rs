/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Format for List data key
 * | reserve1 | key | version | index | reserve2 |
 * |    8B    |     |    8B   |   8B  |   16B    |
 */
pub struct ListsDataKey {
    reserve1: [u8; 8],
    key: Vec<u8>,
    version: u64,
    index: u64,
    reserve2: [u8; 16],
}

impl ListsDataKey {
    pub fn new(key: &[u8], version: u64, index: u64) -> Self {
        Self::with_reserves(key, version, index, [0; 8], [0; 16])
    }

    pub fn with_reserves(
        key: &[u8],
        version: u64,
        index: u64,
        reserve1: [u8; 8],
        reserve2: [u8; 16],
    ) -> Self {
        Self {
            reserve1,
            key: key.to_vec(),
            version,
            index,
            reserve2,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        // Calculate how many bytes we will need.
        let meta_size = self.reserve1.len() + mem::size_of::<u64>() + self.reserve2.len();
        let mut encoded_size = self.key.len() + mem::size_of::<u64>() + ENCODED_KEY_DELIM_SIZE;

        // Each NEED_TRANSFORM_CHARACTER (0) is escaped as two bytes, so we need to
        // account for the extra space.
        let nzero = self
            .key
            .iter()
            .filter(|&&c| c == NEED_TRANSFORM_CHARACTER as u8)
            .count();
        encoded_size += nzero;

        let needed = meta_size + encoded_size;
        let mut dst = vec![0u8; needed];

        let mut offset = 0;

        // 1. reserve1 (8 bytes)
        dst[offset..offset + self.reserve1.len()].copy_from_slice(&self.reserve1);
        offset += self.reserve1.len();

        // 2. encoded user key
        let mut temp_buf = BytesMut::with_capacity(self.key.len() + nzero + ENCODED_KEY_DELIM_SIZE);
        encode_user_key(&self.key, &mut temp_buf)?;
        let encoded_key = temp_buf.as_ref();
        dst[offset..offset + encoded_key.len()].copy_from_slice(encoded_key);
        offset += encoded_key.len();

        // 3. version (8 bytes)
        encode_fixed(&mut dst[offset..offset + U64_LEN], self.version);
        offset += U64_LEN;

        // 4. index (8 bytes)
        encode_fixed(&mut dst[offset..offset + U64_LEN], self.index);
        offset += U64_LEN;

        // 5. reserve2 (16 bytes)
        dst[offset..offset + self.reserve2.len()].copy_from_slice(&self.reserve2);

        Ok(dst)
    }

    pub fn reserve1(&self) -> &[u8; 8] {
        &self.reserve1
    }

    pub fn reserve2(&self) -> &[u8; 16] {
        &self.reserve2
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

        // decode user key
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

        let version = decode_fixed(&key[version_offset..version_offset + U64_LEN]);
        let index = decode_fixed(&key[index_offset..index_offset + U64_LEN]);

        // sanity check: we should end exactly before RESERVE2
        if index_offset + U64_LEN != encoded_key_end {
            return Err(crate::error::Error::InvalidFormat {
                message: "Unexpected bytes between index and reserve2".to_string(),
                location: snafu::location!(),
            });
        }

        // Read reserve1 from the beginning of the key
        let reserve1 =
            key[..RESERVE1_LEN]
                .try_into()
                .map_err(|_| crate::error::Error::InvalidFormat {
                    message: "Failed to read reserve1 field".to_string(),
                    location: snafu::location!(),
                })?;

        // Read reserve2 from the end of the key
        let reserve2 =
            key[encoded_key_end..]
                .try_into()
                .map_err(|_| crate::error::Error::InvalidFormat {
                    message: "Failed to read reserve2 field".to_string(),
                    location: snafu::location!(),
                })?;

        Ok(Self {
            key_str,
            reserve1,
            version,
            index,
            reserve2,
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

    pub fn reserve1(&self) -> &[u8; 8] {
        &self.reserve1
    }

    pub fn reserve2(&self) -> &[u8; 16] {
        &self.reserve2
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

        let data_key = ListsDataKey::new(key, version, index);
        let encoded = data_key.encode()?;

        let parsed = ParsedListsDataKey::from_slice(&encoded)?;

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

        let data_key = ListsDataKey::new(key, version, index);
        let encoded = data_key.encode()?;
        let parsed = ParsedListsDataKey::from_slice(&encoded)?;

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

        let data_key = ListsDataKey::new(key, version, index);
        let encoded = data_key.encode()?;
        let parsed = ParsedListsDataKey::from_slice(&encoded)?;

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

    #[test]
    fn test_reserve_fields_round_trip() -> Result<()> {
        let key = b"test_key";
        let version = 123;
        let index = 456;
        let reserve1 = [1, 2, 3, 4, 5, 6, 7, 8];
        let reserve2 = [
            9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        ];

        let data_key = ListsDataKey::with_reserves(key, version, index, reserve1, reserve2);
        let encoded = data_key.encode()?;

        let parsed = ParsedListsDataKey::from_slice(&encoded)?;

        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.index(), index);
        assert_eq!(parsed.reserve1(), &reserve1);
        assert_eq!(parsed.reserve2(), &reserve2);
        Ok(())
    }

    #[test]
    fn test_reserve_fields_with_zeros() -> Result<()> {
        let key = b"test_key";
        let version = 789;
        let index = 101;
        let reserve1 = [0; 8];
        let reserve2 = [0; 16];

        let data_key = ListsDataKey::with_reserves(key, version, index, reserve1, reserve2);
        let encoded = data_key.encode()?;

        let parsed = ParsedListsDataKey::from_slice(&encoded)?;

        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.index(), index);
        assert_eq!(parsed.reserve1(), &reserve1);
        assert_eq!(parsed.reserve2(), &reserve2);
        Ok(())
    }
}
