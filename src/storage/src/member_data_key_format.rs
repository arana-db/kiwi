// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::{BufMut, Bytes, BytesMut};

use crate::storage_define::seek_userkey_delim;
use crate::{
    error::Result,
    storage_define::{
        ENCODED_KEY_DELIM_SIZE, PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH, decode_user_key,
        encode_user_key, encoded_user_key_len,
    },
};
// used for Hash/Set/Zset's member data key. format:
// | reserve1 | key | version | data | reserve2 |
// |    8B    |     |    8B   |      |   16B    |

#[derive(Debug, Clone)]
pub struct MemberDataKey {
    pub reserve1: [u8; 8],
    pub key: Bytes,
    pub version: u64,
    pub data: Bytes,
    pub reserve2: [u8; 16],
}

impl MemberDataKey {
    pub fn new(key: &[u8], version: u64, data: &[u8]) -> Self {
        MemberDataKey {
            reserve1: [0; PREFIX_RESERVE_LENGTH],
            key: Bytes::copy_from_slice(key),
            version,
            data: Bytes::copy_from_slice(data),
            reserve2: [0; SUFFIX_RESERVE_LENGTH],
        }
    }

    pub fn encode(&self) -> Result<BytesMut> {
        // Calculate exact capacity for better performance
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + encoded_user_key_len(&self.key)  // Precise encoded key length
            + size_of::<u64>()                 // version
            + self.data.len()                  // data
            + SUFFIX_RESERVE_LENGTH;           // reserve2
        let mut dst = BytesMut::with_capacity(estimated_cap);

        dst.put_slice(&self.reserve1);
        encode_user_key(&self.key, &mut dst)?;
        // Use little-endian for consistency with ZSetsScoreKey
        dst.put_u64_le(self.version);
        dst.put_slice(&self.data);
        dst.put_slice(&self.reserve2);
        Ok(dst)
    }

    /// Encode a seek key prefix for iteration
    pub fn encode_seek_key(&self) -> Result<BytesMut> {
        // Calculate exact capacity for better performance
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + encoded_user_key_len(&self.key)  // Precise encoded key length
            + size_of::<u64>()                 // version
            + self.data.len();                 // data (no reserve2 in seek key)
        let mut dst = BytesMut::with_capacity(estimated_cap);

        dst.put_slice(&self.reserve1);
        encode_user_key(&self.key, &mut dst)?;
        // Use little-endian for consistency with ZSetsScoreKey
        dst.put_u64_le(self.version);
        dst.put_slice(&self.data);
        Ok(dst)
    }
}

#[allow(dead_code)]
pub struct ParsedMemberDataKey {
    reserve1: [u8; 8],
    key_str: BytesMut,
    version: u64,
    data: Bytes,
    reserve2: [u8; 16],
}

#[allow(dead_code)]
impl ParsedMemberDataKey {
    pub fn new(encoded_key: &[u8]) -> Result<Self> {
        let mut key_str = BytesMut::new();

        let start_idx = PREFIX_RESERVE_LENGTH;
        let end_idx = encoded_key.len() - SUFFIX_RESERVE_LENGTH;

        // reserve1
        let reserve_slice = &encoded_key[0..PREFIX_RESERVE_LENGTH];
        let mut reserve1 = [0u8; PREFIX_RESERVE_LENGTH];
        reserve1.copy_from_slice(reserve_slice);

        // key
        let key_end_idx = start_idx + seek_userkey_delim(&encoded_key[start_idx..]);
        decode_user_key(&encoded_key[start_idx..key_end_idx], &mut key_str)?;

        // version (little-endian for consistency with ZSetsScoreKey)
        let version_end_idx = key_end_idx + size_of::<u64>();
        let version_slice = &encoded_key[key_end_idx..version_end_idx];
        let version = u64::from_le_bytes(version_slice.try_into().unwrap());

        // data
        let data_slice = &encoded_key[version_end_idx..end_idx];
        let data = Bytes::copy_from_slice(data_slice);

        // reserve2
        let reserve_slice = &encoded_key[end_idx..];
        let mut reserve2 = [0u8; SUFFIX_RESERVE_LENGTH];
        reserve2.copy_from_slice(reserve_slice);

        Ok(ParsedMemberDataKey {
            reserve1,
            key_str,
            version,
            data,
            reserve2,
        })
    }

    pub fn key(&self) -> &[u8] {
        self.key_str.as_ref()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn data(&self) -> &[u8] {
        self.data.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mv_test_member_data_key_encode_and_decode() {
        let test_key = b"hash_key\x00with_zero";
        let test_version: u64 = 42;
        let test_data = b"member_field";

        let data_key = MemberDataKey::new(test_key, test_version, test_data);
        let encoded_result = data_key.encode();
        assert!(
            encoded_result.is_ok(),
            "Encoding failed: {:?}",
            encoded_result.err()
        );
        let encoded = encoded_result.unwrap();

        // computed total length using actual encoded key length
        let mut encoded_key_buf = BytesMut::new();
        crate::storage_define::encode_user_key(test_key, &mut encoded_key_buf)
            .expect("encode key part failed");
        let expected_len = PREFIX_RESERVE_LENGTH
            + size_of::<u64>()
            + encoded_key_buf.len()
            + test_data.len()
            + SUFFIX_RESERVE_LENGTH;
        assert_eq!(encoded.len(), expected_len);

        // parse back
        let parsed_result = ParsedMemberDataKey::new(&encoded);
        assert!(
            parsed_result.is_ok(),
            "Decoding failed: {:?}",
            parsed_result.err()
        );
        let parsed = parsed_result.unwrap();
        assert_eq!(parsed.key(), test_key);
        assert_eq!(parsed.version(), test_version);

        // verify data region by slicing the encoded buffer directly
        let start_idx = PREFIX_RESERVE_LENGTH;
        let end_idx = encoded.len() - SUFFIX_RESERVE_LENGTH;
        let encoded_key_part = &encoded[start_idx..end_idx]; // key | version | data
        // find the position of delimiter in encoded key part ("\x00\x00")
        let mut pos = None;
        for i in 0..encoded_key_part.len().saturating_sub(1) {
            if encoded_key_part[i] == 0x00 && encoded_key_part[i + 1] == 0x00 {
                pos = Some(i + 2);
                break;
            }
        }
        let key_encoded_len = pos.expect("encoded key delimiter not found");
        let data_begin = PREFIX_RESERVE_LENGTH + size_of::<u64>() + key_encoded_len;
        let data_end = encoded.len() - SUFFIX_RESERVE_LENGTH;
        assert_eq!(&encoded[data_begin..data_end], test_data);
    }

    #[test]
    fn mv_test_member_data_key_with_empty_key_and_data() {
        let test_key = b"";
        let test_version: u64 = 0;
        let test_data = b"";

        let data_key = MemberDataKey::new(test_key, test_version, test_data);
        let encoded = data_key.encode().expect("encode failed");

        // length should be reserves + version(8) + encoded(empty key) + suffix
        let mut encoded_key_buf = BytesMut::new();
        crate::storage_define::encode_user_key(test_key, &mut encoded_key_buf)
            .expect("encode key part failed");
        let expected_len = PREFIX_RESERVE_LENGTH
            + size_of::<u64>()
            + encoded_key_buf.len()
            + SUFFIX_RESERVE_LENGTH;
        assert_eq!(encoded.len(), expected_len);

        let parsed = ParsedMemberDataKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.key(), test_key);
        assert_eq!(parsed.version(), test_version);

        // data is empty: verify indices collapse
        let data_begin = PREFIX_RESERVE_LENGTH + size_of::<u64>() + ENCODED_KEY_DELIM_SIZE;
        let data_end = encoded.len() - SUFFIX_RESERVE_LENGTH;
        assert!(data_begin == data_end);
    }
}
