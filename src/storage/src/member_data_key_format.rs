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

use crate::{
    error::Result,
    storage_define::{
        ENCODED_KEY_DELIM_SIZE, PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH, decode_user_key,
        encode_user_key,
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
        // TODO(marsevilspirit): allocate right memory size
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + self.key.len() * 2
            + size_of::<u64>()
            + self.data.len()
            + ENCODED_KEY_DELIM_SIZE
            + SUFFIX_RESERVE_LENGTH;
        let mut dst = BytesMut::with_capacity(estimated_cap);

        dst.put_slice(&self.reserve1);
        dst.put_u64(self.version);
        encode_user_key(&self.key, &mut dst)?;
        dst.put_slice(&self.data);
        dst.put_slice(&self.reserve2);
        Ok(dst)
    }

    /// Encode a seek key prefix for iteration
    pub fn encode_seek_key(&self) -> Result<BytesMut> {
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + self.key.len() * 2
            + size_of::<u64>()
            + self.data.len()
            + ENCODED_KEY_DELIM_SIZE;
        let mut dst = BytesMut::with_capacity(estimated_cap);

        dst.put_slice(&self.reserve1);
        dst.put_u64(self.version);
        encode_user_key(&self.key, &mut dst)?;
        dst.put_slice(&self.data);
        Ok(dst)
    }
}

#[allow(dead_code)]
pub struct ParsedMemberDataKey {
    key_str: BytesMut,
    reserve: [u8; 8],
    version: u64,
    data: Bytes,
}

#[allow(dead_code)]
impl ParsedMemberDataKey {
    pub fn new(encoded_key: &[u8]) -> Result<Self> {
        let mut key_str = BytesMut::new();

        let start_idx = PREFIX_RESERVE_LENGTH;
        let end_idx = encoded_key.len() - SUFFIX_RESERVE_LENGTH;

        // reserve
        let reserve_slice = &encoded_key[0..PREFIX_RESERVE_LENGTH];
        let mut reserve = [0u8; PREFIX_RESERVE_LENGTH];
        reserve.copy_from_slice(reserve_slice);

        // version is 8 bytes right after reserve
        let version_slice = &encoded_key[start_idx..start_idx + size_of::<u64>()];
        let version = u64::from_be_bytes(version_slice.try_into().unwrap());

        // encoded key part starts after version and ends at delimiter before suffix
        let encoded_key_part = &encoded_key[start_idx + size_of::<u64>()..end_idx];

        // find delimiter (0x00 0x00) to know the encoded length of key
        let mut delim_pos = None;
        for i in 0..encoded_key_part.len().saturating_sub(1) {
            if encoded_key_part[i] == 0x00 && encoded_key_part[i + 1] == 0x00 {
                delim_pos = Some(i + 2);
                break;
            }
        }
        let key_encoded_len = delim_pos.expect("encoded key delimiter not found");

        // decode only the key part (including delimiter) into key_str
        decode_user_key(&encoded_key_part[..key_encoded_len], &mut key_str)?;

        // data is whatever remains before suffix
        let data_slice = &encoded_key[start_idx + size_of::<u64>() + key_encoded_len..end_idx];
        let data = Bytes::copy_from_slice(data_slice);

        Ok(ParsedMemberDataKey {
            key_str,
            reserve,
            version,
            data,
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
        let start_idx = PREFIX_RESERVE_LENGTH + size_of::<u64>();
        let end_idx = encoded.len() - SUFFIX_RESERVE_LENGTH;
        let encoded_key_part = &encoded[start_idx..end_idx];
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
