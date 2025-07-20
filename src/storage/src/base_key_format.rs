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

use crate::{
    error::{InvalidFormatSnafu, Result},
    storage_define::{
        decode_user_key, encode_user_key, ENCODED_KEY_DELIM_SIZE, PREFIX_RESERVE_LENGTH,
        SUFFIX_RESERVE_LENGTH,
    },
};
use bytes::{BufMut, Bytes, BytesMut};
use snafu::ensure;
//
// used for string data key or hash/zset/set/list's meta key. format:
// | reserve1 | key | reserve2 |
// |    8B    |     |   16B    |
//

/// TODO: remove allow dead code
#[allow(dead_code)]
struct BaseKey {
    reserve1: [u8; 8],
    key: Bytes,
    reserve2: [u8; 16],
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl BaseKey {
    pub fn new(key: &[u8]) -> Self {
        BaseKey {
            reserve1: [0; PREFIX_RESERVE_LENGTH],
            key: Bytes::copy_from_slice(key),
            reserve2: [0; SUFFIX_RESERVE_LENGTH],
        }
    }

    fn encode(&self) -> Result<BytesMut> {
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + self.key.len() * 2
            + ENCODED_KEY_DELIM_SIZE
            + SUFFIX_RESERVE_LENGTH;
        let mut dst = BytesMut::with_capacity(estimated_cap);

        dst.put_slice(&self.reserve1);
        encode_user_key(&self.key, &mut dst)?;
        dst.put_slice(&self.reserve2);
        Ok(dst)
    }
}

pub struct ParsedBaseKey {
    key_str: BytesMut,
}

impl ParsedBaseKey {
    pub fn new(encoded_key: &[u8]) -> Result<Self> {
        let mut key_str = BytesMut::new();
        Self::decode(encoded_key, &mut key_str)?;
        Ok(ParsedBaseKey { key_str })
    }

    fn decode(encoded_key: &[u8], key_str: &mut BytesMut) -> Result<()> {
        ensure!(
            !encoded_key.is_empty(),
            InvalidFormatSnafu {
                message: "Encoded key too short to contain prefix, suffix, and data".to_string(),
            }
        );

        let start_idx = PREFIX_RESERVE_LENGTH;
        let end_idx = encoded_key.len() - SUFFIX_RESERVE_LENGTH;
        let data_slice = &encoded_key[start_idx..end_idx];
        decode_user_key(data_slice, key_str)
    }

    pub fn key(&self) -> &[u8] {
        self.key_str.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mv_test_base_key_encode_and_decode() {
        let test_key = b"test_key";

        let base_key = BaseKey::new(test_key);
        let encoded_result = base_key.encode();
        assert!(
            encoded_result.is_ok(),
            "Encoding failed: {:?}",
            encoded_result.err()
        );
        let encoded_data = encoded_result.unwrap();

        assert_eq!(
            encoded_data.len(),
            PREFIX_RESERVE_LENGTH + test_key.len() + ENCODED_KEY_DELIM_SIZE + SUFFIX_RESERVE_LENGTH,
        );

        let decode_key_result = ParsedBaseKey::new(&encoded_data);
        assert!(
            decode_key_result.is_ok(),
            "Decoding failed: {:?}",
            decode_key_result.err()
        );
        let decode_key = decode_key_result.unwrap();

        assert_eq!(decode_key.key(), test_key);
    }
}
