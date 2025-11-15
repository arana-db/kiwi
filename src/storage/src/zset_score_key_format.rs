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

use crate::storage_define::{
    ENCODED_KEY_DELIM_SIZE, decode_user_key, encode_user_key, seek_userkey_delim,
};
use crate::{
    error::Result,
    storage_define::{PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH},
};
use bytes::{BufMut, Bytes, BytesMut};

#[allow(dead_code)]
pub type ZsetScoreMember = ScoreMember;

#[derive(Debug, Clone)]
pub struct ScoreMember {
    pub score: f64,
    pub member: Vec<u8>,
}

impl ScoreMember {
    pub fn new(score: f64, member: Vec<u8>) -> Self {
        ScoreMember { score, member }
    }
}

/* zset score to member data key format:
 * | reserve1 | key | version | score | member |  reserve2 |
 * |    8B    |     |    8B   |  8B   |        |    16B    |
 */
#[derive(Debug, Clone)]
pub struct ZSetsScoreKey {
    pub reserve1: [u8; 8],
    pub key: Bytes,
    pub version: u64,
    pub score: f64,
    pub member: Bytes,
    pub reserve2: [u8; 16],
}

impl ZSetsScoreKey {
    pub fn new(key: &[u8], version: u64, score: f64, member: &[u8]) -> Self {
        ZSetsScoreKey {
            reserve1: [0; PREFIX_RESERVE_LENGTH],
            key: Bytes::copy_from_slice(key),
            version,
            score,
            member: Bytes::copy_from_slice(member),
            reserve2: [0; SUFFIX_RESERVE_LENGTH],
        }
    }

    pub fn encode(&self) -> Result<BytesMut> {
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + self.key.len() * 2
            + size_of::<u64>()  // version
            + size_of::<f64>()  // score
            + self.member.len()
            + ENCODED_KEY_DELIM_SIZE
            + SUFFIX_RESERVE_LENGTH;
        let mut dst = BytesMut::with_capacity(estimated_cap);

        dst.put_slice(&self.reserve1);
        encode_user_key(&self.key, &mut dst)?;
        // Use little-endian for version and score (custom comparator expects this)
        dst.put_u64_le(self.version);
        dst.put_u64_le(self.score.to_bits());
        dst.put_slice(&self.member);
        dst.put_slice(&self.reserve2);
        Ok(dst)
    }

    pub fn encode_seek_key(&self) -> Result<BytesMut> {
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + self.key.len() * 2
            + size_of::<u64>()  // version
            + size_of::<f64>()  // score
            + ENCODED_KEY_DELIM_SIZE;
        let mut dst = BytesMut::with_capacity(estimated_cap);

        dst.put_slice(&self.reserve1);
        encode_user_key(&self.key, &mut dst)?;
        // Use little-endian for version and score (custom comparator expects this)
        dst.put_u64_le(self.version);
        dst.put_u64_le(self.score.to_bits());
        Ok(dst)
    }
}

#[allow(dead_code)]
pub struct ParsedZSetsScoreKey {
    pub reserve1: [u8; 8],
    pub key: BytesMut,
    pub version: u64,
    pub score: f64,
    pub member: BytesMut,
    pub reserve2: [u8; 16],
}

impl ParsedZSetsScoreKey {
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
        let encoded_key_part = &encoded_key[start_idx..key_end_idx];
        decode_user_key(encoded_key_part, &mut key_str)?;

        // version (little-endian)
        let version_slice = &encoded_key[key_end_idx..key_end_idx + size_of::<u64>()];
        let version = u64::from_le_bytes(version_slice.try_into().unwrap());
        let version_end_idx = key_end_idx + size_of::<u64>();

        // score (little-endian, decode from raw IEEE 754 bits)
        let score_slice = &encoded_key[version_end_idx..version_end_idx + size_of::<u64>()];
        let score_bits = u64::from_le_bytes(score_slice.try_into().unwrap());
        let score = f64::from_bits(score_bits);
        let score_end_idx = version_end_idx + size_of::<u64>();

        // member
        let encoded_member_part = &encoded_key[score_end_idx..end_idx];
        let member_str = BytesMut::from(encoded_member_part);

        // reserve2
        let reserve_slice = &encoded_key[end_idx..];
        let mut reserve2 = [0u8; SUFFIX_RESERVE_LENGTH];
        reserve2.copy_from_slice(reserve_slice);

        Ok(ParsedZSetsScoreKey {
            reserve1,
            key: key_str,
            version,
            score,
            member: member_str,
            reserve2,
        })
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn member(&self) -> &[u8] {
        self.member.as_ref()
    }

    pub fn score(&self) -> f64 {
        self.score
    }
}
