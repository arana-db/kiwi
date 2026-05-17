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

use crate::error::InvalidFormatSnafu;
use crate::error::Result;
use crate::storage_define::{PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH};
use crate::storage_define::{decode_user_key, encode_user_key, seek_userkey_delim};
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
        let mut encoded_key_buf = BytesMut::new();
        encode_user_key(&self.key, &mut encoded_key_buf)?;
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + encoded_key_buf.len()
            + size_of::<u64>()  // version
            + size_of::<f64>()  // score
            + self.member.len()
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
        let mut encoded_key_buf = BytesMut::new();
        encode_user_key(&self.key, &mut encoded_key_buf)?;
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + encoded_key_buf.len()
            + size_of::<u64>()  // version
            + size_of::<f64>(); // score
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
        let min_len = PREFIX_RESERVE_LENGTH + SUFFIX_RESERVE_LENGTH;
        if encoded_key.len() < min_len {
            return Err(InvalidFormatSnafu {
                message: "encoded key too short".to_string(),
            }
            .build());
        }

        let mut key_str = BytesMut::new();

        let start_idx = PREFIX_RESERVE_LENGTH;
        let end_idx = encoded_key.len() - SUFFIX_RESERVE_LENGTH;

        // reserve1
        let reserve_slice = &encoded_key[0..PREFIX_RESERVE_LENGTH];
        let mut reserve1 = [0u8; PREFIX_RESERVE_LENGTH];
        reserve1.copy_from_slice(reserve_slice);

        // key
        let delim_len = 2;
        if encoded_key.len() < start_idx + delim_len + size_of::<u64>() * 2 {
            return Err(InvalidFormatSnafu {
                message: "encoded key too short for key, version and score".to_string(),
            }
            .build());
        }

        let key_end_idx = start_idx + seek_userkey_delim(&encoded_key[start_idx..]);
        let encoded_key_part = &encoded_key[start_idx..key_end_idx];
        decode_user_key(encoded_key_part, &mut key_str)?;

        // version (little-endian)
        let version_end_idx = key_end_idx + size_of::<u64>();
        if version_end_idx > end_idx {
            return Err(InvalidFormatSnafu {
                message: "encoded key too short for version".to_string(),
            }
            .build());
        }
        let version_slice = &encoded_key[key_end_idx..version_end_idx];
        let mut version_bytes = [0u8; size_of::<u64>()];
        version_bytes.copy_from_slice(version_slice);
        let version = u64::from_le_bytes(version_bytes);

        // score (little-endian, decode from raw IEEE 754 bits)
        let score_end_idx = version_end_idx + size_of::<u64>();
        if score_end_idx > end_idx {
            return Err(InvalidFormatSnafu {
                message: "encoded key too short for score".to_string(),
            }
            .build());
        }
        let score_slice = &encoded_key[version_end_idx..score_end_idx];
        let score_bits = u64::from_le_bytes(score_slice.try_into().expect("slice length mismatch"));
        let score = f64::from_bits(score_bits);

        if encoded_key.len() < score_end_idx + SUFFIX_RESERVE_LENGTH {
            return Err(InvalidFormatSnafu {
                message: "encoded key too short for member and reserve2".to_string(),
            }
            .build());
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip_basic() {
        let key = b"test_key";
        let version = 42u64;
        let score = 3.14f64;
        let member = b"member1";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.score(), score);
        assert_eq!(parsed.member(), member);
    }

    #[test]
    fn test_encode_decode_with_special_chars() {
        // 测试包含 \x00 的 key 和 member
        let key = b"key\x00with\x00nulls";
        let version = 100u64;
        let score = -2.5f64;
        let member = b"mem\x00ber\x00";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.score(), score);
        assert_eq!(parsed.member(), member);
    }

    #[test]
    fn test_encode_decode_empty_key_and_member() {
        let key = b"";
        let version = 0u64;
        let score = 0.0f64;
        let member = b"";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.score(), score);
        assert_eq!(parsed.member(), member);
    }

    #[test]
    fn test_encoded_length_correctness() {
        let key = b"test_key";
        let version = 1u64;
        let score = 1.0f64;
        let member = b"member";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let encoded = score_key.encode().expect("encode failed");

        // 计算实际编码后的 key 长度
        let mut encoded_key_buf = BytesMut::new();
        encode_user_key(key, &mut encoded_key_buf).expect("encode key failed");

        let expected_len = PREFIX_RESERVE_LENGTH // reserve1
            + encoded_key_buf.len()            // encoded key with delimiter
            + size_of::<u64>()                 // version
            + size_of::<u64>()                 // score (as u64 bits)
            + member.len()                     // member
            + SUFFIX_RESERVE_LENGTH; // reserve2

        assert_eq!(encoded.len(), expected_len);
    }

    #[test]
    fn test_score_positive_infinity() {
        let key = b"key";
        let version = 1u64;
        let score = f64::INFINITY;
        let member = b"member";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), version);
        assert!(parsed.score().is_infinite() && parsed.score().is_sign_positive());
        assert_eq!(parsed.member(), member);
    }

    #[test]
    fn test_score_negative_infinity() {
        let key = b"key";
        let version = 1u64;
        let score = f64::NEG_INFINITY;
        let member = b"member";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert!(parsed.score().is_infinite() && parsed.score().is_sign_negative());
    }

    #[test]
    fn test_score_nan() {
        let key = b"key";
        let version = 1u64;
        let score = f64::NAN;
        let member = b"member";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert!(parsed.score().is_nan());
    }

    #[test]
    fn test_version_boundary_values() {
        let key = b"key";
        let member = b"member";
        let score = 1.0f64;

        // Test version = 0
        let score_key = ZSetsScoreKey::new(key, 0, score, member);
        let encoded = score_key.encode().expect("encode failed");
        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.version(), 0);

        // Test version = u64::MAX
        let score_key = ZSetsScoreKey::new(key, u64::MAX, score, member);
        let encoded = score_key.encode().expect("encode failed");
        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.version(), u64::MAX);
    }

    #[test]
    fn test_large_key_and_member() {
        // Test relatively large key and member (keep reasonable size to avoid slow tests)
        let key = vec![b'k'; 1024]; // 1KB key
        let version = 1u64;
        let score = 1.0f64;
        let member = vec![b'm'; 1024]; // 1KB member

        let score_key = ZSetsScoreKey::new(&key, version, score, &member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.key(), &key[..]);
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.score(), score);
        assert_eq!(parsed.member(), &member[..]);
    }

    #[test]
    fn test_score_negative_zero_vs_positive_zero() {
        let key = b"key";
        let version = 1u64;
        let member = b"member";

        // Positive zero
        let score_key_pos = ZSetsScoreKey::new(key, version, 0.0f64, member);
        let encoded_pos = score_key_pos.encode().expect("encode failed");

        // Negative zero
        let score_key_neg = ZSetsScoreKey::new(key, version, -0.0f64, member);
        let encoded_neg = score_key_neg.encode().expect("encode failed");

        // Positive zero and negative zero have different bit representations
        assert_ne!(0.0f64.to_bits(), (-0.0f64).to_bits());
        // Therefore, encoding should differ
        assert_ne!(encoded_pos, encoded_neg);
    }

    #[test]
    fn test_encode_seek_key_format() {
        let key = b"test_key";
        let version = 42u64;
        let score = 3.14f64;
        let member = b"member1";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let seek_encoded = score_key.encode_seek_key().expect("encode_seek_key failed");
        let full_encoded = score_key.encode().expect("encode failed");

        // seek key should be shorter than full encoded (no member and reserve2)
        assert!(seek_encoded.len() < full_encoded.len());

        // seek key prefix should be identical to full encoded prefix
        assert_eq!(&seek_encoded[..], &full_encoded[..seek_encoded.len()]);

        // Calculate expected seek key length
        let mut encoded_key_buf = BytesMut::new();
        encode_user_key(key, &mut encoded_key_buf).expect("encode key failed");
        let expected_seek_len = PREFIX_RESERVE_LENGTH
            + encoded_key_buf.len()
            + size_of::<u64>()  // version
            + size_of::<u64>(); // score

        assert_eq!(seek_encoded.len(), expected_seek_len);
    }

    #[test]
    fn test_encode_seek_key_no_member_or_reserve2() {
        let key = b"key";
        let version = 1u64;
        let score = 1.0f64;
        let member = b"this_should_not_be_in_seek_key";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        let seek_encoded = score_key.encode_seek_key().expect("encode_seek_key failed");

        // seek_encoded should not contain member
        let seek_bytes = seek_encoded.as_ref();
        // member's start position should be after seek_encoded's end
        assert!(
            !seek_bytes
                .windows(member.len())
                .any(|window| window == member)
        );
    }

    #[test]
    fn test_seek_key_range_query_with_comparator() {
        // Simulate range query scenario using proper comparator logic
        let key = b"zset_key";
        let version = 1u64;

        // Create multiple seek keys with different scores
        let seek1 = ZSetsScoreKey::new(key, version, 1.0, b"")
            .encode_seek_key()
            .unwrap();
        let seek2 = ZSetsScoreKey::new(key, version, 2.0, b"")
            .encode_seek_key()
            .unwrap();
        let seek3 = ZSetsScoreKey::new(key, version, 3.0, b"")
            .encode_seek_key()
            .unwrap();

        // Seek keys should be ordered by score using the custom comparator
        // Not by raw byte comparison
        use crate::custom_comparator::zsets_score_key_compare;
        assert_eq!(
            zsets_score_key_compare(&seek1, &seek2),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            zsets_score_key_compare(&seek2, &seek3),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn test_multiple_members_same_key_version() {
        // Same key and version with different members
        let key = b"zset";
        let version = 1u64;
        let score = 1.0f64;

        let members: Vec<&[u8]> = vec![b"alice", b"bob", b"charlie"];
        let mut encoded_keys = Vec::new();

        for member in &members {
            let score_key = ZSetsScoreKey::new(key, version, score, *member);
            let encoded = score_key.encode().expect("encode failed");
            encoded_keys.push(encoded);
        }

        // Verify each encoded key decodes correctly
        for (i, encoded) in encoded_keys.iter().enumerate() {
            let parsed = ParsedZSetsScoreKey::new(encoded).expect("decode failed");
            assert_eq!(parsed.key(), key);
            assert_eq!(parsed.version(), version);
            assert_eq!(parsed.score(), score);
            assert_eq!(parsed.member(), members[i]);
        }
    }

    #[test]
    fn test_different_scores_encoding() {
        let key = b"key";
        let version = 1u64;
        let member = b"member";

        let scores = vec![
            f64::NEG_INFINITY,
            -1000.0,
            -1.0,
            -0.5,
            0.0,
            0.5,
            1.0,
            1000.0,
            f64::INFINITY,
        ];

        for score in scores {
            let score_key = ZSetsScoreKey::new(key, version, score, member);
            let encoded = score_key.encode().expect("encode failed");
            let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");

            if score.is_infinite() {
                assert_eq!(parsed.score().is_infinite(), score.is_infinite());
                assert_eq!(parsed.score().is_sign_positive(), score.is_sign_positive());
            } else {
                assert_eq!(parsed.score(), score);
            }
        }
    }

    #[test]
    fn test_reserve_fields_are_zeros() {
        let key = b"key";
        let version = 1u64;
        let score = 1.0f64;
        let member = b"member";

        let score_key = ZSetsScoreKey::new(key, version, score, member);
        assert_eq!(score_key.reserve1, [0u8; 8]);
        assert_eq!(score_key.reserve2, [0u8; 16]);

        let encoded = score_key.encode().expect("encode failed");
        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("decode failed");
        assert_eq!(parsed.reserve1, [0u8; 8]);
        assert_eq!(parsed.reserve2, [0u8; 16]);
    }

    #[test]
    fn test_exact_capacity_estimation() {
        // Verify accurate capacity estimation and ensure no extra memory allocation
        let test_cases = vec![
            (b"simple" as &[u8], b"member" as &[u8]),
            (b"key\x00with\x00nulls", b"mem\x00ber"),
            (b"", b""),
            (b"a", b"b"),
        ];

        for (key, member) in test_cases {
            let score_key = ZSetsScoreKey::new(key, 1, 1.0, member);
            let encoded = score_key.encode().expect("encode failed");

            // Calculate expected length using actual encoded key length
            let mut encoded_key_buf = BytesMut::new();
            encode_user_key(key, &mut encoded_key_buf).expect("encode key failed");
            let expected_len = PREFIX_RESERVE_LENGTH
                + encoded_key_buf.len()
                + size_of::<u64>()  // version
                + size_of::<f64>()  // score
                + member.len()
                + SUFFIX_RESERVE_LENGTH;

            // Verify actual encoded length matches expected length
            assert_eq!(
                encoded.capacity(),
                expected_len,
                "Capacity mismatch for key={:?}, member={:?}",
                key,
                member
            );

            // Verify BytesMut capacity is sufficient (allocator may allocate more for alignment)
            assert!(
                encoded.capacity() >= expected_len,
                "Capacity underallocated for key={:?}, member={:?}",
                key,
                member
            );
        }
    }

    #[test]
    fn test_seek_key_exact_capacity() {
        let test_cases = vec![b"simple" as &[u8], b"key\x00with\x00nulls", b"", b"a"];

        for key in test_cases {
            let score_key = ZSetsScoreKey::new(key, 1, 1.0, b"ignored");
            let seek_encoded = score_key.encode_seek_key().expect("encode_seek_key failed");

            // Calculate expected length using actual encoded key length
            let mut encoded_key_buf = BytesMut::new();
            encode_user_key(key, &mut encoded_key_buf).expect("encode key failed");
            let expected_len = PREFIX_RESERVE_LENGTH
                + encoded_key_buf.len()
                + size_of::<u64>()  // version
                + size_of::<u64>(); // score

            assert_eq!(
                seek_encoded.len(),
                expected_len,
                "Seek key capacity mismatch for key={:?}",
                key
            );
            assert!(
                seek_encoded.capacity() >= expected_len,
                "Seek key capacity underallocated for key={:?}",
                key
            );
        }
    }

    #[test]
    fn test_parse_error_too_short() {
        // Input too short
        let short_data = vec![0u8; 10];
        let result = ParsedZSetsScoreKey::new(&short_data);
        assert!(result.is_err(), "Should fail on too short input");

        let short_data = vec![0u8; 24];
        let result = ParsedZSetsScoreKey::new(&short_data);
        assert!(result.is_err(), "Should fail on too short input");
    }

    #[test]
    fn test_parse_error_invalid_key_encoding() {
        // Construct input with missing required fields
        let mut invalid = BytesMut::new();
        invalid.put_slice(&[0u8; PREFIX_RESERVE_LENGTH]); // reserve1
        invalid.put_slice(b"\x00\x00"); // Empty key delimiter
        invalid.put_u64_le(1); // version
        // Missing score, member, reserve2 - this will cause parsing to fail

        let result = ParsedZSetsScoreKey::new(&invalid);
        assert!(result.is_err(), "Should fail on incomplete data");
    }

    #[test]
    fn test_parse_error_missing_version() {
        // Normal reserve1 + key, but missing version
        let mut incomplete = BytesMut::new();
        incomplete.put_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        encode_user_key(b"key", &mut incomplete).unwrap();
        // No version/score/member/reserve2

        let result = ParsedZSetsScoreKey::new(&incomplete);
        assert!(result.is_err(), "Should fail on missing version");
    }

    #[test]
    fn test_parse_error_incomplete_version() {
        // Input has key but version is cut off
        let mut incomplete = BytesMut::new();
        incomplete.put_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        encode_user_key(b"key", &mut incomplete).unwrap();
        // Only 4 bytes of version (incomplete u64)
        incomplete.put_u32_le(1);

        let result = ParsedZSetsScoreKey::new(&incomplete);
        assert!(result.is_err(), "Should fail on incomplete version");
    }

    #[test]
    fn test_parse_error_incomplete_score() {
        // Input has key and version but score is cut off
        let mut incomplete = BytesMut::new();
        incomplete.put_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        encode_user_key(b"key", &mut incomplete).unwrap();
        incomplete.put_u64_le(1); // version
        // Only 4 bytes of score (incomplete f64)
        incomplete.put_u32_le(1);

        let result = ParsedZSetsScoreKey::new(&incomplete);
        assert!(result.is_err(), "Should fail on incomplete score");
    }

    #[test]
    fn test_parse_error_incomplete_member() {
        // Input has key, version, score but member is empty (reserve2 incomplete)
        let mut incomplete = BytesMut::new();
        incomplete.put_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        encode_user_key(b"key", &mut incomplete).unwrap();
        incomplete.put_u64_le(1); // version
        incomplete.put_u64_le(f64::to_bits(1.0)); // score
        // No member, no reserve2 - should fail

        let result = ParsedZSetsScoreKey::new(&incomplete);
        assert!(result.is_err(), "Should fail on incomplete member/reserve2");
    }

    #[test]
    fn test_parse_success_minimal_valid() {
        // Minimal valid input: empty key and empty member
        let key = b"";
        let member = b"";
        let score_key = ZSetsScoreKey::new(key, 0, 0.0, member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("parse should succeed");
        assert_eq!(parsed.key(), key);
        assert_eq!(parsed.version(), 0);
        assert_eq!(parsed.score(), 0.0);
        assert_eq!(parsed.member(), member);
    }

    #[test]
    fn test_parse_handles_special_scores() {
        // Test special score values including NaN
        let key = b"key";
        let member = b"member";
        let score_key = ZSetsScoreKey::new(key, 1, f64::NAN, member);
        let encoded = score_key.encode().expect("encode failed");

        let parsed = ParsedZSetsScoreKey::new(&encoded).expect("parse should succeed");
        assert!(parsed.score().is_nan(), "Score should be NaN");
    }
}
