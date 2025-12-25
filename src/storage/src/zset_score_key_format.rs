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

use crate::storage_define::{ENCODED_KEY_DELIM_SIZE, decode_user_key, encode_user_key, encoded_user_key_len, seek_userkey_delim};
use crate::{error::Result, storage_define::{PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH}};
use bytes::{BufMut, Bytes, BytesMut};

/// Format version marker for backward compatibility
/// This constant is used in reserve1[0] to detect and handle different key format versions
///
/// # Versions
/// - 0x00: Old format (v1.x) - Big-Endian version bytes
/// - 0x01: New format (v2.0+) - Little-Endian version bytes
/// - 0xFF: Uninitialized/Default (backward compatible with v1.x)
pub const FORMAT_VERSION_LE: u8 = 0x01;  // Little-Endian format marker
pub const FORMAT_VERSION_BE: u8 = 0x00;  // Big-Endian format marker (legacy)
pub const FORMAT_VERSION_DEFAULT: u8 = 0xFF; // Default (treats as BE for compatibility)

/// Detects the format version of a ZSet score key from reserve1[0]
pub fn detect_format_version(reserve1: &[u8; 8]) -> u8 {
    reserve1[0]
}

/// Returns true if the key is in new Little-Endian format
pub fn is_little_endian_format(reserve1: &[u8; 8]) -> bool {
    detect_format_version(reserve1) == FORMAT_VERSION_LE
}

/// Returns true if the key is in old Big-Endian format (or legacy default)
pub fn is_big_endian_format(reserve1: &[u8; 8]) -> bool {
    let version = detect_format_version(reserve1);
    version == FORMAT_VERSION_BE || version == FORMAT_VERSION_DEFAULT
}

//! # ZSet Score Key Format Module
//!
//! This module implements the binary encoding and decoding of ZSet (Sorted Set) score keys for RocksDB storage.
//!
//! ## Format Versioning and Backward Compatibility
//!
//! To support seamless migration from old (Big-Endian) to new (Little-Endian) formats,
//! we use a version marker stored in reserve1[0]:
//! - 0x00: Old format (v1.x, Big-Endian version bytes)
//! - 0x01: New format (v2.0+, Little-Endian version bytes)
//! - 0xFF: Default/uninitialized (treated as Big-Endian for compatibility)
//!
//! This allows the system to:
//! 1. Read keys in either format without manual data migration
//! 2. Gradually upgrade keys as they are written
//! 3. Detect format mismatches and provide clear error messages
//!
//! ## Breaking Change Notice: Byte Order Update (v2.0)
//!
//! **Issue**: Previous versions used Big-Endian (BE) byte order for version numbers.
//! **Change**: Version 2.0+ uses Little-Endian (LE) byte order for consistency and performance.
//! **Impact**: This is a **breaking change** affecting all existing ZSet data.
//!
//! ### Migration Required
//!
//! If upgrading from v1.x to v2.0+:
//! 1. **Backup your database** before upgrading
//! 2. **Run data migration** to convert existing keys from BE to LE format
//! 3. **Test thoroughly** in a staging environment first
//!
//! Migration can be performed using the `migrate_member_keys_be_to_le()` function or
//! the provided migration tool. See MIGRATION_GUIDE.md for detailed instructions.
//!
//! ### Format Details
//!
//! **Old Format (v1.x - BE):**
//! ```text
//! | reserve1[0]=0x00 | reserve1[1..] | key | version_BE | score | member | reserve2 |
//! |       1B         |       7B       | var |     8B     |  8B   |  var   |   16B    |
//! ```
//!
//! **New Format (v2.0+ - LE):**
//! ```text
//! | reserve1[0]=0x01 | reserve1[1..] | key | version_LE | score | member | reserve2 |
//! |       1B         |       7B       | var |     8B     |  8B   |  var   |   16B    |
//! ```

/// A score-member pair for Sorted Set (ZSet) operations.
///
/// This structure represents a member in a sorted set along with its associated score.
/// In Redis-compatible sorted sets, each member has a floating-point score that determines
/// its position in the set.
///
/// # Examples
///
/// ```ignore
/// use storage::zset_score_key_format::ScoreMember;
///
/// let sm = ScoreMember::new(3.14, b"member1".to_vec());
/// assert_eq!(sm.score, 3.14);
/// assert_eq!(sm.member, b"member1");
/// ```
#[allow(dead_code)]
pub type ZsetScoreMember = ScoreMember;

/// Represents a score-member pair in a sorted set.
///
/// Each member in a sorted set has an associated floating-point score.
/// Members are ordered by their scores in ascending order.
#[derive(Debug, Clone)]
pub struct ScoreMember {
    /// The score associated with this member (IEEE 754 double-precision float)
    pub score: f64,
    /// The member data (arbitrary byte sequence)
    pub member: Vec<u8>,
}

impl ScoreMember {
    /// Creates a new ScoreMember with the given score and member data.
    ///
    /// # Arguments
    ///
    /// * `score` - The floating-point score for this member
    /// * `member` - The member data as a byte vector
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let sm = ScoreMember::new(1.5, b"alice".to_vec());
    /// ```
    pub fn new(score: f64, member: Vec<u8>) -> Self {
        ScoreMember { score, member }
    }
}

/// Internal key format for ZSet score-to-member mapping in RocksDB.
///
/// # Binary Format
///
/// ```text
/// | reserve1 | key | version | score | member | reserve2 |
/// |    8B    | var |   8B    |  8B   |  var   |   16B    |
/// ```
///
/// # Field Details
///
/// - `reserve1` (8 bytes): Reserved prefix for future use, currently all zeros
/// - `key`: User key with special encoding (handles `\x00` bytes)
/// - `version` (8 bytes): Version number in little-endian format
/// - `score` (8 bytes): IEEE 754 double as u64 bits in little-endian format
/// - `member`: Member data (arbitrary bytes)
/// - `reserve2` (16 bytes): Reserved suffix for future use, currently all zeros
///
/// # Ordering
///
/// Keys are ordered by a custom comparator (`zsets_score_key_compare`):
/// 1. First by user `key` (bytewise)
/// 2. Then by `version` (numeric ascending)
/// 3. Then by `score` (numeric ascending, with special NaN handling)
/// 4. Finally by `member` (bytewise)
///
/// # Special Score Values
///
/// - `f64::NEG_INFINITY`: Sorts before all finite values
/// - `f64::INFINITY`: Sorts after all finite values
/// - `f64::NAN`: Sorts after infinity (NaN > any non-NaN value)
/// - `-0.0` and `+0.0`: Treated as equal during comparison
///
/// # Design Rationale
///
/// The reserve fields provide space for future extensions without breaking
/// compatibility. Little-endian encoding is used for numeric fields to match
/// the custom comparator's expectations.
///
/// # Examples
///
/// ```ignore
/// use storage::zset_score_key_format::ZSetsScoreKey;
///
/// let key = ZSetsScoreKey::new(b"myzset", 1, 3.14, b"member1");
/// let encoded = key.encode().unwrap();
/// ```
/* zset score to member data key format:
 * | reserve1 | key | version | score | member |  reserve2 |
 * |    8B    |     |    8B   |  8B   |        |    16B    |
 */
#[derive(Debug, Clone)]
pub struct ZSetsScoreKey {
    /// Reserved prefix (8 bytes, currently all zeros)
    pub reserve1: [u8; 8],
    /// User key (variable length)
    pub key: Bytes,
    /// Version number (8 bytes, little-endian)
    pub version: u64,
    /// Score as f64 (8 bytes, stored as IEEE 754 bits in little-endian)
    pub score: f64,
    /// Member data (variable length)
    pub member: Bytes,
    /// Reserved suffix (16 bytes, currently all zeros)
    pub reserve2: [u8; 16],
}

impl ZSetsScoreKey {
    /// Creates a new ZSetsScoreKey.
    ///
    /// # Arguments
    ///
    /// * `key` - The user key (arbitrary bytes)
    /// * `version` - The version number (used for MVCC)
    /// * `score` - The floating-point score
    /// * `member` - The member data (arbitrary bytes)
    ///
    /// # Returns
    ///
    /// A new `ZSetsScoreKey` with reserve fields initialized to zeros.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let key = ZSetsScoreKey::new(b"myzset", 1, 3.14, b"member1");
    /// ```
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

    /// Encodes the complete key into bytes for storage in RocksDB.
    ///
    /// # Returns
    ///
    /// A `BytesMut` containing the encoded key, or an error if encoding fails.
    ///
    /// # Format
    ///
    /// The encoded format includes all fields:
    /// `reserve1 | encoded_key | version_le | score_bits_le | member | reserve2`
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let key = ZSetsScoreKey::new(b"myzset", 1, 3.14, b"member1");
    /// let encoded = key.encode()?;
    /// ```
    pub fn encode(&self) -> Result<BytesMut> {
        // Calculate exact capacity for better performance
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + encoded_user_key_len(&self.key)  // Precise encoded key length
            + size_of::<u64>()                 // version
            + size_of::<u64>()                 // score (as u64 bits)
            + self.member.len()                // member
            + SUFFIX_RESERVE_LENGTH; // reserve2
        let mut dst = BytesMut::with_capacity(estimated_cap);

        // Mark reserve1[0] as FORMAT_VERSION_LE to indicate new format
        let mut reserve1 = self.reserve1;
        reserve1[0] = FORMAT_VERSION_LE;
        dst.put_slice(&reserve1);
        encode_user_key(&self.key, &mut dst)?;
        // Use little-endian for version and score (custom comparator expects this)
        dst.put_u64_le(self.version);
        dst.put_u64_le(self.score.to_bits());
        dst.put_slice(&self.member);
        dst.put_slice(&self.reserve2);
        Ok(dst)
    }

    /// Encodes a seek key prefix for range queries.
    ///
    /// This creates a shorter key without the `member` and `reserve2` fields,
    /// which is useful for seeking to a specific score range in RocksDB.
    ///
    /// # Returns
    ///
    /// A `BytesMut` containing the seek key prefix, or an error if encoding fails.
    ///
    /// # Format
    ///
    /// The encoded format includes:
    /// `reserve1 | encoded_key | version_le | score_bits_le`
    ///
    /// # Use Case
    ///
    /// This is primarily used for ZRANGEBYSCORE-like operations where you need
    /// to find all members with scores within a range.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Seek to score 1.0 for key "myzset" version 1
    /// let seek_key = ZSetsScoreKey::new(b"myzset", 1, 1.0, b"");
    /// let encoded_seek = seek_key.encode_seek_key()?;
    /// // Use encoded_seek for RocksDB iteration
    /// ```
    pub fn encode_seek_key(&self) -> Result<BytesMut> {
        // Calculate exact capacity for better performance
        let estimated_cap = PREFIX_RESERVE_LENGTH
            + encoded_user_key_len(&self.key)  // Precise encoded key length
            + size_of::<u64>()                 // version
            + size_of::<u64>(); // score (as u64 bits)
        let mut dst = BytesMut::with_capacity(estimated_cap);

        // Mark reserve1[0] as FORMAT_VERSION_LE to indicate new format
        let mut reserve1 = self.reserve1;
        reserve1[0] = FORMAT_VERSION_LE;
        dst.put_slice(&reserve1);
        encode_user_key(&self.key, &mut dst)?;
        // Use little-endian for version and score (custom comparator expects this)
        dst.put_u64_le(self.version);
        dst.put_u64_le(self.score.to_bits());
        Ok(dst)
    }
}

/// Parsed representation of an encoded ZSetsScoreKey.
///
/// This structure is created by parsing an encoded key from RocksDB.
/// It provides convenient access to the individual fields.
///
/// # Examples
///
/// ```ignore
/// let key = ZSetsScoreKey::new(b"myzset", 1, 3.14, b"member1");
/// let encoded = key.encode()?;
/// let parsed = ParsedZSetsScoreKey::new(&encoded)?;
/// assert_eq!(parsed.key(), b"myzset");
/// assert_eq!(parsed.score(), 3.14);
/// ```
#[allow(dead_code)]
pub struct ParsedZSetsScoreKey {
    /// Reserved prefix (8 bytes)
    pub reserve1: [u8; 8],
    /// Decoded user key
    pub key: BytesMut,
    /// Version number
    pub version: u64,
    /// Decoded score
    pub score: f64,
    /// Member data
    pub member: BytesMut,
    /// Reserved suffix (16 bytes)
    pub reserve2: [u8; 16],
}

impl ParsedZSetsScoreKey {
    /// Parses an encoded ZSetsScoreKey from bytes.
    ///
    /// # Arguments
    ///
    /// * `encoded_key` - The encoded key bytes from RocksDB
    ///
    /// # Returns
    ///
    /// A `ParsedZSetsScoreKey` instance, or an error if parsing fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The encoded key is too short
    /// - The key encoding is invalid
    /// - The version or score bytes cannot be parsed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let encoded = /* get from RocksDB */;
    /// let parsed = ParsedZSetsScoreKey::new(&encoded)?;
    /// println!("Score: {}", parsed.score());
    /// ```
    pub fn new(encoded_key: &[u8]) -> Result<Self> {
        use crate::error::InvalidFormatSnafu;
        use snafu::ensure;

        let mut key_str = BytesMut::new();

        // Validate minimum length
        let min_len = PREFIX_RESERVE_LENGTH
            + ENCODED_KEY_DELIM_SIZE
            + size_of::<u64>()
            + size_of::<u64>()
            + SUFFIX_RESERVE_LENGTH;
        ensure!(
            encoded_key.len() >= min_len,
            InvalidFormatSnafu {
                message: format!(
                    "Encoded key too short: got {} bytes, need at least {} bytes",
                    encoded_key.len(),
                    min_len
                )
            }
        );

        let start_idx = PREFIX_RESERVE_LENGTH;
        let end_idx = encoded_key.len() - SUFFIX_RESERVE_LENGTH;

        // reserve1
        let reserve_slice = &encoded_key[0..PREFIX_RESERVE_LENGTH];
        let mut reserve1 = [0u8; PREFIX_RESERVE_LENGTH];
        reserve1.copy_from_slice(reserve_slice);

        // key
        let key_end_idx = start_idx + seek_userkey_delim(&encoded_key[start_idx..]);
        ensure!(
            key_end_idx <= end_idx,
            InvalidFormatSnafu {
                message: "Invalid key encoding: delimiter position out of bounds".to_string()
            }
        );
        let encoded_key_part = &encoded_key[start_idx..key_end_idx];
        decode_user_key(encoded_key_part, &mut key_str)?;

        // version (little-endian)
        let version_end_idx = key_end_idx + size_of::<u64>();
        ensure!(
            version_end_idx <= end_idx,
            InvalidFormatSnafu {
                message: "Invalid version encoding: not enough bytes".to_string()
            }
        );
        let version_slice = &encoded_key[key_end_idx..version_end_idx];
        let version = u64::from_le_bytes(
            version_slice
                .try_into()
                .expect("version slice should be 8 bytes"),
        );

        // score (little-endian, decode from raw IEEE 754 bits)
        let score_end_idx = version_end_idx + size_of::<u64>();
        ensure!(
            score_end_idx <= end_idx,
            InvalidFormatSnafu {
                message: "Invalid score encoding: not enough bytes".to_string()
            }
        );
        let score_slice = &encoded_key[version_end_idx..score_end_idx];
        let score_bits = u64::from_le_bytes(
            score_slice
                .try_into()
                .expect("score slice should be 8 bytes"),
        );
        let score = f64::from_bits(score_bits);

        // member
        let encoded_member_part = &encoded_key[score_end_idx..end_idx];
        let member_str = BytesMut::from(encoded_member_part);

        // reserve2
        let reserve_slice = &encoded_key[end_idx..];
        ensure!(
            reserve_slice.len() == SUFFIX_RESERVE_LENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "Invalid reserve2 length: got {} bytes, expected {} bytes",
                    reserve_slice.len(),
                    SUFFIX_RESERVE_LENGTH
                )
            }
        );
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

    /// Returns the user key as a byte slice.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(parsed.key(), b"myzset");
    /// ```
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    /// Returns the version number.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(parsed.version(), 1);
    /// ```
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns the member data as a byte slice.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(parsed.member(), b"member1");
    /// ```
    pub fn member(&self) -> &[u8] {
        self.member.as_ref()
    }

    /// Returns the score.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(parsed.score(), 3.14);
    /// ```
    pub fn score(&self) -> f64 {
        self.score
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== 基础编码/解码测试 ==========

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

    // ========== 边界值测试 ==========

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

    // ========== encode_seek_key unit tests ==========

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

    // ========== Data integrity tests ==========

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

    // ========== Performance verification tests ==========

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

            // Calculate expected length
            let expected_len = PREFIX_RESERVE_LENGTH
                + encoded_user_key_len(key)
                + size_of::<u64>()  // version
                + size_of::<u64>()  // score
                + member.len()
                + SUFFIX_RESERVE_LENGTH;

            // Verify actual encoded length matches expected length
            assert_eq!(
                encoded.len(),
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

            let expected_len = PREFIX_RESERVE_LENGTH
                + encoded_user_key_len(key)
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

    // ========== Error handling tests ==========

    #[test]
    fn test_parse_error_too_short() {
        // Input too short
        let short_data = vec![0u8; 10];
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
