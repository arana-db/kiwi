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

//! Vector set member data key codec (V1, frozen layout).
//!
//! This codec is vector-set specific. Hash/Set/ZSet keep using the shared
//! `MemberDataKey` in `format_member_data_key.rs`; do not unify them.
//!
//! Layout stored in VectorDataCF:
//!
//! | codec_version | key_len | user_key | storage_incarnation | generation_sequence | element   |
//! |      1B       | 4B BE   | key_len  |        8B BE        |        8B BE        | remainder |
//!
//! - `codec_version` is `VECTOR_MEMBER_KEY_CODEC_VERSION` (1).
//! - `key_len` is a u32 big-endian length of `user_key`; keys that do not fit
//!   in u32 are rejected at encode time.
//! - `storage_incarnation` identifies the RocksDB instance that wrote the
//!   member (see `storage_manifest.rs`).
//! - `generation_sequence` identifies one lifecycle of the vector set; it is
//!   stored in `VectorMeta::version`.
//! - `element` occupies all remaining bytes and may be empty.
//!
//! All fixed-width integers are big-endian so members of one
//! (key, incarnation, generation) prefix stay contiguous under the default
//! RocksDB bytewise comparator.

use bytes::{Buf, BufMut};
use snafu::ensure;

use crate::error::{InvalidArgumentSnafu, InvalidFormatSnafu, Result};

pub const VECTOR_MEMBER_KEY_CODEC_VERSION: u8 = 1;

const CODEC_VERSION_LENGTH: usize = 1;
const KEY_LEN_LENGTH: usize = 4;
const INCARNATION_LENGTH: usize = 8;
const GENERATION_LENGTH: usize = 8;
const HEADER_LENGTH: usize = CODEC_VERSION_LENGTH + KEY_LEN_LENGTH;
const PREFIX_TRAILER_LENGTH: usize = INCARNATION_LENGTH + GENERATION_LENGTH;
const MIN_KEY_LENGTH: usize = HEADER_LENGTH + PREFIX_TRAILER_LENGTH;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMemberDataKey<'a> {
    pub key: &'a [u8],
    pub storage_incarnation: u64,
    pub generation_sequence: u64,
    pub element: &'a [u8],
}

impl<'a> VectorMemberDataKey<'a> {
    /// Encode the full member key including the element.
    pub fn encode_full(&self) -> Result<Vec<u8>> {
        let mut output = Self::encode_key_prefix(self.key)?;
        output.put_u64(self.storage_incarnation);
        output.put_u64(self.generation_sequence);
        output.put_slice(self.element);
        Ok(output)
    }

    /// Encode the iteration prefix ending exactly after `generation_sequence`.
    pub fn encode_prefix(
        key: &[u8],
        storage_incarnation: u64,
        generation_sequence: u64,
    ) -> Result<Vec<u8>> {
        let mut output = Self::encode_key_prefix(key)?;
        output.put_u64(storage_incarnation);
        output.put_u64(generation_sequence);
        Ok(output)
    }

    /// Encode the prefix covering all incarnations and generations of `key`
    /// (ends exactly after `user_key`).
    pub fn encode_key_prefix(key: &[u8]) -> Result<Vec<u8>> {
        let key_len = u32::try_from(key.len()).map_err(|_| {
            InvalidArgumentSnafu {
                message: format!("vector member key too long: {} bytes", key.len()),
            }
            .build()
        })?;
        let mut output = Vec::with_capacity(HEADER_LENGTH + key.len());
        output.put_u8(VECTOR_MEMBER_KEY_CODEC_VERSION);
        output.put_u32(key_len);
        output.put_slice(key);
        Ok(output)
    }

    /// Compute the exclusive upper bound for a prefix: the lexicographic
    /// successor with trailing 0xFF bytes folded by incrementing the last
    /// non-0xFF byte. Returns None when the prefix is empty or all 0xFF.
    pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut bound = prefix.to_vec();
        while let Some(last) = bound.last_mut() {
            if *last == 0xFF {
                bound.pop();
            } else {
                *last += 1;
                return Some(bound);
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedVectorMemberDataKey<'a> {
    key: &'a [u8],
    storage_incarnation: u64,
    generation_sequence: u64,
    element: &'a [u8],
}

impl<'a> ParsedVectorMemberDataKey<'a> {
    pub fn decode(encoded_key: &'a [u8]) -> Result<Self> {
        ensure!(
            encoded_key.len() >= MIN_KEY_LENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "vector member key too short: {} < {MIN_KEY_LENGTH}",
                    encoded_key.len()
                )
            }
        );

        let mut reader = encoded_key;
        let codec_version = reader.get_u8();
        ensure!(
            codec_version == VECTOR_MEMBER_KEY_CODEC_VERSION,
            InvalidFormatSnafu {
                message: format!("unsupported vector member key codec version: {codec_version}")
            }
        );

        let key_len = reader.get_u32() as usize;
        // Bound-check against the remaining bytes before slicing so a hostile
        // key_len can never cause an out-of-bounds read or a large allocation.
        ensure!(
            key_len <= reader.len().saturating_sub(PREFIX_TRAILER_LENGTH),
            InvalidFormatSnafu {
                message: format!(
                    "vector member key_len {} exceeds remaining {} bytes",
                    key_len,
                    reader.len()
                )
            }
        );

        let key = &reader[..key_len];
        reader.advance(key_len);
        let storage_incarnation = reader.get_u64();
        let generation_sequence = reader.get_u64();
        let element = reader;

        Ok(Self {
            key,
            storage_incarnation,
            generation_sequence,
            element,
        })
    }

    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    pub fn storage_incarnation(&self) -> u64 {
        self.storage_incarnation
    }

    pub fn generation_sequence(&self) -> u64 {
        self.generation_sequence
    }

    pub fn element(&self) -> &'a [u8] {
        self.element
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn member<'a>(key: &'a [u8], element: &'a [u8]) -> VectorMemberDataKey<'a> {
        VectorMemberDataKey {
            key,
            storage_incarnation: 0x0102_0304_0506_0708,
            generation_sequence: 0x1112_1314_1516_1718,
            element,
        }
    }

    #[test]
    fn encode_full_matches_frozen_golden_layout() {
        let encoded = member(b"vec", b"e1").encode_full().expect("encode");
        let mut expected = Vec::new();
        expected.push(1u8); // codec_version
        expected.extend_from_slice(&3u32.to_be_bytes()); // key_len
        expected.extend_from_slice(b"vec"); // user_key
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        expected.extend_from_slice(&0x1112_1314_1516_1718u64.to_be_bytes());
        expected.extend_from_slice(b"e1"); // element
        assert_eq!(encoded, expected);
    }

    #[test]
    fn encode_decode_round_trip() {
        for (key, element) in [
            (&b"vectors"[..], &b"member"[..]),
            (b"vectors", b""),
            (b"", b"member"),
            (b"", b""),
            (b"key\x00with\x00zeros", b"el\x00ement"),
            (b"vec", &[0x00, 0xFF, 0x00][..]),
        ] {
            let encoded = member(key, element).encode_full().expect("encode");
            let parsed = ParsedVectorMemberDataKey::decode(&encoded).expect("decode");
            assert_eq!(parsed.key(), key);
            assert_eq!(parsed.element(), element);
            assert_eq!(parsed.storage_incarnation(), 0x0102_0304_0506_0708);
            assert_eq!(parsed.generation_sequence(), 0x1112_1314_1516_1718);
        }
    }

    #[test]
    fn encode_prefix_is_a_prefix_of_the_full_key() {
        let full = member(b"vec\0key", b"elem")
            .encode_full()
            .expect("encode full");
        let prefix = VectorMemberDataKey::encode_prefix(
            b"vec\0key",
            0x0102_0304_0506_0708,
            0x1112_1314_1516_1718,
        )
        .expect("encode prefix");

        assert!(full.starts_with(&prefix));
        assert_eq!(
            prefix.len(),
            HEADER_LENGTH + b"vec\0key".len() + PREFIX_TRAILER_LENGTH
        );
        let parsed = ParsedVectorMemberDataKey::decode(&full).expect("decode");
        assert_eq!(&full[prefix.len()..], parsed.element());
    }

    #[test]
    fn keys_with_prefix_relationship_do_not_collide() {
        // "vec" is a prefix of "vectors"; key_len keeps their encodings apart.
        let short = VectorMemberDataKey::encode_prefix(b"vec", 1, 1).expect("short");
        let long = VectorMemberDataKey::encode_prefix(b"vectors", 1, 1).expect("long");
        assert!(!long.starts_with(&short));
        assert!(!short.starts_with(&long));
    }

    #[test]
    fn same_prefix_members_sort_contiguously() {
        let generation = 7u64;
        let make = |element: &[u8]| {
            VectorMemberDataKey {
                key: b"vec",
                storage_incarnation: 9,
                generation_sequence: generation,
                element,
            }
            .encode_full()
            .expect("encode")
        };
        let mut keys = [make(b"b"), make(b""), make(b"a")];
        keys.sort();
        let prefix = VectorMemberDataKey::encode_prefix(b"vec", 9, generation).expect("prefix");
        assert!(keys.iter().all(|key| key.starts_with(&prefix)));
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));

        // A different generation of the same key sorts outside the prefix range.
        let other = VectorMemberDataKey {
            key: b"vec",
            storage_incarnation: 9,
            generation_sequence: generation + 1,
            element: b"",
        }
        .encode_full()
        .expect("encode");
        assert!(!other.starts_with(&prefix));
        assert!(other > *keys.last().expect("non-empty"));
    }

    #[test]
    fn decode_rejects_malformed_keys_without_panicking() {
        let valid = member(b"vec", b"e").encode_full().expect("encode");

        // Truncated at every boundary, including mid-header. Trimming the
        // single element byte would still be a valid key (empty element), so
        // the loop stops one byte earlier.
        for len in 0..valid.len() - 1 {
            assert!(
                ParsedVectorMemberDataKey::decode(&valid[..len]).is_err(),
                "truncated length {len} must be rejected"
            );
        }

        // Bad codec version.
        let mut bad_version = valid.clone();
        bad_version[0] = 2;
        assert!(ParsedVectorMemberDataKey::decode(&bad_version).is_err());

        // key_len larger than the remaining bytes (no huge allocation).
        let mut bad_len = valid.clone();
        bad_len[1..5].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(ParsedVectorMemberDataKey::decode(&bad_len).is_err());

        // key_len consuming the incarnation/generation trailer.
        let mut greedy_len = valid.clone();
        greedy_len[1..5].copy_from_slice(&10u32.to_be_bytes());
        assert!(ParsedVectorMemberDataKey::decode(&greedy_len).is_err());
    }

    #[test]
    fn prefix_upper_bound_increments_and_carries() {
        assert_eq!(
            VectorMemberDataKey::prefix_upper_bound(&[0x01, 0x00, 0x01]),
            Some(vec![0x01, 0x00, 0x02])
        );
        // 0xFF carry folds into the preceding byte.
        assert_eq!(
            VectorMemberDataKey::prefix_upper_bound(&[0x01, 0xFF, 0xFF]),
            Some(vec![0x02])
        );
        assert_eq!(VectorMemberDataKey::prefix_upper_bound(&[0xFF, 0xFF]), None);
        assert_eq!(VectorMemberDataKey::prefix_upper_bound(&[]), None);

        // A real prefix upper bound excludes every member of the prefix.
        let prefix =
            VectorMemberDataKey::encode_prefix(b"vec", u64::MAX, u64::MAX).expect("prefix");
        let upper = VectorMemberDataKey::prefix_upper_bound(&prefix).expect("bound");
        let member_key = VectorMemberDataKey {
            key: b"vec",
            storage_incarnation: u64::MAX,
            generation_sequence: u64::MAX,
            element: b"x",
        }
        .encode_full()
        .expect("encode");
        assert!(member_key >= prefix);
        assert!(member_key < upper);
    }

    mod props {
        use std::cmp::Ordering;

        use proptest::prelude::*;

        use super::*;

        fn arb_bytes(max_len: usize) -> impl Strategy<Value = Vec<u8>> {
            prop::collection::vec(any::<u8>(), 0..=max_len)
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(64))]

            /// Any (key, incarnation, generation, element) tuple must survive
            /// an encode/decode round trip with every field intact, including
            /// empty and 0x00-containing keys and elements.
            #[test]
            fn encode_decode_round_trip_preserves_all_fields(
                key in arb_bytes(1024),
                incarnation in any::<u64>(),
                generation in any::<u64>(),
                element in arb_bytes(512),
            ) {
                let member = VectorMemberDataKey {
                    key: &key,
                    storage_incarnation: incarnation,
                    generation_sequence: generation,
                    element: &element,
                };
                let encoded = member.encode_full().expect("encode full key");
                let parsed =
                    ParsedVectorMemberDataKey::decode(&encoded).expect("decode encoded key");
                prop_assert_eq!(parsed.key(), &key[..]);
                prop_assert_eq!(parsed.storage_incarnation(), incarnation);
                prop_assert_eq!(parsed.generation_sequence(), generation);
                prop_assert_eq!(parsed.element(), &element[..]);
            }

            /// `encode_key_prefix` ends after the user key, `encode_prefix`
            /// after the generation, and `encode_full` after the element, so
            /// each must be a prefix of the next.
            #[test]
            fn prefixes_are_strictly_nested(
                key in arb_bytes(256),
                incarnation in any::<u64>(),
                generation in any::<u64>(),
                element in arb_bytes(128),
            ) {
                let key_prefix =
                    VectorMemberDataKey::encode_key_prefix(&key).expect("encode key prefix");
                let prefix = VectorMemberDataKey::encode_prefix(&key, incarnation, generation)
                    .expect("encode prefix");
                let full = VectorMemberDataKey {
                    key: &key,
                    storage_incarnation: incarnation,
                    generation_sequence: generation,
                    element: &element,
                }
                .encode_full()
                .expect("encode full key");

                // The trailer always makes the key prefix strictly shorter.
                prop_assert!(prefix.starts_with(&key_prefix));
                prop_assert!(key_prefix.len() < prefix.len());

                prop_assert!(full.starts_with(&prefix));
                prop_assert!(prefix.len() <= full.len());
                // Strictly a prefix whenever the element is non-empty.
                prop_assert_eq!(prefix.len() < full.len(), !element.is_empty());
            }

            /// Every member of one (key, incarnation, generation) prefix must
            /// sort at or after the prefix and, when a lexicographic successor
            /// exists, strictly below the exclusive upper bound.
            #[test]
            fn member_keys_stay_inside_prefix_range(
                key in arb_bytes(256),
                incarnation in any::<u64>(),
                generation in any::<u64>(),
                element in arb_bytes(128),
            ) {
                let prefix = VectorMemberDataKey::encode_prefix(&key, incarnation, generation)
                    .expect("encode prefix");
                let full = VectorMemberDataKey {
                    key: &key,
                    storage_incarnation: incarnation,
                    generation_sequence: generation,
                    element: &element,
                }
                .encode_full()
                .expect("encode full key");

                prop_assert!(full >= prefix);
                if let Some(upper) = VectorMemberDataKey::prefix_upper_bound(&prefix) {
                    prop_assert!(full < upper);
                }
            }

            /// Prefixes of the same key order by (incarnation, generation) in
            /// big-endian byte order, and members of an earlier prefix always
            /// sort before members of a later one under the bytewise
            /// comparator, regardless of the element bytes.
            #[test]
            fn prefixes_order_by_incarnation_then_generation(
                key in arb_bytes(64),
                left in (any::<u64>(), any::<u64>()),
                right in (any::<u64>(), any::<u64>()),
                left_element in arb_bytes(32),
                right_element in arb_bytes(32),
            ) {
                let left_prefix =
                    VectorMemberDataKey::encode_prefix(&key, left.0, left.1).expect("left prefix");
                let right_prefix = VectorMemberDataKey::encode_prefix(&key, right.0, right.1)
                    .expect("right prefix");
                prop_assert_eq!(left_prefix.cmp(&right_prefix), left.cmp(&right));

                if left.cmp(&right) == Ordering::Less {
                    let left_full = VectorMemberDataKey {
                        key: &key,
                        storage_incarnation: left.0,
                        generation_sequence: left.1,
                        element: &left_element,
                    }
                    .encode_full()
                    .expect("left full");
                    let right_full = VectorMemberDataKey {
                        key: &key,
                        storage_incarnation: right.0,
                        generation_sequence: right.1,
                        element: &right_element,
                    }
                    .encode_full()
                    .expect("right full");
                    prop_assert!(left_full < right_full);
                }
            }

            /// Feeding arbitrary bytes to decode must only ever produce Ok or
            /// Err: never a panic, an out-of-bounds read, or a huge allocation
            /// driven by a hostile key_len.
            #[test]
            fn decode_arbitrary_bytes_never_panics(data in arb_bytes(300)) {
                let _ = ParsedVectorMemberDataKey::decode(&data);
            }
        }
    }
}
