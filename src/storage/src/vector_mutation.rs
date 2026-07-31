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

//! Logical vector-set mutation contract.
//!
//! `VectorSetMutationV1` is the deterministic, logical description of a
//! VADD/VREM: it carries the client intent (which element, which canonical
//! vector) but none of the derived state (count, data_revision, generation).
//! The single decision point is `Redis::apply_vector_set_mutation`, which
//! reads the current meta, decides the outcome, and commits it atomically.
//! Standalone VADD/VREM already go through that entry; Raft log replication
//! of logical mutations (PR0) will reuse the same entry so the state machine
//! decides in log order.
//!
//! Wire format (all integers little-endian):
//!
//!<pre>
//! | version(1B) | tag(1B) | payload                                        |
//!
//! tag 1 (Add):    | element_len(u32) | element | dimension(u32) |
//!                 | original_l2(f32) | vector_len(u32) | canonical_vector |
//! tag 2 (Remove): | element_len(u32) | element |
//! </pre>
//!
//! `canonical_vector` is the L2-normalized FP32 components as `dimension * 4`
//! little-endian bytes (NOQUANT form); the apply entry converts it to the
//! set's quantization. Length-prefixed fields let the decoder distinguish an
//! empty element from a missing one; malformed input yields an error, never
//! a panic.

use snafu::ensure;

use crate::error::{InvalidArgumentSnafu, InvalidFormatSnafu, RedisErrSnafu, Result};
use crate::vector::{CanonicalVector, QuantizationType, VectorData};

/// Format version byte; bumped when the wire layout changes.
pub const VECTOR_SET_MUTATION_VERSION_V1: u8 = 1;

const TAG_ADD: u8 = 1;
const TAG_REMOVE: u8 = 2;

/// A logical vector-set mutation, version 1.
#[derive(Debug, Clone, PartialEq)]
pub enum VectorSetMutationV1 {
    /// Add (or overwrite) one member.
    Add {
        element: Vec<u8>,
        dimension: u32,
        /// Pre-normalization L2 norm, preserved so VEMB can restore the
        /// original vector.
        original_l2: f32,
        /// L2-normalized FP32 components, `dimension * 4` LE bytes.
        canonical_vector: Vec<u8>,
    },
    /// Remove one member.
    Remove { element: Vec<u8> },
}

impl VectorSetMutationV1 {
    /// Build an `Add` mutation from a canonical vector, storing the normalized
    /// FP32 form so the apply entry can re-quantize to the set's quantization.
    pub fn add_from_canonical(element: &[u8], vector: &CanonicalVector) -> Result<Self> {
        let components = vector.as_fp32()?;
        let mut canonical_vector = Vec::with_capacity(components.len() * size_of::<f32>());
        for component in components {
            canonical_vector.extend_from_slice(&component.to_le_bytes());
        }
        Ok(Self::Add {
            element: element.to_vec(),
            dimension: vector.dimension(),
            original_l2: vector.original_l2(),
            canonical_vector,
        })
    }

    /// Reconstruct the canonical vector carried by an `Add` mutation.
    /// Returns `None` for `Remove`.
    pub(crate) fn canonical_vector(&self) -> Result<Option<CanonicalVector>> {
        let Self::Add {
            dimension,
            original_l2,
            canonical_vector,
            ..
        } = self
        else {
            return Ok(None);
        };
        Ok(Some(Self::decode_canonical_vector(
            *dimension,
            *original_l2,
            canonical_vector,
        )?))
    }

    fn decode_canonical_vector(
        dimension: u32,
        original_l2: f32,
        canonical_vector: &[u8],
    ) -> Result<CanonicalVector> {
        ensure!(
            dimension != 0,
            InvalidFormatSnafu {
                message: "vector mutation dimension must not be zero".to_string()
            }
        );
        ensure!(
            original_l2.is_finite() && original_l2 > 0.0,
            InvalidFormatSnafu {
                message: "vector mutation L2 norm must be finite and positive".to_string()
            }
        );
        let expected_len = dimension as usize * size_of::<f32>();
        ensure!(
            canonical_vector.len() == expected_len,
            InvalidFormatSnafu {
                message: format!(
                    "vector mutation payload length {} does not match dimension {}",
                    canonical_vector.len(),
                    dimension
                )
            }
        );
        let components = canonical_vector
            .chunks_exact(size_of::<f32>())
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect::<Vec<_>>();
        ensure!(
            components.iter().all(|component| component.is_finite()),
            InvalidFormatSnafu {
                message: "vector mutation components must be finite".to_string()
            }
        );
        Ok(CanonicalVector::from_parts(
            dimension,
            original_l2,
            QuantizationType::None,
            VectorData::Fp32(components),
        ))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut output = vec![VECTOR_SET_MUTATION_VERSION_V1];
        match self {
            Self::Add {
                element,
                dimension,
                original_l2,
                canonical_vector,
            } => {
                output.push(TAG_ADD);
                put_length_prefixed(&mut output, element);
                output.extend_from_slice(&dimension.to_le_bytes());
                output.extend_from_slice(&original_l2.to_le_bytes());
                put_length_prefixed(&mut output, canonical_vector);
            }
            Self::Remove { element } => {
                output.push(TAG_REMOVE);
                put_length_prefixed(&mut output, element);
            }
        }
        output
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = MutationReader::new(bytes);
        let version = reader.take_u8()?;
        ensure!(
            version == VECTOR_SET_MUTATION_VERSION_V1,
            InvalidFormatSnafu {
                message: format!("unsupported vector mutation version: {version}")
            }
        );
        let tag = reader.take_u8()?;
        let mutation = match tag {
            TAG_ADD => {
                let element = reader.take_length_prefixed()?;
                let dimension = reader.take_u32_le()?;
                let original_l2 = reader.take_f32_le()?;
                let canonical_vector = reader.take_length_prefixed()?;
                let mutation = Self::Add {
                    element,
                    dimension,
                    original_l2,
                    canonical_vector,
                };
                // Structural validation of the vector payload happens here so
                // a malformed mutation never reaches the apply entry.
                mutation.canonical_vector()?;
                mutation
            }
            TAG_REMOVE => Self::Remove {
                element: reader.take_length_prefixed()?,
            },
            _ => {
                return InvalidFormatSnafu {
                    message: format!("unsupported vector mutation tag: {tag}"),
                }
                .fail();
            }
        };
        ensure!(
            reader.is_exhausted(),
            InvalidFormatSnafu {
                message: format!("vector mutation has {} trailing bytes", reader.remaining())
            }
        );
        Ok(mutation)
    }
}

fn put_length_prefixed(output: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("mutation field length always fits in u32");
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
}

/// Bounds-checked cursor over the encoded mutation; every read is fallible so
/// malformed input produces an error instead of a panic.
struct MutationReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> MutationReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn is_exhausted(&self) -> bool {
        self.remaining() == 0
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        ensure!(
            self.remaining() >= len,
            InvalidFormatSnafu {
                message: format!(
                    "truncated vector mutation: need {len} bytes, have {}",
                    self.remaining()
                )
            }
        );
        let start = self.position;
        self.position += len;
        Ok(&self.bytes[start..self.position])
    }

    fn take_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn take_u32_le(&mut self) -> Result<u32> {
        let bytes = self.take(size_of::<u32>())?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn take_f32_le(&mut self) -> Result<f32> {
        let bytes = self.take(size_of::<f32>())?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn take_length_prefixed(&mut self) -> Result<Vec<u8>> {
        let len = self.take_u32_le()? as usize;
        Ok(self.take(len)?.to_vec())
    }
}

/// Outcome of applying a vector-set mutation. VADD maps `Added`/`Updated` to
/// 1/0; VREM maps `Removed`/`RemoveMissed` to 1/0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSetApplyResult {
    /// A new member was inserted (set created or member absent).
    Added,
    /// An existing member's vector was overwritten.
    Updated,
    /// The member existed and was removed.
    Removed,
    /// The member (or the set) did not exist; nothing changed.
    RemoveMissed,
}

/// Deterministic business rejections. A Raft apply loop must not treat these
/// as fatal: it records the deterministic error for the client and continues
/// with the next log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSetBusinessError {
    /// The key holds a live value of another data type.
    WrongType,
    /// The mutation's dimension differs from the existing set's dimension.
    DimensionMismatch { expected: u32, got: u32 },
}

impl std::fmt::Display for VectorSetBusinessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongType => {
                write!(
                    f,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            }
            Self::DimensionMismatch { expected, got } => {
                write!(
                    f,
                    "vector dimension mismatch: expected {expected}, got {got}"
                )
            }
        }
    }
}

/// Error from `Redis::apply_vector_set_mutation`, split by how a Raft apply
/// loop should react: business errors are deterministic and skippable, storage
/// errors are fatal.
#[derive(Debug)]
pub enum VectorSetApplyError {
    Business(VectorSetBusinessError),
    Storage(crate::error::Error),
}

impl std::fmt::Display for VectorSetApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Business(business) => write!(f, "{business}"),
            Self::Storage(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for VectorSetApplyError {}

impl From<crate::error::Error> for VectorSetApplyError {
    /// Anything not explicitly classified as a business error is fatal.
    fn from(error: crate::error::Error) -> Self {
        Self::Storage(error)
    }
}

impl From<VectorSetApplyError> for crate::error::Error {
    /// Map back to the storage error type with the exact variants and messages
    /// VADD/VREM produced before the apply entry existed, so standalone
    /// behavior is unchanged.
    fn from(error: VectorSetApplyError) -> Self {
        match error {
            VectorSetApplyError::Business(VectorSetBusinessError::WrongType) => RedisErrSnafu {
                message: "WRONGTYPE Operation against a key holding the wrong kind of value"
                    .to_string(),
            }
            .build(),
            VectorSetApplyError::Business(VectorSetBusinessError::DimensionMismatch {
                expected,
                got,
            }) => InvalidArgumentSnafu {
                message: format!("vector dimension mismatch: expected {expected}, got {got}"),
            }
            .build(),
            VectorSetApplyError::Storage(error) => error,
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn add_mutation(element: &[u8]) -> VectorSetMutationV1 {
        let vector = CanonicalVector::from_values(&[3.0, 4.0]).expect("valid vector");
        VectorSetMutationV1::add_from_canonical(element, &vector).expect("mutation from vector")
    }

    #[test]
    fn add_mutation_round_trips() {
        let mutation = add_mutation(b"member");
        let decoded = VectorSetMutationV1::decode(&mutation.encode()).expect("decode add");
        assert_eq!(decoded, mutation);

        let VectorSetMutationV1::Add {
            dimension,
            original_l2,
            canonical_vector,
            ..
        } = &mutation
        else {
            panic!("expected add mutation");
        };
        assert_eq!(*dimension, 2);
        assert!((*original_l2 - 5.0).abs() < 1e-6);
        assert_eq!(canonical_vector.len(), 2 * size_of::<f32>());

        let vector = mutation
            .canonical_vector()
            .expect("canonical vector")
            .expect("add carries a vector");
        let restored = vector.restore();
        assert!((restored[0] - 3.0).abs() < 1e-6);
        assert!((restored[1] - 4.0).abs() < 1e-6);
    }

    #[test]
    fn remove_mutation_round_trips() {
        let mutation = VectorSetMutationV1::Remove {
            element: b"member".to_vec(),
        };
        let decoded = VectorSetMutationV1::decode(&mutation.encode()).expect("decode remove");
        assert_eq!(decoded, mutation);
        assert!(decoded.canonical_vector().expect("no vector").is_none());
    }

    #[test]
    fn empty_element_round_trips() {
        for mutation in [
            add_mutation(b""),
            VectorSetMutationV1::Remove {
                element: Vec::new(),
            },
        ] {
            let decoded = VectorSetMutationV1::decode(&mutation.encode()).expect("decode");
            assert_eq!(decoded, mutation);
        }
    }

    #[test]
    fn large_element_round_trips() {
        let element = vec![0xABu8; 1 << 20];
        let mutation = add_mutation(&element);
        let decoded = VectorSetMutationV1::decode(&mutation.encode()).expect("decode");
        assert_eq!(decoded, mutation);
    }

    #[test]
    fn decode_rejects_malformed_bytes_without_panicking() {
        assert!(VectorSetMutationV1::decode(&[]).is_err());
        assert!(VectorSetMutationV1::decode(&[0, TAG_ADD]).is_err());
        assert!(VectorSetMutationV1::decode(&[VECTOR_SET_MUTATION_VERSION_V1, 0xFF]).is_err());

        // Every strict prefix of a valid encoding is a truncation error.
        let encoded = add_mutation(b"member").encode();
        for len in 0..encoded.len() {
            assert!(
                VectorSetMutationV1::decode(&encoded[..len]).is_err(),
                "prefix of length {len} must fail to decode"
            );
        }

        // Trailing garbage is rejected.
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(VectorSetMutationV1::decode(&trailing).is_err());
    }

    #[test]
    fn decode_rejects_invalid_vector_payload() {
        let valid = add_mutation(b"member").encode();

        // dimension = 0
        let mut zero_dimension = valid.clone();
        // layout: version(1) tag(1) elem_len(4) elem(6) dimension(4)
        zero_dimension[12..16].copy_from_slice(&0_u32.to_le_bytes());
        assert!(VectorSetMutationV1::decode(&zero_dimension).is_err());

        // canonical_vector length disagrees with dimension
        let mut bad_len = valid.clone();
        bad_len[20..24].copy_from_slice(&4_u32.to_le_bytes());
        assert!(VectorSetMutationV1::decode(&bad_len).is_err());

        // NaN original_l2
        let mut nan_l2 = valid.clone();
        nan_l2[16..20].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(VectorSetMutationV1::decode(&nan_l2).is_err());

        // NaN component
        let mut nan_component = valid.clone();
        let component_offset = 24;
        nan_component[component_offset..component_offset + 4]
            .copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(VectorSetMutationV1::decode(&nan_component).is_err());
    }

    #[test]
    fn apply_error_maps_back_to_storage_errors() {
        let wrong_type: crate::error::Error =
            VectorSetApplyError::Business(VectorSetBusinessError::WrongType).into();
        assert_eq!(
            wrong_type.to_string(),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );

        let mismatch: crate::error::Error =
            VectorSetApplyError::Business(VectorSetBusinessError::DimensionMismatch {
                expected: 2,
                got: 3,
            })
            .into();
        assert_eq!(
            mismatch.to_string(),
            "Invalid argument: vector dimension mismatch: expected 2, got 3"
        );

        let storage = crate::error::Error::OptionNone {
            message: "db is not initialized".to_string(),
            location: Default::default(),
        };
        let fatal = VectorSetApplyError::from(storage);
        assert!(matches!(fatal, VectorSetApplyError::Storage(_)));
    }
}
