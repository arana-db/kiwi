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

use std::{cmp::Ordering, collections::BinaryHeap};

use snafu::ensure;

use crate::error::{InvalidArgumentSnafu, Result};
use crate::format_vector::SimilarityMetric;

/// Quantization type for vector storage.
/// Matches Redis vector set quantization options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QuantizationType {
    /// No quantization, store as FP32 (NOQUANT)
    None = 0,
    /// Binary quantization, 1 bit per component (BIN)
    Binary = 1,
    /// 8-bit integer quantization (Q8)
    Int8 = 2,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalVector {
    dimension: u32,
    original_l2: f32,
    quantization: QuantizationType,
    data: VectorData,
}

/// Internal representation of quantized vector data.
#[derive(Debug, Clone, PartialEq)]
pub enum VectorData {
    /// Unquantized FP32 values (NOQUANT)
    Fp32(Vec<f32>),
    /// Binary packed bits, dimension/8 bytes rounded up (BIN)
    Binary(Vec<u8>),
    /// 8-bit quantized values with min/max range (Q8)
    Int8 { values: Vec<i8>, min: f32, max: f32 },
}

impl CanonicalVector {
    /// Create a CanonicalVector from raw little-endian FP32 bytes.
    /// The vector is stored as-is (NOQUANT quantization).
    pub fn from_fp32_le(raw: &[u8]) -> Result<Self> {
        ensure!(
            !raw.is_empty() && raw.len().is_multiple_of(size_of::<f32>()),
            InvalidArgumentSnafu {
                message: "vector blob must contain one or more little-endian FP32 values"
                    .to_string()
            }
        );

        let values = raw
            .chunks_exact(size_of::<f32>())
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect::<Vec<_>>();
        Self::from_values(&values)
    }

    /// Create a CanonicalVector from FP32 values.
    /// Validates that all values are finite and the norm is representable.
    pub fn from_values(values: &[f32]) -> Result<Self> {
        ensure!(
            !values.is_empty(),
            InvalidArgumentSnafu {
                message: "vector must not be empty".to_string()
            }
        );
        ensure!(
            values.len() <= u32::MAX as usize,
            InvalidArgumentSnafu {
                message: "vector dimension exceeds u32::MAX".to_string()
            }
        );
        ensure!(
            values.iter().all(|value| value.is_finite()),
            InvalidArgumentSnafu {
                message: "vector components must be finite".to_string()
            }
        );

        let norm_squared = values
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>();
        ensure!(
            norm_squared.is_finite(),
            InvalidArgumentSnafu {
                message: "vector L2 norm must be finite".to_string()
            }
        );

        if norm_squared == 0.0 {
            return Ok(Self {
                dimension: values.len() as u32,
                original_l2: 0.0,
                quantization: QuantizationType::None,
                data: VectorData::Fp32(vec![0.0; values.len()]),
            });
        }

        let norm = norm_squared.sqrt();
        let original_l2 = norm as f32;
        ensure!(
            original_l2.is_finite() && original_l2 > 0.0,
            InvalidArgumentSnafu {
                message: "vector L2 norm cannot be represented as FP32".to_string()
            }
        );

        let normalized: Vec<f32> = values
            .iter()
            .map(|value| (f64::from(*value) / norm) as f32)
            .collect();

        Ok(Self {
            dimension: values.len() as u32,
            original_l2,
            quantization: QuantizationType::None,
            data: VectorData::Fp32(normalized),
        })
    }

    /// Quantize to binary (1 bit per component).
    /// Positive values become 1, negative/zero become 0.
    pub fn to_binary(&self) -> Result<Self> {
        let fp32 = self.as_fp32()?;
        let byte_count = (self.dimension as usize).div_ceil(8);
        let mut bits = vec![0u8; byte_count];

        for (i, &value) in fp32.iter().enumerate() {
            if value > 0.0 {
                bits[i / 8] |= 1 << (i % 8);
            }
        }

        Ok(Self {
            dimension: self.dimension,
            original_l2: self.original_l2,
            quantization: QuantizationType::Binary,
            data: VectorData::Binary(bits),
        })
    }

    /// Quantize to 8-bit integers using scalar quantization.
    /// Maps [min, max] to [-128, 127].
    pub fn to_int8(&self) -> Result<Self> {
        let fp32 = self.as_fp32()?;

        let min = fp32.iter().cloned().fold(f32::INFINITY, f32::min);
        let max = fp32.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let range = max - min;

        // A normalized vector can still have all components equal (e.g.
        // [1/sqrt(n), ...]); every code then dequantizes back to `min`.
        let values: Vec<i8> = if range > 0.0 {
            fp32.iter()
                .map(|&v| (((v - min) / range) * 255.0 - 128.0).round() as i8)
                .collect()
        } else {
            vec![-128; fp32.len()]
        };

        Ok(Self {
            dimension: self.dimension,
            original_l2: self.original_l2,
            quantization: QuantizationType::Int8,
            data: VectorData::Int8 { values, min, max },
        })
    }

    /// Convert this vector to `target` quantization, going through the FP32
    /// representation when the quantization differs.
    pub fn to_quantized(&self, target: QuantizationType) -> Result<Self> {
        if self.quantization == target {
            return Ok(self.clone());
        }
        match target {
            QuantizationType::None => Ok(Self::from_parts(
                self.dimension,
                self.original_l2,
                QuantizationType::None,
                VectorData::Fp32(self.as_fp32()?),
            )),
            QuantizationType::Binary => self.to_binary(),
            QuantizationType::Int8 => self.to_int8(),
        }
    }

    /// Get the quantization type of this vector.
    pub fn quantization(&self) -> QuantizationType {
        self.quantization
    }

    pub fn dimension(&self) -> u32 {
        self.dimension
    }

    pub fn original_l2(&self) -> f32 {
        self.original_l2
    }

    /// Get FP32 representation, converting from quantized format if needed.
    pub fn as_fp32(&self) -> Result<Vec<f32>> {
        match &self.data {
            VectorData::Fp32(values) => Ok(values.clone()),
            VectorData::Binary(bits) => {
                let mut values = Vec::with_capacity(self.dimension as usize);
                for i in 0..self.dimension as usize {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    let is_set = (bits[byte_idx] >> bit_idx) & 1;
                    values.push(if is_set == 1 { 1.0 } else { -1.0 });
                }
                Ok(values)
            }
            VectorData::Int8 { values, min, max } => {
                let range = max - min;
                let fp32: Vec<f32> = values
                    .iter()
                    .map(|&v| {
                        let normalized = (f32::from(v) + 128.0) / 255.0; // [0, 1]
                        min + normalized * range
                    })
                    .collect();
                Ok(fp32)
            }
        }
    }

    /// Get the raw quantized data (for storage encoding).
    pub(crate) fn data(&self) -> &VectorData {
        &self.data
    }

    /// Create from pre-quantized data (for loading from storage).
    pub(crate) fn from_parts(
        dimension: u32,
        original_l2: f32,
        quantization: QuantizationType,
        data: VectorData,
    ) -> Self {
        Self {
            dimension,
            original_l2,
            quantization,
            data,
        }
    }

    /// Score against another vector. Both vectors must share the same
    /// quantization: callers are expected to convert the query with
    /// `to_quantized` first (quantization is a per-set property).
    pub fn score(&self, other: &Self) -> Result<f64> {
        ensure!(
            self.quantization == other.quantization,
            InvalidArgumentSnafu {
                message: format!(
                    "vector quantization mismatch: {:?} vs {:?}",
                    self.quantization, other.quantization
                )
            }
        );
        match (&self.data, &other.data) {
            (VectorData::Binary(left), VectorData::Binary(right)) => {
                self.hamming_score(left, right)
            }
            // NOQUANT and Q8 both score on the (dequantized) FP32 form.
            // A native INT8 dot product can replace the Q8 path later.
            _ => self.cosine_score(other),
        }
    }

    /// Hamming similarity for binary vectors, mapped to [0, 1].
    /// Padding bits beyond `dimension` are zero on both sides and therefore
    /// never count as mismatches.
    fn hamming_score(&self, left: &[u8], right: &[u8]) -> Result<f64> {
        ensure!(
            left.len() == right.len(),
            InvalidArgumentSnafu {
                message: format!(
                    "binary vector length mismatch: {} vs {}",
                    left.len(),
                    right.len()
                )
            }
        );
        let mismatches = left
            .iter()
            .zip(right)
            .map(|(a, b)| u64::from((a ^ b).count_ones()))
            .sum::<u64>();
        let dimension = u64::from(self.dimension);
        Ok((dimension.saturating_sub(mismatches)) as f64 / dimension as f64)
    }

    pub fn cosine_score(&self, other: &Self) -> Result<f64> {
        ensure!(
            self.dimension == other.dimension,
            InvalidArgumentSnafu {
                message: format!(
                    "vector dimension mismatch: expected {}, got {}",
                    self.dimension, other.dimension
                )
            }
        );

        let self_fp32 = self.as_fp32()?;
        let other_fp32 = other.as_fp32()?;
        let dot = self_fp32
            .iter()
            .zip(&other_fp32)
            .map(|(left, right)| f64::from(*left) * f64::from(*right))
            .sum::<f64>()
            .clamp(-1.0, 1.0);
        Ok(((dot + 1.0) / 2.0).clamp(0.0, 1.0))
    }

    pub fn restore(&self) -> Vec<f64> {
        self.as_fp32()
            .expect("failed to restore vector")
            .iter()
            .map(|value| f64::from(*value) * f64::from(self.original_l2))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorQuery {
    Element(Vec<u8>),
    Vector(CanonicalVector),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchMode {
    Approximate,
    Truth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorSearchOptions {
    pub count: usize,
    pub mode: VectorSearchMode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorHit {
    pub element: Vec<u8>,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedVectorQuery {
    pub dimension: u32,
    pub element_query: Option<CanonicalVector>,
}

/// Per-set metadata reported by VINFO. Phase 1 only exposes what the stored
/// `VectorMeta` can answer in O(1); FLAT sentinels live in the command layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorInfo {
    pub dimension: u32,
    pub size: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct ScoredCandidate {
    element: Vec<u8>,
    score: f64,
}

impl PartialOrd for ScoredCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredCandidate {
    // Score is reversed so `BinaryHeap` (a max-heap) surfaces the *worst*
    // candidate via `peek()`, enabling top-`count` retention by evicting it.
    // The element tie-break is intentionally NOT reversed; it must match the
    // final `sort_by` ordering used when draining the heap.
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.element.cmp(&other.element))
    }
}

impl Eq for ScoredCandidate {}

/// Search strategy for vector-similarity queries.
///
/// Currently only exhaustive flat search is implemented. Future variants can
/// hold pre-built approximate indices such as HNSW.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchEngine {
    Flat,
}

impl VectorSearchEngine {
    /// Search `candidates` and return up to `count` best hits according to
    /// `metric`.
    pub fn search(
        &self,
        query: &CanonicalVector,
        metric: &SimilarityMetric,
        count: usize,
        candidates: impl Iterator<Item = Result<(Vec<u8>, CanonicalVector)>>,
    ) -> Result<Vec<VectorHit>> {
        match self {
            Self::Flat => Self::flat_search(query, metric, count, candidates),
        }
    }

    fn flat_search(
        query: &CanonicalVector,
        metric: &SimilarityMetric,
        count: usize,
        candidates: impl Iterator<Item = Result<(Vec<u8>, CanonicalVector)>>,
    ) -> Result<Vec<VectorHit>> {
        let mut heap = BinaryHeap::new();

        for candidate in candidates {
            let (element, vector) = candidate?;
            let score = metric.score(query, &vector)?;
            let item = ScoredCandidate { element, score };

            if heap.len() < count {
                heap.push(item);
            } else if heap.peek().is_some_and(|worst| item < *worst) {
                heap.pop();
                heap.push(item);
            }
        }

        let mut hits = heap
            .into_iter()
            .map(|item| VectorHit {
                element: item.element,
                score: item.score,
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.element.cmp(&right.element))
        });
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_vector_normalizes_and_restores_values() {
        let vector = CanonicalVector::from_values(&[3.0, 4.0]).expect("valid vector");
        assert_eq!(vector.dimension(), 2);
        assert!((vector.original_l2() - 5.0).abs() < 1e-6);
        let restored = vector.restore();
        assert!((restored[0] - 3.0).abs() < 1e-6);
        assert!((restored[1] - 4.0).abs() < 1e-6);
    }

    #[test]
    fn canonical_vector_accepts_zero_values_and_scores_neutrally() {
        let zero = CanonicalVector::from_values(&[0.0, 0.0]).expect("zero vector");
        let nonzero = CanonicalVector::from_values(&[1.0, 0.0]).expect("nonzero vector");

        assert_eq!(zero.original_l2(), 0.0);
        assert_eq!(zero.as_fp32().expect("fp32 payload"), &[0.0, 0.0]);
        assert_eq!(zero.restore(), vec![0.0, 0.0]);
        assert_eq!(zero.score(&nonzero).expect("cosine score"), 0.5);
    }

    #[test]
    fn canonical_vector_accepts_zero_fp32() {
        let raw = [0.0_f32, 0.0_f32]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        let zero = CanonicalVector::from_fp32_le(&raw).expect("zero FP32 vector");

        assert_eq!(zero.original_l2(), 0.0);
        assert_eq!(zero.restore(), vec![0.0, 0.0]);
    }

    #[test]
    fn canonical_vector_rejects_invalid_inputs() {
        assert!(CanonicalVector::from_values(&[]).is_err());
        assert!(CanonicalVector::from_values(&[f32::NAN]).is_err());
        assert!(CanonicalVector::from_fp32_le(&[0, 1, 2]).is_err());
    }

    #[test]
    fn cosine_score_maps_to_redis_range() {
        let x = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid x");
        let same = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid same");
        let opposite = CanonicalVector::from_values(&[-1.0, 0.0]).expect("valid opposite");
        assert!((x.score(&same).expect("score") - 1.0).abs() < 1e-12);
        assert!(x.score(&opposite).expect("score").abs() < 1e-12);
    }

    #[test]
    fn binary_score_uses_hamming_similarity() {
        let x = CanonicalVector::from_values(&[1.0, -1.0, 1.0, -1.0]).expect("valid x");
        let y = CanonicalVector::from_values(&[1.0, 1.0, 1.0, -1.0]).expect("valid y");
        let x_bin = x
            .to_quantized(QuantizationType::Binary)
            .expect("quantize x");
        let y_bin = y
            .to_quantized(QuantizationType::Binary)
            .expect("quantize y");
        // Signs differ in one of four components.
        assert!((x_bin.score(&y_bin).expect("score") - 0.75).abs() < 1e-12);
        // Mixed quantization is rejected instead of panicking.
        assert!(x.score(&y_bin).is_err());
    }

    #[test]
    fn int8_round_trip_approximately_restores_values() {
        let x = CanonicalVector::from_values(&[0.5, -0.25, 0.1, 0.9]).expect("valid x");
        let quantized = x.to_quantized(QuantizationType::Int8).expect("quantize");
        let restored = quantized.as_fp32().expect("dequantize");
        for (restored, original) in restored.iter().zip(x.as_fp32().expect("fp32")) {
            assert!((restored - original).abs() < 0.01);
        }
    }

    #[test]
    fn int8_handles_constant_vector() {
        let component = 1.0 / 4.0_f32.sqrt();
        let x = CanonicalVector::from_values(&[component; 4]).expect("valid x");
        let quantized = x.to_quantized(QuantizationType::Int8).expect("quantize");
        for value in quantized.as_fp32().expect("dequantize") {
            assert!((value - component).abs() < 1e-6);
        }
    }
}
