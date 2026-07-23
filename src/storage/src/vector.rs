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

use snafu::ensure;

use crate::error::{InvalidArgumentSnafu, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalVector {
    dimension: u32,
    original_l2: f32,
    normalized: Vec<f32>,
}

impl CanonicalVector {
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
            norm_squared.is_finite() && norm_squared > 0.0,
            InvalidArgumentSnafu {
                message: "vector L2 norm must be finite and greater than zero".to_string()
            }
        );

        let norm = norm_squared.sqrt();
        let original_l2 = norm as f32;
        ensure!(
            original_l2.is_finite() && original_l2 > 0.0,
            InvalidArgumentSnafu {
                message: "vector L2 norm cannot be represented as FP32".to_string()
            }
        );

        let normalized = values
            .iter()
            .map(|value| (f64::from(*value) / norm) as f32)
            .collect();

        Ok(Self {
            dimension: values.len() as u32,
            original_l2,
            normalized,
        })
    }

    pub fn dimension(&self) -> u32 {
        self.dimension
    }

    pub fn original_l2(&self) -> f32 {
        self.original_l2
    }

    pub(crate) fn normalized(&self) -> &[f32] {
        &self.normalized
    }

    pub(crate) fn from_normalized_parts(
        dimension: u32,
        original_l2: f32,
        normalized: Vec<f32>,
    ) -> Self {
        Self {
            dimension,
            original_l2,
            normalized,
        }
    }

    pub fn score(&self, other: &Self) -> Result<f64> {
        ensure!(
            self.dimension == other.dimension,
            InvalidArgumentSnafu {
                message: format!(
                    "vector dimension mismatch: expected {}, got {}",
                    self.dimension, other.dimension
                )
            }
        );

        let dot = self
            .normalized
            .iter()
            .zip(&other.normalized)
            .map(|(left, right)| f64::from(*left) * f64::from(*right))
            .sum::<f64>()
            .clamp(-1.0, 1.0);
        Ok(((dot + 1.0) / 2.0).clamp(0.0, 1.0))
    }

    pub fn restore(&self) -> Vec<f64> {
        self.normalized
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
    fn canonical_vector_rejects_invalid_inputs() {
        assert!(CanonicalVector::from_values(&[]).is_err());
        assert!(CanonicalVector::from_values(&[0.0, 0.0]).is_err());
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
}
