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

use crate::error::Result;
use crate::search_codec::{DecodedVector, codec_for_schema};
use crate::search_types::{DistanceMetric, VectorFieldSchema};

pub trait VectorDistance: Sync {
    fn metric(&self) -> DistanceMetric;

    fn distance(&self, left: &[f32], right: &[f32]) -> f32;
}

#[derive(Debug, Clone, Copy, Default)]
struct L2Distance;

impl VectorDistance for L2Distance {
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::L2
    }

    fn distance(&self, left: &[f32], right: &[f32]) -> f32 {
        left.iter()
            .zip(right.iter())
            .map(|(left, right)| {
                let diff = left - right;
                diff * diff
            })
            .sum()
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct InnerProductDistance;

impl VectorDistance for InnerProductDistance {
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::IP
    }

    fn distance(&self, left: &[f32], right: &[f32]) -> f32 {
        -dot_product(left, right)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct CosineDistance;

impl VectorDistance for CosineDistance {
    fn metric(&self) -> DistanceMetric {
        DistanceMetric::Cosine
    }

    fn distance(&self, left: &[f32], right: &[f32]) -> f32 {
        let dot = dot_product(left, right);
        let left_norm = left.iter().map(|value| value * value).sum::<f32>().sqrt();
        let right_norm = right.iter().map(|value| value * value).sum::<f32>().sqrt();

        if left_norm == 0.0 || right_norm == 0.0 {
            return 1.0;
        }

        1.0 - (dot / (left_norm * right_norm))
    }
}

fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum()
}

static L2_DISTANCE: L2Distance = L2Distance;
static INNER_PRODUCT_DISTANCE: InnerProductDistance = InnerProductDistance;
static COSINE_DISTANCE: CosineDistance = CosineDistance;

pub fn decode_f32_vector(bytes: &[u8], dim: usize) -> Result<Vec<f32>> {
    crate::search_codec::decode_f32_vector(bytes, dim)
}

pub fn calculator_for_metric(metric: DistanceMetric) -> Result<&'static dyn VectorDistance> {
    match metric {
        DistanceMetric::L2 => Ok(&L2_DISTANCE),
        DistanceMetric::IP => Ok(&INNER_PRODUCT_DISTANCE),
        DistanceMetric::Cosine => Ok(&COSINE_DISTANCE),
    }
}

pub fn distance(metric: DistanceMetric, query: &[u8], vector: &[u8], dim: usize) -> Result<f32> {
    let query = decode_f32_vector(query, dim)?;
    let vector = decode_f32_vector(vector, dim)?;
    let calculator = calculator_for_metric(metric)?;
    Ok(calculator.distance(&query, &vector))
}

pub fn distance_for_schema(schema: &VectorFieldSchema, query: &[u8], vector: &[u8]) -> Result<f32> {
    let codec = codec_for_schema(schema)?;
    let query = codec.decode(query, schema.dim as usize)?;
    let vector = codec.decode(vector, schema.dim as usize)?;
    let calculator = calculator_for_metric(schema.distance_metric)?;

    match (query, vector) {
        (DecodedVector::Float32(query), DecodedVector::Float32(vector)) => {
            Ok(calculator.distance(&query, &vector))
        }
    }
}
