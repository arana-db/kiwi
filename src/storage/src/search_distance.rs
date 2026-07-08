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

use snafu::Location;

use crate::error::Result;
use crate::search_types::{DistanceMetric, VectorFieldSchema};

pub fn decode_f32_vector(bytes: &[u8], dim: usize) -> Result<Vec<f32>> {
    let expected_len = dim.checked_mul(std::mem::size_of::<f32>()).ok_or_else(|| {
        crate::error::Error::InvalidFormat {
            message: format!("vector dimension {dim} overflows byte length"),
            location: Location::new(file!(), line!(), column!()),
        }
    })?;

    if bytes.len() != expected_len {
        return Err(crate::error::Error::InvalidFormat {
            message: format!(
                "vector byte length mismatch: expected {expected_len}, got {}",
                bytes.len()
            ),
            location: Location::new(file!(), line!(), column!()),
        });
    }

    let mut values = Vec::with_capacity(dim);
    for chunk in bytes.chunks_exact(4) {
        let raw = [chunk[0], chunk[1], chunk[2], chunk[3]];
        values.push(f32::from_le_bytes(raw));
    }

    Ok(values)
}

pub fn distance(metric: DistanceMetric, query: &[u8], vector: &[u8], dim: usize) -> Result<f32> {
    let query = decode_f32_vector(query, dim)?;
    let vector = decode_f32_vector(vector, dim)?;

    Ok(match metric {
        DistanceMetric::L2 => l2_distance(&query, &vector),
        DistanceMetric::IP => -dot_product(&query, &vector),
        DistanceMetric::Cosine => cosine_distance(&query, &vector),
    })
}

pub fn distance_for_schema(schema: &VectorFieldSchema, query: &[u8], vector: &[u8]) -> Result<f32> {
    distance(schema.distance_metric, query, vector, schema.dim as usize)
}

fn l2_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| {
            let diff = left - right;
            diff * diff
        })
        .sum()
}

fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum()
}

fn cosine_distance(left: &[f32], right: &[f32]) -> f32 {
    let dot = dot_product(left, right);
    let left_norm = left.iter().map(|value| value * value).sum::<f32>().sqrt();
    let right_norm = right.iter().map(|value| value * value).sum::<f32>().sqrt();

    if left_norm == 0.0 || right_norm == 0.0 {
        return 1.0;
    }

    1.0 - (dot / (left_norm * right_norm))
}
