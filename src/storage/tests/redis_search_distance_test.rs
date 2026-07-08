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

#![allow(clippy::unwrap_used)]

use storage::search_distance::{decode_f32_vector, distance, distance_for_schema};
use storage::search_types::{DistanceMetric, VectorAlgorithm, VectorFieldSchema, VectorValueType};

fn f32_bytes(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

#[test]
fn decode_f32_vector_length_check() {
    assert!(decode_f32_vector(&[0, 1, 2], 1).is_err());
}

#[test]
fn distance_metrics_match_expected_values() {
    let query = f32_bytes(&[1.0, 0.0, 0.0]);
    let vector = f32_bytes(&[0.0, 1.0, 0.0]);

    assert_eq!(
        distance(DistanceMetric::L2, &query, &vector, 3).unwrap(),
        2.0
    );
    assert_eq!(
        distance(DistanceMetric::IP, &query, &vector, 3).unwrap(),
        -0.0
    );

    let cosine = distance(DistanceMetric::Cosine, &query, &vector, 3).unwrap();
    assert!((cosine - 1.0).abs() < f32::EPSILON);
}

#[test]
fn distance_for_schema_uses_schema_fields() {
    let schema = VectorFieldSchema {
        algorithm: VectorAlgorithm::Flat,
        value_type: VectorValueType::Float32,
        dim: 3,
        distance_metric: DistanceMetric::L2,
    };
    let query = f32_bytes(&[1.0, 2.0, 3.0]);
    let vector = f32_bytes(&[1.0, 1.0, 1.0]);

    assert_eq!(distance_for_schema(&schema, &query, &vector).unwrap(), 5.0);
}
