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
use crate::search_types::{VectorFieldSchema, VectorValueType};

#[derive(Debug, Clone, PartialEq)]
pub enum DecodedVector {
    Float32(Vec<f32>),
}

pub trait VectorCodec: Sync {
    fn value_type(&self) -> VectorValueType;

    fn validate(&self, bytes: &[u8], dim: usize) -> Result<()>;

    fn decode(&self, bytes: &[u8], dim: usize) -> Result<DecodedVector>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Float32VectorCodec;

impl Float32VectorCodec {
    fn expected_len(dim: usize) -> Result<usize> {
        dim.checked_mul(std::mem::size_of::<f32>()).ok_or_else(|| {
            crate::error::Error::InvalidFormat {
                message: format!("vector dimension {dim} overflows byte length"),
                location: Location::new(file!(), line!(), column!()),
            }
        })
    }
}

impl VectorCodec for Float32VectorCodec {
    fn value_type(&self) -> VectorValueType {
        VectorValueType::Float32
    }

    fn validate(&self, bytes: &[u8], dim: usize) -> Result<()> {
        let expected_len = Self::expected_len(dim)?;
        if bytes.len() != expected_len {
            return Err(crate::error::Error::InvalidFormat {
                message: format!(
                    "vector byte length mismatch: expected {expected_len}, got {}",
                    bytes.len()
                ),
                location: Location::new(file!(), line!(), column!()),
            });
        }
        Ok(())
    }

    fn decode(&self, bytes: &[u8], dim: usize) -> Result<DecodedVector> {
        self.validate(bytes, dim)?;

        let mut values = Vec::with_capacity(dim);
        for chunk in bytes.chunks_exact(4) {
            let raw = [chunk[0], chunk[1], chunk[2], chunk[3]];
            values.push(f32::from_le_bytes(raw));
        }

        Ok(DecodedVector::Float32(values))
    }
}

static FLOAT32_VECTOR_CODEC: Float32VectorCodec = Float32VectorCodec;

pub fn codec_for_value_type(value_type: VectorValueType) -> Result<&'static dyn VectorCodec> {
    match value_type {
        VectorValueType::Float32 => Ok(&FLOAT32_VECTOR_CODEC),
    }
}

pub fn codec_for_schema(schema: &VectorFieldSchema) -> Result<&'static dyn VectorCodec> {
    codec_for_value_type(schema.value_type)
}

pub fn decode_f32_vector(bytes: &[u8], dim: usize) -> Result<Vec<f32>> {
    match FLOAT32_VECTOR_CODEC.decode(bytes, dim)? {
        DecodedVector::Float32(values) => Ok(values),
    }
}
