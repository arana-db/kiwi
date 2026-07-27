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

use bytes::{Buf, BufMut, BytesMut};
use chrono::Utc;
use snafu::ensure;

use crate::{
    DataType,
    error::{InvalidFormatSnafu, Result},
    storage_define::BASE_META_VALUE_LENGTH,
    vector::{CanonicalVector, QuantizationType, VectorData},
};

pub const VECTOR_META_FORMAT: u8 = 1;

pub const VECTOR_ENCODING_NOQUANT: u8 = 0;
pub const VECTOR_ENCODING_BIN: u8 = 1;
pub const VECTOR_ENCODING_Q8: u8 = 2;

pub const VECTOR_METRIC_COSINE: u8 = 1;

pub const VECTOR_VALUE_MAGIC: u8 = 0x56;
pub const VECTOR_VALUE_FORMAT: u8 = 1;

const VECTOR_META_ZERO_RESERVE_LENGTH: usize = 8;
const VECTOR_VALUE_HEADER_LENGTH: usize = 12;
const VECTOR_Q8_PARAMS_LENGTH: usize = 8;

/// Similarity metric used to compare vectors in a vector set.
///
/// The metric is persisted in `VectorMeta` so all future VSIM queries against
/// the set use the same formula that was established at creation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimilarityMetric {
    Cosine,
}

impl QuantizationType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            VECTOR_ENCODING_NOQUANT => Ok(Self::None),
            VECTOR_ENCODING_BIN => Ok(Self::Binary),
            VECTOR_ENCODING_Q8 => Ok(Self::Int8),
            _ => InvalidFormatSnafu {
                message: format!("unsupported quantization type: {value}"),
            }
            .fail(),
        }
    }

    pub const fn to_u8(self) -> u8 {
        match self {
            Self::None => VECTOR_ENCODING_NOQUANT,
            Self::Binary => VECTOR_ENCODING_BIN,
            Self::Int8 => VECTOR_ENCODING_Q8,
        }
    }
}

impl SimilarityMetric {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            VECTOR_METRIC_COSINE => Ok(Self::Cosine),
            _ => InvalidFormatSnafu {
                message: format!("unsupported vector metric: {value}"),
            }
            .fail(),
        }
    }

    pub const fn to_u8(self) -> u8 {
        match self {
            Self::Cosine => VECTOR_METRIC_COSINE,
        }
    }

    pub fn score(&self, left: &CanonicalVector, right: &CanonicalVector) -> Result<f64> {
        match self {
            Self::Cosine => left.score(right),
        }
    }
}

// Vector set meta value layout stored in MetaCF:
//
// | data_type | count | version | format | quant | metric | flags | dimension | zero_reserve | ctime | etime |
// |    1B     |  8B   |   8B    |   1B   |   1B  |   1B   |   1B  |    4B     |     8B       |  8B   |  8B   |
//
// `data_type` is DataType::VectorSet, `quant` is the quantization type (NOQUANT/BIN/Q8),
// and `metric` is the similarity metric used for VSIM (e.g. cosine).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorMeta {
    count: u64,
    pub(crate) version: u64,
    dimension: u32,
    quantization: QuantizationType,
    metric: SimilarityMetric,
    ctime: u64,
    etime: u64,
}

impl VectorMeta {
    pub(crate) fn new_after(
        count: u64,
        dimension: u32,
        quantization: QuantizationType,
        previous_version: u64,
    ) -> Self {
        let now = Utc::now().timestamp_micros() as u64;
        let version = match previous_version >= now {
            true => previous_version + 1,
            false => now,
        };
        Self {
            count,
            version,
            dimension,
            quantization,
            metric: SimilarityMetric::Cosine,
            ctime: now,
            etime: 0,
        }
    }

    pub(crate) fn quantization(&self) -> QuantizationType {
        self.quantization
    }

    pub(crate) fn metric(&self) -> SimilarityMetric {
        self.metric
    }

    pub fn encode(&self) -> BytesMut {
        let mut output = BytesMut::with_capacity(BASE_META_VALUE_LENGTH);
        output.put_u8(DataType::VectorSet as u8);
        output.put_u64_le(self.count);
        output.put_u64_le(self.version);
        output.put_u8(VECTOR_META_FORMAT);
        output.put_u8(self.quantization.to_u8());
        output.put_u8(self.metric.to_u8());
        output.put_u8(0);
        output.put_u32_le(self.dimension);
        output.put_bytes(0, VECTOR_META_ZERO_RESERVE_LENGTH);
        output.put_u64_le(self.ctime);
        output.put_u64_le(self.etime);
        output
    }

    pub fn decode(value: &[u8]) -> Result<Self> {
        ensure!(
            value.len() == BASE_META_VALUE_LENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "invalid vector meta length: {} != {}",
                    value.len(),
                    BASE_META_VALUE_LENGTH
                )
            }
        );

        let mut reader = value;
        let data_type = DataType::try_from(reader.get_u8())?;
        ensure!(
            data_type == DataType::VectorSet,
            InvalidFormatSnafu {
                message: format!("invalid vector meta data type: {data_type:?}")
            }
        );

        let count = reader.get_u64_le();
        let version = reader.get_u64_le();
        let format = reader.get_u8();
        let quant = reader.get_u8();
        let metric = reader.get_u8();
        let flags = reader.get_u8();
        let dimension = reader.get_u32_le();
        let zero_reserve = &reader[..VECTOR_META_ZERO_RESERVE_LENGTH];
        reader.advance(VECTOR_META_ZERO_RESERVE_LENGTH);
        let ctime = reader.get_u64_le();
        let etime = reader.get_u64_le();

        ensure!(
            format == VECTOR_META_FORMAT,
            InvalidFormatSnafu {
                message: format!("unsupported vector meta format: {format}")
            }
        );
        let quantization = QuantizationType::from_u8(quant)?;
        let metric = SimilarityMetric::from_u8(metric)?;
        ensure!(
            flags == 0 && zero_reserve.iter().all(|byte| *byte == 0),
            InvalidFormatSnafu {
                message: "invalid non-zero vector meta reserve".to_string()
            }
        );
        ensure!(
            dimension != 0,
            InvalidFormatSnafu {
                message: "vector dimension must not be zero".to_string()
            }
        );

        Ok(Self {
            count,
            version,
            dimension,
            quantization,
            metric,
            ctime,
            etime,
        })
    }

    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    pub(crate) fn set_count(&mut self, count: u64) {
        self.count = count;
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    pub(crate) fn dimension(&self) -> u32 {
        self.dimension
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.etime != 0 && self.etime < Utc::now().timestamp_micros() as u64
    }

    pub fn set_etime(&mut self, etime: u64) {
        self.etime = etime;
    }
}

// Vector member data value layout stored in VectorDataCF:
//
// | magic | format | quant | flags | dimension | original_l2 | [quant_params] | payload |
// |  1B   |   1B   |  1B   |  1B   |    4B     |     4B      |    0B or 8B    | varies  |
//
// `magic` is VECTOR_VALUE_MAGIC and `original_l2` preserves the pre-normalization
// L2 norm so VEMB can reconstruct the original FP32 vector. `flags` is reserved
// for optional sections (e.g. bit 0 = trailing SETATTR attributes JSON) and
// must be zero until such a section is implemented. The payload layout
// depends on `quant`:
//   NOQUANT: 4B * dimension FP32 components, no quant_params.
//   BIN:     ceil(dimension / 8) bitmap bytes, no quant_params.
//   Q8:      1B * dimension INT8 codes, quant_params = min FP32 + max FP32.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorDataValue {
    canonical: CanonicalVector,
}

impl VectorDataValue {
    pub(crate) fn from_canonical(canonical: &CanonicalVector) -> Self {
        Self {
            canonical: canonical.clone(),
        }
    }

    pub(crate) fn encode(&self) -> BytesMut {
        let canonical = &self.canonical;
        let payload_length = match canonical.data() {
            VectorData::Fp32(values) => values.len() * size_of::<f32>(),
            VectorData::Binary(bits) => bits.len(),
            VectorData::Int8 { values, .. } => VECTOR_Q8_PARAMS_LENGTH + values.len(),
        };
        let mut output = BytesMut::with_capacity(VECTOR_VALUE_HEADER_LENGTH + payload_length);
        output.put_u8(VECTOR_VALUE_MAGIC);
        output.put_u8(VECTOR_VALUE_FORMAT);
        output.put_u8(canonical.quantization().to_u8());
        output.put_u8(0); // flags: reserved, no optional sections yet
        output.put_u32_le(canonical.dimension());
        output.put_f32_le(canonical.original_l2());
        match canonical.data() {
            VectorData::Fp32(values) => {
                for component in values {
                    output.put_f32_le(*component);
                }
            }
            VectorData::Binary(bits) => {
                output.put_slice(bits);
            }
            VectorData::Int8 { values, min, max } => {
                output.put_f32_le(*min);
                output.put_f32_le(*max);
                for value in values {
                    output.put_i8(*value);
                }
            }
        }
        output
    }

    pub(crate) fn decode(value: &[u8]) -> Result<Self> {
        ensure!(
            value.len() >= VECTOR_VALUE_HEADER_LENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "invalid vector value length: {} < {}",
                    value.len(),
                    VECTOR_VALUE_HEADER_LENGTH
                )
            }
        );

        let mut reader = value;
        let magic = reader.get_u8();
        let format = reader.get_u8();
        let quantization = QuantizationType::from_u8(reader.get_u8())?;
        let flags = reader.get_u8();
        let dimension = reader.get_u32_le();
        let original_l2 = reader.get_f32_le();

        ensure!(
            magic == VECTOR_VALUE_MAGIC,
            InvalidFormatSnafu {
                message: format!("invalid vector value magic: {magic:#04x}")
            }
        );
        ensure!(
            format == VECTOR_VALUE_FORMAT,
            InvalidFormatSnafu {
                message: format!("unsupported vector value format: {format}")
            }
        );
        ensure!(
            flags == 0,
            InvalidFormatSnafu {
                message: format!("unsupported vector value flags: {flags:#04x}")
            }
        );
        ensure!(
            dimension != 0,
            InvalidFormatSnafu {
                message: "vector value dimension must not be zero".to_string()
            }
        );
        ensure!(
            original_l2.is_finite() && original_l2 > 0.0,
            InvalidFormatSnafu {
                message: "vector value L2 norm must be finite and positive".to_string()
            }
        );

        let dimension = dimension as usize;
        let data = match quantization {
            QuantizationType::None => {
                ensure!(
                    reader.len() == dimension * size_of::<f32>(),
                    InvalidFormatSnafu {
                        message: format!(
                            "invalid vector payload length: {} for dimension {}",
                            reader.len(),
                            dimension
                        )
                    }
                );
                let normalized = reader
                    .chunks_exact(size_of::<f32>())
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect::<Vec<_>>();
                ensure!(
                    normalized.iter().all(|component| component.is_finite()),
                    InvalidFormatSnafu {
                        message: "vector payload components must be finite".to_string()
                    }
                );
                VectorData::Fp32(normalized)
            }
            QuantizationType::Binary => {
                ensure!(
                    reader.len() == dimension.div_ceil(8),
                    InvalidFormatSnafu {
                        message: format!(
                            "invalid binary vector payload length: {} for dimension {}",
                            reader.len(),
                            dimension
                        )
                    }
                );
                VectorData::Binary(reader.to_vec())
            }
            QuantizationType::Int8 => {
                ensure!(
                    reader.len() == VECTOR_Q8_PARAMS_LENGTH + dimension,
                    InvalidFormatSnafu {
                        message: format!(
                            "invalid q8 vector payload length: {} for dimension {}",
                            reader.len(),
                            dimension
                        )
                    }
                );
                let min = reader.get_f32_le();
                let max = reader.get_f32_le();
                ensure!(
                    min.is_finite() && max.is_finite(),
                    InvalidFormatSnafu {
                        message: "q8 quantization params must be finite".to_string()
                    }
                );
                let values = reader.iter().map(|byte| *byte as i8).collect();
                VectorData::Int8 { values, min, max }
            }
        };

        Ok(Self {
            canonical: CanonicalVector::from_parts(
                dimension as u32,
                original_l2,
                quantization,
                data,
            ),
        })
    }

    pub(crate) fn dimension(&self) -> u32 {
        self.canonical.dimension()
    }

    pub(crate) fn canonical(&self) -> &CanonicalVector {
        &self.canonical
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        format_member_data_key::{MemberDataKey, ParsedMemberDataKey},
        vector::CanonicalVector,
    };

    use super::*;

    #[test]
    fn member_data_key_round_trips_empty_binary_element() {
        let encoded = MemberDataKey::new(b"vectors\0key", 42, b"")
            .encode()
            .expect("encode member key");
        let decoded = ParsedMemberDataKey::new(&encoded).expect("decode member key");

        assert_eq!(decoded.key(), b"vectors\0key");
        assert_eq!(decoded.version(), 42);
        assert_eq!(decoded.data(), b"");
    }

    #[test]
    fn vector_data_value_round_trips() {
        let canonical = CanonicalVector::from_values(&[3.0, 4.0]).expect("valid vector");
        let encoded = VectorDataValue::from_canonical(&canonical).encode();
        let decoded = VectorDataValue::decode(&encoded).expect("decode vector value");

        assert_eq!(decoded.dimension(), 2);
        assert!((decoded.canonical().original_l2() - 5.0).abs() < 1e-6);
        assert_eq!(decoded.canonical(), &canonical);
    }

    #[test]
    fn vector_data_value_round_trips_quantized() {
        let canonical = CanonicalVector::from_values(&[3.0, 4.0, -1.0, 2.0]).expect("valid vector");
        for quantization in [QuantizationType::Binary, QuantizationType::Int8] {
            let quantized = canonical.to_quantized(quantization).expect("quantize");
            let encoded = VectorDataValue::from_canonical(&quantized).encode();
            let decoded = VectorDataValue::decode(&encoded).expect("decode vector value");
            assert_eq!(decoded.canonical(), &quantized);
        }
    }

    #[test]
    fn vector_meta_round_trips() {
        let mut meta = VectorMeta::new_after(2, 2, QuantizationType::None, 0);
        meta.version = 42;
        let encoded = meta.encode();
        let decoded = VectorMeta::decode(&encoded).expect("decode vector meta");

        assert_eq!(decoded.count(), 2);
        assert_eq!(decoded.version(), 42);
        assert_eq!(decoded.dimension(), 2);
        assert!(!decoded.is_stale());
    }

    #[test]
    fn vector_codecs_reject_malformed_bytes() {
        let canonical = CanonicalVector::from_values(&[3.0, 4.0]).expect("valid vector");
        let encoded_value = VectorDataValue::from_canonical(&canonical).encode();

        let mut bad_magic = encoded_value.clone();
        bad_magic[0] = 0;
        assert!(VectorDataValue::decode(&bad_magic).is_err());

        let mut bad_quant = encoded_value.clone();
        bad_quant[2] = 0xFF;
        assert!(VectorDataValue::decode(&bad_quant).is_err());

        let mut bad_flags = encoded_value.clone();
        bad_flags[3] = 0xFF;
        assert!(VectorDataValue::decode(&bad_flags).is_err());

        let mut zero_dimension = encoded_value.clone();
        zero_dimension[4..8].copy_from_slice(&0_u32.to_le_bytes());
        assert!(VectorDataValue::decode(&zero_dimension).is_err());

        let mut non_finite_payload = encoded_value;
        non_finite_payload[12..16].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(VectorDataValue::decode(&non_finite_payload).is_err());

        let encoded_meta = VectorMeta::new_after(2, 2, QuantizationType::None, 0).encode();
        assert!(VectorMeta::decode(&encoded_meta[..encoded_meta.len() - 1]).is_err());

        let mut bad_meta_format = encoded_meta;
        bad_meta_format[17] = 0;
        assert!(VectorMeta::decode(&bad_meta_format).is_err());

        let mut bad_metric = bad_meta_format;
        bad_metric[17] = VECTOR_META_FORMAT;
        bad_metric[19] = 0xFF;
        assert!(VectorMeta::decode(&bad_metric).is_err());
    }
}
