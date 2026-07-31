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

const VECTOR_VALUE_HEADER_LENGTH: usize = 16;
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
// | data_type | count | version | format | quant | metric | flags | dimension | data_revision | ctime | etime |
// |    1B     |  8B   |   8B    |   1B   |   1B  |   1B   |   1B  |    4B     |      8B       |  8B   |  8B   |
//
// `data_type` is DataType::VectorSet, `quant` is the quantization type (NOQUANT/BIN/Q8),
// and `metric` is the similarity metric used for VSIM (e.g. cosine).
// `version` holds the set's generation sequence: the identifier of one
// lifecycle of the key, assigned by the storage generation generator (or the
// creating Raft log index in cluster mode). `data_revision` starts at 1 when
// the set is created and is incremented by every successful VADD/VREM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorMeta {
    count: u64,
    pub(crate) version: u64,
    dimension: u32,
    quantization: QuantizationType,
    metric: SimilarityMetric,
    data_revision: u64,
    ctime: u64,
    etime: u64,
}

impl VectorMeta {
    pub(crate) fn new(
        count: u64,
        dimension: u32,
        quantization: QuantizationType,
        generation: u64,
    ) -> Self {
        let now = Utc::now().timestamp_micros() as u64;
        Self {
            count,
            version: generation,
            dimension,
            quantization,
            metric: SimilarityMetric::Cosine,
            data_revision: 1,
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
        output.put_u64_le(self.data_revision);
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
        let data_revision = reader.get_u64_le();
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
            flags == 0,
            InvalidFormatSnafu {
                message: "invalid non-zero vector meta flags".to_string()
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
            data_revision,
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

    pub fn data_revision(&self) -> u64 {
        self.data_revision
    }

    pub(crate) fn bump_data_revision(&mut self) {
        self.data_revision = self.data_revision.saturating_add(1);
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
// | magic | format | quant | flags | dimension | original_l2 | payload_len | [quant_params] | payload |
// |  1B   |   1B   |  1B   |  1B   |    4B     |     4B      |     4B      |    0B or 8B    | varies  |
//
// `magic` is VECTOR_VALUE_MAGIC and `original_l2` preserves the pre-normalization
// L2 norm so VEMB can reconstruct the original FP32 vector. `flags` is reserved
// for optional sections (e.g. bit 0 = trailing SETATTR attributes JSON) and
// must be zero until such a section is implemented. `payload_len` is the byte
// length of the payload section (excluding `quant_params`). The payload layout
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
        let params_length = match canonical.data() {
            VectorData::Int8 { .. } => VECTOR_Q8_PARAMS_LENGTH,
            _ => 0,
        };
        let payload_length = match canonical.data() {
            VectorData::Fp32(values) => values.len() * size_of::<f32>(),
            VectorData::Binary(bits) => bits.len(),
            VectorData::Int8 { values, .. } => values.len(),
        };
        let payload_len = u32::try_from(payload_length)
            .expect("vector payload length always fits in u32 for a valid canonical vector");
        let mut output =
            BytesMut::with_capacity(VECTOR_VALUE_HEADER_LENGTH + params_length + payload_length);
        output.put_u8(VECTOR_VALUE_MAGIC);
        output.put_u8(VECTOR_VALUE_FORMAT);
        output.put_u8(canonical.quantization().to_u8());
        output.put_u8(0); // flags: reserved, no optional sections yet
        output.put_u32_le(canonical.dimension());
        output.put_f32_le(canonical.original_l2());
        output.put_u32_le(payload_len);
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
        let payload_len = reader.get_u32_le() as usize;

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
                let expected_payload = dimension * size_of::<f32>();
                ensure!(
                    payload_len == expected_payload && reader.len() == expected_payload,
                    InvalidFormatSnafu {
                        message: format!(
                            "invalid vector payload length: payload_len {payload_len}, actual {} for dimension {}",
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
                let expected_payload = dimension.div_ceil(8);
                ensure!(
                    payload_len == expected_payload && reader.len() == expected_payload,
                    InvalidFormatSnafu {
                        message: format!(
                            "invalid binary vector payload length: payload_len {payload_len}, actual {} for dimension {}",
                            reader.len(),
                            dimension
                        )
                    }
                );
                VectorData::Binary(reader.to_vec())
            }
            QuantizationType::Int8 => {
                ensure!(
                    payload_len == dimension && reader.len() == VECTOR_Q8_PARAMS_LENGTH + dimension,
                    InvalidFormatSnafu {
                        message: format!(
                            "invalid q8 vector payload length: payload_len {payload_len}, actual {} for dimension {}",
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
    use crate::vector::CanonicalVector;

    use super::*;

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
        let mut meta = VectorMeta::new(2, 2, QuantizationType::None, 42);
        meta.bump_data_revision();
        let encoded = meta.encode();
        let decoded = VectorMeta::decode(&encoded).expect("decode vector meta");

        assert_eq!(decoded.count(), 2);
        assert_eq!(decoded.version(), 42);
        assert_eq!(decoded.dimension(), 2);
        assert_eq!(decoded.data_revision(), 2);
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

        // payload_len that disagrees with the actual payload size.
        let mut bad_payload_len = encoded_value.clone();
        bad_payload_len[12..16].copy_from_slice(&4_u32.to_le_bytes());
        assert!(VectorDataValue::decode(&bad_payload_len).is_err());

        let mut non_finite_payload = encoded_value;
        non_finite_payload[16..20].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(VectorDataValue::decode(&non_finite_payload).is_err());

        let encoded_meta = VectorMeta::new(2, 2, QuantizationType::None, 1).encode();
        assert!(VectorMeta::decode(&encoded_meta[..encoded_meta.len() - 1]).is_err());

        let mut bad_meta_format = encoded_meta;
        bad_meta_format[17] = 0;
        assert!(VectorMeta::decode(&bad_meta_format).is_err());

        let mut bad_metric = bad_meta_format;
        bad_metric[17] = VECTOR_META_FORMAT;
        bad_metric[19] = 0xFF;
        assert!(VectorMeta::decode(&bad_metric).is_err());
    }

    #[test]
    fn canonical_vector_rejects_infinite_components() {
        // NaN is covered in vector.rs; the same finiteness check must also
        // reject both infinities in any component position.
        assert!(CanonicalVector::from_values(&[f32::INFINITY, 1.0]).is_err());
        assert!(CanonicalVector::from_values(&[f32::NEG_INFINITY, 1.0]).is_err());
        assert!(CanonicalVector::from_values(&[1.0, f32::INFINITY]).is_err());
        assert!(CanonicalVector::from_values(&[1.0, f32::NEG_INFINITY]).is_err());
        let mut blob = Vec::new();
        blob.extend_from_slice(&f32::INFINITY.to_le_bytes());
        blob.extend_from_slice(&1.0_f32.to_le_bytes());
        assert!(CanonicalVector::from_fp32_le(&blob).is_err());
    }

    mod props {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(64))]

            /// Any combination of valid meta fields must round trip through
            /// the frozen byte layout unchanged. The encoded bytes are crafted
            /// directly so count/version/dimension/data_revision and the
            /// timestamps can take arbitrary values (the struct setters cannot
            /// reach every field).
            #[test]
            fn vector_meta_round_trips_arbitrary_valid_fields(
                count in any::<u64>(),
                version in any::<u64>(),
                dimension in 1u32..=u32::MAX,
                quantization in prop_oneof![
                    Just(QuantizationType::None),
                    Just(QuantizationType::Binary),
                    Just(QuantizationType::Int8),
                ],
                data_revision in any::<u64>(),
                ctime in any::<u64>(),
                etime in any::<u64>(),
            ) {
                let mut raw = BytesMut::new();
                raw.put_u8(DataType::VectorSet as u8);
                raw.put_u64_le(count);
                raw.put_u64_le(version);
                raw.put_u8(VECTOR_META_FORMAT);
                raw.put_u8(quantization.to_u8());
                raw.put_u8(SimilarityMetric::Cosine.to_u8());
                raw.put_u8(0); // flags
                raw.put_u32_le(dimension);
                raw.put_u64_le(data_revision);
                raw.put_u64_le(ctime);
                raw.put_u64_le(etime);

                let decoded = VectorMeta::decode(&raw).expect("decode valid meta");
                prop_assert_eq!(decoded.count(), count);
                prop_assert_eq!(decoded.version(), version);
                prop_assert_eq!(decoded.dimension(), dimension);
                prop_assert_eq!(decoded.quantization(), quantization);
                prop_assert_eq!(decoded.metric(), SimilarityMetric::Cosine);
                prop_assert_eq!(decoded.data_revision(), data_revision);
                prop_assert_eq!(&decoded.encode()[..], &raw[..]);
            }

            /// Any finite FP32 vector (dimension 1..=128, positive finite L2
            /// norm) must survive a NOQUANT encode/decode round trip exactly.
            #[test]
            fn vector_data_value_round_trips_finite_fp32(
                values in prop::collection::vec(-1.0e6f32..1.0e6f32, 1..=128usize),
            ) {
                // All-zero vectors are rejected by design; skip those cases.
                prop_assume!(CanonicalVector::from_values(&values).is_ok());
                let canonical = CanonicalVector::from_values(&values).expect("valid vector");
                let encoded = VectorDataValue::from_canonical(&canonical).encode();
                let decoded = VectorDataValue::decode(&encoded).expect("decode vector value");
                prop_assert_eq!(decoded.canonical(), &canonical);
            }

            /// Arbitrary bytes fed to either decoder must only produce Ok or
            /// Err: never a panic, an out-of-bounds read, or an allocation
            /// sized by hostile header fields.
            #[test]
            fn decoders_never_panic_on_arbitrary_bytes(
                data in prop::collection::vec(any::<u8>(), 0..=300),
            ) {
                let _ = VectorMeta::decode(&data);
                let _ = VectorDataValue::decode(&data);
            }
        }
    }
}
