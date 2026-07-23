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
    vector::CanonicalVector,
};

pub const VECTOR_META_FORMAT: u8 = 1;
pub const VECTOR_ENCODING_FP32_LE: u8 = 1;
pub const VECTOR_METRIC_COSINE: u8 = 1;
pub const VECTOR_VALUE_MAGIC: u8 = 0x56;
pub const VECTOR_VALUE_FORMAT: u8 = 1;

const VECTOR_META_ZERO_RESERVE_LENGTH: usize = 8;
const VECTOR_VALUE_HEADER_LENGTH: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorMeta {
    count: u64,
    pub(crate) version: u64,
    dimension: u32,
    ctime: u64,
    etime: u64,
}

impl VectorMeta {
    pub(crate) fn new(count: u64, dimension: u32) -> Self {
        let now = Utc::now().timestamp_micros() as u64;
        Self {
            count,
            version: now,
            dimension,
            ctime: now,
            etime: 0,
        }
    }

    pub(crate) fn encode(&self) -> BytesMut {
        let mut output = BytesMut::with_capacity(BASE_META_VALUE_LENGTH);
        output.put_u8(DataType::VectorSet as u8);
        output.put_u64_le(self.count);
        output.put_u64_le(self.version);
        output.put_u8(VECTOR_META_FORMAT);
        output.put_u8(VECTOR_ENCODING_FP32_LE);
        output.put_u8(VECTOR_METRIC_COSINE);
        output.put_u8(0);
        output.put_u32_le(self.dimension);
        output.put_bytes(0, VECTOR_META_ZERO_RESERVE_LENGTH);
        output.put_u64_le(self.ctime);
        output.put_u64_le(self.etime);
        output
    }

    pub(crate) fn decode(value: &[u8]) -> Result<Self> {
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
        let encoding = reader.get_u8();
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
        ensure!(
            encoding == VECTOR_ENCODING_FP32_LE,
            InvalidFormatSnafu {
                message: format!("unsupported vector encoding: {encoding}")
            }
        );
        ensure!(
            metric == VECTOR_METRIC_COSINE,
            InvalidFormatSnafu {
                message: format!("unsupported vector metric: {metric}")
            }
        );
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

    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    pub(crate) fn dimension(&self) -> u32 {
        self.dimension
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.etime != 0 && self.etime < Utc::now().timestamp_micros() as u64
    }
}

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
        let mut output = BytesMut::with_capacity(
            VECTOR_VALUE_HEADER_LENGTH + size_of_val(self.canonical.normalized()),
        );
        output.put_u8(VECTOR_VALUE_MAGIC);
        output.put_u8(VECTOR_VALUE_FORMAT);
        output.put_u32_le(self.canonical.dimension());
        output.put_f32_le(self.canonical.original_l2());
        for component in self.canonical.normalized() {
            output.put_f32_le(*component);
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

        let payload_length = (dimension as usize).checked_mul(size_of::<f32>());
        ensure!(
            payload_length.is_some_and(|length| reader.len() == length),
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

        Ok(Self {
            canonical: CanonicalVector::from_normalized_parts(dimension, original_l2, normalized),
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
    fn vector_meta_round_trips() {
        let mut meta = VectorMeta::new(2, 2);
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

        let mut zero_dimension = encoded_value.clone();
        zero_dimension[2..6].copy_from_slice(&0_u32.to_le_bytes());
        assert!(VectorDataValue::decode(&zero_dimension).is_err());

        let mut non_finite_payload = encoded_value;
        non_finite_payload[10..14].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(VectorDataValue::decode(&non_finite_payload).is_err());

        let encoded_meta = VectorMeta::new(2, 2).encode();
        assert!(VectorMeta::decode(&encoded_meta[..encoded_meta.len() - 1]).is_err());

        let mut bad_meta_format = encoded_meta;
        bad_meta_format[17] = 0;
        assert!(VectorMeta::decode(&bad_meta_format).is_err());
    }
}
