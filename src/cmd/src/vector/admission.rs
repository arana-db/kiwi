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

use std::fmt;

use bytes::Bytes;

use super::{ERR_VECTOR_BYTES_LIMIT, ERR_VECTOR_DIMENSION_LIMIT, ERR_VECTOR_ELEMENT_LIMIT};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorAdmissionLimits {
    pub max_dimension: usize,
    pub max_element_bytes: usize,
    pub max_vector_bytes: usize,
}

impl From<&conf::vector_config::VectorConfig> for VectorAdmissionLimits {
    fn from(config: &conf::vector_config::VectorConfig) -> Self {
        Self {
            max_dimension: config.max_dimension as usize,
            max_element_bytes: config.max_element_bytes,
            max_vector_bytes: config.max_vector_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorAdmissionError {
    DimensionLimit,
    ElementLimit,
    VectorBytesLimit,
}

impl VectorAdmissionError {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DimensionLimit => ERR_VECTOR_DIMENSION_LIMIT,
            Self::ElementLimit => ERR_VECTOR_ELEMENT_LIMIT,
            Self::VectorBytesLimit => ERR_VECTOR_BYTES_LIMIT,
        }
    }
}

impl fmt::Display for VectorAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for VectorAdmissionError {}

enum ParsedDimension {
    Invalid,
    Overflow,
    Value(usize),
}

fn parse_positive_decimal(raw: &[u8]) -> ParsedDimension {
    let digits = match raw {
        [b'+', rest @ ..] if !rest.is_empty() => rest,
        _ => raw,
    };
    if digits.is_empty() {
        return ParsedDimension::Invalid;
    }

    let mut value = 0usize;
    for byte in digits {
        if !byte.is_ascii_digit() {
            return ParsedDimension::Invalid;
        }
        let Some(next) = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(usize::from(byte - b'0')))
        else {
            return ParsedDimension::Overflow;
        };
        value = next;
    }

    if value == 0 {
        ParsedDimension::Invalid
    } else {
        ParsedDimension::Value(value)
    }
}

fn check_element(
    element: Option<&Bytes>,
    limits: VectorAdmissionLimits,
) -> Result<(), VectorAdmissionError> {
    if element.is_some_and(|element| element.len() > limits.max_element_bytes) {
        return Err(VectorAdmissionError::ElementLimit);
    }
    Ok(())
}

fn checked_vector_token_bytes(
    lengths: impl IntoIterator<Item = usize>,
) -> Result<usize, VectorAdmissionError> {
    lengths.into_iter().try_fold(0usize, |total, length| {
        total
            .checked_add(length)
            .ok_or(VectorAdmissionError::VectorBytesLimit)
    })
}

fn admit_direct_vector(
    argv: &[Bytes],
    has_vadd_element: bool,
    limits: VectorAdmissionLimits,
) -> Result<(), VectorAdmissionError> {
    let Some(kind) = argv.get(2) else {
        return Ok(());
    };

    if kind.eq_ignore_ascii_case(b"FP32") {
        let Some(raw) = argv.get(3) else {
            return Ok(());
        };
        if raw.len() > limits.max_vector_bytes {
            return Err(VectorAdmissionError::VectorBytesLimit);
        }
        if raw.len() / std::mem::size_of::<f32>() > limits.max_dimension {
            return Err(VectorAdmissionError::DimensionLimit);
        }
        if has_vadd_element {
            check_element(argv.get(4), limits)?;
        }
        return Ok(());
    }

    if !kind.eq_ignore_ascii_case(b"VALUES") {
        return Ok(());
    }

    let Some(raw_dimension) = argv.get(3) else {
        return Ok(());
    };
    let dimension = match parse_positive_decimal(raw_dimension) {
        ParsedDimension::Invalid => return Ok(()),
        ParsedDimension::Overflow => return Err(VectorAdmissionError::DimensionLimit),
        ParsedDimension::Value(dimension) => dimension,
    };
    if dimension > limits.max_dimension {
        return Err(VectorAdmissionError::DimensionLimit);
    }

    let vector_bytes = dimension
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or(VectorAdmissionError::VectorBytesLimit)?;
    if vector_bytes > limits.max_vector_bytes {
        return Err(VectorAdmissionError::VectorBytesLimit);
    }

    const VALUES_START: usize = 4;
    let available = argv.len().saturating_sub(VALUES_START);
    let provided_values = dimension.min(available);
    let values_end = VALUES_START + provided_values;
    let raw_bytes =
        checked_vector_token_bytes(argv[VALUES_START..values_end].iter().map(Bytes::len))?;
    if raw_bytes > limits.max_vector_bytes {
        return Err(VectorAdmissionError::VectorBytesLimit);
    }

    if has_vadd_element && available > dimension {
        check_element(argv.get(VALUES_START + dimension), limits)?;
    }
    Ok(())
}

pub fn admit_vector_request(
    argv: &[Bytes],
    limits: VectorAdmissionLimits,
) -> Result<(), VectorAdmissionError> {
    let Some(command) = argv.first() else {
        return Ok(());
    };

    if command.eq_ignore_ascii_case(b"VADD") {
        admit_direct_vector(argv, true, limits)
    } else if command.eq_ignore_ascii_case(b"VSIM") {
        if argv
            .get(2)
            .is_some_and(|kind| kind.eq_ignore_ascii_case(b"ELE"))
        {
            check_element(argv.get(3), limits)
        } else {
            admit_direct_vector(argv, false, limits)
        }
    } else if command.eq_ignore_ascii_case(b"VEMB")
        || command.eq_ignore_ascii_case(b"VREM")
        || command.eq_ignore_ascii_case(b"VISMEMBER")
    {
        check_element(argv.get(2), limits)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        VectorAdmissionError, VectorAdmissionLimits, admit_vector_request,
        checked_vector_token_bytes,
    };

    fn argv(parts: &[&[u8]]) -> Vec<Bytes> {
        parts.iter().copied().map(Bytes::copy_from_slice).collect()
    }

    fn limits(
        max_dimension: usize,
        max_element_bytes: usize,
        max_vector_bytes: usize,
    ) -> VectorAdmissionLimits {
        VectorAdmissionLimits {
            max_dimension,
            max_element_bytes,
            max_vector_bytes,
        }
    }

    #[test]
    fn admission_checks_fp32_actual_bytes_and_dimension_bytes() {
        let config = conf::vector_config::VectorConfig {
            max_dimension: 2,
            max_element_bytes: 4,
            max_vector_bytes: 8,
            ..Default::default()
        };
        assert_eq!(VectorAdmissionLimits::from(&config), limits(2, 4, 8));

        assert_eq!(
            admit_vector_request(&argv(&[b"VSIM", b"key", b"FP32", &[0; 9]]), limits(8, 8, 8),),
            Err(VectorAdmissionError::VectorBytesLimit)
        );
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VSIM", b"key", b"FP32", &[0; 9]]),
                limits(1, 8, 16),
            ),
            Err(VectorAdmissionError::DimensionLimit)
        );
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VSIM", b"key", b"FP32", &[0; 12]]),
                limits(2, 8, 16),
            ),
            Err(VectorAdmissionError::DimensionLimit)
        );
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VADD", b"key", b"FP32", &[0; 8], b"large"]),
                limits(2, 4, 8),
            ),
            Err(VectorAdmissionError::ElementLimit)
        );
    }

    #[test]
    fn admission_counts_values_raw_token_bytes() {
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VSIM", b"key", b"VALUES", b"2", b"12345", b"6789"]),
                limits(4, 8, 8),
            ),
            Err(VectorAdmissionError::VectorBytesLimit)
        );
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VSIM", b"key", b"VALUES", b"3", b"1", b"2", b"3"]),
                limits(4, 8, 8),
            ),
            Err(VectorAdmissionError::VectorBytesLimit)
        );
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VSIM", b"key", b"VALUES", b"1", b"12345"]),
                limits(4, 8, 8),
            ),
            Ok(())
        );
    }

    #[test]
    fn admission_checks_all_element_payload_commands() {
        let cases = [
            argv(&[b"VADD", b"key", b"FP32", &[0; 4], b"large"]),
            argv(&[b"VADD", b"key", b"VALUES", b"1", b"1", b"large"]),
            argv(&[b"VSIM", b"key", b"ELE", b"large"]),
            argv(&[b"VEMB", b"key", b"large"]),
            argv(&[b"vrem", b"key", b"large"]),
            argv(&[b"VISMEMBER", b"key", b"large"]),
        ];

        for command in cases {
            assert_eq!(
                admit_vector_request(&command, limits(4, 4, 16)),
                Err(VectorAdmissionError::ElementLimit),
                "command: {command:?}"
            );
        }
    }

    #[test]
    fn admission_rejects_checked_arithmetic_overflow() {
        assert_eq!(
            VectorAdmissionError::DimensionLimit.as_str(),
            "ERR vector dimension exceeds max_dimension"
        );
        assert_eq!(
            VectorAdmissionError::ElementLimit.to_string(),
            "ERR vector element exceeds max_element_bytes"
        );
        assert_eq!(
            VectorAdmissionError::VectorBytesLimit.as_str(),
            "ERR vector exceeds max_vector_bytes"
        );

        let decimal_overflow = format!("{}0", usize::MAX);
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VSIM", b"key", b"VALUES", decimal_overflow.as_bytes(),]),
                limits(usize::MAX, 8, usize::MAX),
            ),
            Err(VectorAdmissionError::DimensionLimit)
        );

        let multiplication_overflow = (usize::MAX / std::mem::size_of::<f32>()) + 1;
        let multiplication_overflow = multiplication_overflow.to_string();
        assert_eq!(
            admit_vector_request(
                &argv(&[
                    b"VSIM",
                    b"key",
                    b"VALUES",
                    multiplication_overflow.as_bytes(),
                ]),
                limits(usize::MAX, 8, usize::MAX),
            ),
            Err(VectorAdmissionError::VectorBytesLimit)
        );

        assert_eq!(
            checked_vector_token_bytes([usize::MAX, 1]),
            Err(VectorAdmissionError::VectorBytesLimit)
        );
    }

    #[test]
    fn admission_defers_under_limit_invalid_syntax_to_command_parser() {
        let cases = [
            argv(&[b"VSIM", b"key", b"VALUES", b""]),
            argv(&[b"VSIM", b"key", b"VALUES", b"-1"]),
            argv(&[b"VSIM", b"key", b"VALUES", b"not-a-number"]),
            argv(&[b"VSIM", b"key", b"VALUES", b"\xff"]),
            argv(&[b"VSIM", b"key", b"VALUES", b"0"]),
            argv(&[b"VSIM", b"key", b"VALUES", b" 1", b"1"]),
            argv(&[b"VSIM", b"key", b"FP32", &[0; 3]]),
            argv(&[b"VSIM", b"key", b"UNKNOWN", b"large"]),
            argv(&[b"UNKNOWN", b"key", b"VALUES", b"999"]),
            argv(&[]),
        ];

        for command in cases {
            assert_eq!(
                admit_vector_request(&command, limits(4, 4, 16)),
                Ok(()),
                "command: {command:?}"
            );
        }

        assert_eq!(
            admit_vector_request(
                &argv(&[b"VSIM", b"key", b"VALUES", b"+1", b"1"]),
                limits(0, 4, 16),
            ),
            Err(VectorAdmissionError::DimensionLimit)
        );
    }

    #[test]
    fn admission_checks_partial_values_before_parser_error() {
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VADD", b"key", b"VALUES", b"3", b"12345", b"6789"]),
                limits(4, 16, 8),
            ),
            Err(VectorAdmissionError::VectorBytesLimit)
        );
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VADD", b"key", b"VALUES", b"3", b"12"]),
                limits(4, 1, 16),
            ),
            Ok(())
        );
        assert_eq!(
            admit_vector_request(
                &argv(&[b"VADD", b"key", b"VALUES", b"1", b"1"]),
                limits(4, 1, 8),
            ),
            Ok(())
        );
    }
}
