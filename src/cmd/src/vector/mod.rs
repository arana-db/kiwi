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

use client::Client;
use resp::RespData;
use storage::{CanonicalVector, VectorQuery, VectorSearchMode, VectorSearchOptions, error::Error};

const ERR_INVALID_VECTOR: &str = "ERR invalid vector specification";
const ERR_VECTOR_DIMENSION: &str = "ERR vector dimension mismatch";
const ERR_DEFAULT_Q8: &str =
    "ERR default Q8 quantization is not supported in Phase 1; specify NOQUANT";
const ERR_VADD_Q8: &str = "ERR VADD option Q8 is not supported yet";
const ERR_VADD_BIN: &str = "ERR VADD option BIN is not supported yet";
const ERR_VEMB_RAW: &str = "ERR VEMB option RAW is not supported yet";
const ERR_ELEMENT_NOT_FOUND: &str = "ERR element not found in set";
const ERR_KEY_NOT_FOUND: &str = "ERR key does not exist";
const ERR_STORAGE: &str = "ERR storage error";

type ParseResult<T> = std::result::Result<T, &'static str>;

#[derive(Debug)]
struct ParsedVAdd {
    vector: CanonicalVector,
    element: Vec<u8>,
}

#[derive(Debug)]
struct ParsedVSim {
    query: VectorQuery,
    options: VectorSearchOptions,
    with_scores: bool,
}

fn parse_positive_usize(raw: &[u8]) -> Option<usize> {
    let value = std::str::from_utf8(raw).ok()?.parse::<usize>().ok()?;
    (value > 0).then_some(value)
}

fn parse_vector_values(
    argv: &[Vec<u8>],
    dimension_index: usize,
) -> ParseResult<(CanonicalVector, usize)> {
    let dimension = argv
        .get(dimension_index)
        .and_then(|raw| parse_positive_usize(raw))
        .ok_or(ERR_INVALID_VECTOR)?;
    let values_start = dimension_index + 1;
    let values_end = values_start
        .checked_add(dimension)
        .ok_or(ERR_INVALID_VECTOR)?;
    let raw_values = argv
        .get(values_start..values_end)
        .ok_or(ERR_INVALID_VECTOR)?;
    let values = raw_values
        .iter()
        .map(|raw| {
            std::str::from_utf8(raw)
                .ok()
                .and_then(|value| value.parse::<f32>().ok())
                .filter(|value| value.is_finite())
                .ok_or(ERR_INVALID_VECTOR)
        })
        .collect::<ParseResult<Vec<_>>>()?;
    let vector = CanonicalVector::from_values(&values).map_err(|_| ERR_INVALID_VECTOR)?;
    Ok((vector, values_end))
}

fn parse_direct_vector(
    argv: &[Vec<u8>],
    kind_index: usize,
) -> ParseResult<(CanonicalVector, usize)> {
    let kind = argv.get(kind_index).ok_or(ERR_INVALID_VECTOR)?;
    if kind.eq_ignore_ascii_case(b"FP32") {
        let raw = argv.get(kind_index + 1).ok_or(ERR_INVALID_VECTOR)?;
        let vector = CanonicalVector::from_fp32_le(raw).map_err(|_| ERR_INVALID_VECTOR)?;
        Ok((vector, kind_index + 2))
    } else if kind.eq_ignore_ascii_case(b"VALUES") {
        parse_vector_values(argv, kind_index + 1)
    } else {
        Err(ERR_INVALID_VECTOR)
    }
}

fn parse_vadd(argv: &[Vec<u8>]) -> ParseResult<ParsedVAdd> {
    let (vector, element_index) = parse_direct_vector(argv, 2)?;
    let element = argv.get(element_index).cloned().ok_or(ERR_INVALID_VECTOR)?;
    let quantization = &argv[element_index + 1..];
    match quantization {
        [] => Err(ERR_DEFAULT_Q8),
        [option] if option.eq_ignore_ascii_case(b"NOQUANT") => Ok(ParsedVAdd { vector, element }),
        [option] if option.eq_ignore_ascii_case(b"Q8") => Err(ERR_VADD_Q8),
        [option] if option.eq_ignore_ascii_case(b"BIN") => Err(ERR_VADD_BIN),
        _ => Err(ERR_INVALID_VECTOR),
    }
}

fn parse_vsim(argv: &[Vec<u8>]) -> ParseResult<ParsedVSim> {
    let query_kind = argv.get(2).ok_or(ERR_INVALID_VECTOR)?;
    let (query, mut option_index) = if query_kind.eq_ignore_ascii_case(b"ELE") {
        let element = argv.get(3).cloned().ok_or(ERR_INVALID_VECTOR)?;
        (VectorQuery::Element(element), 4)
    } else {
        let (vector, next) = parse_direct_vector(argv, 2)?;
        (VectorQuery::Vector(vector), next)
    };

    let mut count = 10;
    let mut mode = VectorSearchMode::Approximate;
    let mut with_scores = false;
    let mut count_seen = false;
    let mut truth_seen = false;

    while option_index < argv.len() {
        let option = &argv[option_index];
        if option.eq_ignore_ascii_case(b"WITHSCORES") {
            if with_scores {
                return Err(ERR_INVALID_VECTOR);
            }
            with_scores = true;
            option_index += 1;
        } else if option.eq_ignore_ascii_case(b"COUNT") {
            if count_seen {
                return Err(ERR_INVALID_VECTOR);
            }
            count = argv
                .get(option_index + 1)
                .and_then(|raw| parse_positive_usize(raw))
                .ok_or(ERR_INVALID_VECTOR)?;
            count_seen = true;
            option_index += 2;
        } else if option.eq_ignore_ascii_case(b"TRUTH") {
            if truth_seen {
                return Err(ERR_INVALID_VECTOR);
            }
            mode = VectorSearchMode::Truth;
            truth_seen = true;
            option_index += 1;
        } else {
            return Err(ERR_INVALID_VECTOR);
        }
    }

    Ok(ParsedVSim {
        query,
        options: VectorSearchOptions { count, mode },
        with_scores,
    })
}

fn parse_vemb(argv: &[Vec<u8>]) -> ParseResult<Vec<u8>> {
    let element = argv.get(2).cloned().ok_or(ERR_INVALID_VECTOR)?;
    match &argv[3..] {
        [] => Ok(element),
        [option] if option.eq_ignore_ascii_case(b"RAW") => Err(ERR_VEMB_RAW),
        _ => Err(ERR_INVALID_VECTOR),
    }
}

pub(crate) fn error_reply(message: impl Into<String>) -> RespData {
    RespData::Error(message.into().into())
}

#[derive(Clone, Copy)]
pub(crate) enum MissingError {
    Key,
    Element,
}

pub(crate) fn storage_error_reply(error: Error, missing: MissingError) -> RespData {
    match &error {
        Error::RedisErr { message, .. } => error_reply(message.clone()),
        Error::InvalidArgument { message, .. } if message.contains("dimension mismatch") => {
            error_reply(ERR_VECTOR_DIMENSION)
        }
        Error::KeyNotFound { .. } => match missing {
            MissingError::Key => error_reply(ERR_KEY_NOT_FOUND),
            MissingError::Element => error_reply(ERR_ELEMENT_NOT_FOUND),
        },
        _ => {
            log::error!("vector storage command failed: {error}");
            error_reply(ERR_STORAGE)
        }
    }
}

pub(crate) fn set_command_key(client: &Client) -> bool {
    let argv = client.argv();
    let Some(key) = argv.get(1) else {
        client.set_reply(error_reply(ERR_INVALID_VECTOR));
        return false;
    };
    client.set_key(key);
    true
}

pub(crate) fn integer_reply(value: u64) -> RespData {
    match i64::try_from(value) {
        Ok(value) => RespData::Integer(value),
        Err(error) => {
            log::error!("vector integer reply overflow: {error}");
            error_reply(ERR_STORAGE)
        }
    }
}

#[macro_export]
macro_rules! define_vector_command {
    ($type:ident, $name:literal, $arity:expr, $flags:expr, $acl:expr) => {
        #[derive(Clone, Default)]
        pub struct $type {
            meta: CmdMeta,
        }

        impl $type {
            pub fn new() -> Self {
                Self {
                    meta: CmdMeta {
                        name: $name.to_string(),
                        arity: $arity,
                        flags: $flags,
                        acl_category: $acl,
                        ..Default::default()
                    },
                }
            }
        }
    };
}

pub mod vadd;
pub mod vcard;
pub mod vdim;
pub mod vemb;
pub mod vismember;
pub mod vrem;
pub mod vsim;

pub use vadd::VAddCmd;
pub use vcard::VCardCmd;
pub use vdim::VDimCmd;
pub use vemb::VEmbCmd;
pub use vismember::VIsMemberCmd;
pub use vrem::VRemCmd;
pub use vsim::VSimCmd;

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use storage::{VectorQuery, VectorSearchMode};

    use super::*;
    use crate::Cmd;

    fn fp32(values: &[f32]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    #[test]
    fn parses_supported_vadd_shapes() {
        let blob = fp32(&[3.0, 4.0]);
        let parsed = parse_vadd(&[
            b"vadd".to_vec(),
            b"key\0raw".to_vec(),
            b"FP32".to_vec(),
            blob,
            b"\0element".to_vec(),
            b"NOQUANT".to_vec(),
        ])
        .expect("FP32 VADD");
        assert_eq!(parsed.vector.dimension(), 2);
        assert_eq!(parsed.element, b"\0element");

        let parsed = parse_vadd(&[
            b"vadd".to_vec(),
            b"key".to_vec(),
            b"VALUES".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec(),
            b"element".to_vec(),
            b"noquant".to_vec(),
        ])
        .expect("VALUES VADD");
        assert_eq!(parsed.vector.dimension(), 2);
        assert_eq!(parsed.element, b"element");
    }

    #[test]
    fn rejects_unsupported_or_invalid_vadd_shapes() {
        let malformed_fp32 = vec![
            b"vadd".to_vec(),
            b"key".to_vec(),
            b"FP32".to_vec(),
            vec![1, 2, 3],
            b"element".to_vec(),
            b"NOQUANT".to_vec(),
        ];
        assert_eq!(parse_vadd(&malformed_fp32).unwrap_err(), ERR_INVALID_VECTOR);

        let invalid_values = [
            vec![
                b"vadd".to_vec(),
                b"key".to_vec(),
                b"VALUES".to_vec(),
                b"0".to_vec(),
                b"element".to_vec(),
                b"NOQUANT".to_vec(),
            ],
            vec![
                b"vadd".to_vec(),
                b"key".to_vec(),
                b"VALUES".to_vec(),
                b"2".to_vec(),
                b"1".to_vec(),
                b"element".to_vec(),
                b"NOQUANT".to_vec(),
            ],
            vec![
                b"vadd".to_vec(),
                b"key".to_vec(),
                b"VALUES".to_vec(),
                b"1".to_vec(),
                b"not-a-float".to_vec(),
                b"element".to_vec(),
                b"NOQUANT".to_vec(),
            ],
        ];
        for argv in invalid_values {
            assert_eq!(parse_vadd(&argv).unwrap_err(), ERR_INVALID_VECTOR);
        }

        let base = vec![
            b"vadd".to_vec(),
            b"key".to_vec(),
            b"FP32".to_vec(),
            fp32(&[1.0]),
            b"element".to_vec(),
        ];
        assert_eq!(parse_vadd(&base).unwrap_err(), ERR_DEFAULT_Q8);

        let mut q8 = base.clone();
        q8.push(b"Q8".to_vec());
        assert_eq!(parse_vadd(&q8).unwrap_err(), ERR_VADD_Q8);

        let mut bin = base.clone();
        bin.push(b"BIN".to_vec());
        assert_eq!(parse_vadd(&bin).unwrap_err(), ERR_VADD_BIN);

        let mut trailing = base;
        trailing.extend([b"NOQUANT".to_vec(), b"extra".to_vec()]);
        assert_eq!(parse_vadd(&trailing).unwrap_err(), ERR_INVALID_VECTOR);
    }

    #[test]
    fn parses_supported_vsim_shapes_and_options() {
        let ele = parse_vsim(&[
            b"vsim".to_vec(),
            b"key".to_vec(),
            b"ELE".to_vec(),
            b"\0element".to_vec(),
            b"WITHSCORES".to_vec(),
            b"COUNT".to_vec(),
            b"3".to_vec(),
            b"TRUTH".to_vec(),
        ])
        .expect("ELE VSIM");
        assert_eq!(ele.query, VectorQuery::Element(b"\0element".to_vec()));
        assert_eq!(ele.options.count, 3);
        assert_eq!(ele.options.mode, VectorSearchMode::Truth);
        assert!(ele.with_scores);

        let direct = parse_vsim(&[
            b"vsim".to_vec(),
            b"key".to_vec(),
            b"FP32".to_vec(),
            fp32(&[1.0, 0.0]),
        ])
        .expect("FP32 VSIM");
        assert!(matches!(direct.query, VectorQuery::Vector(_)));
        assert_eq!(direct.options.count, 10);
        assert_eq!(direct.options.mode, VectorSearchMode::Approximate);

        let values = parse_vsim(&[
            b"vsim".to_vec(),
            b"key".to_vec(),
            b"VALUES".to_vec(),
            b"2".to_vec(),
            b"1".to_vec(),
            b"0".to_vec(),
            b"COUNT".to_vec(),
            b"1".to_vec(),
        ])
        .expect("VALUES VSIM");
        assert!(matches!(values.query, VectorQuery::Vector(_)));
        assert_eq!(values.options.count, 1);
    }

    #[test]
    fn rejects_invalid_vsim_options_and_vectors() {
        let cases = [
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"FP32".to_vec(),
                vec![1, 2, 3],
            ],
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"VALUES".to_vec(),
                b"2".to_vec(),
                b"1".to_vec(),
            ],
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"VALUES".to_vec(),
                b"1".to_vec(),
                b"NaN".to_vec(),
            ],
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"ELE".to_vec(),
                b"member".to_vec(),
                b"COUNT".to_vec(),
                b"0".to_vec(),
            ],
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"ELE".to_vec(),
                b"member".to_vec(),
                b"COUNT".to_vec(),
                b"invalid".to_vec(),
            ],
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"ELE".to_vec(),
                b"member".to_vec(),
                b"COUNT".to_vec(),
                b"1".to_vec(),
                b"COUNT".to_vec(),
                b"2".to_vec(),
            ],
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"ELE".to_vec(),
                b"member".to_vec(),
                b"WITHSCORES".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"ELE".to_vec(),
                b"member".to_vec(),
                b"TRUTH".to_vec(),
                b"TRUTH".to_vec(),
            ],
            vec![
                b"vsim".to_vec(),
                b"key".to_vec(),
                b"ELE".to_vec(),
                b"member".to_vec(),
                b"unknown".to_vec(),
            ],
        ];
        for argv in cases {
            assert_eq!(parse_vsim(&argv).unwrap_err(), ERR_INVALID_VECTOR);
        }
    }

    #[test]
    fn rejects_vemb_raw_and_unknown_trailing_options() {
        assert_eq!(
            parse_vemb(&[
                b"vemb".to_vec(),
                b"key".to_vec(),
                b"member".to_vec(),
                b"RAW".to_vec(),
            ])
            .unwrap_err(),
            ERR_VEMB_RAW
        );
        assert_eq!(
            parse_vemb(&[
                b"vemb".to_vec(),
                b"key".to_vec(),
                b"member".to_vec(),
                b"unknown".to_vec(),
            ])
            .unwrap_err(),
            ERR_INVALID_VECTOR
        );
    }

    #[test]
    fn vector_command_metadata_matches_redis_shapes() {
        assert_eq!(VAddCmd::new().meta().arity, -5);
        assert_eq!(VSimCmd::new().meta().arity, -4);
        assert_eq!(VRemCmd::new().meta().arity, 3);
        assert_eq!(VCardCmd::new().meta().arity, 2);
        assert_eq!(VDimCmd::new().meta().arity, 2);
        assert_eq!(VEmbCmd::new().meta().arity, -3);
        assert_eq!(VIsMemberCmd::new().meta().arity, 3);
    }
}
