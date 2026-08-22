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
use storage::{CanonicalVector, error::Error};

const ERR_INVALID_VECTOR: &str = "ERR invalid vector specification";
const ERR_VECTOR_DIMENSION: &str = "ERR vector dimension mismatch";
const ERR_ELEMENT_NOT_FOUND: &str = "ERR element not found in set";
const ERR_KEY_NOT_FOUND: &str = "ERR key does not exist";
const ERR_STORAGE: &str = "ERR storage error";

type ParseResult<T> = std::result::Result<T, &'static str>;

fn parse_positive_usize(raw: &[u8]) -> Option<usize> {
    let value = std::str::from_utf8(raw).ok()?.parse::<usize>().ok()?;
    (value > 0).then_some(value)
}

const ERR_VECTOR_DIMENSION_LIMIT: &str = "ERR vector dimension exceeds max_dimension";
const ERR_VECTOR_ELEMENT_LIMIT: &str = "ERR vector element exceeds max_element_bytes";
const ERR_VECTOR_BYTES_LIMIT: &str = "ERR vector exceeds max_vector_bytes";

#[derive(Clone, Copy)]
struct VectorParseLimits {
    max_dimension: usize,
    max_element_bytes: usize,
    max_vector_bytes: usize,
}

impl Default for VectorParseLimits {
    fn default() -> Self {
        Self::from(&conf::vector_config::VectorConfig::default())
    }
}

impl From<&conf::vector_config::VectorConfig> for VectorParseLimits {
    fn from(config: &conf::vector_config::VectorConfig) -> Self {
        Self {
            max_dimension: config.max_dimension as usize,
            max_element_bytes: config.max_element_bytes,
            max_vector_bytes: config.max_vector_bytes,
        }
    }
}

fn parse_vector_values(
    argv: &[Vec<u8>],
    dimension_index: usize,
    limits: VectorParseLimits,
) -> ParseResult<(CanonicalVector, usize)> {
    let dimension = argv
        .get(dimension_index)
        .and_then(|raw| parse_positive_usize(raw))
        .ok_or(ERR_INVALID_VECTOR)?;
    if dimension > limits.max_dimension {
        return Err(ERR_VECTOR_DIMENSION_LIMIT);
    }
    let vector_bytes = dimension
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or(ERR_VECTOR_BYTES_LIMIT)?;
    if vector_bytes > limits.max_vector_bytes {
        return Err(ERR_VECTOR_BYTES_LIMIT);
    }
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
    limits: VectorParseLimits,
) -> ParseResult<(CanonicalVector, usize)> {
    let kind = argv.get(kind_index).ok_or(ERR_INVALID_VECTOR)?;
    if kind.eq_ignore_ascii_case(b"FP32") {
        let raw = argv.get(kind_index + 1).ok_or(ERR_INVALID_VECTOR)?;
        if raw.len() > limits.max_vector_bytes {
            return Err(ERR_VECTOR_BYTES_LIMIT);
        }
        if raw.len() % std::mem::size_of::<f32>() != 0 {
            return Err(ERR_INVALID_VECTOR);
        }
        if raw.len() / std::mem::size_of::<f32>() > limits.max_dimension {
            return Err(ERR_VECTOR_DIMENSION_LIMIT);
        }
        let vector = CanonicalVector::from_fp32_le(raw).map_err(|_| ERR_INVALID_VECTOR)?;
        Ok((vector, kind_index + 2))
    } else if kind.eq_ignore_ascii_case(b"VALUES") {
        parse_vector_values(argv, kind_index + 1, limits)
    } else {
        Err(ERR_INVALID_VECTOR)
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
        // FLAT governance failures carry a client-ready "ERR ..." display.
        Error::VectorFlatQueryTimeout { .. }
        | Error::VectorFlatQueryCancelled { .. }
        | Error::VectorFlatScanBudgetExceeded { .. } => error_reply(error.to_string()),
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

pub mod admission;
pub mod vadd;
pub mod vcard;
pub mod vdim;
pub mod vemb;
pub mod vinfo;
pub mod vismember;
pub mod vrem;
pub mod vsim;

pub use vadd::VAddCmd;
pub use vcard::VCardCmd;
pub use vdim::VDimCmd;
pub use vemb::VEmbCmd;
pub use vinfo::VInfoCmd;
pub use vismember::VIsMemberCmd;
pub use vrem::VRemCmd;
pub use vsim::VSimCmd;

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cmd;

    #[test]
    fn vector_command_metadata_matches_redis_shapes() {
        assert_eq!(VAddCmd::new().meta().arity, -5);
        assert_eq!(VSimCmd::new().meta().arity, -4);
        assert_eq!(VRemCmd::new().meta().arity, 3);
        assert_eq!(VCardCmd::new().meta().arity, 2);
        assert_eq!(VDimCmd::new().meta().arity, 2);
        assert_eq!(VEmbCmd::new().meta().arity, -3);
        assert_eq!(VInfoCmd::new().meta().arity, 2);
        assert_eq!(VIsMemberCmd::new().meta().arity, 3);
    }
}
