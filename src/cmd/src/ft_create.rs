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

use std::sync::Arc;

use client::Client;
use resp::RespData;
use storage::search_types::{
    DistanceMetric, SearchDataType, SearchFieldSchema, SearchIndexSchema, VectorAlgorithm,
    VectorFieldSchema, VectorValueType,
};
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct FtCreateCmd {
    meta: CmdMeta,
}

impl FtCreateCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "ft.create".to_string(),
                arity: -2,
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for FtCreateCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        match parse_ft_create(client.argv()) {
            Ok(schema) => match storage.ft_create(schema) {
                Ok(()) => client.set_reply(RespData::SimpleString("OK".into())),
                Err(error) => client.set_reply(RespData::Error(format!("ERR {error}").into())),
            },
            Err(error) => client.set_reply(RespData::Error(format!("ERR {error}").into())),
        }
    }
}

fn parse_ft_create(argv: Vec<Vec<u8>>) -> Result<SearchIndexSchema, String> {
    if argv.len() < 9 {
        return Err("wrong number of arguments for 'ft.create' command".to_string());
    }

    let index = argv[1].clone();
    let mut pos = 2;

    expect_keyword(&argv, pos, "ON")?;
    pos += 1;
    expect_keyword(&argv, pos, "HASH")?;
    pos += 1;

    expect_keyword(&argv, pos, "PREFIX")?;
    pos += 1;
    let prefix_count = parse_usize(&argv, pos, "prefix count")?;
    pos += 1;
    if prefix_count == 0 {
        return Err("PREFIX count must be greater than zero".to_string());
    }
    if argv.len() < pos + prefix_count + 1 {
        return Err("not enough PREFIX values".to_string());
    }
    let prefixes = argv[pos..pos + prefix_count].to_vec();
    pos += prefix_count;

    expect_keyword(&argv, pos, "SCHEMA")?;
    pos += 1;

    let field_name = argv
        .get(pos)
        .ok_or_else(|| "missing schema field name".to_string())?
        .clone();
    pos += 1;
    expect_keyword(&argv, pos, "VECTOR")?;
    pos += 1;
    expect_keyword(&argv, pos, "FLAT")?;
    pos += 1;
    let attr_count = parse_usize(&argv, pos, "VECTOR FLAT attribute count")?;
    pos += 1;
    if argv.len() != pos + attr_count {
        return Err("VECTOR FLAT attribute count does not match arguments".to_string());
    }

    let mut value_type = None;
    let mut dim = None;
    let mut distance_metric = None;
    let end = pos + attr_count;
    while pos < end {
        if keyword_eq(&argv[pos], "TYPE") {
            pos += 1;
            expect_keyword(&argv, pos, "FLOAT32")?;
            value_type = Some(VectorValueType::Float32);
            pos += 1;
        } else if keyword_eq(&argv[pos], "DIM") {
            pos += 1;
            let parsed_dim = parse_u16(&argv, pos, "DIM")?;
            if parsed_dim == 0 {
                return Err("DIM must be greater than zero".to_string());
            }
            dim = Some(parsed_dim);
            pos += 1;
        } else if keyword_eq(&argv[pos], "DISTANCE_METRIC") {
            pos += 1;
            distance_metric = Some(parse_distance_metric(&argv, pos)?);
            pos += 1;
        } else {
            return Err(format!(
                "unsupported VECTOR FLAT option '{}'",
                String::from_utf8_lossy(&argv[pos])
            ));
        }
    }

    Ok(SearchIndexSchema {
        name: index,
        on: SearchDataType::Hash,
        prefixes,
        fields: vec![(
            field_name,
            SearchFieldSchema::Vector(VectorFieldSchema {
                algorithm: VectorAlgorithm::Flat,
                value_type: value_type.ok_or_else(|| "missing TYPE".to_string())?,
                dim: dim.ok_or_else(|| "missing DIM".to_string())?,
                distance_metric: distance_metric
                    .ok_or_else(|| "missing DISTANCE_METRIC".to_string())?,
            }),
        )],
    })
}

fn expect_keyword(argv: &[Vec<u8>], pos: usize, expected: &str) -> Result<(), String> {
    let Some(value) = argv.get(pos) else {
        return Err(format!("missing {expected}"));
    };
    if keyword_eq(value, expected) {
        Ok(())
    } else {
        Err(format!(
            "expected {expected}, got '{}'",
            String::from_utf8_lossy(value)
        ))
    }
}

fn keyword_eq(value: &[u8], expected: &str) -> bool {
    value.eq_ignore_ascii_case(expected.as_bytes())
}

fn parse_usize(argv: &[Vec<u8>], pos: usize, label: &str) -> Result<usize, String> {
    let value = argv.get(pos).ok_or_else(|| format!("missing {label}"))?;
    std::str::from_utf8(value)
        .map_err(|_| format!("{label} is not valid utf-8"))?
        .parse::<usize>()
        .map_err(|_| format!("{label} is not an integer"))
}

fn parse_u16(argv: &[Vec<u8>], pos: usize, label: &str) -> Result<u16, String> {
    let value = parse_usize(argv, pos, label)?;
    u16::try_from(value).map_err(|_| format!("{label} exceeds u16"))
}

fn parse_distance_metric(argv: &[Vec<u8>], pos: usize) -> Result<DistanceMetric, String> {
    let value = argv
        .get(pos)
        .ok_or_else(|| "missing DISTANCE_METRIC value".to_string())?;
    if keyword_eq(value, "L2") {
        Ok(DistanceMetric::L2)
    } else if keyword_eq(value, "IP") {
        Ok(DistanceMetric::IP)
    } else if keyword_eq(value, "COSINE") {
        Ok(DistanceMetric::Cosine)
    } else {
        Err(format!(
            "unsupported DISTANCE_METRIC '{}'",
            String::from_utf8_lossy(value)
        ))
    }
}
