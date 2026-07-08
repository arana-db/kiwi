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

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct FtSearchCmd {
    meta: CmdMeta,
}

impl FtSearchCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "ft.search".to_string(),
                arity: -3,
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for FtSearchCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        match parse_ft_search(client.argv()) {
            Ok(request) => match execute_search(&storage, request) {
                Ok(response) => client.set_reply(response),
                Err(error) => client.set_reply(RespData::Error(format!("ERR {error}").into())),
            },
            Err(error) => client.set_reply(RespData::Error(format!("ERR {error}").into())),
        }
    }
}

struct FtSearchRequest {
    index: Vec<u8>,
    field: Vec<u8>,
    query_vector: Vec<u8>,
    k: usize,
    return_fields: Option<Vec<Vec<u8>>>,
    limit_offset: usize,
    limit_count: usize,
    no_content: bool,
}

fn parse_ft_search(argv: Vec<Vec<u8>>) -> Result<FtSearchRequest, String> {
    if argv.len() < 3 {
        return Err("wrong number of arguments for 'ft.search' command".to_string());
    }

    let index = argv[1].clone();
    let knn = parse_knn_query(&argv[2])?;
    let mut params: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut return_fields = None;
    let mut limit_offset = 0usize;
    let mut limit_count = knn.k;
    let mut no_content = false;

    let mut pos = 3;
    while pos < argv.len() {
        if keyword_eq(&argv[pos], "PARAMS") {
            pos += 1;
            let arg_count = parse_usize(&argv, pos, "PARAMS count")?;
            pos += 1;
            if !arg_count.is_multiple_of(2) {
                return Err("PARAMS count must be even".to_string());
            }
            if argv.len() < pos + arg_count {
                return Err("not enough PARAMS arguments".to_string());
            }
            for pair_pos in (pos..pos + arg_count).step_by(2) {
                params.insert(argv[pair_pos].clone(), argv[pair_pos + 1].clone());
            }
            pos += arg_count;
        } else if keyword_eq(&argv[pos], "RETURN") {
            pos += 1;
            let field_count = parse_usize(&argv, pos, "RETURN count")?;
            pos += 1;
            if argv.len() < pos + field_count {
                return Err("not enough RETURN fields".to_string());
            }
            return_fields = Some(argv[pos..pos + field_count].to_vec());
            pos += field_count;
        } else if keyword_eq(&argv[pos], "LIMIT") {
            pos += 1;
            limit_offset = parse_usize(&argv, pos, "LIMIT offset")?;
            pos += 1;
            limit_count = parse_usize(&argv, pos, "LIMIT count")?;
            pos += 1;
        } else if keyword_eq(&argv[pos], "NOCONTENT") {
            no_content = true;
            pos += 1;
        } else if keyword_eq(&argv[pos], "DIALECT") {
            pos += 1;
            let dialect = parse_usize(&argv, pos, "DIALECT")?;
            if dialect != 2 {
                return Err("only DIALECT 2 is supported".to_string());
            }
            pos += 1;
        } else {
            return Err(format!(
                "unsupported FT.SEARCH option '{}'",
                String::from_utf8_lossy(&argv[pos])
            ));
        }
    }

    let query_vector = params
        .remove(knn.param.as_bytes())
        .ok_or_else(|| format!("missing PARAMS value for '{}'", knn.param))?;

    Ok(FtSearchRequest {
        index,
        field: knn.field.into_bytes(),
        query_vector,
        k: knn.k,
        return_fields,
        limit_offset,
        limit_count,
        no_content,
    })
}

struct KnnQuery {
    k: usize,
    field: String,
    param: String,
}

fn parse_knn_query(query: &[u8]) -> Result<KnnQuery, String> {
    let query = std::str::from_utf8(query).map_err(|_| "query is not valid utf-8".to_string())?;
    let trimmed = query.trim();
    let inner = trimmed
        .strip_prefix("*=>[")
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| "only '*=>[KNN k @field $param]' query is supported".to_string())?;
    let parts: Vec<&str> = inner.split_whitespace().collect();
    if parts.len() != 4 || !parts[0].eq_ignore_ascii_case("KNN") {
        return Err("only KNN query is supported".to_string());
    }
    let k = parts[1]
        .parse::<usize>()
        .map_err(|_| "KNN count is not an integer".to_string())?;
    if k == 0 {
        return Err("KNN count must be greater than zero".to_string());
    }
    let field = parts[2]
        .strip_prefix('@')
        .ok_or_else(|| "KNN field must start with @".to_string())?;
    let param = parts[3]
        .strip_prefix('$')
        .ok_or_else(|| "KNN parameter must start with $".to_string())?;
    if field.is_empty() || param.is_empty() {
        return Err("KNN field and parameter must be non-empty".to_string());
    }

    Ok(KnnQuery {
        k,
        field: field.to_string(),
        param: param.to_string(),
    })
}

fn execute_search(storage: &Storage, request: FtSearchRequest) -> storage::error::Result<RespData> {
    let hits = storage.ft_search_knn(
        &request.index,
        &request.field,
        &request.query_vector,
        request.k,
    )?;
    let total = hits.len();
    let limited_hits = hits
        .into_iter()
        .skip(request.limit_offset)
        .take(request.limit_count);

    let mut response = Vec::new();
    response.push(RespData::Integer(total as i64));

    for hit in limited_hits {
        response.push(RespData::BulkString(Some(Bytes::from(hit.doc_key.clone()))));
        if request.no_content {
            continue;
        }

        let fields = if let Some(return_fields) = &request.return_fields {
            load_return_fields(storage, &hit.doc_key, return_fields)?
        } else {
            load_all_fields(storage, &hit.doc_key)?
        };
        response.push(RespData::Array(Some(fields)));
    }

    Ok(RespData::Array(Some(response)))
}

fn load_return_fields(
    storage: &Storage,
    doc_key: &[u8],
    return_fields: &[Vec<u8>],
) -> storage::error::Result<Vec<RespData>> {
    let values = storage.hmget(doc_key, return_fields)?;
    let mut fields = Vec::with_capacity(return_fields.len() * 2);
    for (field, value) in return_fields.iter().zip(values) {
        fields.push(RespData::BulkString(Some(Bytes::from(field.clone()))));
        fields.push(match value {
            Some(value) => RespData::BulkString(Some(Bytes::from(value.into_bytes()))),
            None => RespData::BulkString(None),
        });
    }
    Ok(fields)
}

fn load_all_fields(storage: &Storage, doc_key: &[u8]) -> storage::error::Result<Vec<RespData>> {
    let values = storage.hgetall(doc_key)?;
    let mut fields = Vec::with_capacity(values.len() * 2);
    for (field, value) in values {
        fields.push(RespData::BulkString(Some(Bytes::from(field.into_bytes()))));
        fields.push(RespData::BulkString(Some(Bytes::from(value.into_bytes()))));
    }
    Ok(fields)
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
