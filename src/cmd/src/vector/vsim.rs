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

use bytes::Bytes;
use client::Client;
use resp::RespData;
use storage::{VectorQuery, VectorSearchMode, VectorSearchOptions, storage::Storage};

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

use super::{
    ERR_INVALID_VECTOR, MissingError, ParseResult, error_reply, parse_direct_vector,
    parse_positive_usize, storage_error_reply,
};

crate::define_vector_command!(
    VSimCmd,
    "vsim",
    -4, // VSIM key (ELE element | FP32 vector | VALUES num vector) [WITHSCORES] [COUNT count] [TRUTH]
    CmdFlags::READONLY,
    AclCategory::KEYSPACE | AclCategory::READ | AclCategory::SLOW
);

#[derive(Debug)]
struct ParsedVSim {
    query: VectorQuery,
    options: VectorSearchOptions,
    with_scores: bool,
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

impl Cmd for VSimCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        super::set_command_key(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let parsed = match parse_vsim(&client.argv()) {
            Ok(parsed) => parsed,
            Err(message) => {
                client.set_reply(error_reply(message));
                return;
            }
        };
        let reply = match storage.vsim(&client.key(), parsed.query, parsed.options) {
            Ok(hits) if parsed.with_scores => RespData::Map(
                hits.into_iter()
                    .map(|hit| {
                        (
                            RespData::BulkString(Some(Bytes::from(hit.element))),
                            RespData::Double(hit.score),
                        )
                    })
                    .collect(),
            ),
            Ok(hits) => RespData::Array(Some(
                hits.into_iter()
                    .map(|hit| RespData::BulkString(Some(Bytes::from(hit.element))))
                    .collect(),
            )),
            Err(error) => storage_error_reply(error, MissingError::Element),
        };
        client.set_reply(reply);
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn fp32(values: &[f32]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
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
}
