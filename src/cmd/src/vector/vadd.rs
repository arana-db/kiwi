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
use storage::{CanonicalVector, QuantizationType, storage::Storage};

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

use super::admission::{VectorAdmissionLimits, admit_vector_request};
use super::{
    ERR_INVALID_VECTOR, ERR_VECTOR_ELEMENT_LIMIT, MissingError, ParseResult, VectorParseLimits,
    error_reply, parse_direct_vector, parse_positive_usize, storage_error_reply,
};

const ERR_VADD_REDUCE: &str = "ERR VADD option REDUCE is not supported yet";
const ERR_VADD_DEFAULT_QUANTIZATION: &str =
    "ERR default Q8 quantization is not supported in Phase 1; specify NOQUANT";
const ERR_VADD_Q8: &str = "ERR VADD option Q8 is not supported yet";
const ERR_VADD_BIN: &str = "ERR VADD option BIN is not supported yet";
const ERR_VADD_CAS: &str = "ERR VADD option CAS is not supported yet";
const ERR_VADD_EF: &str = "ERR VADD option EF is not supported yet";
const ERR_VADD_SETATTR: &str = "ERR VADD option SETATTR is not supported yet";
const ERR_VADD_M: &str = "ERR VADD option M is not supported yet";

crate::define_vector_command!(
    VAddCmd,
    "vadd",
    -5, // VADD key (FP32 | VALUES num) vector element [NOQUANT | Q8 | BIN]
    CmdFlags::WRITE | CmdFlags::FAST,
    AclCategory::KEYSPACE | AclCategory::WRITE
);

#[derive(Debug)]
struct ParsedVAdd {
    vector: CanonicalVector,
    element: Vec<u8>,
}

fn parse_vadd_with_limits(argv: &[Vec<u8>], limits: VectorParseLimits) -> ParseResult<ParsedVAdd> {
    // REDUCE must precede the vector spec; reject it explicitly until
    // random-projection support lands.
    if argv
        .get(2)
        .is_some_and(|arg| arg.eq_ignore_ascii_case(b"REDUCE"))
    {
        return Err(ERR_VADD_REDUCE);
    }
    let (vector, element_index) = parse_direct_vector(argv, 2, limits)?;
    let element = argv.get(element_index).ok_or(ERR_INVALID_VECTOR)?;
    if element.len() > limits.max_element_bytes {
        return Err(ERR_VECTOR_ELEMENT_LIMIT);
    }
    let element = element.clone();

    let mut quantization = None;
    for option in &argv[element_index + 1..] {
        if option.eq_ignore_ascii_case(b"NOQUANT") {
            quantization = Some(QuantizationType::None);
        } else if option.eq_ignore_ascii_case(b"Q8") {
            return Err(ERR_VADD_Q8);
        } else if option.eq_ignore_ascii_case(b"BIN") {
            return Err(ERR_VADD_BIN);
        } else if option.eq_ignore_ascii_case(b"CAS") {
            return Err(ERR_VADD_CAS);
        } else if option.eq_ignore_ascii_case(b"EF") {
            return Err(ERR_VADD_EF);
        } else if option.eq_ignore_ascii_case(b"SETATTR") {
            return Err(ERR_VADD_SETATTR);
        } else if option.eq_ignore_ascii_case(b"M") {
            return Err(ERR_VADD_M);
        } else {
            return Err(ERR_INVALID_VECTOR);
        }
    }

    // Fold the requested quantization into the vector here; from this point
    // on the vector's own quantization is the single source of truth.
    let vector = vector
        .to_quantized(quantization.ok_or(ERR_VADD_DEFAULT_QUANTIZATION)?)
        .map_err(|_| ERR_INVALID_VECTOR)?;
    Ok(ParsedVAdd { vector, element })
}

#[cfg(test)]
fn parse_vadd(argv: &[Vec<u8>]) -> ParseResult<ParsedVAdd> {
    parse_vadd_with_limits(argv, VectorParseLimits::default())
}

impl Cmd for VAddCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn admit_network_request(
        &self,
        argv: &[Bytes],
        limits: VectorAdmissionLimits,
    ) -> Result<(), RespData> {
        admit_vector_request(argv, limits).map_err(|error| error_reply(error.as_str()))
    }

    fn do_initial(&self, client: &Client) -> bool {
        super::set_command_key(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let limits = storage
            .storage_options()
            .map(|options| VectorParseLimits::from(&options.vector))
            .unwrap_or_default();
        let parsed = match parse_vadd_with_limits(&argv, limits) {
            Ok(parsed) => parsed,
            Err(ERR_INVALID_VECTOR)
                if argv.get(2).is_some_and(|kind| {
                    (kind.eq_ignore_ascii_case(b"FP32") && argv.len() == 4)
                        || (kind.eq_ignore_ascii_case(b"VALUES")
                            && argv
                                .get(3)
                                .and_then(|raw| parse_positive_usize(raw))
                                .is_some_and(|dimension| argv.len() == dimension + 4))
                }) =>
            {
                client.set_reply(error_reply(
                    "ERR wrong number of arguments for 'vadd' command",
                ));
                return;
            }
            Err(message) => {
                client.set_reply(error_reply(message));
                return;
            }
        };
        let reply = match storage.vadd(&client.key(), &parsed.element, &parsed.vector) {
            Ok(inserted) => RespData::Boolean(inserted),
            Err(error) => storage_error_reply(error, MissingError::Key),
        };
        client.set_reply(reply);
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use client::StreamTrait;
    use storage::{StorageOptions, safe_cleanup_test_db, unique_test_db_path};

    use super::*;

    /// A single `(configure, argv, expected_error)` case for the parser limits
    /// test. Factored into a type alias to keep the tuple short.
    type LimitCase = (
        Box<dyn FnOnce(&mut conf::vector_config::VectorConfig)>,
        Vec<Vec<u8>>,
        &'static str,
    );

    struct TestStream;

    #[async_trait::async_trait]
    impl StreamTrait for TestStream {
        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }

        async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }
    }

    fn open_storage_with_limits(
        configure: impl FnOnce(&mut conf::vector_config::VectorConfig),
    ) -> (std::path::PathBuf, Arc<Storage>) {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);
        let mut options = StorageOptions::default();
        configure(&mut options.vector);
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(options), &db_path)
            .expect("open storage");
        (db_path, Arc::new(storage))
    }

    fn run_vadd(client: &Client, storage: &Arc<Storage>, argv: &[Vec<u8>]) -> RespData {
        client.set_argv(argv);
        VAddCmd::new().execute(client, Arc::clone(storage));
        client.take_reply()
    }

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
        assert_eq!(parsed.vector.quantization(), QuantizationType::None);

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

        let repeated_noquant = parse_vadd(&[
            b"vadd".to_vec(),
            b"key".to_vec(),
            b"VALUES".to_vec(),
            b"1".to_vec(),
            b"1".to_vec(),
            b"element".to_vec(),
            b"NOQUANT".to_vec(),
            b"NOQUANT".to_vec(),
        ])
        .expect("repeated NOQUANT VADD");
        assert_eq!(
            repeated_noquant.vector.quantization(),
            QuantizationType::None
        );

        // Phase 1 requires explicit NOQUANT and does not support Q8/BIN yet.
        let base = vec![
            b"vadd".to_vec(),
            b"key".to_vec(),
            b"FP32".to_vec(),
            fp32(&[1.0]),
            b"element".to_vec(),
        ];
        assert_eq!(
            parse_vadd(&base).unwrap_err(),
            "ERR default Q8 quantization is not supported in Phase 1; specify NOQUANT"
        );
        for (option, expected) in [
            (b"Q8".to_vec(), "ERR VADD option Q8 is not supported yet"),
            (b"BIN".to_vec(), "ERR VADD option BIN is not supported yet"),
        ] {
            let mut argv = base.clone();
            argv.push(option);
            assert_eq!(parse_vadd(&argv).unwrap_err(), expected);
        }
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

        // Q8 is rejected before a following quantization option is considered.
        let mut duplicated = base.clone();
        duplicated.extend([b"Q8".to_vec(), b"BIN".to_vec()]);
        assert_eq!(parse_vadd(&duplicated).unwrap_err(), ERR_VADD_Q8);

        // Recognized but not yet supported options get dedicated errors.
        for (option, expected) in [
            (b"CAS".to_vec(), ERR_VADD_CAS),
            (b"EF".to_vec(), ERR_VADD_EF),
            (b"SETATTR".to_vec(), ERR_VADD_SETATTR),
            (b"M".to_vec(), ERR_VADD_M),
        ] {
            let mut argv = base.clone();
            argv.push(option);
            assert_eq!(parse_vadd(&argv).unwrap_err(), expected);
        }

        let mut reduce = vec![b"vadd".to_vec(), b"key".to_vec(), b"REDUCE".to_vec()];
        reduce.extend(base.iter().skip(2).cloned());
        assert_eq!(parse_vadd(&reduce).unwrap_err(), ERR_VADD_REDUCE);

        let mut trailing = base;
        trailing.extend([b"NOQUANT".to_vec(), b"extra".to_vec()]);
        assert_eq!(parse_vadd(&trailing).unwrap_err(), ERR_INVALID_VECTOR);
    }

    #[tokio::test]
    async fn parser_enforces_configured_vector_limits() {
        let client = Client::new(Box::new(TestStream));
        let cases: Vec<LimitCase> = vec![
            (
                Box::new(|vector| vector.max_dimension = 1),
                vec![
                    b"vadd".to_vec(),
                    b"key".to_vec(),
                    b"VALUES".to_vec(),
                    b"2".to_vec(),
                    b"not-a-float".to_vec(),
                    b"also-invalid".to_vec(),
                    b"element".to_vec(),
                    b"NOQUANT".to_vec(),
                ],
                "ERR vector dimension exceeds max_dimension",
            ),
            (
                Box::new(|vector| vector.max_vector_bytes = 4),
                vec![
                    b"vadd".to_vec(),
                    b"key".to_vec(),
                    b"FP32".to_vec(),
                    fp32(&[0.0, 0.0]),
                    b"element".to_vec(),
                    b"NOQUANT".to_vec(),
                ],
                "ERR vector exceeds max_vector_bytes",
            ),
            (
                Box::new(|vector| vector.max_dimension = 1),
                vec![
                    b"vadd".to_vec(),
                    b"key".to_vec(),
                    b"FP32".to_vec(),
                    fp32(&[0.0, 0.0]),
                    b"element".to_vec(),
                    b"NOQUANT".to_vec(),
                ],
                "ERR vector dimension exceeds max_dimension",
            ),
            (
                Box::new(|vector| vector.max_element_bytes = 3),
                vec![
                    b"vadd".to_vec(),
                    b"key".to_vec(),
                    b"VALUES".to_vec(),
                    b"1".to_vec(),
                    b"0".to_vec(),
                    b"abcd".to_vec(),
                    b"NOQUANT".to_vec(),
                ],
                "ERR vector element exceeds max_element_bytes",
            ),
        ];

        for (configure, argv, expected) in cases {
            let (db_path, storage) = open_storage_with_limits(configure);
            assert_eq!(
                run_vadd(&client, &storage, &argv),
                RespData::Error(expected.into())
            );
            drop(storage);
            safe_cleanup_test_db(&db_path);
        }
    }

    #[test]
    fn complete_vector_without_element_returns_wrong_arity() {
        let client = Client::new(Box::new(TestStream));
        client.set_argv(&[
            b"vadd".to_vec(),
            b"key".to_vec(),
            b"VALUES".to_vec(),
            b"1".to_vec(),
            b"1".to_vec(),
        ]);

        VAddCmd::new().execute(&client, Arc::new(Storage::new(1, 0)));

        assert_eq!(
            client.take_reply(),
            RespData::Error("ERR wrong number of arguments for 'vadd' command".into())
        );
    }

    #[test]
    fn incomplete_vector_keeps_typed_invalid_vector_error() {
        let client = Client::new(Box::new(TestStream));
        client.set_argv(&[
            b"vadd".to_vec(),
            b"key".to_vec(),
            b"VALUES".to_vec(),
            b"2".to_vec(),
            b"1".to_vec(),
        ]);

        VAddCmd::new().execute(&client, Arc::new(Storage::new(1, 0)));

        assert_eq!(client.take_reply(), error_reply(ERR_INVALID_VECTOR));
    }
}
