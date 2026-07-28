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
use storage::{CanonicalVector, QuantizationType, storage::Storage};

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

use super::{
    ERR_INVALID_VECTOR, MissingError, ParseResult, error_reply, parse_direct_vector,
    storage_error_reply,
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

fn parse_vadd(argv: &[Vec<u8>]) -> ParseResult<ParsedVAdd> {
    // REDUCE must precede the vector spec; reject it explicitly until
    // random-projection support lands.
    if argv
        .get(2)
        .is_some_and(|arg| arg.eq_ignore_ascii_case(b"REDUCE"))
    {
        return Err(ERR_VADD_REDUCE);
    }
    let (vector, element_index) = parse_direct_vector(argv, 2)?;
    let element = argv.get(element_index).cloned().ok_or(ERR_INVALID_VECTOR)?;

    let mut quantization = None;
    for option in &argv[element_index + 1..] {
        if option.eq_ignore_ascii_case(b"NOQUANT") {
            if quantization.is_some() {
                return Err(ERR_INVALID_VECTOR);
            } else {
                quantization = Some(QuantizationType::None);
            }
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

impl Cmd for VAddCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        super::set_command_key(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let parsed = match parse_vadd(&client.argv()) {
            Ok(parsed) => parsed,
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
    use super::*;

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
}
