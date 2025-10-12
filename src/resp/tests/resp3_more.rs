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

use bytes::Bytes;
use resp::{RespData, RespVersion, decode_many, encode_many, new_decoder, new_encoder};

#[test]
fn bulk_error_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::BulkError(Bytes::from("ERR something"));
    let bytes = encode_many(&mut *enc, &[data.clone()]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::BulkError(s) => assert_eq!(s.as_ref(), b"ERR something"),
        other => panic!("Expected BulkError, got {:?}", other),
    }
}

#[test]
fn verbatim_string_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::VerbatimString {
        format: *b"txt",
        data: Bytes::from("hello"),
    };
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::VerbatimString { format, data } => {
            assert_eq!(format, b"txt");
            assert_eq!(data.as_ref(), b"hello");
        }
        other => panic!("Expected VerbatimString, got {:?}", other),
    }
}

#[test]
fn bignumber_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::BigNumber("12345678901234567890".into());
    let bytes = encode_many(&mut *enc, &[data.clone()]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::BigNumber(s) => assert_eq!(s, "12345678901234567890"),
        other => panic!("Expected BigNumber, got {:?}", other),
    }
}
