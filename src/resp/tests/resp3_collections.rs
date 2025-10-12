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
fn set_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Set(vec![
        RespData::Boolean(true),
        RespData::Null,
        RespData::Double(2.5),
    ]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::Set(items) => assert_eq!(items.len(), 3),
        _ => panic!(),
    }
}

#[test]
fn map_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Map(vec![
        (RespData::Boolean(true), RespData::Double(1.0)),
        (RespData::Null, RespData::BulkError(Bytes::from("ERR x"))),
    ]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::Map(entries) => assert_eq!(entries.len(), 2),
        _ => panic!(),
    }
}

#[test]
fn push_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Push(vec![RespData::Boolean(false), RespData::Double(3.14)]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::Push(items) => assert_eq!(items.len(), 2),
        _ => panic!(),
    }
}
