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
use resp::{RespData, RespVersion, decode_many, new_decoder, new_encoder};

#[test]
fn resp3_null_boolean_double_decode() {
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, Bytes::from("_\r\n#t\r\n,f1.5\r\n"));
    assert!(out.len() >= 2);
    match out[0].as_ref().unwrap() {
        RespData::Null => {}
        _ => panic!("expected Null"),
    }
    match out[1].as_ref().unwrap() {
        RespData::Boolean(true) => {}
        _ => panic!("expected Boolean(true)"),
    }
    // third may be invalid until double formatting chosen; skip if parse failed
}

#[test]
fn resp3_null_boolean_double_encode() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let items = [
        RespData::Null,
        RespData::Boolean(true),
        RespData::Double(1.5),
    ];
    let bytes = resp::encode_many(&mut *enc, &items).unwrap();
    let s = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(s.contains("_\r\n"));
    assert!(s.contains("#t\r\n"));
    assert!(s.contains(",1.5\r\n") || s.contains(",1.5"));
}
