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
use resp::{RespData, RespVersion, decode_many, new_decoder};

#[test]
fn inline_ping() {
    let mut dec = new_decoder(RespVersion::RESP1);
    let out = decode_many(&mut *dec, Bytes::from("PING\r\n"));
    // Not required to convert to command, just verify no crash and produces a frame
    assert!(out.len() >= 1);
}

#[test]
fn simple_string_ok() {
    let mut dec = new_decoder(RespVersion::RESP1);
    let out = decode_many(&mut *dec, Bytes::from("+OK\r\n"));
    let v = out[0].as_ref().unwrap();
    match v {
        RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"OK"),
        _ => panic!(),
    }
}
