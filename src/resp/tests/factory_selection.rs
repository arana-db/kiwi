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
use resp::{RespVersion, new_decoder, new_encoder};

#[test]
fn selects_resp1_impl() {
    let mut dec = new_decoder(RespVersion::RESP1);
    let enc = new_encoder(RespVersion::RESP1);
    assert_eq!(dec.version(), RespVersion::RESP1);
    assert_eq!(enc.version(), RespVersion::RESP1);

    // minimal smoke: inline ping
    dec.push(Bytes::from("PING\r\n"));
    // even if command extraction differs, API shape should not panic
    let _ = dec.next();
}

#[test]
fn selects_resp2_impl() {
    let mut dec = new_decoder(RespVersion::RESP2);
    let enc = new_encoder(RespVersion::RESP2);
    assert_eq!(dec.version(), RespVersion::RESP2);
    assert_eq!(enc.version(), RespVersion::RESP2);

    // minimal smoke: +OK\r\n
    dec.push(Bytes::from("+OK\r\n"));
    let _ = dec.next();
}
