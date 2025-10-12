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
use resp::{RespData, RespVersion, new_decoder};

#[test]
fn resp3_boolean_and_null() {
    let mut dec = new_decoder(RespVersion::RESP3);
    dec.push(Bytes::from("#t\r\n"));
    dec.push(Bytes::from("_\r\n"));

    // Verify Boolean(true) parsing
    let result1 = dec.next().unwrap().unwrap();
    match result1 {
        RespData::Boolean(true) => {}
        _ => panic!("expected Boolean(true), got {:?}", result1),
    }

    // Verify Null parsing
    let result2 = dec.next().unwrap().unwrap();
    match result2 {
        RespData::Null => {}
        _ => panic!("expected Null, got {:?}", result2),
    }
}
