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

use resp::{RespVersion, new_decoder};

#[test]
fn bulk_string_length_limit() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // Test bulk string exceeding 512MB limit
    let oversized_len = 512 * 1024 * 1024 + 1; // 512MB + 1 byte
    let oversized_message = format!("${}\r\n", oversized_len);

    decoder.push(oversized_message.into());
    let result = decoder.next();

    assert!(result.is_some(), "Should return an error");
    assert!(
        result.unwrap().is_err(),
        "Should return parse error for oversized bulk string"
    );
}

#[test]
fn array_length_limit() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // Test array exceeding 1M elements limit
    let oversized_len = 1024 * 1024 + 1; // 1M + 1 elements
    let oversized_message = format!("*{}\r\n", oversized_len);

    decoder.push(oversized_message.into());
    let result = decoder.next();

    assert!(result.is_some(), "Should return an error");
    assert!(
        result.unwrap().is_err(),
        "Should return parse error for oversized array"
    );
}

#[test]
fn map_pairs_limit() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // Test map exceeding 1M pairs limit
    let oversized_pairs = 1024 * 1024 + 1; // 1M + 1 pairs
    let oversized_message = format!("%{}\r\n", oversized_pairs);

    decoder.push(oversized_message.into());
    let result = decoder.next();

    assert!(result.is_some(), "Should return an error");
    assert!(
        result.unwrap().is_err(),
        "Should return parse error for oversized map"
    );
}

#[test]
fn set_length_limit() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // Test set exceeding 1M elements limit
    let oversized_len = 1024 * 1024 + 1; // 1M + 1 elements
    let oversized_message = format!("~{}\r\n", oversized_len);

    decoder.push(oversized_message.into());
    let result = decoder.next();

    assert!(result.is_some(), "Should return an error");
    assert!(
        result.unwrap().is_err(),
        "Should return parse error for oversized set"
    );
}

#[test]
fn push_length_limit() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // Test push exceeding 1M elements limit
    let oversized_len = 1024 * 1024 + 1; // 1M + 1 elements
    let oversized_message = format!(">{}\r\n", oversized_len);

    decoder.push(oversized_message.into());
    let result = decoder.next();

    assert!(result.is_some(), "Should return an error");
    assert!(
        result.unwrap().is_err(),
        "Should return parse error for oversized push"
    );
}

#[test]
fn within_limits_should_work() {
    let mut decoder = new_decoder(RespVersion::RESP3);

    // Test that values within limits work correctly
    let normal_message = "*2\r\n+hello\r\n+world\r\n";

    decoder.push(normal_message.into());
    let result = decoder.next();

    assert!(result.is_some(), "Should parse successfully");
    assert!(result.unwrap().is_ok(), "Should parse without error");
}
