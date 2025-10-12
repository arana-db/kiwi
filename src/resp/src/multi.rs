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

/// Utilities for batch processing of RESP messages.
/// 
/// This module provides functions for encoding and decoding multiple RESP messages
/// in a single operation, which is useful for pipelining and batch operations.

use bytes::{Bytes, BytesMut};

use crate::{
    error::RespResult,
    traits::{Decoder, Encoder},
    types::RespData,
};

/// Decode multiple RESP messages from a single byte chunk.
/// 
/// This function pushes the entire chunk into the decoder and attempts to parse
/// all complete messages available. Useful for processing pipelined commands.
/// 
/// # Arguments
/// * `decoder` - The decoder instance to use for parsing
/// * `chunk` - The byte chunk containing one or more RESP messages
/// 
/// # Returns
/// A vector of parsing results. Each element is either `Ok(RespData)` for a
/// successfully parsed message, or `Err(RespError)` for parsing errors.
/// 
/// # Examples
/// ```
/// use resp::{RespVersion, decode_many, new_decoder};
/// 
/// let mut decoder = new_decoder(RespVersion::RESP2);
/// let results = decode_many(&mut *decoder, "+OK\r\n:42\r\n".into());
/// assert_eq!(results.len(), 2);
/// ```
pub fn decode_many(decoder: &mut dyn Decoder, chunk: Bytes) -> Vec<RespResult<RespData>> {
    decoder.push(chunk);
    let mut out = Vec::new();
    while let Some(frame) = decoder.next() {
        out.push(frame);
    }
    out
}

/// Encode multiple RESP messages into a single byte buffer.
/// 
/// This function encodes each `RespData` value in sequence and concatenates
/// the results into a single `Bytes` buffer. Useful for building pipelined commands.
/// 
/// # Arguments
/// * `encoder` - The encoder instance to use for encoding
/// * `values` - A slice of `RespData` values to encode
/// 
/// # Returns
/// A `Result` containing the concatenated encoded bytes, or an error if encoding fails.
/// 
/// # Examples
/// ```
/// use resp::{RespData, RespVersion, encode_many, new_encoder};
/// 
/// let mut encoder = new_encoder(RespVersion::RESP2);
/// let values = vec![RespData::SimpleString("OK".into()), RespData::Integer(42)];
/// let bytes = encode_many(&mut *encoder, &values).unwrap();
/// // bytes contains "+OK\r\n:42\r\n"
/// assert_eq!(bytes.as_ref(), b"+OK\r\n:42\r\n");
/// ```
pub fn encode_many(encoder: &mut dyn Encoder, values: &[RespData]) -> RespResult<Bytes> {
    let mut buf = BytesMut::new();
    for v in values {
        encoder.encode_into(v, &mut buf)?;
    }
    Ok(buf.freeze())
}
