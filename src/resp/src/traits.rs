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

use bytes::{Bytes, BytesMut};

use crate::{
    error::RespResult,
    types::{RespData, RespVersion},
};

/// Trait for decoding RESP protocol messages from byte streams.
///
/// This trait provides a streaming interface for parsing RESP messages incrementally.
/// Implementations should handle incomplete data gracefully and maintain parsing state.
pub trait Decoder {
    /// Push new data into the decoder's internal buffer.
    ///
    /// This method should be called when new bytes are available for parsing.
    /// The decoder will accumulate data until a complete RESP message can be parsed.
    fn push(&mut self, data: Bytes);

    /// Attempt to parse the next complete RESP message from the buffer.
    ///
    /// Returns `Some(Ok(data))` if a complete message was parsed,
    /// `Some(Err(error))` if parsing failed, or `None` if more data is needed.
    fn next(&mut self) -> Option<RespResult<RespData>>;

    /// Reset the decoder's internal state.
    ///
    /// This clears any buffered data and resets the parser to its initial state.
    fn reset(&mut self);

    /// Get the RESP version this decoder supports.
    fn version(&self) -> RespVersion;
}

/// Trait for encoding RESP protocol messages to byte streams.
///
/// This trait provides methods for converting `RespData` into RESP-formatted bytes.
/// Implementations should handle version-specific encoding rules and downlevel compatibility.
pub trait Encoder {
    /// Encode a single RESP message into a `Bytes` buffer.
    ///
    /// This method creates a new buffer and encodes the given data into it.
    /// For RESP3 types being encoded to older versions, downlevel conversion is applied.
    fn encode_one(&mut self, data: &RespData) -> RespResult<Bytes>;

    /// Encode a RESP message into an existing `BytesMut` buffer.
    ///
    /// This method appends the encoded data to the provided buffer.
    /// Useful for building larger messages or implementing streaming encoders.
    fn encode_into(&mut self, data: &RespData, out: &mut BytesMut) -> RespResult<()>;

    /// Get the RESP version this encoder supports.
    fn version(&self) -> RespVersion;
}
