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

use std::collections::VecDeque;

use bytes::Bytes;

use crate::{
    error::{RespError, RespResult},
    traits::Decoder,
    types::{RespData, RespVersion},
};

// Maximum allowed lengths to prevent DoS attacks
const MAX_BULK_LEN: usize = 512 * 1024 * 1024; // 512 MB
const MAX_BULK_ERROR_LEN: usize = MAX_BULK_LEN; // tune independently if needed
const MAX_VERBATIM_LEN: usize = MAX_BULK_LEN; // tune independently if needed
const MAX_BIGNUM_LEN: usize = 16 * 1024 * 1024; // 16 MB of digits
const MAX_INLINE_LEN: usize = 4 * 1024; // 4 KiB for inline commands
const MAX_ARRAY_LEN: usize = 1024 * 1024; // 1M elements
const MAX_MAP_PAIRS: usize = 1024 * 1024; // 1M pairs
const MAX_SET_LEN: usize = 1024 * 1024; // 1M elements
const MAX_PUSH_LEN: usize = 1024 * 1024; // 1M elements

#[derive(Debug, Clone, Default)]
enum ParsingState {
    #[default]
    Idle,
    Array {
        expected_count: usize,
        items: Vec<RespData>,
    },
    Map {
        expected_pairs: usize,
        items: Vec<(RespData, RespData)>,
    },
    MapWaitingForValue {
        expected_pairs: usize,
        items: Vec<(RespData, RespData)>,
        current_key: RespData,
    },
    Set {
        expected_count: usize,
        items: Vec<RespData>,
    },
    Push {
        expected_count: usize,
        items: Vec<RespData>,
    },
}

#[derive(Default)]
pub struct Resp3Decoder {
    out: VecDeque<RespResult<RespData>>,
    buf: bytes::BytesMut,
    state_stack: Vec<ParsingState>,
}

impl Resp3Decoder {
    /// Continue parsing an ongoing collection
    fn continue_collection_parsing(&mut self) -> Option<RespResult<RespData>> {
        let current_state = self.state_stack.pop()?;
        match current_state {
            ParsingState::Idle => {
                // Should not happen, but handle gracefully
                None
            }
            ParsingState::Array {
                expected_count,
                mut items,
            } => {
                while items.len() < expected_count {
                    if let Some(result) = self.parse_single_value_for_collection() {
                        match result {
                            Ok(item) => items.push(item),
                            Err(e) => {
                                // Clear state stack on error
                                self.state_stack.clear();
                                return Some(Err(e));
                            }
                        }
                    } else {
                        // Need more data, restore state to stack
                        self.state_stack.push(ParsingState::Array {
                            expected_count,
                            items,
                        });
                        return None;
                    }
                }
                // All elements parsed successfully
                Some(Ok(RespData::Array(Some(items))))
            }
            ParsingState::Map {
                expected_pairs,
                mut items,
            } => {
                while items.len() < expected_pairs {
                    // Parse key
                    if let Some(result) = self.parse_single_value_for_collection() {
                        match result {
                            Ok(key) => {
                                // Parse value
                                if let Some(result) = self.parse_single_value_for_collection() {
                                    match result {
                                        Ok(value) => items.push((key, value)),
                                        Err(e) => {
                                            // Clear state stack on error
                                            self.state_stack.clear();
                                            return Some(Err(e));
                                        }
                                    }
                                } else {
                                    // Need more data for value, save key and wait
                                    self.state_stack.push(ParsingState::MapWaitingForValue {
                                        expected_pairs,
                                        items,
                                        current_key: key,
                                    });
                                    return None;
                                }
                            }
                            Err(e) => {
                                // Clear state stack on error
                                self.state_stack.clear();
                                return Some(Err(e));
                            }
                        }
                    } else {
                        // Need more data for key, restore state to stack
                        self.state_stack.push(ParsingState::Map {
                            expected_pairs,
                            items,
                        });
                        return None;
                    }
                }
                // All pairs parsed successfully
                Some(Ok(RespData::Map(items)))
            }
            ParsingState::MapWaitingForValue {
                expected_pairs,
                mut items,
                current_key,
            } => {
                // Parse value for the saved key
                if let Some(result) = self.parse_single_value_for_collection() {
                    match result {
                        Ok(value) => {
                            items.push((current_key, value));
                            // Continue with remaining pairs
                            self.state_stack.push(ParsingState::Map {
                                expected_pairs,
                                items,
                            });
                            self.continue_collection_parsing()
                        }
                        Err(e) => {
                            // Clear state stack on error
                            self.state_stack.clear();
                            Some(Err(e))
                        }
                    }
                } else {
                    // Need more data, restore state to stack
                    self.state_stack.push(ParsingState::MapWaitingForValue {
                        expected_pairs,
                        items,
                        current_key,
                    });
                    None
                }
            }
            ParsingState::Set {
                expected_count,
                mut items,
            } => {
                while items.len() < expected_count {
                    if let Some(result) = self.parse_single_value_for_collection() {
                        match result {
                            Ok(item) => items.push(item),
                            Err(e) => {
                                // Clear state stack on error
                                self.state_stack.clear();
                                return Some(Err(e));
                            }
                        }
                    } else {
                        // Need more data, restore state to stack
                        self.state_stack.push(ParsingState::Set {
                            expected_count,
                            items,
                        });
                        return None;
                    }
                }
                // All elements parsed successfully
                Some(Ok(RespData::Set(items)))
            }
            ParsingState::Push {
                expected_count,
                mut items,
            } => {
                while items.len() < expected_count {
                    if let Some(result) = self.parse_single_value_for_collection() {
                        match result {
                            Ok(item) => items.push(item),
                            Err(e) => {
                                // Clear state stack on error
                                self.state_stack.clear();
                                return Some(Err(e));
                            }
                        }
                    } else {
                        // Need more data, restore state to stack
                        self.state_stack.push(ParsingState::Push {
                            expected_count,
                            items,
                        });
                        return None;
                    }
                }
                // All elements parsed successfully
                Some(Ok(RespData::Push(items)))
            }
        }
    }

    /// Parse a single RESP value (with state tracking for collections)
    fn parse_single_value(&mut self) -> Option<RespResult<RespData>> {
        // First, try to continue parsing any ongoing collection
        if let Some(result) = self.continue_collection_parsing() {
            return Some(result);
        }
        self.parse_single_value_atomic()
    }

    /// Parse a single RESP value that can be part of a collection
    fn parse_single_value_for_collection(&mut self) -> Option<RespResult<RespData>> {
        // If we have ongoing collection parsing, continue with that
        if !self.state_stack.is_empty() {
            return self.continue_collection_parsing();
        }
        // Otherwise parse atomically
        self.parse_single_value_atomic()
    }

    /// Parse a single RESP value atomically (without state tracking)
    fn parse_single_value_atomic(&mut self) -> Option<RespResult<RespData>> {
        if self.buf.is_empty() {
            return None;
        }

        match self.buf[0] {
            // RESP3-specific types
            b'_' => {
                if self.buf.len() < 3 {
                    return None;
                }
                if &self.buf[..3] == b"_\r\n" {
                    let _ = self.buf.split_to(3);
                    Some(Ok(RespData::Null))
                } else {
                    Some(Err(RespError::ParseError("invalid RESP3 null".into())))
                }
            }
            b'#' => {
                if self.buf.len() < 4 {
                    return None;
                }
                if &self.buf[..4] == b"#t\r\n" {
                    let _ = self.buf.split_to(4);
                    Some(Ok(RespData::Boolean(true)))
                } else if &self.buf[..4] == b"#f\r\n" {
                    let _ = self.buf.split_to(4);
                    Some(Ok(RespData::Boolean(false)))
                } else {
                    Some(Err(RespError::ParseError("invalid RESP3 boolean".into())))
                }
            }
            b',' => {
                if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                    let line_len = pos + 1;
                    let line = &self.buf[..line_len];
                    if line.len() < 3 || line[line.len() - 2] != b'\r' {
                        return None;
                    }
                    let chunk = self.buf.split_to(line_len);
                    let body = &chunk[1..chunk.len() - 2];
                    if let Ok(s) = std::str::from_utf8(body) {
                        let sl = s.to_ascii_lowercase();
                        let val = if sl == "inf" {
                            Some(f64::INFINITY)
                        } else if sl == "-inf" {
                            Some(f64::NEG_INFINITY)
                        } else if sl == "nan" {
                            Some(f64::NAN)
                        } else {
                            s.parse::<f64>().ok()
                        };
                        match val {
                            Some(v) => Some(Ok(RespData::Double(v))),
                            None => Some(Err(RespError::ParseError("invalid RESP3 double".into()))),
                        }
                    } else {
                        Some(Err(RespError::ParseError("invalid RESP3 double".into())))
                    }
                } else {
                    None
                }
            }
            b'!' => {
                if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                    if nl < 3 || self.buf[nl - 1] != b'\r' {
                        return None;
                    }
                    let len_bytes = &self.buf[1..nl - 1];
                    let len = match std::str::from_utf8(len_bytes)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return Some(Err(RespError::ParseError(
                                "invalid bulk error len".into(),
                            )));
                        }
                    };
                    if len > MAX_BULK_ERROR_LEN {
                        return Some(Err(RespError::ParseError(
                            "bulk error length exceeds limit".into(),
                        )));
                    }
                    let need = nl + 1 + len + 2;
                    if self.buf.len() < need {
                        return None;
                    }
                    let chunk = self.buf.split_to(need);
                    if &chunk[nl + 1 + len..need] != b"\r\n" {
                        return Some(Err(RespError::ParseError(
                            "invalid bulk error ending".into(),
                        )));
                    }
                    let data = bytes::Bytes::copy_from_slice(&chunk[nl + 1..nl + 1 + len]);
                    Some(Ok(RespData::BulkError(data)))
                } else {
                    None
                }
            }
            b'=' => {
                if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                    if nl < 3 || self.buf[nl - 1] != b'\r' {
                        return None;
                    }
                    let len_bytes = &self.buf[1..nl - 1];
                    let len = match std::str::from_utf8(len_bytes)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return Some(Err(RespError::ParseError("invalid verbatim len".into())));
                        }
                    };
                    if len > MAX_VERBATIM_LEN {
                        return Some(Err(RespError::ParseError(
                            "verbatim string length exceeds limit".into(),
                        )));
                    }
                    let need = nl + 1 + len + 2;
                    if self.buf.len() < need {
                        return None;
                    }
                    let chunk = self.buf.split_to(need);
                    let content = &chunk[nl + 1..nl + 1 + len];
                    if content.len() < 4 || content[3] != b':' {
                        return Some(Err(RespError::ParseError("invalid verbatim header".into())));
                    }
                    let mut fmt = [0u8; 3];
                    fmt.copy_from_slice(&content[0..3]);
                    let data = bytes::Bytes::copy_from_slice(&content[4..]);
                    if &chunk[nl + 1 + len..need] != b"\r\n" {
                        return Some(Err(RespError::ParseError("invalid verbatim ending".into())));
                    }
                    Some(Ok(RespData::VerbatimString { format: fmt, data }))
                } else {
                    None
                }
            }
            b'(' => {
                if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                    let line_len = pos + 1;
                    if line_len < 3 || self.buf[line_len - 2] != b'\r' {
                        return None;
                    }
                    // body length excludes '(' and CRLF
                    let body_len = line_len - 3;
                    if body_len > MAX_BIGNUM_LEN {
                        return Some(Err(RespError::ParseError(
                            "big number length exceeds limit".into(),
                        )));
                    }
                    let chunk = self.buf.split_to(line_len);
                    let body = &chunk[1..chunk.len() - 2];
                    match std::str::from_utf8(body) {
                        Ok(s) => Some(Ok(RespData::BigNumber(s.to_string()))),
                        Err(_) => Some(Err(RespError::ParseError("invalid big number".into()))),
                    }
                } else {
                    None
                }
            }
            // Standard RESP types
            b'+' => {
                if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                    let line_len = pos + 1;
                    let line = &self.buf[..line_len];
                    if line.len() < 3 || line[line.len() - 2] != b'\r' {
                        return None;
                    }
                    let chunk = self.buf.split_to(line_len);
                    let data = bytes::Bytes::copy_from_slice(&chunk[1..chunk.len() - 2]);
                    Some(Ok(RespData::SimpleString(data)))
                } else {
                    None
                }
            }
            b'-' => {
                if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                    let line_len = pos + 1;
                    let line = &self.buf[..line_len];
                    if line.len() < 3 || line[line.len() - 2] != b'\r' {
                        return None;
                    }
                    let chunk = self.buf.split_to(line_len);
                    let data = bytes::Bytes::copy_from_slice(&chunk[1..chunk.len() - 2]);
                    Some(Ok(RespData::Error(data)))
                } else {
                    None
                }
            }
            b':' => {
                if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                    let line_len = pos + 1;
                    let line = &self.buf[..line_len];
                    if line.len() < 3 || line[line.len() - 2] != b'\r' {
                        return None;
                    }
                    let chunk = self.buf.split_to(line_len);
                    let num_str = &chunk[1..chunk.len() - 2];
                    match std::str::from_utf8(num_str)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(n) => Some(Ok(RespData::Integer(n))),
                        None => Some(Err(RespError::ParseError("invalid integer".into()))),
                    }
                } else {
                    None
                }
            }
            b'$' => {
                if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                    if nl < 3 || self.buf[nl - 1] != b'\r' {
                        return None;
                    }
                    let len_bytes = &self.buf[1..nl - 1];
                    let len = match std::str::from_utf8(len_bytes)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return Some(Err(RespError::ParseError(
                                "invalid bulk string len".into(),
                            )));
                        }
                    };
                    if len == -1 {
                        let _ = self.buf.split_to(nl + 1);
                        Some(Ok(RespData::BulkString(None)))
                    } else if len >= 0 {
                        let len_usize = len as usize;
                        if len_usize > MAX_BULK_LEN {
                            return Some(Err(RespError::ParseError(
                                "bulk string length exceeds limit".into(),
                            )));
                        }
                        let need = nl + 1 + len_usize + 2;
                        if self.buf.len() < need {
                            return None;
                        }
                        let chunk = self.buf.split_to(need);
                        if &chunk[nl + 1 + len_usize..need] != b"\r\n" {
                            return Some(Err(RespError::ParseError(
                                "invalid bulk string ending".into(),
                            )));
                        }
                        let data =
                            bytes::Bytes::copy_from_slice(&chunk[nl + 1..nl + 1 + len_usize]);
                        Some(Ok(RespData::BulkString(Some(data))))
                    } else {
                        Some(Err(RespError::ParseError(
                            "negative bulk string len".into(),
                        )))
                    }
                } else {
                    None
                }
            }
            b'*' => {
                if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                    if nl < 3 || self.buf[nl - 1] != b'\r' {
                        return None;
                    }
                    let len_bytes = &self.buf[1..nl - 1];
                    let len = match std::str::from_utf8(len_bytes)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return Some(Err(RespError::ParseError("invalid array len".into())));
                        }
                    };
                    if len == -1 {
                        let _ = self.buf.split_to(nl + 1);
                        Some(Ok(RespData::Array(None)))
                    } else if len >= 0 {
                        let len_usize = len as usize;
                        if len_usize > MAX_ARRAY_LEN {
                            return Some(Err(RespError::ParseError(
                                "array length exceeds limit".into(),
                            )));
                        }
                        // Consume header and start array parsing state
                        let _ = self.buf.split_to(nl + 1);
                        self.state_stack.push(ParsingState::Array {
                            expected_count: len_usize,
                            items: Vec::with_capacity(len_usize),
                        });
                        // Continue parsing elements - this will parse the array elements
                        // and return the complete array when done
                        self.continue_collection_parsing()
                    } else {
                        Some(Err(RespError::ParseError("negative array len".into())))
                    }
                } else {
                    None
                }
            }
            b'%' => {
                // Map: %<len>\r\n<k1><v1>...
                if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                    if nl < 3 || self.buf[nl - 1] != b'\r' {
                        return None;
                    }
                    let len_bytes = &self.buf[1..nl - 1];
                    let pairs = match std::str::from_utf8(len_bytes)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return Some(Err(RespError::ParseError("invalid map len".into())));
                        }
                    };
                    if pairs > MAX_MAP_PAIRS {
                        return Some(Err(RespError::ParseError("map pairs exceed limit".into())));
                    }
                    // Consume header and start map parsing state
                    let _ = self.buf.split_to(nl + 1);
                    self.state_stack.push(ParsingState::Map {
                        expected_pairs: pairs,
                        items: Vec::with_capacity(pairs),
                    });
                    // Continue parsing pairs
                    self.continue_collection_parsing()
                } else {
                    None
                }
            }
            b'~' => {
                // Set: ~<len>\r\n<item>...
                if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                    if nl < 3 || self.buf[nl - 1] != b'\r' {
                        return None;
                    }
                    let len_bytes = &self.buf[1..nl - 1];
                    let count = match std::str::from_utf8(len_bytes)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return Some(Err(RespError::ParseError("invalid set len".into())));
                        }
                    };
                    if count > MAX_SET_LEN {
                        return Some(Err(RespError::ParseError(
                            "set length exceeds limit".into(),
                        )));
                    }
                    // Consume header and start set parsing state
                    let _ = self.buf.split_to(nl + 1);
                    self.state_stack.push(ParsingState::Set {
                        expected_count: count,
                        items: Vec::with_capacity(count),
                    });
                    // Continue parsing elements
                    self.continue_collection_parsing()
                } else {
                    None
                }
            }
            b'>' => {
                // Push: >len\r\n<item>...
                if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                    if nl < 3 || self.buf[nl - 1] != b'\r' {
                        return None;
                    }
                    let len_bytes = &self.buf[1..nl - 1];
                    let count = match std::str::from_utf8(len_bytes)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return Some(Err(RespError::ParseError("invalid push len".into())));
                        }
                    };
                    if count > MAX_PUSH_LEN {
                        return Some(Err(RespError::ParseError(
                            "push length exceeds limit".into(),
                        )));
                    }
                    // Consume header and start push parsing state
                    let _ = self.buf.split_to(nl + 1);
                    self.state_stack.push(ParsingState::Push {
                        expected_count: count,
                        items: Vec::with_capacity(count),
                    });
                    // Continue parsing elements
                    self.continue_collection_parsing()
                } else {
                    None
                }
            }
            // Inline command (no prefix, just data followed by \r\n)
            _ => {
                // Check if this looks like an inline command (no prefix, ends with \r\n)
                if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                    let line_len = pos + 1;
                    let line = &self.buf[..line_len];
                    if line.len() >= 2 && line[line.len() - 2] == b'\r' {
                        // Enforce max length for inline commands
                        let data_len = line_len - 2; // Exclude \r\n
                        if data_len > MAX_INLINE_LEN {
                            return Some(Err(RespError::ParseError(
                                "inline command length exceeds limit".into(),
                            )));
                        }

                        // Reject if first byte is a known RESP prefix or non-printable
                        let first_byte = self.buf[0];
                        if matches!(
                            first_byte,
                            b'+' | b'-'
                                | b':'
                                | b'$'
                                | b'*'
                                | b'%'
                                | b'~'
                                | b'>'
                                | b'_'
                                | b'#'
                                | b','
                                | b'!'
                                | b'='
                                | b'('
                        ) || !(32..=126).contains(&first_byte)
                        {
                            return Some(Err(RespError::ParseError(
                                "invalid inline command prefix".into(),
                            )));
                        }

                        let chunk = self.buf.split_to(line_len);
                        let data = &chunk[..chunk.len() - 2]; // Remove \r\n
                        let parts: Vec<Bytes> = data
                            .split(|&b| b == b' ')
                            .map(bytes::Bytes::copy_from_slice)
                            .collect();
                        Some(Ok(RespData::Inline(parts)))
                    } else {
                        Some(Err(RespError::ParseError("invalid inline command".into())))
                    }
                } else {
                    None
                }
            }
        }
    }
}

impl Decoder for Resp3Decoder {
    fn push(&mut self, data: Bytes) {
        self.buf.extend_from_slice(&data);

        while let Some(result) = self.parse_single_value() {
            match result {
                Ok(data) => {
                    self.out.push_back(Ok(data));
                }
                Err(e) => {
                    self.out.push_back(Err(e));
                    break;
                }
            }
        }
    }

    fn next(&mut self) -> Option<RespResult<RespData>> {
        self.out.pop_front()
    }

    fn reset(&mut self) {
        self.out.clear();
        self.buf.clear();
        self.state_stack.clear();
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP3
    }
}
