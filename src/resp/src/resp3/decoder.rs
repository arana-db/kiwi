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

#[derive(Default)]
pub struct Resp3Decoder {
    out: VecDeque<RespResult<RespData>>,
    buf: bytes::BytesMut,
}

impl Resp3Decoder {
    /// Parse a single RESP value from the buffer
    /// Returns None if more data is needed, Some(Ok(value)) if parsing succeeded,
    /// or Some(Err(error)) if parsing failed
    fn parse_single_value(&mut self) -> Option<RespResult<RespData>> {
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
                    let chunk = self.buf.split_to(line_len);
                    if chunk.len() < 3 || chunk[chunk.len() - 2] != b'\r' {
                        return None;
                    }
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
                        let need = nl + 1 + len as usize + 2;
                        if self.buf.len() < need {
                            return None;
                        }
                        let chunk = self.buf.split_to(need);
                        if &chunk[nl + 1 + len as usize..need] != b"\r\n" {
                            return Some(Err(RespError::ParseError(
                                "invalid bulk string ending".into(),
                            )));
                        }
                        let data =
                            bytes::Bytes::copy_from_slice(&chunk[nl + 1..nl + 1 + len as usize]);
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
                        let _ = self.buf.split_to(nl + 1);
                        let mut items = Vec::with_capacity(len as usize);
                        for _ in 0..len {
                            if let Some(result) = self.parse_single_value() {
                                match result {
                                    Ok(item) => items.push(item),
                                    Err(e) => return Some(Err(e)),
                                }
                            } else {
                                // Need more data, restore the buffer and return None
                                return None;
                            }
                        }
                        Some(Ok(RespData::Array(Some(items))))
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
                    let _ = self.buf.split_to(nl + 1);
                    let mut items = Vec::with_capacity(pairs);
                    for _ in 0..pairs {
                        // parse key
                        if let Some(result) = self.parse_single_value() {
                            match result {
                                Ok(k) => {
                                    // parse value
                                    if let Some(result) = self.parse_single_value() {
                                        match result {
                                            Ok(v) => {
                                                items.push((k, v));
                                            }
                                            Err(e) => {
                                                return Some(Err(e));
                                            }
                                        }
                                    } else {
                                        // Need more data, restore the buffer and return
                                        return None;
                                    }
                                }
                                Err(e) => {
                                    return Some(Err(e));
                                }
                            }
                        } else {
                            // Need more data, restore the buffer and return
                            return None;
                        }
                    }
                    Some(Ok(RespData::Map(items)))
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
                    let _ = self.buf.split_to(nl + 1);
                    let mut items = Vec::with_capacity(count);
                    for _ in 0..count {
                        if let Some(result) = self.parse_single_value() {
                            match result {
                                Ok(val) => {
                                    items.push(val);
                                }
                                Err(e) => {
                                    return Some(Err(e));
                                }
                            }
                        } else {
                            // Need more data, restore the buffer and return
                            return None;
                        }
                    }
                    Some(Ok(RespData::Set(items)))
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
                    let _ = self.buf.split_to(nl + 1);
                    let mut items = Vec::with_capacity(count);
                    for _ in 0..count {
                        if let Some(result) = self.parse_single_value() {
                            match result {
                                Ok(val) => {
                                    items.push(val);
                                }
                                Err(e) => {
                                    return Some(Err(e));
                                }
                            }
                        } else {
                            // Need more data, restore the buffer and return
                            return None;
                        }
                    }
                    Some(Ok(RespData::Push(items)))
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
        // keep empty to avoid unused import warning
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
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP3
    }
}
