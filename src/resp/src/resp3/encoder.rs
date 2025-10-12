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
    encode::{RespEncode, RespEncoder},
    error::RespResult,
    traits::Encoder,
    types::{RespData, RespVersion},
};

#[derive(Default)]
pub struct Resp3Encoder {
    resp2_encoder: Option<RespEncoder>,
}

impl Encoder for Resp3Encoder {
    fn encode_one(&mut self, data: &RespData) -> RespResult<Bytes> {
        let mut buf = BytesMut::new();
        self.encode_into(data, &mut buf)?;
        Ok(buf.freeze())
    }

    fn encode_into(&mut self, data: &RespData, out: &mut BytesMut) -> RespResult<()> {
        match data {
            RespData::Null => {
                out.extend_from_slice(b"_\r\n");
            }
            RespData::Boolean(true) => {
                out.extend_from_slice(b"#t\r\n");
            }
            RespData::Boolean(false) => {
                out.extend_from_slice(b"#f\r\n");
            }
            RespData::Double(v) => {
                out.extend_from_slice(b",");
                if v.is_infinite() {
                    if v.is_sign_positive() {
                        out.extend_from_slice(b"inf");
                    } else {
                        out.extend_from_slice(b"-inf");
                    }
                } else if v.is_nan() {
                    out.extend_from_slice(b"nan");
                } else {
                    use core::fmt::Write as _;
                    let mut s = String::new();
                    let _ = write!(&mut s, "{}", v);
                    out.extend_from_slice(s.as_bytes());
                }
                out.extend_from_slice(b"\r\n");
            }
            RespData::BulkError(b) => {
                use core::fmt::Write as _;
                let len = b.len();
                let _ = write!(out, "!{}\r\n", len);
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            RespData::VerbatimString { format, data } => {
                use core::fmt::Write as _;
                // fmt: exactly 3 bytes
                let payload_len = 3 + 1 + data.len();
                let _ = write!(out, "={}\r\n", payload_len);
                out.extend_from_slice(format);
                out.extend_from_slice(b":");
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
            }
            RespData::BigNumber(s) => {
                out.extend_from_slice(b"(");
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespData::Map(entries) => {
                use core::fmt::Write as _;
                let _ = write!(out, "%{}\r\n", entries.len());
                for (k, v) in entries {
                    self.encode_into(k, out)?;
                    self.encode_into(v, out)?;
                }
            }
            RespData::Set(items) => {
                use core::fmt::Write as _;
                let _ = write!(out, "~{}\r\n", items.len());
                for it in items {
                    self.encode_into(it, out)?;
                }
            }
            RespData::Push(items) => {
                use core::fmt::Write as _;
                let _ = write!(out, ">{}\r\n", items.len());
                for it in items {
                    self.encode_into(it, out)?;
                }
            }
            // Standard RESP types - delegate to RESP2 encoder
            RespData::SimpleString(s) => {
                if self.resp2_encoder.is_none() {
                    self.resp2_encoder = Some(RespEncoder::new(RespVersion::RESP2));
                }
                let encoder = self.resp2_encoder.as_mut().unwrap();
                encoder
                    .clear()
                    .append_simple_string(std::str::from_utf8(s).unwrap_or(""));
                out.extend_from_slice(&encoder.get_response());
            }
            RespData::Error(s) => {
                out.extend_from_slice(b"-");
                out.extend_from_slice(s);
                out.extend_from_slice(b"\r\n");
            }
            RespData::Integer(n) => {
                if self.resp2_encoder.is_none() {
                    self.resp2_encoder = Some(RespEncoder::new(RespVersion::RESP2));
                }
                let encoder = self.resp2_encoder.as_mut().unwrap();
                encoder.clear().append_integer(*n);
                out.extend_from_slice(&encoder.get_response());
            }
            RespData::BulkString(Some(s)) => {
                if self.resp2_encoder.is_none() {
                    self.resp2_encoder = Some(RespEncoder::new(RespVersion::RESP2));
                }
                let encoder = self.resp2_encoder.as_mut().unwrap();
                encoder.clear().append_bulk_string(s);
                out.extend_from_slice(&encoder.get_response());
            }
            RespData::BulkString(None) => {
                out.extend_from_slice(b"$-1\r\n");
            }
            RespData::Array(Some(items)) => {
                if self.resp2_encoder.is_none() {
                    self.resp2_encoder = Some(RespEncoder::new(RespVersion::RESP2));
                }
                let encoder = self.resp2_encoder.as_mut().unwrap();
                encoder.clear().append_array_len(items.len() as i64);
                out.extend_from_slice(&encoder.get_response());
                for item in items {
                    self.encode_into(item, out)?;
                }
                return Ok(());
            }
            RespData::Array(None) => {
                out.extend_from_slice(b"*-1\r\n");
            }
            RespData::Inline(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        out.extend_from_slice(b" ");
                    }
                    out.extend_from_slice(part);
                }
                out.extend_from_slice(b"\r\n");
            }
        }
        Ok(())
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP3
    }
}
