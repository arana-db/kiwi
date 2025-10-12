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
    compat::{BooleanMode, DoubleMode, DownlevelPolicy, MapMode},
    encode::{RespEncode, RespEncoder},
    error::RespResult,
    traits::Encoder,
    types::{RespData, RespVersion},
};

#[derive(Default)]
pub struct Resp2Encoder {
    inner: RespEncoder,
    policy: DownlevelPolicy,
}

impl Resp2Encoder {
    pub fn new() -> Self {
        Self {
            inner: RespEncoder::new(RespVersion::RESP2),
            policy: DownlevelPolicy::default(),
        }
    }

    pub fn with_policy(policy: DownlevelPolicy) -> Self {
        Self {
            inner: RespEncoder::new(RespVersion::RESP2),
            policy,
        }
    }
}

impl Encoder for Resp2Encoder {
    fn encode_one(&mut self, data: &RespData) -> RespResult<Bytes> {
        self.inner.clear();
        self.encode_downleveled(data);
        Ok(self.inner.get_response())
    }

    fn encode_into(&mut self, data: &RespData, out: &mut BytesMut) -> RespResult<()> {
        let bytes = self.encode_one(data)?;
        out.extend_from_slice(&bytes);
        Ok(())
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP2
    }
}

impl Resp2Encoder {
    fn encode_downleveled(&mut self, data: &RespData) {
        match data {
            // RESP2 native types
            RespData::SimpleString(_)
            | RespData::Error(_)
            | RespData::Integer(_)
            | RespData::BulkString(_)
            | RespData::Array(_)
            | RespData::Inline(_) => {
                self.inner.encode_resp_data(data);
            }
            // Downlevel mappings
            RespData::Null => {
                self.inner.set_line_string("$-1");
            }
            RespData::Boolean(b) => {
                match self.policy.boolean_mode {
                    BooleanMode::Integer => self.inner.append_integer(if *b { 1 } else { 0 }),
                    BooleanMode::SimpleString => {
                        if *b {
                            self.inner.append_simple_string("OK")
                        } else {
                            self.inner.append_simple_string("ERR")
                        }
                    }
                };
            }
            RespData::Double(v) => {
                if let DoubleMode::IntegerIfWhole = self.policy.double_mode {
                    if v.fract() == 0.0 && v.is_finite() {
                        self.inner.append_integer(*v as i64);
                        return;
                    }
                }
                self.inner.append_string(&format!("{}", v));
            }
            RespData::BulkError(msg) => {
                self.inner
                    .append_string_raw(&format!("-{}\r\n", String::from_utf8_lossy(msg)));
            }
            RespData::VerbatimString { data, .. } => {
                self.inner.append_bulk_string(data);
            }
            RespData::BigNumber(s) => {
                self.inner.append_string(s);
            }
            RespData::Map(entries) => match self.policy.map_mode {
                MapMode::FlatArray => {
                    self.inner.append_array_len((entries.len() * 2) as i64);
                    for (k, v) in entries {
                        self.encode_downleveled(k);
                        self.encode_downleveled(v);
                    }
                }
                MapMode::ArrayOfPairs => {
                    self.inner.append_array_len(entries.len() as i64);
                    for (k, v) in entries {
                        self.inner.append_array_len(2);
                        self.encode_downleveled(k);
                        self.encode_downleveled(v);
                    }
                }
            },
            RespData::Set(items) | RespData::Push(items) => {
                self.inner.append_array_len(items.len() as i64);
                for it in items {
                    self.encode_downleveled(it);
                }
            }
        }
    }
}
