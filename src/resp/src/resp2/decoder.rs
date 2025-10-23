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
    error::RespResult,
    parse::{Parse, RespParse, RespParseResult},
    traits::Decoder,
    types::{RespData, RespVersion},
};

#[derive(Default)]
pub struct Resp2Decoder {
    inner: RespParse,
    out: VecDeque<RespResult<RespData>>,
}

impl Resp2Decoder {
    pub fn new() -> Self {
        Self {
            inner: RespParse::new(RespVersion::RESP2),
            out: VecDeque::new(),
        }
    }
}

impl Decoder for Resp2Decoder {
    fn push(&mut self, data: Bytes) {
        let mut res = self.inner.parse(data);
        loop {
            match res {
                RespParseResult::Complete(d) => self.out.push_back(Ok(d)),
                RespParseResult::Error(e) => self.out.push_back(Err(e)),
                RespParseResult::Incomplete => break,
            }
            res = self.inner.parse(Bytes::new());
        }
    }

    fn next(&mut self) -> Option<RespResult<RespData>> {
        self.out.pop_front()
    }

    fn reset(&mut self) {
        self.inner.reset();
        self.out.clear();
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP2
    }
}
