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

pub trait Decoder {
    fn push(&mut self, data: Bytes);
    fn next(&mut self) -> Option<RespResult<RespData>>;
    fn reset(&mut self);
    fn version(&self) -> RespVersion;
}

pub trait Encoder {
    fn encode_one(&mut self, data: &RespData) -> RespResult<Bytes>;
    fn encode_into(&mut self, data: &RespData, out: &mut BytesMut) -> RespResult<()>;
    fn version(&self) -> RespVersion;
}
