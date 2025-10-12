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

use crate::{
    compat::DownlevelPolicy,
    traits::{Decoder, Encoder},
    types::RespVersion,
};

pub fn new_decoder(version: RespVersion) -> Box<dyn Decoder> {
    match version {
        RespVersion::RESP1 => Box::new(crate::resp1::decoder::Resp1Decoder::default()),
        RespVersion::RESP2 => Box::new(crate::resp2::decoder::Resp2Decoder::default()),
        RespVersion::RESP3 => Box::new(crate::resp3::decoder::Resp3Decoder::default()),
    }
}

pub fn new_encoder(version: RespVersion) -> Box<dyn Encoder> {
    match version {
        RespVersion::RESP1 => Box::new(crate::resp1::encoder::Resp1Encoder::default()),
        RespVersion::RESP2 => Box::new(crate::resp2::encoder::Resp2Encoder::default()),
        RespVersion::RESP3 => Box::new(crate::resp3::encoder::Resp3Encoder),
    }
}

pub fn new_encoder_with_policy(version: RespVersion, policy: DownlevelPolicy) -> Box<dyn Encoder> {
    match version {
        RespVersion::RESP1 => Box::new(crate::resp1::encoder::Resp1Encoder::with_policy(policy)),
        RespVersion::RESP2 => Box::new(crate::resp2::encoder::Resp2Encoder::with_policy(policy)),
        RespVersion::RESP3 => Box::new(crate::resp3::encoder::Resp3Encoder),
    }
}
