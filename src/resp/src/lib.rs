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

pub mod command;
pub mod encode;
pub mod error;
pub mod parse;
pub mod types;

pub use command::{Command, CommandType, RespCommand};
pub use encode::{CmdRes, RespEncode};
pub use error::{RespError, RespResult};
pub use parse::{Parse, RespParse, RespParseResult};
pub use types::{RespData, RespType, RespVersion};

pub const CRLF: &str = "\r\n";
