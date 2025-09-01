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

use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum RespError {
    #[error("Invalid RESP data: {0}")]
    InvalidData(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Incomplete data")]
    Incomplete,

    #[error("Invalid integer: {0}")]
    InvalidInteger(String),

    #[error("Invalid bulk string length: {0}")]
    InvalidBulkStringLength(String),

    #[error("Invalid array length: {0}")]
    InvalidArrayLength(String),

    #[error("Unsupported RESP type")]
    UnsupportedType,

    #[error("Unknown command: {0}")]
    UnknownCommand(String),

    #[error("Unknown subcommand: {0}")]
    UnknownSubCommand(String),

    #[error("Syntax error: {0}")]
    SyntaxError(String),

    #[error("Wrong number of arguments: {0}")]
    WrongNumberOfArguments(String),

    #[error("Unknown error: {0}")]
    UnknownError(String),
}

pub type RespResult<T> = Result<T, RespError>;
