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
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;

use serde_ini::de::Error as serdeErr;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Could not read file {}: {}", path.display(), source))]
    ConfigFile { source: io::Error, path: PathBuf },

    #[snafu(display("Invalid configuration: {}", source))]
    InvalidConfig { source: serdeErr },

    #[snafu(display("validate fail: {}", source))]
    ValidConfigFail { source: validator::ValidationErrors },

    #[snafu(display("Invalid memory: {}", source))]
    MemoryParse { source: MemoryParseError },
}

#[derive(Debug, Snafu)]
pub enum MemoryParseError {
    #[snafu(display("invalid data: {}", source))]
    InvalidNumber { source: ParseIntError },

    #[snafu(display(
        "invalid memory uint: '{}'. support: B, K, M, G, T (ignore letter case)",
        unit
    ))]
    UnknownUnit { unit: String },

    #[snafu(display("wrong format: '{}'. correct example: 256MB, 1.5GB, 512K", raw))]
    InvalidFormat { raw: String },

    #[snafu(display("out of range: '{}'. max : ~16EB (2^64 bytes)", raw))]
    OutOfRange { raw: String },
}
