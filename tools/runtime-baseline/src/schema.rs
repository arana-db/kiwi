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

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const STARTUP_SCHEMA_VERSION: u32 = 1;
pub const COMPILED_GIT_SHA: &str = env!("KIWI_BASELINE_COMPILED_GIT_SHA");

pub fn source_dirty() -> bool {
    match env!("KIWI_BASELINE_SOURCE_DIRTY") {
        "true" => true,
        "false" => false,
        value => panic!("KIWI_BASELINE_SOURCE_DIRTY must be true or false, got {value}"),
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Publishability {
    Publishable,
    NonPublishable { reasons: Vec<String> },
}

impl Publishability {
    pub fn from_source_dirty(source_dirty: bool) -> Self {
        if source_dirty {
            Self::NonPublishable {
                reasons: vec!["dirty_source_tree".to_owned()],
            }
        } else {
            Self::Publishable
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct StartupEvent {
    pub schema_version: u32,
    pub pid: u32,
    pub redis_addr: SocketAddr,
    pub control_addr: SocketAddr,
    pub data_dir: PathBuf,
    pub git_sha: String,
    pub publishability: Publishability,
}

impl StartupEvent {
    pub fn new(
        pid: u32,
        redis_addr: SocketAddr,
        control_addr: SocketAddr,
        data_dir: &Path,
    ) -> Result<Self> {
        let data_dir = data_dir.canonicalize().with_context(|| {
            format!("cannot canonicalize data directory {}", data_dir.display())
        })?;
        let publishability = Publishability::from_source_dirty(source_dirty());

        Ok(Self {
            schema_version: STARTUP_SCHEMA_VERSION,
            pid,
            redis_addr,
            control_addr,
            data_dir,
            git_sha: COMPILED_GIT_SHA.to_owned(),
            publishability,
        })
    }
}
