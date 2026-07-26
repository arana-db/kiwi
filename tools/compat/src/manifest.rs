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

use std::collections::BTreeMap;

use serde::Deserialize;
use thiserror::Error;

pub const MANIFEST_SCHEMA: &str = "kiwi-redis-compat/v1";
pub const REDIS_TAG: &str = "8.8.1";
pub const REDIS_COMMIT: &str = "77b6c308396c9700672390a210143a8496fb4b10";

#[derive(Debug, PartialEq, Eq)]
pub struct CompatibilityManifest {
    schema: String,
    profile: String,
    redis: RedisBaseline,
    commands: Vec<CommandContract>,
}

impl CompatibilityManifest {
    pub fn from_yaml(input: &str) -> Result<Self, ManifestError> {
        let raw: RawCompatibilityManifest = yaml_serde::from_str(input)?;
        Self::validate(raw)
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn redis(&self) -> &RedisBaseline {
        &self.redis
    }

    pub fn commands(&self) -> &[CommandContract] {
        &self.commands
    }

    fn validate(raw: RawCompatibilityManifest) -> Result<Self, ManifestError> {
        if raw.schema != MANIFEST_SCHEMA {
            return Err(ManifestError::InvalidSchema { actual: raw.schema });
        }

        if raw.redis.tag != REDIS_TAG || raw.redis.commit != REDIS_COMMIT {
            return Err(ManifestError::InvalidRedisIdentity {
                actual_tag: raw.redis.tag,
                actual_commit: raw.redis.commit,
            });
        }

        let mut command_indexes = BTreeMap::new();
        let mut commands = Vec::with_capacity(raw.commands.len());
        for (index, raw_command) in raw.commands.into_iter().enumerate() {
            let mut command = raw_command.command;
            if command.is_empty() || !command.bytes().all(|byte| (b'!'..=b'~').contains(&byte)) {
                return Err(ManifestError::InvalidCommand { command, index });
            }
            command.make_ascii_uppercase();

            if let Some(first_index) = command_indexes.insert(command.clone(), index) {
                return Err(ManifestError::DuplicateCommand {
                    command,
                    first_index,
                    index,
                });
            }

            if raw_command.modes.is_empty() {
                return Err(ManifestError::EmptyModes { index });
            }
            if raw_command.protocols.is_empty() {
                return Err(ManifestError::EmptyProtocols { index });
            }
            if raw_command.owner.trim().is_empty() {
                return Err(ManifestError::EmptyOwner { index });
            }

            commands.push(CommandContract {
                command,
                classification: raw_command.classification.into(),
                modes: raw_command
                    .modes
                    .into_iter()
                    .map(|(mode, classification)| (mode.into(), classification.into()))
                    .collect(),
                protocols: raw_command.protocols.into_iter().map(Into::into).collect(),
                owner: raw_command.owner,
            });
        }

        Ok(Self {
            schema: raw.schema,
            profile: raw.profile,
            redis: RedisBaseline {
                tag: raw.redis.tag,
                commit: raw.redis.commit,
            },
            commands,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RedisBaseline {
    tag: String,
    commit: String,
}

impl RedisBaseline {
    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn commit(&self) -> &str {
        &self.commit
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CommandContract {
    command: String,
    classification: Classification,
    modes: BTreeMap<Mode, Classification>,
    protocols: Vec<Protocol>,
    owner: String,
}

impl CommandContract {
    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn classification(&self) -> Classification {
        self.classification
    }

    pub fn modes(&self) -> &BTreeMap<Mode, Classification> {
        &self.modes
    }

    pub fn protocols(&self) -> &[Protocol] {
        &self.protocols
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Classification {
    Required,
    KnownDifference,
    Deferred,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Mode {
    StandaloneCacheOff,
    RaftSingleGroupCacheOff,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Resp2,
    Resp3,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCompatibilityManifest {
    schema: String,
    profile: String,
    redis: RawRedisBaseline,
    commands: Vec<RawCommandContract>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRedisBaseline {
    tag: String,
    commit: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCommandContract {
    command: String,
    classification: RawClassification,
    modes: BTreeMap<RawMode, RawClassification>,
    protocols: Vec<RawProtocol>,
    owner: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawClassification {
    Required,
    KnownDifference,
    Deferred,
    Unsupported,
}

impl From<RawClassification> for Classification {
    fn from(raw: RawClassification) -> Self {
        match raw {
            RawClassification::Required => Self::Required,
            RawClassification::KnownDifference => Self::KnownDifference,
            RawClassification::Deferred => Self::Deferred,
            RawClassification::Unsupported => Self::Unsupported,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum RawMode {
    StandaloneCacheOff,
    RaftSingleGroupCacheOff,
}

impl From<RawMode> for Mode {
    fn from(raw: RawMode) -> Self {
        match raw {
            RawMode::StandaloneCacheOff => Self::StandaloneCacheOff,
            RawMode::RaftSingleGroupCacheOff => Self::RaftSingleGroupCacheOff,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawProtocol {
    Resp2,
    Resp3,
}

impl From<RawProtocol> for Protocol {
    fn from(raw: RawProtocol) -> Self {
        match raw {
            RawProtocol::Resp2 => Self::Resp2,
            RawProtocol::Resp3 => Self::Resp3,
        }
    }
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("failed to parse compatibility manifest: {0}")]
    Parse(#[from] yaml_serde::Error),
    #[error("schema must equal {MANIFEST_SCHEMA}, got {actual:?}")]
    InvalidSchema { actual: String },
    #[error(
        "redis identity must equal tag {REDIS_TAG} and commit {REDIS_COMMIT}, got tag {actual_tag:?} and commit {actual_commit:?}"
    )]
    InvalidRedisIdentity {
        actual_tag: String,
        actual_commit: String,
    },
    #[error("commands[{index}].command duplicates commands[{first_index}].command {command:?}")]
    DuplicateCommand {
        command: String,
        first_index: usize,
        index: usize,
    },
    #[error("commands[{index}].command must be a non-empty printable ASCII token, got {command:?}")]
    InvalidCommand { command: String, index: usize },
    #[error("commands[{index}].modes must not be empty")]
    EmptyModes { index: usize },
    #[error("commands[{index}].protocols must not be empty")]
    EmptyProtocols { index: usize },
    #[error("commands[{index}].owner must not be empty")]
    EmptyOwner { index: usize },
}
