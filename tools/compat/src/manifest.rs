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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::de::{Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use thiserror::Error;

pub const MANIFEST_SCHEMA: &str = "kiwi-redis-compat/v1";
pub const REQUIRED_VECTOR_JOBS_SCHEMA: &str = "kiwi-vector-required-jobs/v1";
pub const REDIS_TAG: &str = "8.8.1";
pub const REDIS_COMMIT: &str = "77b6c308396c9700672390a210143a8496fb4b10";

#[derive(Debug, PartialEq, Eq)]
pub struct CompatibilityManifest {
    schema: String,
    profile: Profile,
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

    pub fn profile(&self) -> Profile {
        self.profile
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

            if raw_command.modes.0.is_empty() {
                return Err(ManifestError::EmptyModes { index });
            }
            if raw_command.protocols.is_empty() {
                return Err(ManifestError::EmptyProtocols { index });
            }
            let requires_test_evidence = raw_command.classification.is_required()
                || raw_command
                    .modes
                    .0
                    .values()
                    .any(RawClassification::is_required);
            if requires_test_evidence && raw_command.tests.is_empty() {
                return Err(ManifestError::EmptyTests { index });
            }
            let requires_known_difference = raw_command.classification.is_known_difference()
                || raw_command
                    .modes
                    .0
                    .values()
                    .any(RawClassification::is_known_difference);
            if requires_known_difference && raw_command.known_differences.is_empty() {
                return Err(ManifestError::MissingKnownDifferences { index });
            }
            if !requires_known_difference && !raw_command.known_differences.is_empty() {
                return Err(ManifestError::UnexpectedKnownDifferences { index });
            }
            for (difference_index, difference) in raw_command.known_differences.iter().enumerate() {
                for (field, value) in [
                    ("owner", difference.owner.as_str()),
                    ("issue", difference.issue.as_str()),
                    ("reason", difference.reason.as_str()),
                    ("remove_when", difference.remove_when.as_str()),
                    ("introduced", difference.introduced.as_str()),
                    ("affected", difference.affected.as_str()),
                    ("last_verified_ref", difference.last_verified_ref.as_str()),
                ] {
                    if value.trim().is_empty() {
                        return Err(ManifestError::EmptyKnownDifferenceField {
                            index,
                            difference_index,
                            field,
                        });
                    }
                }
                if !is_iso_date(&difference.introduced) {
                    return Err(ManifestError::InvalidKnownDifferenceIntroduced {
                        index,
                        difference_index,
                    });
                }
            }
            if raw_command.owner.trim().is_empty() {
                return Err(ManifestError::EmptyOwner { index });
            }

            commands.push(CommandContract {
                command,
                classification: raw_command.classification.into(),
                modes: raw_command
                    .modes
                    .0
                    .into_iter()
                    .map(|(mode, classification)| (mode.into(), classification.into()))
                    .collect(),
                protocols: raw_command.protocols.into_iter().map(Into::into).collect(),
                arguments: raw_command.arguments.into(),
                reply_schema: raw_command.reply_schema.into(),
                errors: raw_command.errors.into(),
                ttl_semantics: raw_command.ttl_semantics.into(),
                tests: raw_command.tests.into_iter().map(Into::into).collect(),
                known_differences: raw_command
                    .known_differences
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                owner: raw_command.owner,
            });
        }

        Ok(Self {
            schema: raw.schema,
            profile: raw.profile.into(),
            redis: RedisBaseline {
                tag: raw.redis.tag,
                commit: raw.redis.commit,
            },
            commands,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RequiredVectorJobs {
    job_id: String,
    test_module: String,
    pytest_marker: String,
    protocols: Vec<Protocol>,
    commands: Vec<String>,
    raw_cases: BTreeMap<String, Vec<RequiredVectorRawCase>>,
    expected_node_ids: Vec<String>,
    expected_item_count: usize,
    manifest_profile: Profile,
    fast_job_owner: String,
    fast_job_deselect_marker: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RequiredVectorRawCase {
    case_id: String,
    evidence_kind: String,
    node_ids: Vec<String>,
}

impl RequiredVectorRawCase {
    pub fn case_id(&self) -> &str {
        &self.case_id
    }

    pub fn evidence_kind(&self) -> &str {
        &self.evidence_kind
    }

    pub fn node_ids(&self) -> &[String] {
        &self.node_ids
    }
}

impl RequiredVectorJobs {
    pub fn from_yaml(input: &str) -> Result<Self, ManifestError> {
        let raw: RawRequiredVectorJobs = yaml_serde::from_str(input)?;
        if raw.schema != REQUIRED_VECTOR_JOBS_SCHEMA {
            return Err(ManifestError::InvalidRequiredJobs(
                "required-jobs schema is not kiwi-vector-required-jobs/v1".to_string(),
            ));
        }
        if raw.jobs.len() != 1 {
            return Err(ManifestError::InvalidRequiredJobs(
                "required-jobs registry must contain exactly one job".to_string(),
            ));
        }
        let raw_job = raw.jobs.into_iter().next().ok_or_else(|| {
            ManifestError::InvalidRequiredJobs("required job is missing".to_string())
        })?;
        if raw_job.job_id != "trusted-vector-differential"
            || raw_job.test_module != "tests/python/test_vector_set_differential.py"
            || raw_job.pytest_marker != "raw_vector_protocol"
        {
            return Err(ManifestError::InvalidRequiredJobs(
                "required Vector job identity, module, or marker drifted".to_string(),
            ));
        }
        let protocols = raw_job
            .protocols
            .into_iter()
            .map(Protocol::from)
            .collect::<Vec<_>>();
        if protocols != [Protocol::Resp2, Protocol::Resp3] {
            return Err(ManifestError::InvalidRequiredJobs(
                "required Vector protocols must be exactly resp2, resp3".to_string(),
            ));
        }
        let mut commands = BTreeMap::new();
        for command in raw_job.commands {
            if !command.starts_with('V')
                || !command.bytes().all(|byte| (b'A'..=b'Z').contains(&byte))
                || commands.insert(command.clone(), ()).is_some()
            {
                return Err(ManifestError::InvalidRequiredJobs(format!(
                    "invalid or duplicate required Vector command {command:?}"
                )));
            }
        }
        if commands.is_empty() {
            return Err(ManifestError::InvalidRequiredJobs(
                "required Vector command scope must not be empty".to_string(),
            ));
        }
        if raw_job.raw_cases.keys().collect::<Vec<_>>() != commands.keys().collect::<Vec<_>>() {
            return Err(ManifestError::InvalidRequiredJobs(
                "raw-case command ownership must exactly match required Vector commands"
                    .to_string(),
            ));
        }
        let mut node_ids = BTreeMap::new();
        let prefix = format!("{}::", raw_job.test_module);
        for node_id in &raw_job.expected_node_ids {
            if !node_id.starts_with(&prefix)
                || node_id.trim() != node_id
                || node_ids.insert(node_id.clone(), ()).is_some()
            {
                return Err(ManifestError::InvalidRequiredJobs(format!(
                    "invalid or duplicate expected pytest node ID {node_id:?}"
                )));
            }
        }
        if raw_job.expected_item_count == 0
            || raw_job.expected_item_count != raw_job.expected_node_ids.len()
        {
            return Err(ManifestError::InvalidRequiredJobs(
                "expected_item_count must be positive and equal expected_node_ids length"
                    .to_string(),
            ));
        }
        for (command, raw_cases) in &raw_job.raw_cases {
            let case_ids = raw_cases
                .iter()
                .map(|raw_case| raw_case.case_id.as_str())
                .collect::<BTreeSet<_>>();
            if raw_cases.is_empty() || case_ids.len() != raw_cases.len() {
                return Err(ManifestError::InvalidRequiredJobs(format!(
                    "raw cases for {command} must have unique case IDs"
                )));
            }
            for raw_case in raw_cases {
                let unique = raw_case.node_ids.iter().collect::<BTreeSet<_>>();
                let valid_case_id = !raw_case.case_id.is_empty()
                    && raw_case
                        .case_id
                        .bytes()
                        .all(|byte| byte == b'-' || (b'a'..=b'z').contains(&byte));
                if !valid_case_id
                    || !matches!(
                        raw_case.evidence_kind.as_str(),
                        "exact-frame" | "raw-schema"
                    )
                    || raw_case.node_ids.is_empty()
                    || unique.len() != raw_case.node_ids.len()
                    || raw_case
                        .node_ids
                        .iter()
                        .any(|node_id| !node_ids.contains_key(node_id))
                    || !raw_case
                        .node_ids
                        .iter()
                        .any(|node_id| node_id.ends_with("[resp2]"))
                    || !raw_case
                        .node_ids
                        .iter()
                        .any(|node_id| node_id.ends_with("[resp3]"))
                {
                    return Err(ManifestError::InvalidRequiredJobs(format!(
                        "raw case {} for {command} must have a valid kind and unique registered RESP2/RESP3 node IDs",
                        raw_case.case_id
                    )));
                }
            }
        }
        for (command, raw_cases) in &raw_job.raw_cases {
            let actual = raw_cases
                .iter()
                .map(|raw_case| (raw_case.case_id.as_str(), raw_case.evidence_kind.as_str()))
                .collect::<BTreeSet<_>>();
            let expected = if command == "VINFO" {
                BTreeSet::from([("missing-key", "exact-frame"), ("populated", "raw-schema")])
            } else {
                BTreeSet::from([("zero-vector", "exact-frame")])
            };
            if actual != expected {
                return Err(ManifestError::InvalidRequiredJobs(format!(
                    "raw evidence cases for {command} drifted"
                )));
            }
        }
        let manifest_profile = Profile::from(raw_job.manifest_profile);
        if manifest_profile != Profile::Redis881StandaloneCacheOff {
            return Err(ManifestError::InvalidRequiredJobs(
                "required Vector manifest profile must be redis_8_8_1_standalone_cache_off"
                    .to_string(),
            ));
        }
        if raw_job.fast_job.owner != raw_job.job_id
            || raw_job.fast_job.deselect_marker != raw_job.pytest_marker
        {
            return Err(ManifestError::InvalidRequiredJobs(
                "fast-job ownership must name the required job and its pytest marker".to_string(),
            ));
        }

        Ok(Self {
            job_id: raw_job.job_id,
            test_module: raw_job.test_module,
            pytest_marker: raw_job.pytest_marker,
            protocols,
            commands: commands.into_keys().collect(),
            raw_cases: raw_job
                .raw_cases
                .into_iter()
                .map(|(command, cases)| {
                    (
                        command,
                        cases
                            .into_iter()
                            .map(|raw_case| RequiredVectorRawCase {
                                case_id: raw_case.case_id,
                                evidence_kind: raw_case.evidence_kind,
                                node_ids: raw_case.node_ids,
                            })
                            .collect(),
                    )
                })
                .collect(),
            expected_node_ids: raw_job.expected_node_ids,
            expected_item_count: raw_job.expected_item_count,
            manifest_profile,
            fast_job_owner: raw_job.fast_job.owner,
            fast_job_deselect_marker: raw_job.fast_job.deselect_marker,
        })
    }

    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    pub fn test_module(&self) -> &str {
        &self.test_module
    }

    pub fn pytest_marker(&self) -> &str {
        &self.pytest_marker
    }

    pub fn protocols(&self) -> &[Protocol] {
        &self.protocols
    }

    pub fn commands(&self) -> &[String] {
        &self.commands
    }

    pub fn raw_cases(&self) -> &BTreeMap<String, Vec<RequiredVectorRawCase>> {
        &self.raw_cases
    }

    pub fn expected_node_ids(&self) -> &[String] {
        &self.expected_node_ids
    }

    pub fn expected_item_count(&self) -> usize {
        self.expected_item_count
    }

    pub fn manifest_profile(&self) -> Profile {
        self.manifest_profile
    }

    pub fn fast_job_owner(&self) -> &str {
        &self.fast_job_owner
    }

    pub fn fast_job_deselect_marker(&self) -> &str {
        &self.fast_job_deselect_marker
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Redis881CoreResp2,
    Redis881CoreResp3,
    Redis881Runtime,
    Redis881ClientEcosystem,
    Redis881StandaloneCacheOff,
    Redis881RaftSingleGroupCacheOff,
    KiwiRocksdbAuthorityV1,
    KiwiRedisraftPublicV1,
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
    arguments: ArgumentSemantics,
    reply_schema: ReplySchema,
    errors: ErrorSemantics,
    ttl_semantics: TtlSemantics,
    tests: Vec<TestEvidence>,
    known_differences: Vec<KnownDifference>,
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

    pub fn arguments(&self) -> ArgumentSemantics {
        self.arguments
    }

    pub fn reply_schema(&self) -> ReplySchema {
        self.reply_schema
    }

    pub fn errors(&self) -> ErrorSemantics {
        self.errors
    }

    pub fn ttl_semantics(&self) -> TtlSemantics {
        self.ttl_semantics
    }

    pub fn tests(&self) -> &[TestEvidence] {
        &self.tests
    }

    pub fn known_differences(&self) -> &[KnownDifference] {
        &self.known_differences
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArgumentSemantics {
    Exact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplySchema {
    Exact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorSemantics {
    ExactPrefix,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlSemantics {
    Applicable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestEvidence {
    WireDifferential,
    FinalState,
}

#[derive(Debug, PartialEq, Eq)]
pub struct KnownDifference {
    owner: String,
    issue: String,
    reason: String,
    remove_when: String,
    introduced: String,
    affected: String,
    last_verified_ref: String,
}

impl KnownDifference {
    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn issue(&self) -> &str {
        &self.issue
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn remove_when(&self) -> &str {
        &self.remove_when
    }

    pub fn introduced(&self) -> &str {
        &self.introduced
    }

    pub fn affected(&self) -> &str {
        &self.affected
    }

    pub fn last_verified_ref(&self) -> &str {
        &self.last_verified_ref
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCompatibilityManifest {
    schema: String,
    profile: RawProfile,
    redis: RawRedisBaseline,
    commands: Vec<RawCommandContract>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequiredVectorJobs {
    schema: String,
    jobs: Vec<RawRequiredVectorJob>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequiredVectorJob {
    job_id: String,
    test_module: String,
    pytest_marker: String,
    protocols: Vec<RawProtocol>,
    commands: Vec<String>,
    raw_cases: BTreeMap<String, Vec<RawRequiredVectorCase>>,
    expected_node_ids: Vec<String>,
    expected_item_count: usize,
    manifest_profile: RawProfile,
    fast_job: RawFastJobOwnership,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequiredVectorCase {
    case_id: String,
    evidence_kind: String,
    node_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFastJobOwnership {
    owner: String,
    deselect_marker: String,
}

#[derive(Debug, Deserialize)]
enum RawProfile {
    #[serde(rename = "redis_8_8_1_core_resp2")]
    Redis881CoreResp2,
    #[serde(rename = "redis_8_8_1_core_resp3")]
    Redis881CoreResp3,
    #[serde(rename = "redis_8_8_1_runtime")]
    Redis881Runtime,
    #[serde(rename = "redis_8_8_1_client_ecosystem")]
    Redis881ClientEcosystem,
    #[serde(rename = "redis_8_8_1_standalone_cache_off")]
    Redis881StandaloneCacheOff,
    #[serde(rename = "redis_8_8_1_raft_single_group_cache_off")]
    Redis881RaftSingleGroupCacheOff,
    #[serde(rename = "kiwi_rocksdb_authority_v1")]
    KiwiRocksdbAuthorityV1,
    #[serde(rename = "kiwi_redisraft_public_v1")]
    KiwiRedisraftPublicV1,
}

impl From<RawProfile> for Profile {
    fn from(raw: RawProfile) -> Self {
        match raw {
            RawProfile::Redis881CoreResp2 => Self::Redis881CoreResp2,
            RawProfile::Redis881CoreResp3 => Self::Redis881CoreResp3,
            RawProfile::Redis881Runtime => Self::Redis881Runtime,
            RawProfile::Redis881ClientEcosystem => Self::Redis881ClientEcosystem,
            RawProfile::Redis881StandaloneCacheOff => Self::Redis881StandaloneCacheOff,
            RawProfile::Redis881RaftSingleGroupCacheOff => Self::Redis881RaftSingleGroupCacheOff,
            RawProfile::KiwiRocksdbAuthorityV1 => Self::KiwiRocksdbAuthorityV1,
            RawProfile::KiwiRedisraftPublicV1 => Self::KiwiRedisraftPublicV1,
        }
    }
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
    modes: RawModes,
    protocols: Vec<RawProtocol>,
    arguments: RawArgumentSemantics,
    reply_schema: RawReplySchema,
    errors: RawErrorSemantics,
    ttl_semantics: RawTtlSemantics,
    tests: Vec<RawTestEvidence>,
    known_differences: Vec<RawKnownDifference>,
    owner: String,
}

#[derive(Debug)]
struct RawModes(BTreeMap<RawMode, RawClassification>);

impl<'de> Deserialize<'de> for RawModes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RawModesVisitor)
    }
}

struct RawModesVisitor;

impl<'de> Visitor<'de> for RawModesVisitor {
    type Value = RawModes;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a mapping of unique compatibility modes to classifications")
    }

    fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut modes = BTreeMap::new();
        while let Some((mode, classification)) = entries.next_entry()? {
            if modes.contains_key(&mode) {
                return Err(A::Error::custom(format!("duplicate mode {mode:?}")));
            }
            modes.insert(mode, classification);
        }
        Ok(RawModes(modes))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawClassification {
    Required,
    KnownDifference,
    Deferred,
    Unsupported,
}

impl RawClassification {
    fn is_required(&self) -> bool {
        matches!(self, Self::Required)
    }

    fn is_known_difference(&self) -> bool {
        matches!(self, Self::KnownDifference)
    }
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawArgumentSemantics {
    Exact,
}

impl From<RawArgumentSemantics> for ArgumentSemantics {
    fn from(raw: RawArgumentSemantics) -> Self {
        match raw {
            RawArgumentSemantics::Exact => Self::Exact,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawReplySchema {
    Exact,
}

impl From<RawReplySchema> for ReplySchema {
    fn from(raw: RawReplySchema) -> Self {
        match raw {
            RawReplySchema::Exact => Self::Exact,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawErrorSemantics {
    ExactPrefix,
}

impl From<RawErrorSemantics> for ErrorSemantics {
    fn from(raw: RawErrorSemantics) -> Self {
        match raw {
            RawErrorSemantics::ExactPrefix => Self::ExactPrefix,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawTtlSemantics {
    Applicable,
}

impl From<RawTtlSemantics> for TtlSemantics {
    fn from(raw: RawTtlSemantics) -> Self {
        match raw {
            RawTtlSemantics::Applicable => Self::Applicable,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawTestEvidence {
    WireDifferential,
    FinalState,
}

impl From<RawTestEvidence> for TestEvidence {
    fn from(raw: RawTestEvidence) -> Self {
        match raw {
            RawTestEvidence::WireDifferential => Self::WireDifferential,
            RawTestEvidence::FinalState => Self::FinalState,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawKnownDifference {
    owner: String,
    issue: String,
    reason: String,
    remove_when: String,
    introduced: String,
    affected: String,
    last_verified_ref: String,
}

impl From<RawKnownDifference> for KnownDifference {
    fn from(raw: RawKnownDifference) -> Self {
        Self {
            owner: raw.owner,
            issue: raw.issue,
            reason: raw.reason,
            remove_when: raw.remove_when,
            introduced: raw.introduced,
            affected: raw.affected,
            last_verified_ref: raw.last_verified_ref,
        }
    }
}

fn is_iso_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
    {
        return false;
    }
    let year = value[0..4].parse::<u16>().ok();
    let month = value[5..7].parse::<u8>().ok();
    let day = value[8..10].parse::<u8>().ok();
    match (year, month, day) {
        (Some(year), Some(month), Some(day)) if (1..=12).contains(&month) => {
            let max_day = match month {
                2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
                2 => 28,
                4 | 6 | 9 | 11 => 30,
                _ => 31,
            };
            (1..=max_day).contains(&day)
        }
        _ => false,
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
    #[error("invalid required Vector jobs registry: {0}")]
    InvalidRequiredJobs(String),
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
    #[error("commands[{index}].tests must not be empty when the command is required")]
    EmptyTests { index: usize },
    #[error(
        "commands[{index}].known_differences must not be empty when the command has a known difference"
    )]
    MissingKnownDifferences { index: usize },
    #[error(
        "commands[{index}].known_differences must be empty unless the command has a known_difference classification"
    )]
    UnexpectedKnownDifferences { index: usize },
    #[error("commands[{index}].known_differences[{difference_index}].{field} must not be empty")]
    EmptyKnownDifferenceField {
        index: usize,
        difference_index: usize,
        field: &'static str,
    },
    #[error(
        "commands[{index}].known_differences[{difference_index}].introduced must be an ISO date"
    )]
    InvalidKnownDifferenceIntroduced {
        index: usize,
        difference_index: usize,
    },
    #[error("commands[{index}].owner must not be empty")]
    EmptyOwner { index: usize },
}
