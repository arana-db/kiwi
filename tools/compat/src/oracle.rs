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

use chrono::{DateTime, FixedOffset, TimeDelta};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const BUILD_SCHEMA: &str = "kiwi-redis-oracle-build/v3";
pub const PROVENANCE_SCHEMA: &str = "kiwi-redis-oracle-provenance/v4";
pub const DIFFERENTIAL_EVIDENCE_SCHEMA: &str = "kiwi-vector-differential-evidence/v1";
pub const RECIPE_ID: &str = "redis-8.8.1-linux-release-v3";
pub const REDIS_TAG: &str = "8.8.1";
pub const REDIS_COMMIT: &str = "77b6c308396c9700672390a210143a8496fb4b10";

const REDIS_REPOSITORY: &str = "https://github.com/redis/redis.git";
const SOURCE_DATE_EPOCH: u64 = 1_784_834_134;
const MAX_JSON_BYTES: usize = 1024 * 1024;
const MAX_PATH_BYTES: usize = 4096;
const MAX_STRING_BYTES: usize = 4096;
const MAX_VERSION_BYTES: usize = 16 * 1024;
const MAX_TOOLS: usize = 64;
const MAX_ARTIFACTS: usize = 4096;
const MAX_ARGV: usize = 32;
const MAX_SYMLINK_DEPTH: usize = 8;
const MAX_CALLBACK_TIMEOUT_MS: u64 = 600_000;
const MAX_CALLBACK_TERM_GRACE_MS: u64 = 30_000;
const MAX_CALLBACK_OUTPUT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_DIFFERENTIAL_EVIDENCE_BYTES: u64 = 128 * 1024 * 1024;
const FILE_TYPE_MASK: u32 = 0o170000;
const REGULAR_FILE: u32 = 0o100000;
const SYMLINK: u32 = 0o120000;

const REQUIRED_TOOL_ROLES: [&str; 9] = [
    "controller",
    "python",
    "git",
    "shell",
    "make",
    "cc",
    "ld",
    "ar",
    "ranlib",
];

const BUILD_ARGV: [&str; 13] = [
    "make",
    "-C",
    "/proc/self/fd/{source_fd}",
    "SHELL=/proc/self/fd/{shell_fd}",
    "BUILD_TLS=no",
    "MALLOC=libc",
    "DEBUG=",
    "DEBUG_FLAGS=",
    "ENABLE_LTO=",
    "OPT=-O3 -fno-omit-frame-pointer",
    "-j",
    "1",
    "redis-server",
];

#[derive(Debug, PartialEq, Eq)]
pub struct BuildEvidence {
    schema_version: String,
    source: SourceIdentity,
    recipe: BuildRecipe,
    tools: Vec<ToolIdentity>,
    artifacts: Vec<ArtifactEntry>,
    redis_server: RedisBinaryIdentity,
    started_at_utc: String,
    finished_at_utc: String,
}

impl BuildEvidence {
    pub fn from_json(input: &str) -> Result<Self, OracleError> {
        check_document_size(input)?;
        let raw: RawBuildEvidence = serde_json::from_str(input)?;
        Self::validate(raw)
    }

    pub fn schema_version(&self) -> &str {
        &self.schema_version
    }

    pub fn source(&self) -> &SourceIdentity {
        &self.source
    }

    pub fn recipe(&self) -> &BuildRecipe {
        &self.recipe
    }

    pub fn tools(&self) -> &[ToolIdentity] {
        &self.tools
    }

    pub fn artifacts(&self) -> &[ArtifactEntry] {
        &self.artifacts
    }

    pub fn redis_server(&self) -> &RedisBinaryIdentity {
        &self.redis_server
    }

    pub fn started_at_utc(&self) -> &str {
        &self.started_at_utc
    }

    pub fn finished_at_utc(&self) -> &str {
        &self.finished_at_utc
    }

    fn validate(raw: RawBuildEvidence) -> Result<Self, OracleError> {
        require_equal("schema_version", &raw.schema_version, BUILD_SCHEMA)?;
        let source = SourceIdentity::validate(raw.source)?;
        let recipe = BuildRecipe::validate(raw.recipe)?;
        let tools = validate_tools(raw.tools)?;
        let artifacts = validate_artifacts(raw.artifacts)?;
        let redis_server = RedisBinaryIdentity::validate(raw.redis_server, &source, &artifacts)?;
        let started = parse_timestamp("started_at_utc", &raw.started_at_utc)?;
        let finished = parse_timestamp("finished_at_utc", &raw.finished_at_utc)?;
        if started > finished {
            return invalid("finished_at_utc", "must be at or after started_at_utc");
        }

        Ok(Self {
            schema_version: raw.schema_version,
            source,
            recipe,
            tools,
            artifacts,
            redis_server,
            started_at_utc: raw.started_at_utc,
            finished_at_utc: raw.finished_at_utc,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SourceIdentity {
    repository: String,
    tag: String,
    commit: String,
    head: String,
    tag_commit: String,
    root_path: String,
    git_dir_path: String,
    tracked_untracked_clean: bool,
}

impl SourceIdentity {
    pub fn repository(&self) -> &str {
        &self.repository
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn commit(&self) -> &str {
        &self.commit
    }

    pub fn head(&self) -> &str {
        &self.head
    }

    pub fn tag_commit(&self) -> &str {
        &self.tag_commit
    }

    pub fn root_path(&self) -> &str {
        &self.root_path
    }

    pub fn git_dir_path(&self) -> &str {
        &self.git_dir_path
    }

    pub fn tracked_untracked_clean(&self) -> bool {
        self.tracked_untracked_clean
    }

    fn validate(raw: RawSourceIdentity) -> Result<Self, OracleError> {
        require_equal("source.repository", &raw.repository, REDIS_REPOSITORY)?;
        require_equal("source.tag", &raw.tag, REDIS_TAG)?;
        for (field, value) in [
            ("source.commit", raw.commit.as_str()),
            ("source.head", raw.head.as_str()),
            ("source.tag_commit", raw.tag_commit.as_str()),
        ] {
            require_hex40(field, value)?;
            require_equal(field, value, REDIS_COMMIT)?;
        }
        validate_absolute_path("source.root_path", &raw.root_path)?;
        validate_absolute_path("source.git_dir_path", &raw.git_dir_path)?;
        if raw.git_dir_path != format!("{}/.git", raw.root_path) {
            return invalid(
                "source.git_dir_path",
                "must be the .git directory below source.root_path",
            );
        }
        require_true(
            "source.tracked_untracked_clean",
            raw.tracked_untracked_clean,
        )?;

        Ok(Self {
            repository: raw.repository,
            tag: raw.tag,
            commit: raw.commit,
            head: raw.head,
            tag_commit: raw.tag_commit,
            root_path: raw.root_path,
            git_dir_path: raw.git_dir_path,
            tracked_untracked_clean: raw.tracked_untracked_clean,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BuildRecipe {
    id: String,
    build_tls: String,
    malloc: String,
    debug: String,
    debug_flags: String,
    enable_lto: String,
    opt: String,
    jobs: u16,
    source_date_epoch: u64,
    argv: Vec<String>,
}

impl BuildRecipe {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn argv(&self) -> &[String] {
        &self.argv
    }

    pub fn jobs(&self) -> u16 {
        self.jobs
    }

    pub fn source_date_epoch(&self) -> u64 {
        self.source_date_epoch
    }

    fn validate(raw: RawBuildRecipe) -> Result<Self, OracleError> {
        for (field, actual, expected) in [
            ("recipe.id", raw.id.as_str(), RECIPE_ID),
            ("recipe.build_tls", raw.build_tls.as_str(), "no"),
            ("recipe.malloc", raw.malloc.as_str(), "libc"),
            ("recipe.debug", raw.debug.as_str(), ""),
            ("recipe.debug_flags", raw.debug_flags.as_str(), ""),
            ("recipe.enable_lto", raw.enable_lto.as_str(), ""),
            (
                "recipe.opt",
                raw.opt.as_str(),
                "-O3 -fno-omit-frame-pointer",
            ),
        ] {
            require_equal(field, actual, expected)?;
        }
        if raw.jobs != 1 {
            return invalid("recipe.jobs", "must equal 1");
        }
        if raw.source_date_epoch != SOURCE_DATE_EPOCH {
            return invalid(
                "recipe.source_date_epoch",
                format!("must equal {SOURCE_DATE_EPOCH}"),
            );
        }
        validate_string_collection("recipe.argv", &raw.argv, 1, MAX_ARGV)?;
        if raw.argv.len() != BUILD_ARGV.len()
            || raw
                .argv
                .iter()
                .map(String::as_str)
                .ne(BUILD_ARGV.iter().copied())
        {
            return invalid("recipe.argv", "must equal the v3 controlled build argv");
        }

        Ok(Self {
            id: raw.id,
            build_tls: raw.build_tls,
            malloc: raw.malloc,
            debug: raw.debug,
            debug_flags: raw.debug_flags,
            enable_lto: raw.enable_lto,
            opt: raw.opt,
            jobs: raw.jobs,
            source_date_epoch: raw.source_date_epoch,
            argv: raw.argv,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ToolIdentity {
    role: String,
    path: String,
    version: String,
    sha256: String,
    identity: FileIdentity,
    held_fd: bool,
}

impl ToolIdentity {
    pub fn role(&self) -> &str {
        &self.role
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub fn held_fd(&self) -> bool {
        self.held_fd
    }

    fn validate(raw: RawToolIdentity, index: usize) -> Result<Self, OracleError> {
        let prefix = format!("tools[{index}]");
        validate_bounded_string(&format!("{prefix}.role"), &raw.role, 1, 128)?;
        if !raw
            .role
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return invalid(
                format!("{prefix}.role"),
                "must use lowercase ASCII letters, digits, or hyphen",
            );
        }
        validate_absolute_path(&format!("{prefix}.path"), &raw.path)?;
        validate_bounded_string(
            &format!("{prefix}.version"),
            &raw.version,
            1,
            MAX_VERSION_BYTES,
        )?;
        require_sha256(&format!("{prefix}.sha256"), &raw.sha256)?;
        let identity = FileIdentity::validate(raw.identity, &format!("{prefix}.identity"))?;
        require_true(&format!("{prefix}.held_fd"), raw.held_fd)?;

        Ok(Self {
            role: raw.role,
            path: raw.path,
            version: raw.version,
            sha256: raw.sha256,
            identity,
            held_fd: raw.held_fd,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FileIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    size: u64,
    nlink: u64,
}

impl FileIdentity {
    pub fn device(&self) -> u64 {
        self.device
    }

    pub fn inode(&self) -> u64 {
        self.inode
    }

    pub fn mode(&self) -> u32 {
        self.mode
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn nlink(&self) -> u64 {
        self.nlink
    }

    fn validate(raw: RawFileIdentity, field: &str) -> Result<Self, OracleError> {
        if raw.inode == 0 {
            return invalid(format!("{field}.inode"), "must be non-zero");
        }
        if raw.mode & FILE_TYPE_MASK != REGULAR_FILE {
            return invalid(format!("{field}.mode"), "must identify a regular file");
        }
        if raw.size == 0 {
            return invalid(format!("{field}.size"), "must be non-zero");
        }
        if raw.nlink == 0 {
            return invalid(format!("{field}.nlink"), "must be non-zero");
        }
        Ok(Self {
            device: raw.device,
            inode: raw.inode,
            mode: raw.mode,
            size: raw.size,
            nlink: raw.nlink,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ArtifactEntry {
    Regular(RegularArtifact),
    Symlink(SymlinkArtifact),
}

impl ArtifactEntry {
    pub fn path(&self) -> &str {
        match self {
            Self::Regular(artifact) => &artifact.path,
            Self::Symlink(artifact) => &artifact.path,
        }
    }

    pub fn mode(&self) -> u32 {
        match self {
            Self::Regular(artifact) => artifact.mode,
            Self::Symlink(artifact) => artifact.mode,
        }
    }

    pub fn kind(&self) -> ArtifactKind {
        match self {
            Self::Regular(_) => ArtifactKind::Regular,
            Self::Symlink(_) => ArtifactKind::Symlink,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactKind {
    Regular,
    Symlink,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RegularArtifact {
    path: String,
    mode: u32,
    size: u64,
    sha256: String,
}

impl RegularArtifact {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn mode(&self) -> u32 {
        self.mode
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SymlinkArtifact {
    path: String,
    mode: u32,
    target: String,
}

impl SymlinkArtifact {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn mode(&self) -> u32 {
        self.mode
    }

    pub fn target(&self) -> &str {
        &self.target
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RedisBinaryIdentity {
    artifact_path: String,
    path: String,
    sha256: String,
    identity: FileIdentity,
}

impl RedisBinaryIdentity {
    pub fn artifact_path(&self) -> &str {
        &self.artifact_path
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    fn validate(
        raw: RawRedisBinaryIdentity,
        source: &SourceIdentity,
        artifacts: &[ArtifactEntry],
    ) -> Result<Self, OracleError> {
        validate_relative_path("redis_server.artifact_path", &raw.artifact_path)?;
        validate_absolute_path("redis_server.path", &raw.path)?;
        let expected_path = format!("{}/{}", source.root_path, raw.artifact_path);
        if raw.path != expected_path {
            return invalid(
                "redis_server.path",
                "must be source.root_path joined with artifact_path",
            );
        }
        require_sha256("redis_server.sha256", &raw.sha256)?;
        let identity = FileIdentity::validate(raw.identity, "redis_server.identity")?;
        let artifact = artifacts
            .iter()
            .find(|artifact| artifact.path() == raw.artifact_path)
            .ok_or_else(|| validation("redis_server.artifact_path", "must exist in artifacts"))?;
        let ArtifactEntry::Regular(artifact) = artifact else {
            return invalid(
                "redis_server.artifact_path",
                "must refer to a regular artifact",
            );
        };
        if raw.sha256 != artifact.sha256
            || identity.mode != artifact.mode
            || identity.size != artifact.size
        {
            return invalid(
                "redis_server",
                "path, mode, size, and sha256 must match its regular artifact",
            );
        }

        Ok(Self {
            artifact_path: raw.artifact_path,
            path: raw.path,
            sha256: raw.sha256,
            identity,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ArtifactComparison {
    manifests_equal: bool,
    redis_server_sha256_equal: bool,
    source_identity_equal: bool,
    recipe_equal: bool,
    toolchain_equal: bool,
}

impl ArtifactComparison {
    pub fn manifests_equal(&self) -> bool {
        self.manifests_equal
    }

    pub fn redis_server_sha256_equal(&self) -> bool {
        self.redis_server_sha256_equal
    }

    fn validate(raw: RawArtifactComparison) -> Result<Self, OracleError> {
        for (field, value) in [
            ("comparison.manifests_equal", raw.manifests_equal),
            (
                "comparison.redis_server_sha256_equal",
                raw.redis_server_sha256_equal,
            ),
            (
                "comparison.source_identity_equal",
                raw.source_identity_equal,
            ),
            ("comparison.recipe_equal", raw.recipe_equal),
            ("comparison.toolchain_equal", raw.toolchain_equal),
        ] {
            require_true(field, value)?;
        }
        Ok(Self {
            manifests_equal: raw.manifests_equal,
            redis_server_sha256_equal: raw.redis_server_sha256_equal,
            source_identity_equal: raw.source_identity_equal,
            recipe_equal: raw.recipe_equal,
            toolchain_equal: raw.toolchain_equal,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RuntimeIdentity {
    build_role: RuntimeBuildRole,
    binary_path: String,
    binary_sha256: String,
    binary_identity: FileIdentity,
    held_fd: bool,
    pid: u32,
    info_redis_versions: Vec<String>,
}

impl RuntimeIdentity {
    pub fn build_role(&self) -> RuntimeBuildRole {
        self.build_role
    }

    pub fn binary_path(&self) -> &str {
        &self.binary_path
    }

    pub fn binary_sha256(&self) -> &str {
        &self.binary_sha256
    }

    pub fn binary_identity(&self) -> &FileIdentity {
        &self.binary_identity
    }

    pub fn held_fd(&self) -> bool {
        self.held_fd
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn info_redis_versions(&self) -> &[String] {
        &self.info_redis_versions
    }

    fn validate(raw: RawRuntimeIdentity, rebuild: &BuildEvidence) -> Result<Self, OracleError> {
        validate_absolute_path("runtime.binary_path", &raw.binary_path)?;
        require_sha256("runtime.binary_sha256", &raw.binary_sha256)?;
        let binary_identity =
            FileIdentity::validate(raw.binary_identity, "runtime.binary_identity")?;
        require_true("runtime.held_fd", raw.held_fd)?;
        if raw.pid == 0 {
            return invalid("runtime.pid", "must be non-zero");
        }
        if raw.info_redis_versions.len() != 1 || raw.info_redis_versions[0] != REDIS_TAG {
            return invalid(
                "runtime.info_redis_versions",
                format!("must contain exactly one {REDIS_TAG:?} entry"),
            );
        }
        if raw.binary_path != rebuild.redis_server.path
            || raw.binary_sha256 != rebuild.redis_server.sha256
            || binary_identity != rebuild.redis_server.identity
        {
            return invalid(
                "runtime",
                "must be bound to the held rebuild redis-server path, sha256, and file identity",
            );
        }
        Ok(Self {
            build_role: raw.build_role.into(),
            binary_path: raw.binary_path,
            binary_sha256: raw.binary_sha256,
            binary_identity,
            held_fd: raw.held_fd,
            pid: raw.pid,
            info_redis_versions: raw.info_redis_versions,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeBuildRole {
    Rebuild,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BoundedCallbackResult {
    argv: Vec<String>,
    timeout_ms: u64,
    term_grace_ms: u64,
    stdout_limit_bytes: u64,
    stderr_limit_bytes: u64,
    stdout_bytes: u64,
    stderr_bytes: u64,
    started_at_utc: String,
    finished_at_utc: String,
    exit_code: i32,
    timed_out: bool,
    output_truncated: bool,
    process_group_reaped: bool,
}

impl BoundedCallbackResult {
    pub fn argv(&self) -> &[String] {
        &self.argv
    }

    pub fn exit_code(&self) -> i32 {
        self.exit_code
    }

    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    pub fn stdout_limit_bytes(&self) -> u64 {
        self.stdout_limit_bytes
    }

    pub fn stderr_limit_bytes(&self) -> u64 {
        self.stderr_limit_bytes
    }

    fn validate(raw: RawBoundedCallbackResult) -> Result<Self, OracleError> {
        validate_string_collection("callback.argv", &raw.argv, 1, MAX_ARGV)?;
        validate_positive_bound(
            "callback.timeout_ms",
            raw.timeout_ms,
            MAX_CALLBACK_TIMEOUT_MS,
        )?;
        validate_positive_bound(
            "callback.term_grace_ms",
            raw.term_grace_ms,
            MAX_CALLBACK_TERM_GRACE_MS,
        )?;
        if raw.term_grace_ms >= raw.timeout_ms {
            return invalid(
                "callback.term_grace_ms",
                "must be less than callback.timeout_ms",
            );
        }
        validate_positive_bound(
            "callback.stdout_limit_bytes",
            raw.stdout_limit_bytes,
            MAX_CALLBACK_OUTPUT_BYTES,
        )?;
        validate_positive_bound(
            "callback.stderr_limit_bytes",
            raw.stderr_limit_bytes,
            MAX_CALLBACK_OUTPUT_BYTES,
        )?;
        if raw.stdout_bytes > raw.stdout_limit_bytes {
            return invalid(
                "callback.stdout_bytes",
                "must not exceed callback.stdout_limit_bytes",
            );
        }
        if raw.stderr_bytes > raw.stderr_limit_bytes {
            return invalid(
                "callback.stderr_bytes",
                "must not exceed callback.stderr_limit_bytes",
            );
        }
        let started = parse_timestamp("callback.started_at_utc", &raw.started_at_utc)?;
        let finished = parse_timestamp("callback.finished_at_utc", &raw.finished_at_utc)?;
        if started > finished {
            return invalid(
                "callback.finished_at_utc",
                "must be at or after callback.started_at_utc",
            );
        }
        let timeout = TimeDelta::milliseconds(raw.timeout_ms as i64);
        if finished.signed_duration_since(started) > timeout {
            return invalid(
                "callback.finished_at_utc",
                "observed callback duration must not exceed callback.timeout_ms",
            );
        }
        if raw.exit_code != 0 {
            return invalid("callback.exit_code", "must equal 0");
        }
        require_false("callback.timed_out", raw.timed_out)?;
        require_false("callback.output_truncated", raw.output_truncated)?;
        require_true("callback.process_group_reaped", raw.process_group_reaped)?;
        Ok(Self {
            argv: raw.argv,
            timeout_ms: raw.timeout_ms,
            term_grace_ms: raw.term_grace_ms,
            stdout_limit_bytes: raw.stdout_limit_bytes,
            stderr_limit_bytes: raw.stderr_limit_bytes,
            stdout_bytes: raw.stdout_bytes,
            stderr_bytes: raw.stderr_bytes,
            started_at_utc: raw.started_at_utc,
            finished_at_utc: raw.finished_at_utc,
            exit_code: raw.exit_code,
            timed_out: raw.timed_out,
            output_truncated: raw.output_truncated,
            process_group_reaped: raw.process_group_reaped,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CallbackInputIdentity {
    expected_head: String,
    actual_head: String,
    tree_oid: String,
    ref_context: String,
    input_manifest_sha256: String,
    kiwi_sha256: String,
    required_jobs_helper_sha256: String,
    frozen_from_git_objects: bool,
    readonly_mount: bool,
    revalidated_after_callback: bool,
    original_inputs_revalidated: bool,
}

impl CallbackInputIdentity {
    fn validate(raw: RawCallbackInputIdentity) -> Result<Self, OracleError> {
        require_hex40("callback_input.expected_head", &raw.expected_head)?;
        require_hex40("callback_input.actual_head", &raw.actual_head)?;
        require_equal(
            "callback_input.actual_head",
            &raw.actual_head,
            &raw.expected_head,
        )?;
        require_hex40("callback_input.tree_oid", &raw.tree_oid)?;
        validate_bounded_string(
            "callback_input.ref_context",
            &raw.ref_context,
            1,
            MAX_STRING_BYTES,
        )?;
        for (field, value) in [
            (
                "callback_input.input_manifest_sha256",
                raw.input_manifest_sha256.as_str(),
            ),
            ("callback_input.kiwi_sha256", raw.kiwi_sha256.as_str()),
            (
                "callback_input.required_jobs_helper_sha256",
                raw.required_jobs_helper_sha256.as_str(),
            ),
        ] {
            require_sha256(field, value)?;
        }
        for (field, value) in [
            (
                "callback_input.frozen_from_git_objects",
                raw.frozen_from_git_objects,
            ),
            ("callback_input.readonly_mount", raw.readonly_mount),
            (
                "callback_input.revalidated_after_callback",
                raw.revalidated_after_callback,
            ),
            (
                "callback_input.original_inputs_revalidated",
                raw.original_inputs_revalidated,
            ),
        ] {
            require_true(field, value)?;
        }
        Ok(Self {
            expected_head: raw.expected_head,
            actual_head: raw.actual_head,
            tree_oid: raw.tree_oid,
            ref_context: raw.ref_context,
            input_manifest_sha256: raw.input_manifest_sha256,
            kiwi_sha256: raw.kiwi_sha256,
            required_jobs_helper_sha256: raw.required_jobs_helper_sha256,
            frozen_from_git_objects: raw.frozen_from_git_objects,
            readonly_mount: raw.readonly_mount,
            revalidated_after_callback: raw.revalidated_after_callback,
            original_inputs_revalidated: raw.original_inputs_revalidated,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct DifferentialEvidenceIdentity {
    schema_version: String,
    file_name: String,
    size_bytes: u64,
    sha256: String,
    published_atomically: bool,
    verified_after_publish: bool,
}

impl DifferentialEvidenceIdentity {
    fn validate(raw: RawDifferentialEvidenceIdentity) -> Result<Self, OracleError> {
        require_equal(
            "differential_evidence.schema_version",
            &raw.schema_version,
            DIFFERENTIAL_EVIDENCE_SCHEMA,
        )?;
        validate_relative_path("differential_evidence.file_name", &raw.file_name)?;
        if raw.file_name.contains('/') {
            return invalid(
                "differential_evidence.file_name",
                "must be a file name without directory components",
            );
        }
        validate_positive_bound(
            "differential_evidence.size_bytes",
            raw.size_bytes,
            MAX_DIFFERENTIAL_EVIDENCE_BYTES,
        )?;
        require_sha256("differential_evidence.sha256", &raw.sha256)?;
        require_true(
            "differential_evidence.published_atomically",
            raw.published_atomically,
        )?;
        require_true(
            "differential_evidence.verified_after_publish",
            raw.verified_after_publish,
        )?;
        Ok(Self {
            schema_version: raw.schema_version,
            file_name: raw.file_name,
            size_bytes: raw.size_bytes,
            sha256: raw.sha256,
            published_atomically: raw.published_atomically,
            verified_after_publish: raw.verified_after_publish,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CleanupResult {
    redis_process_reaped: bool,
    process_group_reaped: bool,
    runtime_removed: bool,
    checkout_removed: bool,
    logs_removed: bool,
    temp_removed: bool,
    final_identity_revalidated: bool,
    output_parent_revalidated: bool,
    completed_at_utc: String,
}

impl CleanupResult {
    pub fn completed_at_utc(&self) -> &str {
        &self.completed_at_utc
    }

    fn validate(raw: RawCleanupResult) -> Result<Self, OracleError> {
        for (field, value) in [
            ("cleanup.redis_process_reaped", raw.redis_process_reaped),
            ("cleanup.process_group_reaped", raw.process_group_reaped),
            ("cleanup.runtime_removed", raw.runtime_removed),
            ("cleanup.checkout_removed", raw.checkout_removed),
            ("cleanup.logs_removed", raw.logs_removed),
            ("cleanup.temp_removed", raw.temp_removed),
            (
                "cleanup.final_identity_revalidated",
                raw.final_identity_revalidated,
            ),
            (
                "cleanup.output_parent_revalidated",
                raw.output_parent_revalidated,
            ),
        ] {
            require_true(field, value)?;
        }
        parse_timestamp("cleanup.completed_at_utc", &raw.completed_at_utc)?;
        Ok(Self {
            redis_process_reaped: raw.redis_process_reaped,
            process_group_reaped: raw.process_group_reaped,
            runtime_removed: raw.runtime_removed,
            checkout_removed: raw.checkout_removed,
            logs_removed: raw.logs_removed,
            temp_removed: raw.temp_removed,
            final_identity_revalidated: raw.final_identity_revalidated,
            output_parent_revalidated: raw.output_parent_revalidated,
            completed_at_utc: raw.completed_at_utc,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct OracleProvenance {
    schema_version: String,
    primary: BuildEvidence,
    rebuild: BuildEvidence,
    comparison: ArtifactComparison,
    runtime: RuntimeIdentity,
    callback: BoundedCallbackResult,
    callback_input: CallbackInputIdentity,
    differential_evidence: DifferentialEvidenceIdentity,
    cleanup: CleanupResult,
    published_after_cleanup: bool,
    published_at_utc: String,
}

impl OracleProvenance {
    pub fn from_json(input: &str) -> Result<Self, OracleError> {
        check_document_size(input)?;
        let raw: RawOracleProvenance = serde_json::from_str(input)?;
        require_equal("schema_version", &raw.schema_version, PROVENANCE_SCHEMA)?;
        let primary = BuildEvidence::validate(raw.primary)?;
        let rebuild = BuildEvidence::validate(raw.rebuild)?;
        let primary_paths = [
            primary.source.root_path.as_str(),
            primary.source.git_dir_path.as_str(),
        ];
        let rebuild_paths = [
            rebuild.source.root_path.as_str(),
            rebuild.source.git_dir_path.as_str(),
        ];
        if primary_paths.iter().any(|primary_path| {
            rebuild_paths
                .iter()
                .any(|rebuild_path| unix_paths_overlap(primary_path, rebuild_path))
        }) {
            return invalid(
                "rebuild.source",
                "must use a disposable source and Git directory distinct from primary",
            );
        }
        if primary.source.repository != rebuild.source.repository
            || primary.source.tag != rebuild.source.tag
            || primary.source.commit != rebuild.source.commit
            || primary.source.head != rebuild.source.head
            || primary.source.tag_commit != rebuild.source.tag_commit
        {
            return invalid(
                "comparison.source_identity_equal",
                "primary and rebuild source identity must match",
            );
        }
        if primary.recipe != rebuild.recipe {
            return invalid(
                "comparison.recipe_equal",
                "primary and rebuild recipes must match",
            );
        }
        if primary.tools != rebuild.tools {
            return invalid(
                "comparison.toolchain_equal",
                "primary and rebuild tool identities must match",
            );
        }
        if primary.artifacts != rebuild.artifacts {
            return invalid(
                "comparison.manifests_equal",
                "primary and rebuild artifact manifests must match exactly",
            );
        }
        if primary.redis_server.sha256 != rebuild.redis_server.sha256 {
            return invalid(
                "comparison.redis_server_sha256_equal",
                "primary and rebuild redis-server SHA-256 must match",
            );
        }
        let comparison = ArtifactComparison::validate(raw.comparison)?;
        let runtime = RuntimeIdentity::validate(raw.runtime, &rebuild)?;
        let callback = BoundedCallbackResult::validate(raw.callback)?;
        let callback_input = CallbackInputIdentity::validate(raw.callback_input)?;
        let differential_evidence =
            DifferentialEvidenceIdentity::validate(raw.differential_evidence)?;
        let cleanup = CleanupResult::validate(raw.cleanup)?;
        require_true("published_after_cleanup", raw.published_after_cleanup)?;
        let primary_finished =
            parse_timestamp("primary.finished_at_utc", &primary.finished_at_utc)?;
        let rebuild_started = parse_timestamp("rebuild.started_at_utc", &rebuild.started_at_utc)?;
        let rebuild_finished =
            parse_timestamp("rebuild.finished_at_utc", &rebuild.finished_at_utc)?;
        let callback_started =
            parse_timestamp("callback.started_at_utc", &callback.started_at_utc)?;
        let callback_finished =
            parse_timestamp("callback.finished_at_utc", &callback.finished_at_utc)?;
        let cleanup_completed =
            parse_timestamp("cleanup.completed_at_utc", &cleanup.completed_at_utc)?;
        let published = parse_timestamp("published_at_utc", &raw.published_at_utc)?;
        if rebuild_started < primary_finished {
            return invalid(
                "rebuild.started_at_utc",
                "must be at or after primary completion",
            );
        }
        if callback_started < rebuild_finished {
            return invalid(
                "callback.started_at_utc",
                "must be at or after rebuild completion",
            );
        }
        if cleanup_completed < callback_finished {
            return invalid(
                "cleanup.completed_at_utc",
                "must be at or after callback completion",
            );
        }
        if published < cleanup_completed {
            return invalid("published_at_utc", "must be at or after cleanup completion");
        }

        Ok(Self {
            schema_version: raw.schema_version,
            primary,
            rebuild,
            comparison,
            runtime,
            callback,
            callback_input,
            differential_evidence,
            cleanup,
            published_after_cleanup: raw.published_after_cleanup,
            published_at_utc: raw.published_at_utc,
        })
    }

    pub fn schema_version(&self) -> &str {
        &self.schema_version
    }

    pub fn primary(&self) -> &BuildEvidence {
        &self.primary
    }

    pub fn rebuild(&self) -> &BuildEvidence {
        &self.rebuild
    }

    pub fn comparison(&self) -> &ArtifactComparison {
        &self.comparison
    }

    pub fn runtime(&self) -> &RuntimeIdentity {
        &self.runtime
    }

    pub fn callback(&self) -> &BoundedCallbackResult {
        &self.callback
    }

    pub fn callback_input(&self) -> &CallbackInputIdentity {
        &self.callback_input
    }

    pub fn differential_evidence(&self) -> &DifferentialEvidenceIdentity {
        &self.differential_evidence
    }

    pub fn cleanup(&self) -> &CleanupResult {
        &self.cleanup
    }

    pub fn published_at_utc(&self) -> &str {
        &self.published_at_utc
    }

    pub fn verify_external_bindings(
        &self,
        expected_head: &str,
        expected_tree: &str,
        evidence_file_name: &str,
        evidence: &[u8],
    ) -> Result<(), OracleError> {
        require_hex40("external.expected_head", expected_head)?;
        require_hex40("external.expected_tree", expected_tree)?;
        require_equal(
            "callback_input.expected_head",
            &self.callback_input.expected_head,
            expected_head,
        )?;
        require_equal(
            "callback_input.actual_head",
            &self.callback_input.actual_head,
            expected_head,
        )?;
        require_equal(
            "callback_input.tree_oid",
            &self.callback_input.tree_oid,
            expected_tree,
        )?;
        require_equal(
            "differential_evidence.file_name",
            &self.differential_evidence.file_name,
            evidence_file_name,
        )?;
        let size_bytes = u64::try_from(evidence.len()).map_err(|_| {
            validation(
                "differential_evidence.size_bytes",
                "actual evidence length cannot be represented as u64",
            )
        })?;
        if self.differential_evidence.size_bytes != size_bytes {
            return invalid(
                "differential_evidence.size_bytes",
                format!(
                    "must equal actual evidence length {size_bytes}, got {}",
                    self.differential_evidence.size_bytes
                ),
            );
        }
        let sha256 = format!("{:x}", Sha256::digest(evidence));
        require_equal(
            "differential_evidence.sha256",
            &self.differential_evidence.sha256,
            &sha256,
        )
    }
}

fn validate_tools(raw_tools: Vec<RawToolIdentity>) -> Result<Vec<ToolIdentity>, OracleError> {
    if raw_tools.is_empty() || raw_tools.len() > MAX_TOOLS {
        return invalid("tools", format!("must contain 1..={MAX_TOOLS} entries"));
    }
    let mut roles = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut tools = Vec::with_capacity(raw_tools.len());
    for (index, raw) in raw_tools.into_iter().enumerate() {
        let tool = ToolIdentity::validate(raw, index)?;
        if !roles.insert(tool.role.clone()) {
            return invalid(format!("tools[{index}].role"), "must be unique");
        }
        if !paths.insert(tool.path.clone()) {
            return invalid(format!("tools[{index}].path"), "must be unique");
        }
        tools.push(tool);
    }
    for role in REQUIRED_TOOL_ROLES {
        if !roles.contains(role) {
            return invalid("tools", format!("missing required role {role:?}"));
        }
    }
    Ok(tools)
}

fn validate_artifacts(raw: Vec<RawArtifactEntry>) -> Result<Vec<ArtifactEntry>, OracleError> {
    if raw.is_empty() || raw.len() > MAX_ARTIFACTS {
        return invalid(
            "artifacts",
            format!("must contain 1..={MAX_ARTIFACTS} entries"),
        );
    }
    let mut artifacts = Vec::with_capacity(raw.len());
    let mut previous_path: Option<String> = None;
    for (index, entry) in raw.into_iter().enumerate() {
        let entry = match entry {
            RawArtifactEntry::Regular {
                path,
                mode,
                size,
                sha256,
            } => {
                validate_relative_path(&format!("artifacts[{index}].path"), &path)?;
                if mode & FILE_TYPE_MASK != REGULAR_FILE {
                    return invalid(
                        format!("artifacts[{index}].mode"),
                        "must identify a regular file",
                    );
                }
                require_sha256(&format!("artifacts[{index}].sha256"), &sha256)?;
                ArtifactEntry::Regular(RegularArtifact {
                    path,
                    mode,
                    size,
                    sha256,
                })
            }
            RawArtifactEntry::Symlink { path, mode, target } => {
                validate_relative_path(&format!("artifacts[{index}].path"), &path)?;
                if mode & FILE_TYPE_MASK != SYMLINK {
                    return invalid(
                        format!("artifacts[{index}].mode"),
                        "must identify a symlink",
                    );
                }
                validate_symlink_target(&format!("artifacts[{index}].target"), &target)?;
                ArtifactEntry::Symlink(SymlinkArtifact { path, mode, target })
            }
        };
        if let Some(previous) = &previous_path
            && previous.as_str() >= entry.path()
        {
            return invalid(
                format!("artifacts[{index}].path"),
                "must be unique and sorted by source-relative byte order",
            );
        }
        previous_path = Some(entry.path().to_string());
        artifacts.push(entry);
    }
    validate_symlink_graph(&artifacts)?;
    Ok(artifacts)
}

fn validate_symlink_graph(artifacts: &[ArtifactEntry]) -> Result<(), OracleError> {
    let entries = artifacts
        .iter()
        .map(|artifact| (artifact.path(), artifact))
        .collect::<BTreeMap<_, _>>();

    for artifact in artifacts {
        let ArtifactEntry::Symlink(symlink) = artifact else {
            continue;
        };
        let mut current_path = symlink.path.clone();
        let mut visited = BTreeSet::new();
        let mut depth = 0;
        loop {
            if !visited.insert(current_path.clone()) {
                return invalid(
                    format!("artifact {current_path:?}"),
                    "symlink chain must not contain a cycle",
                );
            }
            let Some(entry) = entries.get(current_path.as_str()) else {
                return invalid(
                    format!("artifact {current_path:?}"),
                    "symlink target must exist in the manifest",
                );
            };
            match entry {
                ArtifactEntry::Regular(_) => break,
                ArtifactEntry::Symlink(link) => {
                    depth += 1;
                    if depth > MAX_SYMLINK_DEPTH {
                        return invalid(
                            format!("artifact {:?}", symlink.path),
                            format!("symlink depth must not exceed {MAX_SYMLINK_DEPTH}"),
                        );
                    }
                    current_path = resolve_symlink_target(&link.path, &link.target)?;
                }
            }
        }
    }
    Ok(())
}

fn resolve_symlink_target(path: &str, target: &str) -> Result<String, OracleError> {
    let mut components = path
        .rsplit_once('/')
        .map(|(parent, _)| parent.split('/').collect::<Vec<_>>())
        .unwrap_or_default();
    for component in target.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if components.pop().is_none() {
                    return invalid(
                        format!("artifact {path:?}"),
                        "symlink target must not escape the source root",
                    );
                }
            }
            component => components.push(component),
        }
    }
    if components.is_empty() {
        return invalid(
            format!("artifact {path:?}"),
            "symlink target must resolve to a manifest path",
        );
    }
    Ok(components.join("/"))
}

fn validate_relative_path(field: &str, value: &str) -> Result<(), OracleError> {
    validate_bounded_string(field, value, 1, MAX_PATH_BYTES)?;
    if value.starts_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return invalid(field, "must be a normalized source-relative path");
    }
    Ok(())
}

fn validate_symlink_target(field: &str, value: &str) -> Result<(), OracleError> {
    validate_bounded_string(field, value, 1, MAX_PATH_BYTES)?;
    if value.starts_with('/') || value.contains('\\') {
        return invalid(field, "must be a relative Unix symlink target");
    }
    Ok(())
}

fn validate_absolute_path(field: &str, value: &str) -> Result<(), OracleError> {
    validate_bounded_string(field, value, 1, MAX_PATH_BYTES)?;
    if !value.starts_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .skip(1)
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return invalid(field, "must be a normalized absolute Unix path");
    }
    Ok(())
}

fn validate_string_collection(
    field: &str,
    values: &[String],
    minimum: usize,
    maximum: usize,
) -> Result<(), OracleError> {
    if values.len() < minimum || values.len() > maximum {
        return invalid(field, format!("must contain {minimum}..={maximum} entries"));
    }
    for (index, value) in values.iter().enumerate() {
        validate_bounded_string(&format!("{field}[{index}]"), value, 1, MAX_STRING_BYTES)?;
    }
    Ok(())
}

fn validate_bounded_string(
    field: &str,
    value: &str,
    minimum: usize,
    maximum: usize,
) -> Result<(), OracleError> {
    let length = value.len();
    if length < minimum || length > maximum || value.contains('\0') {
        return invalid(
            field,
            format!("must contain {minimum}..={maximum} non-NUL UTF-8 bytes"),
        );
    }
    Ok(())
}

fn validate_positive_bound(field: &str, value: u64, maximum: u64) -> Result<(), OracleError> {
    if value == 0 || value > maximum {
        return invalid(field, format!("must be in 1..={maximum}"));
    }
    Ok(())
}

fn unix_paths_overlap(left: &str, right: &str) -> bool {
    left == right
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn require_sha256(field: &str, value: &str) -> Result<(), OracleError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid(field, "must be 64 lowercase hexadecimal characters");
    }
    Ok(())
}

fn require_hex40(field: &str, value: &str) -> Result<(), OracleError> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid(field, "must be 40 lowercase hexadecimal characters");
    }
    Ok(())
}

fn parse_timestamp(field: &str, value: &str) -> Result<DateTime<FixedOffset>, OracleError> {
    validate_bounded_string(field, value, 1, 64)?;
    if !value.ends_with('Z') {
        return invalid(field, "must be an RFC3339 UTC timestamp ending in Z");
    }
    DateTime::parse_from_rfc3339(value)
        .map_err(|error| validation(field, format!("must be a valid RFC3339 timestamp: {error}")))
}

fn require_equal(field: &str, actual: &str, expected: &str) -> Result<(), OracleError> {
    if actual != expected {
        return invalid(field, format!("must equal {expected:?}, got {actual:?}"));
    }
    Ok(())
}

fn require_true(field: &str, value: bool) -> Result<(), OracleError> {
    if !value {
        return invalid(field, "must be true");
    }
    Ok(())
}

fn require_false(field: &str, value: bool) -> Result<(), OracleError> {
    if value {
        return invalid(field, "must be false");
    }
    Ok(())
}

fn check_document_size(input: &str) -> Result<(), OracleError> {
    if input.len() > MAX_JSON_BYTES {
        return invalid(
            "document",
            format!("must not exceed {MAX_JSON_BYTES} UTF-8 bytes"),
        );
    }
    Ok(())
}

fn invalid<T>(field: impl Into<String>, message: impl Into<String>) -> Result<T, OracleError> {
    Err(validation(field, message))
}

fn validation(field: impl Into<String>, message: impl Into<String>) -> OracleError {
    OracleError::Validation {
        field: field.into(),
        message: message.into(),
    }
}

#[derive(Debug, Error)]
pub enum OracleError {
    #[error("failed to parse Oracle evidence JSON: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("invalid Oracle evidence at {field}: {message}")]
    Validation { field: String, message: String },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBuildEvidence {
    schema_version: String,
    source: RawSourceIdentity,
    recipe: RawBuildRecipe,
    tools: Vec<RawToolIdentity>,
    artifacts: Vec<RawArtifactEntry>,
    redis_server: RawRedisBinaryIdentity,
    started_at_utc: String,
    finished_at_utc: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSourceIdentity {
    repository: String,
    tag: String,
    commit: String,
    head: String,
    tag_commit: String,
    root_path: String,
    git_dir_path: String,
    tracked_untracked_clean: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBuildRecipe {
    id: String,
    build_tls: String,
    malloc: String,
    debug: String,
    debug_flags: String,
    enable_lto: String,
    opt: String,
    jobs: u16,
    source_date_epoch: u64,
    argv: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawToolIdentity {
    role: String,
    path: String,
    version: String,
    sha256: String,
    identity: RawFileIdentity,
    held_fd: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFileIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    size: u64,
    nlink: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RawArtifactEntry {
    Regular {
        path: String,
        mode: u32,
        size: u64,
        sha256: String,
    },
    Symlink {
        path: String,
        mode: u32,
        target: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRedisBinaryIdentity {
    artifact_path: String,
    path: String,
    sha256: String,
    identity: RawFileIdentity,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArtifactComparison {
    manifests_equal: bool,
    redis_server_sha256_equal: bool,
    source_identity_equal: bool,
    recipe_equal: bool,
    toolchain_equal: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRuntimeIdentity {
    build_role: RawRuntimeBuildRole,
    binary_path: String,
    binary_sha256: String,
    binary_identity: RawFileIdentity,
    held_fd: bool,
    pid: u32,
    info_redis_versions: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RawRuntimeBuildRole {
    Rebuild,
}

impl From<RawRuntimeBuildRole> for RuntimeBuildRole {
    fn from(raw: RawRuntimeBuildRole) -> Self {
        match raw {
            RawRuntimeBuildRole::Rebuild => Self::Rebuild,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBoundedCallbackResult {
    argv: Vec<String>,
    timeout_ms: u64,
    term_grace_ms: u64,
    stdout_limit_bytes: u64,
    stderr_limit_bytes: u64,
    stdout_bytes: u64,
    stderr_bytes: u64,
    started_at_utc: String,
    finished_at_utc: String,
    exit_code: i32,
    timed_out: bool,
    output_truncated: bool,
    process_group_reaped: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCleanupResult {
    redis_process_reaped: bool,
    process_group_reaped: bool,
    runtime_removed: bool,
    checkout_removed: bool,
    logs_removed: bool,
    temp_removed: bool,
    final_identity_revalidated: bool,
    output_parent_revalidated: bool,
    completed_at_utc: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCallbackInputIdentity {
    expected_head: String,
    actual_head: String,
    tree_oid: String,
    ref_context: String,
    input_manifest_sha256: String,
    kiwi_sha256: String,
    required_jobs_helper_sha256: String,
    frozen_from_git_objects: bool,
    readonly_mount: bool,
    revalidated_after_callback: bool,
    original_inputs_revalidated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDifferentialEvidenceIdentity {
    schema_version: String,
    file_name: String,
    size_bytes: u64,
    sha256: String,
    published_atomically: bool,
    verified_after_publish: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOracleProvenance {
    schema_version: String,
    primary: RawBuildEvidence,
    rebuild: RawBuildEvidence,
    comparison: RawArtifactComparison,
    runtime: RawRuntimeIdentity,
    callback: RawBoundedCallbackResult,
    callback_input: RawCallbackInputIdentity,
    differential_evidence: RawDifferentialEvidenceIdentity,
    cleanup: RawCleanupResult,
    published_after_cleanup: bool,
    published_at_utc: String,
}
