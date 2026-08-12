// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::io::Write;

use kiwi_compat::manifest::{Profile, Protocol, RequiredVectorJobs, RequiredVectorRawCase};
use serde::Serialize;

const CANONICAL_SCHEMA: &str = "kiwi-vector-required-jobs/canonical-v1";

#[derive(Serialize)]
struct CanonicalRequiredVectorJobs<'a> {
    schema: &'static str,
    job_id: &'a str,
    test_module: &'a str,
    pytest_marker: &'a str,
    protocols: Vec<&'static str>,
    commands: &'a [String],
    raw_cases: BTreeMap<&'a str, Vec<CanonicalRawCase<'a>>>,
    expected_node_ids: &'a [String],
    expected_item_count: usize,
    manifest_profile: &'static str,
    fast_job: CanonicalFastJob<'a>,
}

#[derive(Serialize)]
struct CanonicalRawCase<'a> {
    case_id: &'a str,
    evidence_kind: &'a str,
    node_ids: &'a [String],
}

impl<'a> From<&'a RequiredVectorRawCase> for CanonicalRawCase<'a> {
    fn from(raw_case: &'a RequiredVectorRawCase) -> Self {
        Self {
            case_id: raw_case.case_id(),
            evidence_kind: raw_case.evidence_kind(),
            node_ids: raw_case.node_ids(),
        }
    }
}

#[derive(Serialize)]
struct CanonicalFastJob<'a> {
    owner: &'a str,
    deselect_marker: &'a str,
}

fn protocol_name(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Resp2 => "resp2",
        Protocol::Resp3 => "resp3",
    }
}

fn profile_name(profile: Profile) -> &'static str {
    match profile {
        Profile::Redis881CoreResp2 => "redis_8_8_1_core_resp2",
        Profile::Redis881CoreResp3 => "redis_8_8_1_core_resp3",
        Profile::Redis881Runtime => "redis_8_8_1_runtime",
        Profile::Redis881ClientEcosystem => "redis_8_8_1_client_ecosystem",
        Profile::Redis881StandaloneCacheOff => "redis_8_8_1_standalone_cache_off",
        Profile::Redis881RaftSingleGroupCacheOff => "redis_8_8_1_raft_single_group_cache_off",
        Profile::KiwiRocksdbAuthorityV1 => "kiwi_rocksdb_authority_v1",
        Profile::KiwiRedisraftPublicV1 => "kiwi_redisraft_public_v1",
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os();
    let program = arguments.next().unwrap_or_default();
    let registry = arguments.next().ok_or_else(|| {
        format!(
            "usage: {} REGISTRY",
            std::path::Path::new(&program)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
        )
    })?;
    if arguments.next().is_some() {
        return Err("required-jobs helper accepts exactly one registry path".into());
    }

    let source = std::fs::read_to_string(registry)?;
    let registry = RequiredVectorJobs::from_yaml(&source)?;
    let raw_cases = registry
        .raw_cases()
        .iter()
        .map(|(command, cases)| {
            (
                command.as_str(),
                cases.iter().map(CanonicalRawCase::from).collect(),
            )
        })
        .collect();
    let canonical = CanonicalRequiredVectorJobs {
        schema: CANONICAL_SCHEMA,
        job_id: registry.job_id(),
        test_module: registry.test_module(),
        pytest_marker: registry.pytest_marker(),
        protocols: registry
            .protocols()
            .iter()
            .copied()
            .map(protocol_name)
            .collect(),
        commands: registry.commands(),
        raw_cases,
        expected_node_ids: registry.expected_node_ids(),
        expected_item_count: registry.expected_item_count(),
        manifest_profile: profile_name(registry.manifest_profile()),
        fast_job: CanonicalFastJob {
            owner: registry.fast_job_owner(),
            deselect_marker: registry.fast_job_deselect_marker(),
        },
    };
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, &canonical)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("required Vector jobs registry: {error}");
        std::process::exit(1);
    }
}
