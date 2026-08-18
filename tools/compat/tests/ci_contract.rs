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

use std::borrow::Cow;
use std::collections::BTreeMap;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::process::Command;

#[cfg(target_os = "linux")]
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::Deserialize;
#[cfg(target_os = "linux")]
use serde_json::{Value, json};
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};

#[cfg(target_os = "linux")]
fn runner_command(runner: &std::path::Path) -> Command {
    let mut command = Command::new("/usr/bin/bash");
    command.arg(runner).env(
        "KIWI_COMPAT_TEST_REQUIRED_JOBS_HELPER",
        env!("CARGO_BIN_EXE_kiwi-required-vector-jobs"),
    );
    command
}

#[cfg(target_os = "linux")]
fn encoded_bytes(payload: &[u8]) -> (String, String) {
    (
        BASE64_STANDARD.encode(payload),
        format!("{:x}", Sha256::digest(payload)),
    )
}

#[cfg(target_os = "linux")]
fn encode_resp_command(command: &str, key: &[u8], arguments: &[&[u8]]) -> Vec<u8> {
    let mut request = format!("*{}\r\n", arguments.len() + 2).into_bytes();
    for part in std::iter::once(command.as_bytes())
        .chain(std::iter::once(key))
        .chain(arguments.iter().copied())
    {
        request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        request.extend_from_slice(part);
        request.extend_from_slice(b"\r\n");
    }
    request
}

#[cfg(target_os = "linux")]
fn final_state_exchange(command: &str, key: &[u8], arguments: &[&[u8]], response: &[u8]) -> Value {
    let request = encode_resp_command(command, key, arguments);
    let (request_base64, request_sha256) = encoded_bytes(&request);
    let (response_base64, response_sha256) = encoded_bytes(response);
    json!({
        "command": command,
        "request_base64": request_base64,
        "request_sha256": request_sha256,
        "kiwi_response_base64": response_base64,
        "kiwi_response_sha256": response_sha256,
        "redis_response_base64": response_base64,
        "redis_response_sha256": response_sha256,
    })
}

#[cfg(target_os = "linux")]
fn resp_integer(value: i64) -> Vec<u8> {
    format!(":{value}\r\n").into_bytes()
}

#[cfg(target_os = "linux")]
fn resp_vector(dimension: usize, protocol: u8) -> Vec<u8> {
    let mut response = format!("*{dimension}\r\n").into_bytes();
    for _ in 0..dimension {
        response.extend_from_slice(if protocol == 2 {
            b"$1\r\n0\r\n"
        } else {
            b",0\r\n"
        });
    }
    response
}

#[cfg(target_os = "linux")]
fn final_state_key_record(
    role: &str,
    key: &[u8],
    key_type: &str,
    protocol: u8,
    dimension: Option<usize>,
    members: &[&[u8]],
    populated_member_count: usize,
) -> Value {
    let mut observations = Vec::new();
    if key_type == "vectorset" {
        let dimension = dimension.expect("vector fixture must declare its dimension");
        assert!(populated_member_count < members.len());
        observations.push(final_state_exchange(
            "VCARD",
            key,
            &[],
            &resp_integer(populated_member_count as i64),
        ));
        observations.push(final_state_exchange(
            "VDIM",
            key,
            &[],
            &resp_integer(dimension as i64),
        ));
        for (index, member) in members.iter().enumerate() {
            let response = if index < populated_member_count {
                resp_vector(dimension, protocol)
            } else if protocol == 2 {
                b"$-1\r\n".to_vec()
            } else {
                b"_\r\n".to_vec()
            };
            observations.push(final_state_exchange("VEMB", key, &[*member], &response));
        }
    } else if key_type == "string" {
        observations.push(final_state_exchange("GET", key, &[], b"$5\r\nvalue\r\n"));
    }

    let exists = key_type != "none";
    let (key_base64, key_sha256) = encoded_bytes(key);
    json!({
        "key_role": role,
        "key_base64": key_base64,
        "key_sha256": key_sha256,
        "before_cleanup": {
            "type": final_state_exchange(
                "TYPE", key, &[], format!("+{key_type}\r\n").as_bytes()
            ),
            "pttl": final_state_exchange(
                "PTTL", key, &[], &resp_integer(if exists { -1 } else { -2 })
            ),
            "observations": observations,
        },
        "cleanup": {
            "first_del": final_state_exchange(
                "DEL", key, &[], &resp_integer(if exists { 1 } else { 0 })
            ),
            "after_type": final_state_exchange("TYPE", key, &[], b"+none\r\n"),
            "after_pttl": final_state_exchange("PTTL", key, &[], &resp_integer(-2)),
            "second_del": final_state_exchange("DEL", key, &[], &resp_integer(0)),
        },
    })
}

#[cfg(target_os = "linux")]
fn final_state_keys(state_profile: &str, protocol: u8) -> Vec<Value> {
    const TYPED_ROLES: &[&str] = &["main", "dense3", "string", "missing"];
    const RAW_ROLES: &[&str] = &[
        "values",
        "fp32",
        "missing-scores",
        "missing-values",
        "missing-fp32",
        "invalid-values",
        "invalid-fp32",
        "repeated",
        "option",
    ];
    const MAIN_MEMBERS: &[&[u8]] = &[
        b"alpha",
        b"beta",
        b"gamma",
        b"delta",
        b"",
        b"\x00bin\x00",
        b"tie-a",
        b"tie-b",
        b"ghost",
    ];
    const DENSE3_MEMBERS: &[&[u8]] = &[b"x", b"y", b"z", b"ghost"];
    const REPEATED_MEMBERS: &[&[u8]] = &[b"element", b"ghost"];

    let typed = state_profile.starts_with("typed-");
    let roles = if typed { TYPED_ROLES } else { RAW_ROLES };
    roles
        .iter()
        .map(|role| {
            let key = if typed {
                format!("test_vdiff:p{protocol}:{role}").into_bytes()
            } else if matches!(*role, "values" | "fp32" | "missing-scores") {
                format!("test_vdiff:raw:p{protocol}:{role}").into_bytes()
            } else {
                format!("test_vdiff:raw:vadd:p{protocol}:{role}").into_bytes()
            };
            let key_type = match (state_profile, *role) {
                ("raw-repeated-vector", "repeated")
                | ("typed-main-vector", "main")
                | ("typed-main-two-member-vector", "main")
                | ("typed-main-dense3-vector", "main")
                | ("typed-main-dense3-vector", "dense3") => "vectorset",
                ("typed-string", "string") => "string",
                _ => "none",
            };
            let (dimension, members): (Option<usize>, &[&[u8]]) = match *role {
                "main" => (Some(4), MAIN_MEMBERS),
                "dense3" => (Some(3), DENSE3_MEMBERS),
                "repeated" => (Some(1), REPEATED_MEMBERS),
                _ => (None, &[]),
            };
            let populated_member_count = match (state_profile, *role) {
                ("raw-repeated-vector", "repeated") => 1,
                ("typed-main-vector", "main") | ("typed-main-dense3-vector", "main") => 8,
                ("typed-main-two-member-vector", "main") => 2,
                ("typed-main-dense3-vector", "dense3") => 3,
                _ => 0,
            };
            final_state_key_record(
                role,
                &key,
                key_type,
                protocol,
                dimension,
                members,
                populated_member_count,
            )
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn replace_exchange_response(exchange: &mut Value, response: &[u8]) {
    let (response_base64, response_sha256) = encoded_bytes(response);
    exchange["kiwi_response_base64"] = json!(response_base64);
    exchange["kiwi_response_sha256"] = json!(response_sha256);
    exchange["redis_response_base64"] = json!(response_base64);
    exchange["redis_response_sha256"] = json!(response_sha256);
}

#[derive(Clone, Deserialize)]
struct Workflow {
    #[serde(rename = "on")]
    triggers: BTreeMap<String, yaml_serde::Value>,
    jobs: BTreeMap<String, Job>,
}

#[derive(Clone, Deserialize)]
struct Job {
    #[serde(rename = "if")]
    condition: Option<yaml_serde::Value>,
    #[serde(rename = "runs-on")]
    runs_on: String,
    #[serde(rename = "continue-on-error")]
    continue_on_error: Option<yaml_serde::Value>,
    #[serde(default)]
    steps: Vec<Step>,
}

#[derive(Clone, Deserialize)]
struct Step {
    #[serde(rename = "if")]
    condition: Option<yaml_serde::Value>,
    uses: Option<String>,
    run: Option<String>,
    #[serde(default)]
    with: StepInputs,
    #[serde(rename = "continue-on-error")]
    continue_on_error: Option<yaml_serde::Value>,
}

#[derive(Clone, Default, Deserialize)]
struct StepInputs {
    name: Option<String>,
    path: Option<String>,
    r#ref: Option<String>,
    timeout_minutes: Option<yaml_serde::Value>,
    #[serde(rename = "retention-days")]
    retention_days: Option<u64>,
    #[serde(rename = "if-no-files-found")]
    if_no_files_found: Option<String>,
}

const VECTOR_CLUSTER_JOB: &str = "vector-cluster-fail-closed";
const TRUSTED_VECTOR_JOB: &str = "trusted-vector-differential";
const TRUSTED_VECTOR_HEAD: &str = "${{ github.event.pull_request.head.sha || github.sha }}";
const TRUSTED_VECTOR_PROVENANCE: &str = "${{ runner.temp }}/kiwi-oracle/oracle-provenance.json";
const TRUSTED_VECTOR_EVIDENCE: &str =
    "${{ runner.temp }}/kiwi-oracle/vector-differential-evidence.json";
const BUILD_AND_TEST_JOB: &str = "build-and-test";
const SANITIZERS_JOB: &str = "sanitizers";
const STATIC_ANALYSIS_JOB: &str = "static-analysis";
const RKYV_TREE_COMMAND: &str =
    "cargo tree --locked --offline --target all --all-features -i rkyv@0.7.46";
const RKYV_SENTINEL_COMMAND: &str = "bash scripts/ci/check-rkyv-reachability.sh";
const GRPCURL_URL: &str = "https://github.com/fullstorydev/grpcurl/releases/download/v1.9.3/grpcurl_1.9.3_linux_x86_64.tar.gz";
const GRPCURL_ARCHIVE_SHA256: &str =
    "a926b62a85787ccf73ef8736b3ae554f1242e39d92bb8767a79d6dd23b11d1d5";
const GRPCURL_OUTPUT: &str = "-o \"$RUNNER_TEMP/grpcurl.tar.gz\"";
const ORACLE_NAMESPACE_PREFLIGHT: &str = "sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0\nunshare --user --map-root-user --mount --pid --fork true";
const GRPCURL_CHECKSUM_VERIFY: &str = "| (cd \"$RUNNER_TEMP\" && sha256sum -c -)";
const GRPCURL_EXTRACT: &str =
    "tar -xzf \"$RUNNER_TEMP/grpcurl.tar.gz\" -C \"$RUNNER_TEMP\" grpcurl";

fn normalized_fixture(source: &str) -> Cow<'_, str> {
    if source.contains("\r\n") {
        Cow::Owned(source.replace("\r\n", "\n"))
    } else {
        Cow::Borrowed(source)
    }
}

fn make_logical_lines(source: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut logical_line = String::new();
    for physical_line in source.lines() {
        let physical_line = physical_line.trim_end_matches('\r');
        let trimmed = physical_line.trim_end();
        let continued = trimmed.ends_with('\\');
        let fragment = if continued {
            trimmed
                .strip_suffix('\\')
                .expect("continued Make line must end with a backslash")
        } else {
            physical_line
        };
        if !logical_line.is_empty() {
            logical_line.push(' ');
        }
        logical_line.push_str(fragment);
        if !continued {
            lines.push(std::mem::take(&mut logical_line));
        }
    }
    if !logical_line.is_empty() {
        lines.push(logical_line);
    }
    lines
}

fn is_make_variable_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

fn make_assignment(line: &str) -> Option<(&str, &str, &str)> {
    if line.starts_with('\t') {
        return None;
    }
    let line = line.trim_start();
    for operator in [":=", "?=", "+=", "="] {
        let Some(index) = line.find(operator) else {
            continue;
        };
        let name = line[..index].trim();
        if is_make_variable_name(name) {
            return Some((name, operator, line[index + operator.len()..].trim()));
        }
    }
    None
}

enum MakeVariable {
    Recursive(String),
    Simple(String),
}

impl MakeVariable {
    fn value(&self) -> &str {
        match self {
            Self::Recursive(value) | Self::Simple(value) => value,
        }
    }
}

fn remove_undefined_make_variables(
    source: &str,
    variables: &BTreeMap<String, MakeVariable>,
) -> String {
    let bytes = source.as_bytes();
    let mut undefined = Vec::<String>::new();
    let mut index = 0;
    while index + 2 < bytes.len() {
        if bytes[index] != b'$' || !matches!(bytes[index + 1], b'(' | b'{') {
            index += 1;
            continue;
        }
        let closing = if bytes[index + 1] == b'(' { b')' } else { b'}' };
        let name_start = index + 2;
        let Some(relative_end) = bytes[name_start..].iter().position(|byte| *byte == closing)
        else {
            index += 1;
            continue;
        };
        let name_end = name_start + relative_end;
        if let Ok(name) = std::str::from_utf8(&bytes[name_start..name_end])
            && is_make_variable_name(name)
            && !variables.contains_key(name)
            && !undefined.iter().any(|candidate| candidate == name)
        {
            undefined.push(name.to_string());
        }
        index = name_end + 1;
    }

    let mut expanded = source.to_string();
    for name in undefined {
        expanded = expanded.replace(&format!("$({name})"), "");
        expanded = expanded.replace(&format!("${{{name}}}"), "");
    }
    expanded
}

fn expand_make_variables(source: &str, variables: &BTreeMap<String, MakeVariable>) -> String {
    let mut expanded = source.to_string();
    for _ in 0..=variables.len().min(32) {
        let mut next = expanded.clone();
        for (name, variable) in variables {
            let value = variable.value();
            next = next.replace(&format!("$({name})"), value);
            next = next.replace(&format!("${{{name}}}"), value);
        }
        if next == expanded {
            break;
        }
        expanded = next;
    }
    remove_undefined_make_variables(&expanded, variables)
}

fn has_vector_differential_path_ignore(source: &str) -> bool {
    let logical_lines = make_logical_lines(source);
    let mut variables = BTreeMap::<String, MakeVariable>::new();
    for line in &logical_lines {
        let Some((name, operator, value)) = make_assignment(line) else {
            continue;
        };
        match operator {
            "?=" if variables.contains_key(name) => {}
            ":=" => {
                variables.insert(
                    name.to_string(),
                    MakeVariable::Simple(expand_make_variables(value, &variables)),
                );
            }
            "+=" => {
                let value = if matches!(variables.get(name), Some(MakeVariable::Simple(_))) {
                    expand_make_variables(value, &variables)
                } else {
                    value.to_string()
                };
                if let Some(variable) = variables.get_mut(name) {
                    let current = match variable {
                        MakeVariable::Recursive(current) | MakeVariable::Simple(current) => current,
                    };
                    if !current.is_empty() && !value.is_empty() {
                        current.push(' ');
                    }
                    current.push_str(&value);
                } else {
                    variables.insert(name.to_string(), MakeVariable::Recursive(value));
                }
            }
            "=" | "?=" => {
                variables.insert(name.to_string(), MakeVariable::Recursive(value.to_string()));
            }
            _ => unreachable!("Make assignment parser returned an unknown operator"),
        }
    }

    logical_lines
        .iter()
        .filter(|line| make_assignment(line).is_none())
        .map(|line| expand_make_variables(line, &variables).to_ascii_lowercase())
        .any(|line| {
            (line.contains("--ignore") || line.contains("--ignore-glob"))
                && line.contains("vector_set_differential.py")
        })
}

fn validate_required_job_action_versions(workflow: &Workflow) -> Result<(), String> {
    for job_id in [TRUSTED_VECTOR_JOB, VECTOR_CLUSTER_JOB, STATIC_ANALYSIS_JOB] {
        let job = workflow
            .jobs
            .get(job_id)
            .ok_or_else(|| format!("required job {job_id} is missing"))?;
        for action in job.steps.iter().filter_map(|step| step.uses.as_deref()) {
            let (_, version) = action
                .rsplit_once('@')
                .ok_or_else(|| format!("{job_id} action is unversioned: {action}"))?;
            let versioned_tag = version.strip_prefix('v').is_some_and(|digits| {
                !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
            });
            let commit_sha =
                version.len() == 40 && version.bytes().all(|byte| byte.is_ascii_hexdigit());
            if !versioned_tag && !commit_sha {
                return Err(format!("{job_id} action is not versioned: {action}"));
            }
        }
    }
    Ok(())
}

fn validate_vector_differential_workflow(workflow: &Workflow) -> Result<(), String> {
    let job = workflow
        .jobs
        .get(TRUSTED_VECTOR_JOB)
        .ok_or_else(|| format!("required job {TRUSTED_VECTOR_JOB} is missing"))?;
    if job.runs_on != "ubuntu-latest" {
        return Err(format!("{TRUSTED_VECTOR_JOB} must run on ubuntu-latest"));
    }
    if job.condition.is_some() {
        return Err(format!("{TRUSTED_VECTOR_JOB} cannot be conditional"));
    }
    if job.continue_on_error.is_some()
        || job
            .steps
            .iter()
            .any(|step| step.continue_on_error.is_some())
    {
        return Err(format!("{TRUSTED_VECTOR_JOB} cannot continue on error"));
    }

    let checkout = find_only_step(job, "exact-head checkout", |step| {
        step.uses.as_deref() == Some("actions/checkout@v7")
    })?;
    if job.steps[checkout].with.r#ref.as_deref() != Some(TRUSTED_VECTOR_HEAD) {
        return Err(format!(
            "{TRUSTED_VECTOR_JOB} checkout must bind the pull-request Head or push SHA"
        ));
    }
    let namespace_preflight = find_only_step(job, "Oracle namespace preflight", |step| {
        step.run
            .as_deref()
            .is_some_and(|command| command.trim_end() == ORACLE_NAMESPACE_PREFLIGHT)
    })?;
    let runner = find_only_step(job, "verifier-supervised differential runner", |step| {
        step.run.as_deref().is_some_and(|command| {
            [
                "KIWI_COMPAT_REQUIRE_ORACLE=1",
                "KIWI_REDIS_ORACLE_SOURCE=\"$RUNNER_TEMP/kiwi-oracle/redis-source\"",
                "KIWI_REDIS_ORACLE_PRIMARY_METADATA=\"$RUNNER_TEMP/kiwi-oracle/primary-build.json\"",
                "KIWI_REDIS_ORACLE_OUTPUT=\"$RUNNER_TEMP/kiwi-oracle/oracle-provenance.json\"",
                "KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT=\"$RUNNER_TEMP/kiwi-oracle/vector-differential-evidence.json\"",
                "KIWI_EXPECTED_HEAD=\"${{ github.event.pull_request.head.sha || github.sha }}\"",
                "bash scripts/compat/run-vector-differential.sh",
            ]
            .iter()
            .all(|required| command.contains(required))
                && !command.contains("redis-cli")
                && !command.contains(" PING")
        })
    })?;
    let upload = find_only_step(job, "post-cleanup provenance upload", |step| {
        step.uses
            .as_deref()
            .is_some_and(|action| action.starts_with("actions/upload-artifact@"))
    })?;
    let upload_step = &job.steps[upload];
    let upload_paths = upload_step
        .with
        .path
        .as_deref()
        .map(|paths| {
            paths
                .lines()
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if upload_step.uses.as_deref() != Some("actions/upload-artifact@v7")
        || upload_step.with.name.as_deref() != Some("trusted-vector-oracle-evidence")
        || upload_paths != [TRUSTED_VECTOR_PROVENANCE, TRUSTED_VECTOR_EVIDENCE]
        || upload_step.with.if_no_files_found.as_deref() != Some("error")
        || upload_step.with.retention_days != Some(7)
    {
        return Err(format!(
            "{TRUSTED_VECTOR_JOB} may upload only the final provenance and evidence files"
        ));
    }
    if checkout >= namespace_preflight || namespace_preflight >= runner || runner >= upload {
        return Err(format!(
            "{TRUSTED_VECTOR_JOB} must checkout exact Head, run the verifier, then upload final evidence"
        ));
    }
    if job.steps[checkout].condition.is_some()
        || job.steps[namespace_preflight].condition.is_some()
        || job.steps[runner].condition.is_some()
        || job.steps[upload].condition.is_some()
    {
        return Err(format!(
            "{TRUSTED_VECTOR_JOB} namespace preflight, runner, and upload steps cannot be conditional"
        ));
    }
    Ok(())
}

fn validate_build_and_test_oracle_namespace_preflight(workflow: &Workflow) -> Result<(), String> {
    let job = workflow
        .jobs
        .get(BUILD_AND_TEST_JOB)
        .ok_or_else(|| format!("required job {BUILD_AND_TEST_JOB} is missing"))?;
    let namespace_preflight = find_only_step(job, "Oracle namespace preflight", |step| {
        step.run
            .as_deref()
            .is_some_and(|command| command.trim_end() == ORACLE_NAMESPACE_PREFLIGHT)
    })?;
    if job.steps[namespace_preflight].condition
        != Some(yaml_serde::Value::String(
            "matrix.os == 'ubuntu-latest'".to_string(),
        ))
    {
        return Err(format!(
            "{BUILD_AND_TEST_JOB} Oracle namespace preflight must run only on Ubuntu"
        ));
    }
    let test = find_only_step(job, "workspace test runner", |step| {
        step.uses.as_deref() == Some("nick-fields/retry@v4")
    })?;
    if job.steps[test].with.timeout_minutes
        != Some(yaml_serde::Value::String(
            "${{ matrix.os == 'windows-latest' && 45 || 30 }}".to_string(),
        ))
    {
        return Err(format!(
            "{BUILD_AND_TEST_JOB} workspace tests must give only Windows the extended timeout"
        ));
    }
    if namespace_preflight >= test {
        return Err(format!(
            "{BUILD_AND_TEST_JOB} Oracle namespace preflight must run before workspace tests"
        ));
    }
    Ok(())
}

fn validate_sanitizer_oracle_namespace_preflight(workflow: &Workflow) -> Result<(), String> {
    let job = workflow
        .jobs
        .get(SANITIZERS_JOB)
        .ok_or_else(|| format!("required job {SANITIZERS_JOB} is missing"))?;
    if job.runs_on != "ubuntu-latest" {
        return Err(format!("{SANITIZERS_JOB} must run on ubuntu-latest"));
    }
    let namespace_preflight = find_only_step(job, "Oracle namespace preflight", |step| {
        step.run
            .as_deref()
            .is_some_and(|command| command.trim_end() == ORACLE_NAMESPACE_PREFLIGHT)
    })?;
    if job.steps[namespace_preflight].condition.is_some() {
        return Err(format!(
            "{SANITIZERS_JOB} Oracle namespace preflight cannot be conditional"
        ));
    }
    let test = find_only_step(job, "sanitizer test runner", |step| {
        step.run
            .as_deref()
            .is_some_and(|command| command.contains("cargo +${SANITIZER_TOOLCHAIN} test"))
    })?;
    if namespace_preflight >= test {
        return Err(format!(
            "{SANITIZERS_JOB} Oracle namespace preflight must run before sanitizer tests"
        ));
    }
    Ok(())
}

fn validate_vector_differential_runner_source(source: &str) -> Result<(), String> {
    let endpoint_guard = "[[ -z ${KIWI_REDIS_ORACLE_HOST:-} ]] \\\n    || die 'Oracle endpoint variables are only accepted inside the verifier callback'";
    if source.matches(endpoint_guard).count() != 1 {
        return Err("outer differential runner must reject ambient Oracle endpoints".to_string());
    }
    let verifier = "scripts/compat/verify-redis-8.8.1.sh \\\n    --source \"$KIWI_REDIS_ORACLE_SOURCE\" \\\n    --primary-metadata \"$KIWI_REDIS_ORACLE_PRIMARY_METADATA\" \\\n    --output \"$KIWI_REDIS_ORACLE_OUTPUT\" \\\n    --evidence-output \"$KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT\" \\\n    --expected-head \"$KIWI_EXPECTED_HEAD\" \\\n    --publication-verifier \"$repository_root/target/debug/kiwi-verify-oracle-evidence\" \\\n    --callback-input \"$repository_root\" \\\n    --run-after-ready /bin/bash \\\n    /callback-input/scripts/compat/run-vector-differential.sh --callback";
    if source.matches(verifier).count() != 1 {
        return Err(
            "differential runner must obtain the rebuild runtime from the verifier supervisor"
                .to_string(),
        );
    }
    let outer = source
        .split_once("if [[ ${1:-} == --callback ]]; then\n    callback_main")
        .map(|(_, outer)| outer)
        .ok_or_else(|| "differential callback dispatch is missing".to_string())?;
    if outer.contains("redis-cli") || outer.contains(" PING") {
        return Err(
            "outer differential runner cannot probe an arbitrary Oracle endpoint".to_string(),
        );
    }
    for required in [
        "verifier did not publish Oracle provenance after cleanup",
        "verifier did not publish differential evidence before provenance",
        "--bin kiwi-verify-oracle-evidence",
        "target/debug/kiwi-verify-oracle-evidence",
    ] {
        if !outer.contains(required) {
            return Err(format!(
                "differential runner is missing post-cleanup proof: {required}"
            ));
        }
    }
    if outer
        .matches("target/debug/kiwi-verify-oracle-evidence")
        .count()
        != 1
        || outer.contains("/usr/bin/python3 -I -B - \"$KIWI_REDIS_ORACLE_OUTPUT\"")
    {
        return Err(
            "publication binding must run inside the controller transaction exactly once"
                .to_string(),
        );
    }
    let scratch_cleanup = "/usr/bin/rmdir -- /work/home /work/tmp || cleanup_status=$?";
    let scratch_cleanup_lines = source
        .lines()
        .enumerate()
        .filter_map(|(index, line)| (line.trim() == scratch_cleanup).then_some(index))
        .collect::<Vec<_>>();
    let cleanup_evidence_index = source
        .lines()
        .position(|line| line.trim() == "callback_stage=cleanup-evidence")
        .ok_or_else(|| "callback cleanup-evidence stage is missing".to_string())?;
    if scratch_cleanup_lines.len() != 1 || scratch_cleanup_lines[0] >= cleanup_evidence_index {
        return Err(
            "callback must remove empty controller scratch directories before cleanup evidence"
                .to_string(),
        );
    }
    Ok(())
}

fn find_only_step(
    job: &Job,
    description: &str,
    predicate: impl Fn(&Step) -> bool,
) -> Result<usize, String> {
    let matches = job
        .steps
        .iter()
        .enumerate()
        .filter_map(|(index, step)| predicate(step).then_some(index))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(format!(
            "{VECTOR_CLUSTER_JOB} must contain exactly one {description} step, got {matches:?}"
        ));
    }
    Ok(matches[0])
}

fn is_pinned_grpcurl_step(step: &Step) -> bool {
    step.run.as_deref().is_some_and(|command| {
        let positions = [
            command.find(GRPCURL_URL),
            command.find(GRPCURL_OUTPUT),
            command.find(GRPCURL_ARCHIVE_SHA256),
            command.find(GRPCURL_CHECKSUM_VERIFY),
            command.find(GRPCURL_EXTRACT),
        ];
        positions.iter().all(Option::is_some)
            && positions
                .windows(2)
                .all(|pair| pair[0].expect("checked") < pair[1].expect("checked"))
            && command.matches(GRPCURL_CHECKSUM_VERIFY).count() == 1
            && command.matches(GRPCURL_EXTRACT).count() == 1
    })
}

fn mutate_grpcurl_command(workflow: &mut Workflow, mutate: impl FnOnce(&str) -> String) {
    let step = workflow
        .jobs
        .get_mut(VECTOR_CLUSTER_JOB)
        .expect("required Vector cluster job must exist")
        .steps
        .iter_mut()
        .find(|step| {
            step.run
                .as_deref()
                .is_some_and(|command| command.contains(GRPCURL_URL))
        })
        .expect("grpcurl preparation step must exist");
    let command = step
        .run
        .as_deref()
        .expect("grpcurl step must have a command");
    step.run = Some(mutate(command));
}

fn validate_vector_cluster_workflow(workflow: &Workflow) -> Result<(), String> {
    let job = workflow
        .jobs
        .get(VECTOR_CLUSTER_JOB)
        .ok_or_else(|| format!("required job {VECTOR_CLUSTER_JOB} is missing"))?;
    if job.runs_on != "ubuntu-latest" {
        return Err(format!("{VECTOR_CLUSTER_JOB} must run on ubuntu-latest"));
    }
    if job.condition.is_some() {
        return Err(format!("{VECTOR_CLUSTER_JOB} cannot be conditional"));
    }
    if job.continue_on_error.is_some()
        || job
            .steps
            .iter()
            .any(|step| step.continue_on_error.is_some())
    {
        return Err(format!("{VECTOR_CLUSTER_JOB} cannot continue on error"));
    }

    let checkout = find_only_step(job, "checkout", |step| {
        step.uses.as_deref() == Some("actions/checkout@v7")
    })?;
    if job.steps[checkout].with.r#ref.is_some() {
        return Err(format!(
            "{VECTOR_CLUSTER_JOB} checkout must build the triggering current Head"
        ));
    }
    let build = find_only_step(job, "current-Head Kiwi build", |step| {
        step.run
            .as_deref()
            .is_some_and(|command| command.trim() == "cargo build --locked --bin kiwi")
    })?;
    let grpcurl = find_only_step(job, "pinned grpcurl preparation", is_pinned_grpcurl_step)?;
    let runner = find_only_step(job, "required cluster runner", |step| {
        step.run.as_deref().is_some_and(|command| {
            [
                "KIWI_RUN_CLUSTER_TESTS=1",
                "KIWI_BINARY=\"$GITHUB_WORKSPACE/target/debug/kiwi\"",
                "KIWI_GRPCURL=\"$RUNNER_TEMP/grpcurl\"",
                "bash scripts/ci/run-vector-cluster.sh",
            ]
            .iter()
            .all(|required| command.contains(required))
        })
    })?;
    for (description, index) in [
        ("current-Head Kiwi build", build),
        ("pinned grpcurl preparation", grpcurl),
        ("required cluster runner", runner),
    ] {
        if job.steps[index].condition.is_some() {
            return Err(format!(
                "{VECTOR_CLUSTER_JOB} {description} step cannot be conditional"
            ));
        }
    }
    if !(checkout < build && build < grpcurl && grpcurl < runner) {
        return Err(format!(
            "{VECTOR_CLUSTER_JOB} steps must be ordered checkout < build < grpcurl < runner, got {checkout} < {build} < {grpcurl} < {runner}"
        ));
    }
    Ok(())
}

fn find_only_static_analysis_step(
    job: &Job,
    description: &str,
    predicate: impl Fn(&Step) -> bool,
) -> Result<usize, String> {
    let matches = job
        .steps
        .iter()
        .enumerate()
        .filter_map(|(index, step)| predicate(step).then_some(index))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(format!(
            "{STATIC_ANALYSIS_JOB} must contain exactly one {description} step, got {matches:?}"
        ));
    }
    Ok(matches[0])
}

fn validate_rkyv_static_analysis_workflow(workflow: &Workflow) -> Result<(), String> {
    if !workflow.triggers.contains_key("pull_request") {
        return Err("CI workflow must run the static-analysis gate for pull requests".to_string());
    }
    let job = workflow
        .jobs
        .get(STATIC_ANALYSIS_JOB)
        .ok_or_else(|| format!("required job {STATIC_ANALYSIS_JOB} is missing"))?;
    if job.condition.is_some() {
        return Err(format!("{STATIC_ANALYSIS_JOB} cannot be conditional"));
    }
    if job.runs_on != "ubuntu-latest" {
        return Err(format!("{STATIC_ANALYSIS_JOB} must run on ubuntu-latest"));
    }
    if job.continue_on_error.is_some()
        || job
            .steps
            .iter()
            .any(|step| step.continue_on_error.is_some())
    {
        return Err(format!("{STATIC_ANALYSIS_JOB} cannot continue on error"));
    }

    let fetch = find_only_static_analysis_step(job, "locked dependency fetch", |step| {
        step.run
            .as_deref()
            .is_some_and(|command| command.trim() == "cargo fetch --locked")
    })?;
    let sentinel = find_only_static_analysis_step(job, "rkyv reachability sentinel", |step| {
        step.run
            .as_deref()
            .is_some_and(|command| command.trim() == RKYV_SENTINEL_COMMAND)
    })?;
    let audit = find_only_static_analysis_step(job, "cargo audit", |step| {
        step.run
            .as_deref()
            .is_some_and(|command| command.trim() == "cargo audit")
    })?;
    for (description, index) in [
        ("locked dependency fetch", fetch),
        ("rkyv reachability sentinel", sentinel),
        ("cargo audit", audit),
    ] {
        if job.steps[index].condition.is_some() {
            return Err(format!(
                "{STATIC_ANALYSIS_JOB} {description} step cannot be conditional"
            ));
        }
    }
    if !(fetch < sentinel && sentinel < audit) {
        return Err(format!(
            "{STATIC_ANALYSIS_JOB} steps must be ordered fetch < sentinel < audit, got {fetch} < {sentinel} < {audit}"
        ));
    }
    Ok(())
}

fn validate_rkyv_sentinel_source(source: &str) -> Result<(), String> {
    if source.matches(RKYV_TREE_COMMAND).count() != 1 {
        return Err("sentinel must invoke the exact rkyv cargo tree command once".to_string());
    }
    let command_line = source
        .lines()
        .find(|line| line.contains(RKYV_TREE_COMMAND))
        .ok_or_else(|| "sentinel cargo tree command is missing".to_string())?;
    if !command_line.trim_start().starts_with("if ! cargo tree ") {
        return Err("sentinel must fail when cargo tree fails".to_string());
    }
    if !command_line.contains(">\"$stdout_file\"")
        || command_line.contains("2>&1")
        || command_line.contains("&>")
    {
        return Err(
            "sentinel must capture stdout without treating stderr as dependency output".to_string(),
        );
    }
    let stdout_check = "if [[ -s \"$stdout_file\" ]]; then";
    let command_position = source
        .find(RKYV_TREE_COMMAND)
        .expect("exact command presence checked");
    let stdout_position = source
        .find(stdout_check)
        .ok_or_else(|| "sentinel must fail on non-empty cargo tree stdout".to_string())?;
    if command_position >= stdout_position {
        return Err("sentinel must inspect stdout after cargo tree completes".to_string());
    }
    Ok(())
}

fn validate_rkyv_audit_governance(source: &str) -> Result<(), String> {
    let advisory_ignore_count = source
        .lines()
        .filter(|line| line.trim() == "\"RUSTSEC-2026-0235\",")
        .count();
    if advisory_ignore_count != 1 {
        return Err(format!(
            "audit governance must ignore RUSTSEC-2026-0235 exactly once, got {advisory_ignore_count}"
        ));
    }
    for required in [
        "owner: WP8 / Issue #421",
        "potential_path: openraft -> byte-unit -> rust_decimal",
        "current_status: unreachable optional dependency",
        "remove_when:",
    ] {
        if !source.contains(required) {
            return Err(format!("audit governance is missing {required}"));
        }
    }
    if source.contains("Raft wire serialization") {
        return Err("audit governance falsely claims current Raft wire usage".to_string());
    }
    Ok(())
}

#[test]
fn vector_cluster_required_job_is_unique_and_fail_closed() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    assert_eq!(
        workflow_source
            .lines()
            .filter(|line| *line == "  vector-cluster-fail-closed:")
            .count(),
        1
    );
    assert_eq!(
        workflow_source
            .matches("scripts/ci/run-vector-cluster.sh")
            .count(),
        1
    );

    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    validate_vector_cluster_workflow(&workflow).expect("required Vector cluster job must be exact");

    for (name, from, to) in [
        (
            "job condition",
            "  vector-cluster-fail-closed:\n    name: Vector cluster fail-closed",
            "  vector-cluster-fail-closed:\n    if: false\n    name: Vector cluster fail-closed",
        ),
        (
            "runner condition",
            "      - name: Run required three-node Vector cluster gate\n        run: |",
            "      - name: Run required three-node Vector cluster gate\n        if: false\n        run: |",
        ),
    ] {
        let mutant = workflow_source.replacen(from, to, 1);
        assert_ne!(
            mutant,
            workflow_source.as_ref(),
            "failed to construct {name} mutant"
        );
        let workflow: Workflow =
            yaml_serde::from_str(&mutant).expect("cluster condition mutant must parse");
        assert!(
            validate_vector_cluster_workflow(&workflow).is_err(),
            "cluster workflow accepted {name} mutant"
        );
    }
}

#[test]
fn rkyv_static_analysis_gate_is_pr_blocking_and_ordered() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    assert_eq!(
        workflow_source
            .lines()
            .filter(|line| *line == "  static-analysis:")
            .count(),
        1
    );
    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    validate_rkyv_static_analysis_workflow(&workflow)
        .expect("static analysis must contain the required fail-closed rkyv gate");
}

#[test]
fn rkyv_static_analysis_contract_rejects_removal_reordering_and_continue_on_error() {
    let source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let workflow: Workflow = yaml_serde::from_str(&source).expect("CI workflow must parse");

    let mut no_pull_request = workflow.clone();
    no_pull_request.triggers.remove("pull_request");

    let mut removed = workflow.clone();
    removed
        .jobs
        .get_mut(STATIC_ANALYSIS_JOB)
        .expect("static analysis job must exist")
        .steps
        .retain(|step| {
            !step
                .run
                .as_deref()
                .is_some_and(|command| command.trim() == RKYV_SENTINEL_COMMAND)
        });
    assert!(validate_rkyv_static_analysis_workflow(&removed).is_err());

    let mut reordered = workflow.clone();
    let job = reordered
        .jobs
        .get_mut(STATIC_ANALYSIS_JOB)
        .expect("static analysis job must exist");
    let fetch = job
        .steps
        .iter()
        .position(|step| {
            step.run
                .as_deref()
                .is_some_and(|command| command.trim() == "cargo fetch --locked")
        })
        .expect("locked dependency fetch must exist");
    let sentinel = job
        .steps
        .iter()
        .position(|step| {
            step.run
                .as_deref()
                .is_some_and(|command| command.trim() == RKYV_SENTINEL_COMMAND)
        })
        .expect("rkyv sentinel must exist");
    job.steps.swap(fetch, sentinel);

    let mut job_continued = workflow.clone();
    job_continued
        .jobs
        .get_mut(STATIC_ANALYSIS_JOB)
        .expect("static analysis job must exist")
        .continue_on_error = Some(yaml_serde::Value::Bool(true));

    let mut step_continued = workflow;
    step_continued
        .jobs
        .get_mut(STATIC_ANALYSIS_JOB)
        .expect("static analysis job must exist")
        .steps
        .iter_mut()
        .find(|step| {
            step.run
                .as_deref()
                .is_some_and(|command| command.trim() == RKYV_SENTINEL_COMMAND)
        })
        .expect("rkyv sentinel must exist")
        .continue_on_error = Some(yaml_serde::Value::Bool(true));

    for (name, mutant) in [
        ("removed pull-request trigger", no_pull_request),
        ("removed sentinel", removed),
        ("reordered sentinel", reordered),
        ("job continue-on-error", job_continued),
        ("step continue-on-error", step_continued),
    ] {
        assert!(
            validate_rkyv_static_analysis_workflow(&mutant).is_err(),
            "static-analysis accepted {name} mutant"
        );
    }
}

#[test]
fn rkyv_static_analysis_contract_rejects_conditional_job_and_critical_steps() {
    let source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let mut accepted = Vec::new();
    for (name, needle, replacement) in [
        (
            "static-analysis job",
            "  static-analysis:\n    name: Static Analysis",
            "  static-analysis:\n    if: false\n    name: Static Analysis",
        ),
        (
            "locked dependency fetch",
            "      - name: Fetch locked dependencies\n        run: cargo fetch --locked",
            "      - name: Fetch locked dependencies\n        if: false\n        run: cargo fetch --locked",
        ),
        (
            "rkyv reachability sentinel",
            "      - name: Verify rkyv advisory remains unreachable\n        run: bash scripts/ci/check-rkyv-reachability.sh",
            "      - name: Verify rkyv advisory remains unreachable\n        if: false\n        run: bash scripts/ci/check-rkyv-reachability.sh",
        ),
        (
            "cargo audit",
            "      - name: Security audit\n        run: cargo audit",
            "      - name: Security audit\n        if: false\n        run: cargo audit",
        ),
    ] {
        let mutant = source.replacen(needle, replacement, 1);
        assert_ne!(
            mutant, source,
            "failed to construct {name} conditional mutant"
        );
        let workflow: Workflow =
            yaml_serde::from_str(&mutant).expect("conditional CI workflow mutant must parse");
        if validate_rkyv_static_analysis_workflow(&workflow).is_ok() {
            accepted.push(name);
        }
    }
    assert!(
        accepted.is_empty(),
        "static-analysis accepted conditional mutants: {accepted:?}"
    );

    let unrelated_condition = source.replacen(
        "  static-analysis:\n    name: Static Analysis\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v7\n\n      - name: Setup toolchain\n        uses: actions-rust-lang/setup-rust-toolchain@v1",
        "  static-analysis:\n    name: Static Analysis\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v7\n\n      - name: Setup toolchain\n        if: false\n        uses: actions-rust-lang/setup-rust-toolchain@v1",
        1,
    );
    assert_ne!(
        unrelated_condition, source,
        "failed to construct unrelated conditional step mutant"
    );
    let workflow: Workflow = yaml_serde::from_str(&unrelated_condition)
        .expect("unrelated conditional step workflow mutant must parse");
    validate_rkyv_static_analysis_workflow(&workflow)
        .expect("unrelated static-analysis step conditions remain outside this contract");
}

#[test]
fn rkyv_sentinel_contract_rejects_ignored_stdout() {
    let source = normalized_fixture(include_str!(
        "../../../scripts/ci/check-rkyv-reachability.sh"
    ));
    validate_rkyv_sentinel_source(&source).expect("rkyv sentinel source must be fail closed");

    let ignored_stdout = source.replacen(
        "if [[ -s \"$stdout_file\" ]]; then",
        "if [[ ! -s \"$stdout_file\" ]]; then",
        1,
    );
    assert!(validate_rkyv_sentinel_source(&ignored_stdout).is_err());
}

#[test]
fn rkyv_audit_ignore_has_owner_path_status_and_removal_condition() {
    let source = normalized_fixture(include_str!("../../../.cargo/audit.toml"));
    validate_rkyv_audit_governance(&source)
        .expect("rkyv advisory ignore must carry accurate governance");

    let removed_advisory = source.replacen("  \"RUSTSEC-2026-0235\",\n", "", 1);
    assert!(
        validate_rkyv_audit_governance(&removed_advisory).is_err(),
        "audit governance accepted removal of the governed advisory ignore"
    );
}

#[test]
fn rkyv_security_workflow_remains_scheduled_visibility() {
    let source = normalized_fixture(include_str!("../../../.github/workflows/security.yml"));
    assert!(source.contains("  schedule:"));
    let workflow: Workflow = yaml_serde::from_str(&source).expect("security workflow must parse");
    let job = workflow
        .jobs
        .get("cargo-audit")
        .expect("scheduled cargo audit visibility job must exist");
    assert_eq!(job.runs_on, "ubuntu-latest");
    assert_eq!(
        job.continue_on_error,
        Some(yaml_serde::Value::Bool(true)),
        "scheduled visibility is not the PR-blocking static-analysis gate"
    );
}

#[test]
fn vector_cluster_workflow_rejects_grpcurl_moved_to_unrelated_job() {
    let source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let mut workflow: Workflow = yaml_serde::from_str(&source).expect("CI workflow must parse");
    let grpcurl_step = {
        let required_job = workflow
            .jobs
            .get_mut(VECTOR_CLUSTER_JOB)
            .expect("required Vector cluster job must exist");
        let index = required_job
            .steps
            .iter()
            .position(is_pinned_grpcurl_step)
            .expect("pinned grpcurl step must exist");
        required_job.steps.remove(index)
    };
    workflow
        .jobs
        .get_mut("sanitizers")
        .expect("unrelated job must exist")
        .steps
        .push(grpcurl_step);

    let error = validate_vector_cluster_workflow(&workflow)
        .expect_err("moving grpcurl preparation to another job must fail closed");
    assert!(error.contains("pinned grpcurl preparation"), "{error}");
}

#[test]
fn vector_cluster_workflow_rejects_grpcurl_extraction_before_checksum() {
    let source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let mut workflow: Workflow = yaml_serde::from_str(&source).expect("CI workflow must parse");
    mutate_grpcurl_command(&mut workflow, |command| {
        let without_extract = command.replacen(&format!("{GRPCURL_EXTRACT}\n"), "", 1);
        without_extract.replacen(
            &format!("{GRPCURL_CHECKSUM_VERIFY}\n"),
            &format!("{GRPCURL_EXTRACT}\n{GRPCURL_CHECKSUM_VERIFY}\n"),
            1,
        )
    });

    let error = validate_vector_cluster_workflow(&workflow)
        .expect_err("extracting grpcurl before checksum verification must fail closed");
    assert!(error.contains("pinned grpcurl preparation"), "{error}");
}

#[test]
fn vector_cluster_workflow_rejects_checksum_literal_without_verification() {
    let source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let mut workflow: Workflow = yaml_serde::from_str(&source).expect("CI workflow must parse");
    mutate_grpcurl_command(&mut workflow, |command| {
        command.replacen(
            GRPCURL_CHECKSUM_VERIFY,
            "| (cd \"$RUNNER_TEMP\" && printf 'checksum verification disabled\\n')",
            1,
        )
    });

    let error = validate_vector_cluster_workflow(&workflow)
        .expect_err("a checksum literal without sha256sum verification must fail closed");
    assert!(error.contains("pinned grpcurl preparation"), "{error}");
}

#[test]
fn vector_cluster_runner_and_collection_are_fail_closed() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let runner = std::fs::read_to_string(root.join("scripts/ci/run-vector-cluster.sh"))
        .expect("required Vector cluster runner must exist");
    for required in [
        "grpcurl_identity=\"grpcurl ${grpcurl_version}\"",
        "62e2e4315bb70fab2e27f86c1f7738d09076a097a2dc8e0f701e386251172e40",
        "--collect-only",
        "--strict-markers",
        "--validate-collection",
        "--validate-summary",
        "--validate-cleanup",
        "-version 2>&1",
        "deselected",
        "xfailed",
        "xpassed",
        "skipped",
        "trap cleanup EXIT",
        "trap 'exit 143' TERM",
        "cleanup_rc",
    ] {
        assert!(
            runner.contains(required),
            "cluster runner is missing {required}"
        );
    }
    assert!(!runner.contains("command -v grpcurl"));

    let cluster_tests =
        normalized_fixture(include_str!("../../../tests/python/test_vector_cluster.py"));
    assert!(!cluster_tests.contains("pytest.mark.skipif"));
    assert!(cluster_tests.contains("@pytest.mark.parametrize"));
    assert!(cluster_tests.contains("signal.SIGTERM"));
    assert!(cluster_tests.contains("signal.SIGKILL"));
    assert!(cluster_tests.contains("signal.pthread_sigmask"));
    assert!(cluster_tests.contains("process_group_gone"));
    assert!(cluster_tests.contains("KIWI_VECTOR_CLUSTER_TEST_FAIL_AFTER_POPEN_NODE"));
    assert!(cluster_tests.contains("KIWI_VECTOR_CLUSTER_TEST_INITIALIZE_NODE1_ONLY"));
    assert!(cluster_tests.contains("wait_converged_cluster"));
    assert!(cluster_tests.contains("currentLeader"));
    assert!(cluster_tests.contains("\"Members\""));
}

#[test]
fn standard_python_integration_excludes_required_vector_cluster_gate() {
    let makefile = normalized_fixture(include_str!("../../../tests/Makefile"));
    assert!(
        makefile.contains("-m \"not raw_vector_protocol and not required_vector_cluster\""),
        "standard Python integration must not collect the dedicated cluster gate"
    );

    let cluster_tests =
        normalized_fixture(include_str!("../../../tests/python/test_vector_cluster.py"));
    assert!(
        cluster_tests.contains("pytest.mark.required_vector_cluster"),
        "cluster tests must own the dedicated required marker"
    );

    let conftest = normalized_fixture(include_str!("../../../tests/python/conftest.py"));
    assert!(
        conftest.contains("required_vector_cluster: dedicated three-node fail-closed gate"),
        "the dedicated cluster marker must remain registered for strict collection"
    );
}

#[test]
#[cfg(target_os = "linux")]
fn vector_cluster_runner_rejects_missing_or_untrusted_grpcurl() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let runner = root.join("scripts/ci/run-vector-cluster.sh");
    let scratch = std::env::temp_dir().join(format!(
        "kiwi-vector-cluster-grpcurl-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("contract")
    ));
    fs::create_dir_all(&scratch).expect("create cluster runner scratch directory");
    let fake_kiwi = scratch.join("kiwi");
    let fake_grpcurl = scratch.join("grpcurl");
    fs::write(&fake_kiwi, "#!/usr/bin/env bash\nexit 0\n").expect("write fake Kiwi");
    fs::write(
        &fake_grpcurl,
        "#!/usr/bin/env bash\necho 'grpcurl v1.9.3' >&2\n",
    )
    .expect("write fake grpcurl");
    for path in [&fake_kiwi, &fake_grpcurl] {
        let mut permissions = fs::metadata(path).expect("read fake mode").permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("make fake executable");
    }

    let missing = Command::new("/usr/bin/bash")
        .arg(&runner)
        .env("KIWI_RUN_CLUSTER_TESTS", "1")
        .env("KIWI_BINARY", &fake_kiwi)
        .env_remove("KIWI_GRPCURL")
        .output()
        .expect("run missing grpcurl mutant");
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("KIWI_GRPCURL must identify"));

    let untrusted = Command::new("/usr/bin/bash")
        .arg(&runner)
        .env("KIWI_RUN_CLUSTER_TESTS", "1")
        .env("KIWI_BINARY", &fake_kiwi)
        .env("KIWI_GRPCURL", &fake_grpcurl)
        .output()
        .expect("run untrusted grpcurl mutant");
    assert!(!untrusted.status.success());
    let untrusted_output = format!(
        "{}{}",
        String::from_utf8_lossy(&untrusted.stdout),
        String::from_utf8_lossy(&untrusted.stderr)
    );
    assert!(untrusted_output.contains("FAILED"), "{untrusted_output}");

    fs::remove_dir_all(&scratch).expect("remove cluster runner scratch directory");
}

#[test]
#[cfg(target_os = "linux")]
fn vector_cluster_validators_reject_collection_totals_and_cleanup_drift() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let validator = root.join("tests/python/test_vector_cluster.py");
    let scratch = std::env::temp_dir().join(format!(
        "kiwi-vector-cluster-validation-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("contract")
    ));
    fs::create_dir_all(&scratch).expect("create cluster validator scratch directory");

    let collection = scratch.join("collection.txt");
    fs::write(
        &collection,
        "tests/python/test_vector_cluster.py::test_vector_command_is_rejected_before_cluster_routing[leader-vadd]\n",
    )
    .expect("write collection mutant");
    let collection_result = Command::new("python3")
        .arg(&validator)
        .arg("--validate-collection")
        .arg(&collection)
        .output()
        .expect("run collection mutant");
    assert!(!collection_result.status.success());

    let summary = scratch.join("summary.json");
    let passing = r#"{"collected":16,"passed":16,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#;
    fs::write(&summary, passing).expect("write passing totals");
    let run_summary = || {
        Command::new("python3")
            .arg(&validator)
            .arg("--validate-summary")
            .arg(&summary)
            .output()
            .expect("run totals validator")
    };
    assert!(run_summary().status.success());
    for (name, mutant) in [
        (
            "zero collection",
            passing.replace("\"collected\":16", "\"collected\":0"),
        ),
        ("failed", passing.replace("\"failed\":0", "\"failed\":1")),
        ("skipped", passing.replace("\"skipped\":0", "\"skipped\":1")),
        ("xfailed", passing.replace("\"xfailed\":0", "\"xfailed\":1")),
        ("xpassed", passing.replace("\"xpassed\":0", "\"xpassed\":1")),
        (
            "deselected",
            passing.replace("\"deselected\":0", "\"deselected\":1"),
        ),
    ] {
        fs::write(&summary, mutant).expect("write totals mutant");
        assert!(
            !run_summary().status.success(),
            "cluster summary accepted {name} mutant"
        );
    }

    let cleanup = scratch.join("cleanup.json");
    fs::write(
        &cleanup,
        r#"{"schema":"kiwi-vector-cluster-cleanup/v1","processes":[{"term_sent":true,"waited":true,"process_group_gone":true},{"term_sent":true,"waited":true,"process_group_gone":false},{"term_sent":true,"waited":true,"process_group_gone":true}]}"#,
    )
    .expect("write cleanup mutant");
    let cleanup_result = Command::new("python3")
        .arg(&validator)
        .arg("--validate-cleanup")
        .arg(&cleanup)
        .output()
        .expect("run cleanup mutant");
    assert!(!cleanup_result.status.success());
    fs::write(
        &cleanup,
        r#"{"schema":"kiwi-vector-cluster-cleanup/v1","processes":[{"term_sent":true,"waited":true,"process_group_gone":true},{"term_sent":true,"waited":true,"process_group_gone":true}]}"#,
    )
    .expect("write short cleanup mutant");
    let cleanup_result = Command::new("python3")
        .arg(&validator)
        .arg("--validate-cleanup")
        .arg(&cleanup)
        .output()
        .expect("run short cleanup mutant");
    assert!(!cleanup_result.status.success());

    let launch_fault_result = Command::new("python3")
        .arg(&validator)
        .arg("--exercise-post-popen-failure-cleanup")
        .output()
        .expect("run post-Popen cleanup proof");
    assert!(
        launch_fault_result.status.success(),
        "post-Popen cleanup proof failed: {}{}",
        String::from_utf8_lossy(&launch_fault_result.stdout),
        String::from_utf8_lossy(&launch_fault_result.stderr)
    );

    fs::remove_dir_all(&scratch).expect("remove cluster validator scratch directory");
}

#[test]
fn vector_differential_required_job_is_unique_and_fail_closed() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    assert_eq!(
        workflow_source
            .lines()
            .filter(|line| *line == "  trusted-vector-differential:")
            .count(),
        1
    );
    assert_eq!(
        workflow_source
            .matches("scripts/compat/run-vector-differential.sh")
            .count(),
        1
    );
    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    let matching = workflow
        .jobs
        .iter()
        .filter(|(id, _)| id.as_str() == "trusted-vector-differential")
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1);
    let (_, job) = matching[0];
    assert_eq!(job.runs_on, "ubuntu-latest");
    assert!(job.continue_on_error.is_none());
    assert!(
        job.steps
            .iter()
            .all(|step| step.continue_on_error.is_none())
    );
    let commands = job
        .steps
        .iter()
        .filter_map(|step| step.run.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(
        commands
            .iter()
            .filter(|command| command.contains("scripts/compat/run-vector-differential.sh"))
            .count(),
        1
    );
    let runner = commands
        .iter()
        .find(|command| command.contains("scripts/compat/run-vector-differential.sh"))
        .expect("required job must invoke the Vector differential runner");
    assert!(runner.contains("KIWI_COMPAT_REQUIRE_ORACLE=1"));
}

#[test]
fn vector_differential_requires_user_namespace_preflight_before_runner() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let mut workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    validate_vector_differential_workflow(&workflow)
        .expect("trusted differential namespace preflight must be fail closed");
    let job = workflow
        .jobs
        .get_mut(TRUSTED_VECTOR_JOB)
        .expect("trusted differential job must exist");
    let preflight = job
        .steps
        .iter()
        .position(|step| {
            step.run
                .as_deref()
                .is_some_and(|command| command.trim_end() == ORACLE_NAMESPACE_PREFLIGHT)
        })
        .expect("trusted differential namespace preflight must exist");
    job.steps.remove(preflight);
    assert!(
        validate_vector_differential_workflow(&workflow).is_err(),
        "trusted differential accepted a missing namespace preflight"
    );
}

#[test]
fn build_and_test_requires_ubuntu_namespace_preflight_before_workspace_tests() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    validate_build_and_test_oracle_namespace_preflight(&workflow)
        .expect("workspace tests must preserve the namespace and platform timeout contracts");

    for (name, timeout_minutes) in [
        ("missing workspace timeout", None),
        (
            "uniformly extended workspace timeout",
            Some(yaml_serde::Value::String("45".to_string())),
        ),
    ] {
        let mut mutant = workflow.clone();
        let retry = mutant
            .jobs
            .get_mut(BUILD_AND_TEST_JOB)
            .expect("build-and-test job must exist")
            .steps
            .iter_mut()
            .find(|step| step.uses.as_deref() == Some("nick-fields/retry@v4"))
            .expect("workspace retry step must exist");
        retry.with.timeout_minutes = timeout_minutes;
        assert!(
            validate_build_and_test_oracle_namespace_preflight(&mutant).is_err(),
            "build-and-test accepted {name}"
        );
    }
}

#[test]
fn sanitizers_require_namespace_preflight_before_oracle_tests() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    validate_sanitizer_oracle_namespace_preflight(&workflow)
        .expect("sanitizer tests must prepare the Oracle namespace sandbox");
}

#[test]
fn required_jobs_reject_unversioned_actions() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    validate_required_job_action_versions(&workflow)
        .expect("required jobs must use versioned action runners");

    for job_id in [TRUSTED_VECTOR_JOB, VECTOR_CLUSTER_JOB, STATIC_ANALYSIS_JOB] {
        let mut workflow = workflow.clone();
        let checkout = workflow
            .jobs
            .get_mut(job_id)
            .expect("required job must exist")
            .steps
            .iter_mut()
            .find(|step| step.uses.as_deref() == Some("actions/checkout@v7"))
            .expect("required job must contain its versioned checkout");
        checkout.uses = Some("actions/checkout@main".to_string());
        assert!(
            validate_required_job_action_versions(&workflow).is_err(),
            "required jobs accepted floating action mutant in {job_id}"
        );
    }
}

#[test]
fn trusted_vector_workflow_rejects_non_exact_head_and_incomplete_evidence_uploads() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    validate_vector_differential_workflow(&workflow)
        .expect("trusted differential exact-Head workflow must be fail closed");

    let assert_rejected = |name: &str, mutant: Workflow| {
        assert!(
            validate_vector_differential_workflow(&mutant).is_err(),
            "trusted differential accepted {name} mutant"
        );
    };
    let step_index = |workflow: &Workflow, predicate: fn(&Step) -> bool| {
        workflow.jobs[TRUSTED_VECTOR_JOB]
            .steps
            .iter()
            .position(predicate)
            .expect("required trusted Vector step must exist")
    };
    let is_checkout = |step: &Step| step.uses.as_deref() == Some("actions/checkout@v7");
    let is_runner = |step: &Step| {
        step.run
            .as_deref()
            .is_some_and(|run| run.contains("bash scripts/compat/run-vector-differential.sh"))
    };
    let is_upload = |step: &Step| {
        step.uses
            .as_deref()
            .is_some_and(|uses| uses.starts_with("actions/upload-artifact@"))
    };

    let mut default_merge_checkout = workflow.clone();
    let checkout = step_index(&default_merge_checkout, is_checkout);
    default_merge_checkout
        .jobs
        .get_mut(TRUSTED_VECTOR_JOB)
        .expect("trusted Vector job must exist")
        .steps[checkout]
        .with
        .r#ref = None;
    assert_rejected("default synthetic merge checkout", default_merge_checkout);

    let mut synthetic_merge = workflow.clone();
    let checkout = step_index(&synthetic_merge, is_checkout);
    synthetic_merge
        .jobs
        .get_mut(TRUSTED_VECTOR_JOB)
        .expect("trusted Vector job must exist")
        .steps[checkout]
        .with
        .r#ref = Some("${{ github.sha }}".to_string());
    assert_rejected("synthetic merge checkout", synthetic_merge);

    let mut missing_expected_head = workflow.clone();
    let runner = step_index(&missing_expected_head, is_runner);
    let command = missing_expected_head
        .jobs
        .get_mut(TRUSTED_VECTOR_JOB)
        .expect("trusted Vector job must exist")
        .steps[runner]
        .run
        .as_mut()
        .expect("trusted Vector runner must have a command");
    *command = command.replace(
        "KIWI_EXPECTED_HEAD=\"${{ github.event.pull_request.head.sha || github.sha }}\" \\\n",
        "",
    );
    assert_rejected("missing expected Head", missing_expected_head);

    let mut missing_evidence_output = workflow.clone();
    let runner = step_index(&missing_evidence_output, is_runner);
    let command = missing_evidence_output
        .jobs
        .get_mut(TRUSTED_VECTOR_JOB)
        .expect("trusted Vector job must exist")
        .steps[runner]
        .run
        .as_mut()
        .expect("trusted Vector runner must have a command");
    *command = command.replace(
        "KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT=\"$RUNNER_TEMP/kiwi-oracle/vector-differential-evidence.json\" \\\n",
        "",
    );
    assert_rejected("missing evidence output", missing_evidence_output);

    for (name, path) in [
        ("provenance-only upload", TRUSTED_VECTOR_PROVENANCE),
        (
            "broad work-directory upload",
            "${{ runner.temp }}/kiwi-oracle",
        ),
    ] {
        let mut mutant = workflow.clone();
        let upload = step_index(&mutant, is_upload);
        mutant
            .jobs
            .get_mut(TRUSTED_VECTOR_JOB)
            .expect("trusted Vector job must exist")
            .steps[upload]
            .with
            .path = Some(path.to_string());
        assert_rejected(name, mutant);
    }

    let mut premature_upload = workflow.clone();
    let runner = step_index(&premature_upload, is_runner);
    let upload = step_index(&premature_upload, is_upload);
    premature_upload
        .jobs
        .get_mut(TRUSTED_VECTOR_JOB)
        .expect("trusted Vector job must exist")
        .steps
        .swap(runner, upload);
    assert_rejected("premature upload", premature_upload);

    let mut conditional_upload = workflow.clone();
    let upload = step_index(&conditional_upload, is_upload);
    conditional_upload
        .jobs
        .get_mut(TRUSTED_VECTOR_JOB)
        .expect("trusted Vector job must exist")
        .steps[upload]
        .condition = Some(yaml_serde::Value::String("always()".to_string()));
    assert_rejected("conditional failure upload", conditional_upload);

    for (name, retention_days) in [
        ("missing bounded retention", None),
        ("zero-day retention", Some(0)),
        ("six-day retention", Some(6)),
        ("oversized retention", Some(90)),
    ] {
        let mut mutant = workflow.clone();
        let upload = step_index(&mutant, is_upload);
        mutant
            .jobs
            .get_mut(TRUSTED_VECTOR_JOB)
            .expect("trusted Vector job must exist")
            .steps[upload]
            .with
            .retention_days = retention_days;
        assert_rejected(name, mutant);
    }
}

#[test]
fn vector_differential_upload_requires_explicit_missing_file_error() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must remain valid YAML");
    validate_vector_differential_workflow(&workflow)
        .expect("explicit error-on-missing provenance must remain accepted");

    let missing_policy = workflow_source.replacen("\n          if-no-files-found: error", "", 1);
    assert_ne!(
        missing_policy,
        workflow_source.as_ref(),
        "failed to construct missing upload policy mutant"
    );
    let workflow: Workflow = yaml_serde::from_str(&missing_policy)
        .expect("missing upload policy mutant must remain valid YAML");
    assert!(
        validate_vector_differential_workflow(&workflow).is_err(),
        "trusted differential accepted upload-artifact's warn-on-missing default"
    );
}

#[test]
fn vector_differential_make_undefined_variable_cannot_split_path_ignore() {
    let makefile = normalized_fixture(include_str!("../../../tests/Makefile"));
    let undetected = [":=", "=", "?=", "+="]
        .into_iter()
        .filter(|operator| {
            let mutant = format!(
                "{makefile}\nDIFF_IGNORE {operator} --ign$(UNDEFINED)ore=python/test_vector_set_differential.py\ntest-undefined-variable:\n\tpytest $(DIFF_IGNORE)"
            );
            !has_vector_differential_path_ignore(&mutant)
        })
        .collect::<Vec<_>>();
    assert!(
        undetected.is_empty(),
        "fast integration path-ignore split by an undefined variable was not detected for {undetected:?}"
    );
}

#[test]
fn vector_differential_make_simple_assignment_freezes_earlier_value() {
    let makefile = normalized_fixture(include_str!("../../../tests/Makefile"));
    let mutant = format!(
        "{makefile}\nDIFF_TEST := python/test_vector_set_differential.py\nDIFF_IGNORE := --ignore=$(DIFF_TEST)\nDIFF_TEST := python/test_other.py\ntest-immediate:\n\tpytest $(DIFF_IGNORE)"
    );
    assert!(
        has_vector_differential_path_ignore(&mutant),
        "fast integration path-ignore hidden by immediate Make expansion was not detected"
    );
}

#[test]
fn vector_differential_make_tab_recipe_scans_env_prefixed_command() {
    let makefile = normalized_fixture(include_str!("../../../tests/Makefile"));
    let mutant = format!(
        "{makefile}\ntest-env-ignore:\n\tPYTEST_ADDOPTS=--ignore=python/test_vector_set_differential.py pytest python/"
    );
    assert!(
        has_vector_differential_path_ignore(&mutant),
        "fast integration path-ignore in an environment-prefixed recipe was not detected"
    );
}

#[test]
fn vector_differential_rejects_supervisor_bypass_and_unsafe_uploads() {
    let workflow_source = normalized_fixture(include_str!("../../../.github/workflows/ci.yml"));
    let workflow: Workflow =
        yaml_serde::from_str(&workflow_source).expect("CI workflow must parse");
    validate_vector_differential_workflow(&workflow)
        .expect("trusted differential workflow must be fail closed");

    let runner_source = normalized_fixture(include_str!(
        "../../../scripts/compat/run-vector-differential.sh"
    ));
    validate_vector_differential_runner_source(&runner_source)
        .expect("trusted differential runner must obtain its runtime from the verifier");

    let runner_bypass = workflow_source.replacen(
        "          KIWI_COMPAT_REQUIRE_ORACLE=1 \\\n          KIWI_REDIS_ORACLE_SOURCE=\"$RUNNER_TEMP/kiwi-oracle/redis-source\" \\\n          KIWI_REDIS_ORACLE_PRIMARY_METADATA=\"$RUNNER_TEMP/kiwi-oracle/primary-build.json\" \\\n          KIWI_REDIS_ORACLE_OUTPUT=\"$RUNNER_TEMP/kiwi-oracle/oracle-provenance.json\" \\\n          KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT=\"$RUNNER_TEMP/kiwi-oracle/vector-differential-evidence.json\" \\\n          KIWI_EXPECTED_HEAD=\"${{ github.event.pull_request.head.sha || github.sha }}\" \\\n            bash scripts/compat/run-vector-differential.sh",
        "          redis-cli -h \"${KIWI_REDIS_ORACLE_HOST:-127.0.0.1}\" \\\n            -p \"${KIWI_REDIS_ORACLE_PORT:-6379}\" PING",
        1,
    );
    assert_ne!(
        runner_bypass, workflow_source,
        "failed to construct supervisor bypass mutant"
    );

    let upload_before_cleanup = workflow_source.replacen(
        "      - name: Run required trusted Vector differential\n        run: |\n          KIWI_COMPAT_REQUIRE_ORACLE=1",
        "      - name: Upload trusted Oracle evidence prematurely\n        uses: actions/upload-artifact@v7\n        with:\n          name: premature-evidence\n          path: ${{ runner.temp }}/kiwi-oracle/oracle-provenance.json\n\n      - name: Run required trusted Vector differential\n        run: |\n          KIWI_COMPAT_REQUIRE_ORACLE=1",
        1,
    );
    assert_ne!(
        upload_before_cleanup, workflow_source,
        "failed to construct premature upload mutant"
    );

    let unsafe_upload = workflow_source.replacen(
        "          path: |\n            ${{ runner.temp }}/kiwi-oracle/oracle-provenance.json\n            ${{ runner.temp }}/kiwi-oracle/vector-differential-evidence.json",
        "          path: ${{ runner.temp }}/kiwi-oracle",
        1,
    );
    assert_ne!(
        unsafe_upload, workflow_source,
        "failed to construct broad upload mutant"
    );
    let missing_upload_mutants = ["ignore", "warn"].map(|behavior| {
        let mutant = workflow_source.replacen(
            "          if-no-files-found: error",
            &format!("          if-no-files-found: {behavior}"),
            1,
        );
        assert_ne!(
            mutant, workflow_source,
            "failed to construct {behavior} missing upload mutant"
        );
        (format!("{behavior} missing provenance"), mutant)
    });
    for (name, mutant) in [
        ("supervisor bypass".to_string(), runner_bypass),
        ("upload before cleanup".to_string(), upload_before_cleanup),
        ("broad live/candidate upload".to_string(), unsafe_upload),
    ]
    .into_iter()
    .chain(missing_upload_mutants)
    {
        let workflow: Workflow =
            yaml_serde::from_str(&mutant).expect("differential mutant must remain valid YAML");
        assert!(
            validate_vector_differential_workflow(&workflow).is_err(),
            "trusted differential accepted {name} mutant"
        );
    }

    for (name, from, to) in [
        (
            "job condition",
            "  trusted-vector-differential:\n    name: trusted Vector differential",
            "  trusted-vector-differential:\n    if: false\n    name: trusted Vector differential",
        ),
        (
            "runner condition",
            "      - name: Run required trusted Vector differential\n        run: |",
            "      - name: Run required trusted Vector differential\n        if: false\n        run: |",
        ),
        (
            "upload condition",
            "      - name: Upload trusted Oracle evidence\n        uses: actions/upload-artifact@v7",
            "      - name: Upload trusted Oracle evidence\n        if: always()\n        uses: actions/upload-artifact@v7",
        ),
    ] {
        let mutant = workflow_source.replacen(from, to, 1);
        assert_ne!(mutant, workflow_source, "failed to construct {name} mutant");
        let workflow: Workflow =
            yaml_serde::from_str(&mutant).expect("differential condition mutant must parse");
        assert!(
            validate_vector_differential_workflow(&workflow).is_err(),
            "trusted differential accepted {name} mutant"
        );
    }

    for (name, mutant) in [
        (
            "ambient Oracle endpoint",
            runner_source.replacen(
                "[[ -z ${KIWI_REDIS_ORACLE_HOST:-} ]] \\\n    || die 'Oracle endpoint variables are only accepted inside the verifier callback'",
                ": # accept ambient Oracle endpoint",
                1,
            ),
        ),
        (
            "direct arbitrary-port PING",
            runner_source.replacen(
                "scripts/compat/verify-redis-8.8.1.sh \\",
                "/usr/bin/redis-cli -h \"${KIWI_REDIS_ORACLE_HOST:-127.0.0.1}\" -p \"${KIWI_REDIS_ORACLE_PORT:-6379}\" PING #",
                1,
            ),
        ),
        (
            "controller scratch residue",
            runner_source.replacen(
                "/usr/bin/rmdir -- /work/home /work/tmp || cleanup_status=$?",
                ": # leave controller scratch directories behind",
                1,
            ),
        ),
        (
            "commented safe scratch cleanup",
            runner_source.replacen(
                "        /usr/bin/rmdir -- /work/home /work/tmp || cleanup_status=$?",
                "        # /usr/bin/rmdir -- /work/home /work/tmp || cleanup_status=$?\n        rm -d -- /work/home /work/tmp || cleanup_status=$?",
                1,
            ),
        ),
        (
            "shadowed bare rmdir",
            runner_source
                .replacen(
                    "/usr/bin/rmdir -- /work/home /work/tmp || cleanup_status=$?",
                    "rmdir -- /work/home /work/tmp || cleanup_status=$?",
                    1,
                )
                .replacen(
                    "callback_exit_cleanup() {",
                    "rmdir() { rm -d \"$@\"; }\n\ncallback_exit_cleanup() {",
                    1,
                ),
        ),
    ] {
        assert_ne!(mutant, runner_source, "failed to construct {name} runner mutant");
        assert!(
            validate_vector_differential_runner_source(&mutant).is_err(),
            "trusted differential runner accepted {name} mutant"
        );
    }
}

#[test]
fn trusted_vector_differential_reaps_test_owned_temporary_directories() {
    let differential = normalized_fixture(include_str!(
        "../../../tests/python/test_vector_set_differential.py"
    ));
    let owns_and_reaps_scratch = |source: &str| {
        !source.contains("tmp_path")
            && source
                .lines()
                .filter(|line| line.trim() == "with TemporaryDirectory() as scratch:")
                .count()
                == 2
    };
    assert!(
        owns_and_reaps_scratch(&differential),
        "local filesystem probes must own and reap their callback TMPDIR entries"
    );

    for (name, mutant) in [
        (
            "pytest-managed tmp_path",
            differential.replacen(
                "def test_raw_comparator_rejects_equal_typed_values_with_different_frames(monkeypatch):",
                "def test_raw_comparator_rejects_equal_typed_values_with_different_frames(monkeypatch, tmp_path):",
                1,
            ),
        ),
        (
            "commented temporary-directory context",
            differential.replacen(
                "    with TemporaryDirectory() as scratch:",
                "    # with TemporaryDirectory() as scratch:",
                1,
            ),
        ),
    ] {
        assert_ne!(mutant, differential, "failed to construct {name} mutant");
        assert!(
            !owns_and_reaps_scratch(&mutant),
            "trusted differential accepted {name} mutant"
        );
    }
}

#[test]
#[cfg(target_os = "linux")]
fn vector_differential_scratch_cleanup_rejects_non_directory_replacements() {
    use std::os::unix::fs::{FileTypeExt, symlink};

    let scratch = std::env::temp_dir().join(format!(
        "kiwi-vector-scratch-cleanup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock must be after the Unix epoch")
            .as_nanos()
    ));
    fs::create_dir(&scratch).expect("create scratch cleanup test directory");

    let home = scratch.join("home");
    let temporary = scratch.join("tmp");
    fs::create_dir(&home).expect("create empty HOME fixture");
    fs::create_dir(&temporary).expect("create empty TMPDIR fixture");
    let empty_status = Command::new("/usr/bin/rmdir")
        .arg("--")
        .args([&home, &temporary])
        .status()
        .expect("rmdir empty scratch directories");
    assert!(empty_status.success());
    assert!(!home.exists() && !temporary.exists());

    let nonempty = scratch.join("nonempty");
    fs::create_dir(&nonempty).expect("create nonempty directory fixture");
    fs::write(nonempty.join("residue"), "unexpected").expect("write nested residue fixture");
    let regular = scratch.join("regular");
    fs::write(&regular, "unexpected").expect("write regular-file replacement fixture");
    let symlink_target = scratch.join("symlink-target");
    fs::create_dir(&symlink_target).expect("create symlink target fixture");
    let link = scratch.join("link");
    symlink(&symlink_target, &link).expect("create symlink replacement fixture");
    let fifo = scratch.join("fifo");
    assert!(
        Command::new("/usr/bin/mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo replacement fixture must start")
            .success()
    );

    for (kind, path) in [
        ("nonempty directory", &nonempty),
        ("regular file", &regular),
        ("symlink", &link),
        ("FIFO", &fifo),
    ] {
        let status = Command::new("/usr/bin/rmdir")
            .arg("--")
            .arg(path)
            .status()
            .expect("rmdir replacement mutant must start");
        assert!(!status.success(), "rmdir accepted {kind} replacement");
        assert!(
            fs::symlink_metadata(path).is_ok(),
            "rmdir removed {kind} replacement"
        );
    }
    assert!(
        fs::symlink_metadata(&fifo)
            .expect("FIFO fixture must remain")
            .file_type()
            .is_fifo()
    );

    fs::remove_dir_all(&scratch).expect("remove scratch cleanup test directory");
}

#[test]
fn trusted_vector_task_8_propagates_expected_head_and_evidence_output() {
    let runner = normalized_fixture(include_str!(
        "../../../scripts/compat/run-vector-differential.sh"
    ));
    let powershell = normalized_fixture(include_str!(
        "../../../scripts/compat/verify-redis-8.8.1.ps1"
    ));
    let controller =
        normalized_fixture(include_str!("../../../scripts/compat/oracle_controller.py"));

    for required in [
        "KIWI_EXPECTED_HEAD",
        "KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT",
        "--expected-head",
        "--evidence-output",
        "--publication-verifier",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD=1",
        "-p pytest_timeout",
    ] {
        assert!(
            runner.contains(required),
            "trusted Vector runner is missing {required}"
        );
    }
    for required in [
        "[string]$ExpectedHead",
        "[string]$EvidenceOutput",
        "[string]$PublicationVerifier",
        "$arguments.Add('--expected-head')",
        "$arguments.Add('--evidence-output')",
        "$arguments.Add('--publication-verifier')",
    ] {
        assert!(
            powershell.contains(required),
            "PowerShell verifier wrapper is missing {required}"
        );
    }
    for required in [
        "--expected-head",
        "--evidence-output",
        "--publication-verifier",
        "expected_head_argument",
        "evidence_output_argument",
        "publication_verifier_argument",
        "kiwi-redis-oracle-provenance/v4",
        "kiwi-vector-differential-evidence/v1",
    ] {
        assert!(
            controller.contains(required),
            "Oracle controller is missing {required}"
        );
    }
}

#[test]
fn vector_differential_fast_job_uses_marker_ownership_not_path_ignore() {
    let makefile = normalized_fixture(include_str!("../../../tests/Makefile"));
    assert!(!has_vector_differential_path_ignore(&makefile));
    assert!(makefile.contains("-m \"not raw_vector_protocol and not required_vector_cluster\""));
    for mutant in [
        format!("{makefile}\npytest --ignore=python/test_vector_set_differential.py"),
        format!("{makefile}\npytest --ignore-glob='*vector_set_differential.py'"),
        format!(
            "{makefile}\nDIFF_TEST := python/test_vector_set_differential.py\npytest --ignore=$(DIFF_TEST)"
        ),
        format!(
            "{makefile}\nDIFF_TEST = python/test_vector_set_differential.py\nDIFF_IGNORE = --ignore=$(DIFF_TEST)\ntest-indirect:\n\tpytest \\\n\t  $(DIFF_IGNORE)"
        ),
    ] {
        assert!(
            has_vector_differential_path_ignore(&mutant),
            "fast integration path-ignore mutant was not detected"
        );
    }
    let marker_only = format!(
        "{makefile}\nDIFF_TEST := python/test_vector_set_differential.py\nDIFF_MARKER := -m \"not raw_vector_protocol\"\ntest-marker-only:\n\tpytest $(DIFF_MARKER)"
    );
    assert!(
        !has_vector_differential_path_ignore(&marker_only),
        "marker-only ownership must not be treated as a path ignore"
    );

    let runner = normalized_fixture(include_str!(
        "../../../scripts/compat/run-vector-differential.sh"
    ));
    for required in [
        "KIWI_COMPAT_REQUIRE_ORACLE",
        "kiwi-required-vector-jobs",
        "vector-required-jobs.json",
        "--collect-only",
        "--strict-markers",
        "expected_node_ids",
        "expected_item_count",
        "KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE",
        "cleanup",
        "deselected",
        "xfailed",
        "xpassed",
        "skipped",
    ] {
        assert!(runner.contains(required), "runner is missing {required}");
    }
    assert!(!runner.contains("sed -n '/^    expected_node_ids:"));
    assert!(!runner.contains("commands_match = re.search"));
    assert!(!runner.contains("raw_cases_match = re.search"));
    let callback_registry = runner
        .find("canonicalize_required_jobs \"$registry\" /work/vector-required-jobs.json")
        .expect("callback must invoke the authoritative registry parser");
    let callback_collection = runner
        .find("KIWI_VECTOR_PYTEST_SUMMARY=/work/collect-summary.json")
        .expect("callback must collect the required pytest module");
    assert!(callback_registry < callback_collection);
    for validator in [
        "validate_collection /work/vector-required-jobs.json",
        "validate_raw_transcript /work/vector-required-jobs.json",
        "validate_final_state /work/vector-required-jobs.json",
        "validate_evidence_set /work/vector-required-jobs.json",
        "validate_summary /work/vector-required-jobs.json",
    ] {
        assert!(
            runner.contains(validator),
            "callback validator bypasses canonical JSON: {validator}"
        );
    }
    for required in [
        "kiwi-vector-wire-transcript/v1",
        "kiwi-vector-final-state/v1",
        "raw-transcript.jsonl",
        "final-state.jsonl",
        "KIWI_VECTOR_RAW_TRANSCRIPT",
        "KIWI_VECTOR_FINAL_STATE",
    ] {
        assert!(
            runner.contains(required),
            "strict evidence runner is missing {required}"
        );
    }
    for forbidden in [
        "raw-coverage.jsonl",
        "KIWI_VECTOR_RAW_COVERAGE",
        "validate_raw_coverage",
    ] {
        assert!(
            !runner.contains(forbidden),
            "hash-only evidence contract remains in the runner: {forbidden}",
        );
    }
    assert!(
        runner
            .find("cargo build --locked -p kiwi-compat --bin kiwi-required-vector-jobs")
            .expect("outer runner must build the current-HEAD registry helper")
            < runner
                .find("scripts/compat/verify-redis-8.8.1.sh")
                .expect("outer runner must invoke the Oracle verifier")
    );
}

#[test]
#[cfg(target_os = "linux")]
fn vector_differential_runner_rejects_collection_and_result_drift() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let runner = root.join("scripts/compat/run-vector-differential.sh");
    let registry = root.join("tests/compat/redis-8.8.1/vector-required-jobs.yaml");
    let scratch = std::env::temp_dir().join(format!(
        "kiwi-vector-contract-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock must be after the Unix epoch")
            .as_nanos()
    ));
    fs::create_dir(&scratch).expect("create differential validator scratch directory");
    let collection = scratch.join("collection.log");
    let summary = scratch.join("summary.json");
    let passing = r#"{"collected":40,"passed":40,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#;
    let yaml = fs::read_to_string(&registry).expect("read required job registry");
    let node_ids = yaml
        .lines()
        .filter_map(|line| line.strip_prefix("      - tests/python/"))
        .map(|line| format!("tests/python/{line}"))
        .collect::<Vec<_>>();
    assert_eq!(node_ids.len(), 40);

    fs::write(&collection, format!("{}\n", node_ids.join("\n")))
        .expect("write exact collection evidence");
    fs::write(&summary, passing).expect("write passing summary evidence");
    assert!(
        runner_command(&runner)
            .arg("--validate-collection")
            .arg(&registry)
            .arg(&collection)
            .status()
            .expect("collection validator must start")
            .success()
    );
    for (name, mutant) in [
        (
            "unknown-field",
            yaml.replacen(
                "    test_module:",
                "    unknown_job_field: true\n    test_module:",
                1,
            ),
        ),
        (
            "reversed-protocols",
            yaml.replacen(
                "    protocols: [resp2, resp3]",
                "    protocols: [resp3, resp2]",
                1,
            ),
        ),
    ] {
        let mutant_registry = scratch.join(format!("{name}.yaml"));
        fs::write(&mutant_registry, mutant).expect("write registry mutant");
        assert!(
            !runner_command(&runner)
                .arg("--validate-collection")
                .arg(&mutant_registry)
                .arg(&collection)
                .status()
                .expect("collection validator mutant must start")
                .success(),
            "runner accepted {name} registry mutant"
        );
        assert!(
            !runner_command(&runner)
                .arg("--validate-summary")
                .arg(&mutant_registry)
                .arg(&summary)
                .status()
                .expect("summary validator mutant must start")
                .success(),
            "summary validator accepted {name} registry mutant"
        );
    }
    fs::write(&collection, "27 tests collected\n").expect("write collection count mutant");
    assert!(
        !runner_command(&runner)
            .arg("--validate-collection")
            .arg(&registry)
            .arg(&collection)
            .status()
            .expect("collection count mutant validator must start")
            .success()
    );
    let mut drifted_node_ids = node_ids.clone();
    drifted_node_ids[0] =
        "tests/python/test_vector_set_differential.py::test_unregistered_node".to_string();
    fs::write(&collection, format!("{}\n", drifted_node_ids.join("\n")))
        .expect("write collection identity mutant");
    assert!(
        !runner_command(&runner)
            .arg("--validate-collection")
            .arg(&registry)
            .arg(&collection)
            .status()
            .expect("collection identity mutant validator must start")
            .success()
    );

    assert!(
        runner_command(&runner)
            .arg("--validate-summary")
            .arg(&registry)
            .arg(&summary)
            .status()
            .expect("summary validator must start")
            .success()
    );
    for mutant in [
        passing.replace("\"collected\":40", "\"collected\":0"),
        passing.replace("\"failed\":0", "\"failed\":1"),
        passing.replace("\"skipped\":0", "\"skipped\":1"),
        passing.replace("\"xfailed\":0", "\"xfailed\":1"),
        passing.replace("\"xpassed\":0", "\"xpassed\":1"),
        passing.replace("\"deselected\":0", "\"deselected\":1"),
    ] {
        fs::write(&summary, mutant).expect("write summary mutant");
        assert!(
            !runner_command(&runner)
                .arg("--validate-summary")
                .arg(&registry)
                .arg(&summary)
                .status()
                .expect("summary mutant validator must start")
                .success()
        );
    }

    let unavailable = runner_command(&runner)
        .env_clear()
        .env("OSTYPE", "linux-gnu")
        .env("KIWI_COMPAT_REQUIRE_ORACLE", "1")
        .status()
        .expect("unavailable runner probe must start");
    assert!(!unavailable.success());
    let identity_mismatch = runner_command(&runner)
        .arg("--callback")
        .env_clear()
        .env("OSTYPE", "linux-gnu")
        .env("KIWI_COMPAT_REQUIRE_ORACLE", "1")
        .env("KIWI_REDIS_ORACLE_HOST", "192.0.2.1")
        .env("KIWI_REDIS_ORACLE_PORT", "6379")
        .env(
            "KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE",
            "/runtime-evidence.json",
        )
        .status()
        .expect("identity mismatch runner probe must start");
    assert!(!identity_mismatch.success());
    let runtime_evidence = scratch.join("runtime-evidence.json");
    let valid_runtime = r#"{"build_role":"rebuild","binary_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","binary_identity":{"device":1,"inode":2,"mode":33261,"size":3,"nlink":1},"held_fd":true,"pid":42,"info_redis_versions":["8.8.1"]}"#;
    fs::write(&runtime_evidence, valid_runtime).expect("write valid runtime evidence");
    assert!(
        runner_command(&runner)
            .arg("--validate-runtime-evidence")
            .arg(&runtime_evidence)
            .status()
            .expect("runtime evidence validator must start")
            .success()
    );
    for mutant in [
        valid_runtime.replace("\"rebuild\"", "\"primary\""),
        valid_runtime.replace("\"held_fd\":true", "\"held_fd\":false"),
        valid_runtime.replace("\"8.8.1\"", "\"8.8.0\""),
        valid_runtime.replace("\"pid\":42", "\"pid\":0"),
        valid_runtime.replace(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bad-hash",
        ),
        valid_runtime.replace("\"inode\":2", "\"inode\":0"),
    ] {
        fs::write(&runtime_evidence, mutant).expect("write runtime evidence mutant");
        assert!(
            !runner_command(&runner)
                .arg("--validate-runtime-evidence")
                .arg(&runtime_evidence)
                .status()
                .expect("runtime evidence mutant validator must start")
                .success()
        );
    }
    assert!(
        !runner_command(&runner)
            .args(["--validate-callback-result", "0", "1"])
            .status()
            .expect("callback result validator must start")
            .success()
    );

    fs::remove_dir_all(&scratch).expect("remove differential validator scratch directory");
}

#[test]
#[cfg(target_os = "linux")]
fn vector_differential_runner_rejects_unreplayable_or_unbounded_evidence() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let runner = root.join("scripts/compat/run-vector-differential.sh");
    let registry = root.join("tests/compat/redis-8.8.1/vector-required-jobs.yaml");
    let scratch = std::env::temp_dir().join(format!(
        "kiwi-vector-strict-evidence-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock must be after the Unix epoch")
            .as_nanos()
    ));
    fs::create_dir(&scratch).expect("create strict evidence scratch directory");
    let transcript = scratch.join("raw-transcript.jsonl");
    let final_state = scratch.join("final-state.jsonl");

    let helper_output = Command::new(env!("CARGO_BIN_EXE_kiwi-required-vector-jobs"))
        .arg(&registry)
        .output()
        .expect("required-jobs helper must run");
    assert!(helper_output.status.success());
    let canonical: Value =
        serde_json::from_slice(&helper_output.stdout).expect("parse canonical required jobs");
    let canonical_request = |command: &str, case_id: &str, node_id: &str| {
        let raw_case = canonical["raw_cases"][command]
            .as_array()
            .expect("canonical command must expose raw cases")
            .iter()
            .find(|raw_case| raw_case["case_id"] == case_id)
            .expect("canonical raw case must exist");
        let request_base64 = raw_case["request_base64_by_node"][node_id]
            .as_str()
            .expect("canonical raw case must bind exact request bytes")
            .to_string();
        let request = BASE64_STANDARD
            .decode(&request_base64)
            .expect("canonical request must use valid Base64");
        let (_, request_sha256) = encoded_bytes(&request);
        (request_base64, request_sha256)
    };
    let raw_node_ids = [
        "tests/python/test_vector_set_differential.py::test_zero_vector_values_raw_differential[resp2]",
        "tests/python/test_vector_set_differential.py::test_zero_vector_values_raw_differential[resp3]",
        "tests/python/test_vector_set_differential.py::test_zero_vector_fp32_raw_differential[resp2]",
        "tests/python/test_vector_set_differential.py::test_zero_vector_fp32_raw_differential[resp3]",
    ];
    let exact_cases = [
        ("VADD", "zero-vector"),
        ("VCARD", "zero-vector"),
        ("VDIM", "zero-vector"),
        ("VEMB", "zero-vector"),
        ("VINFO", "missing-key"),
        ("VISMEMBER", "zero-vector"),
        ("VREM", "zero-vector"),
        ("VSIM", "zero-vector"),
    ];
    let mut transcript_records = Vec::new();
    for node_id in raw_node_ids {
        let protocol = if node_id.ends_with("[resp2]") { 2 } else { 3 };
        for (command, case_id) in exact_cases {
            let (request_base64, request_sha256) = canonical_request(command, case_id, node_id);
            transcript_records.push(json!({
                "schema": "kiwi-vector-wire-transcript/v1",
                "node_id": node_id,
                "case_id": case_id,
                "protocol": protocol,
                "command": command,
                "comparison_kind": "exact-frame",
                "request_base64": request_base64,
                "request_sha256": request_sha256,
                "kiwi_response_base64": "OjENCg==",
                "kiwi_response_sha256": "6d7dbcb27aa6e24f40bf1f4cb2cc8a36e2f3b7f1ae87edf906578a2936b756d2",
                "redis_response_base64": "OjENCg==",
                "redis_response_sha256": "6d7dbcb27aa6e24f40bf1f4cb2cc8a36e2f3b7f1ae87edf906578a2936b756d2",
                "registered_difference_ids": [],
            }));
        }
        let (kiwi_base64, kiwi_sha256, redis_base64, redis_sha256) = if protocol == 2 {
            (
                "KjE4DQorcXVhbnQtdHlwZQ0KK2YzMg0KK2huc3ctbQ0KOjANCit2ZWN0b3ItZGltDQo6Mg0KK3Byb2plY3Rpb24taW5wdXQtZGltDQo6MA0KK3NpemUNCjoxDQorbWF4LWxldmVsDQo6MA0KK2F0dHJpYnV0ZXMtY291bnQNCjowDQordnNldC11aWQNCjo3DQoraG5zdy1tYXgtbm9kZS11aWQNCjowDQo=",
                "a5345a20c71584036c712b8636581a509b31336c64af26d898bbda1aac75bb32",
                "KjE4DQorcXVhbnQtdHlwZQ0KK2YzMg0KK2huc3ctbQ0KOjE2DQordmVjdG9yLWRpbQ0KOjINCitwcm9qZWN0aW9uLWlucHV0LWRpbQ0KOjANCitzaXplDQo6MQ0KK21heC1sZXZlbA0KOjMNCithdHRyaWJ1dGVzLWNvdW50DQo6MA0KK3ZzZXQtdWlkDQo6NDINCitobnN3LW1heC1ub2RlLXVpZA0KOjkNCg==",
                "8c12989f6910ade14b6c2c3b24df811fbf44a4b46dfaa4e51c443358dc49bcb6",
            )
        } else {
            (
                "JTkNCitxdWFudC10eXBlDQorZjMyDQoraG5zdy1tDQo6MA0KK3ZlY3Rvci1kaW0NCjoyDQorcHJvamVjdGlvbi1pbnB1dC1kaW0NCjowDQorc2l6ZQ0KOjENCittYXgtbGV2ZWwNCjowDQorYXR0cmlidXRlcy1jb3VudA0KOjANCit2c2V0LXVpZA0KOjcNCitobnN3LW1heC1ub2RlLXVpZA0KOjANCg==",
                "9d6962697ec057dae8636d27dbd05eb6b35f09a847e54261f7c0fa4fc8bcac20",
                "JTkNCitxdWFudC10eXBlDQorZjMyDQoraG5zdy1tDQo6MTYNCit2ZWN0b3ItZGltDQo6Mg0KK3Byb2plY3Rpb24taW5wdXQtZGltDQo6MA0KK3NpemUNCjoxDQorbWF4LWxldmVsDQo6Mw0KK2F0dHJpYnV0ZXMtY291bnQNCjowDQordnNldC11aWQNCjo0Mg0KK2huc3ctbWF4LW5vZGUtdWlkDQo6OQ0K",
                "3e1875a1d52140156cb4c01c936a42fa041686c2edc58451d9e8c0cff023acfc",
            )
        };
        let (request_base64, request_sha256) = canonical_request("VINFO", "populated", node_id);
        transcript_records.push(json!({
            "schema": "kiwi-vector-wire-transcript/v1",
            "node_id": node_id,
            "case_id": "populated",
            "protocol": protocol,
            "command": "VINFO",
            "comparison_kind": "raw-schema",
            "request_base64": request_base64,
            "request_sha256": request_sha256,
            "kiwi_response_base64": kiwi_base64,
            "kiwi_response_sha256": kiwi_sha256,
            "redis_response_base64": redis_base64,
            "redis_response_sha256": redis_sha256,
            "registered_difference_ids": [
                "vinfo-hnsw-m",
                "vinfo-max-level",
                "vinfo-vset-uid",
                "vinfo-hnsw-max-node-uid",
            ],
        }));
    }
    let write_jsonl = |path: &std::path::Path, records: &[Value]| {
        let text = records
            .iter()
            .map(|record| serde_json::to_string(record).expect("serialize evidence record"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(path, text).expect("write JSONL evidence");
    };
    write_jsonl(&transcript, &transcript_records);
    let validate_transcript = |path: &std::path::Path| {
        runner_command(&runner)
            .arg("--validate-raw-transcript")
            .arg(&registry)
            .arg(path)
            .status()
            .expect("raw transcript validator must start")
            .success()
    };
    assert!(validate_transcript(&transcript));

    let assert_transcript_mutant_rejected = |name: &str, records: &[Value]| {
        let mutant = scratch.join(format!("raw-transcript-{name}.jsonl"));
        write_jsonl(&mutant, records);
        assert!(
            !validate_transcript(&mutant),
            "accepted raw transcript mutant {name}"
        );
    };
    let hash_only = vec![json!({
        "case_id": "zero-vector",
        "command": "VADD",
        "node_id": raw_node_ids[0],
        "protocol": 2,
        "kiwi_frame_sha256": "a".repeat(64),
        "redis_frame_sha256": "a".repeat(64),
    })];
    assert_transcript_mutant_rejected("hash-only", &hash_only);
    let mut invalid_base64 = transcript_records.clone();
    invalid_base64[0]["request_base64"] = json!("%%%=");
    assert_transcript_mutant_rejected("invalid-base64", &invalid_base64);
    let mut hash_mismatch = transcript_records.clone();
    hash_mismatch[0]["request_sha256"] = json!("0".repeat(64));
    assert_transcript_mutant_rejected("hash-mismatch", &hash_mismatch);
    let mut wrong_request_key = transcript_records.clone();
    let wrong_key_request = encode_resp_command(
        "VADD",
        b"test_vdiff:raw:p2:wrong-key",
        &[b"VALUES", b"2", b"0", b"0", b"zero", b"NOQUANT"],
    );
    let (wrong_key_base64, wrong_key_sha256) = encoded_bytes(&wrong_key_request);
    wrong_request_key[0]["request_base64"] = json!(wrong_key_base64);
    wrong_request_key[0]["request_sha256"] = json!(wrong_key_sha256);
    assert_transcript_mutant_rejected("wrong-request-key", &wrong_request_key);
    let mut wrong_request_arguments = transcript_records.clone();
    let wrong_arguments_request = encode_resp_command(
        "VADD",
        b"test_vdiff:raw:p2:values",
        &[b"VALUES", b"2", b"0", b"1", b"zero", b"NOQUANT"],
    );
    let (wrong_arguments_base64, wrong_arguments_sha256) = encoded_bytes(&wrong_arguments_request);
    wrong_request_arguments[0]["request_base64"] = json!(wrong_arguments_base64);
    wrong_request_arguments[0]["request_sha256"] = json!(wrong_arguments_sha256);
    assert_transcript_mutant_rejected("wrong-request-arguments", &wrong_request_arguments);
    assert_transcript_mutant_rejected("missing-case", &transcript_records[1..]);
    let mut duplicate = transcript_records.clone();
    duplicate.push(transcript_records[0].clone());
    assert_transcript_mutant_rejected("duplicate-case", &duplicate);
    let mut extra = transcript_records.clone();
    let mut extra_record = transcript_records[0].clone();
    extra_record["case_id"] = json!("extra-case");
    extra.push(extra_record);
    assert_transcript_mutant_rejected("extra-case", &extra);
    let mut unregistered = transcript_records.clone();
    unregistered[0]["registered_difference_ids"] = json!(["unregistered-difference"]);
    assert_transcript_mutant_rejected("unregistered-difference", &unregistered);
    let mut extra_field = transcript_records.clone();
    extra_field[0]["typed_reply"] = json!(1);
    assert_transcript_mutant_rejected("extra-field", &extra_field);
    let duplicate_transcript_key = scratch.join("raw-transcript-duplicate-object-key.jsonl");
    let transcript_text = fs::read_to_string(&transcript).expect("read transcript fixture");
    let duplicate_transcript_text = transcript_text.replacen(
        "\"schema\":",
        "\"schema\":\"invalid-duplicate\",\"schema\":",
        1,
    );
    assert_ne!(duplicate_transcript_text, transcript_text);
    fs::write(&duplicate_transcript_key, duplicate_transcript_text)
        .expect("write duplicate transcript object-key mutant");
    assert!(
        !validate_transcript(&duplicate_transcript_key),
        "accepted duplicate transcript object key"
    );

    let node_ids = canonical["expected_node_ids"]
        .as_array()
        .expect("canonical registry must expose expected node IDs")
        .iter()
        .map(|node_id| {
            node_id
                .as_str()
                .expect("canonical node ID must be a string")
        })
        .collect::<Vec<_>>();
    assert_eq!(node_ids.len(), 40);
    let final_contracts = canonical["final_state"]
        .as_object()
        .expect("canonical registry must expose final-state contracts");
    let mut final_records = Vec::new();
    for &node_id in &node_ids {
        let contract = final_contracts
            .get(node_id)
            .expect("canonical node must have a final-state contract");
        if contract["applicability"] == "not-applicable" {
            final_records.push(json!({
                "schema": "kiwi-vector-final-state/v1",
                "node_id": node_id,
                "applicability": "not-applicable",
                "reason": contract["reason"],
            }));
            continue;
        }
        let protocol = if node_id.ends_with("[resp2]") { 2 } else { 3 };
        let state_profile = contract["state_profile"]
            .as_str()
            .expect("server-backed contract must expose a state profile");
        final_records.push(json!({
            "schema": "kiwi-vector-final-state/v1",
            "node_id": node_id,
            "applicability": "server-backed",
            "protocol": protocol,
            "known_keys": final_state_keys(state_profile, protocol),
        }));
    }
    write_jsonl(&final_state, &final_records);
    let validate_final_state = |path: &std::path::Path| {
        runner_command(&runner)
            .arg("--validate-final-state")
            .arg(&registry)
            .arg(path)
            .status()
            .expect("final-state validator must start")
            .success()
    };
    assert!(validate_final_state(&final_state));
    let repeated_node = "tests/python/test_vector_set_differential.py::test_repeated_vadd_and_vsim_options_match[resp2]";
    let repeated_index = final_records
        .iter()
        .position(|record| record["node_id"] == repeated_node)
        .expect("fixture must contain the repeated VADD RESP2 node");
    let repeated_main_index = final_records[repeated_index]["known_keys"]
        .as_array()
        .expect("known_keys must be an array")
        .iter()
        .position(|key| key["key_role"] == "main")
        .expect("repeated VADD fixture must expose the main role");
    let repeated_resp3_node = "tests/python/test_vector_set_differential.py::test_repeated_vadd_and_vsim_options_match[resp3]";
    let repeated_resp3_index = final_records
        .iter()
        .position(|record| record["node_id"] == repeated_resp3_node)
        .expect("fixture must contain the repeated VADD RESP3 node");
    let repeated_resp3_main_index = final_records[repeated_resp3_index]["known_keys"]
        .as_array()
        .expect("known_keys must be an array")
        .iter()
        .position(|key| key["key_role"] == "main")
        .expect("repeated VADD RESP3 fixture must expose the main role");
    let mut resp2_double_component = final_records.clone();
    replace_exchange_response(
        &mut resp2_double_component[repeated_index]["known_keys"][repeated_main_index]["before_cleanup"]
            ["observations"][2],
        &resp_vector(4, 3),
    );
    assert!(
        !validate_final_state(&{
            let mutant = scratch.join("final-state-resp2-double-component.jsonl");
            write_jsonl(&mutant, &resp2_double_component);
            mutant
        }),
        "accepted RESP3 double components in a RESP2 final-state record"
    );
    let mut resp3_bulk_component = final_records.clone();
    replace_exchange_response(
        &mut resp3_bulk_component[repeated_resp3_index]["known_keys"][repeated_resp3_main_index]["before_cleanup"]
            ["observations"][2],
        &resp_vector(4, 2),
    );
    assert!(
        !validate_final_state(&{
            let mutant = scratch.join("final-state-resp3-bulk-component.jsonl");
            write_jsonl(&mutant, &resp3_bulk_component);
            mutant
        }),
        "accepted RESP2 bulk components in a RESP3 final-state record"
    );
    let mut resp2_resp3_null = final_records.clone();
    let resp2_ghost = resp2_resp3_null[repeated_index]["known_keys"][repeated_main_index]
        ["before_cleanup"]["observations"]
        .as_array_mut()
        .expect("RESP2 observations must be an array")
        .last_mut()
        .expect("RESP2 observations must include a ghost member");
    replace_exchange_response(resp2_ghost, b"_\r\n");
    assert!(
        !validate_final_state(&{
            let mutant = scratch.join("final-state-resp2-resp3-null.jsonl");
            write_jsonl(&mutant, &resp2_resp3_null);
            mutant
        }),
        "accepted a RESP3 null in a RESP2 final-state record"
    );
    let mut resp3_resp2_null = final_records.clone();
    let resp3_ghost = resp3_resp2_null[repeated_resp3_index]["known_keys"]
        [repeated_resp3_main_index]["before_cleanup"]["observations"]
        .as_array_mut()
        .expect("RESP3 observations must be an array")
        .last_mut()
        .expect("RESP3 observations must include a ghost member");
    replace_exchange_response(resp3_ghost, b"$-1\r\n");
    assert!(
        !validate_final_state(&{
            let mutant = scratch.join("final-state-resp3-resp2-null.jsonl");
            write_jsonl(&mutant, &resp3_resp2_null);
            mutant
        }),
        "accepted a RESP2 null in a RESP3 final-state record"
    );
    let invalid_resp3_components: [(&str, &[u8]); 4] = [
        ("integer-component", b"*4\r\n:1\r\n,0\r\n,0\r\n,0\r\n"),
        ("nan-component", b"*4\r\n,nan\r\n,0\r\n,0\r\n,0\r\n"),
        ("infinite-component", b"*4\r\n,inf\r\n,0\r\n,0\r\n,0\r\n"),
        (
            "nonnumeric-component",
            b"*4\r\n,not-a-number\r\n,0\r\n,0\r\n,0\r\n",
        ),
    ];
    for (name, response) in invalid_resp3_components {
        let mut mutant_records = final_records.clone();
        replace_exchange_response(
            &mut mutant_records[repeated_resp3_index]["known_keys"][repeated_resp3_main_index]["before_cleanup"]
                ["observations"][2],
            response,
        );
        let mutant = scratch.join(format!("final-state-{name}.jsonl"));
        write_jsonl(&mutant, &mutant_records);
        assert!(
            !validate_final_state(&mutant),
            "accepted invalid RESP3 VEMB {name}"
        );
    }
    let mut repeated_two_member_state = final_records.clone();
    let repeated_observations = repeated_two_member_state[repeated_index]["known_keys"]
        [repeated_main_index]["before_cleanup"]["observations"]
        .as_array_mut()
        .expect("repeated VADD observations must be an array");
    replace_exchange_response(&mut repeated_observations[0], b":2\r\n");
    for observation in &mut repeated_observations[4..] {
        replace_exchange_response(observation, b"$-1\r\n");
    }
    let repeated_two_member_final_state = scratch.join("final-state-repeated-two-member.jsonl");
    write_jsonl(&repeated_two_member_final_state, &repeated_two_member_state);
    assert!(
        validate_final_state(&repeated_two_member_final_state),
        "rejected producer-accurate repeated VADD final state with only alpha and beta"
    );
    let final_state_text = fs::read_to_string(&final_state).expect("read final-state fixture");
    let duplicate_final_top_key = scratch.join("final-state-duplicate-top-object-key.jsonl");
    let duplicate_final_top_text = final_state_text.replacen(
        "\"schema\":",
        "\"schema\":\"invalid-duplicate\",\"schema\":",
        1,
    );
    assert_ne!(duplicate_final_top_text, final_state_text);
    fs::write(&duplicate_final_top_key, duplicate_final_top_text)
        .expect("write duplicate final-state top-level object-key mutant");
    assert!(
        !validate_final_state(&duplicate_final_top_key),
        "accepted duplicate final-state top-level object key"
    );
    let duplicate_final_nested_key = scratch.join("final-state-duplicate-nested-object-key.jsonl");
    let duplicate_final_nested_text =
        final_state_text.replacen("\"command\":", "\"command\":\"INVALID\",\"command\":", 1);
    assert_ne!(duplicate_final_nested_text, final_state_text);
    fs::write(&duplicate_final_nested_key, duplicate_final_nested_text)
        .expect("write duplicate final-state nested object-key mutant");
    assert!(
        !validate_final_state(&duplicate_final_nested_key),
        "accepted duplicate final-state nested object key"
    );
    let assert_final_mutant_rejected = |name: &str, records: &[Value]| {
        let mutant = scratch.join(format!("final-state-{name}.jsonl"));
        write_jsonl(&mutant, records);
        assert!(
            !validate_final_state(&mutant),
            "accepted final-state mutant {name}"
        );
    };
    let mut unexpected_two_member_profile_member = repeated_two_member_state.clone();
    replace_exchange_response(
        &mut unexpected_two_member_profile_member[repeated_index]["known_keys"]
            [repeated_main_index]["before_cleanup"]["observations"][4],
        &resp_vector(4, 2),
    );
    assert_final_mutant_rejected(
        "unexpected-two-member-profile-member",
        &unexpected_two_member_profile_member,
    );
    assert_final_mutant_rejected("missing-envelope", &final_records[1..]);
    let mut duplicate_final = final_records.clone();
    duplicate_final.push(final_records[0].clone());
    assert_final_mutant_rejected("duplicate-envelope", &duplicate_final);
    let server_index = final_records
        .iter()
        .position(|record| record["applicability"] == "server-backed")
        .expect("fixture must contain server-backed final state");
    let vector_index = final_records
        .iter()
        .position(|record| {
            record["known_keys"].as_array().is_some_and(|keys| {
                keys.iter().any(|key| {
                    key["key_role"] == "main"
                        && key["before_cleanup"]["observations"]
                            .as_array()
                            .is_some_and(|items| !items.is_empty())
                })
            })
        })
        .expect("fixture must contain a populated main Vector key");
    let vector_key_index = final_records[vector_index]["known_keys"]
        .as_array()
        .expect("known_keys must be an array")
        .iter()
        .position(|key| key["key_role"] == "main")
        .expect("populated Vector fixture must expose the main role");
    let mut missing_key_role = final_records.clone();
    missing_key_role[server_index]["known_keys"]
        .as_array_mut()
        .expect("known_keys must be an array")
        .remove(0);
    assert_final_mutant_rejected("missing-key-role", &missing_key_role);
    let mut wrong_key_bytes = final_records.clone();
    wrong_key_bytes[server_index]["known_keys"][0] =
        final_state_key_record("values", b"wrong-key", "none", 2, None, &[], 0);
    assert_final_mutant_rejected("wrong-key-bytes", &wrong_key_bytes);
    let mut wrong_profile_type = final_records.clone();
    let profile_key = BASE64_STANDARD
        .decode(
            wrong_profile_type[server_index]["known_keys"][0]["key_base64"]
                .as_str()
                .expect("fixture key Base64 must be a string"),
        )
        .expect("fixture key must use valid Base64");
    wrong_profile_type[server_index]["known_keys"][0] =
        final_state_key_record("values", &profile_key, "string", 2, None, &[], 0);
    assert_final_mutant_rejected("wrong-profile-type", &wrong_profile_type);
    let mut missing_vdim = final_records.clone();
    missing_vdim[vector_index]["known_keys"][vector_key_index]["before_cleanup"]["observations"]
        .as_array_mut()
        .expect("Vector observations must be an array")
        .remove(1);
    assert_final_mutant_rejected("missing-vdim", &missing_vdim);
    let mut missing_vemb = final_records.clone();
    missing_vemb[vector_index]["known_keys"][vector_key_index]["before_cleanup"]["observations"]
        .as_array_mut()
        .expect("Vector observations must be an array")
        .remove(2);
    assert_final_mutant_rejected("missing-vemb", &missing_vemb);
    let mut inconsistent_card = final_records.clone();
    replace_exchange_response(
        &mut inconsistent_card[vector_index]["known_keys"][vector_key_index]["before_cleanup"]["observations"]
            [0],
        b":0\r\n",
    );
    assert_final_mutant_rejected("inconsistent-vcard-vemb", &inconsistent_card);
    let mut missing_profile_member = final_records.clone();
    replace_exchange_response(
        &mut missing_profile_member[vector_index]["known_keys"][vector_key_index]["before_cleanup"]
            ["observations"][0],
        b":7\r\n",
    );
    replace_exchange_response(
        &mut missing_profile_member[vector_index]["known_keys"][vector_key_index]["before_cleanup"]
            ["observations"][2],
        b"$-1\r\n",
    );
    assert_final_mutant_rejected("missing-profile-member", &missing_profile_member);
    let mut populated_ghost = final_records.clone();
    let ghost_observation = populated_ghost[vector_index]["known_keys"][vector_key_index]
        ["before_cleanup"]["observations"]
        .as_array_mut()
        .expect("Vector observations must be an array")
        .last_mut()
        .expect("Vector observations must include the ghost member");
    replace_exchange_response(ghost_observation, &resp_vector(4, 2));
    assert_final_mutant_rejected("populated-ghost", &populated_ghost);
    let mut empty_member_vector = final_records.clone();
    replace_exchange_response(
        &mut empty_member_vector[vector_index]["known_keys"][vector_key_index]["before_cleanup"]["observations"]
            [2],
        b"*0\r\n",
    );
    assert_final_mutant_rejected("empty-member-vector", &empty_member_vector);
    let mut missing_pttl = final_records.clone();
    missing_pttl[server_index]["known_keys"][0]["before_cleanup"]
        .as_object_mut()
        .expect("before_cleanup must be an object")
        .remove("pttl");
    assert_final_mutant_rejected("missing-pttl", &missing_pttl);
    let mut wrong_persistent = final_records.clone();
    wrong_persistent[vector_index]["known_keys"][vector_key_index]["before_cleanup"]["pttl"]["kiwi_response_base64"] =
        json!("Oi0yDQo=");
    wrong_persistent[vector_index]["known_keys"][vector_key_index]["before_cleanup"]["pttl"]["kiwi_response_sha256"] =
        json!("b905573911645991d118a4cd4f110a3661d4574d51339a3b489eeaf7ac5383c9");
    wrong_persistent[vector_index]["known_keys"][vector_key_index]["before_cleanup"]["pttl"]["redis_response_base64"] =
        json!("Oi0yDQo=");
    wrong_persistent[vector_index]["known_keys"][vector_key_index]["before_cleanup"]["pttl"]["redis_response_sha256"] =
        json!("b905573911645991d118a4cd4f110a3661d4574d51339a3b489eeaf7ac5383c9");
    assert_final_mutant_rejected("wrong-minus-one", &wrong_persistent);
    let mut wrong_missing = final_records.clone();
    wrong_missing[server_index]["known_keys"][0]["cleanup"]["after_pttl"]["kiwi_response_base64"] =
        json!("Oi0xDQo=");
    wrong_missing[server_index]["known_keys"][0]["cleanup"]["after_pttl"]["kiwi_response_sha256"] =
        json!("8302d7e43fdb0dc1797e8e2a2ef4bd9525450fd23e88c6390af84f32dd5cdf99");
    wrong_missing[server_index]["known_keys"][0]["cleanup"]["after_pttl"]["redis_response_base64"] =
        json!("Oi0xDQo=");
    wrong_missing[server_index]["known_keys"][0]["cleanup"]["after_pttl"]["redis_response_sha256"] =
        json!("8302d7e43fdb0dc1797e8e2a2ef4bd9525450fd23e88c6390af84f32dd5cdf99");
    assert_final_mutant_rejected("wrong-minus-two", &wrong_missing);

    let evidence = scratch.join("evidence");
    fs::create_dir(&evidence).expect("create evidence directory");
    fs::copy(&transcript, evidence.join("raw-transcript.jsonl")).expect("copy transcript fixture");
    fs::copy(&final_state, evidence.join("final-state.jsonl")).expect("copy final-state fixture");
    fs::write(
        evidence.join("vector-required-jobs.json"),
        helper_output.stdout,
    )
    .expect("write canonical registry fixture");
    fs::write(evidence.join("kiwi.conf"), "port 7379\n").expect("write config fixture");
    fs::write(evidence.join("kiwi.log"), "ready\n").expect("write Kiwi log fixture");
    fs::write(
        evidence.join("kiwi-runtime.json"),
        r#"{"schema_version":"kiwi-runtime-identity/v1","pid":1,"binary_path":"/callback-input/target/debug/kiwi","binary_sha256":"0000000000000000000000000000000000000000000000000000000000000000","binary_identity":{"device":1,"inode":1,"mode":33261,"size":1,"nlink":1},"executable_identity_equal":true}
"#,
    )
    .expect("write Kiwi runtime identity fixture");
    fs::write(
        evidence.join("callback-cleanup.json"),
        r#"{"schema_version":"kiwi-vector-callback-cleanup/v1","kiwi_process_reaped":true,"data_directory_removed":true,"log_directory_removed":true,"no_unexpected_work_residue":true}
"#,
    )
    .expect("write callback cleanup fixture");
    fs::write(
        evidence.join("collect.log"),
        format!("{}\n", node_ids.join("\n")),
    )
    .expect("write collection fixture");
    fs::write(evidence.join("pytest.log"), "40 passed\n").expect("write pytest log fixture");
    let collect_summary = r#"{"collected":40,"passed":0,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#;
    let run_summary = r#"{"collected":40,"passed":40,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#;
    fs::write(evidence.join("collect-summary.json"), collect_summary)
        .expect("write collection summary fixture");
    fs::write(evidence.join("run-summary.json"), run_summary).expect("write run summary fixture");
    let duplicate_canonical = scratch.join("duplicate-canonical.json");
    let canonical_text = fs::read_to_string(evidence.join("vector-required-jobs.json"))
        .expect("read canonical registry fixture");
    let duplicate_canonical_text = canonical_text.replacen(
        "\"schema\":",
        "\"schema\":\"invalid-duplicate\",\"schema\":",
        1,
    );
    assert_ne!(duplicate_canonical_text, canonical_text);
    fs::write(&duplicate_canonical, duplicate_canonical_text)
        .expect("write duplicate canonical registry mutant");
    let duplicate_helper = scratch.join("duplicate-required-jobs-helper");
    fs::write(
        &duplicate_helper,
        "#!/usr/bin/env bash\n/usr/bin/cat -- \"$(dirname -- \"$0\")/duplicate-canonical.json\"\n",
    )
    .expect("write duplicate canonical helper");
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&duplicate_helper)
            .expect("read duplicate helper mode")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&duplicate_helper, permissions)
            .expect("make duplicate canonical helper executable");
    }
    for (validator, artifact) in [
        ("--validate-collection", evidence.join("collect.log")),
        ("--validate-summary", evidence.join("run-summary.json")),
        (
            "--validate-collect-summary",
            evidence.join("collect-summary.json"),
        ),
        (
            "--validate-raw-transcript",
            evidence.join("raw-transcript.jsonl"),
        ),
        ("--validate-final-state", evidence.join("final-state.jsonl")),
    ] {
        let output = runner_command(&runner)
            .env("KIWI_COMPAT_TEST_REQUIRED_JOBS_HELPER", &duplicate_helper)
            .arg(validator)
            .arg(&registry)
            .arg(artifact)
            .output()
            .expect("duplicate canonical validator must start");
        let output_text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !output.status.success(),
            "{validator} accepted duplicate canonical JSON"
        );
        assert!(
            output_text.contains("duplicate JSON object key"),
            "{validator} failed for the wrong reason: {output_text}"
        );
    }
    let validate_set = || {
        runner_command(&runner)
            .arg("--validate-evidence-set")
            .arg(&registry)
            .arg(&evidence)
            .status()
            .expect("evidence-set validator must start")
            .success()
    };
    assert!(validate_set());
    let duplicate_run_summary = r#"{"collected":0,"collected":40,"passed":40,"failed":1,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#;
    fs::write(evidence.join("run-summary.json"), duplicate_run_summary)
        .expect("write duplicate run summary mutant");
    assert!(!validate_set(), "accepted duplicate run-summary JSON key");
    fs::write(evidence.join("run-summary.json"), run_summary).expect("restore run summary fixture");
    let duplicate_collect_summary = r#"{"collected":0,"collected":40,"passed":1,"passed":0,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#;
    fs::write(
        evidence.join("collect-summary.json"),
        duplicate_collect_summary,
    )
    .expect("write duplicate collection summary mutant");
    assert!(
        !validate_set(),
        "accepted duplicate collect-summary JSON key"
    );
    fs::write(evidence.join("collect-summary.json"), collect_summary)
        .expect("restore collection summary fixture");
    for (name, mutant) in [
        (
            "float",
            r#"{"collected":40.0,"passed":0,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#,
        ),
        (
            "boolean",
            r#"{"collected":40,"passed":false,"failed":false,"skipped":false,"xfailed":false,"xpassed":false,"deselected":false}"#,
        ),
        (
            "negative",
            r#"{"collected":40,"passed":0,"failed":-1,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#,
        ),
        (
            "string",
            r#"{"collected":40,"passed":0,"failed":"0","skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#,
        ),
    ] {
        fs::write(evidence.join("collect-summary.json"), mutant)
            .expect("write invalid collection summary counter mutant");
        assert!(
            !validate_set(),
            "accepted {name} collect-summary counter mutant"
        );
    }
    fs::write(evidence.join("collect-summary.json"), collect_summary)
        .expect("restore collection summary fixture");
    fs::create_dir(evidence.join("kiwi-data")).expect("create unexpected runtime directory");
    fs::write(evidence.join("kiwi-data/unexpected.bin"), "not evidence")
        .expect("write nested extra evidence mutant");
    assert!(!validate_set(), "accepted nested extra evidence artifact");
    fs::remove_dir_all(evidence.join("kiwi-data")).expect("remove nested extra evidence mutant");
    fs::create_dir(evidence.join("kiwi-log")).expect("create unexpected log directory");
    std::os::unix::fs::symlink(
        evidence.join("kiwi.log"),
        evidence.join("kiwi-log/unexpected-link"),
    )
    .expect("create nested symlink evidence mutant");
    assert!(!validate_set(), "accepted nested symlink evidence artifact");
    fs::remove_dir_all(evidence.join("kiwi-log")).expect("remove nested symlink evidence mutant");
    fs::write(evidence.join("extra-artifact.txt"), "not allowed")
        .expect("write extra artifact mutant");
    assert!(!validate_set(), "accepted extra evidence artifact");
    fs::remove_file(evidence.join("extra-artifact.txt")).expect("remove extra artifact mutant");
    let kiwi_log = fs::OpenOptions::new()
        .write(true)
        .open(evidence.join("kiwi.log"))
        .expect("open Kiwi log mutant");
    kiwi_log
        .set_len(8 * 1024 * 1024 + 1)
        .expect("extend Kiwi log beyond its bound");
    assert!(!validate_set(), "accepted oversized log evidence");
    fs::write(evidence.join("kiwi.log"), "ready\n").expect("restore Kiwi log fixture");
    let final_state_file = fs::OpenOptions::new()
        .write(true)
        .open(evidence.join("final-state.jsonl"))
        .expect("open final-state mutant");
    final_state_file
        .set_len(4 * 1024 * 1024 + 1)
        .expect("extend final-state evidence beyond its bound");
    assert!(!validate_set(), "accepted oversized final-state evidence");

    fs::remove_dir_all(&scratch).expect("remove strict evidence scratch directory");
}
