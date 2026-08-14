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
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::process::Command;

use serde::Deserialize;

#[cfg(target_os = "linux")]
fn runner_command(runner: &std::path::Path) -> Command {
    let mut command = Command::new("/usr/bin/bash");
    command.arg(runner).env(
        "KIWI_COMPAT_TEST_REQUIRED_JOBS_HELPER",
        env!("CARGO_BIN_EXE_kiwi-required-vector-jobs"),
    );
    command
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
    #[serde(rename = "if-no-files-found")]
    if_no_files_found: Option<String>,
}

const VECTOR_CLUSTER_JOB: &str = "vector-cluster-fail-closed";
const TRUSTED_VECTOR_JOB: &str = "trusted-vector-differential";
const STATIC_ANALYSIS_JOB: &str = "static-analysis";
const RKYV_TREE_COMMAND: &str =
    "cargo tree --locked --offline --target all --all-features -i rkyv@0.7.46";
const RKYV_SENTINEL_COMMAND: &str = "bash scripts/ci/check-rkyv-reachability.sh";
const GRPCURL_URL: &str = "https://github.com/fullstorydev/grpcurl/releases/download/v1.9.3/grpcurl_1.9.3_linux_x86_64.tar.gz";
const GRPCURL_ARCHIVE_SHA256: &str =
    "a926b62a85787ccf73ef8736b3ae554f1242e39d92bb8767a79d6dd23b11d1d5";
const GRPCURL_OUTPUT: &str = "-o \"$RUNNER_TEMP/grpcurl.tar.gz\"";
const GRPCURL_CHECKSUM_VERIFY: &str = "| (cd \"$RUNNER_TEMP\" && sha256sum -c -)";
const GRPCURL_EXTRACT: &str =
    "tar -xzf \"$RUNNER_TEMP/grpcurl.tar.gz\" -C \"$RUNNER_TEMP\" grpcurl";

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
        if !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
        {
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
    expanded
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

    let runner = find_only_step(job, "verifier-supervised differential runner", |step| {
        step.run.as_deref().is_some_and(|command| {
            [
                "KIWI_COMPAT_REQUIRE_ORACLE=1",
                "KIWI_REDIS_ORACLE_SOURCE=\"$RUNNER_TEMP/kiwi-oracle/redis-source\"",
                "KIWI_REDIS_ORACLE_PRIMARY_METADATA=\"$RUNNER_TEMP/kiwi-oracle/primary-build.json\"",
                "KIWI_REDIS_ORACLE_OUTPUT=\"$RUNNER_TEMP/kiwi-oracle/oracle-provenance.json\"",
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
    if upload_step.uses.as_deref() != Some("actions/upload-artifact@v7")
        || upload_step.with.name.as_deref() != Some("trusted-vector-oracle-provenance")
        || upload_step.with.path.as_deref()
            != Some("${{ runner.temp }}/kiwi-oracle/oracle-provenance.json")
        || upload_step.with.if_no_files_found.as_deref() != Some("error")
    {
        return Err(format!(
            "{TRUSTED_VECTOR_JOB} may upload only the final post-cleanup provenance file"
        ));
    }
    if runner >= upload {
        return Err(format!(
            "{TRUSTED_VECTOR_JOB} provenance upload must follow the verifier-supervised runner"
        ));
    }
    if job.steps[runner].condition.is_some() || job.steps[upload].condition.is_some() {
        return Err(format!(
            "{TRUSTED_VECTOR_JOB} runner and upload steps cannot be conditional"
        ));
    }
    Ok(())
}

fn validate_vector_differential_runner_source(source: &str) -> Result<(), String> {
    let endpoint_guard = "[[ -z ${KIWI_REDIS_ORACLE_HOST:-} ]] \\\n    || die 'Oracle endpoint variables are only accepted inside the verifier callback'";
    if source.matches(endpoint_guard).count() != 1 {
        return Err("outer differential runner must reject ambient Oracle endpoints".to_string());
    }
    let verifier = "scripts/compat/verify-redis-8.8.1.sh \\\n    --source \"$KIWI_REDIS_ORACLE_SOURCE\" \\\n    --primary-metadata \"$KIWI_REDIS_ORACLE_PRIMARY_METADATA\" \\\n    --output \"$KIWI_REDIS_ORACLE_OUTPUT\" \\\n    --callback-input \"$repository_root\" \\\n    --run-after-ready /bin/bash \\\n    /callback-input/scripts/compat/run-vector-differential.sh --callback";
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
        "Oracle provenance was published before complete cleanup",
        "Oracle provenance publication order is invalid",
    ] {
        if !outer.contains(required) {
            return Err(format!(
                "differential runner is missing post-cleanup proof: {required}"
            ));
        }
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
    let workflow_source = include_str!("../../../.github/workflows/ci.yml");
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

    let workflow: Workflow = yaml_serde::from_str(workflow_source).expect("CI workflow must parse");
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
        assert_ne!(mutant, workflow_source, "failed to construct {name} mutant");
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
    let workflow_source = include_str!("../../../.github/workflows/ci.yml");
    assert_eq!(
        workflow_source
            .lines()
            .filter(|line| *line == "  static-analysis:")
            .count(),
        1
    );
    let workflow: Workflow = yaml_serde::from_str(workflow_source).expect("CI workflow must parse");
    validate_rkyv_static_analysis_workflow(&workflow)
        .expect("static analysis must contain the required fail-closed rkyv gate");
}

#[test]
fn rkyv_static_analysis_contract_rejects_removal_reordering_and_continue_on_error() {
    let source = include_str!("../../../.github/workflows/ci.yml");
    let workflow: Workflow = yaml_serde::from_str(source).expect("CI workflow must parse");

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
    let source = include_str!("../../../.github/workflows/ci.yml").replace("\r\n", "\n");
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
    let source = include_str!("../../../scripts/ci/check-rkyv-reachability.sh");
    validate_rkyv_sentinel_source(source).expect("rkyv sentinel source must be fail closed");

    let ignored_stdout = source.replacen(
        "if [[ -s \"$stdout_file\" ]]; then",
        "if [[ ! -s \"$stdout_file\" ]]; then",
        1,
    );
    assert!(validate_rkyv_sentinel_source(&ignored_stdout).is_err());
}

#[test]
fn rkyv_audit_ignore_has_owner_path_status_and_removal_condition() {
    let source = include_str!("../../../.cargo/audit.toml");
    validate_rkyv_audit_governance(source)
        .expect("rkyv advisory ignore must carry accurate governance");

    let removed_advisory = source.replacen("  \"RUSTSEC-2026-0235\",\n", "", 1);
    assert!(
        validate_rkyv_audit_governance(&removed_advisory).is_err(),
        "audit governance accepted removal of the governed advisory ignore"
    );
}

#[test]
fn rkyv_security_workflow_remains_scheduled_visibility() {
    let source = include_str!("../../../.github/workflows/security.yml");
    assert!(source.contains("  schedule:"));
    let workflow: Workflow = yaml_serde::from_str(source).expect("security workflow must parse");
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
    let mut workflow: Workflow =
        yaml_serde::from_str(include_str!("../../../.github/workflows/ci.yml"))
            .expect("CI workflow must parse");
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
    let mut workflow: Workflow =
        yaml_serde::from_str(include_str!("../../../.github/workflows/ci.yml"))
            .expect("CI workflow must parse");
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
    let mut workflow: Workflow =
        yaml_serde::from_str(include_str!("../../../.github/workflows/ci.yml"))
            .expect("CI workflow must parse");
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

    let cluster_tests = include_str!("../../../tests/python/test_vector_cluster.py");
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
    let workflow_source = include_str!("../../../.github/workflows/ci.yml");
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
    let workflow: Workflow = yaml_serde::from_str(workflow_source).expect("CI workflow must parse");
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
fn required_jobs_reject_unversioned_actions() {
    let workflow_source = include_str!("../../../.github/workflows/ci.yml");
    let workflow: Workflow = yaml_serde::from_str(workflow_source).expect("CI workflow must parse");
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
fn vector_differential_upload_requires_explicit_missing_file_error() {
    let workflow_source = include_str!("../../../.github/workflows/ci.yml");
    let workflow: Workflow =
        yaml_serde::from_str(workflow_source).expect("CI workflow must remain valid YAML");
    validate_vector_differential_workflow(&workflow)
        .expect("explicit error-on-missing provenance must remain accepted");

    let missing_policy = workflow_source.replacen("\n          if-no-files-found: error", "", 1);
    assert_ne!(
        missing_policy, workflow_source,
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
fn vector_differential_make_simple_assignment_freezes_earlier_value() {
    let makefile = include_str!("../../../tests/Makefile");
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
    let makefile = include_str!("../../../tests/Makefile");
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
    let workflow_source = include_str!("../../../.github/workflows/ci.yml");
    let workflow: Workflow = yaml_serde::from_str(workflow_source).expect("CI workflow must parse");
    validate_vector_differential_workflow(&workflow)
        .expect("trusted differential workflow must be fail closed");

    let runner_source = include_str!("../../../scripts/compat/run-vector-differential.sh");
    validate_vector_differential_runner_source(runner_source)
        .expect("trusted differential runner must obtain its runtime from the verifier");

    let runner_bypass = workflow_source.replacen(
        "          KIWI_COMPAT_REQUIRE_ORACLE=1 \\\n          KIWI_REDIS_ORACLE_SOURCE=\"$RUNNER_TEMP/kiwi-oracle/redis-source\" \\\n          KIWI_REDIS_ORACLE_PRIMARY_METADATA=\"$RUNNER_TEMP/kiwi-oracle/primary-build.json\" \\\n          KIWI_REDIS_ORACLE_OUTPUT=\"$RUNNER_TEMP/kiwi-oracle/oracle-provenance.json\" \\\n            bash scripts/compat/run-vector-differential.sh",
        "          redis-cli -h \"${KIWI_REDIS_ORACLE_HOST:-127.0.0.1}\" \\\n            -p \"${KIWI_REDIS_ORACLE_PORT:-6379}\" PING",
        1,
    );
    assert_ne!(
        runner_bypass, workflow_source,
        "failed to construct supervisor bypass mutant"
    );

    let upload_before_cleanup = workflow_source.replacen(
        "      - name: Run required trusted Vector differential\n        run: |\n          KIWI_COMPAT_REQUIRE_ORACLE=1",
        "      - name: Upload trusted Oracle provenance\n        uses: actions/upload-artifact@v7\n        with:\n          name: premature-provenance\n          path: ${{ runner.temp }}/kiwi-oracle/oracle-provenance.json\n\n      - name: Run required trusted Vector differential\n        run: |\n          KIWI_COMPAT_REQUIRE_ORACLE=1",
        1,
    );
    assert_ne!(
        upload_before_cleanup, workflow_source,
        "failed to construct premature upload mutant"
    );

    let unsafe_upload = workflow_source.replacen(
        "          path: ${{ runner.temp }}/kiwi-oracle/oracle-provenance.json",
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
            "      - name: Upload trusted Oracle provenance\n        uses: actions/upload-artifact@v7",
            "      - name: Upload trusted Oracle provenance\n        if: always()\n        uses: actions/upload-artifact@v7",
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
    ] {
        assert_ne!(mutant, runner_source, "failed to construct {name} runner mutant");
        assert!(
            validate_vector_differential_runner_source(&mutant).is_err(),
            "trusted differential runner accepted {name} mutant"
        );
    }
}

#[test]
fn vector_differential_fast_job_uses_marker_ownership_not_path_ignore() {
    let makefile = include_str!("../../../tests/Makefile");
    assert!(!has_vector_differential_path_ignore(makefile));
    assert!(makefile.contains("-m \"not raw_vector_protocol\""));
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

    let runner = include_str!("../../../scripts/compat/run-vector-differential.sh");
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
        "validate_raw_coverage /work/vector-required-jobs.json",
        "validate_summary /work/vector-required-jobs.json",
    ] {
        assert!(
            runner.contains(validator),
            "callback validator bypasses canonical JSON: {validator}"
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
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&scratch).unwrap();
    let collection = scratch.join("collection.log");
    let summary = scratch.join("summary.json");
    let passing = r#"{"collected":40,"passed":40,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#;
    let yaml = fs::read_to_string(&registry).unwrap();
    let node_ids = yaml
        .lines()
        .filter_map(|line| line.strip_prefix("      - tests/python/"))
        .map(|line| format!("tests/python/{line}"))
        .collect::<Vec<_>>();
    assert_eq!(node_ids.len(), 40);

    fs::write(&collection, format!("{}\n", node_ids.join("\n"))).unwrap();
    fs::write(&summary, passing).unwrap();
    assert!(
        runner_command(&runner)
            .arg("--validate-collection")
            .arg(&registry)
            .arg(&collection)
            .status()
            .unwrap()
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
        fs::write(&mutant_registry, mutant).unwrap();
        assert!(
            !runner_command(&runner)
                .arg("--validate-collection")
                .arg(&mutant_registry)
                .arg(&collection)
                .status()
                .unwrap()
                .success(),
            "runner accepted {name} registry mutant"
        );
        assert!(
            !runner_command(&runner)
                .arg("--validate-summary")
                .arg(&mutant_registry)
                .arg(&summary)
                .status()
                .unwrap()
                .success(),
            "summary validator accepted {name} registry mutant"
        );
    }
    fs::write(&collection, "27 tests collected\n").unwrap();
    assert!(
        !runner_command(&runner)
            .arg("--validate-collection")
            .arg(&registry)
            .arg(&collection)
            .status()
            .unwrap()
            .success()
    );
    let mut drifted_node_ids = node_ids.clone();
    drifted_node_ids[0] =
        "tests/python/test_vector_set_differential.py::test_unregistered_node".to_string();
    fs::write(&collection, format!("{}\n", drifted_node_ids.join("\n"))).unwrap();
    assert!(
        !runner_command(&runner)
            .arg("--validate-collection")
            .arg(&registry)
            .arg(&collection)
            .status()
            .unwrap()
            .success()
    );

    assert!(
        runner_command(&runner)
            .arg("--validate-summary")
            .arg(&registry)
            .arg(&summary)
            .status()
            .unwrap()
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
        fs::write(&summary, mutant).unwrap();
        assert!(
            !runner_command(&runner)
                .arg("--validate-summary")
                .arg(&registry)
                .arg(&summary)
                .status()
                .unwrap()
                .success()
        );
    }

    let unavailable = runner_command(&runner)
        .env_clear()
        .env("OSTYPE", "linux-gnu")
        .env("KIWI_COMPAT_REQUIRE_ORACLE", "1")
        .status()
        .unwrap();
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
        .unwrap();
    assert!(!identity_mismatch.success());
    let runtime_evidence = scratch.join("runtime-evidence.json");
    let valid_runtime = r#"{"build_role":"rebuild","binary_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","binary_identity":{"device":1,"inode":2,"mode":33261,"size":3,"nlink":1},"held_fd":true,"pid":42,"info_redis_versions":["8.8.1"]}"#;
    fs::write(&runtime_evidence, valid_runtime).unwrap();
    assert!(
        runner_command(&runner)
            .arg("--validate-runtime-evidence")
            .arg(&runtime_evidence)
            .status()
            .unwrap()
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
        fs::write(&runtime_evidence, mutant).unwrap();
        assert!(
            !runner_command(&runner)
                .arg("--validate-runtime-evidence")
                .arg(&runtime_evidence)
                .status()
                .unwrap()
                .success()
        );
    }
    assert!(
        !runner_command(&runner)
            .args(["--validate-callback-result", "0", "1"])
            .status()
            .unwrap()
            .success()
    );

    fs::remove_dir_all(&scratch).unwrap();
}

#[test]
#[cfg(target_os = "linux")]
fn vector_differential_runner_requires_observed_raw_coverage_for_every_command() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let runner = root.join("scripts/compat/run-vector-differential.sh");
    let registry = root.join("tests/compat/redis-8.8.1/vector-required-jobs.yaml");
    let scratch = std::env::temp_dir().join(format!(
        "kiwi-vector-raw-coverage-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&scratch).unwrap();
    let coverage = scratch.join("raw-coverage.jsonl");
    let node_ids = [
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
    let mut records = String::new();
    for node_id in node_ids {
        let protocol = if node_id.ends_with("[resp2]") { 2 } else { 3 };
        for (command, case_id) in exact_cases {
            records.push_str(&format!(
                "{{\"case_id\":\"{case_id}\",\"command\":\"{command}\",\"evidence_kind\":\"exact-frame\",\"node_id\":\"{node_id}\",\"protocol\":{protocol},\"kiwi_frame_sha256\":\"{}\",\"redis_frame_sha256\":\"{}\"}}\n",
                "a".repeat(64),
                "a".repeat(64)
            ));
        }
        records.push_str(&format!(
            "{{\"case_id\":\"populated\",\"command\":\"VINFO\",\"evidence_kind\":\"raw-schema\",\"node_id\":\"{node_id}\",\"protocol\":{protocol},\"kiwi_frame_sha256\":\"{}\",\"redis_frame_sha256\":\"{}\"}}\n",
            "a".repeat(64),
            "b".repeat(64)
        ));
    }
    fs::write(&coverage, &records).unwrap();
    let validate = |registry_path: &std::path::Path, coverage_path: &std::path::Path| {
        runner_command(&runner)
            .arg("--validate-raw-coverage")
            .arg(registry_path)
            .arg(coverage_path)
            .status()
            .unwrap()
            .success()
    };
    assert!(validate(&registry, &coverage));
    let yaml = fs::read_to_string(&registry).unwrap();
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
        fs::write(&mutant_registry, mutant).unwrap();
        assert!(
            !validate(&mutant_registry, &coverage),
            "raw coverage validator accepted {name} registry mutant"
        );
    }

    let missing_vcard = records
        .lines()
        .filter(|line| !(line.contains("\"command\":\"VCARD\"") && line.contains("[resp3]")))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&coverage, format!("{missing_vcard}\n")).unwrap();
    assert!(!validate(&registry, &coverage));

    let typed_equivalence = records.replacen(
        &format!(
            "\"evidence_kind\":\"exact-frame\",\"node_id\":\"{}\",\"protocol\":2,\"kiwi_frame_sha256\":\"{}\"",
            node_ids[0],
            "a".repeat(64)
        ),
        &format!(
            "\"evidence_kind\":\"exact-frame\",\"node_id\":\"{}\",\"protocol\":2,\"kiwi_frame_sha256\":\"{}\"",
            node_ids[0],
            "b".repeat(64)
        ),
        1,
    );
    fs::write(&coverage, typed_equivalence).unwrap();
    assert!(!validate(&registry, &coverage));

    let without_populated_vinfo = records
        .lines()
        .filter(|line| {
            !(line.contains("\"command\":\"VINFO\"")
                && line.contains("\"case_id\":\"populated\"")
                && line.contains("[resp3]"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&coverage, format!("{without_populated_vinfo}\n")).unwrap();
    assert!(!validate(&registry, &coverage));

    let wrong_evidence_kind = records.replacen(
        "\"case_id\":\"populated\",\"command\":\"VINFO\",\"evidence_kind\":\"raw-schema\"",
        "\"case_id\":\"populated\",\"command\":\"VINFO\",\"evidence_kind\":\"exact-frame\"",
        1,
    );
    fs::write(&coverage, wrong_evidence_kind).unwrap();
    assert!(!validate(&registry, &coverage));

    fs::remove_dir_all(&scratch).unwrap();
}
