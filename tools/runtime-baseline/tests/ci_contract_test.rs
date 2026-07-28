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
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[allow(dead_code)]
#[path = "../build_support.rs"]
mod build_support;

const STABLE_PLATFORMS: [&str; 3] = ["ubuntu-latest", "macos-latest", "windows-latest"];

#[derive(Deserialize)]
struct Workflow {
    jobs: BTreeMap<String, Job>,
}

#[derive(Deserialize)]
struct Job {
    strategy: Option<Strategy>,
    #[serde(default)]
    steps: Vec<Step>,
    #[serde(rename = "runs-on")]
    runs_on: Option<String>,
    #[serde(rename = "if")]
    condition: Option<yaml_serde::Value>,
    #[serde(rename = "continue-on-error")]
    continue_on_error: Option<yaml_serde::Value>,
}

#[derive(Deserialize)]
struct Strategy {
    matrix: Matrix,
}

#[derive(Deserialize)]
struct Matrix {
    #[serde(default)]
    include: Vec<MatrixEntry>,
}

#[derive(Deserialize)]
struct MatrixEntry {
    os: Option<String>,
}

#[derive(Deserialize)]
struct Step {
    run: Option<String>,
    #[serde(rename = "if")]
    condition: Option<yaml_serde::Value>,
    #[serde(rename = "continue-on-error")]
    continue_on_error: Option<yaml_serde::Value>,
}

#[test]
fn stable_platform_matrix_verifies_runtime_baseline_explicitly() {
    let workflow = read_workspace_file(".github/workflows/ci.yml");
    validate_ci_contract(&workflow).expect("stable CI must verify the runtime baseline tool");
}

#[test]
fn benchmark_check_compiles_the_runtime_baseline_binary() {
    let workflow = read_workspace_file(".github/workflows/benchmark.yml");
    validate_benchmark_contract(&workflow)
        .expect("Compile Benchmarks must link the runtime baseline binary explicitly");
}

#[test]
fn ci_contract_rejects_commented_commands_and_missing_platforms() {
    let workflow = read_workspace_file(".github/workflows/ci.yml");
    let commented_build = workflow.replace(
        "run: cargo build -p runtime-baseline --bin kiwi-runtime-baseline",
        "run: '# cargo build -p runtime-baseline --bin kiwi-runtime-baseline'",
    );
    assert!(validate_ci_contract(&commented_build).is_err());

    let missing_windows = workflow.replace("          - os: windows-latest\n", "");
    assert!(validate_ci_contract(&missing_windows).is_err());

    let conditional_test = workflow.replace(
        "      - name: Test runtime baseline tool\n        run:",
        "      - name: Test runtime baseline tool\n        if: false\n        run:",
    );
    assert!(validate_ci_contract(&conditional_test).is_err());

    let ignored_test_failure = workflow.replace(
        "      - name: Test runtime baseline tool\n        run:",
        "      - name: Test runtime baseline tool\n        continue-on-error: true\n        run:",
    );
    assert!(validate_ci_contract(&ignored_test_failure).is_err());

    let pinned_runner = workflow.replace("runs-on: ${{ matrix.os }}", "runs-on: ubuntu-latest");
    assert!(validate_ci_contract(&pinned_runner).is_err());
}

#[test]
fn benchmark_contract_rejects_a_commented_build() {
    let workflow = read_workspace_file(".github/workflows/benchmark.yml");
    let commented = workflow.replace(
        "          cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline",
        "          # cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline",
    );
    assert!(validate_benchmark_contract(&commented).is_err());
}

#[test]
fn benchmark_contract_rejects_a_weakened_or_misordered_clean_check() {
    let workflow = read_workspace_file(".github/workflows/benchmark.yml");
    let clean = source_clean_assignment();

    let commented = workflow.replace(&clean, &format!("# {clean}"));
    assert!(validate_benchmark_contract(&commented).is_err());

    let unbounded = workflow.replace(&clean, "dirty=\"$(git status --porcelain)\"");
    assert!(validate_benchmark_contract(&unbounded).is_err());

    let missing_condition = workflow.replace("          if [[ -n \"$dirty\" ]]; then\n", "");
    assert!(validate_benchmark_contract(&missing_condition).is_err());

    let missing_failure = workflow.replace("            exit 1\n", "");
    assert!(validate_benchmark_contract(&missing_failure).is_err());

    let ignored_failure = workflow.replace("            exit 1\n", "            exit 0\n");
    assert!(validate_benchmark_contract(&ignored_failure).is_err());

    let conditional = workflow.replace(
        "      - name: Verify runtime baseline source inputs are clean\n        shell: bash",
        "      - name: Verify runtime baseline source inputs are clean\n        if: false\n        shell: bash",
    );
    assert!(validate_benchmark_contract(&conditional).is_err());

    let after_build = workflow
        .replace(&clean, "true")
        .replace(
            "          cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline",
            &format!(
                "          cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline\n          {clean}"
            ),
        );
    assert!(validate_benchmark_contract(&after_build).is_err());
}

#[test]
fn runtime_baseline_docs_define_separate_run_and_verify_commands() {
    for path in [
        "docs/superpowers/specs/2026-07-24-storage-runtime-baseline-design.md",
        "docs/superpowers/plans/2026-07-24-storage-runtime-baseline-harness.md",
    ] {
        let contents = read_workspace_file(path);
        assert!(
            !contents.contains("run_baseline.py \\\n  --smoke"),
            "{path} still documents the removed top-level --smoke form"
        );
        for required in [
            "MEMTIER_PREFIX=\"${MEMTIER_PREFIX:?set MEMTIER_PREFIX to the memtier install prefix}\"",
            "TMPDIR=\"${TMPDIR:-${RUNNER_TEMP:-/tmp}}\"",
            "RESULTS_ROOT=\"$TMPDIR/runtime-baseline-results\"",
            "run_baseline.py run",
            "run_baseline.py verify",
            "--suite smoke",
            "--cases tools/runtime-baseline/cases/cases.yaml",
            "--results-root \"$RESULTS_ROOT\"",
            "--expected-git-sha \"$(git rev-parse HEAD)\"",
        ] {
            assert!(
                contents.contains(required),
                "{path} is missing documented controller contract `{required}`"
            );
        }
        assert_eq!(
            contents.matches("--results-root \"$RESULTS_ROOT\"").count(),
            2,
            "{path} must use the same explicit results root for run and verify"
        );
    }
}

fn read_workspace_file(path: &str) -> String {
    fs::read_to_string(workspace_root().join(path))
        .expect("workflow file must be readable")
        .replace("\r\n", "\n")
}

fn validate_ci_contract(contents: &str) -> Result<(), String> {
    let workflow = parse_workflow(contents)?;
    let build_job = required_job(&workflow, "build-and-test")?;
    let clippy_job = required_job(&workflow, "cargo-clippy")?;
    require_stable_platforms(build_job, "build-and-test")?;
    require_stable_platforms(clippy_job, "cargo-clippy")?;
    require_matrix_runner(build_job, "build-and-test")?;
    require_matrix_runner(clippy_job, "cargo-clippy")?;
    require_unconditional_command(
        build_job,
        "cargo build -p runtime-baseline --bin kiwi-runtime-baseline",
    )?;
    require_unconditional_command(build_job, "cargo test -p runtime-baseline --all-targets")?;
    require_unconditional_command(
        clippy_job,
        "cargo clippy -p runtime-baseline --all-targets -- -D warnings -D clippy::unwrap_used",
    )
}

fn validate_benchmark_contract(contents: &str) -> Result<(), String> {
    let workflow = parse_workflow(contents)?;
    let benchmark_job = required_job(&workflow, "benchmark")?;
    let clean = require_fail_closed_clean_gate(benchmark_job)?;
    let build = require_unconditional_command_location(
        benchmark_job,
        "cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline",
    )?;
    if clean < build {
        Ok(())
    } else {
        Err("runtime baseline source clean check must run before the release build".to_string())
    }
}

fn require_fail_closed_clean_gate(job: &Job) -> Result<(usize, usize), String> {
    let assignment = source_clean_assignment();
    let (step_index, step, assignment_index) = find_command(job, &assignment)?;
    if step.condition.is_some() || step.continue_on_error.is_some() {
        return Err(
            "runtime baseline source clean gate must be unconditional and blocking".to_string(),
        );
    }

    let lines = step
        .run
        .as_deref()
        .expect("command step has a run body")
        .lines()
        .map(str::trim)
        .collect::<Vec<_>>();
    let strict_index = lines
        .iter()
        .position(|line| *line == "set -Eeuo pipefail")
        .ok_or_else(|| {
            "runtime baseline source clean gate must enable strict shell mode".to_string()
        })?;
    let condition_index = lines
        .iter()
        .position(|line| *line == "if [[ -n \"$dirty\" ]]; then")
        .ok_or_else(|| {
            "runtime baseline source clean gate must reject non-empty status".to_string()
        })?;
    let exit_index = lines
        .iter()
        .position(|line| *line == "exit 1")
        .ok_or_else(|| "runtime baseline source clean gate must fail the job".to_string())?;
    let fi_index = lines
        .iter()
        .position(|line| *line == "fi")
        .ok_or_else(|| "runtime baseline source clean gate condition is incomplete".to_string())?;
    if strict_index < assignment_index
        && assignment_index < condition_index
        && condition_index < exit_index
        && exit_index < fi_index
    {
        Ok((step_index, assignment_index))
    } else {
        Err("runtime baseline source clean gate commands are out of order".to_string())
    }
}

fn parse_workflow(contents: &str) -> Result<Workflow, String> {
    yaml_serde::from_str(contents).map_err(|error| format!("invalid workflow YAML: {error}"))
}

fn required_job<'a>(workflow: &'a Workflow, name: &str) -> Result<&'a Job, String> {
    let job = workflow
        .jobs
        .get(name)
        .ok_or_else(|| format!("workflow is missing `{name}` job"))?;
    if job.condition.is_some() || job.continue_on_error.is_some() {
        Err(format!("`{name}` job must be unconditional and blocking"))
    } else {
        Ok(job)
    }
}

fn require_stable_platforms(job: &Job, name: &str) -> Result<(), String> {
    let actual = job
        .strategy
        .as_ref()
        .ok_or_else(|| format!("`{name}` job has no strategy"))?
        .matrix
        .include
        .iter()
        .filter_map(|entry| entry.os.as_deref())
        .collect::<BTreeSet<_>>();
    let missing = STABLE_PLATFORMS
        .iter()
        .copied()
        .filter(|platform| !actual.contains(platform))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!("`{name}` job is missing platforms: {missing:?}"))
    }
}

fn require_matrix_runner(job: &Job, name: &str) -> Result<(), String> {
    if job.runs_on.as_deref() == Some("${{ matrix.os }}") {
        Ok(())
    } else {
        Err(format!("`{name}` job must run on `${{{{ matrix.os }}}}`"))
    }
}

fn require_unconditional_command(job: &Job, command: &str) -> Result<(), String> {
    require_unconditional_command_location(job, command).map(|_| ())
}

fn require_unconditional_command_location(
    job: &Job,
    command: &str,
) -> Result<(usize, usize), String> {
    let (step_index, step, line_index) = find_command(job, command)?;
    if step.condition.is_some() || step.continue_on_error.is_some() {
        Err(format!(
            "command `{command}` must be unconditional and blocking"
        ))
    } else {
        Ok((step_index, line_index))
    }
}

fn find_command<'a>(job: &'a Job, command: &str) -> Result<(usize, &'a Step, usize), String> {
    job.steps
        .iter()
        .enumerate()
        .find_map(|(step_index, step)| {
            step.run.as_deref().and_then(|run| {
                run.lines()
                    .map(str::trim)
                    .position(|line| line == command)
                    .map(|line_index| (step_index, step, line_index))
            })
        })
        .ok_or_else(|| format!("job is missing active command `{command}`"))
}

fn source_clean_assignment() -> String {
    let arguments = build_support::source_status_arguments()
        .into_iter()
        .map(|argument| format!("'{argument}'"))
        .collect::<Vec<_>>()
        .join(" ");
    format!("dirty=\"$(git {arguments})\"")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("runtime-baseline must be nested under the workspace root")
        .to_path_buf()
}
