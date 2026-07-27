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
    require_unconditional_command(
        benchmark_job,
        "cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline",
    )
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
    let step = job
        .steps
        .iter()
        .find(|step| {
            step.run
                .as_deref()
                .is_some_and(|run| run.lines().map(str::trim).any(|line| line == command))
        })
        .ok_or_else(|| format!("job is missing active command `{command}`"))?;
    if step.condition.is_some() || step.continue_on_error.is_some() {
        Err(format!(
            "command `{command}` must be unconditional and blocking"
        ))
    } else {
        Ok(())
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("runtime-baseline must be nested under the workspace root")
        .to_path_buf()
}
