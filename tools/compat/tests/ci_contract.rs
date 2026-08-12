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

#[derive(Deserialize)]
struct Workflow {
    jobs: BTreeMap<String, Job>,
}

#[derive(Deserialize)]
struct Job {
    #[serde(rename = "runs-on")]
    runs_on: String,
    #[serde(rename = "continue-on-error")]
    continue_on_error: Option<yaml_serde::Value>,
    #[serde(default)]
    steps: Vec<Step>,
}

#[derive(Deserialize)]
struct Step {
    run: Option<String>,
    #[serde(rename = "continue-on-error")]
    continue_on_error: Option<yaml_serde::Value>,
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
fn vector_differential_fast_job_uses_marker_ownership_not_path_ignore() {
    let makefile = include_str!("../../../tests/Makefile");
    assert!(!makefile.contains("--ignore=$(PYTHON_TEST_DIR)/test_vector_set_differential.py"));
    assert!(makefile.contains("-m \"not raw_vector_protocol\""));

    let runner = include_str!("../../../scripts/compat/run-vector-differential.sh");
    for required in [
        "KIWI_COMPAT_REQUIRE_ORACLE",
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
    let yaml = fs::read_to_string(&registry).unwrap();
    let node_ids = yaml
        .lines()
        .filter_map(|line| line.strip_prefix("      - tests/python/"))
        .map(|line| format!("tests/python/{line}"))
        .collect::<Vec<_>>();
    assert_eq!(node_ids.len(), 29);

    fs::write(&collection, format!("{}\n", node_ids.join("\n"))).unwrap();
    assert!(
        Command::new("/usr/bin/bash")
            .arg(&runner)
            .arg("--validate-collection")
            .arg(&registry)
            .arg(&collection)
            .status()
            .unwrap()
            .success()
    );
    fs::write(&collection, "27 tests collected\n").unwrap();
    assert!(
        !Command::new("/usr/bin/bash")
            .arg(&runner)
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
        !Command::new("/usr/bin/bash")
            .arg(&runner)
            .arg("--validate-collection")
            .arg(&registry)
            .arg(&collection)
            .status()
            .unwrap()
            .success()
    );

    let passing = r#"{"collected":29,"passed":29,"failed":0,"skipped":0,"xfailed":0,"xpassed":0,"deselected":0}"#;
    fs::write(&summary, passing).unwrap();
    assert!(
        Command::new("/usr/bin/bash")
            .arg(&runner)
            .arg("--validate-summary")
            .arg(&registry)
            .arg(&summary)
            .status()
            .unwrap()
            .success()
    );
    for mutant in [
        passing.replace("\"collected\":29", "\"collected\":0"),
        passing.replace("\"failed\":0", "\"failed\":1"),
        passing.replace("\"skipped\":0", "\"skipped\":1"),
        passing.replace("\"xfailed\":0", "\"xfailed\":1"),
        passing.replace("\"xpassed\":0", "\"xpassed\":1"),
        passing.replace("\"deselected\":0", "\"deselected\":1"),
    ] {
        fs::write(&summary, mutant).unwrap();
        assert!(
            !Command::new("/usr/bin/bash")
                .arg(&runner)
                .arg("--validate-summary")
                .arg(&registry)
                .arg(&summary)
                .status()
                .unwrap()
                .success()
        );
    }

    let unavailable = Command::new("/usr/bin/bash")
        .arg(&runner)
        .env_clear()
        .env("OSTYPE", "linux-gnu")
        .env("KIWI_COMPAT_REQUIRE_ORACLE", "1")
        .status()
        .unwrap();
    assert!(!unavailable.success());
    let identity_mismatch = Command::new("/usr/bin/bash")
        .arg(&runner)
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
        Command::new("/usr/bin/bash")
            .arg(&runner)
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
            !Command::new("/usr/bin/bash")
                .arg(&runner)
                .arg("--validate-runtime-evidence")
                .arg(&runtime_evidence)
                .status()
                .unwrap()
                .success()
        );
    }
    assert!(
        !Command::new("/usr/bin/bash")
            .arg(&runner)
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
    let commands = [
        "VADD",
        "VCARD",
        "VDIM",
        "VEMB",
        "VINFO",
        "VISMEMBER",
        "VREM",
        "VSIM",
    ];
    let mut records = String::new();
    for node_id in node_ids {
        let protocol = if node_id.ends_with("[resp2]") { 2 } else { 3 };
        for command in commands {
            records.push_str(&format!(
                "{{\"command\":\"{command}\",\"node_id\":\"{node_id}\",\"protocol\":{protocol},\"kiwi_frame_sha256\":\"{}\",\"redis_frame_sha256\":\"{}\"}}\n",
                "a".repeat(64),
                "a".repeat(64)
            ));
        }
    }
    fs::write(&coverage, &records).unwrap();
    let validate = |path: &std::path::Path| {
        Command::new("/usr/bin/bash")
            .arg(&runner)
            .arg("--validate-raw-coverage")
            .arg(&registry)
            .arg(path)
            .status()
            .unwrap()
            .success()
    };
    assert!(validate(&coverage));

    let missing_vcard = records
        .lines()
        .filter(|line| !(line.contains("\"command\":\"VCARD\"") && line.contains("[resp3]")))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&coverage, format!("{missing_vcard}\n")).unwrap();
    assert!(!validate(&coverage));

    let typed_equivalence = records.replacen(
        &format!("\"kiwi_frame_sha256\":\"{}\"", "a".repeat(64)),
        &format!("\"kiwi_frame_sha256\":\"{}\"", "b".repeat(64)),
        1,
    );
    fs::write(&coverage, typed_equivalence).unwrap();
    assert!(!validate(&coverage));

    fs::remove_dir_all(&scratch).unwrap();
}
