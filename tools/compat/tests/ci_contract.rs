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
    let job = workflow
        .jobs
        .get("vector-cluster-fail-closed")
        .expect("required Vector cluster job must exist");
    assert_eq!(job.runs_on, "ubuntu-latest");
    assert!(job.continue_on_error.is_none());
    assert!(
        job.steps
            .iter()
            .all(|step| step.continue_on_error.is_none())
    );
    let runner_commands = job
        .steps
        .iter()
        .filter_map(|step| step.run.as_deref())
        .filter(|command| command.contains("scripts/ci/run-vector-cluster.sh"))
        .collect::<Vec<_>>();
    assert_eq!(runner_commands.len(), 1);
    let command = runner_commands[0];
    for required in [
        "KIWI_RUN_CLUSTER_TESTS=1",
        "KIWI_BINARY=",
        "KIWI_GRPCURL=",
        "scripts/ci/run-vector-cluster.sh",
    ] {
        assert!(
            command.contains(required),
            "cluster job is missing {required}"
        );
    }
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

    let workflow = include_str!("../../../.github/workflows/ci.yml");
    assert!(workflow.contains("cargo build --locked --bin kiwi"));
    assert!(workflow.contains("grpcurl_1.9.3_linux_x86_64.tar.gz"));
    assert!(workflow.contains("a926b62a85787ccf73ef8736b3ae554f1242e39d92bb8767a79d6dd23b11d1d5"));

    let cluster_tests = include_str!("../../../tests/python/test_vector_cluster.py");
    assert!(!cluster_tests.contains("pytest.mark.skipif"));
    assert!(cluster_tests.contains("@pytest.mark.parametrize"));
    assert!(cluster_tests.contains("signal.SIGTERM"));
    assert!(cluster_tests.contains("signal.SIGKILL"));
    assert!(cluster_tests.contains("process_group_gone"));
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
    fs::write(
        &summary,
        r#"{"collected":16,"passed":15,"failed":0,"skipped":1,"xfailed":0,"xpassed":0,"deselected":0}"#,
    )
    .expect("write totals mutant");
    let summary_result = Command::new("python3")
        .arg(&validator)
        .arg("--validate-summary")
        .arg(&summary)
        .output()
        .expect("run totals mutant");
    assert!(!summary_result.status.success());

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
fn vector_differential_fast_job_uses_marker_ownership_not_path_ignore() {
    let makefile = include_str!("../../../tests/Makefile");
    assert!(!makefile.contains("--ignore=$(PYTHON_TEST_DIR)/test_vector_set_differential.py"));
    assert!(makefile.contains("-m \"not raw_vector_protocol\""));

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
