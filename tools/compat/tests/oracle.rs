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

#![allow(clippy::unwrap_used)]

use kiwi_compat::oracle::{
    BUILD_SCHEMA, BuildEvidence, OracleProvenance, PROVENANCE_SCHEMA, RECIPE_ID, REDIS_COMMIT,
    REDIS_TAG,
};
use serde_json::{Map, Value, json};

const REDIS_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CLI_SHA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const TOOL_SHA: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

#[test]
fn loads_the_canonical_build_and_provenance_fixtures() {
    let build = parse_build(canonical_build("primary"));
    assert_eq!(build.schema_version(), BUILD_SCHEMA);
    assert_eq!(build.source().tag(), REDIS_TAG);
    assert_eq!(build.source().commit(), REDIS_COMMIT);
    assert_eq!(build.recipe().id(), RECIPE_ID);
    assert_eq!(build.artifacts().len(), 3);

    let provenance = parse_provenance(canonical_provenance());
    assert_eq!(provenance.schema_version(), PROVENANCE_SCHEMA);
    assert_eq!(provenance.primary().source().commit(), REDIS_COMMIT);
    assert_eq!(provenance.rebuild().source().commit(), REDIS_COMMIT);
    assert_eq!(provenance.runtime().info_redis_versions(), &[REDIS_TAG]);
}

#[test]
fn rejects_non_exact_schema_source_and_recipe_identity() {
    for (path, value) in [
        (&["schema_version"][..], json!("kiwi-redis-oracle-build/v2")),
        (&["source", "tag"][..], json!("8.8.0")),
        (
            &["source", "commit"][..],
            json!("0000000000000000000000000000000000000000"),
        ),
        (
            &["source", "head"][..],
            json!("0000000000000000000000000000000000000000"),
        ),
        (
            &["source", "tag_commit"][..],
            json!("0000000000000000000000000000000000000000"),
        ),
        (&["recipe", "id"][..], json!("redis-8.8.1-linux-release-v2")),
    ] {
        let mut fixture = canonical_build("primary");
        set_path(&mut fixture, path, value);
        assert_build_rejected(fixture);
    }

    let mut fixture = canonical_provenance();
    fixture["schema_version"] = json!("kiwi-redis-oracle-provenance/v2");
    assert_provenance_rejected(fixture);
}

#[test]
fn rejects_duplicate_unknown_and_missing_keys() {
    let build = canonical_build("primary").to_string();
    let duplicate = format!(r#"{{"schema_version":"{BUILD_SCHEMA}",{}"#, &build[1..]);
    assert!(BuildEvidence::from_json(&duplicate).is_err());

    let nested_duplicate = build.replacen(
        &format!(r#""commit":"{REDIS_COMMIT}""#),
        &format!(r#""commit":"{REDIS_COMMIT}","commit":"{REDIS_COMMIT}""#),
        1,
    );
    assert!(BuildEvidence::from_json(&nested_duplicate).is_err());

    for path in [
        &["source"][..],
        &["recipe"][..],
        &["tools", "0"][..],
        &["tools", "0", "identity"][..],
        &["artifacts", "0"][..],
        &["redis_server"][..],
        &["redis_server", "identity"][..],
    ] {
        let mut unknown = canonical_build("primary");
        object_at_mut(&mut unknown, path).insert("unknown".into(), json!(true));
        assert_build_rejected(unknown);

        let mut missing = canonical_build("primary");
        let object = object_at_mut(&mut missing, path);
        let key = object.keys().next().cloned().unwrap();
        object.remove(&key);
        assert_build_rejected(missing);
    }

    for path in [
        &[][..],
        &["comparison"][..],
        &["runtime"][..],
        &["runtime", "binary_identity"][..],
        &["callback"][..],
        &["cleanup"][..],
    ] {
        let mut unknown = canonical_provenance();
        object_at_mut(&mut unknown, path).insert("unknown".into(), json!(true));
        assert_provenance_rejected(unknown);

        let mut missing = canonical_provenance();
        let object = object_at_mut(&mut missing, path);
        let key = object.keys().next().cloned().unwrap();
        object.remove(&key);
        assert_provenance_rejected(missing);
    }
}

#[test]
fn rejects_integer_width_and_semantic_range_mutations() {
    for (path, value) in [
        (&["recipe", "jobs"][..], json!(70_000)),
        (&["recipe", "jobs"][..], json!(0)),
        (&["recipe", "source_date_epoch"][..], json!(-1)),
        (&["artifacts", "2", "mode"][..], json!(0o777)),
        (&["redis_server", "identity", "inode"][..], json!(0)),
        (&["redis_server", "identity", "nlink"][..], json!(0)),
        (&["redis_server", "identity", "size"][..], json!(0)),
    ] {
        let mut fixture = canonical_build("primary");
        set_path(&mut fixture, path, value);
        assert_build_rejected(fixture);
    }

    for (path, value) in [
        (&["runtime", "pid"][..], json!(0)),
        (&["runtime", "pid"][..], json!(4_294_967_296_u64)),
        (&["callback", "exit_code"][..], json!(-2_147_483_649_i64)),
        (&["callback", "exit_code"][..], json!(1)),
    ] {
        let mut fixture = canonical_provenance();
        set_path(&mut fixture, path, value);
        assert_provenance_rejected(fixture);
    }
}

#[test]
fn rejects_invalid_sha_timestamp_path_and_collection_bounds() {
    for (path, value) in [
        (
            &["tools", "0", "sha256"][..],
            json!("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
        ),
        (&["artifacts", "2", "sha256"][..], json!("abc")),
        (&["started_at_utc"][..], json!("2026-02-30T00:00:00Z")),
        (&["finished_at_utc"][..], json!("2026-08-11 00:00:01")),
        (&["artifacts", "2", "path"][..], json!("/src/redis-server")),
        (
            &["artifacts", "2", "path"][..],
            json!("src/../redis-server"),
        ),
        (&["artifacts", "2", "path"][..], json!("src\\redis-server")),
    ] {
        let mut fixture = canonical_build("primary");
        set_path(&mut fixture, path, value);
        assert_build_rejected(fixture);
    }

    let mut too_many_artifacts = canonical_build("primary");
    too_many_artifacts["artifacts"] = Value::Array(
        (0..=4096)
            .map(|index| regular_artifact(&format!("build/{index:04}"), 1, CLI_SHA))
            .collect(),
    );
    assert_build_rejected(too_many_artifacts);

    let mut too_many_tools = canonical_build("primary");
    too_many_tools["tools"] = Value::Array(
        (0..=64)
            .map(|index| {
                tool(
                    &format!("tool-{index}"),
                    &format!("/usr/bin/tool-{index}"),
                    index + 1,
                )
            })
            .collect(),
    );
    assert_build_rejected(too_many_tools);

    let mut too_many_callback_args = canonical_provenance();
    too_many_callback_args["callback"]["argv"] = Value::Array(
        (0..=32)
            .map(|index| json!(format!("arg-{index}")))
            .collect(),
    );
    assert_provenance_rejected(too_many_callback_args);
}

#[test]
fn rejects_every_primary_rebuild_artifact_difference() {
    for (path, value) in [
        (
            &["rebuild", "artifacts", "1", "path"][..],
            json!("src/redis-client"),
        ),
        (&["rebuild", "artifacts", "1", "mode"][..], json!(0o100754)),
        (&["rebuild", "artifacts", "1", "size"][..], json!(81)),
        (
            &["rebuild", "artifacts", "1", "sha256"][..],
            json!(REDIS_SHA),
        ),
        (
            &["rebuild", "artifacts", "0", "target"][..],
            json!("src/redis-cli"),
        ),
    ] {
        let mut fixture = canonical_provenance();
        set_path(&mut fixture, path, value);
        assert_provenance_rejected(fixture);
    }

    let mut kind = canonical_provenance();
    kind["rebuild"]["artifacts"][1] = symlink_artifact("src/redis-cli", "redis-server");
    assert_provenance_rejected(kind);
}

#[test]
fn rejects_unsafe_or_unresolved_symlink_manifests() {
    for target in ["/tmp/redis-server", "../outside", "missing"] {
        let mut fixture = canonical_build("primary");
        fixture["artifacts"][0]["target"] = json!(target);
        assert_build_rejected(fixture);
    }

    let mut cycle = canonical_build("primary");
    cycle["artifacts"] = json!([
        symlink_artifact("a", "b"),
        symlink_artifact("b", "a"),
        regular_artifact("src/redis-server", 100, REDIS_SHA),
    ]);
    assert_build_rejected(cycle);

    let mut too_deep = canonical_build("primary");
    let mut artifacts = Vec::new();
    for index in 0..9 {
        let target = if index == 8 {
            "target".to_string()
        } else {
            format!("link-{}", index + 1)
        };
        artifacts.push(symlink_artifact(&format!("link-{index}"), &target));
    }
    artifacts.push(regular_artifact("target", 100, REDIS_SHA));
    too_deep["artifacts"] = Value::Array(artifacts);
    too_deep["redis_server"]["artifact_path"] = json!("target");
    too_deep["redis_server"]["path"] = json!("/tmp/primary/source/target");
    assert_build_rejected(too_deep);
}

#[test]
fn rejects_any_false_artifact_comparison() {
    for field in [
        "manifests_equal",
        "redis_server_sha256_equal",
        "source_identity_equal",
        "recipe_equal",
        "toolchain_equal",
    ] {
        let mut fixture = canonical_provenance();
        fixture["comparison"][field] = json!(false);
        assert_provenance_rejected(fixture);
    }
}

#[test]
fn rejects_runtime_not_bound_to_rebuild_in_the_default_gate() {
    assert_runtime_binding_mutations_rejected();
}

#[test]
#[ignore = "security mutant: run explicitly to prove runtime/rebuild binding is enforced"]
fn oracle_rejects_runtime_not_bound_to_rebuild() {
    assert_runtime_binding_mutations_rejected();
}

fn assert_runtime_binding_mutations_rejected() {
    for (field, value) in [
        ("binary_path", json!("/tmp/primary/source/src/redis-server")),
        ("binary_sha256", json!(CLI_SHA)),
        ("held_fd", json!(false)),
        ("build_role", json!("primary")),
    ] {
        let mut fixture = canonical_provenance();
        fixture["runtime"][field] = value;
        assert_provenance_rejected(fixture);
    }

    let mut identity_mismatch = canonical_provenance();
    identity_mismatch["runtime"]["binary_identity"]["inode"] = json!(999);
    assert_provenance_rejected(identity_mismatch);
}

#[test]
fn rejects_primary_and_rebuild_source_tree_overlap() {
    for (primary_root, rebuild_root) in [
        ("/tmp/oracle/source", "/tmp/oracle/source/rebuild"),
        ("/tmp/oracle/source/primary", "/tmp/oracle/source"),
    ] {
        let mut fixture = canonical_provenance();
        set_source_root(&mut fixture["primary"], primary_root);
        set_source_root(&mut fixture["rebuild"], rebuild_root);
        fixture["runtime"]["binary_path"] = json!(format!("{rebuild_root}/src/redis-server"));
        assert_provenance_rejected(fixture);
    }
}

#[test]
fn rejects_non_unique_or_wrong_info_version() {
    for versions in [json!([]), json!([REDIS_TAG, REDIS_TAG]), json!(["8.8.0"])] {
        let mut fixture = canonical_provenance();
        fixture["runtime"]["info_redis_versions"] = versions;
        assert_provenance_rejected(fixture);
    }
}

#[test]
fn rejects_unsuccessful_callback_or_cleanup() {
    for field in ["timed_out", "output_truncated"] {
        let mut fixture = canonical_provenance();
        fixture["callback"][field] = json!(true);
        assert_provenance_rejected(fixture);
    }
    let mut callback_not_reaped = canonical_provenance();
    callback_not_reaped["callback"]["process_group_reaped"] = json!(false);
    assert_provenance_rejected(callback_not_reaped);

    for field in [
        "redis_process_reaped",
        "process_group_reaped",
        "runtime_removed",
        "checkout_removed",
        "logs_removed",
        "temp_removed",
        "final_identity_revalidated",
        "output_parent_revalidated",
    ] {
        let mut fixture = canonical_provenance();
        fixture["cleanup"][field] = json!(false);
        assert_provenance_rejected(fixture);
    }
}

#[test]
fn rejects_unbounded_or_over_limit_callback_evidence() {
    for (field, value) in [
        ("timeout_ms", json!(0)),
        ("timeout_ms", json!(600_001)),
        ("term_grace_ms", json!(0)),
        ("term_grace_ms", json!(30_001)),
        ("stdout_limit_bytes", json!(0)),
        ("stdout_limit_bytes", json!(16 * 1024 * 1024 + 1)),
        ("stderr_limit_bytes", json!(0)),
        ("stderr_limit_bytes", json!(16 * 1024 * 1024 + 1)),
    ] {
        let mut fixture = canonical_provenance();
        fixture["callback"][field] = value;
        assert_provenance_rejected(fixture);
    }

    let mut grace_exceeds_timeout = canonical_provenance();
    grace_exceeds_timeout["callback"]["term_grace_ms"] = json!(10_000);
    grace_exceeds_timeout["callback"]["timeout_ms"] = json!(10_000);
    assert_provenance_rejected(grace_exceeds_timeout);

    let mut duration_exceeds_timeout = canonical_provenance();
    duration_exceeds_timeout["callback"]["timeout_ms"] = json!(30_000);
    duration_exceeds_timeout["callback"]["finished_at_utc"] = json!("2026-08-11T00:00:37Z");
    assert_provenance_rejected(duration_exceeds_timeout);

    let mut fractional_duration_exceeds_timeout = canonical_provenance();
    fractional_duration_exceeds_timeout["callback"]["timeout_ms"] = json!(30_000);
    fractional_duration_exceeds_timeout["callback"]["started_at_utc"] =
        json!("2026-08-11T00:00:06.000000Z");
    fractional_duration_exceeds_timeout["callback"]["finished_at_utc"] =
        json!("2026-08-11T00:00:36.000001Z");
    assert_provenance_rejected(fractional_duration_exceeds_timeout);

    for (actual, limit) in [
        ("stdout_bytes", "stdout_limit_bytes"),
        ("stderr_bytes", "stderr_limit_bytes"),
    ] {
        let mut fixture = canonical_provenance();
        fixture["callback"][actual] = json!(4097);
        fixture["callback"][limit] = json!(4096);
        assert_provenance_rejected(fixture);
    }
}

#[test]
fn rejects_publication_before_cleanup_completion() {
    let mut flag = canonical_provenance();
    flag["published_after_cleanup"] = json!(false);
    assert_provenance_rejected(flag);

    let mut timestamp = canonical_provenance();
    timestamp["published_at_utc"] = json!("2026-08-11T00:00:08Z");
    assert_provenance_rejected(timestamp);
}

#[test]
fn rejects_type_mutation_for_every_canonical_nested_field() {
    for canonical in [canonical_build("primary"), canonical_provenance()] {
        for path in collect_field_paths(&canonical, true) {
            let mut fixture = canonical.clone();
            set_owned_path(&mut fixture, &path, json!({"wrong": "type"}));
            if canonical["schema_version"] == BUILD_SCHEMA {
                assert_build_rejected(fixture);
            } else {
                assert_provenance_rejected(fixture);
            }
        }
    }
}

#[test]
fn rejects_missing_key_for_every_canonical_nested_field() {
    for canonical in [canonical_build("primary"), canonical_provenance()] {
        for path in collect_field_paths(&canonical, false) {
            let mut fixture = canonical.clone();
            remove_owned_path(&mut fixture, &path);
            if canonical["schema_version"] == BUILD_SCHEMA {
                assert_build_rejected(fixture);
            } else {
                assert_provenance_rejected(fixture);
            }
        }
    }
}

#[test]
fn accepts_symlink_resolution_at_the_eight_hop_limit() {
    let mut fixture = canonical_build("primary");
    let mut artifacts = Vec::new();
    for index in 0..8 {
        let target = if index == 7 {
            "target".to_string()
        } else {
            format!("link-{}", index + 1)
        };
        artifacts.push(symlink_artifact(&format!("link-{index}"), &target));
    }
    artifacts.push(regular_artifact("target", 100, REDIS_SHA));
    fixture["artifacts"] = Value::Array(artifacts);
    fixture["redis_server"]["artifact_path"] = json!("target");
    fixture["redis_server"]["path"] = json!("/tmp/primary/source/target");

    parse_build(fixture);
}

fn canonical_provenance() -> Value {
    json!({
        "schema_version": PROVENANCE_SCHEMA,
        "primary": canonical_build("primary"),
        "rebuild": canonical_build("rebuild"),
        "comparison": {
            "manifests_equal": true,
            "redis_server_sha256_equal": true,
            "source_identity_equal": true,
            "recipe_equal": true,
            "toolchain_equal": true
        },
        "runtime": {
            "build_role": "rebuild",
            "binary_path": "/tmp/rebuild/source/src/redis-server",
            "binary_sha256": REDIS_SHA,
            "binary_identity": file_identity(22, 100),
            "held_fd": true,
            "pid": 4242,
            "info_redis_versions": [REDIS_TAG]
        },
        "callback": {
            "argv": ["python3", "tests/compat/vector_raw.py"],
            "timeout_ms": 30000,
            "term_grace_ms": 1000,
            "stdout_limit_bytes": 4096,
            "stderr_limit_bytes": 4096,
            "stdout_bytes": 128,
            "stderr_bytes": 0,
            "started_at_utc": "2026-08-11T00:00:06Z",
            "finished_at_utc": "2026-08-11T00:00:07Z",
            "exit_code": 0,
            "timed_out": false,
            "output_truncated": false,
            "process_group_reaped": true
        },
        "cleanup": {
            "redis_process_reaped": true,
            "process_group_reaped": true,
            "runtime_removed": true,
            "checkout_removed": true,
            "logs_removed": true,
            "temp_removed": true,
            "final_identity_revalidated": true,
            "output_parent_revalidated": true,
            "completed_at_utc": "2026-08-11T00:00:09Z"
        },
        "published_after_cleanup": true,
        "published_at_utc": "2026-08-11T00:00:10Z"
    })
}

fn canonical_build(role: &str) -> Value {
    let inode = if role == "primary" { 11 } else { 22 };
    let root = format!("/tmp/{role}/source");
    json!({
        "schema_version": BUILD_SCHEMA,
        "source": {
            "repository": "https://github.com/redis/redis.git",
            "tag": REDIS_TAG,
            "commit": REDIS_COMMIT,
            "head": REDIS_COMMIT,
            "tag_commit": REDIS_COMMIT,
            "root_path": root,
            "git_dir_path": format!("/tmp/{role}/source/.git"),
            "tracked_untracked_clean": true
        },
        "recipe": {
            "id": RECIPE_ID,
            "build_tls": "no",
            "malloc": "libc",
            "debug": "",
            "debug_flags": "",
            "enable_lto": "",
            "opt": "-O3 -fno-omit-frame-pointer",
            "jobs": 1,
            "source_date_epoch": 1784834134_u64,
            "argv": ["make", "-C", "/proc/self/fd/{source_fd}", "BUILD_TLS=no", "MALLOC=libc", "DEBUG=", "DEBUG_FLAGS=", "ENABLE_LTO=", "OPT=-O3 -fno-omit-frame-pointer", "-j", "1", "redis-server"]
        },
        "tools": required_tools(),
        "artifacts": [
            symlink_artifact("redis-server", "src/redis-server"),
            regular_artifact("src/redis-cli", 80, CLI_SHA),
            regular_artifact("src/redis-server", 100, REDIS_SHA)
        ],
        "redis_server": {
            "artifact_path": "src/redis-server",
            "path": format!("/tmp/{role}/source/src/redis-server"),
            "sha256": REDIS_SHA,
            "identity": file_identity(inode, 100)
        },
        "started_at_utc": "2026-08-11T00:00:00Z",
        "finished_at_utc": "2026-08-11T00:00:05Z"
    })
}

fn required_tools() -> Value {
    Value::Array(
        [
            ("controller", "/opt/kiwi/oracle_controller.py"),
            ("python", "/usr/bin/python3"),
            ("git", "/usr/bin/git"),
            ("shell", "/bin/sh"),
            ("make", "/usr/bin/make"),
            ("cc", "/usr/bin/cc"),
            ("ld", "/usr/bin/ld"),
            ("ar", "/usr/bin/ar"),
            ("ranlib", "/usr/bin/ranlib"),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (role, path))| tool(role, path, index + 1))
        .collect(),
    )
}

fn tool(role: &str, path: &str, inode: usize) -> Value {
    json!({
        "role": role,
        "path": path,
        "version": format!("{role} version 1"),
        "sha256": TOOL_SHA,
        "identity": file_identity(inode, 1024),
        "held_fd": true
    })
}

fn file_identity(inode: usize, size: usize) -> Value {
    json!({
        "device": 1,
        "inode": inode,
        "mode": 0o100755,
        "size": size,
        "nlink": 1
    })
}

fn regular_artifact(path: &str, size: usize, sha256: &str) -> Value {
    json!({
        "kind": "regular",
        "path": path,
        "mode": 0o100755,
        "size": size,
        "sha256": sha256
    })
}

fn symlink_artifact(path: &str, target: &str) -> Value {
    json!({
        "kind": "symlink",
        "path": path,
        "mode": 0o120777,
        "target": target
    })
}

fn parse_build(value: Value) -> BuildEvidence {
    BuildEvidence::from_json(&value.to_string())
        .unwrap_or_else(|error| panic!("canonical build fixture must load: {error}"))
}

fn parse_provenance(value: Value) -> OracleProvenance {
    OracleProvenance::from_json(&value.to_string())
        .unwrap_or_else(|error| panic!("canonical provenance fixture must load: {error}"))
}

fn assert_build_rejected(value: Value) {
    if let Ok(build) = BuildEvidence::from_json(&value.to_string()) {
        panic!("mutated build fixture must be rejected: {build:?}");
    }
}

fn assert_provenance_rejected(value: Value) {
    if let Ok(provenance) = OracleProvenance::from_json(&value.to_string()) {
        panic!("mutated provenance fixture must be rejected: {provenance:?}");
    }
}

fn set_path(root: &mut Value, path: &[&str], value: Value) {
    let (last, parents) = path.split_last().unwrap();
    let mut current = root;
    for segment in parents {
        current = if let Ok(index) = segment.parse::<usize>() {
            &mut current.as_array_mut().unwrap()[index]
        } else {
            &mut current[*segment]
        };
    }
    if let Ok(index) = last.parse::<usize>() {
        current.as_array_mut().unwrap()[index] = value;
    } else {
        current[*last] = value;
    }
}

fn object_at_mut<'a>(root: &'a mut Value, path: &[&str]) -> &'a mut Map<String, Value> {
    let mut current = root;
    for segment in path {
        current = if let Ok(index) = segment.parse::<usize>() {
            &mut current.as_array_mut().unwrap()[index]
        } else {
            &mut current[*segment]
        };
    }
    current.as_object_mut().unwrap()
}

fn set_source_root(build: &mut Value, root: &str) {
    build["source"]["root_path"] = json!(root);
    build["source"]["git_dir_path"] = json!(format!("{root}/.git"));
    build["redis_server"]["path"] = json!(format!("{root}/src/redis-server"));
}

fn collect_field_paths(root: &Value, include_array_elements: bool) -> Vec<Vec<String>> {
    fn visit(
        value: &Value,
        prefix: &mut Vec<String>,
        paths: &mut Vec<Vec<String>>,
        include_array_elements: bool,
    ) {
        match value {
            Value::Object(object) => {
                for (key, child) in object {
                    prefix.push(key.clone());
                    paths.push(prefix.clone());
                    visit(child, prefix, paths, include_array_elements);
                    prefix.pop();
                }
            }
            Value::Array(array) => {
                for (index, child) in array.iter().enumerate() {
                    prefix.push(index.to_string());
                    if include_array_elements {
                        paths.push(prefix.clone());
                    }
                    visit(child, prefix, paths, include_array_elements);
                    prefix.pop();
                }
            }
            _ => {}
        }
    }

    let mut paths = Vec::new();
    visit(root, &mut Vec::new(), &mut paths, include_array_elements);
    paths
}

fn set_owned_path(root: &mut Value, path: &[String], value: Value) {
    let borrowed = path.iter().map(String::as_str).collect::<Vec<_>>();
    set_path(root, &borrowed, value);
}

fn remove_owned_path(root: &mut Value, path: &[String]) {
    let (last, parents) = path.split_last().unwrap();
    let mut current = root;
    for segment in parents {
        current = if let Ok(index) = segment.parse::<usize>() {
            &mut current.as_array_mut().unwrap()[index]
        } else {
            &mut current[segment]
        };
    }
    current.as_object_mut().unwrap().remove(last);
}
