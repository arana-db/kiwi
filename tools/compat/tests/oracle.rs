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
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

const REDIS_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CLI_SHA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const TOOL_SHA: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

#[cfg(unix)]
const CONTROLLED_PYTHON: &str = "/home/alex/miniconda3/bin/python3";

#[cfg(unix)]
struct TestDir(PathBuf);

#[cfg(unix)]
impl TestDir {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock must be after the Unix epoch")
            .as_nanos();
        let path = PathBuf::from(format!(
            "/tmp/kiwi-oracle-test-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("test directory must be created");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

#[cfg(unix)]
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(unix)]
fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("compat crate must be below the repository root")
        .to_path_buf()
}

#[cfg(unix)]
fn controller_path() -> PathBuf {
    repository_root().join("scripts/compat/oracle_controller.py")
}

#[cfg(unix)]
fn build_script_path() -> PathBuf {
    repository_root().join("scripts/compat/build-redis-8.8.1.sh")
}

#[cfg(unix)]
fn run_python_probe(test_dir: &TestDir, body: &str) -> Output {
    let probe = test_dir.path().join("probe.py");
    let controller = controller_path();
    let source = format!(
        r#"import importlib.util
import pathlib
import sys

controller_path = pathlib.Path({controller:?})
spec = importlib.util.spec_from_file_location("kiwi_oracle_controller", controller_path)
assert spec is not None and spec.loader is not None
controller = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = controller
spec.loader.exec_module(controller)

{body}
"#,
        controller = controller.to_string_lossy(),
    );
    fs::write(&probe, source).expect("probe must be written");
    Command::new(CONTROLLED_PYTHON)
        .args(["-I", "-B"])
        .arg(&probe)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .output()
        .expect("controlled Python probe must start")
}

#[cfg(unix)]
fn assert_probe_succeeds(output: Output) {
    assert!(
        output.status.success(),
        "probe failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(unix)]
fn clone_exact_redis(source: &Path) {
    let clone = Command::new("/usr/bin/git")
        .args([
            "clone",
            "--depth",
            "1",
            "--branch",
            REDIS_TAG,
            "https://github.com/redis/redis.git",
        ])
        .arg(source)
        .output()
        .expect("git clone must start");
    assert!(
        clone.status.success(),
        "git clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    let checkout = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(source)
        .args(["checkout", "--detach", REDIS_COMMIT])
        .output()
        .expect("git checkout must start");
    assert!(
        checkout.status.success(),
        "git checkout failed: {}",
        String::from_utf8_lossy(&checkout.stderr)
    );
}

#[test]
#[cfg(unix)]
fn oracle_build_wrapper_rejects_ambient_python_and_controller_selection() {
    let test_dir = TestDir::new("ambient");
    let evil_bin = test_dir.path().join("evil-bin");
    let evil_python_path = test_dir.path().join("evil-pythonpath");
    let python_marker = test_dir.path().join("ambient-python-ran");
    let import_marker = test_dir.path().join("ambient-import-ran");
    fs::create_dir(&evil_bin).unwrap();
    fs::create_dir(&evil_python_path).unwrap();
    fs::write(
        evil_bin.join("python3"),
        format!("#!/bin/sh\ntouch '{}'\nexit 91\n", python_marker.display()),
    )
    .unwrap();
    fs::write(
        evil_python_path.join("sitecustomize.py"),
        format!("from pathlib import Path\nPath({import_marker:?}).touch()\n"),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(evil_bin.join("python3"))
        .unwrap()
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(evil_bin.join("python3"), permissions).unwrap();

    let output = Command::new("/usr/bin/bash")
        .arg(build_script_path())
        .arg("--help")
        .env_clear()
        .env("PATH", &evil_bin)
        .env("PYTHONPATH", &evil_python_path)
        .env("PYTHONHOME", &evil_python_path)
        .env("GIT_CONFIG_GLOBAL", evil_python_path.join("gitconfig"))
        .output()
        .expect("build wrapper must start");

    assert!(
        output.status.success(),
        "wrapper --help failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!python_marker.exists(), "ambient PATH selected python3");
    assert!(
        !import_marker.exists(),
        "ambient PYTHONPATH/PYTHONHOME loaded sitecustomize"
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("--source"),
        "controlled controller help was not emitted"
    );
}

#[test]
#[cfg(unix)]
fn oracle_build_holds_executable_fd_and_freezes_tool_aliases() {
    let test_dir = TestDir::new("held-fd");
    let body = format!(
        r##"import os
import pathlib
import stat

root = pathlib.Path({root:?})
tool = root / "tool"
attacker = root / "attacker.sh"
replacement_marker = root / "replacement-marker"
tool.write_text("#!/bin/sh\nprintf replacement > \"$1\"\n", encoding="utf-8")
attacker.write_text(
    "#!/bin/bash\nset -eu\n"
    "/usr/bin/chmod 0700 \"$1\"\n"
    "/usr/bin/rm -f \"$1/probe\"\n"
    "/usr/bin/cp \"$2\" \"$1/probe\"\n"
    "\"$1/probe\" \"$3\"\n"
    "/usr/bin/rm -f \"$1/probe\"\n",
    encoding="utf-8",
)
tool.chmod(0o755)
attacker.chmod(0o755)

with controller.HeldExecutable.open("probe", tool) as probe:
    aliases = controller.FrozenToolDirectory.create(root / "tools", {{"probe": probe}})
    with controller.HeldExecutable.open("shell", pathlib.Path("/usr/bin/bash")) as shell:
        result = controller.run_bounded(
            shell,
            ["bash", str(attacker), str(aliases.path), str(tool), str(replacement_marker)],
            env={{"PATH": str(aliases.path), "HOME": str(root), "TMPDIR": str(root)}},
            timeout_ms=2000,
            term_grace_ms=100,
            stdout_limit_bytes=64,
            stderr_limit_bytes=4096,
            readonly_bind_paths=(aliases.path,),
        )
    assert not replacement_marker.exists()
    assert result.exit_code != 0
    assert stat.S_IMODE(os.lstat(aliases.path).st_mode) == 0o500
    aliases.verify_frozen()
    aliases.remove()
"##,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(unix)]
fn oracle_build_make_uses_the_held_shell_fd_after_path_replacement() {
    let test_dir = TestDir::new("make-shell");
    let body = format!(
        r##"import os
import pathlib

root = pathlib.Path({root:?})
shell_path = root / "controlled-shell"
replacement = root / "replacement-shell"
original_marker = root / "original-shell"
replacement_marker = root / "replacement-shell-ran"
recipe_marker = root / "recipe-ran"
shell_path.write_text(
    "#!/usr/bin/bash\nprintf original > \"$ORIGINAL_MARKER\"\nexec /usr/bin/bash \"$@\"\n",
    encoding="utf-8",
)
replacement.write_text(
    "#!/usr/bin/bash\nprintf replacement > \"$REPLACEMENT_MARKER\"\nexec /usr/bin/bash \"$@\"\n",
    encoding="utf-8",
)
(root / "Makefile").write_text(
    "all:\n\t@printf recipe > \"$$RECIPE_MARKER\"\n",
    encoding="utf-8",
)
shell_path.chmod(0o755)
replacement.chmod(0o755)
assert controller.BUILD_ARGV[3] == "SHELL=/proc/self/fd/{{shell_fd}}"
with controller.HeldExecutable.open("shell", shell_path) as shell:
    os.replace(replacement, shell_path)
    with controller.HeldExecutable.open("make", pathlib.Path("/usr/bin/make")) as make:
        result = controller.run_bounded(
            make,
            ["make", "-C", str(root), f"SHELL=/proc/self/fd/{{shell.fd}}", "-j", "1", "all"],
            env={{
                "PATH": "/usr/bin:/bin",
                "HOME": str(root),
                "TMPDIR": str(root),
                "ORIGINAL_MARKER": str(original_marker),
                "REPLACEMENT_MARKER": str(replacement_marker),
                "RECIPE_MARKER": str(recipe_marker),
            }},
            timeout_ms=3000,
            term_grace_ms=100,
            stdout_limit_bytes=4096,
            stderr_limit_bytes=4096,
            extra_fds=(shell.fd,),
        )
assert result.exit_code == 0
assert original_marker.read_text(encoding="utf-8") == "original"
assert recipe_marker.read_text(encoding="utf-8") == "recipe"
assert not replacement_marker.exists()
"##,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(unix)]
fn oracle_build_runner_caps_output_times_out_and_reaps_the_process_group() {
    let test_dir = TestDir::new("runner");
    let body = format!(
        r##"import os
import pathlib

root = pathlib.Path({root:?})
script = root / "runaway.sh"
child_pid = root / "child.pid"
script.write_text(
    "#!/bin/sh\n(sleep 30) &\nprintf '%s' \"$!\" > \"$1\"\n"
    "while :; do printf 0123456789; printf abcdefghij >&2; done\n",
    encoding="utf-8",
)
script.chmod(0o755)
(root / "home").mkdir()
(root / "tmp").mkdir()
with controller.HeldExecutable.open("shell", pathlib.Path("/usr/bin/bash")) as held:
    result = controller.run_bounded(
        held,
        ["bash", str(script), str(child_pid)],
        env={{"PATH": "/usr/bin:/bin", "HOME": str(root / "home"), "TMPDIR": str(root / "tmp")}},
        timeout_ms=250,
        term_grace_ms=100,
        stdout_limit_bytes=64,
        stderr_limit_bytes=64,
    )
assert result.timed_out
assert result.output_truncated
assert result.stdout_bytes > result.stdout_limit_bytes
assert result.stderr_bytes > result.stderr_limit_bytes
assert result.process_group_reaped
pid = int(child_pid.read_text(encoding="utf-8"))
try:
    os.kill(pid, 0)
except ProcessLookupError:
    pass
else:
    raise AssertionError(f"descendant {{pid}} survived process-group cleanup")
"##,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(unix)]
fn oracle_build_artifact_scan_is_sorted_bounded_and_fail_closed() {
    let test_dir = TestDir::new("artifacts");
    let body = format!(
        r#"import os
import pathlib
import socket

root = pathlib.Path({root:?}) / "source"
root.mkdir()
baseline = controller.snapshot_tree(root)
(root / "z-last").write_bytes(b"z")
(root / "a-first").write_bytes(b"a")
os.symlink("a-first", root / "m-link")
manifest = controller.scan_artifacts(root, baseline)
assert [entry["path"] for entry in manifest] == ["a-first", "m-link", "z-last"]
assert manifest[0]["kind"] == "regular"
assert manifest[1] == {{"kind": "symlink", "path": "m-link", "mode": os.lstat(root / "m-link").st_mode, "target": "a-first"}}

bad_link = root / "escape"
os.symlink("../outside", bad_link)
try:
    controller.scan_artifacts(root, baseline)
except controller.OracleError:
    pass
else:
    raise AssertionError("escaping symlink was accepted")
bad_link.unlink()

sock = socket.socket(socket.AF_UNIX)
sock.bind(str(root / "socket"))
try:
    try:
        controller.scan_artifacts(root, baseline)
    except controller.OracleError:
        pass
    else:
        raise AssertionError("socket artifact was accepted")
finally:
    sock.close()
    (root / "socket").unlink()

try:
    controller.scan_artifacts(root, baseline, limits=controller.ArtifactLimits(max_count=2, max_file_bytes=1024, max_total_bytes=1024))
except controller.OracleError:
    pass
else:
    raise AssertionError("artifact count bound was not enforced")
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(unix)]
fn oracle_build_candidate_publish_is_exclusive_atomic_and_not_provenance() {
    let test_dir = TestDir::new("publish");
    let metadata = test_dir.path().join("primary-build.json");
    let fixture = canonical_build("primary");
    let body = format!(
        r#"import json
import pathlib

metadata = pathlib.Path({metadata:?})
document = json.loads(r'''{document}''')
controller.publish_candidate(metadata, document)
assert metadata.exists()
assert not list(metadata.parent.glob("*provenance*"))
try:
    controller.publish_candidate(metadata, document)
except controller.OracleError:
    pass
else:
    raise AssertionError("existing candidate metadata was overwritten")
"#,
        metadata = metadata.to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
    let raw = fs::read_to_string(&metadata).unwrap();
    let parsed = BuildEvidence::from_json(&raw).expect("candidate must satisfy Task 1 API");
    assert_eq!(parsed.schema_version(), BUILD_SCHEMA);
    assert!(!test_dir.path().join("provenance.json").exists());
}

#[test]
#[ignore = "external exact checkout; run with --include-ignored"]
#[cfg(unix)]
fn oracle_build_rejects_ignored_preexisting_artifacts_before_make() {
    use std::fs::{File, FileTimes};
    use std::time::Duration;

    let test_dir = TestDir::new("prebuild-artifacts");
    let source = test_dir.path().join("source");
    let metadata = test_dir.path().join("primary-build.json");
    clone_exact_redis(&source);
    let artifacts = [
        source.join("src/redis-cli"),
        source.join("src/server.o"),
        source.join("deps/hiredis/libhiredis.a"),
    ];
    for artifact in &artifacts {
        fs::create_dir_all(artifact.parent().unwrap()).unwrap();
        fs::write(artifact, b"preexisting build artifact").unwrap();
        File::options()
            .write(true)
            .open(artifact)
            .unwrap()
            .set_times(
                FileTimes::new().set_modified(
                    SystemTime::now()
                        .checked_add(Duration::from_secs(24 * 60 * 60))
                        .unwrap(),
                ),
            )
            .unwrap();
        let ignored = Command::new("/usr/bin/git")
            .arg("-C")
            .arg(&source)
            .arg("check-ignore")
            .arg(artifact.strip_prefix(&source).unwrap())
            .status()
            .unwrap();
        assert!(ignored.success(), "fixture must be ignored by exact Redis");
    }

    let output = Command::new("/usr/bin/bash")
        .arg(build_script_path())
        .arg("--source")
        .arg(&source)
        .arg("--metadata")
        .arg(&metadata)
        .env_clear()
        .env("PATH", "/untrusted")
        .output()
        .expect("Redis build wrapper must start");
    assert!(
        !output.status.success(),
        "preexisting artifacts reached make"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("pre-build artifact manifest is not empty"),
        "unexpected rejection: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!metadata.exists());
    for artifact in artifacts {
        assert_eq!(fs::read(artifact).unwrap(), b"preexisting build artifact");
    }
}

#[test]
#[ignore = "real external Redis 8.8.1 build; run with --include-ignored"]
#[cfg(unix)]
fn oracle_build_real_redis_8_8_1_produces_valid_primary_evidence_only() {
    let test_dir = TestDir::new("real-redis");
    let source = test_dir.path().join("source");
    let metadata = test_dir.path().join("primary-build.json");
    clone_exact_redis(&source);

    let output = Command::new("/usr/bin/bash")
        .arg(build_script_path())
        .arg("--source")
        .arg(&source)
        .arg("--metadata")
        .arg(&metadata)
        .env_clear()
        .env("PATH", "/untrusted")
        .env("PYTHONPATH", "/untrusted")
        .env("PYTHONHOME", "/untrusted")
        .output()
        .expect("Redis build wrapper must start");
    assert!(
        output.status.success(),
        "Redis build failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let evidence = fs::read_to_string(&metadata).expect("candidate metadata must exist");
    let build = BuildEvidence::from_json(&evidence).expect("candidate metadata must validate");
    assert_eq!(build.source().commit(), REDIS_COMMIT);
    assert_eq!(build.recipe().id(), RECIPE_ID);
    assert!(!build.artifacts().is_empty());
    assert!(
        fs::read_dir(test_dir.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("provenance")),
        "primary build must not publish final provenance"
    );
}

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

    let mut ambient_shell = canonical_build("primary");
    ambient_shell["recipe"]["argv"][3] = json!("SHELL=/bin/sh");
    assert_build_rejected(ambient_shell);

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
            "argv": ["make", "-C", "/proc/self/fd/{source_fd}", "SHELL=/proc/self/fd/{shell_fd}", "BUILD_TLS=no", "MALLOC=libc", "DEBUG=", "DEBUG_FLAGS=", "ENABLE_LTO=", "OPT=-O3 -fno-omit-frame-pointer", "-j", "1", "redis-server"]
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
