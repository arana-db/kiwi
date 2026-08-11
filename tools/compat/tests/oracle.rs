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

#[test]
fn oracle_build_linux_probes_use_only_approved_system_python() {
    let wrapper = include_str!("../../../scripts/compat/build-redis-8.8.1.sh");
    let tests = include_str!("oracle.rs");
    let developer_home = concat!("/home/", "alex/");
    let broad_unix_cfg = concat!("#[cfg(", "unix)]");

    assert!(!wrapper.contains(developer_home));
    assert!(wrapper.contains("/usr/bin/python3"));
    assert!(wrapper.contains("/bin/python3"));
    assert!(!tests.contains(developer_home));
    assert!(!tests.contains(broad_unix_cfg));
    assert!(tests.contains("#[cfg(target_os = \"linux\")]"));
}

#[cfg(target_os = "linux")]
fn controlled_python() -> &'static str {
    ["/usr/bin/python3", "/bin/python3"]
        .into_iter()
        .find(|candidate| Path::new(candidate).is_file())
        .expect("an approved system Python must be installed")
}

#[cfg(target_os = "linux")]
struct TestDir(PathBuf);

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(target_os = "linux")]
fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("compat crate must be below the repository root")
        .to_path_buf()
}

#[cfg(target_os = "linux")]
fn controller_path() -> PathBuf {
    repository_root().join("scripts/compat/oracle_controller.py")
}

#[cfg(target_os = "linux")]
fn build_script_path() -> PathBuf {
    repository_root().join("scripts/compat/build-redis-8.8.1.sh")
}

#[cfg(target_os = "linux")]
fn verify_script_path() -> PathBuf {
    repository_root().join("scripts/compat/verify-redis-8.8.1.sh")
}

#[cfg(target_os = "linux")]
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
    Command::new(controlled_python())
        .args(["-I", "-B"])
        .arg(&probe)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .output()
        .expect("controlled Python probe must start")
}

#[cfg(target_os = "linux")]
fn assert_probe_succeeds(output: Output) {
    assert!(
        output.status.success(),
        "probe failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "linux")]
fn clone_exact_redis(source: &Path) {
    let clone = Command::new("/usr/bin/git")
        .args([
            "clone",
            "--single-branch",
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

#[cfg(target_os = "linux")]
fn clone_local_exact_redis(seed: &Path, source: &Path) {
    let clone = Command::new("/usr/bin/git")
        .args(["clone", "--no-hardlinks"])
        .arg(seed)
        .arg(source)
        .output()
        .expect("local git clone must start");
    assert!(
        clone.status.success(),
        "local git clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    for args in [
        vec!["checkout", "--detach", REDIS_COMMIT],
        vec![
            "remote",
            "set-url",
            "origin",
            "https://github.com/redis/redis.git",
        ],
    ] {
        let output = Command::new("/usr/bin/git")
            .arg("-C")
            .arg(source)
            .args(args)
            .output()
            .expect("git fixture command must start");
        assert!(
            output.status.success(),
            "git fixture command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(target_os = "linux")]
fn git_fixture(source: &Path, args: &[&str]) {
    let output = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(source)
        .args(args)
        .output()
        .expect("git mutation command must start");
    assert!(
        output.status.success(),
        "git mutation command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
fn oracle_build_holds_executable_fd_and_freezes_tool_aliases() {
    let test_dir = TestDir::new("held-fd");
    let body = format!(
        r##"import os
import pathlib

root = pathlib.Path({root:?})
tool = root / "tool"
replacement_tool = root / "replacement-tool"
original_marker = root / "original-marker"
replacement_marker = root / "replacement-marker"
tool.write_text("#!/bin/sh\nprintf original > \"$1\"\n", encoding="utf-8")
replacement_tool.write_text(
    f"#!/bin/sh\nprintf replacement > \"{{replacement_marker}}\"\n",
    encoding="utf-8",
)
tool.chmod(0o755)
replacement_tool.chmod(0o755)

with controller.HeldExecutable.open("probe", tool) as probe:
    aliases = controller.FrozenToolDirectory.create(root / "tools", {{"probe": probe}})
    held_path = aliases.path.with_name("held-tools")
    os.rename(aliases.path, held_path)
    aliases.path.mkdir(mode=0o700)
    os.symlink(str(replacement_tool), aliases.path / "probe")
    aliases.path.chmod(0o500)
    with controller.HeldExecutable.open("shell", pathlib.Path("/usr/bin/bash")) as shell:
        result = controller.run_bounded(
            shell,
            ["bash", "-c", "probe \"$1\"", "bash", str(original_marker)],
            env={{"PATH": aliases.child_path, "HOME": str(root), "TMPDIR": str(root)}},
            timeout_ms=2000,
            term_grace_ms=100,
            stdout_limit_bytes=64,
            stderr_limit_bytes=4096,
            extra_fds=(probe.fd,),
            readonly_bind_directories=(aliases,),
        )
    assert result.exit_code == 0, (result.stdout, result.stderr)
    assert original_marker.read_text(encoding="utf-8") == "original"
    assert not replacement_marker.exists()
    os.chmod(aliases.path, 0o700)
    os.unlink(aliases.path / "probe")
    os.rmdir(aliases.path)
    os.rename(held_path, aliases.path)
    aliases.verify_frozen()
    aliases.remove()
"##,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_build_runtime_fd_keeps_command_environment_live_and_cleanup_detects_writes() {
    let test_dir = TestDir::new("runtime-fd");
    let body = format!(
        r#"import os
import pathlib

root = pathlib.Path({root:?})
parent = controller.HeldDirectory.open(root)
os.mkdir("runtime", mode=0o700, dir_fd=parent.fd)
runtime = parent.open_directory("runtime")
runtime_path = pathlib.Path(f"/proc/self/fd/{{runtime.fd}}")
home = runtime_path / "home"
temporary = runtime_path / "tmp"
versions = runtime_path / "versions"
home.mkdir(mode=0o700)
temporary.mkdir(mode=0o700)
versions.mkdir(mode=0o700)
normal = controller.HeldExecutable.open("runtime-normal", pathlib.Path("/usr/bin/python3"))
dirty = controller.HeldExecutable.open("runtime-dirty", pathlib.Path("/usr/bin/python3"))
aliases = controller.FrozenToolDirectory.create(
    runtime_path / "tools", {{"runtime-normal": normal, "runtime-dirty": dirty}}
)
env = controller._sanitized_environment(aliases.child_path, home, temporary)
normal_command = """import os, pathlib
home = pathlib.Path(os.environ["HOME"])
temporary = pathlib.Path(os.environ["TMPDIR"])
assert home.is_dir() and os.access(home, os.W_OK)
assert temporary.is_dir() and os.access(temporary, os.W_OK)
marker = temporary / "normal-marker"
marker.write_text("normal", encoding="utf-8")
marker.unlink()
print("runtime normal")
"""
dirty_command = """import os, pathlib
home = pathlib.Path(os.environ["HOME"])
temporary = pathlib.Path(os.environ["TMPDIR"])
assert home.is_dir() and os.access(home, os.W_OK)
assert temporary.is_dir() and os.access(temporary, os.W_OK)
(temporary / "dirty-marker").write_text("dirty", encoding="utf-8")
print("runtime dirty")
"""
try:
    controller._tool_evidence(
        [normal], {{normal.role: ("-I", "-B", "-c", normal_command)}}, env, versions, aliases
    )
    controller._empty_directory(temporary, "normal TMPDIR")
    controller._tool_evidence(
        [dirty], {{dirty.role: ("-I", "-B", "-c", dirty_command)}}, env, versions, aliases
    )
    try:
        controller._empty_directory(temporary, "dirty TMPDIR")
    except controller.OracleError:
        pass
    else:
        raise AssertionError("runtime marker escaped the ending empty-directory check")
    assert (temporary / "dirty-marker").exists()
finally:
    controller._run_cleanup_actions(
        [
            ("alias remove", aliases.remove_path),
            ("alias close", aliases.close),
            ("normal close", normal.close),
            ("dirty close", dirty.close),
            (
                "runtime remove",
                lambda: controller._remove_runtime_directory(parent.fd, "runtime", runtime),
            ),
            ("runtime close", runtime.close),
        ]
    )
    parent.close()

assert not (root / "runtime").exists()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_build_readonly_controlled_path_blocks_same_uid_replacement() {
    let test_dir = TestDir::new("readonly-path");
    let body = format!(
        r##"import pathlib

root = pathlib.Path({root:?})
tool = root / "tool"
replacement = root / "replacement"
marker = root / "replacement-marker"
attacker = root / "attacker.sh"
tool.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
replacement.write_text(
    f"#!/bin/sh\nprintf replacement > \"{{marker}}\"\n",
    encoding="utf-8",
)
attacker.write_text(
    "#!/bin/bash\nset -eu\n"
    "/usr/bin/chmod 0700 \"$1\"\n"
    "/usr/bin/rm -f \"$1/probe\"\n"
    "/usr/bin/ln -s \"$2\" \"$1/probe\"\n"
    "probe\n",
    encoding="utf-8",
)
for path in (tool, replacement, attacker):
    path.chmod(0o755)

with controller.HeldExecutable.open("probe", tool) as probe:
    aliases = controller.FrozenToolDirectory.create(root / "tools", {{"probe": probe}})
    with controller.HeldExecutable.open("shell", pathlib.Path("/usr/bin/bash")) as shell:
        result = controller.run_bounded(
            shell,
            ["bash", str(attacker), aliases.child_path, str(replacement)],
            env={{"PATH": aliases.child_path, "HOME": str(root), "TMPDIR": str(root)}},
            timeout_ms=2000,
            term_grace_ms=100,
            stdout_limit_bytes=64,
            stderr_limit_bytes=4096,
            extra_fds=(probe.fd,),
            readonly_bind_directories=(aliases,),
        )
    assert result.exit_code != 0
    assert not marker.exists()
    aliases.verify_frozen()
    aliases.remove()
"##,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
fn oracle_build_candidate_parent_replacement_fails_without_redirected_entries() {
    let test_dir = TestDir::new("parent-binding");
    let parent = test_dir.path().join("candidate-parent");
    let moved_parent = test_dir.path().join("held-parent");
    let replacement_parent = test_dir.path().join("replacement-parent");
    let source = test_dir.path().join("source");
    fs::create_dir(&parent).unwrap();
    fs::create_dir(&source).unwrap();
    let metadata = parent.join("primary-build.json");
    let fixture = canonical_build("primary");
    let body = format!(
        r#"import json
import os
import pathlib
import stat

parent = pathlib.Path({parent:?})
moved_parent = pathlib.Path({moved_parent:?})
replacement_parent = pathlib.Path({replacement_parent:?})
source = pathlib.Path({source:?})
metadata = pathlib.Path({metadata:?})
document = json.loads(r'''{document}''')
original_fsync = os.fsync
redirected = False

def redirect_after_temp_fsync(fd):
    global redirected
    original_fsync(fd)
    if not redirected and stat.S_ISREG(os.fstat(fd).st_mode):
        parent.rename(moved_parent)
        replacement_parent.mkdir()
        replacement_parent.rename(parent)
        redirected = True

os.fsync = redirect_after_temp_fsync
failure = None
try:
    controller.publish_candidate(metadata, document)
except BaseException as error:
    failure = error
finally:
    os.fsync = original_fsync

def candidate_entries(directory):
    if not directory.exists():
        return []
    return [entry.name for entry in directory.iterdir() if "candidate" in entry.name or entry.name == metadata.name]

leftovers = {{
    "held": candidate_entries(moved_parent),
    "replacement": candidate_entries(parent),
    "source": candidate_entries(source),
}}
if failure is None or any(leftovers.values()):
    raise AssertionError(f"PARENT_REDIRECTED failure={{failure!r}} leftovers={{leftovers!r}}")
"#,
        parent = parent.to_string_lossy(),
        moved_parent = moved_parent.to_string_lossy(),
        replacement_parent = replacement_parent.to_string_lossy(),
        source = source.to_string_lossy(),
        metadata = metadata.to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_build_candidate_json_enforces_the_one_mibibyte_limit_before_temp_create() {
    let test_dir = TestDir::new("json-limit");
    let exact = test_dir.path().join("exact.json");
    let oversized = test_dir.path().join("oversized.json");
    let body = format!(
        r#"import pathlib

limit = 1024 * 1024
exact = pathlib.Path({exact:?})
oversized = pathlib.Path({oversized:?})
empty_size = len(controller.canonical_json_bytes({{"pad": ""}}))
exact_document = {{"pad": "x" * (limit - empty_size)}}
oversized_document = {{"pad": "x" * (limit + 1 - empty_size)}}
assert len(controller.canonical_json_bytes(exact_document)) == limit
assert len(controller.canonical_json_bytes(oversized_document)) == limit + 1
controller.publish_candidate(exact, exact_document)
assert exact.stat().st_size == limit
try:
    controller.publish_candidate(oversized, oversized_document)
except controller.OracleError:
    pass
else:
    raise AssertionError("oversized candidate was published")
assert not oversized.exists()
assert not list(oversized.parent.glob(".*.candidate-*"))
"#,
        exact = exact.to_string_lossy(),
        oversized = oversized.to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
    let exact_bytes = fs::read(&exact).expect("exact-limit candidate must exist");
    assert_eq!(exact_bytes.len(), 1024 * 1024);
    let exact_json: Value =
        serde_json::from_slice(&exact_bytes).expect("exact-limit payload must be Rust JSON");
    assert!(exact_json["pad"].as_str().is_some());
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_build_cleanup_aggregates_failures_and_runs_every_action() {
    let test_dir = TestDir::new("cleanup-aggregation");
    let candidate = test_dir.path().join("primary-build.json");
    let body = format!(
        r#"import pathlib

candidate = pathlib.Path({candidate:?})
action_names = ["alias remove", "tool close", "later close", "runtime remove", "source close"]

def action(events, name, failing_name):
    def run():
        events.append(name)
        if name == failing_name:
            raise OSError(f"{{name}} boom")
    return run

for failing_name in ["alias remove", "tool close", "runtime remove"]:
    events = []
    actions = [(name, action(events, name, failing_name)) for name in action_names]
    try:
        controller._run_cleanup_actions(actions, controller.OracleError("business boom"))
    except controller.OracleError as error:
        message = str(error)
    else:
        raise AssertionError("cleanup failures were ignored")

    assert events == action_names, (failing_name, events)
    for expected in ["business boom", failing_name, f"{{failing_name}} boom"]:
        assert expected in message, (expected, message)
assert not candidate.exists()
"#,
        candidate = candidate.to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_rejects_existing_output_before_source_access() {
    let test_dir = TestDir::new("verifier-existing-output");
    let missing_source = test_dir.path().join("missing-source");
    let missing_primary = test_dir.path().join("missing-primary.json");
    let output_path = test_dir.path().join("oracle-provenance.json");
    fs::write(&output_path, b"do-not-replace\n").unwrap();

    let output = Command::new("/usr/bin/bash")
        .arg(verify_script_path())
        .arg("--source")
        .arg(&missing_source)
        .arg("--primary-metadata")
        .arg(&missing_primary)
        .arg("--output")
        .arg(&output_path)
        .arg("--run-after-ready")
        .arg("/bin/true")
        .env_clear()
        .output()
        .expect("Redis verifier wrapper must start");

    assert!(
        !output.status.success(),
        "existing output target was accepted"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("already exists"),
        "verifier did not reject the output target first: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(&output_path).unwrap(), b"do-not-replace\n");
    assert!(
        fs::read_dir(test_dir.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("oracle-verifier")),
        "verifier created temporary resources before rejecting the existing output"
    );
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_callback_output_flood_reaps_its_process_group() {
    let test_dir = TestDir::new("verifier-callback-flood");
    let body = format!(
        r##"import os
import pathlib

root = pathlib.Path({root:?})
script = root / "callback.sh"
child_pid = root / "child.pid"
script.write_text(
    "#!/bin/sh\n(sleep 30) &\nprintf '%s' \"$!\" > \"$1\"\n"
    "while :; do printf 0123456789; printf abcdefghij >&2; done\n",
    encoding="utf-8",
)
script.chmod(0o755)
(root / "home").mkdir()
(root / "tmp").mkdir()
with controller.HeldExecutable.open("callback", script) as held:
    result = controller.run_bounded(
        held,
        [str(script), str(child_pid)],
        env={{"PATH": "/usr/bin:/bin", "HOME": str(root / "home"), "TMPDIR": str(root / "tmp")}},
        timeout_ms=5000,
        term_grace_ms=100,
        stdout_limit_bytes=64,
        stderr_limit_bytes=64,
        terminate_on_output_limit=True,
    )
assert not result.timed_out
assert result.output_truncated
assert result.process_group_reaped
pid = int(child_pid.read_text(encoding="utf-8"))
try:
    os.kill(pid, 0)
except ProcessLookupError:
    pass
else:
    raise AssertionError(f"callback descendant {{pid}} survived output-flood cleanup")
"##,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_held_source_rename_replace_uses_original_or_fails_closed() {
    let test_dir = TestDir::new("verifier-source-replace");
    let body = format!(
        r#"import os
import pathlib

root = pathlib.Path({root:?})
source = root / "source-a"
moved = root / "held-source-a"
source.mkdir()
(source / "identity").write_text("original", encoding="utf-8")
held = controller.HeldDirectory.open(source)
try:
    source.rename(moved)
    source.mkdir()
    (source / "identity").write_text("replacement", encoding="utf-8")
    fd = held.open_regular("identity")
    try:
        assert os.read(fd, 64) == b"original"
    finally:
        os.close(fd)
    try:
        held.verify_path()
    except controller.OracleError:
        pass
    else:
        raise AssertionError("source A rename-replace was not rejected")
finally:
    held.close()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_provenance_publish_is_exclusive_atomic_and_rust_validated() {
    let test_dir = TestDir::new("verifier-publish");
    let provenance = test_dir.path().join("oracle-provenance.json");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import pathlib

provenance = pathlib.Path({provenance:?})
document = json.loads(r'''{document}''')
controller.publish_provenance(provenance, document)
assert provenance.exists()
try:
    controller.publish_provenance(provenance, document)
except controller.OracleError:
    pass
else:
    raise AssertionError("existing provenance was overwritten")
assert not list(provenance.parent.glob(".*.provenance-*"))
"#,
        provenance = provenance.to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));

    let raw = fs::read_to_string(&provenance).expect("provenance must be published");
    OracleProvenance::from_json(&raw).expect("published provenance must satisfy Task 1 API");
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_provenance_parent_replacement_leaves_no_final_or_temp() {
    let test_dir = TestDir::new("verifier-publish-parent-replace");
    let parent = test_dir.path().join("output-parent");
    let moved = test_dir.path().join("held-output-parent");
    fs::create_dir(&parent).unwrap();
    let provenance = parent.join("oracle-provenance.json");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import os
import pathlib
import stat

parent = pathlib.Path({parent:?})
moved = pathlib.Path({moved:?})
provenance = pathlib.Path({provenance:?})
document = json.loads(r'''{document}''')
original_fsync = os.fsync
redirected = False

def replace_parent_after_temp_fsync(fd):
    global redirected
    original_fsync(fd)
    if not redirected and stat.S_ISREG(os.fstat(fd).st_mode):
        parent.rename(moved)
        parent.mkdir()
        redirected = True

os.fsync = replace_parent_after_temp_fsync
try:
    controller.publish_provenance(provenance, document)
except controller.OracleError:
    pass
else:
    raise AssertionError("output parent replacement was accepted")
finally:
    os.fsync = original_fsync

for directory in (parent, moved):
    assert not (directory / provenance.name).exists()
    assert not list(directory.glob(".*.provenance-*"))
"#,
        parent = parent.to_string_lossy(),
        moved = moved.to_string_lossy(),
        provenance = provenance.to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
fn oracle_verifier_wrappers_keep_verification_in_linux_and_preserve_callback_argv() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("compat crate must be below the repository root");
    let bash = fs::read_to_string(root.join("scripts/compat/verify-redis-8.8.1.sh"))
        .expect("Bash verifier wrapper must exist");
    let powershell = fs::read_to_string(root.join("scripts/compat/verify-redis-8.8.1.ps1"))
        .expect("PowerShell verifier wrapper must exist");

    assert!(bash.contains("oracle_controller.py"));
    assert!(
        bash.contains("\"$@\""),
        "Bash wrapper must preserve argv boundaries"
    );
    assert!(!bash.contains("eval "));
    assert!(powershell.contains("wslpath"));
    assert!(powershell.contains("--run-after-ready"));
    assert!(!powershell.contains("Invoke-Expression"));
    assert!(!powershell.contains("Start-Process"));
}

#[test]
#[ignore = "real external Redis 8.8.1 primary plus independent rebuild"]
#[cfg(target_os = "linux")]
fn oracle_verifier_real_redis_8_8_1_rebuild_runtime_callback_and_cleanup() {
    let test_dir = TestDir::new("verifier-real-redis");
    let source = test_dir.path().join("source-a");
    let primary = test_dir.path().join("primary-build.json");
    let provenance = test_dir.path().join("oracle-provenance.json");
    let callback = test_dir.path().join("callback.py");
    let callback_marker = test_dir.path().join("callback-ok.json");
    clone_exact_redis(&source);

    let primary_output = Command::new("/usr/bin/bash")
        .arg(build_script_path())
        .arg("--source")
        .arg(&source)
        .arg("--metadata")
        .arg(&primary)
        .env_clear()
        .output()
        .expect("primary build wrapper must start");
    assert!(
        primary_output.status.success(),
        "primary build failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&primary_output.stdout),
        String::from_utf8_lossy(&primary_output.stderr)
    );

    fs::write(
        &callback,
        r#"#!/usr/bin/python3
import json
import os
import pathlib
import socket
import sys

required = [
    "KIWI_REDIS_ORACLE_HOST",
    "KIWI_REDIS_ORACLE_PORT",
    "KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE",
]
missing = [name for name in required if not os.environ.get(name)]
if missing:
    raise SystemExit(f"missing Oracle callback environment: {missing}")
evidence_path = pathlib.Path(os.environ["KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE"])
evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
if evidence["binary_sha256"] == "" or evidence["info_redis_versions"] != ["8.8.1"]:
    raise SystemExit("invalid runtime evidence")
try:
    evidence_path.write_text("tampered", encoding="utf-8")
except OSError:
    pass
else:
    raise SystemExit("runtime evidence was writable")
with socket.create_connection(
    (os.environ["KIWI_REDIS_ORACLE_HOST"], int(os.environ["KIWI_REDIS_ORACLE_PORT"])),
    timeout=5,
) as connection:
    connection.sendall(b"*1\r\n$4\r\nPING\r\n")
    if connection.recv(64) != b"+PONG\r\n":
        raise SystemExit("Oracle runtime did not answer PING")
pathlib.Path(sys.argv[1]).write_text(json.dumps(evidence, sort_keys=True), encoding="utf-8")
"#,
    )
    .unwrap();
    let mut permissions = fs::metadata(&callback).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o755);
    fs::set_permissions(&callback, permissions).unwrap();

    let output = Command::new("/usr/bin/bash")
        .arg(verify_script_path())
        .arg("--source")
        .arg(&source)
        .arg("--primary-metadata")
        .arg(&primary)
        .arg("--output")
        .arg(&provenance)
        .arg("--run-after-ready")
        .arg(&callback)
        .arg(&callback_marker)
        .env_clear()
        .output()
        .expect("Redis verifier wrapper must start");
    assert!(
        output.status.success(),
        "verifier failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let raw = fs::read_to_string(&provenance).expect("final provenance must exist");
    let parsed = OracleProvenance::from_json(&raw).expect("final provenance must validate");
    assert_eq!(parsed.primary().source().commit(), REDIS_COMMIT);
    assert_eq!(parsed.rebuild().source().commit(), REDIS_COMMIT);
    assert_eq!(
        parsed.primary().redis_server().sha256(),
        parsed.rebuild().redis_server().sha256()
    );
    assert_eq!(parsed.runtime().info_redis_versions(), &[REDIS_TAG]);
    let document: Value = serde_json::from_str(&raw).unwrap();
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
        assert_eq!(document["cleanup"][field], json!(true), "cleanup.{field}");
    }
    assert!(callback_marker.exists(), "callback did not complete");
    assert!(
        !Path::new(document["rebuild"]["source"]["root_path"].as_str().unwrap()).exists(),
        "disposable rebuild checkout survived cleanup"
    );
    let runtime_pid = document["runtime"]["pid"].as_u64().unwrap();
    assert!(
        !Path::new(&format!("/proc/{runtime_pid}")).exists(),
        "Redis runtime survived cleanup"
    );
    assert!(
        fs::read_dir(test_dir.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("oracle-verifier")),
        "verifier temporary root survived cleanup"
    );
}

#[test]
#[ignore = "accept an externally generated provenance file through the Task 1 API"]
#[cfg(target_os = "linux")]
fn task1_external_provenance_file_is_accepted() {
    let path = std::env::var_os("KIWI_ORACLE_PROVENANCE")
        .map(PathBuf::from)
        .expect("KIWI_ORACLE_PROVENANCE must name the fresh acceptance output");
    let raw = fs::read_to_string(&path).expect("external provenance must be readable");
    let provenance = OracleProvenance::from_json(&raw)
        .unwrap_or_else(|error| panic!("Task 1 rejected {}: {error}", path.display()));
    assert_eq!(provenance.schema_version(), PROVENANCE_SCHEMA);
    assert_eq!(provenance.primary().source().commit(), REDIS_COMMIT);
    assert_eq!(provenance.rebuild().source().commit(), REDIS_COMMIT);
    assert_eq!(provenance.runtime().info_redis_versions(), &[REDIS_TAG]);
}

#[test]
#[ignore = "external exact checkout; run with --include-ignored"]
#[cfg(target_os = "linux")]
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
#[ignore = "external exact checkout; run with --include-ignored"]
#[cfg(target_os = "linux")]
fn oracle_build_rejects_fixed_commit_tree_and_index_mutations_before_make() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let test_dir = TestDir::new("commit-tree");
    let seed = test_dir.path().join("seed");
    clone_exact_redis(&seed);
    for mutation in ["assume", "skip", "regular", "mode", "symlink"] {
        let source = test_dir.path().join(format!("source-{mutation}"));
        let metadata = test_dir.path().join(format!("{mutation}-build.json"));
        let marker = test_dir.path().join(format!("{mutation}-make-started"));
        clone_local_exact_redis(&seed, &source);
        let makefile = source.join("Makefile");
        let original_makefile = fs::read_to_string(&makefile).unwrap();
        fs::write(
            &makefile,
            format!(
                "$(shell /usr/bin/touch {})\n$(error mutation reached make)\n{}",
                marker.display(),
                original_makefile
            ),
        )
        .unwrap();
        match mutation {
            "assume" => {
                git_fixture(&source, &["update-index", "--assume-unchanged", "Makefile"]);
            }
            "skip" => {
                git_fixture(&source, &["update-index", "--skip-worktree", "Makefile"]);
            }
            "regular" => {
                fs::write(source.join("README.md"), b"tracked regular mutation\n").unwrap();
                git_fixture(&source, &["update-index", "--assume-unchanged", "Makefile"]);
            }
            "mode" => {
                let path = source.join("README.md");
                let mut permissions = fs::metadata(&path).unwrap().permissions();
                permissions.set_mode(0o755);
                fs::set_permissions(path, permissions).unwrap();
                git_fixture(&source, &["update-index", "--assume-unchanged", "Makefile"]);
            }
            "symlink" => {
                fs::remove_file(source.join("README.md")).unwrap();
                symlink("LICENSE.txt", source.join("README.md")).unwrap();
                git_fixture(&source, &["update-index", "--assume-unchanged", "Makefile"]);
            }
            _ => unreachable!(),
        }

        let output = Command::new("/usr/bin/bash")
            .arg(build_script_path())
            .arg("--source")
            .arg(&source)
            .arg("--metadata")
            .arg(&metadata)
            .env_clear()
            .output()
            .expect("Redis build wrapper must start");
        assert!(!output.status.success(), "{mutation} mutation was accepted");
        assert!(
            !marker.exists(),
            "{mutation} mutation reached make before the fixed-tree gate"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        let expected_path = if matches!(mutation, "regular" | "mode" | "symlink") {
            "README.md"
        } else {
            "Makefile"
        };
        assert!(
            stderr.contains(expected_path),
            "{mutation} rejection did not identify {expected_path}: {stderr}"
        );
        assert!(!metadata.exists(), "{mutation} mutation published metadata");
    }
}

#[test]
#[ignore = "external exact checkout; run with --include-ignored"]
#[cfg(target_os = "linux")]
fn oracle_build_rejects_replace_ref_tree_before_make() {
    let test_dir = TestDir::new("replace-ref");
    let source = test_dir.path().join("source");
    let metadata = test_dir.path().join("primary-build.json");
    let marker = test_dir.path().join("replace-tree-reached-make");
    clone_exact_redis(&source);

    let makefile = source.join("Makefile");
    let original = fs::read_to_string(&makefile).unwrap();
    fs::write(
        &makefile,
        format!(
            "$(shell /usr/bin/touch {})\n$(error replace tree reached make)\n{}",
            marker.display(),
            original
        ),
    )
    .unwrap();
    git_fixture(&source, &["add", "Makefile"]);
    git_fixture(
        &source,
        &[
            "-c",
            "user.name=Oracle Test",
            "-c",
            "user.email=oracle@example.invalid",
            "commit",
            "-m",
            "malicious replacement tree",
        ],
    );
    let malicious = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(&source)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(malicious.status.success());
    let malicious = String::from_utf8(malicious.stdout).unwrap();
    let malicious = malicious.trim();
    git_fixture(&source, &["replace", REDIS_COMMIT, malicious]);
    git_fixture(&source, &["update-ref", "HEAD", REDIS_COMMIT]);
    let status = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(&source)
        .args(["status", "--porcelain=v1", "--untracked-files=all"])
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(
        status.stdout.is_empty(),
        "replace-ref fixture must look clean"
    );

    let output = Command::new("/usr/bin/bash")
        .arg(build_script_path())
        .arg("--source")
        .arg(&source)
        .arg("--metadata")
        .arg(&metadata)
        .env_clear()
        .output()
        .expect("Redis build wrapper must start");
    assert!(!output.status.success(), "replace ref was accepted");
    assert!(!marker.exists(), "replacement tree reached Make");
    assert!(!metadata.exists(), "replace ref published metadata");
}

#[test]
#[ignore = "external exact checkout; run with --include-ignored"]
#[cfg(target_os = "linux")]
fn oracle_build_disables_repo_local_fsmonitor_before_any_git_query() {
    use std::os::unix::fs::PermissionsExt;

    let test_dir = TestDir::new("git-fsmonitor");
    let source = test_dir.path().join("source");
    let metadata = test_dir.path().join("primary-build.json");
    let marker = test_dir.path().join("fsmonitor-ran");
    let monitor = test_dir.path().join("fsmonitor.sh");
    clone_exact_redis(&source);
    fs::write(
        &monitor,
        format!("#!/bin/sh\n/usr/bin/touch '{}'\nexit 0\n", marker.display()),
    )
    .unwrap();
    let mut permissions = fs::metadata(&monitor).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&monitor, permissions).unwrap();
    git_fixture(
        &source,
        &["config", "core.fsmonitor", monitor.to_str().unwrap()],
    );
    git_fixture(
        &source,
        &[
            "remote",
            "set-url",
            "origin",
            "https://example.invalid/redis.git",
        ],
    );

    let output = Command::new("/usr/bin/bash")
        .arg(build_script_path())
        .arg("--source")
        .arg(&source)
        .arg("--metadata")
        .arg(&metadata)
        .env_clear()
        .output()
        .expect("Redis build wrapper must start");
    assert!(
        !output.status.success(),
        "repo-local fsmonitor config was accepted"
    );
    assert!(!marker.exists(), "repo-local fsmonitor executable ran");
    assert!(!metadata.exists(), "fsmonitor fixture published metadata");
}

#[test]
#[ignore = "external exact checkout; run with --include-ignored"]
#[cfg(target_os = "linux")]
fn oracle_build_rejects_non_independent_git_object_storage() {
    let test_dir = TestDir::new("git-storage");
    let seed = test_dir.path().join("seed");
    clone_exact_redis(&seed);
    for (name, relative, content, expected) in [
        (
            "shallow",
            ".git/shallow",
            format!("{REDIS_COMMIT}\n"),
            ".git/shallow",
        ),
        (
            "grafts",
            ".git/info/grafts",
            format!("{REDIS_COMMIT}\n"),
            ".git/info/grafts",
        ),
        (
            "alternates",
            ".git/objects/info/alternates",
            format!("{}\n", seed.join(".git/objects").display()),
            ".git/objects/info/alternates",
        ),
        (
            "commondir",
            ".git/commondir",
            ".\n".to_owned(),
            ".git/commondir",
        ),
        (
            "promisor",
            ".git/objects/pack/fake.promisor",
            String::new(),
            "promisor packs",
        ),
    ] {
        let source = test_dir.path().join(format!("source-{name}"));
        let metadata = test_dir.path().join(format!("{name}-build.json"));
        clone_local_exact_redis(&seed, &source);
        fs::write(source.join(relative), content).unwrap();

        let output = Command::new("/usr/bin/bash")
            .arg(build_script_path())
            .arg("--source")
            .arg(&source)
            .arg("--metadata")
            .arg(&metadata)
            .env_clear()
            .output()
            .expect("Redis build wrapper must start");
        assert!(!output.status.success(), "{name} Git storage was accepted");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "{name} rejection did not identify {expected}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!metadata.exists(), "{name} Git storage published metadata");
    }
}

#[test]
#[ignore = "real external Redis 8.8.1 build; run with --include-ignored"]
#[cfg(target_os = "linux")]
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
    assert!(
        build
            .tools()
            .iter()
            .all(|tool| !tool.version().starts_with("identity-only sha256:")),
        "tool version evidence must come from an actual controlled command"
    );
    assert!(
        build
            .tools()
            .iter()
            .any(|tool| tool.role() == "cc-component-cc1" && tool.version().contains("GNU C")),
        "held cc1 must have actual version output"
    );
    assert!(
        build
            .tools()
            .iter()
            .all(|tool| !tool.role().starts_with("cc-resource-")),
        "non-executable GCC resources must not masquerade as versioned tools"
    );
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
