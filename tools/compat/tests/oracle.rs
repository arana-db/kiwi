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
use sha2::{Digest, Sha256};
#[cfg(target_os = "linux")]
use std::ffi::CString;
use std::fs;
#[cfg(target_os = "linux")]
use std::io::{Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::process::{Command, Output, Stdio};
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
fn sealed_test_file(name: &str, bytes: &[u8]) -> fs::File {
    let name = CString::new(name).expect("memfd name must not contain NUL");
    let fd =
        unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) };
    assert!(
        fd >= 0,
        "memfd_create failed: {}",
        std::io::Error::last_os_error()
    );
    let mut file = unsafe { fs::File::from_raw_fd(fd) };
    file.write_all(bytes).expect("write sealed test file");
    file.seek(SeekFrom::Start(0))
        .expect("rewind sealed test file");
    let seals = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    let result = unsafe { libc::fcntl(fd, libc::F_ADD_SEALS, seals) };
    assert_eq!(
        result,
        0,
        "seal test file: {}",
        std::io::Error::last_os_error()
    );
    file
}

#[cfg(target_os = "linux")]
fn strict_verifier_command(
    provenance: &fs::File,
    evidence: &fs::File,
    evidence_file_name: &str,
    expected_head: &str,
) -> Command {
    const PROVENANCE_CHILD_FD: i32 = 198;
    const EVIDENCE_CHILD_FD: i32 = 199;
    let provenance_fd = provenance.as_raw_fd();
    let evidence_fd = evidence.as_raw_fd();
    assert!(![PROVENANCE_CHILD_FD, EVIDENCE_CHILD_FD].contains(&provenance_fd));
    assert!(![PROVENANCE_CHILD_FD, EVIDENCE_CHILD_FD].contains(&evidence_fd));

    let mut command = Command::new(env!("CARGO_BIN_EXE_kiwi-verify-oracle-evidence"));
    command
        .arg(PROVENANCE_CHILD_FD.to_string())
        .arg(EVIDENCE_CHILD_FD.to_string())
        .arg(evidence_file_name)
        .arg(expected_head)
        .arg("3333333333333333333333333333333333333333");
    unsafe {
        command.pre_exec(move || {
            for (source, target) in [
                (provenance_fd, PROVENANCE_CHILD_FD),
                (evidence_fd, EVIDENCE_CHILD_FD),
            ] {
                if libc::dup2(source, target) < 0 || libc::fcntl(target, libc::F_SETFD, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    command
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
fn oracle_controlled_path_keeps_placeholder_when_open_returns_reserved_fd() {
    let test_dir = TestDir::new("controlled-path-reserved-fd");
    let body = format!(
        r#"import errno
import os
import pathlib

root = pathlib.Path({root:?})
aliases = controller.FrozenToolDirectory.create(root / "tools", {{}})
padding = []
try:
    with controller.HeldExecutable.open("shell", pathlib.Path("/usr/bin/bash")) as shell:
        while True:
            fd = os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)
            if fd == controller.CONTROLLED_PATH_FD:
                os.close(fd)
                break
            if fd > controller.CONTROLLED_PATH_FD:
                raise AssertionError("unable to reserve the controlled PATH descriptor")
            padding.append(fd)
        result = controller.run_bounded(
            shell,
            ["bash", "-c", "exit 0"],
            env={{"PATH": aliases.child_path, "HOME": str(root), "TMPDIR": str(root)}},
            timeout_ms=2000,
            term_grace_ms=100,
            stdout_limit_bytes=64,
            stderr_limit_bytes=4096,
            readonly_bind_directories=(aliases,),
        )
        assert result.exit_code == 0, (result.stdout, result.stderr)
        try:
            os.fstat(controller.CONTROLLED_PATH_FD)
        except OSError as error:
            assert error.errno == errno.EBADF
        else:
            raise AssertionError("controlled PATH descriptor leaked after command completion")
finally:
    for fd in reversed(padding):
        os.close(fd)
    aliases.remove()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_supervised_setup_keeps_devnull_when_open_returns_stdin_fd() {
    let test_dir = TestDir::new("supervised-setup-stdin-fd");
    let body = format!(
        r#"import errno
import os
import pathlib

root = pathlib.Path({root:?})
aliases = controller.FrozenToolDirectory.create(root / "tools", {{}})
saved_stdin = os.dup(0)
try:
    with controller.HeldExecutable.open("shell", pathlib.Path("/usr/bin/bash")) as shell:
        os.close(0)
        try:
            result = controller.run_bounded(
                shell,
                [
                    "bash",
                    "-c",
                    "[[ $(/usr/bin/readlink /proc/self/fd/0) == /dev/null ]]",
                ],
                env={{"PATH": aliases.child_path, "HOME": str(root), "TMPDIR": str(root)}},
                timeout_ms=2000,
                term_grace_ms=100,
                stdout_limit_bytes=64,
                stderr_limit_bytes=4096,
                readonly_bind_directories=(aliases,),
            )
            try:
                os.fstat(0)
            except OSError as error:
                assert error.errno == errno.EBADF
            else:
                raise AssertionError("run_bounded changed the parent stdin descriptor")
        finally:
            os.dup2(saved_stdin, 0)
        assert result.exit_code == 0, (result.stdout, result.stderr)
finally:
    os.close(saved_stdin)
    aliases.remove()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_supervised_setup_keeps_pipe_when_write_end_is_stdout_fd() {
    let test_dir = TestDir::new("supervised-setup-stdout-fd");
    let body = format!(
        r#"import errno
import os
import pathlib

root = pathlib.Path({root:?})
aliases = controller.FrozenToolDirectory.create(root / "tools", {{}})
saved_stdin = os.dup(0)
saved_stdout = os.dup(1)
try:
    with controller.HeldExecutable.open("shell", pathlib.Path("/usr/bin/bash")) as shell:
        os.close(0)
        os.close(1)
        try:
            result = controller.run_bounded(
                shell,
                [
                    "bash",
                    "-c",
                    "[[ $(/usr/bin/readlink /proc/self/fd/0) == /dev/null ]] && printf probe",
                ],
                env={{"PATH": aliases.child_path, "HOME": str(root), "TMPDIR": str(root)}},
                timeout_ms=2000,
                term_grace_ms=100,
                stdout_limit_bytes=64,
                stderr_limit_bytes=4096,
                readonly_bind_directories=(aliases,),
            )
            for fd in (0, 1):
                try:
                    os.fstat(fd)
                except OSError as error:
                    assert error.errno == errno.EBADF
                else:
                    raise AssertionError(f"run_bounded changed parent descriptor {{fd}}")
        finally:
            os.dup2(saved_stdin, 0)
            os.dup2(saved_stdout, 1)
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert result.stdout == b"probe"
finally:
    os.close(saved_stdout)
    os.close(saved_stdin)
    aliases.remove()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_pid_namespace_keeps_control_pipe_off_standard_fds() {
    let test_dir = TestDir::new("pid-namespace-control-standard-fds");
    let body = format!(
        r#"import errno
import os
import pathlib

root = pathlib.Path({root:?})
def run_case(closed_fds):
    saved = {{fd: os.dup(fd) for fd in closed_fds}}
    try:
        with controller.HeldExecutable.open("shell", pathlib.Path("/usr/bin/bash")) as shell:
            for fd in closed_fds:
                os.close(fd)
            try:
                result = controller.run_bounded(
                    shell,
                    [
                        "bash",
                        "-c",
                        "[[ $(/usr/bin/readlink /proc/self/fd/0) == /dev/null ]] && printf probe",
                    ],
                    env={{"PATH": "/usr/bin:/bin", "HOME": str(root), "TMPDIR": str(root)}},
                    timeout_ms=2000,
                    term_grace_ms=100,
                    stdout_limit_bytes=64,
                    stderr_limit_bytes=4096,
                    pid_namespace=True,
                )
                for fd in closed_fds:
                    try:
                        os.fstat(fd)
                    except OSError as error:
                        assert error.errno == errno.EBADF
                    else:
                        raise AssertionError(f"run_bounded changed parent descriptor {{fd}}")
            finally:
                for fd in closed_fds:
                    os.dup2(saved[fd], fd)
            assert result.exit_code == 0, (result.stdout, result.stderr)
            assert result.stdout == b"probe"
            assert result.namespace_init_pid is not None
    finally:
        for saved_fd in saved.values():
            os.close(saved_fd)

run_case((0, 1))
run_case((0, 2))
"#,
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
(root / "tracked").write_bytes(b"tracked")
baseline = controller.snapshot_tree(root)
tracked_tree = {{"tracked": ("100644", controller._git_blob_oid_bytes(b"tracked"))}}
tracked_stat = (root / "tracked").stat()
os.utime(root / "tracked", ns=(tracked_stat.st_atime_ns, tracked_stat.st_mtime_ns + 1_000_000_000))
(root / "z-last").write_bytes(b"z")
(root / "a-first").write_bytes(b"a")
os.symlink("a-first", root / "m-link")
manifest = controller.scan_artifacts(root, baseline, tracked_tree=tracked_tree)
assert [entry["path"] for entry in manifest] == ["a-first", "m-link", "z-last"]
assert manifest[0]["kind"] == "regular"
assert manifest[1] == {{"kind": "symlink", "path": "m-link", "mode": os.lstat(root / "m-link").st_mode, "target": "a-first"}}
(root / "tracked").write_bytes(b"modified")
modified = controller.scan_artifacts(root, baseline, tracked_tree=tracked_tree)
assert "tracked" in [entry["path"] for entry in modified]
(root / "tracked").write_bytes(b"tracked")

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
        r#"import os
import pathlib

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
    let evidence_path = test_dir.path().join("vector-differential-evidence.json");
    fs::write(&output_path, b"do-not-replace\n").unwrap();

    let output = Command::new("/usr/bin/bash")
        .arg(verify_script_path())
        .arg("--source")
        .arg(&missing_source)
        .arg("--primary-metadata")
        .arg(&missing_primary)
        .arg("--output")
        .arg(&output_path)
        .arg("--evidence-output")
        .arg(&evidence_path)
        .arg("--expected-head")
        .arg("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .arg("--publication-verifier")
        .arg(env!("CARGO_BIN_EXE_kiwi-verify-oracle-evidence"))
        .arg("--callback-input")
        .arg(test_dir.path())
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
fn oracle_verifier_callback_pid_namespace_contains_setsid_descendants_and_pipe_drains() {
    let test_dir = TestDir::new("verifier-callback-pid-namespace");
    let body = format!(
        r#"import os
import pathlib
import signal
import time

root = pathlib.Path({root:?})
(root / "home").mkdir()
(root / "tmp").mkdir()
callback_code = r'''
import os, sys, time
close_pipes = sys.argv[1] == "closed"
flood = sys.argv[2] == "flood"
child = os.fork()
if child == 0:
    os.setsid()
    if close_pipes:
        os.close(1)
        os.close(2)
    time.sleep(1.5)
    os._exit(0)
if flood:
    while True:
        os.write(1, b"x" * 4096)
else:
    time.sleep(30)
'''

failures = []
with controller.HeldExecutable.open("callback-python", pathlib.Path("/usr/bin/python3")) as held:
    for pipes, trigger in (("closed", "timeout"), ("inherited", "timeout"), ("closed", "flood"), ("inherited", "flood")):
        started = time.monotonic()
        result = controller.run_bounded(
            held,
            ["python3", "-I", "-B", "-c", callback_code, pipes, trigger],
            env={{"PATH": "/usr/bin:/bin", "HOME": str(root / "home"), "TMPDIR": str(root / "tmp")}},
            timeout_ms=250,
            term_grace_ms=100,
            stdout_limit_bytes=4096,
            stderr_limit_bytes=4096,
            terminate_on_output_limit=True,
            pid_namespace=True,
        )
        elapsed = time.monotonic() - started
        deadline_ok = elapsed < 0.8
        pid = result.namespace_init_pid
        start_time = result.namespace_init_start_time
        alive = controller._process_matches_start_time(pid, start_time)
        if alive or not deadline_ok:
            failures.append((pipes, trigger, pid, alive, elapsed, result))

if failures:
    raise AssertionError(f"setsid descendants escaped callback containment or pipe deadline: {{failures}}")
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn callback_rejects_noncanonical_script_paths() {
    let test_dir = TestDir::new("callback-noncanonical-script");
    let body = r##"
import os
import pathlib

root = pathlib.Path(__file__).parent

for case, script_argument in (
    ("parent", "/callback-input/scripts/../scripts/runner.sh"),
    ("dot", "/callback-input/scripts/./runner.sh"),
    ("empty", "/callback-input//scripts/runner.sh"),
):
    case_root = root / case
    callback_input = case_root / "callback-input"
    callback_path = case_root / "callback"
    (callback_input / "scripts").mkdir(parents=True)
    callback_path.mkdir()
    runner = callback_input / "scripts" / "runner.sh"
    runner.write_text(
        "#!/bin/bash\nprintf 'executed\\n' > /work/executed.marker\n",
        encoding="utf-8",
    )
    runner.chmod(0o755)
    evidence = case_root / "runtime-evidence.json"
    evidence.write_text('{"runtime":true}', encoding="utf-8")
    evidence.chmod(0o400)
    aliases = controller.FrozenToolDirectory.create(case_root / "aliases", {})
    callback_root = controller.HeldDirectory.open(callback_path)
    callback_input_root = controller.HeldDirectory.open(callback_input)
    callback_shell = controller.HeldExecutable.open("shell", pathlib.Path("/bin/bash"))
    evidence_fd = os.open(evidence, os.O_RDONLY | os.O_CLOEXEC)
    try:
        try:
            controller._run_callback(
                ["/bin/bash", script_argument],
                callback_shell,
                aliases,
                callback_root,
                callback_input_root,
                evidence_fd,
                "127.0.0.1",
                1,
            )
        except controller.OracleError as error:
            if "callback script path must be canonical" not in str(error):
                raise AssertionError(
                    f"{case} failed for an unrelated reason: {error}"
                ) from error
        else:
            raise AssertionError(f"{case} callback path was accepted")
        assert not (callback_path / "work" / "executed.marker").exists(), case
    finally:
        aliases.remove()
        os.close(evidence_fd)
        callback_shell.close()
        callback_input_root.close()
        callback_root.close()
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn callback_rejects_unregistered_executable() {
    let test_dir = TestDir::new("callback-unregistered-executable");
    let body = r##"
import os
import pathlib

root = pathlib.Path(__file__).parent
callback_input = root / "callback-input"
callback_path = root / "callback"
(callback_input / "scripts").mkdir(parents=True)
callback_path.mkdir()
runner = callback_input / "scripts" / "runner.py"
runner.write_text(
    'import pathlib\npathlib.Path("/work/executed.marker").write_text("executed\\n", encoding="utf-8")\n',
    encoding="utf-8",
)
evidence = root / "runtime-evidence.json"
evidence.write_text('{"runtime":true}', encoding="utf-8")
evidence.chmod(0o400)
aliases = controller.FrozenToolDirectory.create(root / "aliases", {})
callback_root = controller.HeldDirectory.open(callback_path)
callback_input_root = controller.HeldDirectory.open(callback_input)
callback_shell = controller.HeldExecutable.open("shell", pathlib.Path("/bin/bash"))
evidence_fd = os.open(evidence, os.O_RDONLY | os.O_CLOEXEC)
try:
    try:
        controller._run_callback(
            ["/usr/bin/python3", "/callback-input/scripts/runner.py"],
            callback_shell,
            aliases,
            callback_root,
            callback_input_root,
            evidence_fd,
            "127.0.0.1",
            1,
        )
    except controller.OracleError as error:
        if "callback executable must be the registered shell" not in str(error):
            raise AssertionError(f"unregistered executable failed for an unrelated reason: {error}") from error
    else:
        raise AssertionError("unregistered callback executable was accepted")
    assert not (callback_path / "work" / "executed.marker").exists()
finally:
    aliases.remove()
    os.close(evidence_fd)
    callback_shell.close()
    callback_input_root.close()
    callback_root.close()
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn callback_symlink_identity_rejects_hard_linked_runtime_entries() {
    let test_dir = TestDir::new("callback-runtime-symlink-hard-link");
    let body = r##"
import os
import pathlib

root = pathlib.Path(__file__).parent
repository = root / "repository"
(repository / "target" / "debug").mkdir(parents=True)
(repository / ".oracle-python").mkdir()
for relative in controller.CALLBACK_RUNTIME_PATHS:
    path = repository / relative
    path.write_bytes(relative.encode("utf-8"))
    path.chmod(0o755)
target = repository / ".oracle-python" / "module.py"
target.write_text("VALUE = 1\n", encoding="utf-8")
first = repository / ".oracle-python" / "first.py"
second = repository / ".oracle-python" / "second.py"
first.symlink_to("module.py")
os.link(first, second, follow_symlinks=False)
assert first.lstat().st_ino == second.lstat().st_ino
assert first.lstat().st_nlink == 2

with controller.HeldDirectory.open(repository) as held_repository:
    try:
        list(controller._runtime_callback_entries(held_repository))
    except controller.OracleError as error:
        if "runtime callback input contains a hard link" not in str(error):
            raise AssertionError(f"symlink hard link failed for an unrelated reason: {error}") from error
    else:
        raise AssertionError("hard-linked runtime symlinks were accepted")
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn callback_runtime_fifo_replacement_fails_without_blocking() {
    let test_dir = TestDir::new("callback-runtime-fifo");
    let body = r##"
import os
import pathlib
import signal
import time

root = pathlib.Path(__file__).parent
repository = root / "repository"
(repository / "target" / "debug").mkdir(parents=True)
(repository / ".oracle-python").mkdir()
os.mkfifo(repository / "target" / "debug" / "kiwi")
helper = repository / "target" / "debug" / "kiwi-required-vector-jobs"
helper.write_bytes(b"jobs-runtime")
helper.chmod(0o755)

def reject_block(_signum, _frame):
    raise AssertionError("runtime FIFO open blocked")

signal.signal(signal.SIGALRM, reject_block)
signal.alarm(2)
started = time.monotonic()
try:
    with controller.HeldDirectory.open(repository) as held_repository:
        try:
            list(controller._runtime_callback_entries(held_repository))
        except controller.OracleError:
            pass
        else:
            raise AssertionError("runtime FIFO was accepted")
finally:
    signal.alarm(0)
assert time.monotonic() - started < 1
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn callback_symlink_identity_rejects_equal_length_runtime_replacement() {
    let test_dir = TestDir::new("callback-runtime-symlink-replacement");
    let body = r##"
import os
import pathlib

root = pathlib.Path(__file__).parent
repository = root / "repository"
(repository / "target" / "debug").mkdir(parents=True)
(repository / ".oracle-python").mkdir()
for relative in controller.CALLBACK_RUNTIME_PATHS:
    path = repository / relative
    path.write_bytes(relative.encode("utf-8"))
    path.chmod(0o755)
python_root = repository / ".oracle-python"
(python_root / "first.py").write_text("VALUE = 1\n", encoding="utf-8")
(python_root / "other.py").write_text("VALUE = 2\n", encoding="utf-8")
link = python_root / "plugin.py"
link.symlink_to("first.py")
before = link.lstat()
original_readlink = controller.os.readlink
replaced = False

def replace_before_readlink(path, *, dir_fd=None):
    global replaced
    if path == "plugin.py" and dir_fd is not None and not replaced:
        os.rename(
            path,
            "plugin-original.py",
            src_dir_fd=dir_fd,
            dst_dir_fd=dir_fd,
        )
        os.symlink("other.py", path, dir_fd=dir_fd)
        replaced = True
    return original_readlink(path, dir_fd=dir_fd)

controller.os.readlink = replace_before_readlink
try:
    with controller.HeldDirectory.open(repository) as held_repository:
        try:
            list(controller._runtime_callback_entries(held_repository))
        except controller.OracleError as error:
            if "runtime callback symlink changed while reading" not in str(error):
                raise AssertionError(f"symlink replacement failed for an unrelated reason: {error}") from error
        else:
            raise AssertionError("equal-length runtime symlink replacement was accepted")
finally:
    controller.os.readlink = original_readlink
assert replaced
assert link.lstat().st_ino != before.st_ino
assert len(os.fsencode(link.readlink())) == before.st_size
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn callback_symlink_identity_rejects_hard_linked_frozen_entries() {
    let test_dir = TestDir::new("callback-frozen-symlink-hard-link");
    let body = r##"
import os
import pathlib
import types

root = pathlib.Path(__file__).parent / "frozen"
root.mkdir()
target = root / "target.py"
target_content = b"VALUE = 1\n"
target.write_bytes(target_content)
first = root / "first.py"
second = root / "second.py"
first.symlink_to("target.py")
second.symlink_to("target.py")
target_text = "target.py"
entries = [
    controller._manifest_entry("first.py", "runtime", "symlink", "120000", target_text),
    controller._manifest_entry("second.py", "runtime", "symlink", "120000", target_text),
    controller._manifest_entry("target.py", "runtime", "regular", "100644", target_content),
]
entries.sort(key=lambda entry: os.fsencode(str(entry["path"])))
manifest = {
    "schema_version": controller.CALLBACK_INPUT_MANIFEST_SCHEMA,
    "entry_count": len(entries),
    "total_bytes": len(target_content) + 2 * len(os.fsencode(target_text)),
    "entries": entries,
}
second.unlink()
os.link(first, second, follow_symlinks=False)
assert first.lstat().st_ino == second.lstat().st_ino
assert first.lstat().st_nlink == 2

with controller.HeldDirectory.open(root) as held_root:
    snapshot = types.SimpleNamespace(root=held_root, manifest=manifest)
    try:
        controller._verify_frozen_callback_manifest(snapshot)
    except controller.OracleError as error:
        if "frozen callback input contains an unexpected hard-link relationship" not in str(error):
            raise AssertionError(f"frozen symlink hard link failed for an unrelated reason: {error}") from error
    else:
        raise AssertionError("hard-linked frozen symlinks were accepted")
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_callback_input_exposes_repo_layout_read_only() {
    let test_dir = TestDir::new("verifier-callback-input");
    let body = format!(
        r#"import os
import pathlib

root = pathlib.Path({root:?})
callback_input = root / "callback-input"
(callback_input / "scripts").mkdir(parents=True)
(callback_input / "resources").mkdir()
(callback_input / "resources/value.txt").write_text("trusted-resource", encoding="utf-8")
runner = callback_input / "scripts/runner.sh"
runner.write_text(r'''#!/bin/bash
set -euo pipefail
value="$(cat /callback-input/resources/value.txt)"
if printf 'changed' > /callback-input/resources/value.txt 2>/dev/null; then
    exit 91
fi
printf '%s' "$value" > /work/result.txt
printf 'executed\n' > /work/executed.marker
''', encoding="utf-8")
runner.chmod(0o755)
callback_path = root / "callback"
callback_path.mkdir()
evidence = root / "runtime-evidence.json"
evidence.write_text('{{"runtime":true}}', encoding="utf-8")
evidence.chmod(0o400)
aliases = controller.FrozenToolDirectory.create(root / "aliases", {{}})
callback_root = controller.HeldDirectory.open(callback_path)
callback_input_root = controller.HeldDirectory.open(callback_input)
callback_shell = controller.HeldExecutable.open("shell", pathlib.Path("/bin/bash"))
evidence_fd = os.open(evidence, os.O_RDONLY | os.O_CLOEXEC)
try:
    controller._run_callback(
        ["/bin/bash", "/callback-input/scripts/runner.sh"],
        callback_shell,
        aliases,
        callback_root,
        callback_input_root,
        evidence_fd,
        "127.0.0.1",
        1,
    )
    assert (callback_path / "work/result.txt").read_text(encoding="utf-8") == "trusted-resource"
    assert (callback_path / "work/executed.marker").read_text(encoding="utf-8") == "executed\n"
    assert (callback_input / "resources/value.txt").read_text(encoding="utf-8") == "trusted-resource"
finally:
    aliases.remove()
    os.close(evidence_fd)
    callback_shell.close()
    callback_input_root.close()
    callback_root.close()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_callback_setup_stall_obeys_wall_clock_deadline() {
    let test_dir = TestDir::new("verifier-callback-setup-stall");
    let probe = test_dir.path().join("stall-probe.py");
    let marker = test_dir.path().join("setup.pid");
    let source = format!(
        r#"import importlib.util
import os
import pathlib
import sys
import time

controller_path = pathlib.Path({controller:?})
spec = importlib.util.spec_from_file_location("kiwi_oracle_controller", controller_path)
controller = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = controller
spec.loader.exec_module(controller)
root = pathlib.Path({root:?})
marker = pathlib.Path({marker:?})
for name in ("work", "input", "sandbox"):
    (root / name).mkdir()
evidence = root / "runtime-evidence.json"
evidence.write_text("{{}}", encoding="utf-8")
fds = [
    os.open(root / "work", os.O_RDONLY | os.O_DIRECTORY),
    os.open(root / "input", os.O_RDONLY | os.O_DIRECTORY),
    os.open(evidence, os.O_RDONLY),
    os.open(root / "sandbox", os.O_RDONLY | os.O_DIRECTORY),
]
original = controller._callback_filesystem_setup
original_preexec = controller._pid_namespace_preexec
def record_supervisor(*args):
    marker.write_text(str(os.getpid()), encoding="ascii")
    return original_preexec(*args)
def stall(*args):
    time.sleep(10)
controller._callback_filesystem_setup = stall
controller._pid_namespace_preexec = record_supervisor
started = time.monotonic()
try:
    with controller.HeldExecutable.open("callback", pathlib.Path("/bin/true")) as held:
        result = controller.run_bounded(
            held, ["/bin/true"], env={{"PATH":"/usr/bin:/bin"}}, timeout_ms=250,
            term_grace_ms=100, stdout_limit_bytes=1024, stderr_limit_bytes=1024,
            pid_namespace=True, callback_filesystem=tuple(fds),
        )
        assert result.timed_out
finally:
    controller._callback_filesystem_setup = original
    controller._pid_namespace_preexec = original_preexec
    for fd in fds:
        try: os.close(fd)
        except OSError: pass
assert time.monotonic() - started < 1.0
"#,
        controller = controller_path().to_string_lossy(),
        root = test_dir.path().to_string_lossy(),
        marker = marker.to_string_lossy(),
    );
    fs::write(&probe, source).unwrap();
    let output = Command::new("/usr/bin/timeout")
        .args(["--kill-after=0.5s", "1.5s", controlled_python(), "-I", "-B"])
        .arg(&probe)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .output()
        .expect("setup-stall probe must start");
    if let Ok(raw_pid) = fs::read_to_string(&marker)
        && let Ok(pid) = raw_pid.trim().parse::<i32>()
    {
        let probe_bytes = probe.as_os_str().as_encoded_bytes();
        let matches_probe = || {
            let cmdline = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
            cmdline
                .windows(probe_bytes.len())
                .any(|window| window == probe_bytes)
        };
        if matches_probe() {
            let _ = Command::new("/bin/kill")
                .args(["-TERM", "--", &pid.to_string()])
                .status();
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        if matches_probe() {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", "--", &pid.to_string()])
                .status();
        }
        assert!(
            !matches_probe(),
            "setup-stall supervisor PID survived cleanup"
        );
    }
    assert_probe_succeeds(output);
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_held_cleanup_rejects_replacement_without_touching_it() {
    let test_dir = TestDir::new("verifier-held-cleanup-replacement");
    let body = format!(
        r#"import os
import pathlib

root = pathlib.Path({root:?})
visible = root / "aliases"
moved = root / "held-aliases"
aliases = controller.FrozenToolDirectory.create(visible, {{}})
visible.rename(moved)
visible.mkdir()
marker = visible / "replacement.txt"
marker.write_text("untouched", encoding="utf-8")
try:
    aliases.remove_path()
except controller.OracleError:
    pass
else:
    raise AssertionError("replacement controlled-tool directory was removed")
assert marker.read_text(encoding="utf-8") == "untouched"
assert moved.exists()
aliases.close()

parent = controller.HeldDirectory.open(root)
os.mkdir("runtime", mode=0o700, dir_fd=parent.fd)
runtime = parent.open_directory("runtime")
(root / "runtime").rename(root / "held-runtime")
(root / "runtime").mkdir()
runtime_marker = root / "runtime/replacement.txt"
runtime_marker.write_text("untouched", encoding="utf-8")
try:
    controller._remove_runtime_directory(parent.fd, "runtime", runtime)
except controller.OracleError:
    pass
else:
    raise AssertionError("replacement runtime directory was removed")
assert runtime_marker.read_text(encoding="utf-8") == "untouched"
runtime.close()
parent.close()
assert not (root / "oracle-provenance.json").exists()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_held_cleanup_rechecks_identity_immediately_before_rmdir() {
    let test_dir = TestDir::new("verifier-held-cleanup-final-rmdir");
    let body = format!(
        r#"import os
import pathlib

root = pathlib.Path({root:?})
parent = controller.HeldDirectory.open(root)
os.mkdir("runtime", mode=0o700, dir_fd=parent.fd)
runtime = parent.open_directory("runtime")
moved = root / "held-runtime"
replacement = root / "runtime"
output = root / "oracle-provenance.json"
original_remove_contents = controller._remove_directory_contents

def replace_after_content_removal(directory_fd):
    original_remove_contents(directory_fd)
    replacement.rename(moved)
    replacement.mkdir()

controller._remove_directory_contents = replace_after_content_removal
cleanup_complete = False
try:
    try:
        controller._remove_runtime_directory(parent.fd, "runtime", runtime)
        cleanup_complete = True
    except controller.OracleError:
        pass
finally:
    controller._remove_directory_contents = original_remove_contents
    runtime.close()
    parent.close()

assert not cleanup_complete
assert replacement.is_dir(), "replacement directory was removed by final rmdir"
assert moved.is_dir(), "held directory was not preserved after cleanup rejection"
assert not output.exists()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_callback_mount_sandbox_blocks_transient_resource_mutations() {
    let test_dir = TestDir::new("verifier-callback-mount-sandbox");
    let body = format!(
        r#"import json
import os
import pathlib

root = pathlib.Path({root:?})
verifier = root / "verifier-root"
callback_path = verifier / "callback"
callback_path.mkdir(parents=True)
protected = {{}}
for name in ("source-a", "checkout-b", "runtime", "logs", "metadata", "tools", "output-parent"):
    directory = root / name
    directory.mkdir()
    path = directory / "protected.txt"
    path.write_text(name, encoding="utf-8")
    protected[name] = path
evidence = root / "runtime-evidence.json"
evidence.write_text('{{"runtime":true}}', encoding="utf-8")
evidence.chmod(0o400)
callback = root / "mount-sandbox.py"
callback.write_text(r'''#!/usr/bin/python3
import json, os, pathlib, sys
results = {{}}
pathlib.Path("/work/executed.marker").write_text("executed\n", encoding="utf-8")
evidence = pathlib.Path(os.environ["KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE"])
original = evidence.read_bytes()
mode = evidence.stat().st_mode & 0o777
try:
    os.chmod(evidence, 0o600)
    evidence.write_bytes(b"tampered")
    evidence.write_bytes(original)
    os.chmod(evidence, mode)
except OSError:
    results["evidence_restore"] = "blocked"
else:
    results["evidence_restore"] = "succeeded"
for raw in sys.argv[1:-1]:
    path = pathlib.Path(raw)
    try:
        original = path.read_bytes()
        before = path.stat()
        path.write_bytes(b"tampered")
        path.write_bytes(original)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    except OSError:
        results[path.parent.name] = "blocked"
    else:
        results[path.parent.name] = "succeeded"
verifier = pathlib.Path(sys.argv[-1])
moved = verifier.with_name("verifier-root-moved")
try:
    verifier.rename(moved)
    moved.rename(verifier)
except OSError:
    results["verifier_rename_restore"] = "blocked"
else:
    results["verifier_rename_restore"] = "succeeded"
pathlib.Path("result.json").write_text(json.dumps(results, sort_keys=True), encoding="utf-8")
''', encoding="utf-8")
callback.chmod(0o755)
wrapper = root / "mount-sandbox.sh"
wrapper.write_text(
    '#!/bin/bash\nexec /usr/bin/python3 -I -B /callback-input/mount-sandbox.py "$@"\n',
    encoding="utf-8",
)
wrapper.chmod(0o755)

parent = controller.HeldDirectory.open(root)
callback_root = controller.HeldDirectory.open(callback_path)
evidence_fd = os.open(evidence, os.O_RDONLY | os.O_CLOEXEC)
aliases = controller.FrozenToolDirectory.create(root / "aliases", {{}})
callback_shell = controller.HeldExecutable.open("shell", pathlib.Path("/bin/bash"))
try:
    controller._run_callback(
        [
            "/bin/bash",
            "/callback-input/mount-sandbox.sh",
            *(f"/callback-input/{{path.relative_to(root)}}" for path in protected.values()),
            "/callback-input/verifier-root",
        ],
        callback_shell,
        aliases,
        callback_root,
        parent,
        evidence_fd,
        "127.0.0.1",
        1,
    )
    results = json.loads((callback_path / "work" / "result.json").read_text(encoding="utf-8"))
    assert (callback_path / "work" / "executed.marker").read_text(encoding="utf-8") == "executed\n"
    expected = {{"evidence_restore", *protected, "verifier_rename_restore"}}
    if set(results) != expected or any(value != "blocked" for value in results.values()):
        raise AssertionError(f"callback escaped filesystem sandbox: {{results}}")
finally:
    aliases.remove()
    os.close(evidence_fd)
    callback_shell.close()
    callback_root.close()
    parent.close()
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_callback_mount_setup_rejects_source_and_target_rename_replace() {
    let test_dir = TestDir::new("verifier-callback-mount-setup-race");
    let body = format!(
        r#"import json
import os
import pathlib

root = pathlib.Path({root:?})
callback_source = r'''#!/usr/bin/python3
import json, os, pathlib
pathlib.Path("/work/executed.marker").write_text("executed\n", encoding="utf-8")
evidence = json.loads(pathlib.Path(os.environ["KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE"]).read_text(encoding="utf-8"))
result = {{
    "work": pathlib.Path("/work/identity").read_text(encoding="utf-8"),
    "evidence": evidence["identity"],
    "root": [os.stat("/").st_dev, os.stat("/").st_ino],
    "dev": [os.stat("/dev").st_dev, os.stat("/dev").st_ino],
}}
pathlib.Path("/work/observed.json").write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
'''

def replacement_sandbox(path):
    path.mkdir(mode=0o700)
    for relative in ("usr", "proc", "dev", "work"):
        (path / relative).mkdir(mode=0o700)
    for relative in ("runtime-evidence.json", "dev/null"):
        (path / relative).touch(mode=0o600)
    for link, target in (("bin", "usr/bin"), ("lib", "usr/lib"), ("lib64", "usr/lib64")):
        (path / link).symlink_to(target)

failures = []
executed_cases = set()
for attack in ("control", "work-source", "evidence-source", "sandbox-root-target"):
    case = root / attack
    callback_path = case / "callback"
    callback_path.mkdir(parents=True)
    callback = case / "mount-race.py"
    callback.write_text(callback_source, encoding="utf-8")
    callback.chmod(0o755)
    wrapper = case / "mount-race.sh"
    wrapper.write_text(
        '#!/bin/bash\nexec /usr/bin/python3 -I -B /callback-input/mount-race.py\n',
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    evidence = case / "runtime-evidence.json"
    evidence.write_text('{{"identity":"original-evidence"}}', encoding="utf-8")
    evidence.chmod(0o400)
    aliases = controller.FrozenToolDirectory.create(case / "aliases", {{}})
    callback_root = controller.HeldDirectory.open(callback_path)
    callback_input_root = controller.HeldDirectory.open(case)
    callback_shell = controller.HeldExecutable.open("shell", pathlib.Path("/bin/bash"))
    evidence_fd = os.open(evidence, os.O_RDONLY | os.O_CLOEXEC)
    original_mount = controller._mount
    original_setup = controller._callback_filesystem_setup

    def attack_mount(source, target, filesystem, flags, data=None):
        target_text = os.fspath(target)
        if target_text.rstrip("/").endswith("work"):
            visible = callback_path / "work"
            if not (visible / "identity").exists():
                (visible / "identity").write_text("original-work", encoding="utf-8")
            moved = callback_path / "work-held"
            if attack == "work-source" and not moved.exists():
                visible.rename(moved)
                visible.mkdir(mode=0o700)
                (visible / "home").mkdir(mode=0o700)
                (visible / "tmp").mkdir(mode=0o700)
                (visible / "identity").write_text("replacement-work", encoding="utf-8")
        elif attack == "evidence-source" and target_text.endswith("runtime-evidence.json") and source is not None:
            moved = case / "runtime-evidence-held.json"
            if not moved.exists():
                evidence.rename(moved)
                evidence.write_text('{{"identity":"replacement-evidence"}}', encoding="utf-8")
                evidence.chmod(0o400)
        return original_mount(source, target, filesystem, flags, data)

    def attack_setup(*args):
        if attack == "sandbox-root-target":
            visible = callback_path / "sandbox-root"
            moved = callback_path / "sandbox-root-held"
            visible.rename(moved)
            replacement_sandbox(visible)
            identities = {{
                "root": [visible.stat().st_dev, visible.stat().st_ino],
                "dev": [(visible / "dev").stat().st_dev, (visible / "dev").stat().st_ino],
            }}
            (case / "replacement-identities.json").write_text(
                json.dumps(identities, sort_keys=True), encoding="utf-8"
            )
        return original_setup(*args)

    controller._mount = attack_mount
    controller._callback_filesystem_setup = attack_setup
    rejected = False
    try:
        try:
            controller._run_callback(
                ["/bin/bash", "/callback-input/mount-race.sh"],
                callback_shell,
                aliases,
                callback_root,
                callback_input_root,
                evidence_fd,
                "127.0.0.1",
                1,
            )
        except controller.OracleError as error:
            if "callback script must execute" in str(error):
                failures.append(f"{{attack}} failed argv validation instead of mount behavior: {{error}}")
            print(f"{{attack}} rejected during setup: {{error}}")
            rejected = True

        if rejected:
            if attack == "control":
                failures.append("control callback did not execute inside the namespace")
            continue
        if attack == "work-source":
            observed_path = callback_path / "work-held" / "observed.json"
            if not observed_path.exists():
                failures.append("work source replacement was consumed")
                continue
        else:
            observed_path = callback_path / "work" / "observed.json"
        marker_path = observed_path.parent / "executed.marker"
        if marker_path.read_text(encoding="utf-8") != "executed\n":
            failures.append(f"{{attack}} callback execution marker is missing")
            continue
        executed_cases.add(attack)
        observed = json.loads(observed_path.read_text(encoding="utf-8"))
        if attack == "work-source" and observed["work"] != "original-work":
            failures.append(f"callback observed replacement work: {{observed}}")
        if attack == "evidence-source" and observed["evidence"] != "original-evidence":
            failures.append(f"callback observed replacement evidence: {{observed}}")
        if attack == "sandbox-root-target":
            replacement = json.loads(
                (case / "replacement-identities.json").read_text(encoding="utf-8")
            )
            if observed["root"] == replacement["root"] or observed["dev"] == replacement["dev"]:
                failures.append(
                    f"callback consumed replacement sandbox root/target: observed={{observed}} replacement={{replacement}}"
                )
    finally:
        controller._mount = original_mount
        controller._callback_filesystem_setup = original_setup
        aliases.remove()
        os.close(evidence_fd)
        callback_shell.close()
        callback_input_root.close()
        callback_root.close()

if "control" not in executed_cases:
    failures.append("control callback never executed inside the namespace")
if failures:
    raise AssertionError(f"callback mount setup trusted rename-replaced paths: {{failures}}")
"#,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[ignore = "fresh exact Redis checkout for artifact-closure mutants"]
#[cfg(target_os = "linux")]
fn oracle_verifier_artifact_closure_rejects_unlisted_and_modified_source_entries() {
    let test_dir = TestDir::new("verifier-artifact-closure");
    let source = test_dir.path().join("source-a");
    clone_exact_redis(&source);
    let body = format!(
        r#"import hashlib
import os
import pathlib

source = pathlib.Path({source:?})
binary = source / "src/redis-server"
binary.write_bytes(b"dummy redis server")
binary.chmod(0o755)
metadata = binary.stat()
document = {{
    "artifacts": [{{
        "kind": "regular",
        "path": "src/redis-server",
        "mode": metadata.st_mode,
        "size": metadata.st_size,
        "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
    }}],
    "redis_server": {{
        "artifact_path": "src/redis-server",
        "path": str(binary),
        "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "identity": controller._file_identity(metadata),
    }},
}}
tracked = source / "README.md"
tracked_bytes = tracked.read_bytes()
tracked_mode = tracked.stat().st_mode & 0o777
accepted = []
runtime_path = source.parent / "git-runtime"
runtime_path.mkdir()
runtime = controller.HeldDirectory.open(runtime_path)
held_runtime_path = pathlib.Path(f"/proc/self/fd/{{runtime.fd}}")
home = held_runtime_path / "home"
temporary = held_runtime_path / "tmp"
home.mkdir()
temporary.mkdir()

with controller.HeldDirectory.open(source) as held:
    git_dir = held.open_directory(".git")
    git = controller.HeldExecutable.open("git", pathlib.Path("/usr/bin/git"))
    env = controller._sanitized_environment("/usr/bin:/bin", home, temporary)
    tracked_tree = controller._fixed_commit_tree(held, git_dir, git, env)
    extra = source / "unlisted-regular"
    extra.write_text("rogue", encoding="utf-8")
    try:
        controller._validate_artifact_document(held, document, tracked_tree)
    except controller.OracleError:
        pass
    else:
        accepted.append("unlisted regular")
    extra.unlink()

    link = source / "unlisted-symlink"
    link.symlink_to("README.md")
    try:
        controller._validate_artifact_document(held, document, tracked_tree)
    except controller.OracleError:
        pass
    else:
        accepted.append("unlisted symlink")
    link.unlink()

    tracked.write_bytes(tracked_bytes + b"\nmutated")
    try:
        controller._validate_artifact_document(held, document, tracked_tree)
    except controller.OracleError:
        pass
    else:
        accepted.append("modified tracked source")
    tracked.write_bytes(tracked_bytes)
    tracked.chmod(tracked_mode)

    tracked.unlink()
    try:
        controller._validate_artifact_document(held, document, tracked_tree)
    except controller.OracleError:
        pass
    else:
        accepted.append("missing tracked source")
    tracked.write_bytes(tracked_bytes)
    tracked.chmod(tracked_mode)
    git.close()
    git_dir.close()
runtime.close()

if accepted:
    raise AssertionError(f"artifact closure accepted mutations: {{accepted}}")
"#,
        source = source.to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_runtime_info_rejects_an_occupied_port_with_the_wrong_process_id() {
    let test_dir = TestDir::new("verifier-runtime-process-id");
    let body = format!(
        r##"import os
import pathlib
import signal
import socket
import subprocess
import time

root = pathlib.Path({root:?})
runtime_path = root / "runtime"
logs_path = root / "logs"
runtime_path.mkdir()
logs_path.mkdir()
source = root / "sleeper.c"
binary_path = root / "held-sleeper"
source.write_text('#include <unistd.h>\nint main(void) {{ for (;;) pause(); }}\n', encoding="ascii")
compile_result = subprocess.run(
    ["/usr/bin/cc", "-O2", "-o", str(binary_path), str(source)],
    check=False,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
assert compile_result.returncode == 0, compile_result.stderr
server = root / "occupant.py"
server.write_text(r'''import os, socket, sys
port = int(sys.argv[1])
ready = sys.argv[2]
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(8)
    open(ready, "w", encoding="ascii").write(str(os.getpid()))
    while True:
        connection, _ = listener.accept()
        with connection:
            connection.recv(4096)
            payload = f"# Server\r\nredis_version:8.8.1\r\nprocess_id:{{os.getpid()}}\r\n".encode("ascii")
            connection.sendall(b"$" + str(len(payload)).encode("ascii") + b"\r\n" + payload + b"\r\n")
''', encoding="utf-8")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
    reservation.bind(("127.0.0.1", 0))
    occupied_port = reservation.getsockname()[1]
ready = root / "occupant.ready"
occupant = subprocess.Popen(["/usr/bin/python3", "-I", "-B", str(server), str(occupied_port), str(ready)])
deadline = time.monotonic() + 2
while not ready.exists() and time.monotonic() < deadline:
    time.sleep(0.01)
assert ready.exists(), "occupant did not bind"

real_socket = controller.socket.socket
reservation_supplied = False
class FixedReservation:
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def bind(self, _address): return None
    def getsockname(self): return ("127.0.0.1", occupied_port)
    def close(self): return None
def socket_dispatch(*args, **kwargs):
    global reservation_supplied
    if not reservation_supplied:
        reservation_supplied = True
        return FixedReservation()
    return real_socket(*args, **kwargs)

runtime = controller.HeldDirectory.open(runtime_path)
logs = controller.HeldDirectory.open(logs_path)
held = controller.HeldExecutable.open("occupied-port-sleeper", binary_path)
spawned = None
accepted = False
controller.socket.socket = socket_dispatch
try:
    try:
        spawned, _port, _document, log_fd = controller._start_redis_runtime(held, runtime, logs)
    except controller.OracleError:
        pass
    else:
        accepted = True
finally:
    controller.socket.socket = real_socket
    if spawned is not None:
        controller._cleanup_redis_runtime(spawned, log_fd)
    held.close()
    logs.close()
    runtime.close()
    occupant.terminate()
    try:
        occupant.wait(timeout=2)
    except subprocess.TimeoutExpired:
        occupant.kill()
        occupant.wait()

if accepted:
    raise AssertionError("Redis startup accepted INFO from a different process occupying the selected port")
"##,
        root = test_dir.path().to_string_lossy(),
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_redis_log_close_failure_reaps_the_spawned_process() {
    let test_dir = TestDir::new("verifier-redis-log-close");
    let body = format!(
        r#"import os
import pathlib
import signal
import subprocess
import time

root = pathlib.Path({root:?})
runtime_path = root / "runtime"
logs_path = root / "logs"
runtime_path.mkdir()
logs_path.mkdir()
source = root / "sleeper.c"
binary_path = root / "held-sleeper"
source.write_text('#include <unistd.h>\nint main(void) {{ for (;;) pause(); }}\n', encoding="ascii")
compile_result = subprocess.run(
    ["/usr/bin/cc", "-O2", "-o", str(binary_path), str(source)],
    check=False,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
assert compile_result.returncode == 0, compile_result.stderr
binary_stat = binary_path.stat()
runtime = controller.HeldDirectory.open(runtime_path)
logs = controller.HeldDirectory.open(logs_path)
held = controller.HeldExecutable.open("log-close-sleeper", binary_path)
log_fd = os.open(
    "redis.log",
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
    0o600,
    dir_fd=logs.fd,
)
process = subprocess.Popen(
    [str(binary_path)],
    executable=f"/proc/self/fd/{{held.fd}}",
    stdin=subprocess.DEVNULL,
    stdout=log_fd,
    stderr=log_fd,
    close_fds=True,
    pass_fds=(held.fd,),
    start_new_session=True,
)
original_close = os.close
injected = False

def fail_log_close(fd):
    global injected
    try:
        target = os.readlink(f"/proc/self/fd/{{fd}}")
    except OSError:
        target = ""
    if not injected and target.endswith("/logs/redis.log"):
        injected = True
        original_close(fd)
        raise OSError("injected Redis log close failure")
    return original_close(fd)

controller.os.close = fail_log_close
try:
    try:
        controller._cleanup_redis_runtime(process, log_fd)
    except controller.OracleError as error:
        assert "Redis log close failure" in str(error)
    else:
        raise AssertionError("Redis log close fault was ignored")
finally:
    controller.os.close = original_close

survivors = []
for entry in pathlib.Path("/proc").iterdir():
    if not entry.name.isdigit():
        continue
    try:
        metadata = (entry / "exe").stat()
    except OSError:
        continue
    if metadata.st_dev == binary_stat.st_dev and metadata.st_ino == binary_stat.st_ino:
        survivors.append(int(entry.name))
for pid in survivors:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:
        pass
held.close()
logs.close()
runtime.close()
assert not (root / "oracle-provenance.json").exists()
assert not list(root.glob(".*.provenance-*"))
if survivors:
    raise AssertionError(f"Redis process survived log close failure: {{survivors}}")
"#,
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
fn oracle_verifier_borrowed_provenance_target_success_keeps_caller_fd_ownership() {
    let test_dir = TestDir::new("verifier-borrowed-publish-success");
    let provenance = test_dir.path().join("oracle-provenance.json");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import os
import pathlib

provenance = pathlib.Path({provenance:?})
document = json.loads(r'''{document}''')
target = controller.CandidateTarget.open(provenance)
before = len(os.listdir("/proc/self/fd"))
controller.publish_provenance(target, document)
after = len(os.listdir("/proc/self/fd"))
assert after == before, (before, after)
assert target.parent.fd >= 0
os.fstat(target.parent.fd)
target.verify_visible_parent()
assert provenance.exists()
target.close()
"#,
        provenance = provenance.to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_borrowed_provenance_target_failure_rolls_back_without_fd_leak() {
    let test_dir = TestDir::new("verifier-borrowed-publish-failure");
    let provenance = test_dir.path().join("oracle-provenance.json");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import os
import pathlib
import stat

provenance = pathlib.Path({provenance:?})
document = json.loads(r'''{document}''')
target = controller.CandidateTarget.open(provenance)
before = len(os.listdir("/proc/self/fd"))
original_fsync = os.fsync
parent_fsyncs = 0

def fail_first_parent_fsync(fd):
    global parent_fsyncs
    if stat.S_ISDIR(os.fstat(fd).st_mode):
        parent_fsyncs += 1
        if parent_fsyncs == 1:
            raise OSError("injected post-rename parent fsync failure")
    return original_fsync(fd)

os.fsync = fail_first_parent_fsync
try:
    controller.publish_provenance(target, document)
except OSError as error:
    assert "post-rename parent fsync failure" in str(error)
else:
    raise AssertionError("borrowed-target publication failure was accepted")
finally:
    os.fsync = original_fsync

after = len(os.listdir("/proc/self/fd"))
assert after == before, (before, after)
assert target.parent.fd >= 0
os.fstat(target.parent.fd)
target.verify_visible_parent()
assert not provenance.exists()
assert not list(provenance.parent.glob(".*.provenance-*"))
target.close()
"#,
        provenance = provenance.to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
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
#[cfg(target_os = "linux")]
fn oracle_verifier_provenance_parent_close_failure_rolls_back_final_and_temp() {
    let test_dir = TestDir::new("verifier-publish-parent-close");
    let provenance = test_dir.path().join("oracle-provenance.json");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import pathlib

provenance = pathlib.Path({provenance:?})
document = json.loads(r'''{document}''')
target = controller.CandidateTarget.open(provenance)
original_close = controller.CandidateTarget.close
failed = False

def fail_output_parent_close(self):
    global failed
    original_close(self)
    if not failed:
        failed = True
        raise OSError("injected output-parent close failure")

controller.CandidateTarget.close = fail_output_parent_close
try:
    controller.publish_provenance(target, document, close_target=True)
except OSError as error:
    assert "output-parent close failure" in str(error)
else:
    raise AssertionError("output-parent close failure was ignored")
finally:
    controller.CandidateTarget.close = original_close

assert not provenance.exists(), "final provenance survived output-parent close failure"
assert not list(provenance.parent.glob(".*.provenance-*")), "temporary provenance survived"
"#,
        provenance = provenance.to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_provenance_late_fd_close_failures_roll_back_final() {
    let test_dir = TestDir::new("verifier-publish-late-fd-close");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import os
import pathlib
import stat

root = pathlib.Path({root:?})
document = json.loads(r'''{document}''')
surviving_finals = []

for failure_kind in ("published-fd", "rollback-parent-fd"):
    case = root / failure_kind
    case.mkdir()
    provenance = case / "oracle-provenance.json"
    target = controller.CandidateTarget.open(provenance)
    original_open = controller.os.open
    original_close = controller.os.close
    original_dup = controller.os.dup
    rollback_parent_fd = -1
    published_fd = -1
    failed_fd = -1
    injected = False

    def track_rollback_parent(fd):
        global rollback_parent_fd
        duplicate = original_dup(fd)
        rollback_parent_fd = duplicate
        return duplicate

    def track_published_open(path, flags, mode=0o777, *, dir_fd=None):
        global published_fd
        fd = original_open(path, flags, mode, dir_fd=dir_fd)
        if path == provenance.name and dir_fd == target.parent.fd and published_fd < 0:
            published_fd = fd
        return fd

    def fail_late_close(fd):
        global failed_fd, injected
        fail_published = failure_kind == "published-fd" and fd == published_fd
        fail_rollback_parent = (
            failure_kind == "rollback-parent-fd" and fd == rollback_parent_fd
        )
        if not injected and (fail_published or fail_rollback_parent):
            injected = True
            failed_fd = fd
            raise OSError(5, f"injected legacy {{failure_kind}} close failure")
        return original_close(fd)

    controller.os.dup = track_rollback_parent
    controller.os.open = track_published_open
    controller.os.close = fail_late_close
    try:
        try:
            controller.publish_provenance(target, document, close_target=True)
        except (controller.OracleError, OSError):
            pass
        else:
            raise AssertionError(f"legacy {{failure_kind}} close failure was accepted")
    finally:
        controller.os.dup = original_dup
        controller.os.open = original_open
        controller.os.close = original_close
        for fd in {{failed_fd, published_fd, rollback_parent_fd}}:
            if fd >= 0:
                try:
                    original_close(fd)
                except OSError:
                    pass
        target.close()
    assert injected, f"legacy {{failure_kind}} close fault was not reached"
    assert not list(case.glob(".*.provenance-*")), failure_kind
    if provenance.exists():
        surviving_finals.append(failure_kind)
        provenance.unlink()
assert not surviving_finals, surviving_finals
"#,
        root = test_dir.path().to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_provenance_parent_move_during_successful_close_rolls_back() {
    let test_dir = TestDir::new("verifier-publish-parent-close-move");
    let parent = test_dir.path().join("output-parent");
    let moved = test_dir.path().join("held-output-parent");
    fs::create_dir(&parent).unwrap();
    let provenance = parent.join("oracle-provenance.json");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import pathlib

parent = pathlib.Path({parent:?})
moved = pathlib.Path({moved:?})
provenance = pathlib.Path({provenance:?})
document = json.loads(r'''{document}''')
target = controller.CandidateTarget.open(provenance)
original_close = controller.CandidateTarget.close

def move_parent_during_successful_close(self):
    original_close(self)
    parent.rename(moved)
    parent.mkdir()

controller.CandidateTarget.close = move_parent_during_successful_close
try:
    controller.publish_provenance(target, document, close_target=True)
except controller.OracleError:
    pass
else:
    raise AssertionError("output parent move during successful close was accepted")
finally:
    controller.CandidateTarget.close = original_close

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
#[cfg(target_os = "linux")]
fn oracle_verifier_provenance_rollback_preserves_replacement_final() {
    let test_dir = TestDir::new("verifier-publish-rollback-replacement");
    let parent = test_dir.path().join("output-parent");
    let moved = test_dir.path().join("held-output-parent");
    fs::create_dir(&parent).unwrap();
    let provenance = parent.join("oracle-provenance.json");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import pathlib

parent = pathlib.Path({parent:?})
moved = pathlib.Path({moved:?})
provenance = pathlib.Path({provenance:?})
document = json.loads(r'''{document}''')
target = controller.CandidateTarget.open(provenance)
original_close = controller.CandidateTarget.close
replacement = b"replacement-final-must-survive\n"

def replace_final_during_close(self):
    original_close(self)
    parent.rename(moved)
    (moved / provenance.name).rename(moved / "published-original.json")
    (moved / provenance.name).write_bytes(replacement)
    parent.mkdir()

controller.CandidateTarget.close = replace_final_during_close
try:
    controller.publish_provenance(target, document, close_target=True)
except controller.OracleError:
    pass
else:
    raise AssertionError("output parent replacement during close was accepted")
finally:
    controller.CandidateTarget.close = original_close

assert (moved / provenance.name).read_bytes() == replacement
assert not list(parent.glob(".*.provenance-*"))
assert not list(moved.glob(".*.provenance-*"))
"#,
        parent = parent.to_string_lossy(),
        moved = moved.to_string_lossy(),
        provenance = provenance.to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_python_publication_rejects_cross_stage_timestamp_mutations() {
    let test_dir = TestDir::new("verifier-python-timestamp-order");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import copy
import json
import pathlib

root = pathlib.Path({root:?})
canonical = json.loads(r'''{document}''')
mutations = {{
    "future-primary": (("primary", "started_at_utc", "2026-08-11T00:00:11Z"), ("primary", "finished_at_utc", "2026-08-11T00:00:12Z")),
    "swapped-build-order": (("rebuild", "started_at_utc", "2026-08-11T00:00:04Z"),),
    "callback-before-rebuild": (("rebuild", "finished_at_utc", "2026-08-11T00:00:08Z"),),
}}
for name, changes in mutations.items():
    document = copy.deepcopy(canonical)
    for section, field, value in changes:
        document[section][field] = value
    output = root / f"{{name}}.json"
    try:
        controller.publish_provenance(output, document)
    except controller.OracleError:
        pass
    else:
        raise AssertionError(f"cross-stage timestamp mutation was published: {{name}}")
    assert not output.exists()
    assert not list(root.glob(f".{{output.name}}.provenance-*"))
"#,
        root = test_dir.path().to_string_lossy(),
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
    assert!(powershell.contains("--callback-input"));
    assert!(powershell.contains("--publication-verifier"));
    assert!(powershell.contains("--run-after-ready"));
    assert!(!powershell.contains("Invoke-Expression"));
    assert!(!powershell.contains("Start-Process"));
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_powershell_wrapper_preserves_literal_argv_and_converts_drive_paths() {
    let powershell = Path::new("/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe");
    if !powershell.is_file() {
        eprintln!("Windows PowerShell interop is unavailable; skipping argv semantics probe");
        return;
    }
    let test_dir = TestDir::new("verifier-powershell-argv");
    let harness = test_dir.path().join("harness.ps1");
    let marker = test_dir.path().join("argv.json");
    fs::write(
        &harness,
        r#"param(
    [Parameter(Mandatory = $true)][string]$Wrapper,
    [Parameter(Mandatory = $true)][string]$Marker
)
function global:wsl.exe {
    $actual = @($args)
    if ($actual.Count -ge 3 -and $actual[0] -eq '--exec' -and $actual[1] -eq '/usr/bin/wslpath') {
        $global:LASTEXITCODE = 0
        return "WSL<$($actual[-1])>"
    }
    [Console]::Out.WriteLine('FINALARGV:' + ($actual | ConvertTo-Json -Compress))
    $global:LASTEXITCODE = 0
}

& $Wrapper `
    -Source 'D:\source path' `
    -PrimaryMetadata 'D:\primary path\primary.json' `
    -Output 'E:\out\oracle.json' `
    -EvidenceOutput 'E:\out\evidence.json' `
    -ExpectedHead 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' `
    -PublicationVerifier 'G:\tools\kiwi-verify-oracle-evidence.exe' `
    -CallbackInput 'F:\callback input' `
    -RunAfterReady @('C:\callback dir\callback.exe', 'literal ; $() []')
"#,
    )
    .unwrap();

    let windows_path = |path: &Path| -> String {
        let output = Command::new("/usr/bin/wslpath")
            .args(["-w", "--"])
            .arg(path)
            .output()
            .expect("wslpath must start");
        assert!(output.status.success(), "wslpath failed: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    let output = Command::new(powershell)
        .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(windows_path(&harness))
        .arg("-Wrapper")
        .arg(windows_path(
            &repository_root().join("scripts/compat/verify-redis-8.8.1.ps1"),
        ))
        .arg("-Marker")
        .arg(windows_path(&marker))
        .output()
        .expect("PowerShell argv harness must start");
    assert!(
        output.status.success(),
        "PowerShell argv harness failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let marker = stdout
        .lines()
        .find_map(|line| line.strip_prefix("FINALARGV:"))
        .unwrap_or_else(|| {
            panic!(
                "argv marker must exist in PowerShell stdout; stdout={stdout:?} stderr={:?}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
    let actual: Vec<String> = serde_json::from_str(marker).unwrap();
    let expected = vec![
        "--exec".to_string(),
        "/usr/bin/bash".to_string(),
        format!(
            "WSL<{}>",
            windows_path(&repository_root().join("scripts/compat/verify-redis-8.8.1.sh"))
        ),
        "--source".to_string(),
        "WSL<D:\\source path>".to_string(),
        "--primary-metadata".to_string(),
        "WSL<D:\\primary path\\primary.json>".to_string(),
        "--output".to_string(),
        "WSL<E:\\out\\oracle.json>".to_string(),
        "--evidence-output".to_string(),
        "WSL<E:\\out\\evidence.json>".to_string(),
        "--expected-head".to_string(),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        "--publication-verifier".to_string(),
        "WSL<G:\\tools\\kiwi-verify-oracle-evidence.exe>".to_string(),
        "--callback-input".to_string(),
        "WSL<F:\\callback input>".to_string(),
        "--run-after-ready".to_string(),
        "WSL<C:\\callback dir\\callback.exe>".to_string(),
        "literal ; $() []".to_string(),
    ];
    assert_eq!(
        actual, expected,
        "PowerShell wrapper changed argv semantics"
    );
}

#[test]
#[cfg(target_os = "linux")]
fn oracle_verifier_powershell_wrapper_rejects_unc_before_wslpath() {
    let powershell = Path::new("/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe");
    if !powershell.is_file() {
        eprintln!("Windows PowerShell interop is unavailable; skipping UNC boundary probe");
        return;
    }
    let test_dir = TestDir::new("verifier-powershell-unc");
    let harness = test_dir.path().join("harness.ps1");
    fs::write(
        &harness,
        r#"param([Parameter(Mandatory = $true)][string]$Wrapper)
$global:WslCalls = 0
function global:wsl.exe {
    $global:WslCalls++
    $global:LASTEXITCODE = 0
    return '/unexpected'
}
try {
    & $Wrapper -Source '\\server\share\source' -PrimaryMetadata 'D:\primary.json' -Output 'E:\out.json' -EvidenceOutput 'E:\evidence.json' -ExpectedHead 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' -PublicationVerifier 'G:\tools\kiwi-verify-oracle-evidence.exe' -CallbackInput 'F:\input' -RunAfterReady @('C:\callback.exe')
} catch {
    [Console]::Out.WriteLine('ERROR:' + $_.Exception.Message)
}
[Console]::Out.WriteLine('WSLCALLS:' + $global:WslCalls)
"#,
    )
    .unwrap();
    let windows_path = |path: &Path| -> String {
        let output = Command::new("/usr/bin/wslpath")
            .args(["-w", "--"])
            .arg(path)
            .output()
            .expect("wslpath must start");
        assert!(output.status.success(), "wslpath failed: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    let output = Command::new(powershell)
        .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(windows_path(&harness))
        .arg("-Wrapper")
        .arg(windows_path(
            &repository_root().join("scripts/compat/verify-redis-8.8.1.ps1"),
        ))
        .output()
        .expect("PowerShell UNC harness must start");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("UNC paths are not supported"),
        "stdout={stdout:?}"
    );
    assert!(stdout.contains("WSLCALLS:0"), "stdout={stdout:?}");
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
    let callback_input = test_dir.path().join("callback-input");
    fs::create_dir(&callback_input).unwrap();
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

required = [
    "KIWI_REDIS_ORACLE_HOST",
    "KIWI_REDIS_ORACLE_PORT",
    "KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE",
    "KIWI_REDIS_ORACLE_CALLBACK_INPUT",
    "KIWI_REDIS_ORACLE_WORKDIR",
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
workdir = pathlib.Path(os.environ["KIWI_REDIS_ORACLE_WORKDIR"])
marker = workdir / "callback-ok.json"
marker.write_text(json.dumps(evidence, sort_keys=True), encoding="utf-8")
if json.loads(marker.read_text(encoding="utf-8"))["pid"] != evidence["pid"]:
    raise SystemExit("callback work-area marker was not durable during the callback")
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
        .arg("--callback-input")
        .arg(&callback_input)
        .arg("--run-after-ready")
        .arg(&callback)
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
fn build_evidence_errors_do_not_claim_provenance_v4() {
    for error in [
        BuildEvidence::from_json("{")
            .expect_err("malformed build evidence must fail")
            .to_string(),
        BuildEvidence::from_json("{}")
            .expect_err("incomplete build evidence must fail")
            .to_string(),
    ] {
        let normalized = error.to_ascii_lowercase();
        assert!(
            normalized.contains("oracle evidence"),
            "error must identify the shared Oracle evidence boundary: {error}"
        );
        assert!(
            !normalized.contains("provenance") && !normalized.contains("v4"),
            "build-v3 errors must not claim provenance-v4 validation: {error}"
        );
    }
}

#[test]
fn provenance_external_bindings_require_current_head_tree_and_evidence() {
    let evidence = b"{\"schema_version\":\"kiwi-vector-differential-evidence/v1\"}\n".to_vec();
    let mut fixture = canonical_provenance();
    fixture["differential_evidence"]["size_bytes"] = json!(evidence.len());
    fixture["differential_evidence"]["sha256"] = json!(format!("{:x}", Sha256::digest(&evidence)));
    let provenance = parse_provenance(fixture);
    let expected_head = "2222222222222222222222222222222222222222";
    let expected_tree = "3333333333333333333333333333333333333333";
    let evidence_file_name = "vector-differential-evidence.json";
    let replacement_evidence = vec![b'x'; evidence.len()];

    provenance
        .verify_external_bindings(expected_head, expected_tree, evidence_file_name, &evidence)
        .expect("canonical provenance must bind to the external CI inputs");

    for result in [
        provenance.verify_external_bindings(
            "4444444444444444444444444444444444444444",
            expected_tree,
            evidence_file_name,
            &evidence,
        ),
        provenance.verify_external_bindings(
            expected_head,
            "5555555555555555555555555555555555555555",
            evidence_file_name,
            &evidence,
        ),
        provenance.verify_external_bindings(
            expected_head,
            expected_tree,
            "replacement.json",
            &evidence,
        ),
        provenance.verify_external_bindings(
            expected_head,
            expected_tree,
            evidence_file_name,
            &evidence[..evidence.len() - 1],
        ),
        provenance.verify_external_bindings(
            expected_head,
            expected_tree,
            evidence_file_name,
            &replacement_evidence,
        ),
    ] {
        assert!(result.is_err(), "external binding drift must fail closed");
    }
}

#[test]
#[cfg(target_os = "linux")]
fn strict_oracle_evidence_cli_parses_and_binds_the_published_files() {
    let evidence = b"{\"schema_version\":\"kiwi-vector-differential-evidence/v1\"}\n".to_vec();
    let mut fixture = canonical_provenance();
    fixture["differential_evidence"]["size_bytes"] = json!(evidence.len());
    fixture["differential_evidence"]["sha256"] = json!(format!("{:x}", Sha256::digest(&evidence)));
    let provenance = sealed_test_file("oracle-provenance", &serde_json::to_vec(&fixture).unwrap());
    let evidence = sealed_test_file("vector-differential-evidence", &evidence);

    let invoke = |head: &str| {
        strict_verifier_command(
            &provenance,
            &evidence,
            "vector-differential-evidence.json",
            head,
        )
        .output()
        .expect("strict Oracle evidence verifier must start")
    };
    let accepted = invoke("2222222222222222222222222222222222222222");
    assert!(
        accepted.status.success(),
        "canonical published files must pass\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&accepted.stdout),
        String::from_utf8_lossy(&accepted.stderr)
    );

    let rejected = invoke("4444444444444444444444444444444444444444");
    assert!(
        !rejected.status.success(),
        "external Head drift must fail closed"
    );
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("callback_input.expected_head"),
        "failure must identify the mismatched external binding: {}",
        String::from_utf8_lossy(&rejected.stderr)
    );
}

#[test]
#[cfg(target_os = "linux")]
fn strict_oracle_evidence_cli_rejects_a_fifo_without_blocking() {
    let test_dir = TestDir::new("strict-evidence-fifo");
    let evidence_path = test_dir.path().join("vector-differential-evidence.json");
    let provenance = sealed_test_file(
        "oracle-provenance",
        canonical_provenance().to_string().as_bytes(),
    );
    let mkfifo = Command::new("/usr/bin/mkfifo")
        .arg(&evidence_path)
        .status()
        .expect("mkfifo must start");
    assert!(mkfifo.success(), "mkfifo must create the evidence mutant");
    let evidence = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&evidence_path)
        .expect("open FIFO without blocking");

    let mut child = strict_verifier_command(
        &provenance,
        &evidence,
        "vector-differential-evidence.json",
        "2222222222222222222222222222222222222222",
    )
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .spawn()
    .expect("strict Oracle evidence verifier must start");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = child.try_wait().expect("poll FIFO mutant") {
            assert!(!status.success(), "FIFO evidence must fail closed");
            break;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill blocked FIFO mutant");
            child.wait().expect("reap blocked FIFO mutant");
            panic!("FIFO evidence blocked the required publication gate");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[cfg(target_os = "linux")]
fn publication_binding_failure_rolls_back_both_final_files() {
    let test_dir = TestDir::new("publication-binding-rollback");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import json
import pathlib
import types

root = pathlib.Path({root:?})
evidence_path = root / "vector-differential-evidence.json"
provenance_path = root / "oracle-provenance.json"
evidence_target = controller.CandidateTarget.open(evidence_path)
provenance_target = controller.CandidateTarget.open(provenance_path)
publication_verifier = controller.HeldExecutable.open(
    "publication-verifier", pathlib.Path("/bin/true")
)
observed_published = False
original_run_bounded = controller.run_bounded

def reject_publication(*args, **kwargs):
    global observed_published
    assert evidence_path.is_file()
    assert provenance_path.is_file()
    observed_published = True
    return types.SimpleNamespace(
        timed_out=False,
        output_truncated=False,
        exit_code=1,
        stderr=b"injected publication binding failure",
        process_group_reaped=True,
    )

controller.run_bounded = reject_publication
try:
    try:
        controller.publish_evidence_then_provenance(
            evidence_target,
            provenance_target,
            {{"schema_version": "kiwi-vector-differential-evidence/v1"}},
            json.loads(r'''{document}'''),
            post_publish_verifier=publication_verifier,
            expected_head="2222222222222222222222222222222222222222",
            expected_tree="3333333333333333333333333333333333333333",
            close_post_publish_verifier=True,
        )
    except controller.OracleError:
        pass
    else:
        raise AssertionError("publication binding failure was accepted")
finally:
    controller.run_bounded = original_run_bounded
    publication_verifier.close()
    evidence_target.close()
    provenance_target.close()

assert observed_published
assert not evidence_path.exists()
assert not provenance_path.exists()
"#,
        root = test_dir.path().to_string_lossy(),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn publication_binding_success_commits_both_final_files() {
    let test_dir = TestDir::new("publication-binding-success");
    let evidence_payload =
        b"{\"schema_version\":\"kiwi-vector-differential-evidence/v1\",\"value\":1}\n";
    let mut fixture = canonical_provenance();
    fixture["differential_evidence"]["size_bytes"] = json!(evidence_payload.len());
    fixture["differential_evidence"]["sha256"] =
        json!(format!("{:x}", Sha256::digest(evidence_payload)));
    let body = format!(
        r#"import json
import pathlib

root = pathlib.Path({root:?})
evidence_path = root / "vector-differential-evidence.json"
provenance_path = root / "oracle-provenance.json"
evidence_target = controller.CandidateTarget.open(evidence_path)
provenance_target = controller.CandidateTarget.open(provenance_path)
publication_verifier = controller.HeldExecutable.open(
    "publication-verifier", pathlib.Path({verifier:?})
)
try:
    controller.publish_evidence_then_provenance(
        evidence_target,
        provenance_target,
        {{
            "schema_version": "kiwi-vector-differential-evidence/v1",
            "value": 1,
        }},
        json.loads(r'''{document}'''),
        post_publish_verifier=publication_verifier,
        expected_head="2222222222222222222222222222222222222222",
        expected_tree="3333333333333333333333333333333333333333",
        close_post_publish_verifier=True,
    )
finally:
    publication_verifier.close()
    evidence_target.close()
    provenance_target.close()

assert evidence_path.is_file()
assert provenance_path.is_file()
"#,
        root = test_dir.path().to_string_lossy(),
        verifier = env!("CARGO_BIN_EXE_kiwi-verify-oracle-evidence"),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn publication_binding_reads_held_outputs_when_visible_paths_are_swapped_and_restored() {
    let test_dir = TestDir::new("publication-binding-held-output-fds");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import copy
import hashlib
import json
import pathlib

root = pathlib.Path({root:?})
evidence_path = root / "vector-differential-evidence.json"
provenance_path = root / "oracle-provenance.json"
evidence_document = {{
    "schema_version": "kiwi-vector-differential-evidence/v1",
    "value": 1,
}}
evidence_payload = controller.canonical_json_bytes(evidence_document)
valid_provenance = json.loads(r'''{document}''')
valid_provenance["differential_evidence"] = {{
    "schema_version": "kiwi-vector-differential-evidence/v1",
    "file_name": evidence_path.name,
    "size_bytes": len(evidence_payload),
    "sha256": hashlib.sha256(evidence_payload).hexdigest(),
    "published_atomically": True,
    "verified_after_publish": True,
}}
invalid_provenance = copy.deepcopy(valid_provenance)
invalid_provenance["callback_input"]["expected_head"] = "4" * 40

evidence_target = controller.CandidateTarget.open(evidence_path)
provenance_target = controller.CandidateTarget.open(provenance_path)
publication_verifier = controller.HeldExecutable.open(
    "publication-verifier", pathlib.Path({verifier:?})
)
original_run_bounded = controller.run_bounded
attack_observed = False

def swap_visible_outputs(executable, argv, **kwargs):
    global attack_observed
    evidence_backup = root / "held-evidence.json"
    provenance_backup = root / "held-provenance.json"
    evidence_path.rename(evidence_backup)
    provenance_path.rename(provenance_backup)
    evidence_path.write_bytes(evidence_payload)
    provenance_path.write_bytes(controller.canonical_json_bytes(valid_provenance))
    attack_observed = True
    try:
        return original_run_bounded(executable, argv, **kwargs)
    finally:
        evidence_path.unlink()
        provenance_path.unlink()
        evidence_backup.rename(evidence_path)
        provenance_backup.rename(provenance_path)

controller.run_bounded = swap_visible_outputs
rejected = False
try:
    try:
        controller.publish_evidence_then_provenance(
            evidence_target,
            provenance_target,
            evidence_document,
            invalid_provenance,
            post_publish_verifier=publication_verifier,
            expected_head="2" * 40,
            expected_tree="3" * 40,
            close_post_publish_verifier=True,
        )
    except controller.OracleError:
        rejected = True
finally:
    controller.run_bounded = original_run_bounded
    publication_verifier.close()
    evidence_target.close()
    provenance_target.close()

assert attack_observed
assert rejected, "visible replacement files bypassed the held-output publication gate"
assert not evidence_path.exists()
assert not provenance_path.exists()
"#,
        root = test_dir.path().to_string_lossy(),
        verifier = env!("CARGO_BIN_EXE_kiwi-verify-oracle-evidence"),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn publication_binding_reads_sealed_output_snapshots_during_inode_rewrite_and_restore() {
    let test_dir = TestDir::new("publication-binding-sealed-output-fds");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import copy
import hashlib
import json
import pathlib

root = pathlib.Path({root:?})
evidence_path = root / "vector-differential-evidence.json"
provenance_path = root / "oracle-provenance.json"
evidence_document = {{"schema_version": "kiwi-vector-differential-evidence/v1"}}
evidence_payload = controller.canonical_json_bytes(evidence_document)
valid_provenance = json.loads(r'''{document}''')
valid_provenance["differential_evidence"] = {{
    "schema_version": "kiwi-vector-differential-evidence/v1",
    "file_name": evidence_path.name,
    "size_bytes": len(evidence_payload),
    "sha256": hashlib.sha256(evidence_payload).hexdigest(),
    "published_atomically": True,
    "verified_after_publish": True,
}}
invalid_provenance = copy.deepcopy(valid_provenance)
invalid_provenance["callback_input"]["expected_head"] = "4" * 40
valid_payload = controller.canonical_json_bytes(valid_provenance)

evidence_target = controller.CandidateTarget.open(evidence_path)
provenance_target = controller.CandidateTarget.open(provenance_path)
publication_verifier = controller.HeldExecutable.open(
    "publication-verifier", pathlib.Path({verifier:?})
)
original_run_bounded = controller.run_bounded
attack_observed = False

def rewrite_published_inode(executable, argv, **kwargs):
    global attack_observed
    original_payload = provenance_path.read_bytes()
    original_inode = provenance_path.stat().st_ino
    assert len(valid_payload) == len(original_payload)
    provenance_path.write_bytes(valid_payload)
    assert provenance_path.stat().st_ino == original_inode
    attack_observed = True
    try:
        return original_run_bounded(executable, argv, **kwargs)
    finally:
        provenance_path.write_bytes(original_payload)
        assert provenance_path.stat().st_ino == original_inode

controller.run_bounded = rewrite_published_inode
rejected = False
try:
    try:
        controller.publish_evidence_then_provenance(
            evidence_target,
            provenance_target,
            evidence_document,
            invalid_provenance,
            post_publish_verifier=publication_verifier,
            expected_head="2" * 40,
            expected_tree="3" * 40,
            close_post_publish_verifier=True,
        )
    except controller.OracleError:
        rejected = True
finally:
    controller.run_bounded = original_run_bounded
    publication_verifier.close()
    evidence_target.close()
    provenance_target.close()

assert attack_observed
assert rejected, "same-inode output rewrite/restore bypassed the publication gate"
assert not evidence_path.exists()
assert not provenance_path.exists()
"#,
        root = test_dir.path().to_string_lossy(),
        verifier = env!("CARGO_BIN_EXE_kiwi-verify-oracle-evidence"),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
}

#[test]
#[cfg(target_os = "linux")]
fn publication_binding_executes_a_sealed_verifier_snapshot() {
    let test_dir = TestDir::new("publication-binding-sealed-verifier");
    let fixture = canonical_provenance();
    let body = format!(
        r#"import copy
import json
import pathlib
import shutil

root = pathlib.Path({root:?})
evidence_path = root / "vector-differential-evidence.json"
provenance_path = root / "oracle-provenance.json"
verifier_path = root / "kiwi-verify-oracle-evidence"
shutil.copyfile(pathlib.Path({verifier:?}), verifier_path)
verifier_path.chmod(0o700)
original_verifier = verifier_path.read_bytes()
original_mode = verifier_path.stat().st_mode & 0o777
true_program = pathlib.Path("/bin/true").read_bytes()
assert len(true_program) <= len(original_verifier)
replacement_verifier = true_program + b"\0" * (len(original_verifier) - len(true_program))

valid_provenance = json.loads(r'''{document}''')
invalid_provenance = copy.deepcopy(valid_provenance)
invalid_provenance["callback_input"]["expected_head"] = "4" * 40
evidence_document = {{"schema_version": "kiwi-vector-differential-evidence/v1"}}
evidence_target = controller.CandidateTarget.open(evidence_path)
provenance_target = controller.CandidateTarget.open(provenance_path)
publication_verifier = controller.HeldExecutable.open(
    "publication-verifier", verifier_path
)
original_run_bounded = controller.run_bounded
attack_observed = False

def rewrite_verifier_inode(executable, argv, **kwargs):
    global attack_observed
    verifier_path.write_bytes(replacement_verifier)
    verifier_path.chmod(original_mode)
    attack_observed = True
    try:
        return original_run_bounded(executable, argv, **kwargs)
    finally:
        verifier_path.write_bytes(original_verifier)
        verifier_path.chmod(original_mode)

controller.run_bounded = rewrite_verifier_inode
rejected = False
try:
    try:
        controller.publish_evidence_then_provenance(
            evidence_target,
            provenance_target,
            evidence_document,
            invalid_provenance,
            post_publish_verifier=publication_verifier,
            expected_head="2" * 40,
            expected_tree="3" * 40,
            close_post_publish_verifier=True,
        )
    except controller.OracleError:
        rejected = True
finally:
    controller.run_bounded = original_run_bounded
    publication_verifier.close()
    evidence_target.close()
    provenance_target.close()

assert attack_observed
assert rejected, "same-inode rewrite/execute/restore bypassed the publication gate"
assert not evidence_path.exists()
assert not provenance_path.exists()
"#,
        root = test_dir.path().to_string_lossy(),
        verifier = env!("CARGO_BIN_EXE_kiwi-verify-oracle-evidence"),
        document = fixture,
    );
    assert_probe_succeeds(run_python_probe(&test_dir, &body));
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
fn rejects_cross_stage_timestamp_reordering() {
    let mut future_primary = canonical_provenance();
    future_primary["primary"]["started_at_utc"] = json!("2026-08-11T00:00:11Z");
    future_primary["primary"]["finished_at_utc"] = json!("2026-08-11T00:00:12Z");
    assert_provenance_rejected(future_primary);

    let mut swapped_build_order = canonical_provenance();
    swapped_build_order["rebuild"]["started_at_utc"] = json!("2026-08-11T00:00:04Z");
    assert_provenance_rejected(swapped_build_order);

    let mut callback_before_rebuild = canonical_provenance();
    callback_before_rebuild["rebuild"]["finished_at_utc"] = json!("2026-08-11T00:00:08Z");
    assert_provenance_rejected(callback_before_rebuild);
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
fn oracle_provenance_v4_requires_frozen_callback_input_and_atomic_differential_evidence() {
    parse_provenance(canonical_provenance());

    for (path, value) in [
        (
            &["callback_input", "expected_head"][..],
            json!("not-a-head"),
        ),
        (
            &["callback_input", "actual_head"][..],
            json!("1111111111111111111111111111111111111111"),
        ),
        (&["callback_input", "tree_oid"][..], json!("not-a-tree")),
        (
            &["callback_input", "input_manifest_sha256"][..],
            json!("not-a-hash"),
        ),
        (&["callback_input", "kiwi_sha256"][..], json!("not-a-hash")),
        (
            &["callback_input", "required_jobs_helper_sha256"][..],
            json!("not-a-hash"),
        ),
        (
            &["callback_input", "frozen_from_git_objects"][..],
            json!(false),
        ),
        (&["callback_input", "readonly_mount"][..], json!(false)),
        (
            &["callback_input", "revalidated_after_callback"][..],
            json!(false),
        ),
        (
            &["callback_input", "original_inputs_revalidated"][..],
            json!(false),
        ),
        (
            &["differential_evidence", "schema_version"][..],
            json!("wrong-schema"),
        ),
        (
            &["differential_evidence", "file_name"][..],
            json!("../escape.json"),
        ),
        (&["differential_evidence", "size_bytes"][..], json!(0)),
        (
            &["differential_evidence", "sha256"][..],
            json!("not-a-hash"),
        ),
        (
            &["differential_evidence", "published_atomically"][..],
            json!(false),
        ),
        (
            &["differential_evidence", "verified_after_publish"][..],
            json!(false),
        ),
    ] {
        let mut mutant = canonical_provenance();
        set_path(&mut mutant, path, value);
        assert_provenance_rejected(mutant);
    }

    let mut oversized_evidence = canonical_provenance();
    oversized_evidence["differential_evidence"]["size_bytes"] = json!(128_u64 * 1024 * 1024 + 1);
    assert_provenance_rejected(oversized_evidence);

    let mut uppercase_head = canonical_provenance();
    uppercase_head["callback_input"]["expected_head"] =
        json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    uppercase_head["callback_input"]["actual_head"] =
        json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    assert_provenance_rejected(uppercase_head);
}

#[test]
fn frozen_callback_controller_contract_is_wired() {
    let controller = include_str!("../../../scripts/compat/oracle_controller.py");

    for required in [
        "kiwi-redis-oracle-provenance/v4",
        "kiwi-vector-differential-evidence/v1",
        "MAX_CALLBACK_INPUT_ENTRIES = 8192",
        "MAX_CALLBACK_INPUT_FILE_BYTES = 512 * 1024 * 1024",
        "MAX_CALLBACK_INPUT_TOTAL_BYTES = 1024 * 1024 * 1024",
        "MAX_DIFFERENTIAL_EVIDENCE_BYTES = 128 * 1024 * 1024",
        "def _validate_callback_repository(",
        "def _materialize_callback_input(",
        "def _revalidate_callback_input(",
        "def _collect_differential_evidence(",
        "def publish_evidence_then_provenance(",
        "def _verify_published_binding(",
        "PYTHONNOUSERSITE",
        "PYTHONDONTWRITEBYTECODE",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
        "--expected-head",
        "--evidence-output",
        "--publication-verifier",
    ] {
        assert!(
            controller.contains(required),
            "frozen callback controller is missing {required}"
        );
    }

    let verify = controller
        .find("def verify_oracle(")
        .expect("controller must expose the final verifier");
    let verifier = &controller[verify..];
    let materialize = verifier
        .find("callback_input_snapshot = _materialize_callback_input(")
        .expect("controller must materialize frozen callback input");
    let callback = verifier
        .find("callback_document = _run_callback(")
        .expect("controller must run the callback");
    let collect = verifier
        .find("differential_evidence_document = _collect_differential_evidence(")
        .expect("controller must collect callback evidence");
    let after_collect = &verifier[collect..];
    let cleanup_frozen_revalidation = after_collect
        .find("_revalidate_callback_input(")
        .expect("controller must revalidate frozen and original inputs after evidence collection");
    let temp_removal = after_collect
        .find("cleanup(\"verifier temp root remove\"")
        .expect("controller must remove the verifier temp root before publication");
    let frozen_input_close = after_collect
        .find("cleanup(\"callback input close after verifier removal\"")
        .expect("controller must retain the frozen input descriptor through temp removal");
    let cleanup_final_revalidation = after_collect
        .find("callback_input_document = _finalize_callback_input_document(")
        .expect("controller must build callback input evidence from cleanup-final revalidation");
    let callback_repository_close = after_collect
        .find("cleanup(\"callback repository close\"")
        .expect("controller must close the callback repository after final revalidation");
    let evidence_publish = verifier
        .find("publish_evidence_then_provenance(")
        .expect("controller must publish evidence transactionally");
    assert!(materialize < callback);
    assert!(callback < evidence_publish);
    assert!(collect < evidence_publish);
    assert!(cleanup_frozen_revalidation < temp_removal);
    assert!(temp_removal < frozen_input_close);
    assert!(temp_removal < cleanup_final_revalidation);
    assert!(cleanup_final_revalidation < callback_repository_close);
    assert!(collect + cleanup_final_revalidation < evidence_publish);
}

#[test]
fn provenance_v4_controller_produces_callback_and_evidence_identity() {
    let controller = include_str!("../../../scripts/compat/oracle_controller.py");
    assert!(controller.contains("PROVENANCE_SCHEMA = \"kiwi-redis-oracle-provenance/v4\""));
    assert!(controller.contains("\"callback_input\": callback_input_document"));
    assert!(
        controller.contains(
            "final_provenance[\"differential_evidence\"] = differential_evidence_identity"
        )
    );
    assert!(controller.contains("publish_evidence_then_provenance("));
}

#[test]
#[cfg(target_os = "linux")]
fn frozen_callback_json_contract_rejects_non_finite_values() {
    let test_dir = TestDir::new("frozen-callback-non-finite-json");
    let body = r##"
for token in (b"NaN", b"Infinity", b"-Infinity"):
    payload = b'{"value":' + token + b'}'
    try:
        controller._strict_json_bytes(payload, "strict input")
    except controller.OracleError:
        pass
    else:
        raise AssertionError(f"strict JSON accepted {token!r}")

    try:
        controller._parse_json_lines(payload + b"\n", "JSONL evidence")
    except controller.OracleError:
        pass
    else:
        raise AssertionError(f"JSONL evidence accepted {token!r}")

for value in (float("nan"), float("inf"), float("-inf")):
    try:
        controller.canonical_json_bytes({"value": value})
    except controller.OracleError:
        pass
    else:
        raise AssertionError(f"canonical JSON accepted non-finite value {value!r}")
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn provenance_v4_publication_rejects_non_finite_values_without_final_files() {
    let test_dir = TestDir::new("provenance-v4-non-finite-publication");
    let body = r##"
import pathlib

root = pathlib.Path(__file__).parent

def provenance():
    return {
        "schema_version": "kiwi-redis-oracle-provenance/v4",
        "primary": {
            "started_at_utc": "2026-08-17T00:00:00Z",
            "finished_at_utc": "2026-08-17T00:00:01Z",
        },
        "rebuild": {
            "started_at_utc": "2026-08-17T00:00:01Z",
            "finished_at_utc": "2026-08-17T00:00:02Z",
        },
        "callback": {
            "started_at_utc": "2026-08-17T00:00:02Z",
            "finished_at_utc": "2026-08-17T00:00:03Z",
        },
        "cleanup": {"completed_at_utc": "2026-08-17T00:00:04Z"},
        "published_at_utc": "2026-08-17T00:00:05Z",
    }

for location in ("evidence", "provenance"):
    for label, value in (("nan", float("nan")), ("infinity", float("inf")), ("negative-infinity", float("-inf"))):
        case = root / f"{location}-{label}"
        case.mkdir()
        evidence_target = controller.CandidateTarget.open(case / "evidence.json")
        provenance_target = controller.CandidateTarget.open(case / "provenance.json")
        evidence = {
            "schema_version": "kiwi-vector-differential-evidence/v1",
            "value": value if location == "evidence" else 1,
        }
        document = provenance()
        document["value"] = value if location == "provenance" else 1
        try:
            try:
                controller.publish_evidence_then_provenance(
                    evidence_target, provenance_target, evidence, document
                )
            except controller.OracleError:
                pass
            else:
                raise AssertionError(f"{location} publication accepted {label}")
        finally:
            evidence_target.close()
            provenance_target.close()
        assert not (case / "evidence.json").exists()
        assert not (case / "provenance.json").exists()
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn frozen_callback_input_enforces_bounds_before_reading_the_next_entry() {
    let test_dir = TestDir::new("frozen-callback-bounds");
    let body = r##"
import os
import pathlib
import subprocess

root = pathlib.Path(__file__).parent
repository = root / "repository"
home = root / "home"
temporary = root / "tmp"
for directory in (repository, home, temporary):
    directory.mkdir()
(repository / "target" / "debug").mkdir(parents=True)
(repository / ".oracle-python" / "empty-a").mkdir(parents=True)
(repository / "tracked-a").write_bytes(b"A")
(repository / "tracked-b").write_bytes(b"B")
(repository / "target" / "debug" / "kiwi").write_bytes(b"K")
(repository / "target" / "debug" / "kiwi-required-vector-jobs").write_bytes(b"J")
for executable in (
    repository / "target" / "debug" / "kiwi",
    repository / "target" / "debug" / "kiwi-required-vector-jobs",
):
    executable.chmod(0o755)
subprocess.run(["/usr/bin/git", "-C", repository, "init", "-q"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "config", "user.email", "oracle@example.invalid"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "config", "user.name", "Oracle Test"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "add", "tracked-a", "tracked-b"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "commit", "-qm", "fixture"], check=True)
expected_head = subprocess.check_output(
    ["/usr/bin/git", "-C", repository, "rev-parse", "HEAD"], text=True
).strip()

with controller.HeldDirectory.open(repository) as held_repository:
    with controller.HeldExecutable.open("git", pathlib.Path("/usr/bin/git")) as git:
        env = controller._sanitized_environment("/usr/bin:/bin", home, temporary)
        identity = controller._validate_callback_repository(
            held_repository, expected_head, git, env
        )
        assert len(identity.tracked_entries) == 2

        exact_entries = controller.CallbackInputLimits(
            max_entries=6,
            max_file_bytes=16,
            max_total_bytes=16,
        )
        exact_destination = root / "exact-entries"
        exact_destination.mkdir()
        with controller.HeldDirectory.open(exact_destination) as held_destination:
            snapshot = controller._materialize_callback_input(
                held_repository,
                held_destination,
                identity,
                git,
                env,
                limits=exact_entries,
            )
        assert snapshot.manifest["entry_count"] == 4

        (repository / ".oracle-python" / "empty-b").mkdir()
        over_destination = root / "over-entries"
        over_destination.mkdir()
        python_root_identity = (repository / ".oracle-python").stat()
        original_listdir = controller.os.listdir

        def reject_unbounded_python_listdir(path):
            if isinstance(path, int):
                current = os.fstat(path)
                if (
                    current.st_dev == python_root_identity.st_dev
                    and current.st_ino == python_root_identity.st_ino
                ):
                    raise AssertionError("runtime callback directory used unbounded listdir")
            return original_listdir(path)

        controller.os.listdir = reject_unbounded_python_listdir
        try:
            with controller.HeldDirectory.open(over_destination) as held_destination:
                try:
                    controller._materialize_callback_input(
                        held_repository,
                        held_destination,
                        identity,
                        git,
                        env,
                        limits=exact_entries,
                    )
                except controller.OracleError as error:
                    assert "entry-count bound" in str(error)
                else:
                    raise AssertionError("limit+1 empty runtime directory was accepted")
        finally:
            controller.os.listdir = original_listdir
        (repository / ".oracle-python" / "empty-b").rmdir()

        tracked_limit = controller.CallbackInputLimits(
            max_entries=16,
            max_file_bytes=16,
            max_total_bytes=1,
        )
        tracked_destination = root / "tracked-over-total"
        tracked_destination.mkdir()
        second_blob_oid = identity.tracked_entries[1][2]
        second_blob = (
            repository
            / ".git"
            / "objects"
            / second_blob_oid[:2]
            / second_blob_oid[2:]
        )
        second_blob.unlink()
        with controller.HeldDirectory.open(tracked_destination) as held_destination:
            try:
                controller._materialize_callback_input(
                    held_repository,
                    held_destination,
                    identity,
                    git,
                    env,
                    limits=tracked_limit,
                )
            except controller.OracleError as error:
                assert "aggregate byte bound" in str(error)
            else:
                raise AssertionError("tracked aggregate limit+1 was accepted")

        (repository / ".oracle-python" / "a.py").write_bytes(b"P")
        exact_runtime = controller.CallbackInputLimits(
            max_entries=16,
            max_file_bytes=2,
            max_total_bytes=5,
        )
        entries = list(
            controller._runtime_callback_entries(
                held_repository,
                limits=exact_runtime,
                initial_entry_count=2,
                initial_total_bytes=2,
            )
        )
        assert [entry[0] for entry in entries] == [
            "target/debug/kiwi",
            "target/debug/kiwi-required-vector-jobs",
            ".oracle-python/a.py",
        ]

        over_path = repository / ".oracle-python" / "z.py"
        over_path.write_bytes(b"Z")
        over_inode = over_path.stat().st_ino
        original_read = controller.os.read
        over_file_read = False

        def reject_over_file_read(fd, size):
            global over_file_read
            if os.fstat(fd).st_ino == over_inode:
                over_file_read = True
            return original_read(fd, size)

        controller.os.read = reject_over_file_read
        try:
            try:
                list(
                    controller._runtime_callback_entries(
                        held_repository,
                        limits=exact_runtime,
                        initial_entry_count=2,
                        initial_total_bytes=2,
                    )
                )
            except controller.OracleError as error:
                assert "aggregate byte bound" in str(error)
            else:
                raise AssertionError("runtime aggregate limit+1 was accepted")
        finally:
            controller.os.read = original_read
        assert not over_file_read, "over-limit runtime file was read before rejection"

        over_path.write_bytes(b"ZZZ")
        per_file_limit = controller.CallbackInputLimits(
            max_entries=16,
            max_file_bytes=2,
            max_total_bytes=32,
        )
        over_inode = over_path.stat().st_ino
        over_file_read = False
        controller.os.read = reject_over_file_read
        try:
            try:
                list(
                    controller._runtime_callback_entries(
                        held_repository,
                        limits=per_file_limit,
                    )
                )
            except controller.OracleError as error:
                assert "file byte bound" in str(error)
            else:
                raise AssertionError("per-file limit+1 was accepted")
        finally:
            controller.os.read = original_read
        assert not over_file_read, "oversized runtime file was read before rejection"
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn frozen_callback_tree_oid_matches_expected_head_tree_under_valid_oid_mutant() {
    let test_dir = TestDir::new("frozen-callback-tree-oid");
    let body = r##"
import pathlib
import subprocess

root = pathlib.Path(__file__).parent
repository = root / "repository"
home = root / "home"
temporary = root / "tmp"
destination = root / "frozen"
for directory in (repository, home, temporary, destination):
    directory.mkdir()
(repository / "target" / "debug").mkdir(parents=True)
(repository / ".oracle-python").mkdir()
(repository / "tracked.txt").write_text("wrong tree\n", encoding="utf-8")
subprocess.run(["/usr/bin/git", "-C", repository, "init", "-q"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "config", "user.email", "oracle@example.invalid"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "config", "user.name", "Oracle Test"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "add", "tracked.txt"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "commit", "-qm", "wrong-tree"], check=True)
wrong_tree = subprocess.check_output(
    ["/usr/bin/git", "-C", repository, "rev-parse", "HEAD^{tree}"], text=True
).strip()
(repository / "tracked.txt").write_text("expected tree\n", encoding="utf-8")
subprocess.run(["/usr/bin/git", "-C", repository, "add", "tracked.txt"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "commit", "-qm", "expected-tree"], check=True)
expected_head = subprocess.check_output(
    ["/usr/bin/git", "-C", repository, "rev-parse", "HEAD"], text=True
).strip()
expected_tree = subprocess.check_output(
    ["/usr/bin/git", "-C", repository, "rev-parse", f"{expected_head}^{{tree}}"],
    text=True,
).strip()
assert wrong_tree != expected_tree
(repository / "target" / "debug" / "kiwi").write_bytes(b"kiwi")
(repository / "target" / "debug" / "kiwi-required-vector-jobs").write_bytes(b"jobs")
for executable in (
    repository / "target" / "debug" / "kiwi",
    repository / "target" / "debug" / "kiwi-required-vector-jobs",
):
    executable.chmod(0o755)

with controller.HeldDirectory.open(repository) as held_repository:
    with controller.HeldExecutable.open("git", pathlib.Path("/usr/bin/git")) as git:
        env = controller._sanitized_environment("/usr/bin:/bin", home, temporary)
        original_git_text = controller._callback_git_text

        def wrong_tree_mutant(git_tool, source, arguments, command_env):
            if arguments == ["rev-parse", f"{expected_head}^{{tree}}"]:
                return wrong_tree
            return original_git_text(git_tool, source, arguments, command_env)

        controller._callback_git_text = wrong_tree_mutant
        try:
            try:
                controller._validate_callback_repository(
                    held_repository, expected_head, git, env
                )
            except controller.OracleError as error:
                assert "tree OID differs from expected Head" in str(error)
            else:
                raise AssertionError("valid but wrong expected-Head tree OID was accepted")
        finally:
            controller._callback_git_text = original_git_text

        identity = controller._validate_callback_repository(
            held_repository, expected_head, git, env
        )
        with controller.HeldDirectory.open(destination) as held_destination:
            snapshot = controller._materialize_callback_input(
                held_repository, held_destination, identity, git, env
            )
        provenance_callback_input = dict(snapshot.document)
        assert identity.tree_oid == expected_tree
        assert snapshot.document["tree_oid"] == expected_tree
        assert provenance_callback_input["tree_oid"] == expected_tree
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn frozen_callback_head_race_uses_expected_commit_bytes_or_fails_before_callback() {
    let test_dir = TestDir::new("frozen-callback-head-race");
    let body = r##"
import pathlib
import subprocess

root = pathlib.Path(__file__).parent
repository = root / "repository"
destination = root / "frozen"
home = root / "home"
temporary = root / "tmp"
for directory in (repository, destination, home, temporary):
    directory.mkdir()
(repository / "scripts").mkdir()
(repository / "target" / "debug").mkdir(parents=True)
(repository / ".oracle-python").mkdir()
callback = repository / "scripts" / "callback.sh"
expected_bytes = b"#!/bin/sh\nprintf 'expected-head-bytes\\n'\n"
replacement_bytes = b"#!/bin/sh\nprintf 'replacement-head-bytes\\n'\n"
callback.write_bytes(expected_bytes)
callback.chmod(0o755)
(repository / "target" / "debug" / "kiwi").write_bytes(b"kiwi-runtime")
(repository / "target" / "debug" / "kiwi-required-vector-jobs").write_bytes(b"jobs-runtime")
(repository / ".oracle-python" / "plugin.py").write_text("PLUGIN = 'frozen'\n", encoding="utf-8")
for executable in (
    repository / "target" / "debug" / "kiwi",
    repository / "target" / "debug" / "kiwi-required-vector-jobs",
):
    executable.chmod(0o755)
subprocess.run(["/usr/bin/git", "-C", repository, "init", "-q"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "config", "user.email", "oracle@example.invalid"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "config", "user.name", "Oracle Test"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "add", "scripts/callback.sh"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "commit", "-qm", "expected"], check=True)
expected_head = subprocess.check_output(
    ["/usr/bin/git", "-C", repository, "rev-parse", "HEAD"], text=True
).strip()
callback.write_bytes(replacement_bytes)
subprocess.run(["/usr/bin/git", "-C", repository, "add", "scripts/callback.sh"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "commit", "-qm", "replacement"], check=True)
replacement_head = subprocess.check_output(
    ["/usr/bin/git", "-C", repository, "rev-parse", "HEAD"], text=True
).strip()
subprocess.run(["/usr/bin/git", "-C", repository, "update-ref", "HEAD", expected_head], check=True)
callback.write_bytes(expected_bytes)

original_git_text = controller._callback_git_text
state = {"switched": False}

def race_git_text(git, source, arguments, env):
    result = original_git_text(git, source, arguments, env)
    if arguments == ["rev-parse", "HEAD^{commit}"] and not state["switched"]:
        state["switched"] = True
        subprocess.run(
            ["/usr/bin/git", "-C", repository, "update-ref", "HEAD", replacement_head],
            check=True,
        )
        callback.write_bytes(replacement_bytes)
    return result

callback_marker = root / "callback-invoked"
controller._callback_git_text = race_git_text
try:
    with controller.HeldDirectory.open(repository) as held_repository:
        with controller.HeldDirectory.open(destination) as held_destination:
            with controller.HeldExecutable.open("git", pathlib.Path("/usr/bin/git")) as git:
                env = controller._sanitized_environment("/usr/bin:/bin", home, temporary)
                try:
                    identity = controller._validate_callback_repository(
                        held_repository, expected_head, git, env
                    )
                    controller._materialize_callback_input(
                        held_repository, held_destination, identity, git, env
                    )
                except controller.OracleError as error:
                    if "HEAD changed while validating callback input" not in str(error):
                        raise AssertionError(
                            f"HEAD race failed for an unrelated reason: {error}"
                        ) from error
                    assert not callback_marker.exists()
                else:
                    frozen = destination / "scripts" / "callback.sh"
                    observed = subprocess.check_output(["/bin/sh", frozen], text=True)
                    if observed != "expected-head-bytes\n":
                        raise AssertionError(
                            f"callback consumed bytes outside expected Head {expected_head}: {observed!r}"
                        )
                    callback_marker.write_text("invoked\n", encoding="utf-8")
                    assert callback_marker.read_text(encoding="utf-8") == "invoked\n"
finally:
    controller._callback_git_text = original_git_text
assert state["switched"]
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn frozen_callback_cleanup_final_revalidation_blocks_late_original_input_drift() {
    let test_dir = TestDir::new("frozen-callback-cleanup-final-revalidation");
    let body = r##"
import os
import pathlib
import subprocess

root = pathlib.Path(__file__).parent
repository = root / "repository"
home = root / "home"
temporary = root / "tmp"
for directory in (repository, home, temporary):
    directory.mkdir()
(repository / "scripts").mkdir()
(repository / "target" / "debug").mkdir(parents=True)
(repository / ".oracle-python").mkdir()
fixtures = {
    "scripts/callback.sh": b"#!/bin/sh\nprintf 'tracked-callback\\n'\n",
    "target/debug/kiwi": b"kiwi-runtime",
    "target/debug/kiwi-required-vector-jobs": b"jobs-runtime",
    ".oracle-python/plugin.py": b"PLUGIN = 'frozen'\n",
}
for relative, payload in fixtures.items():
    path = repository / relative
    path.write_bytes(payload)
for relative in (
    "scripts/callback.sh",
    "target/debug/kiwi",
    "target/debug/kiwi-required-vector-jobs",
):
    (repository / relative).chmod(0o755)
subprocess.run(["/usr/bin/git", "-C", repository, "init", "-q"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "config", "user.email", "oracle@example.invalid"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "config", "user.name", "Oracle Test"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "add", "scripts/callback.sh"], check=True)
subprocess.run(["/usr/bin/git", "-C", repository, "commit", "-qm", "fixture"], check=True)
expected_head = subprocess.check_output(
    ["/usr/bin/git", "-C", repository, "rev-parse", "HEAD"], text=True
).strip()

provenance = {
    "schema_version": "kiwi-redis-oracle-provenance/v4",
    "primary": {
        "started_at_utc": "2026-08-17T00:00:00Z",
        "finished_at_utc": "2026-08-17T00:00:01Z",
    },
    "rebuild": {
        "started_at_utc": "2026-08-17T00:00:01Z",
        "finished_at_utc": "2026-08-17T00:00:02Z",
    },
    "callback": {
        "started_at_utc": "2026-08-17T00:00:02Z",
        "finished_at_utc": "2026-08-17T00:00:03Z",
    },
    "cleanup": {"completed_at_utc": "2026-08-17T00:00:04Z"},
    "published_at_utc": "2026-08-17T00:00:05Z",
}

covered = set()
with controller.HeldDirectory.open(repository) as held_repository:
    with controller.HeldExecutable.open("git", pathlib.Path("/usr/bin/git")) as git:
        env = controller._sanitized_environment("/usr/bin:/bin", home, temporary)
        for relative, original_bytes in fixtures.items():
            original_path = repository / relative
            original_mode = original_path.stat().st_mode & 0o777
            for mutation in ("same-inode-rewrite", "inode-replacement"):
                case_name = relative.replace("/", "-") + "-" + mutation
                case_root = root / case_name
                frozen_root = case_root / "frozen"
                frozen_root.mkdir(parents=True)
                evidence_path = case_root / "evidence.json"
                provenance_path = case_root / "provenance.json"
                try:
                    with controller.HeldDirectory.open(frozen_root) as held_frozen:
                        identity = controller._validate_callback_repository(
                            held_repository, expected_head, git, env
                        )
                        snapshot = controller._materialize_callback_input(
                            held_repository, held_frozen, identity, git, env
                        )
                        controller._revalidate_callback_input(
                            snapshot, held_repository, git, env
                        )
                        frozen_bytes = (frozen_root / relative).read_bytes()
                        assert frozen_bytes == original_bytes

                        before_inode = original_path.stat().st_ino
                        replacement_bytes = (
                            b"S" if mutation == "same-inode-rewrite" else b"I"
                        ) * len(original_bytes)
                        if mutation == "same-inode-rewrite":
                            with original_path.open("r+b") as mutable:
                                mutable.write(replacement_bytes)
                                mutable.truncate()
                                mutable.flush()
                                os.fsync(mutable.fileno())
                            assert original_path.stat().st_ino == before_inode
                        else:
                            replacement = original_path.with_name(
                                original_path.name + ".replacement"
                            )
                            replacement.write_bytes(replacement_bytes)
                            replacement.chmod(original_mode)
                            os.replace(replacement, original_path)
                            assert original_path.stat().st_ino != before_inode
                        assert (frozen_root / relative).read_bytes() == frozen_bytes

                        evidence_target = controller.CandidateTarget.open(evidence_path)
                        provenance_target = controller.CandidateTarget.open(provenance_path)
                        try:
                            try:
                                callback_input_document = (
                                    controller._finalize_callback_input_document(
                                        snapshot, held_repository, git, env
                                    )
                                )
                                final_provenance = dict(provenance)
                                final_provenance["callback_input"] = callback_input_document
                                controller.publish_evidence_then_provenance(
                                    evidence_target,
                                    provenance_target,
                                    {
                                        "schema_version": "kiwi-vector-differential-evidence/v1",
                                        "case": case_name,
                                    },
                                    final_provenance,
                                )
                            except controller.OracleError as error:
                                expected_error = (
                                    "Kiwi checkout has tracked changes"
                                    if relative == "scripts/callback.sh"
                                    else "original callback runtime inputs changed"
                                )
                                if expected_error not in str(error):
                                    raise AssertionError(
                                        f"late {relative} {mutation} drift failed for "
                                        f"an unrelated reason: {error}"
                                    ) from error
                        finally:
                            evidence_target.close()
                            provenance_target.close()

                        if evidence_path.exists() or provenance_path.exists():
                            raise AssertionError(
                                f"late {relative} {mutation} drift reached publication: "
                                f"evidence={evidence_path.exists()} provenance={provenance_path.exists()}"
                            )
                        covered.add(f"{relative}:{mutation}")
                finally:
                    original_path.write_bytes(original_bytes)
                    original_path.chmod(original_mode)

assert covered == {
    f"{relative}:{mutation}"
    for relative in fixtures
    for mutation in ("same-inode-rewrite", "inode-replacement")
}
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn provenance_v4_evidence_allowlist_rejects_git_and_nested_artifacts() {
    let test_dir = TestDir::new("provenance-v4-evidence-artifacts");
    let body = r##"
import pathlib

root = pathlib.Path(__file__).parent
for index, relative in enumerate((".git", ".git/config", "nested/artifact")):
    work = root / f"work-{index}"
    work.mkdir()
    artifact = work / relative
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("unexpected", encoding="utf-8")
    with controller.HeldDirectory.open(work) as held_work:
        try:
            controller._collect_differential_evidence(
                held_work, -1, None, {}, {}, []
            )
        except controller.OracleError as error:
            if relative not in str(error):
                raise AssertionError(
                    f"evidence allowlist hid {relative!r} instead of reporting it: {error}"
                )
        else:
            raise AssertionError(f"evidence allowlist accepted {relative!r}")
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn frozen_callback_executes_head_object_bytes_and_rejects_original_drift() {
    let test_dir = TestDir::new("frozen-callback");
    let body = r##"
import os
import pathlib
import subprocess

root = pathlib.Path(__file__).parent
repository = root / "repository"
destination = root / "frozen"
home = root / "home"
temporary = root / "tmp"
repository.mkdir()
destination.mkdir()
home.mkdir()
temporary.mkdir()
(repository / "scripts").mkdir()
(repository / "target" / "debug").mkdir(parents=True)
(repository / ".oracle-python").mkdir()
(repository / "scripts" / "callback.sh").write_text(
    "#!/bin/sh\nprintf 'HEAD-object-bytes\\n'\n", encoding="utf-8"
)
(repository / "tracked.txt").write_text("tracked-from-head\n", encoding="utf-8")
(repository / "target" / "debug" / "kiwi").write_bytes(b"kiwi-runtime")
(repository / "target" / "debug" / "kiwi-required-vector-jobs").write_bytes(b"jobs-runtime")
(repository / ".oracle-python" / "plugin.py").write_text(
    "PLUGIN = 'frozen'\n", encoding="utf-8"
)
(repository / "target" / "debug" / "deps").mkdir()
for executable in (
    repository / "scripts" / "callback.sh",
    repository / "target" / "debug" / "kiwi",
    repository / "target" / "debug" / "kiwi-required-vector-jobs",
):
    executable.chmod(0o755)
for executable in (
    repository / "target" / "debug" / "kiwi",
    repository / "target" / "debug" / "kiwi-required-vector-jobs",
):
    os.link(
        executable,
        repository / "target" / "debug" / "deps" / f"{executable.name}-cargo-artifact",
    )
    assert executable.stat().st_nlink == 2
subprocess.run(["/usr/bin/git", "-C", repository, "init", "-q"], check=True)
subprocess.run(
    ["/usr/bin/git", "-C", repository, "config", "user.email", "oracle@example.invalid"],
    check=True,
)
subprocess.run(
    ["/usr/bin/git", "-C", repository, "config", "user.name", "Oracle Test"],
    check=True,
)
subprocess.run(
    ["/usr/bin/git", "-C", repository, "add", "scripts/callback.sh", "tracked.txt"],
    check=True,
)
subprocess.run(["/usr/bin/git", "-C", repository, "commit", "-qm", "fixture"], check=True)
expected_head = subprocess.check_output(
    ["/usr/bin/git", "-C", repository, "rev-parse", "HEAD"], text=True
).strip()

with controller.HeldDirectory.open(repository) as held_repository:
    with controller.HeldDirectory.open(destination) as held_destination:
        with controller.HeldExecutable.open("git", pathlib.Path("/usr/bin/git")) as git:
            env = controller._sanitized_environment("/usr/bin:/bin", home, temporary)
            identity = controller._validate_callback_repository(
                held_repository, expected_head, git, env
            )
            snapshot = controller._materialize_callback_input(
                held_repository, held_destination, identity, git, env
            )
            for relative in controller.CALLBACK_RUNTIME_PATHS:
                source_runtime = repository / relative
                frozen_runtime = destination / relative
                assert source_runtime.stat().st_nlink == 2
                assert frozen_runtime.stat().st_nlink == 1
                assert source_runtime.stat().st_ino != frozen_runtime.stat().st_ino
                assert source_runtime.read_bytes() == frozen_runtime.read_bytes()
                assert (source_runtime.stat().st_mode & 0o777) == (
                    frozen_runtime.stat().st_mode & 0o777
                )
            (repository / "scripts" / "callback.sh").write_text(
                "#!/bin/sh\nprintf 'host-replacement\\n'\n", encoding="utf-8"
            )
            observed = subprocess.check_output(
                ["/bin/sh", destination / "scripts" / "callback.sh"], text=True
            )
            assert observed == "HEAD-object-bytes\n"
            try:
                controller._revalidate_callback_input(snapshot, held_repository, git, env)
            except controller.OracleError:
                pass
            else:
                raise AssertionError("tracked original-input drift was accepted")

            (repository / "scripts" / "callback.sh").write_text(
                "#!/bin/sh\nprintf 'HEAD-object-bytes\\n'\n", encoding="utf-8"
            )
            (repository / ".oracle-python" / "plugin.py").write_text(
                "PLUGIN = 'host replacement'\n", encoding="utf-8"
            )
            try:
                controller._revalidate_callback_input(snapshot, held_repository, git, env)
            except controller.OracleError:
                pass
            else:
                raise AssertionError("runtime dependency drift was accepted")
            (repository / ".oracle-python" / "plugin.py").write_text(
                "PLUGIN = 'frozen'\n", encoding="utf-8"
            )

            replay_source = repository / "target" / "debug" / "kiwi"
            replay_original = replay_source.read_bytes()
            replay_bytes = b"M" * len(replay_original)
            replay_root = root / "hard-link-replay"
            replay_root.mkdir()
            replay_source.write_bytes(replay_bytes)
            try:
                with controller.HeldDirectory.open(replay_root) as held_replay:
                    replay_identity = controller._validate_callback_repository(
                        held_repository, expected_head, git, env
                    )
                    replay_snapshot = controller._materialize_callback_input(
                        held_repository, held_replay, replay_identity, git, env
                    )
                    assert (replay_root / "target" / "debug" / "kiwi").read_bytes() == replay_bytes
                    replay_source.write_bytes(replay_original)
                    replay_source.write_bytes(replay_bytes)
                    try:
                        controller._revalidate_callback_input(
                            replay_snapshot, held_repository, git, env
                        )
                    except controller.OracleError:
                        pass
                    else:
                        raise AssertionError(
                            "hard-linked runtime content replay was accepted"
                        )
            finally:
                replay_source.write_bytes(replay_original)

            covered_runtime_mutations = set()
            for relative in (
                "target/debug/kiwi",
                "target/debug/kiwi-required-vector-jobs",
            ):
                source_path = repository / relative
                original_bytes = source_path.read_bytes()
                original_mode = source_path.stat().st_mode & 0o777
                for mutation in (
                    "same-inode-rewrite",
                    "inode-replacement",
                    "same-bytes-inode-replacement",
                ):
                    case_name = relative.replace("/", "-") + "-" + mutation
                    case_root = root / case_name
                    frozen_root = case_root / "frozen"
                    frozen_root.mkdir(parents=True)
                    evidence_final = case_root / "evidence.json"
                    provenance_final = case_root / "provenance.json"
                    try:
                        with controller.HeldDirectory.open(frozen_root) as held_frozen:
                            case_identity = controller._validate_callback_repository(
                                held_repository, expected_head, git, env
                            )
                            case_snapshot = controller._materialize_callback_input(
                                held_repository, held_frozen, case_identity, git, env
                            )
                            frozen_path = frozen_root / relative
                            assert frozen_path.read_bytes() == original_bytes
                            before_inode = source_path.stat().st_ino
                            replacement_bytes = (
                                original_bytes
                                if mutation == "same-bytes-inode-replacement"
                                else (
                                    b"R" if mutation == "same-inode-rewrite" else b"P"
                                )
                                * len(original_bytes)
                            )
                            if mutation == "same-inode-rewrite":
                                with source_path.open("r+b") as mutable:
                                    mutable.write(replacement_bytes)
                                    mutable.truncate()
                                    mutable.flush()
                                    os.fsync(mutable.fileno())
                                assert source_path.stat().st_ino == before_inode
                            else:
                                replacement = source_path.with_name(source_path.name + ".replacement")
                                replacement.write_bytes(replacement_bytes)
                                replacement.chmod(original_mode)
                                os.replace(replacement, source_path)
                                assert source_path.stat().st_ino != before_inode

                            assert frozen_path.read_bytes() == original_bytes
                            evidence_target = controller.CandidateTarget.open(evidence_final)
                            provenance_target = controller.CandidateTarget.open(provenance_final)
                            try:
                                try:
                                    controller._revalidate_callback_input(
                                        case_snapshot, held_repository, git, env
                                    )
                                except controller.OracleError:
                                    pass
                                else:
                                    controller.publish_evidence_then_provenance(
                                        evidence_target,
                                        provenance_target,
                                        {
                                            "schema_version": "kiwi-vector-differential-evidence/v1",
                                            "case": case_name,
                                        },
                                        {
                                            "schema_version": "kiwi-redis-oracle-provenance/v4",
                                            "primary": {
                                                "started_at_utc": "2026-08-17T00:00:00Z",
                                                "finished_at_utc": "2026-08-17T00:00:01Z",
                                            },
                                            "rebuild": {
                                                "started_at_utc": "2026-08-17T00:00:01Z",
                                                "finished_at_utc": "2026-08-17T00:00:02Z",
                                            },
                                            "callback": {
                                                "started_at_utc": "2026-08-17T00:00:02Z",
                                                "finished_at_utc": "2026-08-17T00:00:03Z",
                                            },
                                            "cleanup": {
                                                "completed_at_utc": "2026-08-17T00:00:04Z"
                                            },
                                            "published_at_utc": "2026-08-17T00:00:05Z",
                                        },
                                    )
                                    raise AssertionError(
                                        f"{relative} {mutation} reached publication"
                                    )
                            finally:
                                evidence_target.close()
                                provenance_target.close()
                            assert not evidence_final.exists()
                            assert not provenance_final.exists()
                            covered_runtime_mutations.add(f"{relative}:{mutation}")
                    finally:
                        source_path.write_bytes(original_bytes)
                        source_path.chmod(original_mode)

            assert covered_runtime_mutations == {
                "target/debug/kiwi:same-inode-rewrite",
                "target/debug/kiwi:inode-replacement",
                "target/debug/kiwi:same-bytes-inode-replacement",
                "target/debug/kiwi-required-vector-jobs:same-inode-rewrite",
                "target/debug/kiwi-required-vector-jobs:inode-replacement",
                "target/debug/kiwi-required-vector-jobs:same-bytes-inode-replacement",
            }

            (repository / "target" / "debug" / "kiwi-required-vector-jobs").unlink()
            os.link(
                repository / "target" / "debug" / "kiwi",
                repository / "target" / "debug" / "kiwi-required-vector-jobs",
            )
            try:
                list(controller._runtime_callback_entries(held_repository))
            except controller.OracleError:
                pass
            else:
                raise AssertionError("runtime hard-link relationship was accepted")
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn provenance_v4_publication_close_faults_roll_back_both_final_files() {
    let test_dir = TestDir::new("provenance-v4-publication-close");
    let body = r##"
import pathlib

root = pathlib.Path(__file__).parent
evidence_document = {
    "schema_version": "kiwi-vector-differential-evidence/v1",
    "payload": "evidence",
}
provenance_document = {
    "schema_version": "kiwi-redis-oracle-provenance/v4",
    "primary": {
        "started_at_utc": "2026-08-17T00:00:00Z",
        "finished_at_utc": "2026-08-17T00:00:01Z",
    },
    "rebuild": {
        "started_at_utc": "2026-08-17T00:00:01Z",
        "finished_at_utc": "2026-08-17T00:00:02Z",
    },
    "callback": {
        "started_at_utc": "2026-08-17T00:00:02Z",
        "finished_at_utc": "2026-08-17T00:00:03Z",
    },
    "cleanup": {"completed_at_utc": "2026-08-17T00:00:04Z"},
    "published_at_utc": "2026-08-17T00:00:05Z",
}

def targets(case):
    directory = root / case
    evidence_parent = directory / "evidence-parent"
    provenance_parent = directory / "provenance-parent"
    evidence_parent.mkdir(parents=True)
    provenance_parent.mkdir()
    return (
        controller.CandidateTarget.open(evidence_parent / "evidence.json"),
        controller.CandidateTarget.open(provenance_parent / "provenance.json"),
    )

def assert_clean(case, evidence_target, provenance_target):
    assert not evidence_target.path.exists(), case
    assert not provenance_target.path.exists(), case
    assert not list(evidence_target.path.parent.glob(".*.evidence-*")), case
    assert not list(provenance_target.path.parent.glob(".*.provenance-*")), case

evidence_target, provenance_target = targets("published-output-fd")
original_open = controller.os.open
original_close = controller.os.close
published_output_fds = {}
injected = False

def track_published_output_open(path, flags, mode=0o777, *, dir_fd=None):
    fd = original_open(path, flags, mode, dir_fd=dir_fd)
    if (
        path in {"evidence.json", "provenance.json"}
        and path not in published_output_fds
    ):
        published_output_fds[path] = fd
    return fd

def fail_published_output_close(fd):
    global injected
    if fd in published_output_fds.values() and not injected:
        injected = True
        raise OSError(5, "injected PublishedOutput.fd close failure")
    return original_close(fd)

controller.os.open = track_published_output_open
controller.os.close = fail_published_output_close
try:
    try:
        controller.publish_evidence_then_provenance(
            evidence_target,
            provenance_target,
            evidence_document,
            provenance_document,
            close_targets=True,
        )
    except (controller.OracleError, OSError):
        pass
    else:
        raise AssertionError("PublishedOutput.fd close failure was accepted")
finally:
    controller.os.open = original_open
    controller.os.close = original_close
    for fd in published_output_fds.values():
        try:
            original_close(fd)
        except OSError:
            pass
    evidence_target.close()
    provenance_target.close()
assert injected, "PublishedOutput.fd close fault was not reached"
assert_clean("published-output-fd", evidence_target, provenance_target)

rollback_parent_survivors = []
for failed_basename in ("evidence.json", "provenance.json"):
    case = "candidate-" + failed_basename.removesuffix(".json") + "-parent-fd"
    evidence_target, provenance_target = targets(case)
    original_close = controller.os.close
    failed_parent_fd = (
        evidence_target.parent.fd
        if failed_basename == "evidence.json"
        else provenance_target.parent.fd
    )
    injected = False

    def fail_candidate_parent_close(fd):
        global injected
        if fd == failed_parent_fd and not injected:
            injected = True
            raise OSError(5, f"injected {failed_basename} parent FD close failure")
        return original_close(fd)

    controller.os.close = fail_candidate_parent_close
    try:
        try:
            controller.publish_evidence_then_provenance(
                evidence_target,
                provenance_target,
                evidence_document,
                provenance_document,
                close_targets=True,
            )
        except (controller.OracleError, OSError):
            pass
        else:
            raise AssertionError(f"{failed_basename} parent close failure was accepted")
    finally:
        controller.os.close = original_close
        evidence_target.close()
        provenance_target.close()
    assert injected, f"{failed_basename} parent close fault was not reached"
    assert_clean(case, evidence_target, provenance_target)

for failed_basename in ("evidence.json", "provenance.json"):
    case = "rollback-" + failed_basename.removesuffix(".json") + "-parent-fd"
    evidence_target, provenance_target = targets(case)
    original_dup = controller.os.dup
    original_close = controller.os.close
    parent_names = {
        evidence_target.parent.fd: "evidence.json",
        provenance_target.parent.fd: "provenance.json",
    }
    rollback_parent_fds = {}
    injected = False

    def track_rollback_parent(fd):
        duplicate = original_dup(fd)
        rollback_parent_fds[parent_names[fd]] = duplicate
        return duplicate

    def fail_rollback_parent_close(fd):
        global injected
        if fd == rollback_parent_fds.get(failed_basename) and not injected:
            injected = True
            raise OSError(5, f"injected {failed_basename} rollback-parent close failure")
        return original_close(fd)

    controller.os.dup = track_rollback_parent
    controller.os.close = fail_rollback_parent_close
    try:
        try:
            controller.publish_evidence_then_provenance(
                evidence_target,
                provenance_target,
                evidence_document,
                provenance_document,
                close_targets=True,
            )
        except (controller.OracleError, OSError):
            pass
        else:
            raise AssertionError(
                f"{failed_basename} rollback-parent close failure was accepted"
            )
    finally:
        controller.os.dup = original_dup
        controller.os.close = original_close
        for fd in rollback_parent_fds.values():
            try:
                original_close(fd)
            except OSError:
                pass
        evidence_target.close()
        provenance_target.close()
    assert injected, f"{failed_basename} rollback-parent close fault was not reached"
    if evidence_target.path.exists() or provenance_target.path.exists():
        rollback_parent_survivors.append(failed_basename)
    assert not list(evidence_target.path.parent.glob(".*.evidence-*")), case
    assert not list(provenance_target.path.parent.glob(".*.provenance-*")), case
    for final_path in (evidence_target.path, provenance_target.path):
        if final_path.exists():
            final_path.unlink()
assert not rollback_parent_survivors, rollback_parent_survivors
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
}

#[test]
#[cfg(target_os = "linux")]
fn provenance_v4_publication_faults_roll_back_both_final_files() {
    let test_dir = TestDir::new("provenance-v4-publication");
    let body = r##"
import hashlib
import json
import os
import pathlib

root = pathlib.Path(__file__).parent
evidence_document = {
    "schema_version": "kiwi-vector-differential-evidence/v1",
    "payload": "evidence",
}
provenance_document = {
    "schema_version": "kiwi-redis-oracle-provenance/v4",
    "primary": {
        "started_at_utc": "2026-08-17T00:00:00Z",
        "finished_at_utc": "2026-08-17T00:00:01Z",
    },
    "rebuild": {
        "started_at_utc": "2026-08-17T00:00:01Z",
        "finished_at_utc": "2026-08-17T00:00:02Z",
    },
    "callback": {
        "started_at_utc": "2026-08-17T00:00:02Z",
        "finished_at_utc": "2026-08-17T00:00:03Z",
    },
    "cleanup": {"completed_at_utc": "2026-08-17T00:00:04Z"},
    "published_at_utc": "2026-08-17T00:00:05Z",
}

def targets(case, distinct_parents=False):
    directory = root / case
    directory.mkdir()
    evidence_parent = directory
    provenance_parent = directory
    if distinct_parents:
        evidence_parent = directory / "evidence-parent"
        provenance_parent = directory / "provenance-parent"
        evidence_parent.mkdir()
        provenance_parent.mkdir()
    return (
        controller.CandidateTarget.open(evidence_parent / "evidence.json"),
        controller.CandidateTarget.open(provenance_parent / "provenance.json"),
    )

evidence_target, provenance_target = targets("success")
try:
    identity = controller.publish_evidence_then_provenance(
        evidence_target, provenance_target, evidence_document, provenance_document
    )
    evidence = evidence_target.path.read_bytes()
    provenance = json.loads(provenance_target.path.read_text(encoding="utf-8"))
    assert identity["size_bytes"] == len(evidence)
    assert identity["sha256"] == hashlib.sha256(evidence).hexdigest()
    assert provenance["differential_evidence"] == identity
finally:
    evidence_target.close()
    provenance_target.close()

def run_fault(case, kind, fail_at, distinct_parents=False):
    evidence_target, provenance_target = targets(case, distinct_parents)
    evidence_path = evidence_target.path
    provenance_path = provenance_target.path
    original_open = controller.os.open
    original_write = controller.os.write
    original_fsync = controller.os.fsync
    original_rename = controller._rename_noreplace
    calls = {"write": 0, "fsync": 0, "rename": 0, "verify": 0}
    final_open_counts = {}

    def fail(name):
        calls[name] += 1
        if name == kind and calls[name] == fail_at:
            raise OSError(5, f"injected {name} failure")

    def write(fd, payload):
        fail("write")
        return original_write(fd, payload)

    def fsync(fd):
        fail("fsync")
        return original_fsync(fd)

    def open_file(path, flags, mode=0o777, *, dir_fd=None):
        if (
            path in {"evidence.json", "provenance.json"}
            and flags & os.O_ACCMODE == os.O_RDONLY
        ):
            final_open_counts[path] = final_open_counts.get(path, 0) + 1
            if final_open_counts[path] == 2:
                fail("verify")
        return original_open(path, flags, mode, dir_fd=dir_fd)

    def rename(directory_fd, source, target):
        fail("rename")
        return original_rename(directory_fd, source, target)

    controller.os.open = open_file
    controller.os.write = write
    controller.os.fsync = fsync
    controller._rename_noreplace = rename
    try:
        try:
            controller.publish_evidence_then_provenance(
                evidence_target, provenance_target, evidence_document, provenance_document
            )
        except (controller.OracleError, OSError):
            pass
        else:
            raise AssertionError(f"{case} fault was accepted")
    finally:
        controller.os.open = original_open
        controller.os.write = original_write
        controller.os.fsync = original_fsync
        controller._rename_noreplace = original_rename
        evidence_target.close()
        provenance_target.close()
    assert not evidence_path.exists(), case
    assert not provenance_path.exists(), case

for case, kind, fail_at in (
    ("evidence-write", "write", 1),
    ("evidence-file-fsync", "fsync", 1),
    ("evidence-rename", "rename", 1),
    ("evidence-parent-fsync", "fsync", 2),
    ("provenance-write", "write", 2),
    ("provenance-file-fsync", "fsync", 3),
    ("provenance-parent-fsync", "fsync", 4),
    ("provenance-rename", "rename", 2),
    ("post-evidence-rehash", "verify", 1),
    ("post-provenance-rehash", "verify", 2),
    ("final-transaction-parent-fsync", "fsync", 5),
):
    run_fault(case, kind, fail_at)

for case, fail_at in (
    ("distinct-provenance-parent-fsync", 4),
    ("distinct-final-evidence-parent-fsync", 5),
    ("distinct-final-provenance-parent-fsync", 6),
):
    run_fault(case, "fsync", fail_at, distinct_parents=True)

original_limit = controller.MAX_DIFFERENTIAL_EVIDENCE_BYTES
controller.MAX_DIFFERENTIAL_EVIDENCE_BYTES = 32
evidence_target, provenance_target = targets("oversized-evidence")
try:
    try:
        controller.publish_evidence_then_provenance(
            evidence_target,
            provenance_target,
            {"schema_version": "kiwi-vector-differential-evidence/v1", "pad": "x" * 64},
            provenance_document,
        )
    except controller.OracleError:
        pass
    else:
        raise AssertionError("oversized evidence was accepted")
finally:
    controller.MAX_DIFFERENTIAL_EVIDENCE_BYTES = original_limit
    evidence_target.close()
    provenance_target.close()
assert not (root / "oversized-evidence" / "evidence.json").exists()
assert not (root / "oversized-evidence" / "provenance.json").exists()
"##;
    assert_probe_succeeds(run_python_probe(&test_dir, body));
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
        "callback_input": {
            "expected_head": "2222222222222222222222222222222222222222",
            "actual_head": "2222222222222222222222222222222222222222",
            "tree_oid": "3333333333333333333333333333333333333333",
            "ref_context": "refs/pull/422/head",
            "input_manifest_sha256": TOOL_SHA,
            "kiwi_sha256": CLI_SHA,
            "required_jobs_helper_sha256": REDIS_SHA,
            "frozen_from_git_objects": true,
            "readonly_mount": true,
            "revalidated_after_callback": true,
            "original_inputs_revalidated": true
        },
        "differential_evidence": {
            "schema_version": "kiwi-vector-differential-evidence/v1",
            "file_name": "vector-differential-evidence.json",
            "size_bytes": 4096,
            "sha256": TOOL_SHA,
            "published_atomically": true,
            "verified_after_publish": true
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
    let (started_at_utc, finished_at_utc) = if role == "primary" {
        ("2026-08-11T00:00:00Z", "2026-08-11T00:00:05Z")
    } else {
        ("2026-08-11T00:00:05Z", "2026-08-11T00:00:06Z")
    };
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
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc
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
