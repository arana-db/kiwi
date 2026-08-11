#!/usr/bin/env python3
# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

"""Fail-closed Redis 8.8.1 primary Oracle build controller."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import os
import pathlib
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence

BUILD_SCHEMA = "kiwi-redis-oracle-build/v3"
RECIPE_ID = "redis-8.8.1-linux-release-v3"
REDIS_TAG = "8.8.1"
REDIS_COMMIT = "77b6c308396c9700672390a210143a8496fb4b10"
REDIS_REPOSITORY = "https://github.com/redis/redis.git"
SOURCE_DATE_EPOCH = 1_784_834_134
BUILD_ARGV = [
    "make",
    "-C",
    "/proc/self/fd/{source_fd}",
    "SHELL=/proc/self/fd/{shell_fd}",
    "BUILD_TLS=no",
    "MALLOC=libc",
    "DEBUG=",
    "DEBUG_FLAGS=",
    "ENABLE_LTO=",
    "OPT=-O3 -fno-omit-frame-pointer",
    "-j",
    "1",
    "redis-server",
]

COMMAND_TIMEOUT_MS = 30_000
BUILD_TIMEOUT_MS = 1_200_000
TERM_GRACE_MS = 5_000
VERSION_OUTPUT_LIMIT = 16 * 1024
BUILD_OUTPUT_LIMIT = 16 * 1024 * 1024


class OracleError(RuntimeError):
    """A fail-closed Oracle controller error."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _file_identity(st: os.stat_result) -> dict[str, int]:
    return {
        "device": st.st_dev,
        "inode": st.st_ino,
        "mode": st.st_mode,
        "size": st.st_size,
        "nlink": st.st_nlink,
    }


def _same_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        left.st_mode,
        left.st_size,
        left.st_nlink,
    ) == (
        right.st_dev,
        right.st_ino,
        right.st_mode,
        right.st_size,
        right.st_nlink,
    )


def _sha256_fd(fd: int) -> str:
    offset = os.lseek(fd, 0, os.SEEK_CUR)
    os.lseek(fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while True:
        chunk = os.read(fd, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
    os.lseek(fd, offset, os.SEEK_SET)
    return digest.hexdigest()


class HeldDirectory:
    """A source root retained and traversed only through no-follow dir FDs."""

    def __init__(self, path: pathlib.Path, fd: int):
        self.path = path
        self.fd = fd
        self.stat = os.fstat(fd)
        if not stat.S_ISDIR(self.stat.st_mode):
            raise OracleError(f"held source root is not a directory: {path}")

    @classmethod
    def open(cls, path: pathlib.Path) -> "HeldDirectory":
        resolved = path.resolve(strict=True)
        before = os.stat(resolved, follow_symlinks=False)
        fd = os.open(
            resolved, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        try:
            held = cls(resolved, fd)
            if not _same_identity(before, held.stat):
                raise OracleError(f"source root changed while it was opened: {resolved}")
            return held
        except BaseException:
            os.close(fd)
            raise

    @staticmethod
    def _parts(relative: str) -> list[str]:
        parts = relative.split("/")
        if not relative or any(part in {"", ".", ".."} for part in parts):
            raise OracleError(f"non-canonical source-relative path: {relative!r}")
        return parts

    def _parent_fd(self, relative: str) -> tuple[int, str]:
        parts = self._parts(relative)
        directory_fd = os.dup(self.fd)
        try:
            for part in parts[:-1]:
                next_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=directory_fd,
                )
                os.close(directory_fd)
                directory_fd = next_fd
            return directory_fd, parts[-1]
        except BaseException:
            os.close(directory_fd)
            raise

    def lstat(self, relative: str) -> os.stat_result:
        directory_fd, name = self._parent_fd(relative)
        try:
            return os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        finally:
            os.close(directory_fd)

    def readlink(self, relative: str) -> str:
        directory_fd, name = self._parent_fd(relative)
        try:
            return os.readlink(name, dir_fd=directory_fd)
        finally:
            os.close(directory_fd)

    def regular_evidence(self, relative: str) -> tuple[os.stat_result, str]:
        directory_fd, name = self._parent_fd(relative)
        try:
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if not stat.S_ISREG(before.st_mode):
                raise OracleError(f"artifact is not a regular file: {relative}")
            fd = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory_fd,
            )
        finally:
            os.close(directory_fd)
        try:
            held = os.fstat(fd)
            if not _same_identity(before, held):
                raise OracleError(f"artifact changed while it was opened: {relative}")
            return held, _sha256_fd(fd)
        finally:
            os.close(fd)

    def open_regular(self, relative: str) -> int:
        directory_fd, name = self._parent_fd(relative)
        try:
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            fd = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory_fd,
            )
        finally:
            os.close(directory_fd)
        held = os.fstat(fd)
        if not stat.S_ISREG(held.st_mode) or not _same_identity(before, held):
            os.close(fd)
            raise OracleError(f"regular file changed while it was opened: {relative}")
        return fd

    def verify_path(self) -> None:
        current = os.stat(self.path, follow_symlinks=False)
        if not _same_identity(self.stat, current):
            raise OracleError(f"source root path changed during build: {self.path}")

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self) -> "HeldDirectory":
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


@dataclass
class RunResult:
    argv: list[str]
    timeout_ms: int
    term_grace_ms: int
    stdout_limit_bytes: int
    stderr_limit_bytes: int
    stdout_bytes: int
    stderr_bytes: int
    stdout: bytes
    stderr: bytes
    started_at_utc: str
    finished_at_utc: str
    exit_code: int
    timed_out: bool
    output_truncated: bool
    process_group_reaped: bool


class HeldExecutable:
    """A regular executable retained through an open file descriptor."""

    def __init__(self, role: str, path: pathlib.Path, fd: int):
        self.role = role
        self.path = path
        self.fd = fd
        self.stat = os.fstat(fd)
        if not stat.S_ISREG(self.stat.st_mode) or self.stat.st_size == 0:
            raise OracleError(f"tool {role!r} must be a non-empty regular file: {path}")
        self.sha256 = _sha256_fd(fd)

    @classmethod
    def open(cls, role: str, path: pathlib.Path) -> "HeldExecutable":
        resolved = path.resolve(strict=True)
        fd = os.open(resolved, os.O_RDONLY | os.O_CLOEXEC)
        try:
            held = cls(role, resolved, fd)
            if not _same_identity(held.stat, os.stat(resolved, follow_symlinks=False)):
                raise OracleError(f"tool {role!r} changed while it was opened: {resolved}")
            return held
        except BaseException:
            os.close(fd)
            raise

    @classmethod
    def from_fd(cls, role: str, path: pathlib.Path, fd: int) -> "HeldExecutable":
        resolved = path.resolve(strict=True)
        duplicate = os.dup(fd)
        try:
            held = cls(role, resolved, duplicate)
            if not _same_identity(held.stat, os.stat(resolved, follow_symlinks=False)):
                raise OracleError(f"bootstrap {role!r} path does not match its held FD: {resolved}")
            return held
        except BaseException:
            os.close(duplicate)
            raise

    def evidence(self, version: str) -> dict[str, object]:
        if not version or len(version.encode("utf-8")) > VERSION_OUTPUT_LIMIT:
            raise OracleError(f"invalid version evidence for tool {self.role!r}")
        return {
            "role": self.role,
            "path": str(self.path),
            "version": version,
            "sha256": self.sha256,
            "identity": _file_identity(self.stat),
            "held_fd": True,
        }

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self) -> "HeldExecutable":
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


def _enable_subreaper() -> None:
    if sys.platform != "linux":
        raise OracleError("Oracle controller requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        error = ctypes.get_errno()
        raise OracleError(f"failed to enable child subreaper: {os.strerror(error)}")


def _write_proc_mapping(path: str, value: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CLOEXEC)
    try:
        os.write(fd, value.encode("ascii"))
    finally:
        os.close(fd)


def _readonly_mount_namespace_setup(
    bindings: Sequence[tuple[int, bytes]], host_uid: int, host_gid: int
):
    clone_newns = 0x00020000
    clone_newuser = 0x10000000
    ms_rdonly = 1
    ms_nosuid = 2
    ms_nodev = 4
    ms_remount = 32
    ms_bind = 4096
    ms_rec = 16384
    ms_private = 1 << 18
    pr_set_securebits = 28
    pr_set_no_new_privs = 38
    secure_noroot = 1 | 2
    secure_no_setuid_fixup = 4 | 8
    linux_capability_version_3 = 0x20080522

    class CapabilityHeader(ctypes.Structure):
        _fields_ = [("version", ctypes.c_uint32), ("pid", ctypes.c_int)]

    class CapabilityData(ctypes.Structure):
        _fields_ = [
            ("effective", ctypes.c_uint32),
            ("permitted", ctypes.c_uint32),
            ("inheritable", ctypes.c_uint32),
        ]

    libc = ctypes.CDLL(None, use_errno=True)

    def checked(result: int, operation: str) -> None:
        if result != 0:
            error = ctypes.get_errno()
            raise OSError(error, f"{operation}: {os.strerror(error)}")

    checked(libc.unshare(clone_newuser | clone_newns), "unshare user+mount namespace")
    try:
        _write_proc_mapping("/proc/self/setgroups", "deny")
    except FileNotFoundError:
        pass
    _write_proc_mapping("/proc/self/uid_map", f"0 {host_uid} 1")
    _write_proc_mapping("/proc/self/gid_map", f"0 {host_gid} 1")
    checked(libc.mount(None, b"/", None, ms_rec | ms_private, None), "make mounts private")
    for directory_fd, target in bindings:
        checked(libc.mount(target, target, None, ms_bind, None), "bind tool directory")
        checked(
            libc.mount(
                None,
                target,
                None,
                ms_bind | ms_remount | ms_rdonly | ms_nosuid | ms_nodev,
                None,
            ),
            "remount tool directory read-only",
        )
        os.close(directory_fd)
    checked(
        libc.prctl(
            pr_set_securebits,
            secure_noroot | secure_no_setuid_fixup,
            0,
            0,
            0,
        ),
        "lock namespace securebits",
    )
    header = CapabilityHeader(linux_capability_version_3, 0)
    data = (CapabilityData * 2)()
    checked(libc.capset(ctypes.byref(header), ctypes.byref(data)), "drop capabilities")
    checked(libc.prctl(pr_set_no_new_privs, 1, 0, 0, 0), "set no_new_privs")


def _signal_group(pid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _reap_descendants(group_id: int, deadline: float) -> bool:
    while time.monotonic() < deadline:
        reaped = False
        while True:
            try:
                child, _status = os.waitpid(-group_id, os.WNOHANG)
            except ChildProcessError:
                break
            if child <= 0:
                break
            reaped = True
        try:
            os.killpg(group_id, 0)
        except ProcessLookupError:
            return True
        if not reaped:
            time.sleep(0.01)
    return False


def run_bounded(
    executable: HeldExecutable,
    argv: Sequence[str],
    *,
    env: Mapping[str, str],
    timeout_ms: int,
    term_grace_ms: int,
    stdout_limit_bytes: int,
    stderr_limit_bytes: int,
    extra_fds: Iterable[int] = (),
    readonly_bind_paths: Sequence[pathlib.Path] = (),
) -> RunResult:
    """Run one held executable in a new process group with bounded evidence."""
    if not argv or any(not isinstance(value, str) or not value for value in argv):
        raise OracleError("command argv must contain non-empty strings")
    if timeout_ms <= 0 or term_grace_ms <= 0 or term_grace_ms >= timeout_ms:
        raise OracleError("command deadline and TERM grace are invalid")
    if stdout_limit_bytes <= 0 or stderr_limit_bytes <= 0:
        raise OracleError("command output limits must be positive")
    if executable.fd < 0:
        raise OracleError("held executable is closed")

    _enable_subreaper()
    started_at = _utc_now()
    started = time.monotonic()
    readonly_bindings: list[tuple[int, bytes]] = []
    for path in readonly_bind_paths:
        resolved = path.resolve(strict=True)
        directory_fd = os.open(
            resolved, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        metadata = os.fstat(directory_fd)
        if not stat.S_ISDIR(metadata.st_mode):
            os.close(directory_fd)
            raise OracleError(f"read-only bind target is not a directory: {resolved}")
        readonly_bindings.append((directory_fd, os.fsencode(resolved)))
    passed_fds = tuple(
        dict.fromkeys((executable.fd, *extra_fds, *(fd for fd, _ in readonly_bindings)))
    )
    preexec_fn = None
    if readonly_bindings:
        host_uid = os.getuid()
        host_gid = os.getgid()

        def enter_namespace() -> None:
            try:
                _readonly_mount_namespace_setup(readonly_bindings, host_uid, host_gid)
            except BaseException as error:
                os.write(2, f"Oracle namespace setup failed: {error}\n".encode("utf-8"))
                raise

        preexec_fn = enter_namespace
    try:
        process = subprocess.Popen(
            list(argv),
            executable=f"/proc/self/fd/{executable.fd}",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=dict(env),
            close_fds=True,
            pass_fds=passed_fds,
            start_new_session=True,
            preexec_fn=preexec_fn,
        )
    finally:
        for directory_fd, _target in readonly_bindings:
            try:
                os.close(directory_fd)
            except OSError as error:
                if error.errno != errno.EBADF:
                    raise
    assert process.stdout is not None and process.stderr is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, ("stdout", stdout_limit_bytes))
    selector.register(process.stderr, selectors.EVENT_READ, ("stderr", stderr_limit_bytes))
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    counts = {"stdout": 0, "stderr": 0}
    timed_out = False
    deadline = started + timeout_ms / 1000

    while selector.get_map():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out = True
            _signal_group(process.pid, signal.SIGTERM)
            term_deadline = time.monotonic() + term_grace_ms / 1000
            while time.monotonic() < term_deadline and process.poll() is None:
                time.sleep(0.01)
            if process.poll() is None:
                _signal_group(process.pid, signal.SIGKILL)
            break
        events = selector.select(max(0.0, min(remaining, 0.05)))
        for key, _mask in events:
            name, limit = key.data
            chunk = os.read(key.fileobj.fileno(), 64 * 1024)
            if not chunk:
                selector.unregister(key.fileobj)
                key.fileobj.close()
                continue
            counts[name] += len(chunk)
            capacity = limit - len(buffers[name])
            if capacity > 0:
                buffers[name].extend(chunk[:capacity])
        if process.poll() is not None and not events:
            # Pipes can still have buffered data, so continue until EOF.
            continue

    if timed_out:
        _signal_group(process.pid, signal.SIGKILL)
    try:
        exit_code = process.wait(timeout=max(1.0, term_grace_ms / 1000))
    except subprocess.TimeoutExpired as error:
        _signal_group(process.pid, signal.SIGKILL)
        process.wait()
        raise OracleError("command could not be reaped after SIGKILL") from error

    for stream, name, limit in [
        (process.stdout, "stdout", stdout_limit_bytes),
        (process.stderr, "stderr", stderr_limit_bytes),
    ]:
        if stream.closed:
            continue
        while True:
            chunk = os.read(stream.fileno(), 64 * 1024)
            if not chunk:
                break
            counts[name] += len(chunk)
            capacity = limit - len(buffers[name])
            if capacity > 0:
                buffers[name].extend(chunk[:capacity])
        stream.close()

    if timed_out:
        group_reaped = _reap_descendants(process.pid, time.monotonic() + 2.0)
    else:
        group_reaped = _reap_descendants(process.pid, time.monotonic() + 0.2)
        if not group_reaped:
            _signal_group(process.pid, signal.SIGKILL)
            group_reaped = _reap_descendants(process.pid, time.monotonic() + 2.0)
    if not group_reaped:
        raise OracleError("command process group was not fully reaped")

    return RunResult(
        argv=list(argv),
        timeout_ms=timeout_ms,
        term_grace_ms=term_grace_ms,
        stdout_limit_bytes=stdout_limit_bytes,
        stderr_limit_bytes=stderr_limit_bytes,
        stdout_bytes=counts["stdout"],
        stderr_bytes=counts["stderr"],
        stdout=bytes(buffers["stdout"]),
        stderr=bytes(buffers["stderr"]),
        started_at_utc=started_at,
        finished_at_utc=_utc_now(),
        exit_code=exit_code,
        timed_out=timed_out,
        output_truncated=(
            counts["stdout"] > stdout_limit_bytes or counts["stderr"] > stderr_limit_bytes
        ),
        process_group_reaped=group_reaped,
    )


class FrozenToolDirectory:
    def __init__(self, path: pathlib.Path, aliases: Mapping[str, str]):
        self.path = path
        self.aliases = dict(aliases)

    @classmethod
    def create(
        cls, path: pathlib.Path, aliases: Mapping[str, HeldExecutable]
    ) -> "FrozenToolDirectory":
        path.mkdir(mode=0o700)
        expected: dict[str, str] = {}
        for alias, tool in aliases.items():
            if not alias or "/" in alias or alias in {".", ".."}:
                raise OracleError(f"invalid controlled tool alias: {alias!r}")
            target = f"/proc/self/fd/{tool.fd}"
            os.symlink(target, path / alias)
            expected[alias] = target
        directory = cls(path, expected)
        directory._verify_entries()
        os.chmod(path, 0o500)
        directory.verify_frozen()
        return directory

    def _verify_entries(self) -> None:
        actual: dict[str, str] = {}
        with os.scandir(self.path) as entries:
            for entry in entries:
                metadata = entry.stat(follow_symlinks=False)
                if not stat.S_ISLNK(metadata.st_mode):
                    raise OracleError(f"controlled tool alias is not a symlink: {entry.name}")
                actual[entry.name] = os.readlink(entry.path)
        if actual != self.aliases:
            raise OracleError("controlled tool directory changed after population")

    def verify_frozen(self) -> None:
        metadata = os.stat(self.path, follow_symlinks=False)
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o500:
            raise OracleError("controlled tool directory must remain frozen at mode 0500")
        self._verify_entries()

    def remove(self) -> None:
        os.chmod(self.path, 0o700)
        shutil.rmtree(self.path)


@dataclass(frozen=True)
class ArtifactLimits:
    max_count: int = 4096
    max_file_bytes: int = 256 * 1024 * 1024
    max_total_bytes: int = 1024 * 1024 * 1024


def _tree_entries(
    root: HeldDirectory,
) -> tuple[dict[str, tuple[object, ...]], set[str]]:
    entries: dict[str, tuple[object, ...]] = {}
    directories: set[str] = set()
    pending = [(os.dup(root.fd), "")]
    while pending:
        directory_fd, prefix = pending.pop()
        try:
            ordered = sorted(os.listdir(directory_fd), key=os.fsencode, reverse=True)
        except BaseException:
            os.close(directory_fd)
            raise
        for name in ordered:
            relative = f"{prefix}/{name}" if prefix else name
            if relative == ".git" or relative.startswith(".git/"):
                continue
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode):
                child_fd = os.open(
                    name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=directory_fd,
                )
                directories.add(relative)
                pending.append((child_fd, relative))
            elif stat.S_ISREG(metadata.st_mode):
                entries[relative] = (
                    "regular",
                    metadata.st_mode,
                    metadata.st_size,
                    metadata.st_mtime_ns,
                    metadata.st_ino,
                )
            elif stat.S_ISLNK(metadata.st_mode):
                entries[relative] = (
                    "symlink",
                    metadata.st_mode,
                    os.readlink(name, dir_fd=directory_fd),
                )
            else:
                raise OracleError(f"unsupported file type in source tree: {relative}")
        os.close(directory_fd)
    return entries, directories


def snapshot_tree(root: pathlib.Path) -> dict[str, tuple[object, ...]]:
    with HeldDirectory.open(root) as held:
        entries, _directories = _tree_entries(held)
        return entries


def _resolve_manifest_symlink(path: str, target: str) -> str:
    if not target or target.startswith("/") or "\\" in target:
        raise OracleError(f"artifact symlink {path!r} must use a relative target")
    parts = path.split("/")[:-1]
    for part in target.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if not parts:
                raise OracleError(f"artifact symlink {path!r} escapes the source root")
            parts.pop()
        else:
            parts.append(part)
    if not parts:
        raise OracleError(f"artifact symlink {path!r} has an empty resolved target")
    return "/".join(parts)


def scan_artifacts(
    root: pathlib.Path | HeldDirectory,
    baseline: Mapping[str, tuple[object, ...]],
    *,
    limits: ArtifactLimits = ArtifactLimits(),
) -> list[dict[str, object]]:
    owned_root = not isinstance(root, HeldDirectory)
    held = HeldDirectory.open(root) if owned_root else root
    try:
        current, _directories = _tree_entries(held)
        return _scan_artifacts_from_entries(held, baseline, current, limits)
    finally:
        if owned_root:
            held.close()


def _scan_artifacts_from_entries(
    root: HeldDirectory,
    baseline: Mapping[str, tuple[object, ...]],
    current: Mapping[str, tuple[object, ...]],
    limits: ArtifactLimits,
) -> list[dict[str, object]]:
    changed = [path for path, fingerprint in current.items() if baseline.get(path) != fingerprint]
    changed.sort(key=os.fsencode)
    if not changed:
        raise OracleError("artifact manifest is empty after build")
    if len(changed) > limits.max_count:
        raise OracleError(f"artifact manifest exceeds {limits.max_count} entries")

    manifest: list[dict[str, object]] = []
    total_bytes = 0
    changed_set = set(changed)
    for relative in changed:
        metadata = root.lstat(relative)
        if stat.S_ISREG(metadata.st_mode):
            metadata, sha256 = root.regular_evidence(relative)
            if metadata.st_size > limits.max_file_bytes:
                raise OracleError(f"artifact exceeds per-file byte bound: {relative}")
            total_bytes += metadata.st_size
            if total_bytes > limits.max_total_bytes:
                raise OracleError("artifact manifest exceeds total byte bound")
            manifest.append(
                {
                    "kind": "regular",
                    "path": relative,
                    "mode": metadata.st_mode,
                    "size": metadata.st_size,
                    "sha256": sha256,
                }
            )
        elif stat.S_ISLNK(metadata.st_mode):
            target = root.readlink(relative)
            resolved = _resolve_manifest_symlink(relative, target)
            if resolved not in changed_set:
                raise OracleError(
                    f"artifact symlink {relative!r} does not resolve to a manifest entry"
                )
            manifest.append(
                {
                    "kind": "symlink",
                    "path": relative,
                    "mode": metadata.st_mode,
                    "target": target,
                }
            )
        else:
            raise OracleError(f"unsupported artifact kind: {relative}")

    entries = {entry["path"]: entry for entry in manifest}
    for entry in manifest:
        if entry["kind"] != "symlink":
            continue
        current_path = str(entry["path"])
        visited: set[str] = set()
        for _depth in range(9):
            if current_path in visited:
                raise OracleError(f"artifact symlink cycle at {entry['path']!r}")
            visited.add(current_path)
            target_entry = entries.get(current_path)
            if target_entry is None:
                raise OracleError(f"artifact symlink target is absent: {current_path!r}")
            if target_entry["kind"] == "regular":
                break
            current_path = _resolve_manifest_symlink(
                str(target_entry["path"]), str(target_entry["target"])
            )
        else:
            raise OracleError(f"artifact symlink depth exceeds eight: {entry['path']!r}")
    return manifest


def canonical_json_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def publish_candidate(path: pathlib.Path, document: Mapping[str, object]) -> None:
    path = path.absolute()
    if path.name.endswith("provenance.json") or "provenance" in path.name.lower():
        raise OracleError("primary controller must never publish final provenance")
    parent = path.parent.resolve(strict=True)
    if path.exists() or path.is_symlink():
        raise OracleError(f"candidate metadata already exists: {path}")
    payload = canonical_json_bytes(document)
    temporary = parent / f".{path.name}.candidate-{os.getpid()}-{time.monotonic_ns()}"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
    linked = False
    completed = False
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
        os.close(fd)
        fd = -1
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as error:
            raise OracleError(f"candidate metadata already exists: {path}") from error
        linked = True
        directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        completed = True
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        if linked and not completed:
            try:
                path.unlink()
            except FileNotFoundError:
                pass


REQUIRED_TOOL_PATHS: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    ("git", ("/usr/bin/git",), ("--version",)),
    ("shell", ("/usr/bin/bash", "/bin/bash"), ("--version",)),
    ("make", ("/usr/bin/make",), ("--version",)),
    ("cc", ("/usr/bin/cc",), ("--version",)),
    ("ld", ("/usr/bin/ld",), ("--version",)),
    ("ar", ("/usr/bin/ar",), ("--version",)),
    ("ranlib", ("/usr/bin/ranlib",), ("--version",)),
)

UTILITY_ALIASES = (
    "as",
    "awk",
    "basename",
    "cat",
    "chmod",
    "cmp",
    "cp",
    "cut",
    "date",
    "dirname",
    "echo",
    "env",
    "expr",
    "find",
    "grep",
    "head",
    "install",
    "ln",
    "md5sum",
    "mkdir",
    "mv",
    "nm",
    "objcopy",
    "perl",
    "pkg-config",
    "printf",
    "pwd",
    "readlink",
    "realpath",
    "rm",
    "sed",
    "sh",
    "sort",
    "strip",
    "tail",
    "tar",
    "touch",
    "tr",
    "uname",
    "wc",
    "which",
    "xargs",
)


def _fixed_tool_path(candidates: Sequence[str]) -> pathlib.Path:
    for candidate in candidates:
        path = pathlib.Path(candidate)
        try:
            resolved = path.resolve(strict=True)
        except FileNotFoundError:
            continue
        metadata = os.stat(resolved, follow_symlinks=False)
        if stat.S_ISREG(metadata.st_mode) and metadata.st_mode & 0o111:
            return resolved
    raise OracleError(f"none of the fixed tool candidates is usable: {candidates!r}")


def _command_text(result: RunResult, role: str) -> str:
    if result.timed_out or result.output_truncated or result.exit_code != 0:
        raise OracleError(
            f"tool {role!r} identity command failed: exit={result.exit_code} "
            f"timeout={result.timed_out} truncated={result.output_truncated}"
        )
    text = (result.stdout + result.stderr).decode("utf-8", "replace").strip()
    if not text:
        raise OracleError(f"tool {role!r} returned empty version evidence")
    return text


def _empty_directory(path: pathlib.Path, field: str) -> None:
    if any(path.iterdir()):
        raise OracleError(f"isolated {field} directory is not empty: {path}")


def _git_text(
    git: HeldExecutable,
    source: pathlib.Path | HeldDirectory,
    args: Sequence[str],
    env: Mapping[str, str],
) -> str:
    source_path = (
        f"/proc/self/fd/{source.fd}" if isinstance(source, HeldDirectory) else str(source)
    )
    source_fds = (source.fd,) if isinstance(source, HeldDirectory) else ()
    result = run_bounded(
        git,
        ["git", "-C", source_path, *args],
        env=env,
        timeout_ms=COMMAND_TIMEOUT_MS,
        term_grace_ms=1_000,
        stdout_limit_bytes=1024 * 1024,
        stderr_limit_bytes=1024 * 1024,
        extra_fds=source_fds,
    )
    if result.timed_out or result.output_truncated or result.exit_code != 0:
        raise OracleError(
            f"controlled git command failed: {' '.join(args)}; "
            f"exit={result.exit_code}; stderr={result.stderr.decode('utf-8', 'replace')}"
        )
    return result.stdout.decode("utf-8", "strict").strip()


def _validate_source(
    source: HeldDirectory, git: HeldExecutable, env: Mapping[str, str]
) -> dict[str, object]:
    head = _git_text(git, source, ["rev-parse", "HEAD"], env)
    tag_commit = _git_text(git, source, ["rev-parse", f"{REDIS_TAG}^{{commit}}"], env)
    status_output = _git_text(
        git, source, ["status", "--porcelain=v1", "--untracked-files=all"], env
    )
    repository = _git_text(git, source, ["remote", "get-url", "origin"], env)
    if head != REDIS_COMMIT or tag_commit != REDIS_COMMIT:
        raise OracleError(
            f"source must be exact Redis {REDIS_TAG} commit {REDIS_COMMIT}; "
            f"HEAD={head!r}, tag={tag_commit!r}"
        )
    if repository != REDIS_REPOSITORY:
        raise OracleError(f"source origin must equal {REDIS_REPOSITORY}, got {repository!r}")
    if status_output:
        raise OracleError("source checkout must be tracked/untracked clean before build")
    git_dir_metadata = source.lstat(".git")
    if not stat.S_ISDIR(git_dir_metadata.st_mode):
        raise OracleError("source .git must be a real directory below source root")
    git_dir = source.path / ".git"
    return {
        "repository": REDIS_REPOSITORY,
        "tag": REDIS_TAG,
        "commit": REDIS_COMMIT,
        "head": head,
        "tag_commit": tag_commit,
        "root_path": str(source.path),
        "git_dir_path": str(git_dir),
        "tracked_untracked_clean": True,
    }


def _validate_pristine_source_tree(
    source: HeldDirectory,
    git: HeldExecutable,
    env: Mapping[str, str],
) -> dict[str, tuple[object, ...]]:
    tracked = set(_git_text(git, source, ["ls-files", "--cached"], env).splitlines())
    if not tracked:
        raise OracleError("exact source checkout has no tracked files")
    allowed_directories: set[str] = set()
    for path in tracked:
        parts = path.split("/")
        if not path or any(part in {"", ".", ".."} for part in parts):
            raise OracleError(f"git returned a non-canonical tracked path: {path!r}")
        for index in range(1, len(parts)):
            allowed_directories.add("/".join(parts[:index]))

    entries, directories = _tree_entries(source)
    extra_entries = sorted(set(entries).difference(tracked), key=os.fsencode)
    extra_directories = sorted(directories.difference(allowed_directories), key=os.fsencode)
    if extra_entries or extra_directories:
        extras = [*extra_entries, *(f"{path}/" for path in extra_directories)]
        raise OracleError(f"pre-build artifact manifest is not empty: {extras}")
    missing = sorted(tracked.difference(entries), key=os.fsencode)
    if missing:
        raise OracleError(f"tracked source entries are missing: {missing}")
    return entries


def _sanitized_environment(
    tool_directory: pathlib.Path, home: pathlib.Path, temporary: pathlib.Path
) -> dict[str, str]:
    return {
        "PATH": str(tool_directory),
        "HOME": str(home),
        "TMPDIR": str(temporary),
        "TMP": str(temporary),
        "TEMP": str(temporary),
        "LC_ALL": "C",
        "LANG": "C",
        "TZ": "UTC",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "CCACHE_DISABLE": "1",
        "SCCACHE_DISABLE": "1",
        "SOURCE_DATE_EPOCH": str(SOURCE_DATE_EPOCH),
        "CC": "cc",
        "LD": "ld",
        "AR": "ar",
        "RANLIB": "ranlib",
        "MAKEFLAGS": "",
    }


def _register_tools(
    controller: HeldExecutable, python: HeldExecutable
) -> tuple[list[HeldExecutable], dict[str, HeldExecutable], dict[str, tuple[str, ...]]]:
    tools = [controller, python]
    aliases: dict[str, HeldExecutable] = {"python3": python}
    versions: dict[str, tuple[str, ...]] = {
        "controller": (),
        "python": ("--version",),
    }
    by_path = {controller.path: controller, python.path: python}
    for role, candidates, version_args in REQUIRED_TOOL_PATHS:
        path = _fixed_tool_path(candidates)
        if path in by_path:
            raise OracleError(f"required tool paths must be unique: {path}")
        tool = HeldExecutable.open(role, path)
        tools.append(tool)
        by_path[path] = tool
        versions[role] = version_args
        aliases["bash" if role == "shell" else role] = tool
        if role == "shell":
            aliases["sh"] = tool

    for alias in UTILITY_ALIASES:
        if alias in aliases:
            continue
        candidates = (f"/usr/bin/{alias}", f"/bin/{alias}")
        try:
            path = _fixed_tool_path(candidates)
        except OracleError:
            continue
        tool = by_path.get(path)
        if tool is None:
            role = f"utility-{alias}"
            tool = HeldExecutable.open(role, path)
            tools.append(tool)
            by_path[path] = tool
            versions[role] = ("--version",)
        aliases[alias] = tool
    if len(tools) > 64:
        raise OracleError("controlled tool registry exceeds schema limit")
    return tools, aliases, versions


def _tool_evidence(
    tools: Sequence[HeldExecutable],
    version_args: Mapping[str, tuple[str, ...]],
    env: Mapping[str, str],
) -> list[dict[str, object]]:
    evidence = []
    for tool in tools:
        if tool.role == "controller":
            version = "kiwi Redis Oracle controller v3"
        elif tool.role.startswith("utility-"):
            version = f"identity-only sha256:{tool.sha256}"
        else:
            args = version_args[tool.role]
            result = run_bounded(
                tool,
                [tool.role, *args],
                env=env,
                timeout_ms=COMMAND_TIMEOUT_MS,
                term_grace_ms=1_000,
                stdout_limit_bytes=VERSION_OUTPUT_LIMIT,
                stderr_limit_bytes=VERSION_OUTPUT_LIMIT,
            )
            version = _command_text(result, tool.role)
        evidence.append(tool.evidence(version))
    return evidence


def _recipe() -> dict[str, object]:
    return {
        "id": RECIPE_ID,
        "build_tls": "no",
        "malloc": "libc",
        "debug": "",
        "debug_flags": "",
        "enable_lto": "",
        "opt": "-O3 -fno-omit-frame-pointer",
        "jobs": 1,
        "source_date_epoch": SOURCE_DATE_EPOCH,
        "argv": BUILD_ARGV,
    }


def _assert_no_checkout_path_in_binary(
    source_root: HeldDirectory, binary: str
) -> None:
    marker = str(source_root.path).encode("utf-8")
    overlap = max(0, len(marker) - 1)
    tail = b""
    fd = source_root.open_regular(binary)
    try:
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            data = tail + chunk
            if marker in data:
                raise OracleError("release redis-server contains checkout path in binary/DWARF")
            tail = data[-overlap:] if overlap else b""
    finally:
        os.close(fd)


def build_primary(
    source_argument: str,
    metadata_argument: str,
    bootstrap_python_path: pathlib.Path,
    bootstrap_python_fd: int,
    bootstrap_controller_path: pathlib.Path,
    bootstrap_controller_fd: int,
) -> None:
    if not sys.flags.isolated or sys.dont_write_bytecode is False:
        raise OracleError("controller must run with Python -I -B")
    if sys.platform != "linux" or os.uname().machine not in {"x86_64", "amd64"}:
        raise OracleError("Redis Oracle build supports Linux x86_64 only")
    source_input = pathlib.Path(source_argument)
    metadata = pathlib.Path(metadata_argument)
    if not source_input.is_absolute() or not metadata.is_absolute():
        raise OracleError("--source and --metadata must be absolute paths")
    source = source_input.resolve(strict=True)
    if not source.is_dir() or source.is_symlink():
        raise OracleError("--source must resolve to a real directory")
    metadata_parent = metadata.parent.resolve(strict=True)
    if metadata.exists() or metadata.is_symlink():
        raise OracleError(f"candidate metadata already exists: {metadata}")
    if metadata_parent == source or source in metadata_parent.parents:
        raise OracleError("candidate metadata must be outside the source checkout")

    source_root = HeldDirectory.open(source)
    controller = HeldExecutable.from_fd(
        "controller", bootstrap_controller_path, bootstrap_controller_fd
    )
    python = HeldExecutable.from_fd("python", bootstrap_python_path, bootstrap_python_fd)
    tools: list[HeldExecutable] = []
    aliases_directory: FrozenToolDirectory | None = None
    runtime = pathlib.Path(
        tempfile.mkdtemp(prefix=".kiwi-oracle-primary-", dir=metadata_parent)
    )
    try:
        home = runtime / "home"
        temporary = runtime / "tmp"
        tool_path = runtime / "tools"
        home.mkdir(mode=0o700)
        temporary.mkdir(mode=0o700)
        _empty_directory(home, "HOME")
        _empty_directory(temporary, "TMPDIR")

        tools, aliases, versions = _register_tools(controller, python)
        aliases_directory = FrozenToolDirectory.create(tool_path, aliases)
        env = _sanitized_environment(aliases_directory.path, home, temporary)
        tool_evidence = _tool_evidence(tools, versions, env)
        git = next(tool for tool in tools if tool.role == "git")
        source_evidence = _validate_source(source_root, git, env)
        baseline = _validate_pristine_source_tree(source_root, git, env)

        source_fd = os.dup(source_root.fd)
        started_at = _utc_now()
        try:
            make = next(tool for tool in tools if tool.role == "make")
            shell = next(tool for tool in tools if tool.role == "shell")
            actual_argv = [
                "make",
                "-C",
                f"/proc/self/fd/{source_fd}",
                f"SHELL=/proc/self/fd/{shell.fd}",
                *BUILD_ARGV[4:],
            ]
            result = run_bounded(
                make,
                actual_argv,
                env=env,
                timeout_ms=BUILD_TIMEOUT_MS,
                term_grace_ms=TERM_GRACE_MS,
                stdout_limit_bytes=BUILD_OUTPUT_LIMIT,
                stderr_limit_bytes=BUILD_OUTPUT_LIMIT,
                extra_fds=(source_fd, *(tool.fd for tool in tools)),
                readonly_bind_paths=(aliases_directory.path,),
            )
        finally:
            os.close(source_fd)
        if result.timed_out or result.output_truncated or result.exit_code != 0:
            raise OracleError(
                "controlled Redis build failed: "
                f"exit={result.exit_code}, timeout={result.timed_out}, "
                f"truncated={result.output_truncated}\n"
                f"stdout:\n{result.stdout.decode('utf-8', 'replace')}\n"
                f"stderr:\n{result.stderr.decode('utf-8', 'replace')}"
            )
        aliases_directory.verify_frozen()
        _empty_directory(home, "HOME")
        _empty_directory(temporary, "TMPDIR")
        artifacts = scan_artifacts(source_root, baseline)
        redis_path = source / "src/redis-server"
        redis_metadata, redis_sha256 = source_root.regular_evidence("src/redis-server")
        if not stat.S_ISREG(redis_metadata.st_mode) or not redis_metadata.st_mode & 0o111:
            raise OracleError("build did not produce executable src/redis-server")
        redis_entry = next(
            (
                entry
                for entry in artifacts
                if entry["kind"] == "regular" and entry["path"] == "src/redis-server"
            ),
            None,
        )
        if redis_entry is None:
            raise OracleError("artifact manifest does not contain regular src/redis-server")
        if redis_entry["sha256"] != redis_sha256:
            raise OracleError("src/redis-server changed after artifact manifest capture")
        _assert_no_checkout_path_in_binary(source_root, "src/redis-server")
        source_root.verify_path()
        document = {
            "schema_version": BUILD_SCHEMA,
            "source": source_evidence,
            "recipe": _recipe(),
            "tools": tool_evidence,
            "artifacts": artifacts,
            "redis_server": {
                "artifact_path": "src/redis-server",
                "path": str(redis_path),
                "sha256": redis_entry["sha256"],
                "identity": _file_identity(redis_metadata),
            },
            "started_at_utc": started_at,
            "finished_at_utc": _utc_now(),
        }
    finally:
        if aliases_directory is not None and aliases_directory.path.exists():
            aliases_directory.remove()
        for tool in tools:
            tool.close()
        if controller not in tools:
            controller.close()
        if python not in tools:
            python.close()
        source_root.close()
        shutil.rmtree(runtime)
        if runtime.exists():
            raise OracleError(f"primary runtime cleanup failed: {runtime}")

    publish_candidate(metadata, document)
    print(str(metadata))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build exact Redis 8.8.1 primary Oracle candidate evidence"
    )
    parser.add_argument("--bootstrap-python-path", required=True, help=argparse.SUPPRESS)
    parser.add_argument("--bootstrap-python-fd", type=int, required=True, help=argparse.SUPPRESS)
    parser.add_argument("--bootstrap-controller-path", required=True, help=argparse.SUPPRESS)
    parser.add_argument("--bootstrap-controller-fd", type=int, required=True, help=argparse.SUPPRESS)
    parser.add_argument("--source", required=True, help="absolute exact Redis 8.8.1 checkout")
    parser.add_argument("--metadata", required=True, help="absolute candidate build metadata path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        build_primary(
            arguments.source,
            arguments.metadata,
            pathlib.Path(arguments.bootstrap_python_path),
            arguments.bootstrap_python_fd,
            pathlib.Path(arguments.bootstrap_controller_path),
            arguments.bootstrap_controller_fd,
        )
    except (OracleError, OSError, UnicodeError, ValueError) as error:
        print(f"oracle build rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
