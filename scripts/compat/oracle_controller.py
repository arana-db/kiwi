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
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable, Mapping, Sequence

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
CONTROLLED_PATH_FD = 198
MAX_JSON_BYTES = 1024 * 1024


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


def _same_directory_object(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        stat.S_IFMT(left.st_mode),
    ) == (
        right.st_dev,
        right.st_ino,
        stat.S_IFMT(right.st_mode),
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


def _git_blob_oid_fd(fd: int, size: int) -> str:
    offset = os.lseek(fd, 0, os.SEEK_CUR)
    os.lseek(fd, 0, os.SEEK_SET)
    digest = hashlib.sha1(usedforsecurity=False)
    digest.update(f"blob {size}\0".encode("ascii"))
    while True:
        chunk = os.read(fd, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
    os.lseek(fd, offset, os.SEEK_SET)
    return digest.hexdigest()


def _git_blob_oid_bytes(content: bytes) -> str:
    digest = hashlib.sha1(usedforsecurity=False)
    digest.update(f"blob {len(content)}\0".encode("ascii"))
    digest.update(content)
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

    @classmethod
    def open_absolute_nofollow(cls, path: pathlib.Path) -> "HeldDirectory":
        if not path.is_absolute():
            raise OracleError(f"directory path must be absolute: {path}")
        parts = path.parts
        if not parts or parts[0] != os.sep or any(
            part in {"", ".", ".."} for part in parts[1:]
        ):
            raise OracleError(f"directory path must be canonical: {path}")
        fd = os.open(
            os.sep, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        try:
            for part in parts[1:]:
                next_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=fd,
                )
                os.close(fd)
                fd = next_fd
            return cls(path, fd)
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

    def open_directory(self, relative: str) -> "HeldDirectory":
        directory_fd, name = self._parent_fd(relative)
        try:
            fd = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory_fd,
            )
        finally:
            os.close(directory_fd)
        try:
            held = HeldDirectory(self.path / relative, fd)
            before = self.lstat(relative)
            if not _same_identity(before, held.stat):
                raise OracleError(f"directory changed while it was opened: {relative}")
            return held
        except BaseException:
            os.close(fd)
            raise

    def readlink(self, relative: str) -> str:
        directory_fd, name = self._parent_fd(relative)
        try:
            return os.readlink(name, dir_fd=directory_fd)
        finally:
            os.close(directory_fd)

    def readlink_bytes(self, relative: str) -> bytes:
        directory_fd, name = self._parent_fd(relative)
        try:
            return os.readlink(os.fsencode(name), dir_fd=directory_fd)
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


class CandidateTarget:
    """A candidate basename bound to one held, no-follow parent directory."""

    def __init__(
        self,
        path: pathlib.Path,
        parent_path: pathlib.Path,
        basename: str,
        parent: HeldDirectory,
    ):
        self.path = path
        self.parent_path = parent_path
        self.basename = basename
        self.parent = parent

    @classmethod
    def open(cls, argument: str | pathlib.Path) -> "CandidateTarget":
        raw = os.fspath(argument)
        if not os.path.isabs(raw):
            raise OracleError("candidate metadata path must be absolute")
        basename = os.path.basename(raw)
        if (
            not basename
            or basename in {".", ".."}
            or "/" in basename
            or "\\" in basename
        ):
            raise OracleError(f"invalid candidate metadata basename: {basename!r}")
        parent_raw = os.path.dirname(raw)
        if os.path.normpath(raw) != raw:
            raise OracleError("candidate metadata path must be canonical")
        parent_path = pathlib.Path(parent_raw)
        parent = HeldDirectory.open_absolute_nofollow(parent_path)
        target = cls(pathlib.Path(raw), parent_path, basename, parent)
        try:
            target.reject_existing()
            return target
        except BaseException:
            parent.close()
            raise

    def reject_existing(self) -> None:
        try:
            os.stat(self.basename, dir_fd=self.parent.fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        raise OracleError(f"candidate metadata already exists: {self.path}")

    def verify_visible_parent(self) -> None:
        visible = HeldDirectory.open_absolute_nofollow(self.parent_path)
        try:
            if not _same_directory_object(self.parent.stat, visible.stat):
                raise OracleError(
                    f"candidate metadata parent changed during build: {self.parent_path}"
                )
        finally:
            visible.close()

    def close(self) -> None:
        self.parent.close()


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
    directories: Sequence["FrozenToolDirectory"], host_uid: int, host_gid: int
):
    clone_newns = 0x00020000
    clone_newuser = 0x10000000
    at_empty_path = 0x1000
    fsopen_cloexec = 1
    fsconfig_set_string = 1
    fsconfig_cmd_create = 6
    fsmount_cloexec = 1
    mount_attr_rdonly = 1
    mount_attr_nosuid = 2
    mount_attr_nodev = 4
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

    class MountAttr(ctypes.Structure):
        _fields_ = [
            ("attr_set", ctypes.c_uint64),
            ("attr_clr", ctypes.c_uint64),
            ("propagation", ctypes.c_uint64),
            ("userns_fd", ctypes.c_uint64),
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
    if len(directories) != 1:
        raise OracleError("exactly one controlled PATH directory is required")
    directory = directories[0]
    fs_context = libc.syscall(430, b"tmpfs", fsopen_cloexec)
    if fs_context < 0:
        checked(-1, "fsopen controlled PATH tmpfs")
    mount_fd = -1
    root_fd = -1
    try:
        checked(
            libc.syscall(
                431,
                fs_context,
                fsconfig_set_string,
                b"mode",
                b"0700",
                0,
            ),
            "configure controlled PATH tmpfs",
        )
        checked(
            libc.syscall(431, fs_context, fsconfig_cmd_create, None, None, 0),
            "create controlled PATH tmpfs",
        )
        mount_fd = libc.syscall(432, fs_context, fsmount_cloexec, 0)
        if mount_fd < 0:
            checked(-1, "fsmount controlled PATH tmpfs")
        root_fd = os.open(
            ".", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC, dir_fd=mount_fd
        )
        for alias, target in directory.aliases.items():
            os.symlink(target, alias, dir_fd=root_fd)
        os.fchmod(root_fd, 0o500)
        FrozenToolDirectory.verify_alias_fd(root_fd, directory.aliases)
        attributes = MountAttr(
            mount_attr_rdonly | mount_attr_nosuid | mount_attr_nodev, 0, 0, 0
        )
        checked(
            libc.syscall(
                442,
                mount_fd,
                b"",
                at_empty_path,
                ctypes.byref(attributes),
                ctypes.sizeof(attributes),
            ),
            "make controlled PATH tmpfs read-only",
        )
        if not os.statvfs(root_fd).f_flag & os.ST_RDONLY:
            raise OracleError("controlled PATH tmpfs is not read-only")
        os.dup2(root_fd, directory.child_fd, inheritable=True)
        FrozenToolDirectory.verify_alias_fd(directory.child_fd, directory.aliases)
    finally:
        if root_fd >= 0:
            os.close(root_fd)
        if mount_fd >= 0:
            os.close(mount_fd)
        os.close(fs_context)
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
    readonly_bind_directories: Sequence["FrozenToolDirectory"] = (),
    cwd: pathlib.Path | None = None,
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
    for directory in readonly_bind_directories:
        directory.verify_frozen()
        if directory.child_fd in (executable.fd, *extra_fds):
            raise OracleError("controlled PATH fd collides with a held command fd")
    reserved_path_fd = -1
    if readonly_bind_directories:
        try:
            os.fstat(CONTROLLED_PATH_FD)
        except OSError as error:
            if error.errno != errno.EBADF:
                raise
        else:
            raise OracleError("controlled PATH fd is already open")
        placeholder = os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)
        try:
            os.dup2(placeholder, CONTROLLED_PATH_FD, inheritable=True)
        finally:
            os.close(placeholder)
        reserved_path_fd = CONTROLLED_PATH_FD
    path_fds = (reserved_path_fd,) if reserved_path_fd >= 0 else ()
    passed_fds = tuple(dict.fromkeys((executable.fd, *extra_fds, *path_fds)))
    preexec_fn = None
    if readonly_bind_directories:
        host_uid = os.getuid()
        host_gid = os.getgid()

        def enter_namespace() -> None:
            try:
                _readonly_mount_namespace_setup(
                    readonly_bind_directories, host_uid, host_gid
                )
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
            cwd=cwd,
            close_fds=True,
            pass_fds=passed_fds,
            start_new_session=True,
            preexec_fn=preexec_fn,
        )
    finally:
        if reserved_path_fd >= 0:
            os.close(reserved_path_fd)
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
    def __init__(
        self, path: pathlib.Path, aliases: Mapping[str, str], fd: int, identity: os.stat_result
    ):
        self.path = path
        self.aliases = dict(aliases)
        self.fd = fd
        self.identity = identity
        self.child_fd = CONTROLLED_PATH_FD

    @property
    def child_path(self) -> str:
        return f"/proc/self/fd/{self.child_fd}"

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
        os.chmod(path, 0o500)
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            directory = cls(path, expected, fd, os.fstat(fd))
            directory.verify_frozen()
            return directory
        except BaseException:
            os.close(fd)
            raise

    @staticmethod
    def verify_alias_fd(fd: int, aliases: Mapping[str, str]) -> None:
        actual: dict[str, str] = {}
        for name in os.listdir(fd):
            metadata = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if not stat.S_ISLNK(metadata.st_mode):
                raise OracleError(f"controlled tool alias is not a symlink: {name}")
            actual[name] = os.readlink(name, dir_fd=fd)
        if actual != aliases:
            raise OracleError("controlled tool directory changed after population")

    def _verify_entries(self) -> None:
        self.verify_alias_fd(self.fd, self.aliases)

    def verify_frozen(self) -> None:
        metadata = os.fstat(self.fd)
        if not _same_identity(self.identity, metadata):
            raise OracleError("held controlled tool directory identity changed")
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o500:
            raise OracleError("controlled tool directory must remain frozen at mode 0500")
        self._verify_entries()

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def remove_path(self) -> None:
        os.chmod(self.path, 0o700)
        shutil.rmtree(self.path)

    def remove(self) -> None:
        _run_cleanup_actions(
            [
                ("controlled alias remove", self.remove_path),
                ("controlled alias close", self.close),
            ]
        )


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


def publish_candidate(
    path: pathlib.Path | CandidateTarget, document: Mapping[str, object]
) -> None:
    owned_target = not isinstance(path, CandidateTarget)
    target = CandidateTarget.open(path) if owned_target else path
    try:
        if (
            target.basename.endswith("provenance.json")
            or "provenance" in target.basename.lower()
        ):
            raise OracleError("primary controller must never publish final provenance")
        payload = canonical_json_bytes(document)
        if len(payload) > MAX_JSON_BYTES:
            raise OracleError(
                f"candidate metadata exceeds {MAX_JSON_BYTES} byte JSON limit"
            )
        target.reject_existing()
        temporary = (
            f".{target.basename}.candidate-{os.getpid()}-{time.monotonic_ns()}"
        )
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
            dir_fd=target.parent.fd,
        )
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
            target.verify_visible_parent()
            try:
                os.link(
                    temporary,
                    target.basename,
                    src_dir_fd=target.parent.fd,
                    dst_dir_fd=target.parent.fd,
                    follow_symlinks=False,
                )
            except FileExistsError as error:
                raise OracleError(
                    f"candidate metadata already exists: {target.path}"
                ) from error
            linked = True
            os.fsync(target.parent.fd)
            target.verify_visible_parent()
            completed = True
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temporary, dir_fd=target.parent.fd)
            except FileNotFoundError:
                pass
            if linked and not completed:
                try:
                    os.unlink(target.basename, dir_fd=target.parent.fd)
                except FileNotFoundError:
                    pass
    finally:
        if owned_target:
            target.close()


def _run_cleanup_actions(
    actions: Sequence[tuple[str, Callable[[], None]]],
    business_error: BaseException | None = None,
) -> None:
    cleanup_errors: list[tuple[str, BaseException]] = []
    for label, action in actions:
        try:
            action()
        except BaseException as error:
            cleanup_errors.append((label, error))
    if cleanup_errors:
        details = "; ".join(
            f"{label}: {type(error).__name__}: {error}"
            for label, error in cleanup_errors
        )
        if business_error is not None:
            raise OracleError(
                f"{type(business_error).__name__}: {business_error}; "
                f"cleanup errors: {details}"
            ) from business_error
        raise OracleError(f"primary cleanup errors: {details}")
    if business_error is not None:
        raise business_error


def _remove_directory_contents(directory_fd: int) -> None:
    for name in os.listdir(directory_fd):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if stat.S_ISDIR(metadata.st_mode):
            child_fd = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory_fd,
            )
            try:
                os.fchmod(child_fd, 0o700)
                _remove_directory_contents(child_fd)
            finally:
                os.close(child_fd)
            os.rmdir(name, dir_fd=directory_fd)
        else:
            os.unlink(name, dir_fd=directory_fd)


def _remove_runtime_directory(
    parent_fd: int, runtime_name: str, runtime_root: HeldDirectory
) -> None:
    os.fchmod(runtime_root.fd, 0o700)
    _remove_directory_contents(runtime_root.fd)
    os.rmdir(runtime_name, dir_fd=parent_fd)


def _remove_runtime_name(parent_fd: int, runtime_name: str) -> None:
    fd = os.open(
        runtime_name,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        dir_fd=parent_fd,
    )
    runtime_root = HeldDirectory(pathlib.Path(runtime_name), fd)
    try:
        _remove_runtime_directory(parent_fd, runtime_name, runtime_root)
    finally:
        runtime_root.close()


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


def _git_bytes(
    git: HeldExecutable,
    source: HeldDirectory,
    git_dir: HeldDirectory,
    args: Sequence[str],
    env: Mapping[str, str],
) -> bytes:
    source_path = f"/proc/self/fd/{source.fd}"
    git_dir_path = f"/proc/self/fd/{git_dir.fd}"
    runtime_fd = _runtime_fd_from_path(pathlib.Path(env["HOME"]))
    git_env = dict(env)
    git_env.update(
        {
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_LITERAL_PATHSPECS": "1",
            "GIT_ATTR_NOSYSTEM": "1",
            "XDG_CONFIG_HOME": env["HOME"],
        }
    )
    safe_config = [
        "-c",
        "core.useReplaceRefs=false",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.attributesFile=/dev/null",
        "-c",
        "diff.external=",
        "-c",
        "protocol.file.allow=never",
        "-c",
        "protocol.ext.allow=never",
    ]
    result = run_bounded(
        git,
        [
            "git",
            *safe_config,
            f"--git-dir={git_dir_path}",
            f"--work-tree={source_path}",
            *args,
        ],
        env=git_env,
        timeout_ms=COMMAND_TIMEOUT_MS,
        term_grace_ms=1_000,
        stdout_limit_bytes=1024 * 1024,
        stderr_limit_bytes=1024 * 1024,
        extra_fds=(runtime_fd, source.fd, git_dir.fd),
    )
    if result.timed_out or result.output_truncated or result.exit_code != 0:
        raise OracleError(
            f"controlled git command failed: {' '.join(args)}; "
            f"exit={result.exit_code}; stderr={result.stderr.decode('utf-8', 'replace')}"
        )
    return result.stdout


def _git_text(
    git: HeldExecutable,
    source: HeldDirectory,
    git_dir: HeldDirectory,
    args: Sequence[str],
    env: Mapping[str, str],
) -> str:
    return _git_bytes(git, source, git_dir, args, env).decode("utf-8", "strict").strip()


def _read_held_regular(root: HeldDirectory, relative: str, limit: int = 1024 * 1024) -> bytes:
    fd = root.open_regular(relative)
    try:
        metadata = os.fstat(fd)
        if metadata.st_size > limit:
            raise OracleError(f"Git control file exceeds byte bound: {relative}")
        content = bytearray()
        while len(content) <= limit:
            chunk = os.read(fd, min(64 * 1024, limit + 1 - len(content)))
            if not chunk:
                return bytes(content)
            content.extend(chunk)
        raise OracleError(f"Git control file exceeds byte bound: {relative}")
    finally:
        os.close(fd)


def _reject_git_path(git_dir: HeldDirectory, relative: str) -> None:
    try:
        git_dir.lstat(relative)
    except FileNotFoundError:
        return
    raise OracleError(f"source Git storage is not independent: .git/{relative}")


def _validate_git_trust_root(
    source: HeldDirectory,
    git_dir: HeldDirectory,
    git: HeldExecutable,
    env: Mapping[str, str],
) -> None:
    for relative in (
        "commondir",
        "shallow",
        "info/grafts",
        "objects/info/alternates",
        "objects/info/http-alternates",
        "refs/replace",
    ):
        _reject_git_path(git_dir, relative)

    try:
        packed_refs = _read_held_regular(git_dir, "packed-refs")
    except FileNotFoundError:
        packed_refs = b""
    if b" refs/replace/" in packed_refs:
        raise OracleError("source Git storage contains packed replacement refs")

    try:
        pack_dir = git_dir.open_directory("objects/pack")
    except FileNotFoundError:
        pack_dir = None
    if pack_dir is not None:
        try:
            promisor = sorted(
                (name for name in os.listdir(pack_dir.fd) if name.endswith(".promisor")),
                key=os.fsencode,
            )
            if promisor:
                raise OracleError(f"source Git storage contains promisor packs: {promisor}")
        finally:
            pack_dir.close()

    config_keys = _git_bytes(
        git,
        source,
        git_dir,
        ["config", "--local", "--null", "--name-only", "--list", "--no-includes"],
        env,
    ).split(b"\0")
    unsafe_keys: list[str] = []
    for raw_key in config_keys:
        if not raw_key:
            continue
        key = raw_key.decode("utf-8", "strict").lower()
        executable_or_authority = (
            key in {
                "core.fsmonitor",
                "core.hookspath",
                "core.attributesfile",
                "core.worktree",
                "core.sshcommand",
                "gpg.program",
                "sequence.editor",
            }
            or key.startswith(("include.", "includeif.", "extensions."))
            or (key.startswith("filter.") and key.rsplit(".", 1)[-1] in {"clean", "smudge", "process"})
            or (key.startswith("diff.") and key.rsplit(".", 1)[-1] in {"command", "textconv"})
            or (key.startswith("remote.") and key.endswith(".promisor"))
            or (key.startswith("credential.") and key.endswith(".helper"))
            or (key.startswith("submodule.") and key.endswith(".update"))
        )
        if executable_or_authority:
            unsafe_keys.append(key)
    if unsafe_keys:
        raise OracleError(f"source Git config contains unsafe extensions: {unsafe_keys}")

    replace_refs = _git_text(
        git,
        source,
        git_dir,
        ["for-each-ref", "--format=%(refname)", "refs/replace"],
        env,
    )
    if replace_refs:
        raise OracleError(f"source Git storage contains replacement refs: {replace_refs}")


def _validate_source(
    source: HeldDirectory,
    git_dir: HeldDirectory,
    git: HeldExecutable,
    env: Mapping[str, str],
) -> dict[str, object]:
    head = _git_text(git, source, git_dir, ["rev-parse", "HEAD"], env)
    tag_commit = _git_text(
        git, source, git_dir, ["rev-parse", f"{REDIS_TAG}^{{commit}}"], env
    )
    status_output = _git_text(
        git,
        source,
        git_dir,
        ["status", "--porcelain=v1", "--untracked-files=all"],
        env,
    )
    repository = _git_text(
        git, source, git_dir, ["remote", "get-url", "origin"], env
    )
    if head != REDIS_COMMIT or tag_commit != REDIS_COMMIT:
        raise OracleError(
            f"source must be exact Redis {REDIS_TAG} commit {REDIS_COMMIT}; "
            f"HEAD={head!r}, tag={tag_commit!r}"
        )
    if repository != REDIS_REPOSITORY:
        raise OracleError(f"source origin must equal {REDIS_REPOSITORY}, got {repository!r}")
    if status_output:
        raise OracleError("source checkout must be tracked/untracked clean before build")
    return {
        "repository": REDIS_REPOSITORY,
        "tag": REDIS_TAG,
        "commit": REDIS_COMMIT,
        "head": head,
        "tag_commit": tag_commit,
        "root_path": str(source.path),
        "git_dir_path": str(source.path / ".git"),
        "tracked_untracked_clean": True,
    }


def _validate_pristine_source_tree(
    source: HeldDirectory,
    git_dir: HeldDirectory,
    git: HeldExecutable,
    env: Mapping[str, str],
) -> dict[str, tuple[object, ...]]:
    index_records = _git_bytes(
        git, source, git_dir, ["ls-files", "-v", "-z", "--cached"], env
    ).split(b"\0")
    non_default_index_flags: list[str] = []
    for record in index_records:
        if not record:
            continue
        if len(record) < 3 or record[1:2] != b" ":
            raise OracleError("git returned malformed index flag evidence")
        tag = record[:1]
        path = record[2:].decode("utf-8", "strict")
        if tag != b"H":
            non_default_index_flags.append(f"{tag.decode('ascii', 'replace')} {path}")
    tree_records = _git_bytes(
        git,
        source,
        git_dir,
        ["ls-tree", "-rz", "--full-tree", REDIS_COMMIT],
        env,
    ).split(b"\0")
    expected: dict[str, tuple[str, str]] = {}
    for record in tree_records:
        if not record:
            continue
        try:
            header, raw_path = record.split(b"\t", 1)
            raw_mode, raw_kind, raw_oid = header.split(b" ", 2)
        except ValueError as error:
            raise OracleError("git returned malformed fixed-commit tree evidence") from error
        mode = raw_mode.decode("ascii", "strict")
        kind = raw_kind.decode("ascii", "strict")
        oid = raw_oid.decode("ascii", "strict")
        path = raw_path.decode("utf-8", "strict")
        parts = path.split("/")
        if not path or any(part in {"", ".", ".."} for part in parts):
            raise OracleError(f"git returned a non-canonical tree path: {path!r}")
        if kind != "blob" or mode not in {"100644", "100755", "120000"}:
            raise OracleError(
                f"fixed Redis commit contains unsupported tree entry: {mode} {kind} {path}"
            )
        if len(oid) != 40 or any(character not in "0123456789abcdef" for character in oid):
            raise OracleError(f"git returned an invalid object id for {path!r}")
        if path in expected:
            raise OracleError(f"fixed Redis commit contains duplicate path: {path!r}")
        expected[path] = (mode, oid)
    if not expected:
        raise OracleError("exact fixed Redis commit tree has no tracked files")

    tracked = set(expected)
    allowed_directories: set[str] = set()
    for path in tracked:
        parts = path.split("/")
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

    mismatches: list[str] = []
    for path in sorted(tracked, key=os.fsencode):
        expected_mode, expected_oid = expected[path]
        metadata = source.lstat(path)
        if expected_mode == "120000":
            if not stat.S_ISLNK(metadata.st_mode):
                mismatches.append(f"{path}: expected symlink mode 120000")
                continue
            actual_oid = _git_blob_oid_bytes(source.readlink_bytes(path))
        else:
            if not stat.S_ISREG(metadata.st_mode):
                mismatches.append(f"{path}: expected regular mode {expected_mode}")
                continue
            executable = bool(metadata.st_mode & 0o111)
            if executable != (expected_mode == "100755"):
                mismatches.append(f"{path}: executable mode differs from {expected_mode}")
                continue
            fd = source.open_regular(path)
            try:
                actual_oid = _git_blob_oid_fd(fd, os.fstat(fd).st_size)
            finally:
                os.close(fd)
        if actual_oid != expected_oid:
            mismatches.append(f"{path}: Git blob differs from {expected_oid}")
    problems: list[str] = []
    if non_default_index_flags:
        problems.append(
            "source index has assume-unchanged/skip-worktree flags: "
            f"{non_default_index_flags}"
        )
    if mismatches:
        problems.append(f"source differs from fixed Redis commit tree: {mismatches}")
    if problems:
        raise OracleError("; ".join(problems))
    return entries


def _sanitized_environment(
    tool_directory: str, home: pathlib.Path, temporary: pathlib.Path
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
        "COMPILER_PATH": str(tool_directory),
        "LIBRARY_PATH": str(tool_directory),
        "MAKEFLAGS": "",
    }


def _runtime_fd_from_path(path: pathlib.Path) -> int:
    parts = path.parts
    if len(parts) < 5 or parts[:4] != (os.sep, "proc", "self", "fd"):
        raise OracleError(f"runtime path is not held-FD based: {path}")
    try:
        runtime_fd = int(parts[4])
    except ValueError as error:
        raise OracleError(f"runtime path has an invalid held FD: {path}") from error
    metadata = os.fstat(runtime_fd)
    if not stat.S_ISDIR(metadata.st_mode):
        raise OracleError(f"runtime FD is not a directory: {runtime_fd}")
    return runtime_fd


def _register_tools(
    controller: HeldExecutable,
    python: HeldExecutable,
    discovery_env: Mapping[str, str],
    discovery_cwd: pathlib.Path,
    registry: list[HeldExecutable] | None = None,
) -> tuple[list[HeldExecutable], dict[str, HeldExecutable], dict[str, tuple[str, ...]]]:
    runtime_fd = _runtime_fd_from_path(discovery_cwd)
    tools = registry if registry is not None else []
    if tools:
        raise OracleError("tool registry must be empty before discovery")
    tools.extend([controller, python])
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

    cc = next(tool for tool in tools if tool.role == "cc")
    for program, query, component_version in (
        ("cc1", "-print-prog-name=cc1", ("-version", "-o", "/dev/null", "/dev/null")),
        ("collect2", "-print-prog-name=collect2", ("--version",)),
        ("liblto_plugin.so", "-print-file-name=liblto_plugin.so", None),
        ("crtbegin.o", "-print-file-name=crtbegin.o", None),
        ("crtbeginS.o", "-print-file-name=crtbeginS.o", None),
        ("crtbeginT.o", "-print-file-name=crtbeginT.o", None),
        ("crtend.o", "-print-file-name=crtend.o", None),
        ("crtendS.o", "-print-file-name=crtendS.o", None),
        ("libgcc.a", "-print-file-name=libgcc.a", None),
        ("libgcc_s.so", "-print-file-name=libgcc_s.so", None),
    ):
        result = run_bounded(
            cc,
            ["cc", query],
            env=discovery_env,
            timeout_ms=COMMAND_TIMEOUT_MS,
            term_grace_ms=1_000,
            stdout_limit_bytes=VERSION_OUTPUT_LIMIT,
            stderr_limit_bytes=VERSION_OUTPUT_LIMIT,
            extra_fds=(runtime_fd,),
            cwd=discovery_cwd,
        )
        path_text = _command_text(result, f"cc {program} discovery").splitlines()[0]
        path = pathlib.Path(path_text)
        if not path.is_absolute():
            raise OracleError(f"cc returned a non-absolute internal program: {path_text!r}")
        role_suffix = "".join(
            character.lower() if character.isascii() and character.isalnum() else "-"
            for character in program
        ).strip("-")
        role_prefix = "cc-component" if component_version is not None else "cc-resource"
        internal = HeldExecutable.open(f"{role_prefix}-{role_suffix}", path)
        tools.append(internal)
        by_path[path] = internal
        if component_version is not None:
            versions[internal.role] = component_version
        aliases[program] = internal

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
    version_cwd: pathlib.Path,
    aliases_directory: FrozenToolDirectory,
) -> list[dict[str, object]]:
    runtime_fd = _runtime_fd_from_path(version_cwd)
    evidence = []
    for tool in tools:
        if tool.role.startswith("cc-resource-"):
            continue
        if tool.role == "controller":
            version = "kiwi Redis Oracle controller v3"
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
                extra_fds=(runtime_fd, *(candidate.fd for candidate in tools)),
                readonly_bind_directories=(aliases_directory,),
                cwd=version_cwd,
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
    if not source_input.is_absolute() or not pathlib.Path(metadata_argument).is_absolute():
        raise OracleError("--source and --metadata must be absolute paths")
    candidate = CandidateTarget.open(metadata_argument)
    source_root: HeldDirectory | None = None
    controller: HeldExecutable | None = None
    python: HeldExecutable | None = None
    tools: list[HeldExecutable] = []
    aliases_directory: FrozenToolDirectory | None = None
    git_directory: HeldDirectory | None = None
    runtime_name: str | None = None
    runtime_root: HeldDirectory | None = None
    document: Mapping[str, object] | None = None
    business_error: BaseException | None = None
    try:
        source = source_input.resolve(strict=True)
        if not source.is_dir() or source.is_symlink():
            raise OracleError("--source must resolve to a real directory")
        if candidate.parent_path == source or source in candidate.parent_path.parents:
            raise OracleError("candidate metadata must be outside the source checkout")

        source_root = HeldDirectory.open(source)
        controller = HeldExecutable.from_fd(
            "controller", bootstrap_controller_path, bootstrap_controller_fd
        )
        python = HeldExecutable.from_fd(
            "python", bootstrap_python_path, bootstrap_python_fd
        )
        for attempt in range(100):
            runtime_name = (
                f".kiwi-oracle-primary-{os.getpid()}-{time.monotonic_ns()}-{attempt}"
            )
            try:
                os.mkdir(runtime_name, mode=0o700, dir_fd=candidate.parent.fd)
                break
            except FileExistsError:
                continue
        else:
            raise OracleError("unable to reserve primary runtime directory")
        runtime_root = candidate.parent.open_directory(runtime_name)
        runtime = pathlib.Path(f"/proc/self/fd/{runtime_root.fd}")
        home = runtime / "home"
        temporary = runtime / "tmp"
        tool_path = runtime / "tools"
        version_working = runtime / "versions"
        home.mkdir(mode=0o700)
        temporary.mkdir(mode=0o700)
        version_working.mkdir(mode=0o700)
        _empty_directory(home, "HOME")
        _empty_directory(temporary, "TMPDIR")

        discovery_env = _sanitized_environment("/usr/bin:/bin", home, temporary)
        tools, aliases, versions = _register_tools(
            controller, python, discovery_env, version_working, tools
        )
        aliases_directory = FrozenToolDirectory.create(tool_path, aliases)
        env = _sanitized_environment(aliases_directory.child_path, home, temporary)
        tool_evidence = _tool_evidence(
            tools, versions, env, version_working, aliases_directory
        )
        _empty_directory(version_working, "tool version working directory")
        git = next(tool for tool in tools if tool.role == "git")
        git_directory = source_root.open_directory(".git")
        _validate_git_trust_root(source_root, git_directory, git, env)
        baseline = _validate_pristine_source_tree(source_root, git_directory, git, env)
        source_evidence = _validate_source(source_root, git_directory, git, env)

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
                extra_fds=(runtime_root.fd, source_fd, *(tool.fd for tool in tools)),
                readonly_bind_directories=(aliases_directory,),
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
        candidate.verify_visible_parent()
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
    except BaseException as error:
        business_error = error

    cleanup_actions: list[tuple[str, Callable[[], None]]] = []
    if aliases_directory is not None:
        cleanup_actions.extend(
            [
                ("controlled alias remove", aliases_directory.remove_path),
                ("controlled alias close", aliases_directory.close),
            ]
        )
    if git_directory is not None:
        cleanup_actions.append(("Git directory close", git_directory.close))
    for index, tool in enumerate(tools):
        cleanup_actions.append((f"tool {index} close", tool.close))
    if controller is not None and all(tool is not controller for tool in tools):
        cleanup_actions.append(("controller close", controller.close))
    if python is not None and all(tool is not python for tool in tools):
        cleanup_actions.append(("Python close", python.close))
    if runtime_root is not None and runtime_name is not None:
        cleanup_actions.append(
            (
                "runtime remove",
                lambda: _remove_runtime_directory(
                    candidate.parent.fd, runtime_name, runtime_root
                ),
            )
        )
        cleanup_actions.append(("runtime close", runtime_root.close))
    elif runtime_name is not None:
        cleanup_actions.append(
            (
                "runtime remove",
                lambda: _remove_runtime_name(candidate.parent.fd, runtime_name),
            )
        )
    if source_root is not None:
        cleanup_actions.append(("source close", source_root.close))

    try:
        _run_cleanup_actions(cleanup_actions, business_error)
        if document is None:
            raise OracleError("primary build produced no candidate document")
        candidate.verify_visible_parent()
        publish_candidate(candidate, document)
        print(str(candidate.path))
    finally:
        candidate.close()


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
