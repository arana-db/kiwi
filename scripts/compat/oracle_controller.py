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
import socket
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable, Mapping, Sequence

BUILD_SCHEMA = "kiwi-redis-oracle-build/v3"
PROVENANCE_SCHEMA = "kiwi-redis-oracle-provenance/v3"
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
CALLBACK_TIMEOUT_MS = 600_000
CALLBACK_OUTPUT_LIMIT = 16 * 1024 * 1024
REDIS_START_TIMEOUT_MS = 30_000
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


class HeldRegularFile:
    """A bounded regular input retained through a no-follow file descriptor."""

    def __init__(
        self,
        path: pathlib.Path,
        parent_path: pathlib.Path,
        basename: str,
        parent: HeldDirectory,
        fd: int,
        limit: int,
    ):
        self.path = path
        self.parent_path = parent_path
        self.basename = basename
        self.parent = parent
        self.fd = fd
        self.stat = os.fstat(fd)
        if not stat.S_ISREG(self.stat.st_mode) or self.stat.st_size > limit:
            raise OracleError(f"input must be a bounded regular file: {path}")
        self.sha256 = _sha256_fd(fd)

    @classmethod
    def open(cls, argument: str | pathlib.Path, limit: int) -> "HeldRegularFile":
        raw = os.fspath(argument)
        if not os.path.isabs(raw) or os.path.normpath(raw) != raw:
            raise OracleError("input file path must be absolute and canonical")
        basename = os.path.basename(raw)
        if not basename or basename in {".", ".."} or "/" in basename or "\\" in basename:
            raise OracleError(f"invalid input basename: {basename!r}")
        parent_path = pathlib.Path(os.path.dirname(raw))
        parent = HeldDirectory.open_absolute_nofollow(parent_path)
        fd = -1
        try:
            before = os.stat(basename, dir_fd=parent.fd, follow_symlinks=False)
            fd = os.open(
                basename,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent.fd,
            )
            held = cls(pathlib.Path(raw), parent_path, basename, parent, fd, limit)
            if not _same_identity(before, held.stat):
                raise OracleError(f"input changed while it was opened: {raw}")
            return held
        except BaseException:
            if fd >= 0:
                os.close(fd)
            parent.close()
            raise

    def read_bytes(self) -> bytes:
        os.lseek(self.fd, 0, os.SEEK_SET)
        content = bytearray()
        while len(content) <= self.stat.st_size:
            chunk = os.read(self.fd, min(64 * 1024, self.stat.st_size + 1 - len(content)))
            if not chunk:
                break
            content.extend(chunk)
        os.lseek(self.fd, 0, os.SEEK_SET)
        if len(content) != self.stat.st_size:
            raise OracleError(f"input size changed while reading: {self.path}")
        return bytes(content)

    def verify_path(self) -> None:
        visible = HeldDirectory.open_absolute_nofollow(self.parent_path)
        try:
            if not _same_directory_object(self.parent.stat, visible.stat):
                raise OracleError(f"input parent changed: {self.parent_path}")
        finally:
            visible.close()
        current = os.stat(self.basename, dir_fd=self.parent.fd, follow_symlinks=False)
        if not _same_identity(self.stat, current) or _sha256_fd(self.fd) != self.sha256:
            raise OracleError(f"input identity changed: {self.path}")

    def close(self) -> None:
        errors: list[BaseException] = []
        if self.fd >= 0:
            try:
                os.close(self.fd)
            except BaseException as error:
                errors.append(error)
            self.fd = -1
        try:
            self.parent.close()
        except BaseException as error:
            errors.append(error)
        if errors:
            raise OracleError(f"input close failed: {errors}")


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
    namespace_init_pid: int | None = None
    namespace_init_start_time: int | None = None


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

    def verify_path(self) -> None:
        current = os.stat(self.path, follow_symlinks=False)
        if not _same_identity(self.stat, current):
            raise OracleError(f"tool {self.role!r} path identity changed: {self.path}")
        if _sha256_fd(self.fd) != self.sha256:
            raise OracleError(f"held tool {self.role!r} content changed")

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


def _drop_namespace_privileges() -> None:
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
    if libc.prctl(
        pr_set_securebits,
        secure_noroot | secure_no_setuid_fixup,
        0,
        0,
        0,
    ) != 0:
        error = ctypes.get_errno()
        raise OSError(error, f"lock namespace securebits: {os.strerror(error)}")
    header = CapabilityHeader(linux_capability_version_3, 0)
    data = (CapabilityData * 2)()
    if libc.capset(ctypes.byref(header), ctypes.byref(data)) != 0:
        error = ctypes.get_errno()
        raise OSError(error, f"drop namespace capabilities: {os.strerror(error)}")
    if libc.prctl(pr_set_no_new_privs, 1, 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise OSError(error, f"set no_new_privs: {os.strerror(error)}")


def _mount(
    source: str | None,
    target: str,
    filesystem: str | None,
    flags: int,
    data: str | None = None,
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.mount(
        None if source is None else os.fsencode(source),
        os.fsencode(target),
        None if filesystem is None else os.fsencode(filesystem),
        flags,
        None if data is None else os.fsencode(data),
    )
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, f"mount {source!r} on {target!r}: {os.strerror(error)}")


def _reopen_held_mount_source(
    held_fd: int, *, directory: bool, label: str
) -> int:
    visible = os.readlink(f"/proc/self/fd/{held_fd}")
    if not visible.startswith("/") or visible.endswith(" (deleted)"):
        raise OracleError(f"held callback {label} has no visible absolute path")
    flags = os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC
    if directory:
        flags |= os.O_DIRECTORY
    current_fd = os.open(visible, flags)
    if not _same_identity(os.fstat(held_fd), os.fstat(current_fd)):
        os.close(current_fd)
        raise OracleError(f"held callback {label} changed while entering its mount namespace")
    return current_fd


def _detached_tmpfs_mount() -> int:
    fsopen_clexec = 1
    fsconfig_cmd_create = 6
    fsmount_cloexec = 1
    syscall_fsopen = 430
    syscall_fsconfig = 431
    syscall_fsmount = 432
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long

    filesystem_fd = libc.syscall(
        syscall_fsopen, ctypes.c_char_p(b"tmpfs"), fsopen_clexec
    )
    if filesystem_fd < 0:
        error = ctypes.get_errno()
        raise OSError(error, f"fsopen callback tmpfs: {os.strerror(error)}")
    try:
        result = libc.syscall(
            syscall_fsconfig,
            filesystem_fd,
            fsconfig_cmd_create,
            ctypes.c_void_p(),
            ctypes.c_void_p(),
            0,
        )
        if result != 0:
            error = ctypes.get_errno()
            raise OSError(error, f"fsconfig callback tmpfs: {os.strerror(error)}")
        mount_fd = libc.syscall(syscall_fsmount, filesystem_fd, fsmount_cloexec, 0)
        if mount_fd < 0:
            error = ctypes.get_errno()
            raise OSError(error, f"fsmount callback tmpfs: {os.strerror(error)}")
        return int(mount_fd)
    finally:
        os.close(filesystem_fd)


def _move_detached_mount(mount_fd: int, target_fd: int) -> None:
    move_mount_f_empty_path = 0x00000004
    move_mount_t_empty_path = 0x00000040
    syscall_move_mount = 429
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    result = libc.syscall(
        syscall_move_mount,
        mount_fd,
        ctypes.c_char_p(b""),
        target_fd,
        ctypes.c_char_p(b""),
        move_mount_f_empty_path | move_mount_t_empty_path,
    )
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, f"move_mount callback tmpfs: {os.strerror(error)}")


def _callback_filesystem_setup(
    work_fd: int,
    callback_input_fd: int,
    runtime_evidence_fd: int,
    sandbox_root_fd: int,
) -> None:
    ms_rdonly = 1
    ms_nosuid = 2
    ms_nodev = 4
    ms_noexec = 8
    ms_remount = 32
    ms_bind = 4096
    ms_rec = 16384
    ms_private = 1 << 18
    _mount(None, "/", None, ms_rec | ms_private)
    current_work_fd = _reopen_held_mount_source(
        work_fd, directory=True, label="work directory"
    )
    current_evidence_fd = _reopen_held_mount_source(
        runtime_evidence_fd, directory=False, label="runtime evidence"
    )
    current_callback_input_fd = _reopen_held_mount_source(
        callback_input_fd, directory=True, label="callback input"
    )
    current_sandbox_fd = _reopen_held_mount_source(
        sandbox_root_fd, directory=True, label="sandbox root"
    )
    root_mount_fd = _detached_tmpfs_mount()
    try:
        _move_detached_mount(root_mount_fd, current_sandbox_fd)
        for relative in ("usr", "proc", "dev", "work", "callback-input"):
            os.mkdir(relative, mode=0o700, dir_fd=root_mount_fd)
        for relative in ("runtime-evidence.json", "dev/null"):
            placeholder = os.open(
                relative,
                os.O_RDONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o600,
                dir_fd=root_mount_fd,
            )
            os.close(placeholder)
        for link, target in (
            ("bin", "usr/bin"),
            ("lib", "usr/lib"),
            ("lib64", "usr/lib64"),
        ):
            os.symlink(target, link, dir_fd=root_mount_fd)

        os.fchdir(root_mount_fd)
        _mount("/usr", "usr", None, ms_bind | ms_rec)
        _mount(
            None,
            "usr",
            None,
            ms_bind | ms_remount | ms_rdonly | ms_nosuid | ms_nodev,
        )
        _mount(f"/proc/self/fd/{current_work_fd}", "work", None, ms_bind)
        _mount(
            f"/proc/self/fd/{current_callback_input_fd}",
            "callback-input",
            None,
            ms_bind | ms_rec,
        )
        _mount(
            None,
            "callback-input",
            None,
            ms_bind | ms_remount | ms_rdonly | ms_nosuid | ms_nodev,
        )
        _mount(
            f"/proc/self/fd/{current_evidence_fd}",
            "runtime-evidence.json",
            None,
            ms_bind,
        )
        _mount(
            None,
            "runtime-evidence.json",
            None,
            ms_bind | ms_remount | ms_rdonly | ms_nosuid | ms_nodev,
        )
        _mount("/dev/null", "dev/null", None, ms_bind)
        _mount(
            "proc",
            "proc",
            "proc",
            ms_rdonly | ms_nosuid | ms_nodev | ms_noexec,
        )

        if not _same_identity(os.fstat(work_fd), os.stat("work", follow_symlinks=False)):
            raise OracleError("callback work mount differs from its held directory")
        if not _same_identity(
            os.fstat(callback_input_fd),
            os.stat("callback-input", follow_symlinks=False),
        ):
            raise OracleError("callback input mount differs from its held directory")
        if not _same_identity(
            os.fstat(runtime_evidence_fd),
            os.stat("runtime-evidence.json", follow_symlinks=False),
        ):
            raise OracleError("callback evidence mount differs from its held file")
        if not os.statvfs("runtime-evidence.json").f_flag & os.ST_RDONLY:
            raise OracleError("callback runtime evidence mount is not read-only")
        if os.statvfs("work").f_flag & os.ST_RDONLY:
            raise OracleError("callback work mount is unexpectedly read-only")
        if not os.statvfs("callback-input").f_flag & os.ST_RDONLY:
            raise OracleError("callback input mount is not read-only")

        _mount(
            None,
            ".",
            None,
            ms_remount | ms_rdonly | ms_nosuid | ms_nodev,
        )
        if not os.statvfs(".").f_flag & os.ST_RDONLY:
            raise OracleError("callback sandbox root mount is not read-only")
        os.chroot(".")
        os.chdir("/work")
    finally:
        for fd in (
            root_mount_fd,
            current_sandbox_fd,
            current_callback_input_fd,
            current_evidence_fd,
            current_work_fd,
            sandbox_root_fd,
            runtime_evidence_fd,
            callback_input_fd,
            work_fd,
        ):
            try:
                os.close(fd)
            except OSError:
                pass


def _pid_namespace_preexec(
    report_fd: int,
    host_uid: int,
    host_gid: int,
    term_grace_ms: int,
    child_setup: Callable[[], None] | None,
) -> None:
    clone_newns = 0x00020000
    clone_newuser = 0x10000000
    clone_newpid = 0x20000000
    flags = clone_newuser | clone_newpid
    if child_setup is not None:
        flags |= clone_newns
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.unshare(flags) != 0:
        error = ctypes.get_errno()
        raise OSError(error, f"unshare callback namespaces: {os.strerror(error)}")
    try:
        _write_proc_mapping("/proc/self/setgroups", "deny")
    except FileNotFoundError:
        pass
    _write_proc_mapping("/proc/self/uid_map", f"0 {host_uid} 1")
    _write_proc_mapping("/proc/self/gid_map", f"0 {host_gid} 1")
    namespace_init = os.fork()
    if namespace_init == 0:
        try:
            if child_setup is not None:
                child_setup()
            _drop_namespace_privileges()
            os.write(report_fd, b"READY\n")
        except BaseException as error:
            os.write(
                report_fd,
                f"ERROR:{type(error).__name__}:{error}\n".encode("utf-8", "replace"),
            )
            raise
        finally:
            os.close(report_fd)
        return

    os.write(
        report_fd,
        f"INIT:{namespace_init}:{_process_start_time(namespace_init)}\n".encode("ascii"),
    )
    os.close(report_fd)
    for fd in (0, 1, 2):
        try:
            os.close(fd)
        except OSError:
            pass
    for raw_fd in os.listdir("/proc/self/fd"):
        try:
            fd = int(raw_fd)
        except ValueError:
            continue
        if fd <= 2:
            continue
        try:
            os.close(fd)
        except OSError:
            pass

    def terminate(_signal_number: int, _frame: object) -> None:
        try:
            os.kill(namespace_init, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + term_grace_ms / 1000
        while time.monotonic() < deadline:
            try:
                child, _status = os.waitpid(namespace_init, os.WNOHANG)
            except ChildProcessError:
                os._exit(128 + signal.SIGTERM)
            if child == namespace_init:
                os._exit(128 + signal.SIGTERM)
            time.sleep(0.005)
        try:
            os.kill(namespace_init, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            os.waitpid(namespace_init, 0)
        except ChildProcessError:
            pass
        os._exit(128 + signal.SIGTERM)

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    while True:
        try:
            _child, status = os.waitpid(namespace_init, 0)
            break
        except InterruptedError:
            continue
    if os.WIFEXITED(status):
        os._exit(os.WEXITSTATUS(status))
    if os.WIFSIGNALED(status):
        os._exit(128 + os.WTERMSIG(status))
    os._exit(127)


def _signal_group(pid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _process_start_time(pid: int) -> int:
    raw = pathlib.Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    closing = raw.rfind(")")
    if closing < 0:
        raise OracleError(f"process {pid} has malformed /proc stat evidence")
    fields = raw[closing + 2 :].split()
    if len(fields) <= 19:
        raise OracleError(f"process {pid} has incomplete /proc stat evidence")
    return int(fields[19])


def _process_matches_start_time(pid: int | None, start_time: int | None) -> bool:
    if pid is None or start_time is None or pid <= 0:
        return False
    try:
        return _process_start_time(pid) == start_time
    except (FileNotFoundError, ProcessLookupError):
        return False


class _ForkedProcess:
    def __init__(self, pid: int, argv: Sequence[str], stdout_fd: int, stderr_fd: int):
        self.pid = pid
        self.argv = list(argv)
        self.stdout = os.fdopen(stdout_fd, "rb", buffering=0)
        self.stderr = os.fdopen(stderr_fd, "rb", buffering=0)
        self.returncode: int | None = None

    @staticmethod
    def _decode_status(status: int) -> int:
        if os.WIFEXITED(status):
            return os.WEXITSTATUS(status)
        if os.WIFSIGNALED(status):
            return -os.WTERMSIG(status)
        return 127

    def poll(self) -> int | None:
        if self.returncode is not None:
            return self.returncode
        try:
            child, status = os.waitpid(self.pid, os.WNOHANG)
        except ChildProcessError:
            return self.returncode
        if child == 0:
            return None
        self.returncode = self._decode_status(status)
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is not None:
            return self.returncode
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            result = self.poll()
            if result is not None:
                return result
            if deadline is not None and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(self.argv, timeout)
            time.sleep(0.005)


def _close_child_fds(except_fds: set[int]) -> None:
    for raw_fd in os.listdir("/proc/self/fd"):
        try:
            fd = int(raw_fd)
        except ValueError:
            continue
        if fd in except_fds:
            continue
        try:
            os.close(fd)
        except OSError:
            pass


def _spawn_with_supervised_setup(
    executable: HeldExecutable,
    argv: Sequence[str],
    env: Mapping[str, str],
    cwd: pathlib.Path | None,
    inherited_fds: Sequence[int],
    exec_fds: Sequence[int],
    child_setup: Callable[[], None],
    report_fd: int,
) -> _ForkedProcess:
    stdout_read, stdout_write = os.pipe2(os.O_CLOEXEC)
    stderr_read, stderr_write = os.pipe2(os.O_CLOEXEC)
    try:
        pid = os.fork()
    except BaseException:
        for fd in (stdout_read, stdout_write, stderr_read, stderr_write):
            os.close(fd)
        raise
    if pid == 0:
        try:
            os.close(stdout_read)
            os.close(stderr_read)
            os.setsid()
            null_fd = os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)
            try:
                os.dup2(null_fd, 0)
            finally:
                os.close(null_fd)
            os.dup2(stdout_write, 1)
            os.dup2(stderr_write, 2)
            keep = {0, 1, 2, executable.fd, *inherited_fds}
            _close_child_fds(keep)
            if cwd is not None:
                os.chdir(cwd)
            child_setup()
            for fd in exec_fds:
                os.set_inheritable(fd, True)
            os.execve(
                f"/proc/self/fd/{executable.fd}",
                list(argv),
                dict(env),
            )
        except BaseException as error:
            if report_fd >= 0:
                try:
                    os.write(
                        report_fd,
                        f"ERROR:{type(error).__name__}:{error}\n".encode(
                            "utf-8", "replace"
                        ),
                    )
                except OSError:
                    pass
            try:
                os.write(
                    2,
                    f"Oracle child setup failed: {error}\n".encode("utf-8", "replace"),
                )
            except OSError:
                pass
            os._exit(127)
    os.close(stdout_write)
    os.close(stderr_write)
    return _ForkedProcess(pid, argv, stdout_read, stderr_read)


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
    terminate_on_output_limit: bool = False,
    pid_namespace: bool = False,
    callback_filesystem: tuple[int, int, int, int] | None = None,
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
    if callback_filesystem is not None and not pid_namespace:
        raise OracleError("callback filesystem isolation requires a PID namespace")
    if pid_namespace and readonly_bind_directories:
        raise OracleError("callback PID namespace cannot reuse the build PATH namespace")

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
    report_read_fd = -1
    report_write_fd = -1
    if pid_namespace:
        report_read_fd, report_write_fd = os.pipe2(os.O_CLOEXEC)
    report_fds = (report_write_fd,) if report_write_fd >= 0 else ()
    passed_fds = tuple(
        dict.fromkeys((executable.fd, *extra_fds, *path_fds, *report_fds))
    )
    child_setup_fn: Callable[[], None] | None = None
    if pid_namespace:
        host_uid = os.getuid()
        host_gid = os.getgid()

        def enter_pid_namespace() -> None:
            child_setup = None
            if callback_filesystem is not None:
                (
                    work_fd,
                    callback_input_fd,
                    runtime_evidence_fd,
                    sandbox_root_fd,
                ) = callback_filesystem

                def setup_callback_filesystem() -> None:
                    _callback_filesystem_setup(
                        work_fd,
                        callback_input_fd,
                        runtime_evidence_fd,
                        sandbox_root_fd,
                    )

                child_setup = setup_callback_filesystem
            _pid_namespace_preexec(
                report_write_fd,
                host_uid,
                host_gid,
                term_grace_ms,
                child_setup,
            )

        child_setup_fn = enter_pid_namespace
    elif readonly_bind_directories:
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

        child_setup_fn = enter_namespace
    try:
        if child_setup_fn is not None:
            process = _spawn_with_supervised_setup(
                executable,
                argv,
                env,
                cwd,
                passed_fds,
                passed_fds if readonly_bind_directories else (executable.fd,),
                child_setup_fn,
                report_write_fd,
            )
        else:
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
            )
    except BaseException:
        if report_read_fd >= 0:
            os.close(report_read_fd)
            report_read_fd = -1
        raise
    finally:
        if report_write_fd >= 0:
            os.close(report_write_fd)
        if reserved_path_fd >= 0:
            os.close(reserved_path_fd)
    assert process.stdout is not None and process.stderr is not None
    selector = selectors.DefaultSelector()
    for stream, name, limit in (
        (process.stdout, "stdout", stdout_limit_bytes),
        (process.stderr, "stderr", stderr_limit_bytes),
    ):
        os.set_blocking(stream.fileno(), False)
        selector.register(stream, selectors.EVENT_READ, (name, limit))
    if report_read_fd >= 0:
        os.set_blocking(report_read_fd, False)
        selector.register(report_read_fd, selectors.EVENT_READ, ("control", 4096))
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    counts = {"stdout": 0, "stderr": 0}
    control_buffer = bytearray()
    control_errors: list[str] = []
    namespace_ready = False
    namespace_init_pid: int | None = None
    namespace_init_start_time: int | None = None
    timed_out = False
    output_limit_hit = False
    deadline = started + timeout_ms / 1000
    termination_deadline: float | None = None

    def request_termination() -> None:
        nonlocal termination_deadline
        if termination_deadline is not None:
            return
        if pid_namespace:
            try:
                os.kill(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        else:
            _signal_group(process.pid, signal.SIGTERM)
        termination_deadline = time.monotonic() + term_grace_ms / 1000 + 0.25

    def force_termination() -> None:
        if _process_matches_start_time(
            namespace_init_pid, namespace_init_start_time
        ):
            try:
                assert namespace_init_pid is not None
                os.kill(namespace_init_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if pid_namespace:
            try:
                os.kill(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            _signal_group(process.pid, signal.SIGKILL)

    while selector.get_map() or process.poll() is None:
        now = time.monotonic()
        if termination_deadline is None and now >= deadline:
            timed_out = True
            request_termination()
        active_deadline = termination_deadline if termination_deadline is not None else deadline
        if now >= active_deadline:
            force_termination()
            break
        events = selector.select(max(0.0, min(active_deadline - now, 0.05)))
        for key, _mask in events:
            name, limit = key.data
            try:
                fd = key.fileobj if isinstance(key.fileobj, int) else key.fileobj.fileno()
                chunk = os.read(fd, 64 * 1024)
            except BlockingIOError:
                continue
            if not chunk:
                selector.unregister(key.fileobj)
                if isinstance(key.fileobj, int):
                    os.close(key.fileobj)
                    report_read_fd = -1
                else:
                    key.fileobj.close()
                continue
            if name == "control":
                control_buffer.extend(chunk)
                while b"\n" in control_buffer:
                    raw_line, _, remainder = control_buffer.partition(b"\n")
                    control_buffer = bytearray(remainder)
                    line = raw_line.decode("utf-8", "replace")
                    if line.startswith("INIT:"):
                        try:
                            _prefix, raw_pid, raw_start = line.split(":", 2)
                            namespace_init_pid = int(raw_pid)
                            namespace_init_start_time = int(raw_start)
                        except ValueError:
                            control_errors.append(
                                "callback namespace supervisor reported malformed host PID evidence"
                            )
                    elif line == "READY":
                        namespace_ready = True
                    elif line.startswith("ERROR:"):
                        control_errors.append(line)
                    else:
                        control_errors.append(
                            f"callback namespace supervisor reported unexpected evidence: {line}"
                        )
                continue
            counts[name] += len(chunk)
            capacity = limit - len(buffers[name])
            if capacity > 0:
                buffers[name].extend(chunk[:capacity])
            if terminate_on_output_limit and counts[name] > limit:
                output_limit_hit = True
                request_termination()

    final_deadline = (
        termination_deadline
        if termination_deadline is not None
        else max(deadline, time.monotonic() + 0.2)
    )
    try:
        exit_code = process.wait(timeout=max(0.01, final_deadline - time.monotonic()))
    except subprocess.TimeoutExpired as error:
        force_termination()
        try:
            exit_code = process.wait(timeout=0.25)
        except subprocess.TimeoutExpired:
            raise OracleError("command could not be reaped within its wall-clock deadline") from error

    selector.close()
    if report_read_fd >= 0:
        os.close(report_read_fd)
    for stream in (process.stdout, process.stderr):
        if stream.closed:
            continue
        stream.close()

    if pid_namespace:
        containment_deadline = time.monotonic() + 0.25
        while _process_matches_start_time(
            namespace_init_pid, namespace_init_start_time
        ) and time.monotonic() < containment_deadline:
            if not _process_matches_start_time(
                namespace_init_pid, namespace_init_start_time
            ):
                break
            time.sleep(0.005)
        group_reaped = not _process_matches_start_time(
            namespace_init_pid, namespace_init_start_time
        )
    else:
        group_reaped = _reap_descendants(process.pid, time.monotonic() + 0.2)
        if not group_reaped:
            _signal_group(process.pid, signal.SIGKILL)
            group_reaped = _reap_descendants(process.pid, time.monotonic() + 2.0)
    if not group_reaped:
        raise OracleError("command process group was not fully reaped")
    if pid_namespace and control_errors:
        raise OracleError(f"callback namespace setup failed: {control_errors}")
    if pid_namespace and not (timed_out or output_limit_hit) and not namespace_ready:
        raise OracleError("callback namespace setup did not report READY")

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
        namespace_init_pid=namespace_init_pid,
        namespace_init_start_time=namespace_init_start_time,
    )


class FrozenToolDirectory:
    def __init__(
        self,
        path: pathlib.Path,
        aliases: Mapping[str, str],
        fd: int,
        identity: os.stat_result,
        parent: HeldDirectory,
        basename: str,
    ):
        self.path = path
        self.aliases = dict(aliases)
        self.fd = fd
        self.identity = identity
        self.parent = parent
        self.basename = basename
        self.child_fd = CONTROLLED_PATH_FD

    @property
    def child_path(self) -> str:
        return f"/proc/self/fd/{self.child_fd}"

    @classmethod
    def create(
        cls, path: pathlib.Path, aliases: Mapping[str, HeldExecutable]
    ) -> "FrozenToolDirectory":
        parent = HeldDirectory.open(path.parent)
        basename = path.name
        try:
            os.mkdir(basename, mode=0o700, dir_fd=parent.fd)
        except BaseException:
            parent.close()
            raise
        expected: dict[str, str] = {}
        fd = -1
        try:
            fd = os.open(
                basename,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent.fd,
            )
            for alias, tool in aliases.items():
                if not alias or "/" in alias or alias in {".", ".."}:
                    raise OracleError(f"invalid controlled tool alias: {alias!r}")
                target = f"/proc/self/fd/{tool.fd}"
                os.symlink(target, alias, dir_fd=fd)
                expected[alias] = target
            os.fchmod(fd, 0o500)
            directory = cls(path, expected, fd, os.fstat(fd), parent, basename)
            directory.verify_frozen()
            return directory
        except BaseException:
            if fd >= 0:
                os.close(fd)
            parent.close()
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
        errors: list[BaseException] = []
        if self.fd >= 0:
            try:
                os.close(self.fd)
            except BaseException as error:
                errors.append(error)
            self.fd = -1
        try:
            self.parent.close()
        except BaseException as error:
            errors.append(error)
        if errors:
            raise OracleError(f"controlled alias close failed: {errors}")

    def remove_path(self) -> None:
        self.verify_frozen()
        _remove_held_directory(
            self.parent.fd,
            self.basename,
            self.fd,
            "controlled alias directory",
        )

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
    tracked_tree: Mapping[str, tuple[str, str]] | None = None,
    limits: ArtifactLimits = ArtifactLimits(),
) -> list[dict[str, object]]:
    owned_root = not isinstance(root, HeldDirectory)
    held = HeldDirectory.open(root) if owned_root else root
    try:
        current, _directories = _tree_entries(held)
        return _scan_artifacts_from_entries(
            held, baseline, current, tracked_tree or {}, limits
        )
    finally:
        if owned_root:
            held.close()


def _scan_artifacts_from_entries(
    root: HeldDirectory,
    baseline: Mapping[str, tuple[object, ...]],
    current: Mapping[str, tuple[object, ...]],
    tracked_tree: Mapping[str, tuple[str, str]],
    limits: ArtifactLimits,
) -> list[dict[str, object]]:
    changed = [path for path, fingerprint in current.items() if baseline.get(path) != fingerprint]
    semantically_unchanged_tracked: set[str] = set()
    for path in changed:
        expected = tracked_tree.get(path)
        if expected is None:
            continue
        expected_mode, expected_oid = expected
        metadata = root.lstat(path)
        if expected_mode == "120000":
            matches = stat.S_ISLNK(metadata.st_mode) and (
                _git_blob_oid_bytes(root.readlink_bytes(path)) == expected_oid
            )
        else:
            required_permissions = 0o755 if expected_mode == "100755" else 0o644
            if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != required_permissions:
                matches = False
            else:
                fd = root.open_regular(path)
                try:
                    matches = _git_blob_oid_fd(fd, os.fstat(fd).st_size) == expected_oid
                finally:
                    os.close(fd)
        if matches:
            semantically_unchanged_tracked.add(path)
    changed = [path for path in changed if path not in semantically_unchanged_tracked]
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


def _parse_utc_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise OracleError(f"{field} must be an RFC3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as error:
        raise OracleError(f"{field} must be a valid RFC3339 timestamp") from error
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise OracleError(f"{field} must use UTC")
    return parsed


def _validate_provenance_timestamp_order(document: Mapping[str, object]) -> None:
    primary = _require_object(document.get("primary"), "primary")
    rebuild = _require_object(document.get("rebuild"), "rebuild")
    callback = _require_object(document.get("callback"), "callback")
    cleanup = _require_object(document.get("cleanup"), "cleanup")
    ordered = [
        ("primary.started_at_utc", primary.get("started_at_utc")),
        ("primary.finished_at_utc", primary.get("finished_at_utc")),
        ("rebuild.started_at_utc", rebuild.get("started_at_utc")),
        ("rebuild.finished_at_utc", rebuild.get("finished_at_utc")),
        ("callback.started_at_utc", callback.get("started_at_utc")),
        ("callback.finished_at_utc", callback.get("finished_at_utc")),
        ("cleanup.completed_at_utc", cleanup.get("completed_at_utc")),
        ("published_at_utc", document.get("published_at_utc")),
    ]
    parsed = [(_parse_utc_timestamp(value, field), field) for field, value in ordered]
    for (earlier, _earlier_field), (later, later_field) in zip(parsed, parsed[1:]):
        if later < earlier:
            raise OracleError(f"{later_field} is earlier than the preceding Oracle stage")


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


def _rename_noreplace(directory_fd: int, source: str, target: str) -> None:
    rename_noreplace = 1
    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    target_bytes = os.fsencode(target)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is not None:
        result = renameat2(
            directory_fd,
            source_bytes,
            directory_fd,
            target_bytes,
            rename_noreplace,
        )
    else:
        result = libc.syscall(
            316,
            directory_fd,
            source_bytes,
            directory_fd,
            target_bytes,
            rename_noreplace,
        )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise OracleError(f"output target already exists: {target}")
    raise OSError(error, f"renameat2 RENAME_NOREPLACE: {os.strerror(error)}")


def publish_provenance(
    path: pathlib.Path | CandidateTarget,
    document: Mapping[str, object],
    *,
    close_target: bool = False,
) -> None:
    """Publish final provenance only with same-directory atomic no-replace rename."""
    owned_target = not isinstance(path, CandidateTarget)
    target = CandidateTarget.open(path) if owned_target else path
    close_after_publication = owned_target or close_target
    published = False
    completed = False
    rollback_fd = -1
    rollback_identity: os.stat_result | None = None
    published_fd = -1
    published_identity: os.stat_result | None = None
    payload_sha256 = ""

    def rollback_publication() -> None:
        if rollback_fd < 0 or published_identity is None:
            raise OracleError("output publication rollback identity is unavailable")
        try:
            visible_fd = os.open(
                target.basename,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=rollback_fd,
            )
        except FileNotFoundError as error:
            raise OracleError(
                "output publication rollback is ambiguous because final is missing"
            ) from error
        try:
            visible_identity = os.fstat(visible_fd)
            if not _same_identity(published_identity, visible_identity):
                raise OracleError(
                    "output publication rollback refused a replacement final"
                )
            if (
                not stat.S_ISREG(visible_identity.st_mode)
                or visible_identity.st_size != len(payload)
                or _sha256_fd(visible_fd) != payload_sha256
            ):
                raise OracleError(
                    "output publication rollback refused changed final content"
                )
        finally:
            os.close(visible_fd)
        os.unlink(target.basename, dir_fd=rollback_fd)
        os.fsync(rollback_fd)

    try:
        if close_after_publication:
            rollback_fd = os.dup(target.parent.fd)
            rollback_identity = os.fstat(rollback_fd)
        if document.get("schema_version") == PROVENANCE_SCHEMA:
            _validate_provenance_timestamp_order(document)
        payload = canonical_json_bytes(document)
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        if len(payload) > MAX_JSON_BYTES:
            raise OracleError(
                f"final provenance exceeds {MAX_JSON_BYTES} byte JSON limit"
            )
        target.reject_existing()
        temporary = (
            f".{target.basename}.provenance-{os.getpid()}-{time.monotonic_ns()}"
        )
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
            dir_fd=target.parent.fd,
        )
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OracleError("final provenance write made no progress")
                view = view[written:]
            os.fsync(fd)
            temporary_identity = os.fstat(fd)
            os.close(fd)
            fd = -1
            target.verify_visible_parent()
            _rename_noreplace(target.parent.fd, temporary, target.basename)
            published = True
            published_fd = os.open(
                target.basename,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=target.parent.fd,
            )
            published_identity = os.fstat(published_fd)
            if (
                not _same_identity(temporary_identity, published_identity)
                or not stat.S_ISREG(published_identity.st_mode)
                or published_identity.st_size != len(payload)
                or _sha256_fd(published_fd) != payload_sha256
            ):
                raise OracleError("published provenance differs from its held temporary file")
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
    finally:
        if close_after_publication:
            close_error: BaseException | None = None
            try:
                target.close()
            except BaseException as error:
                close_error = error
            parent_error: BaseException | None = None
            if published:
                try:
                    visible = HeldDirectory.open_absolute_nofollow(target.parent_path)
                    try:
                        assert rollback_identity is not None
                        if not _same_directory_object(rollback_identity, visible.stat):
                            raise OracleError(
                                "output parent identity changed while its original handle closed"
                            )
                    finally:
                        visible.close()
                except BaseException as error:
                    parent_error = error
            if published and (not completed or close_error is not None or parent_error is not None):
                rollback_error: BaseException | None = None
                try:
                    rollback_publication()
                except BaseException as error:
                    rollback_error = error
                if published_fd >= 0:
                    try:
                        os.close(published_fd)
                    except BaseException as error:
                        if rollback_error is None:
                            rollback_error = error
                    published_fd = -1
                if rollback_fd >= 0:
                    try:
                        os.close(rollback_fd)
                    except BaseException as error:
                        if rollback_error is None:
                            rollback_error = error
                if rollback_error is not None:
                    raise OracleError(
                        "output-parent close/identity verification failed and publication rollback failed: "
                        f"close={close_error}; identity={parent_error}; rollback={rollback_error}"
                    ) from (close_error or parent_error)
                if close_error is not None:
                    raise close_error
                if parent_error is not None:
                    raise OracleError(
                        f"output-parent identity verification failed after close: {parent_error}"
                    ) from parent_error
            if published_fd >= 0:
                os.close(published_fd)
            if rollback_fd >= 0:
                os.close(rollback_fd)


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


def _remove_held_directory(
    parent_fd: int, directory_name: str, held_fd: int, label: str
) -> None:
    visible_fd = os.open(
        directory_name,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        dir_fd=parent_fd,
    )
    try:
        held_identity = os.fstat(held_fd)
        visible_identity = os.fstat(visible_fd)
        if not _same_directory_object(held_identity, visible_identity):
            raise OracleError(f"{label} path no longer names its held directory")
        os.fchmod(visible_fd, 0o700)
        _remove_directory_contents(visible_fd)
    finally:
        os.close(visible_fd)
    final_fd = os.open(
        directory_name,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        dir_fd=parent_fd,
    )
    try:
        if not _same_directory_object(os.fstat(held_fd), os.fstat(final_fd)):
            raise OracleError(f"{label} path changed before final removal")
    finally:
        os.close(final_fd)
    os.rmdir(directory_name, dir_fd=parent_fd)


def _remove_runtime_directory(
    parent_fd: int, runtime_name: str, runtime_root: HeldDirectory
) -> None:
    _remove_held_directory(
        parent_fd, runtime_name, runtime_root.fd, "runtime directory"
    )


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


def _fixed_commit_tree(
    source: HeldDirectory,
    git_dir: HeldDirectory,
    git: HeldExecutable,
    env: Mapping[str, str],
) -> dict[str, tuple[str, str]]:
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
    return expected


def _validate_pristine_source_tree(
    source: HeldDirectory,
    git_dir: HeldDirectory,
    git: HeldExecutable,
    env: Mapping[str, str],
) -> tuple[dict[str, tuple[object, ...]], dict[str, tuple[str, str]]]:
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
    expected = _fixed_commit_tree(source, git_dir, git, env)

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
    return entries, expected


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
            if tool.role == "cc-component-cc1":
                stable_lines = [
                    line.strip()
                    for line in version.splitlines()
                    if line.strip()
                ][:2]
                if not stable_lines or "GNU" not in stable_lines[0]:
                    raise OracleError("cc1 returned no stable GNU version identity")
                version = "\n".join(stable_lines)
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


def _load_json_object(held: HeldRegularFile, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise OracleError(f"{label} contains duplicate JSON key: {key!r}")
            result[key] = value
        return result

    try:
        decoded = held.read_bytes().decode("utf-8", "strict")
        document = json.loads(decoded, object_pairs_hook=reject_duplicates)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise OracleError(f"{label} is not strict JSON: {error}") from error
    if not isinstance(document, dict):
        raise OracleError(f"{label} root must be an object")
    return document


def _require_object(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise OracleError(f"{field} must be an object")
    return value


def _require_list(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise OracleError(f"{field} must be an array")
    return value


def _require_exact_keys(value: Mapping[str, object], keys: set[str], field: str) -> None:
    actual = set(value)
    if actual != keys:
        raise OracleError(
            f"{field} keys differ: missing={sorted(keys - actual)}, unknown={sorted(actual - keys)}"
        )


def _validate_artifact_document(
    root: HeldDirectory,
    document: Mapping[str, object],
    tracked_tree: Mapping[str, tuple[str, str]],
) -> None:
    artifacts = _require_list(document.get("artifacts"), "artifacts")
    if not artifacts or len(artifacts) > ArtifactLimits().max_count:
        raise OracleError("artifact manifest count is invalid")
    paths: list[str] = []
    by_path: dict[str, dict[str, object]] = {}
    total_bytes = 0
    for index, raw_entry in enumerate(artifacts):
        entry = _require_object(raw_entry, f"artifacts[{index}]")
        kind = entry.get("kind")
        path = entry.get("path")
        if not isinstance(path, str):
            raise OracleError(f"artifacts[{index}].path must be a string")
        HeldDirectory._parts(path)
        if path in by_path:
            raise OracleError(f"duplicate artifact path: {path}")
        metadata = root.lstat(path)
        if kind == "regular":
            _require_exact_keys(entry, {"kind", "path", "mode", "size", "sha256"}, f"artifacts[{index}]")
            actual, sha256 = root.regular_evidence(path)
            expected_mode = entry.get("mode")
            expected_size = entry.get("size")
            expected_sha = entry.get("sha256")
            if (
                isinstance(expected_mode, bool)
                or not isinstance(expected_mode, int)
                or isinstance(expected_size, bool)
                or not isinstance(expected_size, int)
                or not isinstance(expected_sha, str)
                or actual.st_mode != expected_mode
                or actual.st_size != expected_size
                or sha256 != expected_sha
            ):
                raise OracleError(f"regular artifact evidence changed: {path}")
            total_bytes += actual.st_size
            if total_bytes > ArtifactLimits().max_total_bytes:
                raise OracleError("artifact manifest exceeds total byte bound")
        elif kind == "symlink":
            _require_exact_keys(entry, {"kind", "path", "mode", "target"}, f"artifacts[{index}]")
            target = entry.get("target")
            expected_mode = entry.get("mode")
            if (
                isinstance(expected_mode, bool)
                or not isinstance(expected_mode, int)
                or not isinstance(target, str)
                or not stat.S_ISLNK(metadata.st_mode)
                or metadata.st_mode != expected_mode
                or root.readlink(path) != target
            ):
                raise OracleError(f"symlink artifact evidence changed: {path}")
            _resolve_manifest_symlink(path, target)
        else:
            raise OracleError(f"unsupported artifact kind at {path!r}: {kind!r}")
        paths.append(path)
        by_path[path] = entry
    if paths != sorted(paths, key=os.fsencode):
        raise OracleError("artifact manifest is not source-relative byte ordered")

    for entry in by_path.values():
        if entry["kind"] != "symlink":
            continue
        current_path = str(entry["path"])
        visited: set[str] = set()
        for _depth in range(9):
            if current_path in visited:
                raise OracleError(f"artifact symlink cycle at {entry['path']!r}")
            visited.add(current_path)
            target_entry = by_path.get(current_path)
            if target_entry is None:
                raise OracleError(f"artifact symlink target is absent: {current_path!r}")
            if target_entry["kind"] == "regular":
                break
            current_path = _resolve_manifest_symlink(
                str(target_entry["path"]), str(target_entry["target"])
            )
        else:
            raise OracleError(f"artifact symlink depth exceeds eight: {entry['path']!r}")

    tracked_paths = set(tracked_tree)
    artifact_paths = set(by_path)
    overlap = sorted(tracked_paths.intersection(artifact_paths), key=os.fsencode)
    if overlap:
        raise OracleError(f"tracked source entries cannot be declared artifacts: {overlap}")
    entries, directories = _tree_entries(root)
    expected_paths = tracked_paths.union(artifact_paths)
    missing = sorted(expected_paths.difference(entries), key=os.fsencode)
    extra = sorted(set(entries).difference(expected_paths), key=os.fsencode)
    allowed_directories: set[str] = set()
    for path in expected_paths:
        parts = path.split("/")
        for index in range(1, len(parts)):
            allowed_directories.add("/".join(parts[:index]))
    extra_directories = sorted(directories.difference(allowed_directories), key=os.fsencode)
    if missing or extra or extra_directories:
        raise OracleError(
            "artifact closure differs from tracked source plus declared artifacts: "
            f"missing={missing}, extra={extra}, extra_directories={extra_directories}"
        )
    tracked_mismatches: list[str] = []
    for path in sorted(tracked_paths, key=os.fsencode):
        expected_mode, expected_oid = tracked_tree[path]
        metadata = root.lstat(path)
        if expected_mode == "120000":
            if not stat.S_ISLNK(metadata.st_mode):
                tracked_mismatches.append(f"{path}: expected symlink")
                continue
            actual_oid = _git_blob_oid_bytes(root.readlink_bytes(path))
        else:
            required_permissions = 0o755 if expected_mode == "100755" else 0o644
            if not stat.S_ISREG(metadata.st_mode):
                tracked_mismatches.append(f"{path}: expected regular file")
                continue
            if stat.S_IMODE(metadata.st_mode) != required_permissions:
                tracked_mismatches.append(
                    f"{path}: mode {stat.S_IMODE(metadata.st_mode):04o} != {required_permissions:04o}"
                )
                continue
            fd = root.open_regular(path)
            try:
                actual_oid = _git_blob_oid_fd(fd, os.fstat(fd).st_size)
            finally:
                os.close(fd)
        if actual_oid != expected_oid:
            tracked_mismatches.append(f"{path}: Git blob differs from {expected_oid}")
    if tracked_mismatches:
        raise OracleError(f"tracked source closure changed: {tracked_mismatches}")

    redis_server = _require_object(document.get("redis_server"), "redis_server")
    _require_exact_keys(
        redis_server,
        {"artifact_path", "path", "sha256", "identity"},
        "redis_server",
    )
    if redis_server.get("artifact_path") != "src/redis-server":
        raise OracleError("redis_server.artifact_path must equal src/redis-server")
    if redis_server.get("path") != str(root.path / "src/redis-server"):
        raise OracleError("redis_server.path is not bound to the held source")
    binary_stat, binary_sha = root.regular_evidence("src/redis-server")
    if (
        redis_server.get("sha256") != binary_sha
        or redis_server.get("identity") != _file_identity(binary_stat)
        or not binary_stat.st_mode & 0o111
    ):
        raise OracleError("redis_server identity/hash changed")
    binary_entry = by_path.get("src/redis-server")
    if binary_entry is None or binary_entry.get("kind") != "regular":
        raise OracleError("artifact manifest lacks regular src/redis-server")
    if binary_entry.get("sha256") != binary_sha:
        raise OracleError("redis_server SHA differs from its artifact entry")


def _validate_build_document(
    document: Mapping[str, object],
    source_root: HeldDirectory,
    expected_tools: Sequence[Mapping[str, object]],
    tracked_tree: Mapping[str, tuple[str, str]],
) -> None:
    _require_exact_keys(
        document,
        {
            "schema_version",
            "source",
            "recipe",
            "tools",
            "artifacts",
            "redis_server",
            "started_at_utc",
            "finished_at_utc",
        },
        "build evidence",
    )
    if document.get("schema_version") != BUILD_SCHEMA:
        raise OracleError("build evidence schema is not v3")
    source = _require_object(document.get("source"), "source")
    expected_source = {
        "repository": REDIS_REPOSITORY,
        "tag": REDIS_TAG,
        "commit": REDIS_COMMIT,
        "head": REDIS_COMMIT,
        "tag_commit": REDIS_COMMIT,
        "root_path": str(source_root.path),
        "git_dir_path": str(source_root.path / ".git"),
        "tracked_untracked_clean": True,
    }
    if source != expected_source:
        raise OracleError("build source evidence is not exact or held-source bound")
    if document.get("recipe") != _recipe():
        raise OracleError("build recipe evidence differs from the fixed v3 recipe")
    actual_tools = document.get("tools")
    if actual_tools != list(expected_tools):
        if not isinstance(actual_tools, list):
            detail = "metadata tools is not an array"
        elif len(actual_tools) != len(expected_tools):
            detail = f"count metadata={len(actual_tools)} verifier={len(expected_tools)}"
        else:
            detail = "unknown tool difference"
            for index, (actual, expected) in enumerate(zip(actual_tools, expected_tools)):
                if actual != expected:
                    actual_role = actual.get("role") if isinstance(actual, dict) else None
                    expected_role = expected.get("role")
                    differing = sorted(
                        key
                        for key in set(actual if isinstance(actual, dict) else {}).union(expected)
                        if not isinstance(actual, dict) or actual.get(key) != expected.get(key)
                    )
                    detail = (
                        f"index={index} metadata_role={actual_role!r} "
                        f"verifier_role={expected_role!r} fields={differing}"
                    )
                    break
        raise OracleError(
            f"build tool evidence differs from independently held tools: {detail}"
        )
    if not isinstance(document.get("started_at_utc"), str) or not isinstance(
        document.get("finished_at_utc"), str
    ):
        raise OracleError("build timestamps must be strings")
    _validate_artifact_document(source_root, document, tracked_tree)


def _compare_builds(
    primary: Mapping[str, object], rebuild: Mapping[str, object]
) -> dict[str, bool]:
    primary_source = _require_object(primary.get("source"), "primary.source")
    rebuild_source = _require_object(rebuild.get("source"), "rebuild.source")
    source_fields = ("repository", "tag", "commit", "head", "tag_commit")
    source_equal = all(primary_source.get(field) == rebuild_source.get(field) for field in source_fields)
    comparisons = {
        "manifests_equal": primary.get("artifacts") == rebuild.get("artifacts"),
        "redis_server_sha256_equal": _require_object(
            primary.get("redis_server"), "primary.redis_server"
        ).get("sha256")
        == _require_object(rebuild.get("redis_server"), "rebuild.redis_server").get("sha256"),
        "source_identity_equal": source_equal,
        "recipe_equal": primary.get("recipe") == rebuild.get("recipe"),
        "toolchain_equal": primary.get("tools") == rebuild.get("tools"),
    }
    failed = [field for field, equal in comparisons.items() if not equal]
    if failed:
        raise OracleError(f"primary/rebuild equality failed before Redis startup: {failed}")
    return comparisons


def _reserve_child_directory(parent: HeldDirectory, prefix: str) -> tuple[str, HeldDirectory]:
    for attempt in range(100):
        name = f".{prefix}-{os.getpid()}-{time.monotonic_ns()}-{attempt}"
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent.fd)
        except FileExistsError:
            continue
        return name, parent.open_directory(name)
    raise OracleError(f"unable to reserve {prefix} directory")


def _require_command_success(result: RunResult, label: str) -> None:
    if result.timed_out or result.output_truncated or result.exit_code != 0:
        raise OracleError(
            f"{label} failed: exit={result.exit_code}, timeout={result.timed_out}, "
            f"truncated={result.output_truncated}; "
            f"stderr={result.stderr.decode('utf-8', 'replace')}"
        )


def _object_file_identities(root: HeldDirectory) -> set[tuple[int, int]]:
    identities: set[tuple[int, int]] = set()
    pending = [os.dup(root.fd)]
    while pending:
        directory_fd = pending.pop()
        try:
            for name in os.listdir(directory_fd):
                metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if stat.S_ISDIR(metadata.st_mode):
                    pending.append(
                        os.open(
                            name,
                            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=directory_fd,
                        )
                    )
                elif stat.S_ISREG(metadata.st_mode):
                    if metadata.st_nlink != 1:
                        raise OracleError("independent Git object storage contains hardlinks")
                    identities.add((metadata.st_dev, metadata.st_ino))
                else:
                    raise OracleError("independent Git object storage contains non-regular entry")
        finally:
            os.close(directory_fd)
    return identities


def _clone_independent_source(
    source_root: HeldDirectory,
    verifier_root: HeldDirectory,
    git: HeldExecutable,
    tools: Sequence[HeldExecutable],
    env: Mapping[str, str],
    aliases_directory: FrozenToolDirectory,
) -> tuple[HeldDirectory, dict[str, tuple[str, str]]]:
    checkout_name = "checkout-b"
    checkout_path = verifier_root.path / checkout_name
    result = run_bounded(
        git,
        [
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "protocol.file.allow=always",
            "clone",
            "--no-hardlinks",
            "--no-checkout",
            f"/proc/self/fd/{source_root.fd}",
            f"/proc/self/fd/{verifier_root.fd}/{checkout_name}",
        ],
        env=env,
        timeout_ms=COMMAND_TIMEOUT_MS,
        term_grace_ms=1_000,
        stdout_limit_bytes=1024 * 1024,
        stderr_limit_bytes=1024 * 1024,
        extra_fds=(verifier_root.fd, source_root.fd, *(tool.fd for tool in tools)),
        readonly_bind_directories=(aliases_directory,),
    )
    _require_command_success(result, "fresh independent checkout clone")
    checkout = HeldDirectory.open(checkout_path)
    git_dir: HeldDirectory | None = None
    source_git: HeldDirectory | None = None
    try:
        git_dir = checkout.open_directory(".git")
        for args in (
            ["checkout", "--detach", REDIS_COMMIT],
            ["remote", "set-url", "origin", REDIS_REPOSITORY],
        ):
            _git_bytes(git, checkout, git_dir, args, env)
        _validate_git_trust_root(checkout, git_dir, git, env)
        _baseline, tracked_tree = _validate_pristine_source_tree(checkout, git_dir, git, env)
        _validate_source(checkout, git_dir, git, env)
        source_git = source_root.open_directory(".git")
        source_objects = source_git.open_directory("objects")
        checkout_objects = git_dir.open_directory("objects")
        try:
            source_ids = _object_file_identities(source_objects)
            checkout_ids = _object_file_identities(checkout_objects)
            if source_ids.intersection(checkout_ids):
                raise OracleError("fresh checkout shares Git object identities with source A")
        finally:
            checkout_objects.close()
            source_objects.close()
        reopened = HeldDirectory.open(checkout_path)
        if not _same_directory_object(checkout.stat, reopened.stat):
            reopened.close()
            raise OracleError("checkout B root was replaced during exact checkout")
        checkout.close()
        checkout = reopened
        source_root.verify_path()
        return checkout, tracked_tree
    except BaseException:
        checkout.close()
        raise
    finally:
        if source_git is not None:
            source_git.close()
        if git_dir is not None:
            git_dir.close()


def _validate_built_source_revision(
    source: HeldDirectory,
    git: HeldExecutable,
    env: Mapping[str, str],
) -> dict[str, tuple[str, str]]:
    git_dir = source.open_directory(".git")
    try:
        _validate_git_trust_root(source, git_dir, git, env)
        head = _git_text(git, source, git_dir, ["rev-parse", "HEAD"], env)
        tag_commit = _git_text(
            git, source, git_dir, ["rev-parse", f"{REDIS_TAG}^{{commit}}"], env
        )
        repository = _git_text(
            git, source, git_dir, ["remote", "get-url", "origin"], env
        )
        if head != REDIS_COMMIT or tag_commit != REDIS_COMMIT or repository != REDIS_REPOSITORY:
            raise OracleError("primary source revision/origin is no longer exact Redis 8.8.1")
        return _fixed_commit_tree(source, git_dir, git, env)
    finally:
        git_dir.close()


def _build_rebuild_candidate(
    checkout: HeldDirectory,
    verifier_root: HeldDirectory,
    python: HeldExecutable,
    controller: HeldExecutable,
    tools: Sequence[HeldExecutable],
    env: Mapping[str, str],
) -> tuple[HeldRegularFile, dict[str, object]]:
    metadata_path = verifier_root.path / "rebuild-build.json"
    result = run_bounded(
        python,
        [
            str(python.path),
            "-I",
            "-B",
            f"/proc/self/fd/{controller.fd}",
            "--bootstrap-python-path",
            str(python.path),
            "--bootstrap-python-fd",
            str(python.fd),
            "--bootstrap-controller-path",
            str(controller.path),
            "--bootstrap-controller-fd",
            str(controller.fd),
            "--source",
            str(checkout.path),
            "--metadata",
            str(metadata_path),
        ],
        env=env,
        timeout_ms=BUILD_TIMEOUT_MS + COMMAND_TIMEOUT_MS,
        term_grace_ms=TERM_GRACE_MS,
        stdout_limit_bytes=BUILD_OUTPUT_LIMIT,
        stderr_limit_bytes=BUILD_OUTPUT_LIMIT,
        extra_fds=(
            verifier_root.fd,
            checkout.fd,
            controller.fd,
            python.fd,
            *(tool.fd for tool in tools),
        ),
    )
    _require_command_success(result, "independent Redis rebuild")
    metadata = HeldRegularFile.open(metadata_path, MAX_JSON_BYTES)
    document = _load_json_object(metadata, "rebuild metadata")
    return metadata, document


def _read_resp_line(connection: socket.socket, limit: int) -> bytes:
    line = bytearray()
    while len(line) <= limit:
        chunk = connection.recv(1)
        if not chunk:
            raise OracleError("Redis runtime closed INFO response early")
        line.extend(chunk)
        if line.endswith(b"\r\n"):
            return bytes(line[:-2])
    raise OracleError("Redis runtime response line exceeds byte bound")


def _query_redis_info(host: str, port: int) -> tuple[list[str], list[int]]:
    with socket.create_connection((host, port), timeout=1.0) as connection:
        connection.settimeout(1.0)
        connection.sendall(b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n")
        header = _read_resp_line(connection, 128)
        if not header.startswith(b"$"):
            raise OracleError(f"Redis INFO did not return a bulk string: {header!r}")
        try:
            size = int(header[1:])
        except ValueError as error:
            raise OracleError("Redis INFO returned an invalid bulk length") from error
        if size <= 0 or size > 1024 * 1024:
            raise OracleError("Redis INFO bulk length is outside the byte bound")
        payload = bytearray()
        while len(payload) < size + 2:
            chunk = connection.recv(size + 2 - len(payload))
            if not chunk:
                raise OracleError("Redis INFO bulk payload ended early")
            payload.extend(chunk)
        if payload[-2:] != b"\r\n":
            raise OracleError("Redis INFO bulk payload lacks CRLF")
        text = bytes(payload[:-2]).decode("utf-8", "strict")
    versions = [
        line.split(":", 1)[1]
        for line in text.splitlines()
        if line.startswith("redis_version:")
    ]
    raw_process_ids = [
        line.split(":", 1)[1]
        for line in text.splitlines()
        if line.startswith("process_id:")
    ]
    try:
        process_ids = [int(value) for value in raw_process_ids]
    except ValueError as error:
        raise OracleError("Redis INFO process_id must be a decimal integer") from error
    return versions, process_ids


def _start_redis_runtime(
    binary: HeldExecutable,
    runtime_root: HeldDirectory,
    logs_root: HeldDirectory,
) -> tuple[subprocess.Popen[bytes], int, dict[str, object], int]:
    log_fd = os.open(
        "redis.log",
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
        dir_fd=logs_root.fd,
    )
    process: subprocess.Popen[bytes] | None = None
    port = 0
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = int(reservation.getsockname()[1])
        process = subprocess.Popen(
            [
                str(binary.path),
                "--bind",
                "127.0.0.1",
                "--port",
                str(port),
                "--protected-mode",
                "no",
                "--save",
                "",
                "--appendonly",
                "no",
                "--daemonize",
                "no",
                "--dir",
                f"/proc/self/fd/{runtime_root.fd}",
                "--dbfilename",
                "oracle.rdb",
                "--loglevel",
                "warning",
            ],
            executable=f"/proc/self/fd/{binary.fd}",
            stdin=subprocess.DEVNULL,
            stdout=log_fd,
            stderr=log_fd,
            env={
                "PATH": "/usr/bin:/bin",
                "HOME": f"/proc/self/fd/{runtime_root.fd}",
                "TMPDIR": f"/proc/self/fd/{runtime_root.fd}",
                "LC_ALL": "C",
                "LANG": "C",
                "TZ": "UTC",
            },
            cwd=f"/proc/self/fd/{runtime_root.fd}",
            close_fds=True,
            pass_fds=(binary.fd, runtime_root.fd),
            start_new_session=True,
        )
    except BaseException:
        os.close(log_fd)
        raise
    assert process is not None
    try:
        deadline = time.monotonic() + REDIS_START_TIMEOUT_MS / 1000
        info: tuple[list[str], list[int]] | None = None
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise OracleError(
                    f"held rebuild Redis exited before readiness: {process.returncode}"
                )
            try:
                info = _query_redis_info("127.0.0.1", port)
                break
            except (ConnectionError, OSError, OracleError):
                time.sleep(0.05)
        if info is None:
            raise OracleError("held rebuild Redis did not become ready before deadline")
        info_versions, info_process_ids = info
        if info_versions != [REDIS_TAG]:
            raise OracleError(
                f"Redis INFO server must contain exactly one redis_version:{REDIS_TAG}; got {info_versions}"
            )
        if info_process_ids != [process.pid]:
            raise OracleError(
                "Redis INFO server must contain exactly one process_id matching the "
                f"held runtime PID {process.pid}; got {info_process_ids}"
            )
        proc_identity = os.stat(f"/proc/{process.pid}/exe")
        if not _same_identity(binary.stat, proc_identity):
            raise OracleError("Redis /proc executable identity differs from held rebuild binary")
        binary.verify_path()
        return process, port, {
            "build_role": "rebuild",
            "binary_path": str(binary.path),
            "binary_sha256": binary.sha256,
            "binary_identity": _file_identity(binary.stat),
            "held_fd": True,
            "pid": process.pid,
            "info_redis_versions": info_versions,
        }, log_fd
    except BaseException as business_error:
        cleanup_errors: list[BaseException] = []
        for action in (
            lambda: _stop_process_group(process, "failed Redis startup"),
            lambda: os.close(log_fd),
        ):
            try:
                action()
            except BaseException as error:
                cleanup_errors.append(error)
        if cleanup_errors:
            raise OracleError(
                f"{business_error}; Redis startup cleanup failed: {cleanup_errors}"
            ) from business_error
        raise


def _stop_process_group(process: subprocess.Popen[bytes], label: str) -> None:
    if process.poll() is None:
        _signal_group(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=TERM_GRACE_MS / 1000)
        except subprocess.TimeoutExpired:
            _signal_group(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
    _signal_group(process.pid, signal.SIGKILL)
    if not _reap_descendants(process.pid, time.monotonic() + 2.0):
        raise OracleError(f"{label} process group was not fully reaped")
    if process.poll() is None:
        raise OracleError(f"{label} process was not reaped")


def _cleanup_redis_runtime(process: subprocess.Popen[bytes], log_fd: int) -> None:
    errors: list[tuple[str, BaseException]] = []
    for label, action in (
        ("process", lambda: _stop_process_group(process, "Redis runtime")),
        ("log fd", lambda: os.close(log_fd)),
    ):
        try:
            action()
        except BaseException as error:
            errors.append((label, error))
    if errors:
        details = "; ".join(
            f"{label}: {type(error).__name__}: {error}" for label, error in errors
        )
        raise OracleError(f"Redis runtime cleanup failed: {details}")


def _write_runtime_evidence(
    runtime_root: HeldDirectory, runtime: Mapping[str, object]
) -> int:
    payload = canonical_json_bytes(runtime)
    fd = os.open(
        "runtime-evidence.json",
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o400,
        dir_fd=runtime_root.fd,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OracleError("runtime evidence write made no progress")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    evidence_fd = runtime_root.open_regular("runtime-evidence.json")
    if _sha256_fd(evidence_fd) != hashlib.sha256(payload).hexdigest():
        os.close(evidence_fd)
        raise OracleError("runtime evidence changed after publication to held FD")
    return evidence_fd


def _run_callback(
    callback_argv: Sequence[str],
    aliases_directory: FrozenToolDirectory,
    callback_root: HeldDirectory,
    callback_input_root: HeldDirectory,
    runtime_evidence_fd: int,
    host: str,
    port: int,
) -> dict[str, object]:
    if not callback_argv or len(callback_argv) > 32:
        raise OracleError("--run-after-ready requires 1..32 callback argv entries")
    executable_path = pathlib.Path(callback_argv[0])
    if not executable_path.is_absolute():
        raise OracleError("callback executable must use an absolute path")
    working = callback_root.path / "work"
    working.mkdir(mode=0o700)
    (working / "home").mkdir(mode=0o700)
    (working / "tmp").mkdir(mode=0o700)
    sandbox_root = callback_root.path / "sandbox-root"
    sandbox_root.mkdir(mode=0o700)
    work_root: HeldDirectory | None = None
    held_sandbox_root: HeldDirectory | None = None
    executable: HeldExecutable | None = None
    try:
        work_root = callback_root.open_directory("work")
        held_sandbox_root = callback_root.open_directory("sandbox-root")
        executable = HeldExecutable.open("callback", executable_path)
        aliases_directory.verify_frozen()
        env = _sanitized_environment(
            "/usr/bin:/bin", pathlib.Path("/work/home"), pathlib.Path("/work/tmp")
        )
        env.update(
            {
                "KIWI_REDIS_ORACLE_HOST": host,
                "KIWI_REDIS_ORACLE_PORT": str(port),
                "KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE": "/runtime-evidence.json",
                "KIWI_REDIS_ORACLE_CALLBACK_INPUT": "/callback-input",
                "KIWI_REDIS_ORACLE_WORKDIR": "/work",
            }
        )
        result = run_bounded(
            executable,
            callback_argv,
            env=env,
            timeout_ms=CALLBACK_TIMEOUT_MS,
            term_grace_ms=TERM_GRACE_MS,
            stdout_limit_bytes=CALLBACK_OUTPUT_LIMIT,
            stderr_limit_bytes=CALLBACK_OUTPUT_LIMIT,
            extra_fds=(
                work_root.fd,
                callback_input_root.fd,
                runtime_evidence_fd,
                held_sandbox_root.fd,
            ),
            terminate_on_output_limit=True,
            pid_namespace=True,
            callback_filesystem=(
                work_root.fd,
                callback_input_root.fd,
                runtime_evidence_fd,
                held_sandbox_root.fd,
            ),
        )
        aliases_directory.verify_frozen()
    finally:
        if executable is not None:
            executable.close()
        if held_sandbox_root is not None:
            held_sandbox_root.close()
        if work_root is not None:
            work_root.close()
    if result.timed_out or result.output_truncated or result.exit_code != 0:
        raise OracleError(
            "Oracle callback failed: "
            f"exit={result.exit_code}, timeout={result.timed_out}, "
            f"truncated={result.output_truncated}; "
            f"stderr={result.stderr.decode('utf-8', 'replace')}"
        )
    return {
        "argv": result.argv,
        "timeout_ms": result.timeout_ms,
        "term_grace_ms": result.term_grace_ms,
        "stdout_limit_bytes": result.stdout_limit_bytes,
        "stderr_limit_bytes": result.stderr_limit_bytes,
        "stdout_bytes": result.stdout_bytes,
        "stderr_bytes": result.stderr_bytes,
        "started_at_utc": result.started_at_utc,
        "finished_at_utc": result.finished_at_utc,
        "exit_code": result.exit_code,
        "timed_out": result.timed_out,
        "output_truncated": result.output_truncated,
        "process_group_reaped": result.process_group_reaped,
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


def verify_oracle(
    source_argument: str,
    primary_metadata_argument: str,
    output_argument: str,
    callback_input_argument: str,
    callback_argv: Sequence[str],
    bootstrap_python_path: pathlib.Path,
    bootstrap_python_fd: int,
    bootstrap_controller_path: pathlib.Path,
    bootstrap_controller_fd: int,
) -> None:
    if not sys.flags.isolated or sys.dont_write_bytecode is False:
        raise OracleError("controller must run with Python -I -B")
    if sys.platform != "linux" or os.uname().machine not in {"x86_64", "amd64"}:
        raise OracleError("Redis Oracle verification supports Linux x86_64 only")
    if not callback_argv:
        raise OracleError("--run-after-ready requires callback argv")
    for value in (
        source_argument,
        primary_metadata_argument,
        output_argument,
        callback_input_argument,
    ):
        if not pathlib.Path(value).is_absolute():
            raise OracleError(
                "--source, --primary-metadata, --output, and --callback-input must be absolute"
            )

    target = CandidateTarget.open(output_argument)
    source_root: HeldDirectory | None = None
    primary_metadata: HeldRegularFile | None = None
    controller: HeldExecutable | None = None
    python: HeldExecutable | None = None
    tools: list[HeldExecutable] = []
    aliases_directory: FrozenToolDirectory | None = None
    verifier_name: str | None = None
    verifier_root: HeldDirectory | None = None
    checkout: HeldDirectory | None = None
    rebuild_metadata: HeldRegularFile | None = None
    rebuild_binary: HeldExecutable | None = None
    runtime_root: HeldDirectory | None = None
    logs_root: HeldDirectory | None = None
    callback_root: HeldDirectory | None = None
    callback_input_root: HeldDirectory | None = None
    runtime_evidence_fd = -1
    redis_log_fd = -1
    redis_process: subprocess.Popen[bytes] | None = None
    primary_document: dict[str, object] | None = None
    rebuild_document: dict[str, object] | None = None
    primary_tracked_tree: dict[str, tuple[str, str]] | None = None
    rebuild_tracked_tree: dict[str, tuple[str, str]] | None = None
    comparison: dict[str, bool] | None = None
    runtime_document: dict[str, object] | None = None
    callback_document: dict[str, object] | None = None
    business_error: BaseException | None = None
    cleanup_errors: list[tuple[str, BaseException]] = []
    cleanup_state = {
        "redis_process_reaped": False,
        "process_group_reaped": False,
        "runtime_removed": False,
        "checkout_removed": False,
        "logs_removed": False,
        "temp_removed": False,
        "final_identity_revalidated": False,
        "output_parent_revalidated": False,
    }

    def cleanup(label: str, action: Callable[[], None]) -> None:
        try:
            action()
        except BaseException as error:
            cleanup_errors.append((label, error))

    try:
        source_path = pathlib.Path(source_argument).resolve(strict=True)
        if not source_path.is_dir() or source_path.is_symlink():
            raise OracleError("--source must resolve to a real directory")
        if target.parent_path == source_path or source_path in target.parent_path.parents:
            raise OracleError("final provenance and verifier temp root must be outside source A")
        source_root = HeldDirectory.open(source_path)
        callback_input_path = pathlib.Path(callback_input_argument).resolve(strict=True)
        if not callback_input_path.is_dir() or callback_input_path.is_symlink():
            raise OracleError("--callback-input must resolve to a real directory")
        callback_input_root = HeldDirectory.open(callback_input_path)
        primary_metadata = HeldRegularFile.open(primary_metadata_argument, MAX_JSON_BYTES)
        if primary_metadata.path == target.path:
            raise OracleError("primary metadata and final provenance paths must differ")

        verifier_name, verifier_root = _reserve_child_directory(
            target.parent, "kiwi-oracle-verifier"
        )
        held_verifier_path = pathlib.Path(f"/proc/self/fd/{verifier_root.fd}")
        runtime_path = held_verifier_path / "runtime"
        logs_path = held_verifier_path / "logs"
        callback_path = held_verifier_path / "callback"
        for path in (
            held_verifier_path / "home",
            held_verifier_path / "tmp",
            held_verifier_path / "versions",
            runtime_path,
            logs_path,
            callback_path,
        ):
            path.mkdir(mode=0o700)
        runtime_root = verifier_root.open_directory("runtime")
        logs_root = verifier_root.open_directory("logs")
        callback_root = verifier_root.open_directory("callback")
        home = held_verifier_path / "home"
        temporary = held_verifier_path / "tmp"
        versions = held_verifier_path / "versions"
        tool_path = held_verifier_path / "tools"

        controller = HeldExecutable.from_fd(
            "controller", bootstrap_controller_path, bootstrap_controller_fd
        )
        python = HeldExecutable.from_fd(
            "python", bootstrap_python_path, bootstrap_python_fd
        )
        discovery_env = _sanitized_environment("/usr/bin:/bin", home, temporary)
        tools, aliases, versions_by_role = _register_tools(
            controller, python, discovery_env, versions, tools
        )
        aliases_directory = FrozenToolDirectory.create(tool_path, aliases)
        env = _sanitized_environment(aliases_directory.child_path, home, temporary)
        tool_evidence = _tool_evidence(
            tools, versions_by_role, env, versions, aliases_directory
        )
        _empty_directory(versions, "verifier tool version working directory")
        _empty_directory(home, "verifier HOME")
        _empty_directory(temporary, "verifier TMPDIR")
        git = next(tool for tool in tools if tool.role == "git")

        primary_tracked_tree = _validate_built_source_revision(source_root, git, env)
        primary_document = _load_json_object(primary_metadata, "primary metadata")
        _validate_build_document(
            primary_document, source_root, tool_evidence, primary_tracked_tree
        )
        checkout, rebuild_tracked_tree = _clone_independent_source(
            source_root, verifier_root, git, tools, env, aliases_directory
        )
        rebuild_metadata, rebuild_document = _build_rebuild_candidate(
            checkout,
            verifier_root,
            python,
            controller,
            tools,
            env,
        )
        _validate_build_document(
            rebuild_document, checkout, tool_evidence, rebuild_tracked_tree
        )
        comparison = _compare_builds(primary_document, rebuild_document)

        rebuild_binary = HeldExecutable.open(
            "redis-server", checkout.path / "src/redis-server"
        )
        redis_process, port, runtime_document, redis_log_fd = _start_redis_runtime(
            rebuild_binary, runtime_root, logs_root
        )
        runtime_evidence_fd = _write_runtime_evidence(runtime_root, runtime_document)
        immutable_path_snapshots = {
            home: snapshot_tree(home),
            temporary: snapshot_tree(temporary),
            versions: snapshot_tree(versions),
        }
        checkout_snapshot = _tree_entries(checkout)
        runtime_snapshot = _tree_entries(runtime_root)
        verifier_identity_before_callback = os.fstat(verifier_root.fd)
        runtime_evidence_sha256 = hashlib.sha256(
            canonical_json_bytes(runtime_document)
        ).hexdigest()
        callback_document = _run_callback(
            callback_argv,
            aliases_directory,
            callback_root,
            callback_input_root,
            runtime_evidence_fd,
            "127.0.0.1",
            port,
        )
        if redis_process.poll() is not None:
            raise OracleError("Redis runtime exited during the supervised callback")
        if _query_redis_info("127.0.0.1", port) != ([REDIS_TAG], [redis_process.pid]):
            raise OracleError("Redis runtime INFO identity changed after callback")
        if not _same_identity(
            rebuild_binary.stat, os.stat(f"/proc/{redis_process.pid}/exe")
        ):
            raise OracleError("Redis /proc executable identity changed after callback")
        rebuild_binary.verify_path()
        aliases_directory.verify_frozen()
        _validate_artifact_document(checkout, rebuild_document, rebuild_tracked_tree)
        if _tree_entries(checkout) != checkout_snapshot:
            raise OracleError("callback modified the verifier checkout B resource tree")
        if _tree_entries(runtime_root) != runtime_snapshot:
            raise OracleError("callback modified the verifier runtime resource tree")
        for path, expected in immutable_path_snapshots.items():
            if snapshot_tree(path) != expected:
                raise OracleError(f"callback modified verifier resource directory: {path}")
        verifier_identity_after_callback = os.fstat(verifier_root.fd)
        if (
            not _same_directory_object(
                verifier_identity_before_callback, verifier_identity_after_callback
            )
            or stat.S_IMODE(verifier_identity_after_callback.st_mode) != 0o700
        ):
            raise OracleError("callback changed the verifier temp-root identity or mode")
        rebuild_metadata.verify_path()
        if _sha256_fd(runtime_evidence_fd) != runtime_evidence_sha256:
            raise OracleError("callback modified held runtime evidence")
    except BaseException as error:
        business_error = error

    if redis_process is not None:
        def stop_redis() -> None:
            nonlocal redis_log_fd
            assert redis_process is not None
            try:
                _cleanup_redis_runtime(redis_process, redis_log_fd)
            finally:
                redis_log_fd = -1
            cleanup_state["redis_process_reaped"] = True
            cleanup_state["process_group_reaped"] = True

        cleanup("Redis runtime and log cleanup", stop_redis)
    elif redis_log_fd >= 0:
        cleanup("Redis log close", lambda: os.close(redis_log_fd))
        redis_log_fd = -1
    if runtime_evidence_fd >= 0:
        cleanup("runtime evidence close", lambda: os.close(runtime_evidence_fd))
        runtime_evidence_fd = -1
    if rebuild_binary is not None:
        cleanup("rebuild binary close", rebuild_binary.close)
    if rebuild_metadata is not None:
        cleanup("rebuild metadata close", rebuild_metadata.close)
    if callback_input_root is not None:
        def close_callback_input() -> None:
            assert callback_input_root is not None
            try:
                callback_input_root.verify_path()
            finally:
                callback_input_root.close()

        cleanup("callback input revalidation and close", close_callback_input)

    if verifier_root is not None:
        def remove_runtime() -> None:
            assert verifier_root is not None and runtime_root is not None
            _remove_runtime_directory(verifier_root.fd, "runtime", runtime_root)
            cleanup_state["runtime_removed"] = True

        def remove_checkout() -> None:
            assert verifier_root is not None and checkout is not None
            _remove_runtime_directory(verifier_root.fd, "checkout-b", checkout)
            cleanup_state["checkout_removed"] = True

        def remove_logs() -> None:
            assert verifier_root is not None and logs_root is not None
            _remove_runtime_directory(verifier_root.fd, "logs", logs_root)
            cleanup_state["logs_removed"] = True

        if runtime_root is not None:
            cleanup("runtime remove", remove_runtime)
        if checkout is not None:
            cleanup("checkout B remove", remove_checkout)
        if logs_root is not None:
            cleanup("logs remove", remove_logs)
    if callback_root is not None:
        cleanup("callback directory close", callback_root.close)
    if logs_root is not None:
        cleanup("logs directory close", logs_root.close)
    if runtime_root is not None:
        cleanup("runtime directory close", runtime_root.close)
    if checkout is not None:
        cleanup("checkout B close", checkout.close)
    if aliases_directory is not None:
        cleanup("controlled aliases remove", aliases_directory.remove_path)
        cleanup("controlled aliases close", aliases_directory.close)
    if verifier_root is not None and verifier_name is not None:
        def remove_temp_root() -> None:
            assert verifier_root is not None and verifier_name is not None
            _remove_runtime_directory(target.parent.fd, verifier_name, verifier_root)
            cleanup_state["temp_removed"] = True

        cleanup("verifier temp root remove", remove_temp_root)
        cleanup("verifier temp root close", verifier_root.close)

    if business_error is None and not cleanup_errors:
        def final_revalidation() -> None:
            assert source_root is not None
            assert primary_metadata is not None
            assert primary_document is not None
            assert primary_tracked_tree is not None
            source_root.verify_path()
            primary_metadata.verify_path()
            _validate_artifact_document(
                source_root, primary_document, primary_tracked_tree
            )
            for tool in tools:
                tool.verify_path()
            target.verify_visible_parent()
            target.reject_existing()
            cleanup_state["final_identity_revalidated"] = True
            cleanup_state["output_parent_revalidated"] = True

        cleanup("final evidence identity revalidation", final_revalidation)

    for index, tool in enumerate(tools):
        cleanup(f"tool {index} close", tool.close)
    if controller is not None and all(tool is not controller for tool in tools):
        cleanup("controller close", controller.close)
    if python is not None and all(tool is not python for tool in tools):
        cleanup("Python close", python.close)
    if primary_metadata is not None:
        cleanup("primary metadata close", primary_metadata.close)
    if source_root is not None:
        cleanup("source A close", source_root.close)

    try:
        if cleanup_errors:
            details = "; ".join(
                f"{label}: {type(error).__name__}: {error}"
                for label, error in cleanup_errors
            )
            if business_error is not None:
                raise OracleError(
                    f"{type(business_error).__name__}: {business_error}; cleanup errors: {details}"
                ) from business_error
            raise OracleError(f"verifier cleanup errors: {details}")
        if business_error is not None:
            raise business_error
        if not all(cleanup_state.values()):
            raise OracleError(f"cleanup-before-publish is incomplete: {cleanup_state}")
        if any(
            value is None
            for value in (
                primary_document,
                rebuild_document,
                comparison,
                runtime_document,
                callback_document,
            )
        ):
            raise OracleError("verifier produced incomplete provenance evidence")
        completed_at = _utc_now()
        document = {
            "schema_version": PROVENANCE_SCHEMA,
            "primary": primary_document,
            "rebuild": rebuild_document,
            "comparison": comparison,
            "runtime": runtime_document,
            "callback": callback_document,
            "cleanup": {**cleanup_state, "completed_at_utc": completed_at},
            "published_after_cleanup": True,
            "published_at_utc": _utc_now(),
        }
        publish_provenance(target, document, close_target=True)
        print(str(target.path))
    finally:
        target.close()


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
        baseline, tracked_tree = _validate_pristine_source_tree(
            source_root, git_directory, git, env
        )
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
        artifacts = scan_artifacts(
            source_root, baseline, tracked_tree=tracked_tree
        )
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
        _validate_artifact_document(source_root, document, tracked_tree)
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
        description="Build or independently verify exact Redis 8.8.1 Oracle evidence"
    )
    parser.add_argument("--bootstrap-python-path", required=True, help=argparse.SUPPRESS)
    parser.add_argument("--bootstrap-python-fd", type=int, required=True, help=argparse.SUPPRESS)
    parser.add_argument("--bootstrap-controller-path", required=True, help=argparse.SUPPRESS)
    parser.add_argument("--bootstrap-controller-fd", type=int, required=True, help=argparse.SUPPRESS)
    parser.add_argument("--source", required=True, help="absolute exact Redis 8.8.1 checkout")
    parser.add_argument("--metadata", help="absolute candidate build metadata path")
    parser.add_argument("--primary-metadata", help="absolute primary build metadata path")
    parser.add_argument("--output", help="absolute final provenance path")
    parser.add_argument(
        "--callback-input",
        help="absolute read-only callback input root exposed as /callback-input",
    )
    parser.add_argument(
        "--run-after-ready",
        nargs=argparse.REMAINDER,
        default=None,
        metavar="CALLBACK_ARG",
        help="callback executable and argv run inside the verifier runtime lease",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.metadata is not None:
            if any(
                value is not None
                for value in (
                    arguments.primary_metadata,
                    arguments.output,
                    arguments.callback_input,
                    arguments.run_after_ready,
                )
            ):
                raise OracleError("primary-build and verifier arguments cannot be mixed")
            build_primary(
                arguments.source,
                arguments.metadata,
                pathlib.Path(arguments.bootstrap_python_path),
                arguments.bootstrap_python_fd,
                pathlib.Path(arguments.bootstrap_controller_path),
                arguments.bootstrap_controller_fd,
            )
        else:
            if (
                arguments.primary_metadata is None
                or arguments.output is None
                or arguments.callback_input is None
                or not arguments.run_after_ready
            ):
                raise OracleError(
                    "verifier requires --primary-metadata, --output, --callback-input, and --run-after-ready argv"
                )
            verify_oracle(
                arguments.source,
                arguments.primary_metadata,
                arguments.output,
                arguments.callback_input,
                arguments.run_after_ready,
                pathlib.Path(arguments.bootstrap_python_path),
                arguments.bootstrap_python_fd,
                pathlib.Path(arguments.bootstrap_controller_path),
                arguments.bootstrap_controller_fd,
            )
    except (OracleError, OSError, UnicodeError, ValueError) as error:
        print(f"oracle operation rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
