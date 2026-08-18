# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Small frame-preserving RESP client for trusted-Oracle tests."""

import math
import socket
import time
from dataclasses import dataclass


_monotonic = time.monotonic


@dataclass(frozen=True)
class RespFrameLimits:
    max_frame_bytes: int = 16 * 1024 * 1024
    max_header_bytes: int = 64 * 1024
    max_items: int = 100_000
    max_depth: int = 64

    def __post_init__(self):
        for field in ("max_frame_bytes", "max_header_bytes", "max_items"):
            value = getattr(self, field)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{field} must be a positive integer")
        if type(self.max_depth) is not int or self.max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer")


def _validate_timeout(timeout):
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("timeout must be a finite positive number")
    return float(timeout)


class _ReadBudget:
    def __init__(self, limits, deadline):
        self.limits = limits
        self.deadline = deadline
        self.bytes_consumed = 0
        self.items_consumed = 0

    def remaining_time(self):
        remaining = self.deadline - _monotonic()
        if remaining <= 0:
            raise TimeoutError("RESP frame absolute deadline exceeded")
        return remaining

    def check_deadline(self):
        self.remaining_time()

    def require_bytes(self, length):
        if length > self.limits.max_frame_bytes - self.bytes_consumed:
            raise ValueError("RESP frame byte budget exceeded")

    def consume_bytes(self, length):
        self.check_deadline()
        self.require_bytes(length)
        self.bytes_consumed += length

    def require_items(self, count):
        if count > self.limits.max_items - self.items_consumed:
            raise ValueError("RESP aggregate item budget exceeded")

    def consume_item(self, depth):
        self.check_deadline()
        if depth > self.limits.max_depth:
            raise ValueError("RESP nesting depth budget exceeded")
        self.require_items(1)
        self.items_consumed += 1


def _as_bytes(part):
    if isinstance(part, bytes):
        return part
    if isinstance(part, (bytearray, memoryview)):
        return bytes(part)
    raise TypeError(f"RESP command parts must be bytes-like, got {type(part).__name__}")


def encode_command(*parts):
    """Encode bytes-like command parts without changing their payload bytes."""
    encoded = [f"*{len(parts)}\r\n".encode("ascii")]
    for part in parts:
        payload = _as_bytes(part)
        encoded.extend(
            (f"${len(payload)}\r\n".encode("ascii"), payload, b"\r\n")
        )
    return b"".join(encoded)


class RawRespConnection:
    """A single socket whose replies are returned as complete raw RESP frames."""

    def __init__(self, connection, protocol, timeout=5.0, limits=None):
        self.socket = connection
        self.protocol = protocol
        self.timeout = _validate_timeout(timeout)
        if limits is not None and not isinstance(limits, RespFrameLimits):
            raise TypeError("limits must be a RespFrameLimits instance")
        self.limits = limits or RespFrameLimits()
        self._buffer = bytearray()

    @classmethod
    def connect(cls, host, port, protocol, timeout=5.0, *, limits=None):
        if protocol not in (2, 3):
            raise ValueError(f"RESP protocol must be 2 or 3, got {protocol!r}")
        timeout = _validate_timeout(timeout)
        if limits is not None and not isinstance(limits, RespFrameLimits):
            raise TypeError("limits must be a RespFrameLimits instance")
        connection = socket.create_connection((host, port), timeout=timeout)
        client = cls(connection, protocol, timeout=timeout, limits=limits)
        try:
            if protocol == 3:
                hello = client.execute_raw(b"HELLO", b"3")
                if not hello.startswith(b"%"):
                    raise ValueError(f"RESP3 HELLO must return a map, got {hello!r}")
            return client
        except BaseException:
            client.close()
            raise

    def execute_raw(self, *parts):
        if self.socket is None:
            raise RuntimeError("RESP connection is closed")
        command = encode_command(*parts)
        try:
            budget = _ReadBudget(self.limits, _monotonic() + self.timeout)
            self.socket.settimeout(budget.remaining_time())
            self.socket.sendall(command)
            budget.check_deadline()
            frame = self._read_frame(budget)
            budget.check_deadline()
            return frame
        except BaseException:
            self.close()
            raise

    def close(self):
        connection = self.socket
        self.socket = None
        self._buffer.clear()
        if connection is not None:
            connection.close()

    def _receive(self, budget):
        if self.socket is None:
            raise RuntimeError("RESP connection is closed")
        self.socket.settimeout(budget.remaining_time())
        chunk = self.socket.recv(4096)
        budget.check_deadline()
        if not chunk:
            raise EOFError("RESP connection closed before a complete frame was received")
        self._buffer.extend(chunk)

    def _consume(self, length, budget):
        budget.consume_bytes(length)
        payload = bytes(self._buffer[:length])
        del self._buffer[:length]
        return payload

    def _read_line(self, budget):
        while True:
            end = self._buffer.find(b"\r\n")
            if end >= 0:
                end += 2
                if end > budget.limits.max_header_bytes:
                    raise ValueError("RESP header exceeds its byte budget")
                return self._consume(end, budget)
            if len(self._buffer) >= budget.limits.max_header_bytes:
                raise ValueError("RESP header exceeds its byte budget")
            self._receive(budget)

    def _read_exact(self, length, budget):
        while len(self._buffer) < length:
            self._receive(budget)
        return self._consume(length, budget)

    @staticmethod
    def _frame_length(header, *, allow_null=False):
        encoded_length = header[1:-2]
        if encoded_length == b"-1" and allow_null:
            return -1
        if encoded_length == b"0":
            return 0
        if not (
            encoded_length
            and encoded_length[0] in b"123456789"
            and encoded_length.isdigit()
        ):
            raise ValueError(f"invalid RESP frame length: {header!r}")
        try:
            return int(encoded_length)
        except ValueError as error:
            raise ValueError(f"invalid RESP frame length: {header!r}") from error

    def _read_frame(self, budget, depth=0):
        budget.consume_item(depth)
        while not self._buffer:
            self._receive(budget)
        prefix = self._buffer[0]
        header = self._read_line(budget)

        if prefix in b"+-:,#_(":
            return header
        if prefix in b"$!=":
            length = self._frame_length(
                header,
                allow_null=self.protocol == 2 and prefix == ord("$"),
            )
            if length < 0:
                return header
            budget.require_bytes(length + 2)
            payload = self._read_exact(length + 2, budget)
            if not payload.endswith(b"\r\n"):
                raise ValueError("RESP bulk payload is missing its terminating CRLF")
            return header + payload
        if prefix in b"*~>":
            count = self._frame_length(
                header,
                allow_null=self.protocol == 2 and prefix == ord("*"),
            )
            if count < 0:
                return header
            budget.require_items(count)
            return header + b"".join(
                self._read_frame(budget, depth + 1) for _ in range(count)
            )
        if prefix in b"%|":
            count = self._frame_length(header)
            if count < 0:
                return header
            budget.require_items(count * 2)
            return header + b"".join(
                self._read_frame(budget, depth + 1) for _ in range(count * 2)
            )
        raise ValueError(f"unsupported RESP frame prefix: {bytes([prefix])!r}")
