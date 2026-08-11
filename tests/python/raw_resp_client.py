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

import socket


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

    def __init__(self, connection, protocol):
        self.socket = connection
        self.protocol = protocol
        self._buffer = bytearray()

    @classmethod
    def connect(cls, host, port, protocol, timeout=5.0):
        if protocol not in (2, 3):
            raise ValueError(f"RESP protocol must be 2 or 3, got {protocol!r}")
        connection = socket.create_connection((host, port), timeout=timeout)
        client = cls(connection, protocol)
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
        self.socket.sendall(encode_command(*parts))
        return self._read_frame()

    def close(self):
        connection = self.socket
        self.socket = None
        self._buffer.clear()
        if connection is not None:
            connection.close()

    def _receive(self):
        if self.socket is None:
            raise RuntimeError("RESP connection is closed")
        chunk = self.socket.recv(4096)
        if not chunk:
            raise EOFError("RESP connection closed before a complete frame was received")
        self._buffer.extend(chunk)

    def _read_line(self):
        while True:
            end = self._buffer.find(b"\r\n")
            if end >= 0:
                end += 2
                line = bytes(self._buffer[:end])
                del self._buffer[:end]
                return line
            self._receive()

    def _read_exact(self, length):
        while len(self._buffer) < length:
            self._receive()
        payload = bytes(self._buffer[:length])
        del self._buffer[:length]
        return payload

    @staticmethod
    def _frame_length(header):
        try:
            return int(header[1:-2])
        except ValueError as error:
            raise ValueError(f"invalid RESP frame length: {header!r}") from error

    def _read_frame(self):
        while not self._buffer:
            self._receive()
        prefix = self._buffer[0]
        header = self._read_line()

        if prefix in b"+-:,#_(":
            return header
        if prefix in b"$!=":
            length = self._frame_length(header)
            if length < 0:
                return header
            payload = self._read_exact(length + 2)
            if not payload.endswith(b"\r\n"):
                raise ValueError("RESP bulk payload is missing its terminating CRLF")
            return header + payload
        if prefix in b"*~>":
            count = self._frame_length(header)
            if count < 0:
                return header
            return header + b"".join(self._read_frame() for _ in range(count))
        if prefix in b"%|":
            count = self._frame_length(header)
            if count < 0:
                return header
            return header + b"".join(self._read_frame() for _ in range(count * 2))
        raise ValueError(f"unsupported RESP frame prefix: {bytes([prefix])!r}")
