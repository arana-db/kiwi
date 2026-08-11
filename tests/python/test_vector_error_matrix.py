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

"""Kiwi Vector command behavior coverage pending Redis 8.8.1 Oracle evidence.

These assertions describe current Kiwi behavior. Redis 8.8.1 error-precedence
priority remains subject to the separately gated differential Oracle suite.
"""

import struct

import pytest
import redis

import raw_resp_client
from raw_resp_client import RawRespConnection, encode_command


def _error(client, *args):
    with pytest.raises(redis.ResponseError) as caught:
        client.execute_command(*args)
    return str(caught.value)


def test_kiwi_current_vsim_missing_key_precedes_vector_and_options(redis_binary_client):
    client = redis_binary_client
    client.delete(b"test_vector_missing")

    assert client.execute_command(
        b"VSIM", b"test_vector_missing", b"FP32", b"bad"
    ) == []
    assert client.execute_command(
        b"VSIM",
        b"test_vector_missing",
        b"VALUES",
        b"1",
        b"1",
        b"COUNT",
        b"0",
    ) == []


def test_kiwi_current_vsim_wrongtype_and_missing_ele_precede_options(redis_binary_client):
    client = redis_binary_client
    key = b"test_vector_precedence"
    client.set(key, b"string")
    assert "WRONGTYPE" in _error(client, b"VSIM", key, b"FP32", b"bad")

    client.delete(key)
    client.execute_command(b"VADD", key, b"VALUES", b"2", b"1", b"0", b"present", b"NOQUANT")
    assert "element not found" in _error(
        client, b"VSIM", key, b"ELE", b"missing", b"COUNT", b"0"
    ).lower()


class ScriptedSocket:
    def __init__(self, exchanges):
        self.exchanges = list(exchanges)
        self.pending = []
        self.sent = []
        self.closed = False

    def sendall(self, payload):
        assert self.exchanges, f"unexpected command: {payload!r}"
        expected, response_chunks = self.exchanges.pop(0)
        assert payload == expected
        self.sent.append(payload)
        self.pending.extend(response_chunks)

    def recv(self, length):
        if not self.pending:
            return b""
        chunk = self.pending.pop(0)
        if len(chunk) > length:
            self.pending.insert(0, chunk[length:])
            return chunk[:length]
        return chunk

    def close(self):
        self.closed = True


def _byte_chunks(frame):
    return [bytes([byte]) for byte in frame]


@pytest.mark.raw_vector_protocol
def test_resp2_client_is_not_polluted_by_resp3_negotiation(monkeypatch):
    hello = encode_command(b"HELLO", b"3")
    ping = encode_command(b"PING")
    hello_map = b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n"
    resp2_socket = ScriptedSocket([(ping, _byte_chunks(b"+PONG\r\n"))])
    resp3_socket = ScriptedSocket(
        [
            (hello, _byte_chunks(hello_map)),
            (ping, _byte_chunks(b"+PONG\r\n")),
        ]
    )
    sockets = iter((resp2_socket, resp3_socket))
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: next(sockets),
    )

    resp2 = RawRespConnection.connect("resp2.invalid", 1, protocol=2)
    resp3 = RawRespConnection.connect("resp3.invalid", 2, protocol=3)
    try:
        assert resp2.execute_raw(b"PING") == b"+PONG\r\n"
        assert resp3.execute_raw(b"PING") == b"+PONG\r\n"
    finally:
        resp3.close()
        resp2.close()

    assert resp2_socket.sent == [ping]
    assert resp3_socket.sent == [hello, ping]
    assert resp2_socket.exchanges == []
    assert resp3_socket.exchanges == []
    assert resp2_socket.closed
    assert resp3_socket.closed


@pytest.mark.raw_vector_protocol
def test_raw_client_preserves_binary_bulk_lengths_and_nested_frames(monkeypatch):
    vector = struct.pack("<ff", 0.0, 1.0)
    element = b"x\x00y"
    command = encode_command(b"VADD", b"key", b"FP32", vector, element)
    response = b"%1\r\n+vectors\r\n*2\r\n$3\r\nx\x00y\r\n_\r\n"
    scripted_socket = ScriptedSocket([(command, _byte_chunks(response))])
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: scripted_socket,
    )

    client = RawRespConnection.connect("binary.invalid", 1, protocol=2)
    try:
        assert b"$8\r\n" + vector + b"\r\n" in command
        assert b"$3\r\n" + element + b"\r\n" in command
        assert (
            client.execute_raw(b"VADD", b"key", b"FP32", vector, element)
            == response
        )
    finally:
        client.close()
        client.close()

    assert scripted_socket.sent == [command]
    assert scripted_socket.exchanges == []
    assert scripted_socket.closed


@pytest.mark.raw_vector_protocol
def test_protocol_clients_are_function_scoped_and_explicit(
    request, raw_kiwi_resp2, raw_kiwi_resp3
):
    for fixture_name in ("raw_kiwi_resp2", "raw_kiwi_resp3"):
        fixture = request._fixturemanager.getfixturedefs(fixture_name, request.node)[-1]
        assert fixture.scope == "function"
    assert raw_kiwi_resp2.protocol == 2
    assert raw_kiwi_resp3.protocol == 3
    assert raw_kiwi_resp2.socket is not raw_kiwi_resp3.socket
    assert raw_kiwi_resp2.execute_raw(b"PING") == b"+PONG\r\n"
    assert raw_kiwi_resp3.execute_raw(b"PING") == b"+PONG\r\n"


def test_kiwi_current_vadd_complete_vector_without_element_is_wrong_arity(redis_binary_client):
    message = _error(
        redis_binary_client,
        b"VADD",
        b"test_vector_arity",
        b"VALUES",
        b"1",
        b"1",
    )
    assert "wrong number of arguments" in message.lower()


def test_kiwi_current_vadd_incomplete_vector_remains_typed_invalid_vector(redis_binary_client):
    message = _error(
        redis_binary_client,
        b"VADD",
        b"test_vector_arity",
        b"VALUES",
        b"2",
        b"1",
    )
    assert "invalid vector specification" in message.lower()
