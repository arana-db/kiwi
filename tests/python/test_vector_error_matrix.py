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
        self.recv_calls = 0
        self.timeouts = []
        self.closed = False

    def sendall(self, payload):
        assert self.exchanges, f"unexpected command: {payload!r}"
        expected, response_chunks = self.exchanges.pop(0)
        assert payload == expected
        self.sent.append(payload)
        self.pending.extend(response_chunks)

    def recv(self, length):
        self.recv_calls += 1
        if not self.pending:
            return b""
        chunk = self.pending.pop(0)
        if len(chunk) > length:
            self.pending.insert(0, chunk[length:])
            return chunk[:length]
        return chunk

    def settimeout(self, timeout):
        self.timeouts.append(timeout)

    def close(self):
        self.closed = True


def _byte_chunks(frame):
    return [bytes([byte]) for byte in frame]


def _raw_limits(**overrides):
    values = {
        "max_frame_bytes": 1024,
        "max_header_bytes": 64,
        "max_items": 32,
        "max_depth": 8,
    }
    values.update(overrides)
    return raw_resp_client.RespFrameLimits(**values)


def test_raw_client_rejects_unterminated_header_at_header_budget(monkeypatch):
    command = encode_command(b"PING")
    scripted_socket = ScriptedSocket([(command, [b"+abc"])])
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: scripted_socket,
    )

    client = RawRespConnection.connect(
        "header.invalid",
        1,
        protocol=2,
        limits=_raw_limits(max_header_bytes=4),
    )
    with pytest.raises(ValueError, match="header"):
        client.execute_raw(b"PING")

    assert scripted_socket.recv_calls == 1, "reader must reject without another recv"
    assert scripted_socket.closed


def test_raw_client_rejects_declared_bulk_above_frame_budget_before_payload(
    monkeypatch,
):
    command = encode_command(b"GET", b"large")
    scripted_socket = ScriptedSocket([(command, [b"$100\r\n"])])
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: scripted_socket,
    )

    client = RawRespConnection.connect(
        "bulk.invalid",
        1,
        protocol=2,
        limits=_raw_limits(max_frame_bytes=16),
    )
    with pytest.raises(ValueError, match="frame byte"):
        client.execute_raw(b"GET", b"large")

    assert scripted_socket.closed


@pytest.mark.parametrize("header", [b"*4\r\n", b"%2\r\n"])
def test_raw_client_rejects_aggregate_item_budget(monkeypatch, header):
    command = encode_command(b"PING")
    scripted_socket = ScriptedSocket([(command, [header])])
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: scripted_socket,
    )

    client = RawRespConnection.connect(
        "items.invalid",
        1,
        protocol=2,
        limits=_raw_limits(max_items=4),
    )
    with pytest.raises(ValueError, match="item"):
        client.execute_raw(b"PING")

    assert scripted_socket.closed


def test_raw_client_rejects_nesting_depth_budget(monkeypatch):
    command = encode_command(b"PING")
    response = b"*1\r\n*1\r\n*1\r\n+ok\r\n"
    scripted_socket = ScriptedSocket([(command, [response])])
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: scripted_socket,
    )

    client = RawRespConnection.connect(
        "depth.invalid",
        1,
        protocol=2,
        limits=_raw_limits(max_depth=2),
    )
    with pytest.raises(ValueError, match="depth"):
        client.execute_raw(b"PING")

    assert scripted_socket.closed


def test_raw_client_uses_one_absolute_deadline_during_slow_progress(monkeypatch):
    class FakeClock:
        def __init__(self):
            self.value = 10.0

        def monotonic(self):
            return self.value

        def advance(self, seconds):
            self.value += seconds

    class SlowProgressSocket(ScriptedSocket):
        def __init__(self, exchanges, clock):
            super().__init__(exchanges)
            self.clock = clock

        def recv(self, length):
            self.clock.advance(0.25)
            return super().recv(length)

    clock = FakeClock()
    command = encode_command(b"PING")
    scripted_socket = SlowProgressSocket(
        [(command, _byte_chunks(b"+PONG\r\n"))],
        clock,
    )
    monkeypatch.setattr(raw_resp_client, "_monotonic", clock.monotonic)
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: scripted_socket,
    )

    client = RawRespConnection.connect(
        "deadline.invalid",
        1,
        protocol=2,
        timeout=1.0,
        limits=_raw_limits(),
    )
    with pytest.raises(TimeoutError, match="deadline"):
        client.execute_raw(b"PING")

    assert scripted_socket.closed
    assert scripted_socket.timeouts
    assert all(timeout > 0 for timeout in scripted_socket.timeouts)
    assert scripted_socket.timeouts == sorted(scripted_socket.timeouts, reverse=True)


def test_raw_client_accepts_nested_binary_at_exact_limits(monkeypatch):
    command = encode_command(b"VINFO", b"key")
    response = b"%1\r\n+vectors\r\n*2\r\n$3\r\nx\x00y\r\n_\r\n"
    scripted_socket = ScriptedSocket([(command, [response])])
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: scripted_socket,
    )

    client = RawRespConnection.connect(
        "exact.invalid",
        1,
        protocol=2,
        limits=_raw_limits(
            max_frame_bytes=len(response),
            max_header_bytes=len(b"+vectors\r\n"),
            max_items=5,
            max_depth=2,
        ),
    )
    try:
        assert client.execute_raw(b"VINFO", b"key") == response
    finally:
        client.close()


def test_raw_client_charges_only_current_frame_and_preserves_pipeline_tail(
    monkeypatch,
):
    first_command = encode_command(b"GET", b"one")
    second_command = encode_command(b"GET", b"two")
    first_frame = b"$3\r\none\r\n"
    second_frame = b"$3\r\ntwo\r\n"
    scripted_socket = ScriptedSocket(
        [
            (first_command, [first_frame + second_frame]),
            (second_command, []),
        ]
    )
    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        lambda _endpoint, timeout: scripted_socket,
    )

    client = RawRespConnection.connect(
        "pipeline.invalid",
        1,
        protocol=2,
        limits=_raw_limits(max_frame_bytes=len(first_frame)),
    )
    try:
        assert client.execute_raw(b"GET", b"one") == first_frame
        assert client.execute_raw(b"GET", b"two") == second_frame
    finally:
        client.close()

    assert scripted_socket.sent == [first_command, second_command]
    assert scripted_socket.exchanges == []
    assert scripted_socket.pending == []


def test_raw_client_local_encoding_error_preserves_unsent_connection():
    scripted_socket = ScriptedSocket([])
    client = RawRespConnection(scripted_socket, protocol=2)

    with pytest.raises(TypeError, match="command parts"):
        client.execute_raw("PING")

    assert client.socket is scripted_socket
    assert not scripted_socket.closed
    assert scripted_socket.sent == []


@pytest.mark.parametrize(
    ("protocol", "response"),
    [
        pytest.param(2, b"$-1\r\n", id="resp2-null-bulk"),
        pytest.param(2, b"*-1\r\n", id="resp2-null-array"),
        pytest.param(3, b"_\r\n", id="resp3-null"),
    ],
)
def test_raw_client_accepts_protocol_null_sentinel(protocol, response):
    command = encode_command(b"GET", b"missing")
    scripted_socket = ScriptedSocket([(command, [response])])
    client = RawRespConnection(scripted_socket, protocol=protocol)

    try:
        assert client.execute_raw(b"GET", b"missing") == response
    finally:
        client.close()


@pytest.mark.parametrize(
    ("protocol", "malformed_header"),
    [
        pytest.param(2, b"$-2\r\n", id="resp2-bulk-below-null"),
        pytest.param(2, b"*-2\r\n", id="resp2-array-below-null"),
        pytest.param(3, b"$-1\r\n", id="resp3-bulk-null-sentinel"),
        pytest.param(3, b"*-1\r\n", id="resp3-array-null-sentinel"),
        pytest.param(3, b"!-1\r\n", id="blob-error-null-sentinel"),
        pytest.param(3, b"=-1\r\n", id="verbatim-null-sentinel"),
        pytest.param(3, b"~-1\r\n", id="set-null-sentinel"),
        pytest.param(3, b"%-1\r\n", id="map-null-sentinel"),
        pytest.param(3, b">-1\r\n", id="push-null-sentinel"),
        pytest.param(3, b"|-1\r\n", id="attribute-null-sentinel"),
        pytest.param(2, b"$01\r\n", id="leading-zero-bulk"),
        pytest.param(2, b"*+1\r\n", id="explicit-plus-array"),
        pytest.param(3, b"!-0\r\n", id="negative-zero-blob-error"),
        pytest.param(3, b"~-01\r\n", id="signed-leading-zero-set"),
        pytest.param(3, b"% 1\r\n", id="space-prefixed-map"),
        pytest.param(3, b"|1 \r\n", id="space-suffixed-attribute"),
    ],
)
def test_malformed_resp_length_invalidates_connection_and_discards_trailing_frame(
    protocol,
    malformed_header,
):
    command = encode_command(b"PING")
    trailing_frame = b"+TRAILING\r\n"
    scripted_socket = ScriptedSocket(
        [(command, [malformed_header + trailing_frame])]
    )
    client = RawRespConnection(scripted_socket, protocol=protocol)

    with pytest.raises(ValueError, match="invalid RESP frame length"):
        client.execute_raw(b"PING")

    assert scripted_socket.closed
    assert scripted_socket.pending == []
    assert client.socket is None
    assert client._buffer == bytearray()
    with pytest.raises(RuntimeError, match="closed"):
        client.execute_raw(b"PING")
    assert scripted_socket.sent == [command]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_frame_bytes", 0),
        ("max_header_bytes", 0),
        ("max_items", 0),
        ("max_depth", -1),
    ],
)
def test_raw_client_rejects_invalid_frame_limits(field, value):
    with pytest.raises(ValueError, match=field):
        _raw_limits(**{field: value})


@pytest.mark.parametrize("timeout", [0.0, -1.0, float("inf"), float("nan")])
def test_raw_client_rejects_invalid_absolute_timeout_before_connect(
    monkeypatch,
    timeout,
):
    def unexpected_connect(_endpoint, timeout):
        pytest.fail(f"invalid timeout reached socket.create_connection: {timeout!r}")

    monkeypatch.setattr(
        raw_resp_client.socket,
        "create_connection",
        unexpected_connect,
    )
    with pytest.raises(ValueError, match="timeout"):
        RawRespConnection.connect("timeout.invalid", 1, protocol=2, timeout=timeout)


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
