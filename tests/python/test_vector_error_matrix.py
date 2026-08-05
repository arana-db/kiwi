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


def test_kiwi_current_resp3_missing_key_withscores_is_array(redis_binary_client):
    client = redis_binary_client
    client.execute_command(b"HELLO", b"3")
    client.delete(b"test_vector_missing_scores")

    result = client.execute_command(
        b"VSIM",
        b"test_vector_missing_scores",
        b"VALUES",
        b"1",
        b"1",
        b"WITHSCORES",
    )
    assert isinstance(result, list)
    assert result == []


@pytest.mark.parametrize(
    "kind,payload",
    [
        (b"VALUES", (b"2", b"0", b"0")),
        (b"FP32", (struct.pack("<ff", 0.0, 0.0),)),
    ],
)
def test_kiwi_current_zero_vectors_round_trip_and_score_neutrally(redis_binary_client, kind, payload):
    client = redis_binary_client
    key = b"test_vector_zero_" + kind.lower()
    client.delete(key)

    client.execute_command(b"VADD", key, kind, *payload, b"zero", b"NOQUANT")
    client.execute_command(b"VADD", key, b"VALUES", b"2", b"1", b"0", b"x", b"NOQUANT")
    assert client.execute_command(b"VEMB", key, b"zero") == [0.0, 0.0]

    hits = client.execute_command(
        b"VSIM", key, b"ELE", b"zero", b"WITHSCORES", b"COUNT", b"2"
    )
    assert hits == {b"x": 0.5, b"zero": 0.5}


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
