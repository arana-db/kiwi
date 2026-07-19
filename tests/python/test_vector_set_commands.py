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

import os
import re
import struct
import time

import pytest
import redis


@pytest.fixture(params=[2, 3])
def vector_client(request):
    protocol = request.param
    prefix = f"test_vset:p{protocol}:".encode()
    keys = [
        prefix + b"values",
        prefix + b"binary:\x00key",
        prefix + b"search",
        prefix + b"missing",
        prefix + b"expired",
        prefix + b"recreated",
        prefix + b"errors",
        prefix + b"string",
        prefix + b"malformed",
    ]
    client = redis.Redis(
        host=os.getenv("KIWI_HOST", "127.0.0.1"),
        port=int(os.getenv("KIWI_PORT", "7379")),
        decode_responses=False,
        protocol=protocol,
    )
    client.ping()
    client.delete(*keys)
    yield client, protocol, prefix
    client.delete(*keys)
    client.close()


def vadd_values(client, key, values, element, quantization=b"NOQUANT"):
    return client.execute_command(
        b"VADD",
        key,
        b"VALUES",
        len(values),
        *values,
        element,
        quantization,
    )


def assert_response_error(client, expected, *command):
    with pytest.raises(redis.ResponseError, match=re.escape(expected)):
        client.execute_command(*command)


def assert_vector(values, expected):
    assert [float(value) for value in values] == pytest.approx(expected)


def test_values_create_update_and_point_commands(vector_client):
    client, _protocol, prefix = vector_client
    key = prefix + b"values"

    assert vadd_values(client, key, [1, 0], b"member") == 1
    assert vadd_values(client, key, [0.5, 0.5], b"member") == 0
    assert client.execute_command(b"VCARD", key) == 1
    assert client.execute_command(b"VDIM", key) == 2
    assert client.execute_command(b"VISMEMBER", key, b"member") == 1
    assert_vector(client.execute_command(b"VEMB", key, b"member"), [0.5, 0.5])
    assert client.type(key) == b"vectorset"

    assert_response_error(
        client,
        "vector dimension mismatch",
        b"VADD",
        key,
        b"VALUES",
        3,
        1,
        0,
        0,
        b"other",
        b"NOQUANT",
    )


def test_fp32_binary_members_and_last_member_removal(vector_client):
    client, _protocol, prefix = vector_client
    key = prefix + b"binary:\x00key"
    blob = struct.pack("<2f", 1.0, 0.0)

    assert client.execute_command(b"VADD", key, b"FP32", blob, b"", b"NOQUANT") == 1
    assert vadd_values(client, key, [0, 1], b"\x00member") == 1
    assert client.execute_command(b"VCARD", key) == 2
    assert_vector(client.execute_command(b"VEMB", key, b""), [1.0, 0.0])
    assert client.execute_command(b"VISMEMBER", key, b"\x00member") == 1

    assert client.execute_command(b"VREM", key, b"") == 1
    assert client.execute_command(b"VREM", key, b"\x00member") == 1
    assert client.execute_command(b"VREM", key, b"\x00member") == 0
    assert client.type(key) == b"none"


def test_vsim_queries_scores_truth_and_stable_ties(vector_client):
    client, protocol, prefix = vector_client
    key = prefix + b"search"
    vadd_values(client, key, [1, 0], b"a")
    vadd_values(client, key, [1, 0], b"b")
    vadd_values(client, key, [0, 1], b"c")

    direct = client.execute_command(
        b"VSIM", key, b"VALUES", 2, 1, 0, b"COUNT", 2, b"TRUTH"
    )
    assert direct == [b"a", b"b"]

    by_element = client.execute_command(
        b"VSIM", key, b"ELE", b"a", b"COUNT", 3, b"TRUTH"
    )
    assert by_element == [b"a", b"b", b"c"]

    scores = client.execute_command(
        b"VSIM",
        key,
        b"FP32",
        struct.pack("<2f", 1.0, 0.0),
        b"WITHSCORES",
        b"COUNT",
        2,
        b"TRUTH",
    )
    if protocol == 2:
        assert scores[::2] == [b"a", b"b"]
        assert [float(value) for value in scores[1::2]] == pytest.approx([1.0, 1.0])
    else:
        assert list(scores) == [b"a", b"b"]
        assert list(scores.values()) == pytest.approx([1.0, 1.0])


def test_missing_key_and_missing_element_semantics(vector_client):
    client, _protocol, prefix = vector_client
    key = prefix + b"missing"

    assert client.execute_command(b"VCARD", key) == 0
    assert client.execute_command(b"VEMB", key, b"member") is None
    assert client.execute_command(b"VISMEMBER", key, b"member") == 0
    assert client.execute_command(b"VSIM", key, b"VALUES", 2, 1, 0) == []
    assert_response_error(client, "key does not exist", b"VDIM", key)

    vadd_values(client, key, [1, 0], b"present")
    assert_response_error(
        client, "element not found in set", b"VSIM", key, b"ELE", b"absent"
    )


def test_expire_del_and_recreate_with_new_dimension(vector_client):
    client, _protocol, prefix = vector_client
    expired = prefix + b"expired"
    recreated = prefix + b"recreated"

    vadd_values(client, expired, [1, 0], b"member")
    assert client.expire(expired, 1)
    time.sleep(1.1)
    assert client.type(expired) == b"none"
    assert client.execute_command(b"VCARD", expired) == 0
    assert client.execute_command(b"VEMB", expired, b"member") is None

    vadd_values(client, recreated, [1, 0], b"member")
    assert client.delete(recreated) == 1
    assert client.execute_command(b"VISMEMBER", recreated, b"member") == 0
    assert vadd_values(client, recreated, [1, 0, 0], b"new-member") == 1
    assert client.execute_command(b"VDIM", recreated) == 3


def test_wrongtype_and_unsupported_options(vector_client):
    client, _protocol, prefix = vector_client
    key = prefix + b"errors"
    string_key = prefix + b"string"
    client.set(string_key, b"value")

    assert_response_error(
        client,
        "WRONGTYPE Operation against a key holding the wrong kind of value",
        b"VADD",
        string_key,
        b"VALUES",
        2,
        1,
        0,
        b"member",
        b"NOQUANT",
    )

    base = (b"VADD", key, b"VALUES", 2, 1, 0, b"member")
    assert_response_error(client, "default Q8 quantization is not supported", *base)
    assert_response_error(client, "VADD option Q8 is not supported yet", *base, b"Q8")
    assert_response_error(client, "VADD option BIN is not supported yet", *base, b"BIN")

    vadd_values(client, key, [1, 0], b"member")
    assert_response_error(
        client, "VEMB option RAW is not supported yet", b"VEMB", key, b"member", b"RAW"
    )


def test_malformed_vectors_and_options(vector_client):
    client, _protocol, prefix = vector_client
    key = prefix + b"malformed"

    assert_response_error(
        client,
        "invalid vector specification",
        b"VADD",
        key,
        b"FP32",
        b"abc",
        b"member",
        b"NOQUANT",
    )
    assert_response_error(
        client,
        "invalid vector specification",
        b"VADD",
        key,
        b"VALUES",
        2,
        1,
        b"not-a-float",
        b"member",
        b"NOQUANT",
    )
    assert_response_error(
        client,
        "invalid vector specification",
        b"VSIM",
        key,
        b"VALUES",
        2,
        1,
        0,
        b"COUNT",
        0,
    )
