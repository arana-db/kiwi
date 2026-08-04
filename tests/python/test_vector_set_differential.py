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

"""Differential tests comparing Kiwi vector sets against Redis 8.

Every test issues the same commands to a Kiwi server (KIWI_HOST/KIWI_PORT,
default 127.0.0.1:7379) and to a Redis 8 reference server
(VECTOR_REDIS_HOST/VECTOR_REDIS_PORT, default 127.0.0.1:6380) and compares
the replies. Both sides are populated with an explicit NOQUANT option.

Out of scope on purpose:
- Redis' default Q8 quantization path. Phase 1 Kiwi only supports NOQUANT;
  the error contract for omitted/unsupported quantization options is covered
  by test_vector_set_commands.py and not repeated here.
- VINFO field *values* such as hnsw-m, max-level and vset-uid. Redis 8
  reports real HNSW internals while Kiwi Phase 1 reports FLAT sentinels with
  a different meaning; only the field-name set and value types are compared.

All datasets use a fixed seed so runs are reproducible.
"""

import os
import random
import struct

import pytest
import redis

KIWI_HOST = os.getenv("KIWI_HOST", "127.0.0.1")
KIWI_PORT = int(os.getenv("KIWI_PORT", "7379"))
REDIS8_HOST = os.getenv("VECTOR_REDIS_HOST", "127.0.0.1")
REDIS8_PORT = int(os.getenv("VECTOR_REDIS_PORT", "6380"))
KIWI_COMPAT_REQUIRE_ORACLE = os.getenv("KIWI_COMPAT_REQUIRE_ORACLE") == "1"

SCORE_TOLERANCE = 1e-6


def _server_reachable(host, port):
    client = redis.Redis(
        host=host, port=port, socket_connect_timeout=1, socket_timeout=1
    )
    try:
        client.ping()
        return True
    except (redis.RedisError, OSError):
        return False
    finally:
        client.close()


if not _server_reachable(KIWI_HOST, KIWI_PORT):
    if KIWI_COMPAT_REQUIRE_ORACLE:
        raise RuntimeError(
            "KIWI_COMPAT_REQUIRE_ORACLE=1 but Kiwi reference server is not "
            f"reachable at {KIWI_HOST}:{KIWI_PORT}; vector set differential "
            "cannot run and must not be silently skipped"
        )
    pytest.skip(
        f"Kiwi server not reachable at {KIWI_HOST}:{KIWI_PORT}; "
        "skipping vector set differential tests",
        allow_module_level=True,
    )
if not _server_reachable(REDIS8_HOST, REDIS8_PORT):
    if KIWI_COMPAT_REQUIRE_ORACLE:
        raise RuntimeError(
            "KIWI_COMPAT_REQUIRE_ORACLE=1 but Redis 8.8.1 reference server is "
            f"not reachable at {REDIS8_HOST}:{REDIS8_PORT}; vector set "
            "differential cannot run and must not be silently skipped"
        )
    pytest.skip(
        f"Redis 8 reference server not reachable at {REDIS8_HOST}:{REDIS8_PORT}; "
        "skipping vector set differential tests",
        allow_module_level=True,
    )


TEST_KEY_NAMES = (b"main", b"dense3", b"string", b"missing")


def _build_main_members():
    """Fixed-seed dim-4 dataset: regular, empty and binary elements plus a
    tie pair sharing one identical vector (tie order must be element bytes
    ascending on both servers)."""
    rng = random.Random(20240701)
    members = []
    for element in (b"alpha", b"beta", b"gamma", b"delta", b"", b"\x00bin\x00"):
        members.append((element, [rng.uniform(-2.0, 2.0) for _ in range(4)]))
    tie_vector = [0.25, -0.5, 1.0, 0.75]
    members.append((b"tie-a", list(tie_vector)))
    members.append((b"tie-b", list(tie_vector)))
    return members


def _build_dense3_members():
    rng = random.Random(7)
    return [
        (element, [rng.uniform(-1.0, 1.0) for _ in range(3)])
        for element in (b"x", b"y", b"z")
    ]


MAIN_MEMBERS = _build_main_members()
DENSE3_MEMBERS = _build_dense3_members()
MAIN_QUERY = [0.5, -0.25, 1.5, 0.125]


@pytest.fixture(params=[2, 3], ids=["resp2", "resp3"])
def backends(request):
    protocol = request.param
    prefix = f"test_vdiff:p{protocol}:".encode()
    keys = [prefix + name for name in TEST_KEY_NAMES]
    kiwi = redis.Redis(
        host=KIWI_HOST, port=KIWI_PORT, decode_responses=False, protocol=protocol
    )
    reference = redis.Redis(
        host=REDIS8_HOST, port=REDIS8_PORT, decode_responses=False, protocol=protocol
    )
    for client in (kiwi, reference):
        client.delete(*keys)
    yield kiwi, reference, protocol, prefix
    for client in (kiwi, reference):
        client.delete(*keys)
        client.close()


def vadd_noquant(client, key, values, element):
    return client.execute_command(
        b"VADD", key, b"VALUES", len(values), *values, element, b"NOQUANT"
    )


def populate(client, key, members):
    for element, vector in members:
        vadd_noquant(client, key, vector, element)


def assert_same_reply(kiwi, reference, *command):
    kiwi_reply = kiwi.execute_command(*command)
    redis_reply = reference.execute_command(*command)
    assert kiwi_reply == redis_reply, (
        f"{command!r}: kiwi={kiwi_reply!r} != redis={redis_reply!r}"
    )
    return kiwi_reply


def normalized_vemb(reply):
    if reply is None:
        return None
    return [float(component) for component in reply]


def assert_same_vemb(kiwi, reference, key, element):
    kiwi_vemb = normalized_vemb(kiwi.execute_command(b"VEMB", key, element))
    redis_vemb = normalized_vemb(reference.execute_command(b"VEMB", key, element))
    if redis_vemb is None:
        assert kiwi_vemb is None
        return
    assert kiwi_vemb is not None
    assert len(kiwi_vemb) == len(redis_vemb)
    assert kiwi_vemb == pytest.approx(redis_vemb, abs=SCORE_TOLERANCE)


def parse_vsim(reply, protocol, withscores):
    """Normalize a VSIM reply to an ordered list of (element, score) pairs."""
    if not withscores:
        return [(element, None) for element in reply]
    if protocol == 3:
        return [(element, float(score)) for element, score in reply.items()]
    return [
        (reply[index], float(reply[index + 1]))
        for index in range(0, len(reply), 2)
    ]


def assert_same_vsim(kiwi, reference, protocol, query_args, *options):
    withscores = b"WITHSCORES" in options
    kiwi_hits = parse_vsim(
        kiwi.execute_command(b"VSIM", *query_args, *options), protocol, withscores
    )
    redis_hits = parse_vsim(
        reference.execute_command(b"VSIM", *query_args, *options),
        protocol,
        withscores,
    )
    assert [element for element, _ in kiwi_hits] == [
        element for element, _ in redis_hits
    ]
    if withscores:
        assert len(kiwi_hits) == len(redis_hits)
        for (_, kiwi_score), (_, redis_score) in zip(kiwi_hits, redis_hits):
            assert kiwi_score == pytest.approx(redis_score, abs=SCORE_TOLERANCE)


def vinfo_as_dict(reply, protocol):
    if protocol == 3:
        assert type(reply) is dict
        return reply
    assert type(reply) is list
    return dict(zip(reply[::2], reply[1::2]))


def test_vadd_vcard_vdim_vismember_match(backends):
    kiwi, reference, _protocol, prefix = backends
    main_key = prefix + b"main"
    dense3_key = prefix + b"dense3"

    for key, members in ((main_key, MAIN_MEMBERS), (dense3_key, DENSE3_MEMBERS)):
        for element, vector in members:
            assert_same_reply(kiwi, reference, b"VADD", key, b"VALUES",
                              len(vector), *vector, element, b"NOQUANT")
        assert_same_reply(kiwi, reference, b"VCARD", key)
        assert_same_reply(kiwi, reference, b"VDIM", key)
        for element, _ in members:
            assert_same_reply(kiwi, reference, b"VISMEMBER", key, element)
        assert_same_reply(kiwi, reference, b"VISMEMBER", key, b"ghost")

    # Re-adding an existing element with a new vector is an update (0/False).
    updated = [0.1, 0.2, 0.3, 0.4]
    assert_same_reply(kiwi, reference, b"VADD", main_key, b"VALUES",
                      4, *updated, b"alpha", b"NOQUANT")
    assert_same_reply(kiwi, reference, b"VCARD", main_key)
    assert_same_vemb(kiwi, reference, main_key, b"alpha")


def test_vemb_matches_for_every_member(backends):
    kiwi, reference, _protocol, prefix = backends
    main_key = prefix + b"main"
    populate(kiwi, main_key, MAIN_MEMBERS)
    populate(reference, main_key, MAIN_MEMBERS)

    for element, _ in MAIN_MEMBERS:
        assert_same_vemb(kiwi, reference, main_key, element)
    assert_same_vemb(kiwi, reference, main_key, b"ghost")


def test_vsim_variants_match(backends):
    kiwi, reference, protocol, prefix = backends
    main_key = prefix + b"main"
    populate(kiwi, main_key, MAIN_MEMBERS)
    populate(reference, main_key, MAIN_MEMBERS)

    fp32_blob = struct.pack(f"<{len(MAIN_QUERY)}f", *MAIN_QUERY)
    query_variants = [
        (main_key, b"ELE", b"alpha"),
        (main_key, b"ELE", b""),
        (main_key, b"VALUES", len(MAIN_QUERY), *MAIN_QUERY),
        (main_key, b"FP32", fp32_blob),
    ]
    option_variants = [
        (b"COUNT", 16, b"WITHSCORES", b"TRUTH"),
        (b"COUNT", 3, b"WITHSCORES", b"TRUTH"),
        (b"COUNT", 16, b"TRUTH"),
        (b"COUNT", 3, b"TRUTH"),
    ]
    for query_args in query_variants:
        for options in option_variants:
            assert_same_vsim(kiwi, reference, protocol, query_args, *options)


def test_vrem_replies_and_set_disappears_when_emptied(backends):
    kiwi, reference, _protocol, prefix = backends
    main_key = prefix + b"main"
    populate(kiwi, main_key, MAIN_MEMBERS)
    populate(reference, main_key, MAIN_MEMBERS)

    # Removing an absent element first: 0/False on both.
    assert_same_reply(kiwi, reference, b"VREM", main_key, b"ghost")

    for element, _ in MAIN_MEMBERS:
        assert_same_reply(kiwi, reference, b"VREM", main_key, element)
        assert_same_reply(kiwi, reference, b"VISMEMBER", main_key, element)

    # The last removal deletes the key on both servers.
    assert_same_reply(kiwi, reference, b"VCARD", main_key)
    assert_same_reply(kiwi, reference, b"TYPE", main_key)


def test_wrongtype_errors_match(backends):
    kiwi, reference, _protocol, prefix = backends
    string_key = prefix + b"string"
    for client in (kiwi, reference):
        client.set(string_key, b"plain-string")

    commands = [
        (b"VADD", string_key, b"VALUES", 2, 1, 0, b"member", b"NOQUANT"),
        (b"VCARD", string_key),
        (b"VDIM", string_key),
        (b"VEMB", string_key, b"member"),
        (b"VISMEMBER", string_key, b"member"),
        (b"VREM", string_key, b"member"),
        (b"VSIM", string_key, b"VALUES", 2, 1, 0, b"TRUTH"),
        (b"VINFO", string_key),
    ]
    for command in commands:
        for name, client in (("kiwi", kiwi), ("redis", reference)):
            with pytest.raises(redis.ResponseError) as excinfo:
                client.execute_command(*command)
            assert str(excinfo.value).startswith("WRONGTYPE"), (
                f"{name} {command!r}: {excinfo.value}"
            )


def test_missing_key_semantics_match(backends):
    kiwi, reference, _protocol, prefix = backends
    missing_key = prefix + b"missing"

    assert_same_reply(kiwi, reference, b"VCARD", missing_key)
    assert_same_reply(kiwi, reference, b"VISMEMBER", missing_key, b"member")
    assert_same_reply(
        kiwi, reference, b"VSIM", missing_key, b"VALUES", 4, *MAIN_QUERY, b"TRUTH"
    )
    assert_same_vemb(kiwi, reference, missing_key, b"member")
    # VDIM on a missing key is deliberately not compared: Kiwi Phase 1 returns
    # an error while Redis 8 returns 0; this known divergence is covered by
    # the Kiwi-side contract tests.


def test_vinfo_field_names_and_types_match(backends):
    kiwi, reference, protocol, prefix = backends
    main_key = prefix + b"main"

    # Missing key: null reply on both servers.
    assert_same_reply(kiwi, reference, b"VINFO", main_key)

    populate(kiwi, main_key, MAIN_MEMBERS)
    populate(reference, main_key, MAIN_MEMBERS)
    kiwi_info = vinfo_as_dict(kiwi.execute_command(b"VINFO", main_key), protocol)
    redis_info = vinfo_as_dict(reference.execute_command(b"VINFO", main_key), protocol)

    # Only the field-name set and value types are compared. Values such as
    # hnsw-m, max-level and vset-uid describe Redis' real HNSW index, while
    # Kiwi Phase 1 reports FLAT sentinels with different semantics.
    assert set(kiwi_info) == set(redis_info), (
        f"VINFO fields differ: kiwi={sorted(kiwi_info)} redis={sorted(redis_info)}"
    )
    for field in kiwi_info:
        assert type(kiwi_info[field]) is type(redis_info[field]), (
            f"VINFO field {field!r} type: kiwi={type(kiwi_info[field])} "
            f"redis={type(redis_info[field])}"
        )
