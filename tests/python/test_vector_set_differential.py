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

"""WP1 trusted-Oracle opt-in differential tests for Kiwi Vector Sets.

Every test issues the same commands to a Kiwi server (KIWI_HOST/KIWI_PORT,
default 127.0.0.1:7379) and to a Redis 8 reference server
(VECTOR_REDIS_HOST/VECTOR_REDIS_PORT, default 127.0.0.1:6380) and compares
the replies. Both sides are populated with an explicit NOQUANT option.

Out of scope on purpose:
- Redis' default Q8 quantization path. Phase 1 Kiwi only supports NOQUANT;
  the error contract for omitted/unsupported quantization options is covered
  by test_vector_set_commands.py and not repeated here.
- VINFO HNSW/FLAT payload values for hnsw-m, max-level, vset-uid and
  hnsw-max-node-uid. Redis 8 reports real HNSW internals while Kiwi Phase 1
  reports FLAT sentinels with a different meaning. Their raw field encoding,
  order, container, pair count and value frame types are still compared.

Both configured endpoints are mandatory: unavailable endpoints fail closed rather
than producing a skipped test result. All datasets use a fixed seed.
"""

import base64
import hashlib
import json
import math
import os
import random
import socket
import struct
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
import redis

from raw_resp_client import RawRespConnection, encode_command

pytestmark = pytest.mark.raw_vector_protocol

KIWI_HOST = os.getenv("KIWI_HOST", "127.0.0.1")
KIWI_PORT = int(os.getenv("KIWI_PORT", "7379"))
REDIS8_HOST = os.getenv("VECTOR_REDIS_HOST", "127.0.0.1")
REDIS8_PORT = int(os.getenv("VECTOR_REDIS_PORT", "6380"))
SCORE_TOLERANCE = 1e-6
RAW_TRANSCRIPT_SCHEMA = "kiwi-vector-wire-transcript/v1"
FINAL_STATE_SCHEMA = "kiwi-vector-final-state/v1"
VINFO_DIFFERENCE_IDS = {
    b"hnsw-m": "vinfo-hnsw-m",
    b"max-level": "vinfo-max-level",
    b"vset-uid": "vinfo-vset-uid",
    b"hnsw-max-node-uid": "vinfo-hnsw-max-node-uid",
}
TYPED_FINAL_KEY_ROLES = (b"main", b"dense3", b"string", b"missing")
RAW_FINAL_KEY_ROLES = (
    b"values",
    b"fp32",
    b"missing-scores",
    b"missing-values",
    b"missing-fp32",
    b"invalid-values",
    b"invalid-fp32",
    b"repeated",
    b"option",
)
FINAL_STATE_PROFILE_TYPES = {
    "raw-all-missing": {},
    "raw-repeated-vector": {b"repeated": b"vectorset"},
    "typed-all-missing": {},
    "typed-main-vector": {b"main": b"vectorset"},
    "typed-main-two-member-vector": {b"main": b"vectorset"},
    "typed-main-dense3-vector": {
        b"main": b"vectorset",
        b"dense3": b"vectorset",
    },
    "typed-string": {b"string": b"string"},
}
FINAL_STATE_VECTOR_MEMBERS = {
    b"main": (b"alpha", b"beta", b"gamma", b"delta", b"", b"\x00bin\x00", b"tie-a", b"tie-b", b"ghost"),
    b"dense3": (b"x", b"y", b"z", b"ghost"),
    b"repeated": (b"element", b"ghost"),
}


def _append_jsonl(path, entry):
    with open(path, "a", encoding="utf-8") as output:
        output.write(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n")


def _reject_duplicate_object_pairs(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate JSON object key {key!r}")
        document[key] = value
    return document


def _encoded_bytes(payload):
    return {
        "base64": base64.b64encode(payload).decode("ascii"),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _wire_exchange(command, kiwi_frame, redis_frame):
    request = encode_command(*command)
    request_evidence = _encoded_bytes(request)
    kiwi_evidence = _encoded_bytes(kiwi_frame)
    redis_evidence = _encoded_bytes(redis_frame)
    return {
        "command": command[0].decode("ascii"),
        "request_base64": request_evidence["base64"],
        "request_sha256": request_evidence["sha256"],
        "kiwi_response_base64": kiwi_evidence["base64"],
        "kiwi_response_sha256": kiwi_evidence["sha256"],
        "redis_response_base64": redis_evidence["base64"],
        "redis_response_sha256": redis_evidence["sha256"],
    }


def _execute_same_raw(kiwi, reference, *command):
    kiwi_frame = kiwi.execute_raw(*command)
    redis_frame = reference.execute_raw(*command)
    assert kiwi_frame == redis_frame, (
        f"final-state {command[0].decode('ascii')} differs: "
        f"kiwi={kiwi_frame!r} redis={redis_frame!r}"
    )
    return _wire_exchange(command, kiwi_frame, redis_frame)


def _normalize_raw_vemb(frame, protocol):
    parsed, consumed = _read_resp_frame(frame)
    assert consumed == len(frame), "VEMB final-state frame has trailing bytes"
    prefix, payload, children = parsed
    if (protocol == 2 and prefix == b"$" and payload is None) or (
        protocol == 3 and prefix == b"_" and payload == b""
    ):
        return None
    assert prefix in {b"*", b"~"}, "VEMB final-state reply must be an aggregate"
    expected_component_prefix = b"$" if protocol == 2 else b","
    values = []
    for child_prefix, child_payload, grandchildren in children:
        assert (
            child_prefix == expected_component_prefix
            and child_payload is not None
            and not grandchildren
        ), "VEMB final-state component does not match the RESP protocol"
        value = float(child_payload)
        assert math.isfinite(value), "VEMB final-state components must be finite"
        values.append(value)
    return values


def _execute_final_observation(kiwi, reference, protocol, *command):
    kiwi_frame = kiwi.execute_raw(*command)
    redis_frame = reference.execute_raw(*command)
    if command[0] == b"VEMB":
        kiwi_vector = _normalize_raw_vemb(kiwi_frame, protocol)
        redis_vector = _normalize_raw_vemb(redis_frame, protocol)
        if redis_vector is None:
            assert kiwi_vector is None
        else:
            assert kiwi_vector is not None and len(kiwi_vector) == len(redis_vector)
            for kiwi_value, redis_value in zip(kiwi_vector, redis_vector):
                assert kiwi_value == pytest.approx(redis_value, abs=SCORE_TOLERANCE)
    else:
        assert kiwi_frame == redis_frame, (
            f"final-state {command[0].decode('ascii')} differs: "
            f"kiwi={kiwi_frame!r} redis={redis_frame!r}"
        )
    return _wire_exchange(command, kiwi_frame, redis_frame)


def _raw_integer(exchange, command):
    frame = base64.b64decode(exchange["kiwi_response_base64"], validate=True)
    if not frame.startswith(b":") or not frame.endswith(b"\r\n"):
        raise AssertionError(f"{command} must return a RESP integer frame, got {frame!r}")
    try:
        return int(frame[1:-2])
    except ValueError as error:
        raise AssertionError(f"{command} returned an invalid RESP integer {frame!r}") from error


def _raw_type(exchange):
    frame = base64.b64decode(exchange["kiwi_response_base64"], validate=True)
    if not frame.startswith(b"+") or not frame.endswith(b"\r\n"):
        raise AssertionError(f"TYPE must return a RESP simple string, got {frame!r}")
    return frame[1:-2]


def _profiled_final_keys(state_profile, protocol):
    if state_profile.startswith("typed-"):
        roles = TYPED_FINAL_KEY_ROLES
        prefix = f"test_vdiff:p{protocol}:".encode()
    elif state_profile.startswith("raw-"):
        roles = RAW_FINAL_KEY_ROLES
        prefix = f"test_vdiff:raw:p{protocol}:".encode()
    else:
        raise AssertionError(f"unknown final-state profile {state_profile!r}")
    keys = []
    for role in roles:
        if state_profile.startswith("raw-") and role not in {
            b"values",
            b"fp32",
            b"missing-scores",
        }:
            key = f"test_vdiff:raw:vadd:p{protocol}:".encode() + role
        else:
            key = prefix + role
        keys.append((role, key))
    return keys


def _capture_server_final_state(
    node_id, protocol, keys, kiwi, reference, contract=None
):
    if contract is None:
        profiled_keys = [(f"key-{index}".encode(), key) for index, key in enumerate(keys)]
        expected_types = None
    else:
        state_profile = contract["state_profile"]
        profiled_keys = _profiled_final_keys(state_profile, protocol)
        assert keys == [key for _role, key in profiled_keys], (
            f"known keys differ from final-state profile {state_profile}"
        )
        expected_types = FINAL_STATE_PROFILE_TYPES[state_profile]
    known_keys = []
    for role, key in profiled_keys:
        type_before = _execute_same_raw(kiwi, reference, b"TYPE", key)
        key_type = _raw_type(type_before)
        if key_type not in {b"none", b"string", b"vectorset"}:
            raise AssertionError(f"TYPE returned unsupported known-key type {key_type!r}")
        if expected_types is not None:
            expected_type = expected_types.get(role, b"none")
            assert key_type == expected_type, (
                f"final-state role {role!r} must be {expected_type!r}, got {key_type!r}"
            )
        pttl_before = _execute_same_raw(kiwi, reference, b"PTTL", key)
        expected_before_pttl = -2 if key_type == b"none" else -1
        actual_before_pttl = _raw_integer(pttl_before, "PTTL")
        assert actual_before_pttl == expected_before_pttl, (
            f"PTTL before cleanup must be {expected_before_pttl}, "
            f"got {actual_before_pttl} for {key!r}"
        )

        observations = []
        if key_type == b"vectorset":
            observations.append(
                _execute_final_observation(kiwi, reference, protocol, b"VCARD", key)
            )
            if contract is not None:
                observations.append(
                    _execute_final_observation(kiwi, reference, protocol, b"VDIM", key)
                )
                for member in FINAL_STATE_VECTOR_MEMBERS[role]:
                    observations.append(
                        _execute_final_observation(
                            kiwi, reference, protocol, b"VEMB", key, member
                        )
                    )
        elif key_type == b"string":
            observations.append(
                _execute_final_observation(kiwi, reference, protocol, b"GET", key)
            )

        first_del = _execute_same_raw(kiwi, reference, b"DEL", key)
        expected_first_del = 0 if key_type == b"none" else 1
        actual_first_del = _raw_integer(first_del, "DEL")
        assert actual_first_del == expected_first_del, (
            f"first DEL must return {expected_first_del}, got {actual_first_del} "
            f"for {key!r}"
        )
        type_after = _execute_same_raw(kiwi, reference, b"TYPE", key)
        assert _raw_type(type_after) == b"none", "TYPE after cleanup must be none"
        pttl_after = _execute_same_raw(kiwi, reference, b"PTTL", key)
        assert _raw_integer(pttl_after, "PTTL") == -2, (
            "PTTL after cleanup must be the missing-key sentinel -2"
        )
        second_del = _execute_same_raw(kiwi, reference, b"DEL", key)
        assert _raw_integer(second_del, "DEL") == 0, (
            "idempotent second DEL must return 0"
        )

        key_evidence = _encoded_bytes(key)
        known_keys.append(
            {
                "key_role": role.decode("ascii"),
                "key_base64": key_evidence["base64"],
                "key_sha256": key_evidence["sha256"],
                "before_cleanup": {
                    "type": type_before,
                    "pttl": pttl_before,
                    "observations": observations,
                },
                "cleanup": {
                    "first_del": first_del,
                    "after_type": type_after,
                    "after_pttl": pttl_after,
                    "second_del": second_del,
                },
            }
        )
    return {
        "schema": FINAL_STATE_SCHEMA,
        "node_id": node_id,
        "applicability": "server-backed",
        "protocol": protocol,
        "known_keys": known_keys,
    }


def _required_final_state_contract(node_id):
    registry_path = os.getenv("KIWI_VECTOR_REQUIRED_JOBS")
    required = os.getenv("KIWI_COMPAT_REQUIRE_ORACLE") == "1"
    if not registry_path and not required:
        return None
    if not registry_path:
        pytest.fail("canonical required-jobs path is missing for final-state evidence")
    try:
        with open(registry_path, encoding="utf-8") as source:
            registry = json.load(
                source, object_pairs_hook=_reject_duplicate_object_pairs
            )
    except (OSError, ValueError) as error:
        pytest.fail(f"canonical required-jobs cannot be read: {error}", pytrace=False)
    if registry.get("schema") != "kiwi-vector-required-jobs/canonical-v1":
        pytest.fail("canonical required-jobs schema identity mismatch", pytrace=False)
    contract = registry.get("final_state", {}).get(node_id)
    if not isinstance(contract, dict):
        pytest.fail(f"final-state applicability is missing for {node_id}", pytrace=False)
    return contract


def _record_server_final_state(node_id, protocol, keys, kiwi, reference):
    path = os.getenv("KIWI_VECTOR_FINAL_STATE")
    contract = _required_final_state_contract(node_id)
    if contract is None and not path:
        return
    if (
        not path
        or set(contract) != {
            "applicability",
            "state_profile",
            "observation_profile",
        }
        or contract["applicability"] != "server-backed"
        or contract["observation_profile"] != "complete-vector-state-v1"
    ):
        pytest.fail(f"server-backed final-state ownership drifted for {node_id}")
    _append_jsonl(
        path,
        _capture_server_final_state(
            node_id, protocol, keys, kiwi, reference, contract=contract
        ),
    )


@pytest.fixture(autouse=True)
def final_state_envelope(request):
    yield
    if "backends" in request.fixturenames or "raw_backends" in request.fixturenames:
        return
    contract = _required_final_state_contract(request.node.nodeid)
    if contract is None:
        return
    path = os.getenv("KIWI_VECTOR_FINAL_STATE")
    if not path or contract.get("applicability") != "not-applicable":
        pytest.fail(f"not-applicable final-state ownership drifted for {request.node.nodeid}")
    if set(contract) != {"applicability", "reason"} or contract["reason"] not in {
        "parser",
        "comparator",
    }:
        pytest.fail(f"not-applicable final-state reason drifted for {request.node.nodeid}")
    _append_jsonl(
        path,
        {
            "schema": FINAL_STATE_SCHEMA,
            "node_id": request.node.nodeid,
            "applicability": "not-applicable",
            "reason": contract["reason"],
        },
    )


def _endpoints_overlap():
    kiwi_addresses = {
        address[:2]
        for _family, _type, _proto, _canonname, address in socket.getaddrinfo(
            KIWI_HOST, KIWI_PORT, type=socket.SOCK_STREAM
        )
    }
    redis_addresses = {
        address[:2]
        for _family, _type, _proto, _canonname, address in socket.getaddrinfo(
            REDIS8_HOST, REDIS8_PORT, type=socket.SOCK_STREAM
        )
    }
    return bool(kiwi_addresses & redis_addresses)


def _require_distinct_endpoints():
    try:
        overlaps = _endpoints_overlap()
    except OSError as error:
        pytest.fail(f"trusted Oracle endpoint resolution failed: {error}", pytrace=False)
    if overlaps:
        pytest.fail(
            "Kiwi and Redis 8.8.1 Oracle must resolve to different endpoints",
            pytrace=False,
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
    _require_distinct_endpoints()
    protocol = request.param
    prefix = f"test_vdiff:p{protocol}:".encode()
    keys = [prefix + name for name in TEST_KEY_NAMES]
    kiwi = redis.Redis(
        host=KIWI_HOST, port=KIWI_PORT, decode_responses=False, protocol=protocol
    )
    reference = redis.Redis(
        host=REDIS8_HOST, port=REDIS8_PORT, decode_responses=False, protocol=protocol
    )
    clients = (kiwi, reference)
    try:
        for client in clients:
            client.delete(*keys)
    except (redis.RedisError, OSError) as error:
        for client in clients:
            client.close()
        pytest.fail(f"trusted Oracle endpoint setup failed: {error}", pytrace=False)

    try:
        yield kiwi, reference, protocol, prefix
    finally:
        cleanup_errors = []
        raw_kiwi = None
        raw_reference = None
        try:
            raw_kiwi = RawRespConnection.connect(KIWI_HOST, KIWI_PORT, protocol)
            raw_reference = RawRespConnection.connect(REDIS8_HOST, REDIS8_PORT, protocol)
            _record_server_final_state(
                request.node.nodeid,
                protocol,
                keys,
                raw_kiwi,
                raw_reference,
            )
        except (OSError, ValueError, EOFError, RuntimeError, AssertionError) as error:
            cleanup_errors.append(str(error))
        finally:
            if raw_kiwi is not None:
                raw_kiwi.close()
            if raw_reference is not None:
                raw_reference.close()
        for client in clients:
            try:
                client.delete(*keys)
            except (redis.RedisError, OSError) as error:
                cleanup_errors.append(str(error))
            finally:
                client.close()
        if cleanup_errors:
            pytest.fail(
                f"trusted Oracle endpoint cleanup failed: {cleanup_errors}",
                pytrace=False,
            )


@pytest.fixture
def raw_backends(request, raw_protocol):
    _require_distinct_endpoints()
    kiwi = None
    reference = None
    cleanup_errors = []
    try:
        kiwi = RawRespConnection.connect(KIWI_HOST, KIWI_PORT, raw_protocol)
        reference = RawRespConnection.connect(REDIS8_HOST, REDIS8_PORT, raw_protocol)
        if kiwi.socket.getpeername() == reference.socket.getpeername():
            pytest.fail(
                "Kiwi and Redis 8.8.1 Oracle connected to the same peer",
                pytrace=False,
            )
        keys = raw_test_keys(raw_protocol)
        reset_raw_client_keys(kiwi, keys, "Kiwi setup")
        reset_raw_client_keys(reference, keys, "Redis setup")
        yield kiwi, reference, raw_protocol
    except (OSError, ValueError, EOFError, AssertionError) as error:
        pytest.fail(f"trusted raw Oracle endpoint failed: {error}", pytrace=False)
    finally:
        if kiwi is not None and reference is not None:
            try:
                _record_server_final_state(
                    request.node.nodeid,
                    raw_protocol,
                    raw_test_keys(raw_protocol),
                    kiwi,
                    reference,
                )
            except (
                OSError,
                ValueError,
                EOFError,
                RuntimeError,
                AssertionError,
            ) as error:
                cleanup_errors.append(str(error))
        for endpoint, client in (("Redis teardown", reference), ("Kiwi teardown", kiwi)):
            if client is None:
                continue
            try:
                reset_raw_client_keys(client, raw_test_keys(raw_protocol), endpoint)
            except (
                OSError,
                ValueError,
                EOFError,
                RuntimeError,
                AssertionError,
            ) as error:
                cleanup_errors.append(str(error))
            finally:
                client.close()
        if cleanup_errors:
            pytest.fail(
                f"trusted raw Oracle endpoint cleanup failed: {cleanup_errors}",
                pytrace=False,
            )


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


def command_outcome(client, *command):
    """Return a reply or Redis error so differential cases can compare both."""
    try:
        return ("reply", client.execute_command(*command))
    except redis.ResponseError as error:
        return ("error", str(error))


def assert_same_outcome(kiwi, reference, *command):
    kiwi_outcome = command_outcome(kiwi, *command)
    redis_outcome = command_outcome(reference, *command)
    assert kiwi_outcome == redis_outcome, (
        f"{command!r}: kiwi={kiwi_outcome!r} != redis={redis_outcome!r}"
    )
    return kiwi_outcome


def assert_same_raw(kiwi, reference, *command, coverage=None, case_id="zero-vector"):
    kiwi_frame = kiwi.execute_raw(*command)
    reference_frame = reference.execute_raw(*command)
    assert kiwi_frame == reference_frame, (
        f"{command!r}: kiwi={kiwi_frame!r} != redis={reference_frame!r}"
    )
    if coverage is not None:
        coverage(
            command,
            kiwi_frame,
            reference_frame,
            case_id=case_id,
            comparison_kind="exact-frame",
            registered_difference_ids=(),
        )
    return kiwi_frame


def test_raw_comparator_rejects_equal_typed_values_with_different_frames(monkeypatch):
    class FrameClient:
        def __init__(self, frame):
            self.frame = frame

        def execute_raw(self, *command):
            assert command == (b"VISMEMBER", b"key", b"member")
            return self.frame

        def execute_command(self, *command):
            assert command == (b"VISMEMBER", b"key", b"member")
            return True

    with pytest.raises(AssertionError, match="kiwi=.*redis="):
        assert_same_raw(
            FrameClient(b":1\r\n"),
            FrameClient(b"#t\r\n"),
            b"VISMEMBER",
            b"key",
            b"member",
        )

    with TemporaryDirectory() as scratch:
        transcript = Path(scratch) / "raw-transcript.jsonl"
        monkeypatch.setenv("KIWI_VECTOR_RAW_TRANSCRIPT", str(transcript))
        monkeypatch.setenv(
            "PYTEST_CURRENT_TEST",
            "tests/python/test_vector_set_differential.py::"
            "test_raw_comparator_rejects_equal_typed_values_with_different_frames (call)",
        )
        parts = (b"VADD", b"nul\x00key", b"FP32", b"\x00\x01\x00", b"member\x00")
        recorder = raw_transcript_recorder(2)
        recorder(
            parts,
            b":1\r\n",
            b":1\r\n",
            case_id="zero-vector",
            comparison_kind="exact-frame",
            registered_difference_ids=(),
        )
        record = json.loads(transcript.read_text(encoding="utf-8"))
        request = base64.b64decode(record["request_base64"], validate=True)
        assert request == encode_command(*parts)
        assert b"\x00" in request
        assert record["request_sha256"] == hashlib.sha256(request).hexdigest()
        assert (
            base64.b64decode(record["kiwi_response_base64"], validate=True)
            == b":1\r\n"
        )
        assert (
            base64.b64decode(record["redis_response_base64"], validate=True)
            == b":1\r\n"
        )


test_raw_comparator_rejects_equal_typed_values_with_different_frames.transcript = True


def _read_resp_frame(frame, offset=0):
    line_end = frame.find(b"\r\n", offset)
    if line_end < 0:
        raise AssertionError("VINFO raw frame is missing CRLF")
    line_end += 2
    prefix = frame[offset : offset + 1]
    header = frame[offset:line_end]
    if prefix in b"+-:,#_(":
        payload = header[1:-2]
        if prefix == b":":
            unsigned = payload[1:] if payload.startswith(b"-") else payload
            canonical = (
                payload == b"0"
                or (
                    unsigned
                    and unsigned[:1] in b"123456789"
                    and unsigned.isdigit()
                )
            )
            if not canonical:
                raise AssertionError("VINFO integer is not a canonical RESP integer")
            value = int(payload)
            if not -(2**63) <= value <= 2**63 - 1:
                raise AssertionError("VINFO integer is not a canonical RESP integer")
        return (prefix, payload, []), line_end
    if prefix in b"$!=":
        try:
            length = int(header[1:-2])
        except ValueError as error:
            raise AssertionError("VINFO raw frame length is invalid") from error
        if length < 0:
            return (prefix, None, []), line_end
        payload_end = line_end + length
        if frame[payload_end : payload_end + 2] != b"\r\n":
            raise AssertionError("VINFO raw bulk payload is truncated")
        return (prefix, frame[line_end:payload_end], []), payload_end + 2
    if prefix not in b"*%":
        raise AssertionError(f"VINFO raw frame prefix is unsupported: {prefix!r}")
    try:
        count = int(header[1:-2])
    except ValueError as error:
        raise AssertionError("VINFO raw container count is invalid") from error
    child_count = count if prefix == b"*" else count * 2
    children = []
    cursor = line_end
    for _ in range(child_count):
        child, cursor = _read_resp_frame(frame, cursor)
        children.append(child)
    return (prefix, count, children), cursor


def test_vinfo_raw_parser_rejects_noncanonical_integer_grammar():
    invalid_payloads = (
        b"",
        b"-",
        b"+1",
        b"01",
        b"-0",
        b"-01",
        b"1x",
        b"9223372036854775808",
        b"-9223372036854775809",
    )
    for payload in invalid_payloads:
        with pytest.raises(AssertionError, match="canonical RESP integer"):
            _read_resp_frame(b":" + payload + b"\r\n")


def parse_vinfo_schema_frame(frame, protocol):
    expected_prefix = b"*" if protocol == 2 else b"%"
    assert frame[:1] == expected_prefix, "VINFO container type differs from RESP protocol"
    expected_header = b"*18\r\n" if protocol == 2 else b"%9\r\n"
    assert frame.startswith(expected_header), "VINFO pair count differs from Redis"
    parsed, consumed = _read_resp_frame(frame)
    assert consumed == len(frame), "VINFO raw frame has trailing bytes"
    prefix, count, children = parsed
    assert prefix == expected_prefix, "VINFO container type differs from RESP protocol"
    expected_count = 18 if protocol == 2 else 9
    assert count == expected_count, "VINFO pair count differs from Redis"
    assert len(children) == 18
    pairs = []
    for index in range(0, len(children), 2):
        field_prefix, field_name, field_children = children[index]
        assert not field_children
        assert field_prefix == b"+", "VINFO field token must be a simple string"
        value_prefix, value_payload, value_children = children[index + 1]
        assert not value_children
        pairs.append((field_name, value_prefix, value_payload))
    return pairs


def assert_same_vinfo_schema_raw(kiwi, reference, key, protocol, coverage=None):
    kiwi_frame = kiwi.execute_raw(b"VINFO", key)
    redis_frame = reference.execute_raw(b"VINFO", key)
    kiwi_pairs = parse_vinfo_schema_frame(kiwi_frame, protocol)
    redis_pairs = parse_vinfo_schema_frame(redis_frame, protocol)
    kiwi_fields = [field for field, _prefix, _payload in kiwi_pairs]
    redis_fields = [field for field, _prefix, _payload in redis_pairs]
    assert kiwi_fields == redis_fields, "VINFO field token order differs from Redis"
    allowed_payload_differences = set(VINFO_DIFFERENCE_IDS)
    registered_difference_ids = []
    for (field, kiwi_prefix, kiwi_payload), (_, redis_prefix, redis_payload) in zip(
        kiwi_pairs, redis_pairs
    ):
        assert kiwi_prefix == redis_prefix, (
            f"VINFO {field!r} value frame type differs from Redis"
        )
        if field not in allowed_payload_differences:
            assert kiwi_payload == redis_payload, (
                f"VINFO {field.decode('ascii')} value differs from Redis"
            )
        elif kiwi_payload != redis_payload:
            registered_difference_ids.append(VINFO_DIFFERENCE_IDS[field])
    if coverage is not None:
        coverage(
            (b"VINFO", key),
            kiwi_frame,
            redis_frame,
            case_id="populated",
            comparison_kind="raw-schema",
            registered_difference_ids=registered_difference_ids,
        )
    return kiwi_frame, redis_frame


def _vinfo_schema_frame(protocol, *, hnsw_m, max_level, vset_uid, max_node_uid):
    header = b"*18\r\n" if protocol == 2 else b"%9\r\n"
    return header + b"".join(
        (
            b"+quant-type\r\n+f32\r\n",
            b"+hnsw-m\r\n:" + str(hnsw_m).encode() + b"\r\n",
            b"+vector-dim\r\n:2\r\n",
            b"+projection-input-dim\r\n:0\r\n",
            b"+size\r\n:1\r\n",
            b"+max-level\r\n:" + str(max_level).encode() + b"\r\n",
            b"+attributes-count\r\n:0\r\n",
            b"+vset-uid\r\n:" + str(vset_uid).encode() + b"\r\n",
            b"+hnsw-max-node-uid\r\n:" + str(max_node_uid).encode() + b"\r\n",
        )
    )


class _RawFrameClient:
    def __init__(self, frame):
        self.frame = frame

    def execute_raw(self, *command):
        assert command == (b"VINFO", b"key")
        return self.frame


@pytest.mark.parametrize("protocol", [2, 3], ids=["resp2", "resp3"])
def test_vinfo_raw_schema_allows_only_registered_value_payload_differences(protocol):
    kiwi_frame = _vinfo_schema_frame(
        protocol, hnsw_m=0, max_level=0, vset_uid=7, max_node_uid=0
    )
    redis_frame = _vinfo_schema_frame(
        protocol, hnsw_m=16, max_level=3, vset_uid=42, max_node_uid=9
    )
    assert_same_vinfo_schema_raw(
        _RawFrameClient(kiwi_frame), _RawFrameClient(redis_frame), b"key", protocol
    )


@pytest.mark.parametrize(
    ("mutant", "expected"),
    [
        ("field-token-prefix", "field token"),
        ("container", "container"),
        ("pair-count", "pair count"),
        ("field-order", "field token order"),
        ("value-frame-type", "value frame type"),
        ("hnsw-malformed-integer", "canonical RESP integer"),
        ("uid-malformed-integer", "canonical RESP integer"),
        ("unregistered-value", "quant-type"),
    ],
)
def test_vinfo_raw_schema_rejects_unregistered_wire_drift(mutant, expected):
    reference = _vinfo_schema_frame(
        3, hnsw_m=16, max_level=3, vset_uid=42, max_node_uid=9
    )
    if mutant == "field-token-prefix":
        kiwi = reference.replace(b"+hnsw-m\r\n", b"$6\r\nhnsw-m\r\n", 1)
    elif mutant == "container":
        kiwi = reference.replace(b"%9\r\n", b"*18\r\n", 1)
    elif mutant == "pair-count":
        kiwi = reference.replace(b"%9\r\n", b"%8\r\n", 1)
    elif mutant == "field-order":
        first = b"+quant-type\r\n+f32\r\n"
        second = b"+hnsw-m\r\n:16\r\n"
        kiwi = reference.replace(first + second, second + first, 1)
    elif mutant == "value-frame-type":
        kiwi = reference.replace(b":16\r\n", b"$2\r\n16\r\n", 1)
    elif mutant == "hnsw-malformed-integer":
        kiwi = reference.replace(b":16\r\n", b":not-an-integer\r\n", 1)
    elif mutant == "uid-malformed-integer":
        kiwi = reference.replace(b":42\r\n", b":+42\r\n", 1)
    else:
        kiwi = reference.replace(b"+f32\r\n", b"+fp32\r\n", 1)

    with pytest.raises(AssertionError, match=expected):
        assert_same_vinfo_schema_raw(
            _RawFrameClient(kiwi), _RawFrameClient(reference), b"key", 3
        )


def reset_raw_key(kiwi, reference, key):
    reset_raw_client_keys(kiwi, [key], "Kiwi reset")
    reset_raw_client_keys(reference, [key], "Redis reset")


def delete_raw_keys(client, keys, endpoint):
    frame = client.execute_raw(b"DEL", *keys)
    if not frame.startswith(b":") or not frame.endswith(b"\r\n"):
        raise AssertionError(
            f"{endpoint} DEL must return a RESP integer frame, got {frame!r}"
        )
    try:
        deleted = int(frame[1:-2])
    except ValueError as error:
        raise AssertionError(
            f"{endpoint} DEL returned an invalid RESP integer: {frame!r}"
        ) from error
    if deleted < 0:
        raise AssertionError(f"{endpoint} DEL returned a negative count: {frame!r}")
    return deleted


def reset_raw_client_keys(client, keys, endpoint):
    delete_raw_keys(client, keys, endpoint)
    second = delete_raw_keys(client, keys, endpoint)
    if second != 0:
        raise AssertionError(
            f"{endpoint} DEL must be idempotent; second count was {second}"
        )


def raw_test_keys(protocol):
    vadd_prefix = f"test_vdiff:raw:vadd:p{protocol}:".encode()
    return [
        f"test_vdiff:raw:p{protocol}:values".encode(),
        f"test_vdiff:raw:p{protocol}:fp32".encode(),
        f"test_vdiff:raw:p{protocol}:missing-scores".encode(),
        *(
            vadd_prefix + suffix
            for suffix in (
                b"missing-values",
                b"missing-fp32",
                b"invalid-values",
                b"invalid-fp32",
                b"repeated",
                b"option",
            )
        ),
    ]


def test_raw_cleanup_requires_a_nonnegative_integer_frame():
    class ErrorFrameClient:
        def execute_raw(self, *command):
            assert command == (b"DEL", b"key")
            return b"-ERR cleanup disabled\r\n"

    with pytest.raises(AssertionError, match="RESP integer frame"):
        reset_raw_client_keys(ErrorFrameClient(), [b"key"], "fake endpoint")


def test_raw_endpoint_separation_and_cleanup_idempotency_guards(monkeypatch):
    assert _normalize_raw_vemb(b"$-1\r\n", 2) is None
    assert _normalize_raw_vemb(b"_\r\n", 3) is None
    with pytest.raises(AssertionError):
        _normalize_raw_vemb(b"_\r\n", 2)
    with pytest.raises(AssertionError):
        _normalize_raw_vemb(b"$-1\r\n", 3)
    assert _normalize_raw_vemb(b"*2\r\n$3\r\n1.0\r\n$4\r\n-2.5\r\n", 2) == [
        1.0,
        -2.5,
    ]
    assert _normalize_raw_vemb(b"*2\r\n,1.0\r\n,-2.5\r\n", 3) == [1.0, -2.5]
    with pytest.raises(AssertionError, match="RESP protocol"):
        _normalize_raw_vemb(b"*1\r\n:1\r\n", 2)
    with pytest.raises(AssertionError, match="RESP protocol"):
        _normalize_raw_vemb(b"*1\r\n,1.0\r\n", 2)
    with pytest.raises(AssertionError, match="RESP protocol"):
        _normalize_raw_vemb(b"*1\r\n$3\r\n1.0\r\n", 3)
    with pytest.raises(AssertionError, match="finite"):
        _normalize_raw_vemb(b"*1\r\n,inf\r\n", 3)
    with pytest.raises(ValueError):
        _normalize_raw_vemb(b"*1\r\n$3\r\nbad\r\n", 2)

    address = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 6379))
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [address])
    with pytest.raises(pytest.fail.Exception, match="different endpoints"):
        _require_distinct_endpoints()

    class CountingClient:
        def __init__(self):
            self.commands = []

        def execute_raw(self, *command):
            self.commands.append(command)
            return b":1\r\n" if len(self.commands) == 1 else b":0\r\n"

    client = CountingClient()
    reset_raw_client_keys(client, [b"key"], "fake endpoint")
    assert client.commands == [(b"DEL", b"key"), (b"DEL", b"key")]

    class StateClient:
        def __init__(self, *, type_frame=b"+vectorset\r\n", pttl_frame=b":-1\r\n"):
            self.type_frame = type_frame
            self.pttl_frame = pttl_frame

        def execute_raw(self, *command):
            if command[0] == b"TYPE":
                return self.type_frame
            if command[0] == b"PTTL":
                return self.pttl_frame
            if command[0] == b"VCARD":
                return b":1\r\n"
            if command[0] == b"DEL":
                return b":1\r\n"
            raise AssertionError(f"unexpected command {command!r}")

    with pytest.raises(AssertionError, match="TYPE"):
        _capture_server_final_state(
            "tests/python/test_vector_set_differential.py::fake[resp2]",
            2,
            [b"key"],
            StateClient(),
            StateClient(type_frame=b"+string\r\n"),
        )
    with pytest.raises(AssertionError, match="PTTL"):
        _capture_server_final_state(
            "tests/python/test_vector_set_differential.py::fake[resp2]",
            2,
            [b"key"],
            StateClient(),
            StateClient(pttl_frame=b":-2\r\n"),
        )

    with TemporaryDirectory() as scratch:
        duplicate_registry = Path(scratch) / "duplicate-required-jobs.json"
        duplicate_registry.write_text(
            '{"schema":"invalid","schema":"kiwi-vector-required-jobs/canonical-v1",'
            '"final_state":{"fake":{"applicability":"not-applicable",'
            '"reason":"comparator"}}}\n',
            encoding="utf-8",
        )
        monkeypatch.setenv("KIWI_VECTOR_REQUIRED_JOBS", str(duplicate_registry))
        with pytest.raises(pytest.fail.Exception, match="duplicate JSON object key"):
            _required_final_state_contract("fake")


test_raw_endpoint_separation_and_cleanup_idempotency_guards.final_state = True
test_raw_endpoint_separation_and_cleanup_idempotency_guards.ttl = True


def raw_transcript_recorder(protocol):
    path = os.getenv("KIWI_VECTOR_RAW_TRANSCRIPT")
    node_id = os.getenv("PYTEST_CURRENT_TEST", "").split(" ", 1)[0]
    if not path and os.getenv("KIWI_COMPAT_REQUIRE_ORACLE") != "1":
        return lambda _command, _kiwi_frame, _redis_frame, **_metadata: None
    if not path or not node_id:
        pytest.fail("required raw transcript destination or pytest node ID is missing")

    def record(
        command,
        kiwi_frame,
        redis_frame,
        *,
        case_id,
        comparison_kind,
        registered_difference_ids,
    ):
        request = encode_command(*command)
        request_evidence = _encoded_bytes(request)
        kiwi_evidence = _encoded_bytes(kiwi_frame)
        redis_evidence = _encoded_bytes(redis_frame)
        entry = {
            "schema": RAW_TRANSCRIPT_SCHEMA,
            "case_id": case_id,
            "command": command[0].decode("ascii"),
            "comparison_kind": comparison_kind,
            "node_id": node_id,
            "protocol": protocol,
            "request_base64": request_evidence["base64"],
            "request_sha256": request_evidence["sha256"],
            "kiwi_response_base64": kiwi_evidence["base64"],
            "kiwi_response_sha256": kiwi_evidence["sha256"],
            "redis_response_base64": redis_evidence["base64"],
            "redis_response_sha256": redis_evidence["sha256"],
            "registered_difference_ids": list(registered_difference_ids),
        }
        _append_jsonl(path, entry)

    return record


def assert_zero_vector_raw(raw_backends, kind, payload, element):
    kiwi, reference, protocol = raw_backends
    key = f"test_vdiff:raw:p{protocol}:{kind.decode().lower()}".encode()
    reset_raw_key(kiwi, reference, key)
    coverage = raw_transcript_recorder(protocol)
    assert_same_raw(
        kiwi,
        reference,
        b"VADD",
        key,
        kind,
        *payload,
        element,
        b"NOQUANT",
        coverage=coverage,
    )
    assert_same_raw(kiwi, reference, b"VCARD", key, coverage=coverage)
    assert_same_raw(kiwi, reference, b"VDIM", key, coverage=coverage)
    assert_same_raw(kiwi, reference, b"VEMB", key, element, coverage=coverage)
    missing_key = f"test_vdiff:raw:p{protocol}:missing-scores".encode()
    assert_same_raw(
        kiwi,
        reference,
        b"VINFO",
        missing_key,
        coverage=coverage,
        case_id="missing-key",
    )
    assert_same_vinfo_schema_raw(kiwi, reference, key, protocol, coverage=coverage)
    assert_same_raw(kiwi, reference, b"VISMEMBER", key, element, coverage=coverage)
    hits = assert_same_raw(
        kiwi,
        reference,
        b"VSIM",
        key,
        b"ELE",
        element,
        b"WITHSCORES",
        b"COUNT",
        b"2",
        coverage=coverage,
    )
    assert hits.startswith(b"%" if protocol == 3 else b"*")
    assert_same_raw(kiwi, reference, b"VREM", key, element, coverage=coverage)


def test_zero_vector_values_raw_differential(raw_backends):
    assert_zero_vector_raw(
        raw_backends, b"VALUES", (b"2", b"0", b"0"), b"zero"
    )


def test_zero_vector_fp32_raw_differential(raw_backends):
    assert_zero_vector_raw(
        raw_backends, b"FP32", (struct.pack("<ff", 0.0, 0.0),), b"\x00zero"
    )


def test_vsim_missing_key_withscores_raw_frame(raw_backends):
    kiwi, reference, protocol = raw_backends
    key = f"test_vdiff:raw:p{protocol}:missing-scores".encode()
    reset_raw_key(kiwi, reference, key)

    frame = assert_same_raw(
        kiwi,
        reference,
        b"VSIM",
        key,
        b"VALUES",
        b"1",
        b"1",
        b"WITHSCORES",
    )
    assert frame == b"*0\r\n"


def test_vadd_typed_error_precedence_raw(raw_backends):
    kiwi, reference, protocol = raw_backends
    prefix = f"test_vdiff:raw:vadd:p{protocol}:".encode()
    cases = [
        (
            (b"VADD", prefix + b"missing-values", b"VALUES", b"1", b"1"),
            b"wrong number of arguments",
        ),
        (
            (b"VADD", prefix + b"missing-fp32", b"FP32", b"bad"),
            b"wrong number of arguments",
        ),
        (
            (
                b"VADD",
                prefix + b"invalid-values",
                b"VALUES",
                b"1",
                b"not-a-float",
            ),
            b"invalid vector specification",
        ),
        (
            (b"VADD", prefix + b"invalid-fp32", b"FP32", b"bad", b"element"),
            b"invalid vector specification",
        ),
    ]
    for command, expected in cases:
        frame = assert_same_raw(kiwi, reference, *command)
        assert frame.startswith(b"-ERR ")
        assert expected in frame.lower()

    repeated_key = prefix + b"repeated"
    reset_raw_key(kiwi, reference, repeated_key)
    repeated = assert_same_raw(
        kiwi,
        reference,
        b"VADD",
        repeated_key,
        b"VALUES",
        b"1",
        b"1",
        b"element",
        b"NOQUANT",
        b"NOQUANT",
    )
    assert repeated in (b":1\r\n", b"#t\r\n")

    invalid_option = assert_same_raw(
        kiwi,
        reference,
        b"VADD",
        prefix + b"option",
        b"VALUES",
        b"1",
        b"1",
        b"element",
        b"invalid-option",
    )
    assert invalid_option == b"-ERR invalid option after element\r\n"


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
    return kiwi_hits


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


def test_repeated_vadd_and_vsim_options_match(backends):
    kiwi, reference, protocol, prefix = backends
    main_key = prefix + b"main"

    for element, vector in MAIN_MEMBERS[:2]:
        assert_same_reply(
            kiwi,
            reference,
            b"VADD",
            main_key,
            b"VALUES",
            len(vector),
            *vector,
            element,
            b"NOQUANT",
            b"NOQUANT",
        )
    hits = assert_same_vsim(
        kiwi,
        reference,
        protocol,
        (main_key, b"VALUES", 4, *MAIN_QUERY),
        b"WITHSCORES",
        b"WITHSCORES",
        b"TRUTH",
        b"TRUTH",
        b"COUNT",
        16,
        b"COUNT",
        1,
    )
    assert len(hits) == 1


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
    assert_same_outcome(kiwi, reference, b"VDIM", missing_key)


def test_vsim_missing_key_precedes_malformed_and_option_validation(backends):
    kiwi, reference, _protocol, prefix = backends
    missing_key = prefix + b"missing"
    malformed_and_option_variants = [
        (b"FP32", b"bad"),
        (b"FP32", b"bad", b"COUNT", b"0"),
        (b"FP32", b"bad", b"UNKNOWN"),
        (b"FP32", b"bad", b"COUNT", b"0", b"COUNT", b"0"),
    ]

    for args in malformed_and_option_variants:
        assert_same_outcome(kiwi, reference, b"VSIM", missing_key, *args)


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
