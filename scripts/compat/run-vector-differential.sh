#!/usr/bin/env bash
# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -Eeuo pipefail

die() {
    printf 'trusted Vector differential: %s\n' "$*" >&2
    exit 1
}

script_path=${BASH_SOURCE[0]}
[[ $script_path == /* ]] || script_path=$PWD/$script_path
script_directory=${script_path%/*}
cd -P -- "$script_directory"
script_directory=$PWD
repository_root=$(cd -P -- "$script_directory/../.." && pwd)

canonicalize_required_jobs() {
    local registry=$1
    local canonical=$2
    local mode=${3:-production}
    local helper=$repository_root/target/debug/kiwi-required-vector-jobs
    if [[ $mode == test && -n ${KIWI_COMPAT_TEST_REQUIRED_JOBS_HELPER:-} ]]; then
        helper=$KIWI_COMPAT_TEST_REQUIRED_JOBS_HELPER
    fi
    [[ -x $helper ]] || die "authoritative required-jobs helper is missing: $helper"
    "$helper" "$registry" >"$canonical" \
        || die 'required-jobs registry failed authoritative validation'
    [[ -s $canonical ]] || die 'authoritative required-jobs helper produced no canonical JSON'
}

validate_collection() (
    local canonical=$1
    local collected=$2
    local scratch
    scratch=$(mktemp -d "${TMPDIR:-/tmp}/kiwi-vector-collection.XXXXXX")
    trap 'rm -rf -- "$scratch"' EXIT

    /usr/bin/python3 -I -B - "$canonical" "$scratch/expected" <<'PY'
import json
import pathlib
import sys


def reject_duplicate_object_pairs(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate JSON object key {key!r}")
        document[key] = value
    return document


document = json.loads(
    pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"),
    object_pairs_hook=reject_duplicate_object_pairs,
)
if document.get("schema") != "kiwi-vector-required-jobs/canonical-v1":
    raise SystemExit("canonical required-jobs schema identity mismatch")
expected = document.get("expected_node_ids")
count = document.get("expected_item_count")
if (
    not isinstance(expected, list)
    or not expected
    or any(not isinstance(node_id, str) or not node_id for node_id in expected)
    or type(count) is not int
    or count <= 0
    or len(expected) != count
):
    raise SystemExit("canonical expected node IDs/count drifted")
pathlib.Path(sys.argv[2]).write_text("\n".join(expected) + "\n", encoding="utf-8")
PY
    grep '^tests/python/test_vector_set_differential.py::' "$collected" \
        >"$scratch/actual" || true
    [[ $(wc -l <"$scratch/actual") -eq $(wc -l <"$scratch/expected") ]] \
        || die 'pytest collected count differs from the registry'
    cmp -s "$scratch/expected" "$scratch/actual" \
        || die 'pytest collected node IDs differ from the registry'
)

finish_callback() {
    local callback_status=$1
    local cleanup_status=$2
    [[ $cleanup_status -eq 0 ]] || return "$cleanup_status"
    return "$callback_status"
}

validate_summary() {
    local canonical=$1
    local summary=$2
    /usr/bin/python3 -I -B - "$canonical" "$summary" <<'PY'
import json
import pathlib
import sys

def reject_duplicate_object_pairs(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate JSON object key {key!r}")
        document[key] = value
    return document


def strict_json_loads(source):
    return json.loads(source, object_pairs_hook=reject_duplicate_object_pairs)


registry = strict_json_loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
if registry.get("schema") != "kiwi-vector-required-jobs/canonical-v1":
    raise SystemExit("canonical required-jobs schema identity mismatch")
expected = registry.get("expected_item_count")
if type(expected) is not int or expected <= 0:
    raise SystemExit("canonical expected_item_count is invalid")
document = strict_json_loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
required = {"collected", "passed", "failed", "skipped", "xfailed", "xpassed", "deselected"}
if set(document) != required:
    raise SystemExit("pytest summary fields drifted")
if any(type(document[name]) is not int or document[name] < 0 for name in required):
    raise SystemExit("pytest summary totals must be nonnegative integers")
if document["collected"] != expected or document["passed"] != expected:
    raise SystemExit("pytest collected/passed totals differ from the registry")
for name in ("failed", "skipped", "xfailed", "xpassed", "deselected"):
    if document[name] != 0:
        raise SystemExit(f"pytest reported forbidden {name}={document[name]}")
PY
}

validate_differential_jsonl() {
    local canonical=$1
    local evidence=$2
    local kind=$3
    /usr/bin/python3 -I -B - "$canonical" "$evidence" "$kind" <<'PY'
import base64
import binascii
import hashlib
import json
import math
import pathlib
import re
import sys


def reject_duplicate_object_pairs(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate JSON object key {key!r}")
        document[key] = value
    return document


def strict_json_loads(source):
    return json.loads(source, object_pairs_hook=reject_duplicate_object_pairs)


registry = strict_json_loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
evidence_path = pathlib.Path(sys.argv[2])
kind = sys.argv[3]
if registry.get("schema") != "kiwi-vector-required-jobs/canonical-v1":
    raise SystemExit("canonical required-jobs schema identity mismatch")
if not evidence_path.is_file() or evidence_path.is_symlink():
    raise SystemExit(f"{kind} evidence is missing or is not a regular file")
file_limit = 64 * 1024 * 1024 if kind == "transcript" else 4 * 1024 * 1024
if evidence_path.stat().st_size > file_limit:
    raise SystemExit(f"{kind} evidence exceeds its file-size bound")
try:
    lines = evidence_path.read_text(encoding="utf-8").splitlines()
except (OSError, UnicodeError) as error:
    raise SystemExit(f"{kind} evidence cannot be read: {error}")
if not lines or any(not line for line in lines):
    raise SystemExit(f"{kind} evidence is empty or contains blank records")

decoded_total = 0
decoded_limit = 16 * 1024 * 1024 if kind == "transcript" else 4 * 1024 * 1024


def decode_bytes(container, prefix, context):
    global decoded_total
    encoded = container.get(f"{prefix}_base64")
    digest = container.get(f"{prefix}_sha256")
    if not isinstance(encoded, str) or not isinstance(digest, str):
        raise SystemExit(f"{context} {prefix} evidence is missing")
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as error:
        raise SystemExit(f"{context} {prefix} is not strict Base64: {error}")
    if base64.b64encode(decoded).decode("ascii") != encoded:
        raise SystemExit(f"{context} {prefix} Base64 is not canonical")
    if hashlib.sha256(decoded).hexdigest() != digest:
        raise SystemExit(f"{context} {prefix} SHA-256 mismatch")
    decoded_total += len(decoded)
    if decoded_total > decoded_limit:
        raise SystemExit(f"{kind} decoded evidence exceeds its bound")
    return decoded


def canonical_integer(payload, *, allow_minus_one=False):
    if allow_minus_one and payload == b"-1":
        return -1
    if not re.fullmatch(rb"0|-?[1-9][0-9]*", payload):
        raise ValueError("non-canonical RESP integer")
    return int(payload)


def read_line(data, offset):
    end = data.find(b"\r\n", offset)
    if end < 0:
        raise ValueError("RESP line is truncated")
    return data[offset:end], end + 2


def parse_frame(data, offset=0, depth=0):
    if depth > 64 or offset >= len(data):
        raise ValueError("RESP frame depth or length is invalid")
    prefix = data[offset : offset + 1]
    payload, cursor = read_line(data, offset + 1)
    if prefix in b"+-,(":
        return (prefix, payload, []), cursor
    if prefix == b":":
        canonical_integer(payload)
        return (prefix, payload, []), cursor
    if prefix == b"#":
        if payload not in {b"t", b"f"}:
            raise ValueError("RESP Boolean is invalid")
        return (prefix, payload, []), cursor
    if prefix == b"_":
        if payload:
            raise ValueError("RESP null is invalid")
        return (prefix, payload, []), cursor
    if prefix in b"$!=":
        length = canonical_integer(payload, allow_minus_one=True)
        if length == -1:
            return (prefix, None, []), cursor
        if length < 0 or cursor + length + 2 > len(data):
            raise ValueError("RESP bulk frame length is invalid")
        end = cursor + length
        if data[end : end + 2] != b"\r\n":
            raise ValueError("RESP bulk frame is truncated")
        return (prefix, data[cursor:end], []), end + 2
    if prefix not in b"*%~>":
        raise ValueError(f"unsupported RESP prefix {prefix!r}")
    count = canonical_integer(payload, allow_minus_one=True)
    if count == -1:
        return (prefix, count, []), cursor
    if count < 0 or count > 100000:
        raise ValueError("RESP aggregate count is invalid")
    child_count = count * 2 if prefix == b"%" else count
    children = []
    for _ in range(child_count):
        child, cursor = parse_frame(data, cursor, depth + 1)
        children.append(child)
    return (prefix, count, children), cursor


def one_frame(data, context):
    try:
        parsed, consumed = parse_frame(data)
    except ValueError as error:
        raise SystemExit(f"{context} is not a complete RESP frame: {error}")
    if consumed != len(data):
        raise SystemExit(f"{context} has trailing RESP bytes")
    return parsed


def parse_request(data, context):
    try:
        header, cursor = read_line(data, 1)
        if data[:1] != b"*":
            raise ValueError("request is not a RESP array")
        count = canonical_integer(header)
        if count <= 0 or count > 100000:
            raise ValueError("request item count is invalid")
        parts = []
        for _ in range(count):
            if data[cursor : cursor + 1] != b"$":
                raise ValueError("request item is not a bulk string")
            length_payload, cursor = read_line(data, cursor + 1)
            length = canonical_integer(length_payload)
            if length < 0 or cursor + length + 2 > len(data):
                raise ValueError("request bulk length is invalid")
            end = cursor + length
            if data[end : end + 2] != b"\r\n":
                raise ValueError("request bulk item is truncated")
            parts.append(data[cursor:end])
            cursor = end + 2
        if cursor != len(data):
            raise ValueError("request has trailing bytes")
        return parts
    except ValueError as error:
        raise SystemExit(f"{context} request is invalid: {error}")


def vinfo_differences(kiwi, redis, protocol, context):
    kiwi_parsed = one_frame(kiwi, f"{context} Kiwi response")
    redis_parsed = one_frame(redis, f"{context} Redis response")
    expected_prefix = b"*" if protocol == 2 else b"%"
    expected_count = 18 if protocol == 2 else 9
    pairs = []
    for name, parsed in (("Kiwi", kiwi_parsed), ("Redis", redis_parsed)):
        prefix, count, children = parsed
        if prefix != expected_prefix or count != expected_count or len(children) != 18:
            raise SystemExit(f"{context} {name} VINFO container drifted")
        current = []
        for index in range(0, len(children), 2):
            field_prefix, field, field_children = children[index]
            value_prefix, value, value_children = children[index + 1]
            if field_prefix != b"+" or field_children or value_children:
                raise SystemExit(f"{context} {name} VINFO field shape drifted")
            current.append((field, value_prefix, value))
        pairs.append(current)
    if [field for field, _prefix, _value in pairs[0]] != [
        field for field, _prefix, _value in pairs[1]
    ]:
        raise SystemExit(f"{context} VINFO field order drifted")
    allowed = {
        b"hnsw-m": "vinfo-hnsw-m",
        b"max-level": "vinfo-max-level",
        b"vset-uid": "vinfo-vset-uid",
        b"hnsw-max-node-uid": "vinfo-hnsw-max-node-uid",
    }
    differences = []
    for (field, kiwi_prefix, kiwi_value), (_, redis_prefix, redis_value) in zip(*pairs):
        if kiwi_prefix != redis_prefix:
            raise SystemExit(f"{context} VINFO value frame type drifted for {field!r}")
        if field not in allowed and kiwi_value != redis_value:
            raise SystemExit(f"{context} has an unregistered VINFO difference for {field!r}")
        if field in allowed and kiwi_value != redis_value:
            differences.append(allowed[field])
    return differences


def response_integer(frame, context):
    prefix, payload, children = one_frame(frame, context)
    if prefix != b":" or children:
        raise SystemExit(f"{context} must be a RESP integer")
    return int(payload)


def response_type(frame, context):
    prefix, payload, children = one_frame(frame, context)
    if prefix != b"+" or children or payload not in {b"none", b"string", b"vectorset"}:
        raise SystemExit(f"{context} must be a supported TYPE response")
    return payload


def response_vemb(frame, context):
    prefix, payload, children = one_frame(frame, context)
    if (prefix == b"$" and payload is None) or prefix == b"_":
        return None
    if prefix not in {b"*", b"~"}:
        raise SystemExit(f"{context} must be an aggregate or null VEMB response")
    values = []
    for child_prefix, child_payload, grandchildren in children:
        if child_prefix != b"$" or child_payload is None or grandchildren:
            raise SystemExit(f"{context} VEMB component is not a bulk string")
        try:
            value = float(child_payload)
        except ValueError as error:
            raise SystemExit(f"{context} VEMB component is not numeric: {error}")
        if not math.isfinite(value):
            raise SystemExit(f"{context} VEMB component is not finite")
        values.append(value)
    return values


def validate_exchange(exchange, expected_command, key, context, arguments=()):
    required = {
        "command", "request_base64", "request_sha256", "kiwi_response_base64",
        "kiwi_response_sha256", "redis_response_base64", "redis_response_sha256",
    }
    if not isinstance(exchange, dict) or set(exchange) != required:
        raise SystemExit(f"{context} wire exchange fields drifted")
    if exchange["command"] != expected_command:
        raise SystemExit(f"{context} command identity drifted")
    request = decode_bytes(exchange, "request", context)
    expected_request = [expected_command.encode("ascii"), key, *arguments]
    if parse_request(request, context) != expected_request:
        raise SystemExit(f"{context} request bytes do not target the recorded key")
    kiwi = decode_bytes(exchange, "kiwi_response", context)
    redis = decode_bytes(exchange, "redis_response", context)
    if expected_command == "VEMB":
        kiwi_vector = response_vemb(kiwi, f"{context} Kiwi response")
        redis_vector = response_vemb(redis, f"{context} Redis response")
        if (kiwi_vector is None) != (redis_vector is None):
            raise SystemExit(f"{context} Kiwi/Redis VEMB nullability differs")
        if kiwi_vector is not None and (
            len(kiwi_vector) != len(redis_vector)
            or any(
                not math.isclose(left, right, rel_tol=0.0, abs_tol=1e-6)
                for left, right in zip(kiwi_vector, redis_vector)
            )
        ):
            raise SystemExit(f"{context} Kiwi/Redis VEMB values differ")
    else:
        one_frame(kiwi, f"{context} Kiwi response")
        one_frame(redis, f"{context} Redis response")
        if kiwi != redis:
            raise SystemExit(f"{context} Kiwi/Redis responses differ")
    return kiwi


if kind == "transcript":
    commands = registry.get("commands")
    raw_cases = registry.get("raw_cases")
    if not isinstance(commands, list) or not isinstance(raw_cases, dict):
        raise SystemExit("canonical commands or raw cases are missing")
    expected = {}
    for command, cases in raw_cases.items():
        if not isinstance(command, str) or not isinstance(cases, list):
            raise SystemExit("canonical raw cases are invalid")
        for case in cases:
            if not isinstance(case, dict) or set(case) != {
                "case_id", "evidence_kind", "node_ids", "request_base64_by_node"
            }:
                raise SystemExit("canonical raw case fields drifted")
            case_id = case["case_id"]
            evidence_kind = case["evidence_kind"]
            node_ids = case["node_ids"]
            requests = case["request_base64_by_node"]
            if (
                not isinstance(case_id, str)
                or not isinstance(evidence_kind, str)
                or not isinstance(node_ids, list)
                or not node_ids
                or any(not isinstance(node_id, str) for node_id in node_ids)
                or len(node_ids) != len(set(node_ids))
                or not isinstance(requests, dict)
                or set(requests) != set(node_ids)
            ):
                raise SystemExit("canonical raw case request ownership drifted")
            for node_id in node_ids:
                context = f"canonical raw case {command}/{case_id}/{node_id}"
                encoded_request = requests[node_id]
                if not isinstance(encoded_request, str):
                    raise SystemExit(f"{context} request Base64 is invalid")
                try:
                    canonical_request = base64.b64decode(encoded_request, validate=True)
                except (binascii.Error, ValueError) as error:
                    raise SystemExit(f"{context} request is not strict Base64: {error}")
                if len(canonical_request) > 64 * 1024:
                    raise SystemExit(f"{context} request exceeds its byte limit")
                parts = parse_request(canonical_request, context)
                if not parts or parts[0] != command.encode("ascii"):
                    raise SystemExit(f"{context} command differs from exact request bytes")
                identity = (command, case_id, evidence_kind, node_id)
                if identity in expected:
                    raise SystemExit(f"canonical raw case duplicates {identity!r}")
                expected[identity] = canonical_request
    if {item[0] for item in expected} != set(commands):
        raise SystemExit("registry raw-case command coverage drifted")
    observed = set()
    fields = {
        "schema", "node_id", "case_id", "protocol", "command", "comparison_kind",
        "request_base64", "request_sha256", "kiwi_response_base64",
        "kiwi_response_sha256", "redis_response_base64", "redis_response_sha256",
        "registered_difference_ids",
    }
    for line_number, line in enumerate(lines, 1):
        context = f"raw transcript line {line_number}"
        try:
            record = strict_json_loads(line)
        except (json.JSONDecodeError, ValueError) as error:
            raise SystemExit(f"{context} is invalid JSON: {error}")
        if not isinstance(record, dict) or set(record) != fields:
            raise SystemExit(f"{context} fields drifted")
        if record["schema"] != "kiwi-vector-wire-transcript/v1":
            raise SystemExit(f"{context} schema identity drifted")
        command = record["command"]
        case_id = record["case_id"]
        comparison_kind = record["comparison_kind"]
        node_id = record["node_id"]
        protocol = record["protocol"]
        if not all(isinstance(item, str) for item in (command, case_id, comparison_kind, node_id)):
            raise SystemExit(f"{context} identity fields are invalid")
        expected_protocol = 2 if node_id.endswith("[resp2]") else 3 if node_id.endswith("[resp3]") else None
        if protocol != expected_protocol:
            raise SystemExit(f"{context} protocol/node identity drifted")
        key = (command, case_id, comparison_kind, node_id)
        if key in observed:
            raise SystemExit(f"{context} duplicates {key!r}")
        observed.add(key)
        request = decode_bytes(record, "request", context)
        parts = parse_request(request, context)
        if not parts or parts[0] != command.encode("ascii"):
            raise SystemExit(f"{context} command differs from exact request bytes")
        if request != expected.get(key):
            raise SystemExit(f"{context} request differs from the registry's exact bytes")
        kiwi = decode_bytes(record, "kiwi_response", context)
        redis = decode_bytes(record, "redis_response", context)
        differences = record["registered_difference_ids"]
        if (
            not isinstance(differences, list)
            or any(not isinstance(item, str) for item in differences)
            or len(differences) != len(set(differences))
        ):
            raise SystemExit(f"{context} registered difference IDs are invalid")
        if comparison_kind == "exact-frame":
            one_frame(kiwi, f"{context} Kiwi response")
            one_frame(redis, f"{context} Redis response")
            if kiwi != redis or differences:
                raise SystemExit(f"{context} exact-frame comparison drifted")
        elif comparison_kind == "raw-schema" and command == "VINFO":
            actual_differences = vinfo_differences(kiwi, redis, protocol, context)
            if differences != actual_differences:
                raise SystemExit(f"{context} registered VINFO differences drifted")
        else:
            raise SystemExit(f"{context} comparison kind is not registered")
    if observed != set(expected):
        raise SystemExit(
            f"raw transcript differs from registry: missing={sorted(set(expected) - observed)} "
            f"extra={sorted(observed - set(expected))}"
        )
elif kind == "final-state":
    expected = registry.get("final_state")
    if not isinstance(expected, dict) or set(expected) != set(registry.get("expected_node_ids", [])):
        raise SystemExit("canonical final-state applicability drifted")
    typed_roles = ("main", "dense3", "string", "missing")
    raw_roles = (
        "values", "fp32", "missing-scores", "missing-values", "missing-fp32",
        "invalid-values", "invalid-fp32", "repeated", "option",
    )
    profile_types = {
        "raw-all-missing": {},
        "raw-repeated-vector": {"repeated": b"vectorset"},
        "typed-all-missing": {},
        "typed-main-vector": {"main": b"vectorset"},
        "typed-main-two-member-vector": {"main": b"vectorset"},
        "typed-main-dense3-vector": {
            "main": b"vectorset", "dense3": b"vectorset",
        },
        "typed-string": {"string": b"string"},
    }
    profile_member_counts = {
        "raw-repeated-vector": {"repeated": 1},
        "typed-main-vector": {"main": 8},
        "typed-main-two-member-vector": {"main": 2},
        "typed-main-dense3-vector": {"main": 8, "dense3": 3},
    }
    vector_members = {
        "main": (b"alpha", b"beta", b"gamma", b"delta", b"", b"\x00bin\x00", b"tie-a", b"tie-b", b"ghost"),
        "dense3": (b"x", b"y", b"z", b"ghost"),
        "repeated": (b"element", b"ghost"),
    }
    vector_dimensions = {"main": 4, "dense3": 3, "repeated": 1}

    def expected_key(role, state_profile, protocol):
        if state_profile.startswith("typed-"):
            return f"test_vdiff:p{protocol}:{role}".encode()
        if role in {"values", "fp32", "missing-scores"}:
            return f"test_vdiff:raw:p{protocol}:{role}".encode()
        return f"test_vdiff:raw:vadd:p{protocol}:{role}".encode()

    observed = set()
    for line_number, line in enumerate(lines, 1):
        context = f"final-state line {line_number}"
        try:
            record = strict_json_loads(line)
        except (json.JSONDecodeError, ValueError) as error:
            raise SystemExit(f"{context} is invalid JSON: {error}")
        if not isinstance(record, dict) or record.get("schema") != "kiwi-vector-final-state/v1":
            raise SystemExit(f"{context} schema identity drifted")
        node_id = record.get("node_id")
        if not isinstance(node_id, str) or node_id not in expected or node_id in observed:
            raise SystemExit(f"{context} node identity is missing, extra, or duplicate")
        observed.add(node_id)
        contract = expected[node_id]
        if record.get("applicability") != contract.get("applicability"):
            raise SystemExit(f"{context} applicability differs from the registry")
        if contract["applicability"] == "not-applicable":
            if set(contract) != {"applicability", "reason"}:
                raise SystemExit(f"{context} not-applicable registry fields drifted")
            if set(record) != {"schema", "node_id", "applicability", "reason"}:
                raise SystemExit(f"{context} not-applicable fields drifted")
            if record["reason"] != contract.get("reason"):
                raise SystemExit(f"{context} not-applicable reason drifted")
            continue
        if set(contract) != {
            "applicability", "state_profile", "observation_profile"
        }:
            raise SystemExit(f"{context} server-backed registry fields drifted")
        state_profile = contract["state_profile"]
        if (
            state_profile not in profile_types
            or contract["observation_profile"] != "complete-vector-state-v1"
        ):
            raise SystemExit(f"{context} final-state profile is unsupported")
        if set(record) != {"schema", "node_id", "applicability", "protocol", "known_keys"}:
            raise SystemExit(f"{context} server-backed fields drifted")
        protocol = record["protocol"]
        expected_protocol = 2 if node_id.endswith("[resp2]") else 3 if node_id.endswith("[resp3]") else None
        if protocol != expected_protocol:
            raise SystemExit(f"{context} protocol/node identity drifted")
        known_keys = record["known_keys"]
        expected_roles = typed_roles if state_profile.startswith("typed-") else raw_roles
        if not isinstance(known_keys, list) or len(known_keys) != len(expected_roles):
            raise SystemExit(f"{context} known keys are missing")
        observed_keys = set()
        for key_index, (key_record, expected_role) in enumerate(zip(known_keys, expected_roles)):
            key_context = f"{context} key {key_index}"
            if not isinstance(key_record, dict) or set(key_record) != {
                "key_role", "key_base64", "key_sha256", "before_cleanup", "cleanup"
            }:
                raise SystemExit(f"{key_context} fields drifted")
            if key_record["key_role"] != expected_role:
                raise SystemExit(f"{key_context} role differs from the registry profile")
            key = decode_bytes(key_record, "key", key_context)
            if key != expected_key(expected_role, state_profile, protocol):
                raise SystemExit(f"{key_context} bytes differ from the registry profile")
            if key in observed_keys:
                raise SystemExit(f"{key_context} duplicates a known key")
            observed_keys.add(key)
            before = key_record["before_cleanup"]
            cleanup = key_record["cleanup"]
            if not isinstance(before, dict) or set(before) != {"type", "pttl", "observations"}:
                raise SystemExit(f"{key_context} before-cleanup fields drifted")
            if not isinstance(cleanup, dict) or set(cleanup) != {
                "first_del", "after_type", "after_pttl", "second_del"
            }:
                raise SystemExit(f"{key_context} cleanup fields drifted")
            type_before = response_type(
                validate_exchange(before["type"], "TYPE", key, f"{key_context} TYPE before"),
                f"{key_context} TYPE before",
            )
            expected_type = profile_types[state_profile].get(expected_role, b"none")
            if type_before != expected_type:
                raise SystemExit(f"{key_context} TYPE differs from the registry profile")
            pttl_before = response_integer(
                validate_exchange(before["pttl"], "PTTL", key, f"{key_context} PTTL before"),
                f"{key_context} PTTL before",
            )
            expected_pttl = -2 if type_before == b"none" else -1
            if pttl_before != expected_pttl:
                raise SystemExit(f"{key_context} PTTL before must be {expected_pttl}")
            observations = before["observations"]
            expected_observations = []
            if type_before == b"string":
                expected_observations.append(("GET", ()))
            elif type_before == b"vectorset":
                expected_observations.extend((("VCARD", ()), ("VDIM", ())))
                expected_observations.extend(
                    ("VEMB", (member,)) for member in vector_members[expected_role]
                )
            if not isinstance(observations, list) or len(observations) != len(expected_observations):
                raise SystemExit(f"{key_context} type-specific observations drifted")
            observation_frames = []
            for observation, (expected_command, arguments) in zip(
                observations, expected_observations
            ):
                observation_frames.append(validate_exchange(
                    observation, expected_command, key,
                    f"{key_context} {expected_command} observation", arguments,
                ))
            if type_before == b"vectorset":
                card = response_integer(
                    observation_frames[0], f"{key_context} VCARD observation"
                )
                dimension = response_integer(
                    observation_frames[1], f"{key_context} VDIM observation"
                )
                if dimension != vector_dimensions[expected_role]:
                    raise SystemExit(f"{key_context} VDIM differs from its profile")
                members = vector_members[expected_role]
                member_vectors = [
                    response_vemb(frame, f"{key_context} VEMB observation")
                    for frame in observation_frames[2:]
                ]
                expected_member_count = profile_member_counts.get(state_profile, {}).get(
                    expected_role
                )
                if expected_member_count is None:
                    raise SystemExit(f"{key_context} Vector member count is not profiled")
                for member, vector in zip(
                    members[:expected_member_count],
                    member_vectors[:expected_member_count],
                ):
                    if vector is None:
                        raise SystemExit(
                            f"{key_context} profile member {member!r} is missing"
                        )
                    if len(vector) != dimension:
                        raise SystemExit(
                            f"{key_context} profile member {member!r} dimension drifted"
                        )
                for member, vector in zip(
                    members[expected_member_count:-1],
                    member_vectors[expected_member_count:-1],
                ):
                    if vector is not None:
                        raise SystemExit(
                            f"{key_context} undeclared profile member {member!r} must be missing"
                        )
                if member_vectors[-1] is not None:
                    raise SystemExit(f"{key_context} ghost member must be missing")
                if card != expected_member_count:
                    raise SystemExit(f"{key_context} VCARD differs from its profile")
            first_del = response_integer(
                validate_exchange(cleanup["first_del"], "DEL", key, f"{key_context} first DEL"),
                f"{key_context} first DEL",
            )
            if first_del != (0 if type_before == b"none" else 1):
                raise SystemExit(f"{key_context} first DEL sentinel drifted")
            type_after = response_type(
                validate_exchange(cleanup["after_type"], "TYPE", key, f"{key_context} TYPE after"),
                f"{key_context} TYPE after",
            )
            if type_after != b"none":
                raise SystemExit(f"{key_context} TYPE after cleanup must be none")
            pttl_after = response_integer(
                validate_exchange(cleanup["after_pttl"], "PTTL", key, f"{key_context} PTTL after"),
                f"{key_context} PTTL after",
            )
            if pttl_after != -2:
                raise SystemExit(f"{key_context} PTTL after cleanup must be -2")
            second_del = response_integer(
                validate_exchange(cleanup["second_del"], "DEL", key, f"{key_context} second DEL"),
                f"{key_context} second DEL",
            )
            if second_del != 0:
                raise SystemExit(f"{key_context} idempotent second DEL must be 0")
    if observed != set(expected):
        raise SystemExit(
            f"final-state envelopes differ from registry: missing={sorted(set(expected) - observed)} "
            f"extra={sorted(observed - set(expected))}"
        )
else:
    raise SystemExit(f"unsupported differential evidence kind {kind!r}")
PY
}

validate_raw_transcript() {
    validate_differential_jsonl "$1" "$2" transcript
}

validate_final_state() {
    validate_differential_jsonl "$1" "$2" final-state
}

validate_collect_summary() {
    local canonical=$1
    local summary=$2
    /usr/bin/python3 -I -B - "$canonical" "$summary" <<'PY'
import json
import pathlib
import sys


def reject_duplicate_object_pairs(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate JSON object key {key!r}")
        document[key] = value
    return document


def strict_json_loads(source):
    return json.loads(source, object_pairs_hook=reject_duplicate_object_pairs)


registry = strict_json_loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
document = strict_json_loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
required = {"collected", "passed", "failed", "skipped", "xfailed", "xpassed", "deselected"}
if set(document) != required:
    raise SystemExit("pytest collection summary fields or count drifted")
if any(type(document[name]) is not int or document[name] < 0 for name in required):
    raise SystemExit("pytest collection summary totals must be nonnegative integers")
if document["collected"] != registry.get("expected_item_count"):
    raise SystemExit("pytest collection summary fields or count drifted")
if document["passed"] != 0 or any(
    document[name] != 0 for name in ("failed", "skipped", "xfailed", "xpassed", "deselected")
):
    raise SystemExit("pytest collection summary outcomes drifted")
PY
}

validate_evidence_set() {
    local canonical=$1
    local evidence_directory=$2
    /usr/bin/python3 -I -B - "$canonical" "$evidence_directory" <<'PY'
import os
import pathlib
import sys

canonical = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
if not root.is_dir() or root.is_symlink():
    raise SystemExit("differential evidence directory is missing or unsafe")
files = {
    "vector-required-jobs.json": 1024 * 1024,
    "kiwi.conf": 64 * 1024,
    "kiwi.log": 8 * 1024 * 1024,
    "kiwi-runtime.json": 1024 * 1024,
    "callback-cleanup.json": 1024 * 1024,
    "collect.log": 8 * 1024 * 1024,
    "collect-summary.json": 1024 * 1024,
    "pytest.log": 8 * 1024 * 1024,
    "run-summary.json": 1024 * 1024,
    "raw-transcript.jsonl": 64 * 1024 * 1024,
    "final-state.jsonl": 4 * 1024 * 1024,
}
observed = {entry.name for entry in root.iterdir()}
expected = set(files)
if observed != expected:
    raise SystemExit(
        f"differential evidence artifact allowlist drifted: "
        f"missing={sorted(expected - observed)} extra={sorted(observed - expected)}"
    )
for name, limit in files.items():
    path = root / name
    if path.is_symlink() or not path.is_file():
        raise SystemExit(f"differential evidence artifact {name} is not a regular file")
    size = path.stat().st_size
    if size > limit:
        raise SystemExit(f"differential evidence artifact {name} exceeds its bound")
    if name not in {"kiwi.log"} and size == 0:
        raise SystemExit(f"differential evidence artifact {name} is empty")
if (root / "vector-required-jobs.json").read_bytes() != canonical.read_bytes():
    raise SystemExit("published canonical required-jobs document drifted")
PY
    validate_collection "$canonical" "$evidence_directory/collect.log"
    validate_collect_summary "$canonical" "$evidence_directory/collect-summary.json"
    validate_summary "$canonical" "$evidence_directory/run-summary.json"
    validate_raw_transcript "$canonical" "$evidence_directory/raw-transcript.jsonl"
    validate_final_state "$canonical" "$evidence_directory/final-state.jsonl"
}

validate_registry_artifact() (
    local registry=$1
    local validator=$2
    shift 2
    local scratch
    scratch=$(mktemp -d "${TMPDIR:-/tmp}/kiwi-vector-registry.XXXXXX")
    trap 'rm -rf -- "$scratch"' EXIT
    canonicalize_required_jobs "$registry" "$scratch/required-jobs.json" test
    "$validator" "$scratch/required-jobs.json" "$@"
)

validate_runtime_document() {
    local evidence=$1
    /usr/bin/python3 -I -B - "$evidence" <<'PY'
import json
import pathlib
import re
import sys

document = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
if document.get("build_role") != "rebuild":
    raise SystemExit("Oracle runtime is not the independent rebuild")
if document.get("held_fd") is not True:
    raise SystemExit("Oracle runtime binary is not held by file descriptor")
if document.get("info_redis_versions") != ["8.8.1"]:
    raise SystemExit("Oracle runtime version identity mismatch")
if type(document.get("pid")) is not int or document["pid"] <= 0:
    raise SystemExit("Oracle runtime PID identity is invalid")
if not re.fullmatch(r"[0-9a-f]{64}", document.get("binary_sha256", "")):
    raise SystemExit("Oracle runtime binary hash identity is invalid")
identity = document.get("binary_identity")
required_identity = {"device", "inode", "mode", "size", "nlink"}
if not isinstance(identity, dict) or set(identity) != required_identity:
    raise SystemExit("Oracle runtime binary file identity is invalid")
if any(type(identity[name]) is not int or identity[name] <= 0 for name in required_identity):
    raise SystemExit("Oracle runtime binary file identity is invalid")
PY
}

case ${1:-} in
    --validate-collection)
        [[ $# -eq 3 ]] || die 'usage: --validate-collection REGISTRY LOG'
        validate_registry_artifact "$2" validate_collection "$3"
        exit 0
        ;;
    --validate-summary)
        [[ $# -eq 3 ]] || die 'usage: --validate-summary REGISTRY SUMMARY'
        validate_registry_artifact "$2" validate_summary "$3"
        exit 0
        ;;
    --validate-collect-summary)
        [[ $# -eq 3 ]] || die 'usage: --validate-collect-summary REGISTRY SUMMARY'
        validate_registry_artifact "$2" validate_collect_summary "$3"
        exit 0
        ;;
    --validate-raw-transcript)
        [[ $# -eq 3 ]] || die 'usage: --validate-raw-transcript REGISTRY EVIDENCE'
        validate_registry_artifact "$2" validate_raw_transcript "$3"
        exit 0
        ;;
    --validate-final-state)
        [[ $# -eq 3 ]] || die 'usage: --validate-final-state REGISTRY EVIDENCE'
        validate_registry_artifact "$2" validate_final_state "$3"
        exit 0
        ;;
    --validate-evidence-set)
        [[ $# -eq 3 ]] || die 'usage: --validate-evidence-set REGISTRY DIRECTORY'
        validate_registry_artifact "$2" validate_evidence_set "$3"
        exit 0
        ;;
    --validate-callback-result)
        [[ $# -eq 3 && $2 =~ ^[0-9]+$ && $3 =~ ^[0-9]+$ ]] \
            || die 'usage: --validate-callback-result CALLBACK_STATUS CLEANUP_STATUS'
        finish_callback "$2" "$3"
        exit $?
        ;;
    --validate-runtime-evidence)
        [[ $# -eq 2 ]] || die 'usage: --validate-runtime-evidence EVIDENCE'
        validate_runtime_document "$2"
        exit 0
        ;;
esac

[[ ${OSTYPE:-} == linux* ]] || die 'the trusted Oracle runner requires Linux'

if [[ ${1:-} == --callback ]]; then
    [[ -n ${KIWI_REDIS_ORACLE_HOST:-} \
        && -n ${KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE:-} ]] \
        || die 'callback mode requires verifier-injected Oracle identity'
    export KIWI_COMPAT_REQUIRE_ORACLE=1
else
    [[ ${KIWI_COMPAT_REQUIRE_ORACLE:-} == 1 ]] \
        || die 'KIWI_COMPAT_REQUIRE_ORACLE=1 is required'
fi

registry=$repository_root/tests/compat/redis-8.8.1/vector-required-jobs.yaml
module=tests/python/test_vector_set_differential.py

validate_runtime_evidence() {
    [[ ${KIWI_REDIS_ORACLE_HOST:-} == 127.0.0.1 ]] \
        || die 'Oracle host identity mismatch'
    [[ ${KIWI_REDIS_ORACLE_PORT:-} =~ ^[1-9][0-9]*$ ]] \
        || die 'Oracle port identity is missing'
    [[ ${KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE:-} == /runtime-evidence.json ]] \
        || die 'Oracle runtime evidence path identity mismatch'
    validate_runtime_document "$KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE"
}

cleanup_kiwi() {
    local status=0
    if [[ -n ${kiwi_pid:-} ]] && ! kill -0 "$kiwi_pid" 2>/dev/null; then
        printf 'current-HEAD Kiwi was not alive during cleanup: pid=%s\n' \
            "$kiwi_pid" >&2
        status=1
    elif [[ -n ${kiwi_pid:-} ]]; then
        if ! kill -INT "$kiwi_pid" 2>/dev/null; then
            printf 'failed to interrupt current-HEAD Kiwi: pid=%s\n' \
                "$kiwi_pid" >&2
            status=1
        fi
        for _ in {1..50}; do
            kill -0 "$kiwi_pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$kiwi_pid" 2>/dev/null; then
            if ! kill -TERM "$kiwi_pid" 2>/dev/null; then
                printf 'failed to terminate current-HEAD Kiwi: pid=%s\n' \
                    "$kiwi_pid" >&2
                status=1
            fi
            for _ in {1..30}; do
                kill -0 "$kiwi_pid" 2>/dev/null || break
                sleep 0.1
            done
        fi
        if kill -0 "$kiwi_pid" 2>/dev/null; then
            if ! kill -KILL "$kiwi_pid" 2>/dev/null; then
                printf 'failed to kill current-HEAD Kiwi: pid=%s\n' \
                    "$kiwi_pid" >&2
                status=1
            fi
        fi
    fi
    if [[ -n ${kiwi_pid:-} ]]; then
        wait "$kiwi_pid" 2>/dev/null || true
        kill -0 "$kiwi_pid" 2>/dev/null && status=1
    fi
    return "$status"
}

print_callback_diagnostics() {
    local callback_status=$1
    local cleanup_status=$2
    printf 'trusted Vector differential callback failed: stage=%s callback=%s cleanup=%s\n' \
        "${callback_stage:-unknown}" "$callback_status" "$cleanup_status" >&2
    local file
    for file in kiwi.log collect.log collect-summary.json pytest.log run-summary.json \
        raw-transcript.jsonl final-state.jsonl; do
        [[ -e /work/$file ]] || continue
        printf '%s\n' "--- /work/$file (first 200 lines, at most 32768 bytes) ---" >&2
        sed -n '1,200p' "/work/$file" | head -c 32768 >&2 || true
        printf '\n' >&2
    done
}

callback_exit_cleanup() {
    local callback_status=$?
    local cleanup_status=0 final_status=0
    local failure_stage=${callback_stage:-unknown}
    trap - EXIT
    callback_stage=cleanup
    cleanup_kiwi || cleanup_status=$?
    if [[ $callback_status -eq 0 && $cleanup_status -ne 0 ]]; then
        failure_stage=cleanup
    fi
    if [[ $callback_status -eq 0 && $cleanup_status -eq 0 ]]; then
        rm -rf -- /work/kiwi-data /work/kiwi-log || cleanup_status=$?
        if [[ $cleanup_status -ne 0 ]]; then
            failure_stage=cleanup
        fi
    fi
    if [[ $callback_status -eq 0 && $cleanup_status -eq 0 ]]; then
        callback_stage=cleanup-evidence
        /usr/bin/python3 -I -B - /work /work/callback-cleanup.json <<'PY' \
            || callback_status=$?
import json
import os
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
expected_before_cleanup_evidence = {
    "vector-required-jobs.json",
    "kiwi.conf",
    "kiwi.log",
    "kiwi-runtime.json",
    "collect.log",
    "collect-summary.json",
    "pytest.log",
    "run-summary.json",
    "raw-transcript.jsonl",
    "final-state.jsonl",
}
observed = {entry.name for entry in root.iterdir()}
if observed != expected_before_cleanup_evidence:
    raise SystemExit(
        f"callback work residue drifted before cleanup evidence: "
        f"missing={sorted(expected_before_cleanup_evidence - observed)} "
        f"extra={sorted(observed - expected_before_cleanup_evidence)}"
    )
document = {
    "schema_version": "kiwi-vector-callback-cleanup/v1",
    "kiwi_process_reaped": True,
    "data_directory_removed": not (root / "kiwi-data").exists(),
    "log_directory_removed": not (root / "kiwi-log").exists(),
    "no_unexpected_work_residue": True,
}
payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
try:
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise SystemExit("callback cleanup evidence write made no progress")
        view = view[written:]
    os.fsync(fd)
finally:
    os.close(fd)
PY
        if [[ $callback_status -ne 0 ]]; then
            failure_stage=cleanup-evidence
        fi
    fi
    if [[ $callback_status -eq 0 && $cleanup_status -eq 0 ]]; then
        callback_stage=evidence-set-contract
        validate_evidence_set /work/vector-required-jobs.json /work \
            || callback_status=$?
        if [[ $callback_status -ne 0 ]]; then
            failure_stage=evidence-set-contract
        fi
    fi
    if [[ $callback_status -ne 0 || $cleanup_status -ne 0 ]]; then
        callback_stage=$failure_stage
        print_callback_diagnostics "$callback_status" "$cleanup_status"
    fi
    finish_callback "$callback_status" "$cleanup_status" || final_status=$?
    exit "$final_status"
}

callback_main() {
    callback_stage=preflight
    kiwi_pid=
    trap callback_exit_cleanup EXIT
    validate_runtime_evidence
    [[ ${KIWI_REDIS_ORACLE_CALLBACK_INPUT:-} == /callback-input ]] \
        || die 'Oracle callback input identity mismatch'
    [[ ${KIWI_REDIS_ORACLE_WORKDIR:-} == /work ]] \
        || die 'Oracle callback workdir identity mismatch'
    [[ -x /callback-input/target/debug/kiwi ]] \
        || die 'current-HEAD Kiwi binary is missing from callback input'
    [[ -x /callback-input/target/debug/kiwi-required-vector-jobs ]] \
        || die 'authoritative required-jobs helper is missing from callback input'
    callback_stage=registry-contract
    canonicalize_required_jobs "$registry" /work/vector-required-jobs.json

    local kiwi_port
    kiwi_port=$(/usr/bin/python3 -I -B - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)
    [[ $kiwi_port != "$KIWI_REDIS_ORACLE_PORT" ]] \
        || die 'Kiwi and Oracle endpoints resolved to the same host:port'
    mkdir -p /work/kiwi-data /work/kiwi-log
    cat >/work/kiwi.conf <<EOF
port $kiwi_port
binding 127.0.0.1
data-dir /work/kiwi-data
log-dir /work/kiwi-log
EOF
    /callback-input/target/debug/kiwi --config /work/kiwi.conf \
        >/work/kiwi.log 2>&1 &
    kiwi_pid=$!
    callback_stage=kiwi-readiness
    local binary_identity process_identity ready=0
    binary_identity=$(stat -Lc '%d:%i' /callback-input/target/debug/kiwi)
    sleep 0.1
    if ! kill -0 "$kiwi_pid" 2>/dev/null; then
        local kiwi_exit=0
        wait "$kiwi_pid" || kiwi_exit=$?
        kiwi_pid=
        printf 'current-HEAD Kiwi exited before readiness: exit=%s\n' "$kiwi_exit" >&2
        printf '%s\n' 'callback startup evidence:' >&2
        stat -Lc 'binary=%A uid=%u gid=%g size=%s identity=%d:%i' \
            /callback-input/target/debug/kiwi >&2 || true
        stat -Lc 'work=%A uid=%u gid=%g' /work \
            /work/kiwi-data /work/kiwi-log >&2 || true
        sed -n '1,80p' /work/kiwi.conf >&2 || true
        printf '%s\n' 'current-HEAD Kiwi startup log:' >&2
        sed -n '1,200p' /work/kiwi.log >&2 || true
        die 'current-HEAD Kiwi exited before becoming ready'
    fi
    for _ in {1..120}; do
        kill -0 "$kiwi_pid" 2>/dev/null || break
        if /usr/bin/python3 -I -B - "$kiwi_port" 2>/dev/null <<'PY'
import socket
import sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.2) as sock:
    sock.sendall(b"*1\r\n$4\r\nPING\r\n")
    raise SystemExit(0 if sock.recv(64) == b"+PONG\r\n" else 1)
PY
        then
            ready=1
            break
        fi
        sleep 0.25
    done
    if [[ $ready -ne 1 ]]; then
        printf '%s\n' 'current-HEAD Kiwi startup log:' >&2
        cat /work/kiwi.log >&2 || true
        die 'current-HEAD Kiwi did not become ready'
    fi
    process_identity=$(stat -Lc '%d:%i' "/proc/$kiwi_pid/exe")
    [[ $process_identity == "$binary_identity" ]] \
        || die 'Kiwi runtime executable identity mismatch'
    /usr/bin/python3 -I -B - "$kiwi_pid" \
        /callback-input/target/debug/kiwi /work/kiwi-runtime.json <<'PY'
import hashlib
import json
import os
import pathlib
import sys

pid = int(sys.argv[1])
binary = pathlib.Path(sys.argv[2])
output = pathlib.Path(sys.argv[3])
executable = pathlib.Path(f"/proc/{pid}/exe")

def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()

binary_stat = binary.stat()
executable_stat = executable.stat()
identity_equal = (
    binary_stat.st_dev,
    binary_stat.st_ino,
    binary_stat.st_mode,
    binary_stat.st_size,
) == (
    executable_stat.st_dev,
    executable_stat.st_ino,
    executable_stat.st_mode,
    executable_stat.st_size,
)
binary_sha256 = sha256(binary)
if not identity_equal or sha256(executable) != binary_sha256:
    raise SystemExit("Kiwi executable identity/hash mismatch")
document = {
    "schema_version": "kiwi-runtime-identity/v1",
    "pid": pid,
    "binary_path": str(binary),
    "binary_sha256": binary_sha256,
    "binary_identity": {
        "device": binary_stat.st_dev,
        "inode": binary_stat.st_ino,
        "mode": binary_stat.st_mode,
        "size": binary_stat.st_size,
        "nlink": binary_stat.st_nlink,
    },
    "executable_identity_equal": True,
}
output.write_text(
    json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
with output.open("rb") as published:
    os.fsync(published.fileno())
PY

    export KIWI_HOST=127.0.0.1
    export KIWI_PORT=$kiwi_port
    export VECTOR_REDIS_HOST=$KIWI_REDIS_ORACLE_HOST
    export VECTOR_REDIS_PORT=$KIWI_REDIS_ORACLE_PORT
    export KIWI_TEST_REQUIRE_SERVER=1
    export KIWI_TEST_ISOLATED_SERVER=1
    export PYTHONPATH=/callback-input/.oracle-python
    export PYTHONNOUSERSITE=1
    export PYTHONDONTWRITEBYTECODE=1
    export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
    export KIWI_VECTOR_REQUIRED_JOBS=/work/vector-required-jobs.json
    export KIWI_VECTOR_RAW_TRANSCRIPT=/work/raw-transcript.jsonl
    export KIWI_VECTOR_FINAL_STATE=/work/final-state.jsonl
    : >"$KIWI_VECTOR_RAW_TRANSCRIPT"
    : >"$KIWI_VECTOR_FINAL_STATE"

    local callback_status=0
    cd /callback-input
    callback_stage=collection
    KIWI_VECTOR_PYTEST_SUMMARY=/work/collect-summary.json \
        /usr/bin/python3 -m pytest "$module" --collect-only -q \
        --strict-markers -p no:cacheprovider -p pytest_timeout \
        > /work/collect.log 2>&1 \
        || callback_status=$?
    if [[ $callback_status -eq 0 ]]; then
        callback_stage=collection-contract
        validate_collection /work/vector-required-jobs.json /work/collect.log \
            || callback_status=$?
    fi
    if [[ $callback_status -eq 0 ]]; then
        callback_stage=execution
        KIWI_VECTOR_PYTEST_SUMMARY=/work/run-summary.json \
            /usr/bin/python3 -m pytest "$module" -v -ra --strict-markers \
            --maxfail=1 -p no:cacheprovider -p pytest_timeout \
            2>&1 | tee /work/pytest.log \
            || callback_status=${PIPESTATUS[0]}
    fi
    if [[ $callback_status -eq 0 ]]; then
        callback_stage=raw-transcript-contract
        validate_raw_transcript /work/vector-required-jobs.json "$KIWI_VECTOR_RAW_TRANSCRIPT" \
            || callback_status=$?
    fi
    if [[ $callback_status -eq 0 ]]; then
        callback_stage=final-state-contract
        validate_final_state /work/vector-required-jobs.json "$KIWI_VECTOR_FINAL_STATE" \
            || callback_status=$?
    fi
    if [[ $callback_status -eq 0 ]]; then
        callback_stage=summary-contract
        validate_summary /work/vector-required-jobs.json /work/run-summary.json \
            || callback_status=$?
    fi
    if [[ $callback_status -eq 0 ]]; then
        callback_stage=complete
    fi
    return "$callback_status"
}

if [[ ${1:-} == --callback ]]; then
    callback_main
    exit $?
fi

[[ -z ${KIWI_REDIS_ORACLE_HOST:-} ]] \
    || die 'Oracle endpoint variables are only accepted inside the verifier callback'
[[ -n ${KIWI_REDIS_ORACLE_SOURCE:-} ]] \
    || die 'KIWI_REDIS_ORACLE_SOURCE is required'
[[ -n ${KIWI_REDIS_ORACLE_PRIMARY_METADATA:-} ]] \
    || die 'KIWI_REDIS_ORACLE_PRIMARY_METADATA is required'
[[ -n ${KIWI_REDIS_ORACLE_OUTPUT:-} ]] \
    || die 'KIWI_REDIS_ORACLE_OUTPUT is required'
[[ -n ${KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT:-} ]] \
    || die 'KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT is required'
[[ ${KIWI_EXPECTED_HEAD:-} =~ ^[0-9a-f]{40}$ ]] \
    || die 'KIWI_EXPECTED_HEAD must be a 40-character lowercase hexadecimal OID'
[[ $KIWI_REDIS_ORACLE_SOURCE == /* \
    && $KIWI_REDIS_ORACLE_PRIMARY_METADATA == /* \
    && $KIWI_REDIS_ORACLE_OUTPUT == /* \
    && $KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT == /* ]] \
    || die 'Oracle source, metadata, provenance, and evidence paths must be absolute'
[[ $KIWI_REDIS_ORACLE_OUTPUT != "$KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT" ]] \
    || die 'Oracle provenance and evidence output paths must differ'
[[ ! -e $KIWI_REDIS_ORACLE_OUTPUT ]] || die 'Oracle provenance output already exists'
[[ ! -e $KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT ]] \
    || die 'Oracle differential evidence output already exists'

kiwi_git() {
    if git -C "$repository_root" "$@" 2>/dev/null; then
        return 0
    fi
    if command -v git.exe >/dev/null 2>&1 && command -v wslpath >/dev/null 2>&1; then
        git.exe -C "$(wslpath -w "$repository_root")" "$@" | tr -d '\r'
        return ${PIPESTATUS[0]}
    fi
    return 1
}

actual_head=$(kiwi_git rev-parse 'HEAD^{commit}') \
    || die 'unable to resolve the Kiwi HEAD commit'
[[ $actual_head == "$KIWI_EXPECTED_HEAD" ]] \
    || die "Kiwi HEAD differs from KIWI_EXPECTED_HEAD: expected=$KIWI_EXPECTED_HEAD actual=$actual_head"
head_tree=$(kiwi_git rev-parse 'HEAD^{tree}') \
    || die 'unable to resolve the Kiwi HEAD tree'
[[ $head_tree =~ ^[0-9a-f]{40}$ ]] || die 'Kiwi HEAD tree OID is invalid'
tracked_status=$(kiwi_git status --porcelain --untracked-files=no) \
    || die 'unable to verify the Kiwi checkout identity'
[[ -z $tracked_status ]] || die 'Kiwi checkout has tracked changes and is not current HEAD'

cd "$repository_root"
env -u RUSTC_WRAPPER -u CARGO_BUILD_RUSTC_WRAPPER \
    CARGO_TARGET_DIR="$repository_root/target" \
    cargo build --locked -p kiwi-compat --bin kiwi-required-vector-jobs \
    --bin kiwi-verify-oracle-evidence
env -u RUSTC_WRAPPER -u CARGO_BUILD_RUSTC_WRAPPER \
    CARGO_TARGET_DIR="$repository_root/target" \
    cargo build --locked -p server --bin kiwi
scripts/compat/build-redis-8.8.1.sh \
    --source "$KIWI_REDIS_ORACLE_SOURCE" \
    --metadata "$KIWI_REDIS_ORACLE_PRIMARY_METADATA"
scripts/compat/verify-redis-8.8.1.sh \
    --source "$KIWI_REDIS_ORACLE_SOURCE" \
    --primary-metadata "$KIWI_REDIS_ORACLE_PRIMARY_METADATA" \
    --output "$KIWI_REDIS_ORACLE_OUTPUT" \
    --evidence-output "$KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT" \
    --expected-head "$KIWI_EXPECTED_HEAD" \
    --publication-verifier "$repository_root/target/debug/kiwi-verify-oracle-evidence" \
    --callback-input "$repository_root" \
    --run-after-ready /bin/bash \
    /callback-input/scripts/compat/run-vector-differential.sh --callback
[[ -s $KIWI_REDIS_ORACLE_OUTPUT ]] \
    || die 'verifier did not publish Oracle provenance after cleanup'
[[ -s $KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT ]] \
    || die 'verifier did not publish differential evidence before provenance'
