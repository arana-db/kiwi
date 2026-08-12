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

document = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
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

registry = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
if registry.get("schema") != "kiwi-vector-required-jobs/canonical-v1":
    raise SystemExit("canonical required-jobs schema identity mismatch")
expected = registry.get("expected_item_count")
if type(expected) is not int or expected <= 0:
    raise SystemExit("canonical expected_item_count is invalid")
document = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
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

validate_raw_coverage() {
    local canonical=$1
    local coverage=$2
    /usr/bin/python3 -I -B - "$canonical" "$coverage" <<'PY'
import json
import pathlib
import re
import sys

registry = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
coverage_path = pathlib.Path(sys.argv[2])
if registry.get("schema") != "kiwi-vector-required-jobs/canonical-v1":
    raise SystemExit("canonical required-jobs schema identity mismatch")
commands = registry.get("commands")
raw_cases = registry.get("raw_cases")
if not isinstance(commands, list) or not isinstance(raw_cases, dict):
    raise SystemExit("canonical commands or raw cases are missing")
required_commands = set(commands)
expected = set()
for command, cases in raw_cases.items():
    if not isinstance(command, str) or not isinstance(cases, list):
        raise SystemExit("canonical raw cases are invalid")
    for case in cases:
        if not isinstance(case, dict) or set(case) != {"case_id", "evidence_kind", "node_ids"}:
            raise SystemExit("canonical raw case fields drifted")
        case_id = case["case_id"]
        evidence_kind = case["evidence_kind"]
        node_ids = case["node_ids"]
        if (
            not isinstance(case_id, str)
            or evidence_kind not in {"exact-frame", "raw-schema"}
            or not isinstance(node_ids, list)
        ):
            raise SystemExit("canonical raw case is invalid")
        for node_id in node_ids:
            if not isinstance(node_id, str):
                raise SystemExit("canonical raw case node ID is invalid")
            expected.add((command, case_id, evidence_kind, node_id))
if {command for command, _case_id, _kind, _node_id in expected} != required_commands:
    raise SystemExit("registry raw-case command coverage drifted")
if not coverage_path.is_file():
    raise SystemExit("raw coverage evidence is missing")
observed = set()
for line_number, line in enumerate(coverage_path.read_text(encoding="utf-8").splitlines(), 1):
    try:
        record = json.loads(line)
    except json.JSONDecodeError as error:
        raise SystemExit(f"raw coverage line {line_number} is invalid JSON: {error}")
    required_fields = {
        "case_id", "command", "evidence_kind", "node_id", "protocol",
        "kiwi_frame_sha256", "redis_frame_sha256"
    }
    if set(record) != required_fields:
        raise SystemExit(f"raw coverage line {line_number} fields drifted")
    command = record["command"]
    case_id = record["case_id"]
    evidence_kind = record["evidence_kind"]
    node_id = record["node_id"]
    protocol = record["protocol"]
    expected_protocol = 2 if node_id.endswith("[resp2]") else 3 if node_id.endswith("[resp3]") else None
    if expected_protocol != protocol:
        raise SystemExit(f"raw coverage line {line_number} protocol/node drifted")
    kiwi_hash = record["kiwi_frame_sha256"]
    redis_hash = record["redis_frame_sha256"]
    if not re.fullmatch(r"[0-9a-f]{64}", kiwi_hash) or not re.fullmatch(
        r"[0-9a-f]{64}", redis_hash
    ):
        raise SystemExit(f"raw coverage line {line_number} frame hash drifted")
    if evidence_kind == "exact-frame" and kiwi_hash != redis_hash:
        raise SystemExit(f"raw coverage line {line_number} frame identity drifted")
    key = (command, case_id, evidence_kind, node_id)
    if key in observed:
        raise SystemExit(f"raw coverage line {line_number} duplicates {key!r}")
    observed.add(key)
if observed != expected:
    missing = sorted(expected - observed)
    extra = sorted(observed - expected)
    raise SystemExit(f"raw coverage differs from registry: missing={missing} extra={extra}")
PY
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
    --validate-raw-coverage)
        [[ $# -eq 3 ]] || die 'usage: --validate-raw-coverage REGISTRY EVIDENCE'
        validate_registry_artifact "$2" validate_raw_coverage "$3"
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
    for file in kiwi.log collect.log collect-summary.json pytest.log run-summary.json raw-coverage.jsonl; do
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

    export KIWI_HOST=127.0.0.1
    export KIWI_PORT=$kiwi_port
    export VECTOR_REDIS_HOST=$KIWI_REDIS_ORACLE_HOST
    export VECTOR_REDIS_PORT=$KIWI_REDIS_ORACLE_PORT
    export KIWI_TEST_REQUIRE_SERVER=1
    export KIWI_TEST_ISOLATED_SERVER=1
    export PYTHONPATH=/callback-input/.oracle-python
    export KIWI_VECTOR_RAW_COVERAGE=/work/raw-coverage.jsonl
    : >"$KIWI_VECTOR_RAW_COVERAGE"

    local callback_status=0
    cd /callback-input
    callback_stage=collection
    KIWI_VECTOR_PYTEST_SUMMARY=/work/collect-summary.json \
        /usr/bin/python3 -m pytest "$module" --collect-only -q \
        --strict-markers -p no:cacheprovider > /work/collect.log 2>&1 \
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
            --maxfail=1 -p no:cacheprovider 2>&1 | tee /work/pytest.log \
            || callback_status=${PIPESTATUS[0]}
    fi
    if [[ $callback_status -eq 0 ]]; then
        callback_stage=raw-coverage-contract
        validate_raw_coverage /work/vector-required-jobs.json "$KIWI_VECTOR_RAW_COVERAGE" \
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
[[ $KIWI_REDIS_ORACLE_SOURCE == /* \
    && $KIWI_REDIS_ORACLE_PRIMARY_METADATA == /* \
    && $KIWI_REDIS_ORACLE_OUTPUT == /* ]] \
    || die 'Oracle source, metadata, and output paths must be absolute'
[[ ! -e $KIWI_REDIS_ORACLE_OUTPUT ]] || die 'Oracle provenance output already exists'
if ! tracked_status=$(git -C "$repository_root" status --porcelain --untracked-files=no 2>/dev/null); then
    if command -v git.exe >/dev/null 2>&1 && command -v wslpath >/dev/null 2>&1; then
        tracked_status=$(git.exe -C "$(wslpath -w "$repository_root")" \
            status --porcelain --untracked-files=no | tr -d '\r') \
            || die 'unable to verify the Kiwi checkout identity'
    else
        die 'unable to verify the Kiwi checkout identity'
    fi
fi
[[ -z $tracked_status ]] || die 'Kiwi checkout has tracked changes and is not current HEAD'

cd "$repository_root"
env -u RUSTC_WRAPPER -u CARGO_BUILD_RUSTC_WRAPPER \
    CARGO_TARGET_DIR="$repository_root/target" \
    cargo build --locked -p kiwi-compat --bin kiwi-required-vector-jobs
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
    --callback-input "$repository_root" \
    --run-after-ready /bin/bash \
    /callback-input/scripts/compat/run-vector-differential.sh --callback
[[ -s $KIWI_REDIS_ORACLE_OUTPUT ]] \
    || die 'verifier did not publish Oracle provenance after cleanup'
/usr/bin/python3 -I -B - "$KIWI_REDIS_ORACLE_OUTPUT" <<'PY'
import json
import pathlib
import sys

document = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
if document.get("schema_version") != "kiwi-redis-oracle-provenance/v3":
    raise SystemExit("Oracle provenance schema identity mismatch")
cleanup = document.get("cleanup")
required_cleanup = {
    "redis_process_reaped",
    "process_group_reaped",
    "runtime_removed",
    "checkout_removed",
    "logs_removed",
    "temp_removed",
    "final_identity_revalidated",
    "output_parent_revalidated",
}
if not isinstance(cleanup, dict) or any(cleanup.get(name) is not True for name in required_cleanup):
    raise SystemExit("Oracle provenance was published before complete cleanup")
if document.get("published_after_cleanup") is not True:
    raise SystemExit("Oracle provenance publication order is invalid")
PY
