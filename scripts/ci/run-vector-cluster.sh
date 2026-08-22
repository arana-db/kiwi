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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
test_module="$repo_root/tests/python/test_vector_cluster.py"
grpcurl_version="1.9.3"
grpcurl_binary_sha256="62e2e4315bb70fab2e27f86c1f7738d09076a097a2dc8e0f701e386251172e40"
grpcurl_identity="grpcurl ${grpcurl_version}"
expected_grpcurl_output="grpcurl v${grpcurl_version}"
work_dir=""

die() {
  printf 'vector cluster runner: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  local rc=$?
  local cleanup_rc=0
  trap - EXIT INT TERM
  if [[ -n "$work_dir" && -d "$work_dir" ]]; then
    if [[ -f "$work_dir/pids.json" ]]; then
      python3 - "$work_dir/pids.json" <<'PY' || cleanup_rc=$?
import json
import os
import signal
import sys
import time

with open(sys.argv[1], encoding="utf-8") as source:
    records = json.load(source).get("processes", [])

for record in records:
    pgid = int(record["pgid"])
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        continue

deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    alive = []
    for record in records:
        try:
            os.killpg(int(record["pgid"]), 0)
            alive.append(record)
        except ProcessLookupError:
            pass
    if not alive:
        break
    time.sleep(0.1)
else:
    for record in alive:
        try:
            os.killpg(int(record["pgid"]), signal.SIGKILL)
        except ProcessLookupError:
            pass

deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    remaining = []
    for record in records:
        try:
            os.killpg(int(record["pgid"]), 0)
            remaining.append(record)
        except ProcessLookupError:
            pass
    if not remaining:
        break
    time.sleep(0.1)
else:
    raise SystemExit(f"cluster cleanup left process groups alive: {remaining}")
PY
    fi
    rm -rf -- "$work_dir" || cleanup_rc=$?
  fi
  if (( rc == 0 && cleanup_rc != 0 )); then
    rc=$cleanup_rc
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

[[ "${KIWI_RUN_CLUSTER_TESTS:-}" == "1" ]] || die "KIWI_RUN_CLUSTER_TESTS=1 is required"
[[ -n "${KIWI_BINARY:-}" ]] || die "KIWI_BINARY must identify the current-Head Kiwi binary"
[[ -x "$KIWI_BINARY" ]] || die "KIWI_BINARY is not executable: $KIWI_BINARY"
[[ -n "${KIWI_GRPCURL:-}" ]] || die "KIWI_GRPCURL must identify the pinned grpcurl binary"
[[ -x "$KIWI_GRPCURL" ]] || die "KIWI_GRPCURL is not executable: $KIWI_GRPCURL"

work_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/kiwi-vector-cluster.XXXXXX")"
grpcurl="$work_dir/grpcurl"
cp -- "$KIWI_GRPCURL" "$grpcurl"

printf '%s  %s\n' "$grpcurl_binary_sha256" "$grpcurl" | sha256sum -c -
[[ "$($grpcurl -version 2>&1)" == "$expected_grpcurl_output" ]] || die "$grpcurl_identity identity mismatch"

export KIWI_GRPCURL="$grpcurl"
export KIWI_VECTOR_CLUSTER_PID_REGISTRY="$work_dir/pids.json"
export KIWI_VECTOR_CLUSTER_CLEANUP="$work_dir/cleanup.json"
export KIWI_VECTOR_CLUSTER_PYTEST_SUMMARY="$work_dir/summary.json"

cd "$repo_root"
python3 -m pytest "$test_module" --collect-only -q --strict-markers >"$work_dir/collection.txt"
python3 "$test_module" --validate-collection "$work_dir/collection.txt"

python3 -m pytest "$test_module" -v -ra --strict-markers --maxfail=1
python3 "$test_module" --validate-summary "$work_dir/summary.json"
python3 "$test_module" --validate-cleanup "$work_dir/cleanup.json"

python3 - "$work_dir/pids.json" <<'PY'
import json
import os
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    processes = json.load(source).get("processes", [])
if processes:
    raise SystemExit(f"cluster PID registry is not empty after cleanup: {processes}")
PY

printf 'vector cluster required gate passed: collected=16 passed=16 skipped=0 xfailed=0 xpassed=0 deselected=0\n'
