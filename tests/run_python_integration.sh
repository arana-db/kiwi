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

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEMP_ROOT="${RUNNER_TEMP:-/tmp}"

KIWI_BIN="${KIWI_BIN:-target/debug/kiwi}"
if [[ "${KIWI_BIN}" != /* ]]; then
  KIWI_BIN="${REPO_ROOT}/${KIWI_BIN}"
fi
readonly KIWI_BIN

KIWI_PID=""
TEMP_DIR=""
SERVER_LOG=""

process_is_alive() {
  [[ -n "${KIWI_PID}" ]] && kill -0 "${KIWI_PID}" 2>/dev/null
}

wait_for_exit() {
  local attempts="$1"
  local attempt

  for ((attempt = 0; attempt < attempts; attempt++)); do
    if ! process_is_alive; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM

  if process_is_alive; then
    kill -INT "${KIWI_PID}" 2>/dev/null || true
    if ! wait_for_exit 50; then
      kill -TERM "${KIWI_PID}" 2>/dev/null || true
      if ! wait_for_exit 30; then
        kill -KILL "${KIWI_PID}" 2>/dev/null || true
      fi
    fi
  fi

  if [[ -n "${KIWI_PID}" ]]; then
    wait "${KIWI_PID}" 2>/dev/null || true
  fi

  if ((status != 0)) && [[ -f "${SERVER_LOG}" ]]; then
    echo "Kiwi server log:" >&2
    cat "${SERVER_LOG}" >&2
  fi

  if [[ -n "${TEMP_DIR}" ]]; then
    rm -rf -- "${TEMP_DIR}"
  fi

  exit "${status}"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "${REPO_ROOT}"

if [[ ! -x "${KIWI_BIN}" ]]; then
  echo "Kiwi binary is missing or not executable: ${KIWI_BIN}" >&2
  exit 1
fi

if python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.settimeout(0.2)
    raise SystemExit(0 if sock.connect_ex(("127.0.0.1", 6379)) == 0 else 1)
PY
then
  echo "127.0.0.1:6379 is already in use" >&2
  exit 1
fi

mkdir -p -- "${TEMP_ROOT}"
TEMP_DIR="$(mktemp -d "${TEMP_ROOT%/}/kiwi-python-integration.XXXXXX")"
SERVER_LOG="${TEMP_DIR}/kiwi.log"
readonly CONFIG_PATH="${TEMP_DIR}/kiwi.conf"
readonly DATA_DIR="${TEMP_DIR}/data"
readonly LOG_DIR="${TEMP_DIR}/logs"
mkdir -p -- "${DATA_DIR}" "${LOG_DIR}"

cat >"${CONFIG_PATH}" <<EOF
port 6379
binding 127.0.0.1
data-dir ${DATA_DIR}
log-dir ${LOG_DIR}
EOF

"${KIWI_BIN}" --config "${CONFIG_PATH}" >"${SERVER_LOG}" 2>&1 &
KIWI_PID=$!

ready=0
for _ in {1..120}; do
  if ! process_is_alive; then
    echo "Kiwi exited before becoming ready" >&2
    exit 1
  fi

  if python3 - <<'PY'
import redis

client = redis.Redis(
    host="127.0.0.1",
    port=6379,
    socket_connect_timeout=0.2,
    socket_timeout=0.2,
)
try:
    raise SystemExit(0 if client.ping() else 1)
except redis.RedisError:
    raise SystemExit(1)
finally:
    client.close()
PY
  then
    ready=1
    break
  fi
  sleep 0.25
done

if ((ready == 0)); then
  echo "Kiwi did not become ready on 127.0.0.1:6379" >&2
  exit 1
fi

set +e
KIWI_TEST_REQUIRE_SERVER=1 \
  KIWI_TEST_ISOLATED_SERVER=1 \
  make -C tests test-python
test_status=$?
set -e

if ! process_is_alive; then
  echo "Kiwi exited while Python integration tests were running" >&2
  if ((test_status == 0)); then
    test_status=1
  fi
fi

exit "${test_status}"
