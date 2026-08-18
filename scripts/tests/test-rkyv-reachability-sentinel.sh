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

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
sentinel="$repo_root/scripts/ci/check-rkyv-reachability.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

fake_bin="$scratch/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >"$FAKE_CARGO_ARGV_FILE"
case "$FAKE_CARGO_SCENARIO" in
  command-failure)
    printf '%s\n' 'fake cargo tree failure' >&2
    exit 42
    ;;
  inverse-dependency)
    printf '%s\n' 'rkyv v0.7.46' '└── rust_decimal v1.36.0' '    └── byte-unit v5.1.6' '        └── openraft v0.9.25'
    ;;
  stderr-warning)
    printf '%s\n' 'warning: nothing to print' >&2
    ;;
  *)
    printf 'unknown fake cargo scenario: %s\n' "$FAKE_CARGO_SCENARIO" >&2
    exit 64
    ;;
esac
EOF
chmod +x "$fake_bin/cargo"

expected_argv='tree --locked --offline --target all --all-features -i rkyv@0.7.46'

run_case() {
  local scenario="$1"
  local expected_status="$2"
  local argv_file="$scratch/$scenario.argv"
  local stdout_file="$scratch/$scenario.stdout"
  local stderr_file="$scratch/$scenario.stderr"

  set +e
  PATH="$fake_bin:$PATH" \
    FAKE_CARGO_SCENARIO="$scenario" \
    FAKE_CARGO_ARGV_FILE="$argv_file" \
    bash "$sentinel" >"$stdout_file" 2>"$stderr_file"
  local status=$?
  set -e

  if [[ "$expected_status" == success && $status -ne 0 ]]; then
    printf 'scenario %s unexpectedly failed with status %s\n' "$scenario" "$status" >&2
    cat "$stderr_file" >&2
    return 1
  fi
  if [[ "$expected_status" == failure && $status -eq 0 ]]; then
    printf 'scenario %s unexpectedly succeeded\n' "$scenario" >&2
    return 1
  fi
  if [[ ! -f "$argv_file" ]]; then
    printf 'scenario %s did not invoke cargo\n' "$scenario" >&2
    cat "$stderr_file" >&2
    return 1
  fi
  if [[ "$(<"$argv_file")" != "$expected_argv" ]]; then
    printf 'scenario %s used unexpected cargo argv: %s\n' "$scenario" "$(<"$argv_file")" >&2
    return 1
  fi
}

run_case command-failure failure
run_case inverse-dependency failure
run_case stderr-warning success

printf '%s\n' 'rkyv reachability sentinel fake-cargo tests passed'
