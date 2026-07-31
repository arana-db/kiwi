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

# Regression coverage for PR #388: Git Bash must preserve MSVC compiler selection.

case "${DEV_SCCACHE_TEST_HELPER:-}:$(basename "$0")" in
    1:cargo)
        printf '__TEST_CC__=%s\n' "${CC-<unset>}"
        printf '__TEST_CXX__=%s\n' "${CXX-<unset>}"
        exit 0
        ;;
    1:nproc)
        printf '2\n'
        exit 0
        ;;
    1:sccache)
        exit 0
        ;;
    1:uname)
        printf '%s\n' "$DEV_SCCACHE_TEST_UNAME"
        exit 0
        ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FAKE_BIN="$(mktemp -d)"
trap 'rm -rf "$FAKE_BIN"' EXIT

for command_name in cargo nproc sccache uname; do
    ln -s "$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")" "$FAKE_BIN/$command_name"
done

run_case() {
    local name=$1
    local uname_value=$2
    local initial_cc=$3
    local initial_cxx=$4
    local expected_cc=$5
    local expected_cxx=$6
    local output actual_cc actual_cxx

    output=$(
        if [[ $initial_cc == '<unset>' ]]; then
            unset CC CXX
        else
            export CC="$initial_cc"
            export CXX="$initial_cxx"
        fi
        export DEV_SCCACHE_TEST_HELPER=1
        export DEV_SCCACHE_TEST_UNAME="$uname_value"
        export PATH="$FAKE_BIN:$PATH"
        cd "$REPO_ROOT"
        sed 's/\r$//' scripts/dev.sh | bash -s -- check
    )

    actual_cc=$(printf '%s\n' "$output" | sed -n 's/^__TEST_CC__=//p')
    actual_cxx=$(printf '%s\n' "$output" | sed -n 's/^__TEST_CXX__=//p')

    if [[ $actual_cc != "$expected_cc" || $actual_cxx != "$expected_cxx" ]]; then
        printf 'FAIL %s: expected CC=%s CXX=%s, got CC=%s CXX=%s\n' \
            "$name" "$expected_cc" "$expected_cxx" "$actual_cc" "$actual_cxx" >&2
        return 1
    fi

    printf 'PASS %s\n' "$name"
}

run_case mingw_does_not_force_gnu_compilers MINGW64_NT-10.0 '<unset>' '<unset>' '<unset>' '<unset>'
run_case msys_does_not_force_gnu_compilers MSYS_NT-10.0 '<unset>' '<unset>' '<unset>' '<unset>'
run_case cygwin_does_not_force_gnu_compilers CYGWIN_NT-10.0 '<unset>' '<unset>' '<unset>' '<unset>'
run_case linux_wraps_default_compilers Linux '<unset>' '<unset>' 'sccache cc' 'sccache c++'
run_case macos_wraps_default_compilers Darwin '<unset>' '<unset>' 'sccache cc' 'sccache c++'
run_case linux_preserves_explicit_compilers Linux clang-18 clang++-18 clang-18 clang++-18
run_case existing_wrappers_are_not_nested Linux 'sccache clang-18' 'sccache clang++-18' 'sccache clang-18' 'sccache clang++-18'
