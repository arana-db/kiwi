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

readonly EXPECTED_BASE_REF="688d905fec31b54aec76f36676f55efd8b5cfa17"
readonly EXPECTED_VECTOR_V1_REF="733888fc90ad8ef039947e87b08d7500a405954a"
readonly EXPECTED_RUST_RELEASE="1.97.1"
readonly EXPECTED_RUST_HOST="x86_64-unknown-linux-gnu"
readonly EXPECTED_RUST_TOOLCHAIN="1.97.1-x86_64-unknown-linux-gnu"

usage() {
    cat <<'EOF'
Usage: test-vector-storage-compat.sh \
  --base-ref 688d905fec31b54aec76f36676f55efd8b5cfa17 \
  --vector-v1-ref 733888fc90ad8ef039947e87b08d7500a405954a \
  --head-ref <exact-40-hex-commit>

Builds disposable exact-ref Linux test executables and runs the required
Base/Vector-v1/Head storage migration, rollback, and snapshot matrix.
EOF
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

if (($# == 1)) && [[ $1 == --help || $1 == -h ]]; then
    usage
    exit 0
fi
for argument in "$@"; do
    if [[ $argument == --help || $argument == -h ]]; then
        die "--help must be used as the only argument"
    fi
done

BASE_REF=""
VECTOR_V1_REF=""
HEAD_REF=""
BASE_SEEN=0
VECTOR_SEEN=0
HEAD_SEEN=0

while (($# > 0)); do
    case "$1" in
        --base-ref)
            ((BASE_SEEN == 0)) || die "duplicate --base-ref"
            (($# >= 2)) || die "--base-ref requires a value"
            BASE_REF=$2
            BASE_SEEN=1
            shift 2
            ;;
        --vector-v1-ref)
            ((VECTOR_SEEN == 0)) || die "duplicate --vector-v1-ref"
            (($# >= 2)) || die "--vector-v1-ref requires a value"
            VECTOR_V1_REF=$2
            VECTOR_SEEN=1
            shift 2
            ;;
        --head-ref)
            ((HEAD_SEEN == 0)) || die "duplicate --head-ref"
            (($# >= 2)) || die "--head-ref requires a value"
            HEAD_REF=$2
            HEAD_SEEN=1
            shift 2
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

((BASE_SEEN == 1)) || die "missing --base-ref"
((VECTOR_SEEN == 1)) || die "missing --vector-v1-ref"
((HEAD_SEEN == 1)) || die "missing --head-ref"

for ref_name in BASE_REF VECTOR_V1_REF HEAD_REF; do
    ref_value=${!ref_name}
    [[ $ref_value =~ ^[0-9a-fA-F]{40}$ ]] || die "$ref_name must be an exact 40-hex commit"
done

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd -P)
FIXTURE_TOOL="$REPO_ROOT/tests/compat/vector_storage_fixture.py"
[[ -f $FIXTURE_TOOL ]] || die "fixture tool is missing: $FIXTURE_TOOL"

for tool in awk bash basename c++ cat cc chmod cmp cp date dirname find git grep mkdir mktemp \
    python3 readlink realpath rm sed sha256sum stat tail tr wc; do
    command -v "$tool" >/dev/null 2>&1 || die "required Linux tool is missing: $tool"
done

if [[ -d $REPO_ROOT/.git ]]; then
    CONTROLLER_GIT_DIR=$(realpath -- "$REPO_ROOT/.git")
    COMMON_GIT_DIR=$CONTROLLER_GIT_DIR
elif [[ -f $REPO_ROOT/.git ]]; then
    GITDIR_RECORD=$(sed -n 's/^gitdir:[[:space:]]*//p' "$REPO_ROOT/.git")
    [[ -n $GITDIR_RECORD ]] || die "cannot parse linked-worktree .git file"
    if [[ $GITDIR_RECORD =~ ^[A-Za-z]:[/\\] ]]; then
        command -v wslpath >/dev/null 2>&1 || die "wslpath is required for a Windows linked worktree"
        GITDIR_RECORD=$(wslpath -u "$GITDIR_RECORD")
    elif [[ $GITDIR_RECORD != /* ]]; then
        GITDIR_RECORD="$REPO_ROOT/$GITDIR_RECORD"
    fi
    GITDIR_RECORD=$(realpath -- "$GITDIR_RECORD")
    CONTROLLER_GIT_DIR=$GITDIR_RECORD
    case "$GITDIR_RECORD" in
        */.git/worktrees/*) COMMON_GIT_DIR=${GITDIR_RECORD%/worktrees/*} ;;
        *) die "linked-worktree gitdir is outside the expected common Git layout: $GITDIR_RECORD" ;;
    esac
else
    die "repository .git metadata is missing"
fi
[[ -d $COMMON_GIT_DIR ]] || die "common Git directory is missing: $COMMON_GIT_DIR"
[[ -d $CONTROLLER_GIT_DIR ]] || die "controller Git directory is missing: $CONTROLLER_GIT_DIR"

path_is_within() {
    local candidate=$1
    local authority=$2
    [[ $candidate == "$authority" || $candidate == "$authority"/* ]]
}

declare -a REGISTERED_WORKTREE_ROOTS=()
while IFS= read -r record; do
    [[ $record == worktree\ * ]] || continue
    worktree_root=${record#worktree }
    if [[ $worktree_root =~ ^[A-Za-z]:[/\\] ]]; then
        command -v wslpath >/dev/null 2>&1 || die "wslpath is required for Windows worktree paths"
        worktree_root=$(wslpath -u "$worktree_root")
    fi
    worktree_root=$(realpath -- "$worktree_root")
    REGISTERED_WORKTREE_ROOTS+=("$worktree_root")
done < <(git --git-dir="$COMMON_GIT_DIR" worktree list --porcelain)
((${#REGISTERED_WORKTREE_ROOTS[@]} > 0)) || die "Git reported zero registered worktrees"

resolve_exact_commit() {
    git --git-dir="$COMMON_GIT_DIR" rev-parse --verify --end-of-options "$1^{commit}"
}

BASE_SHA=$(resolve_exact_commit "$BASE_REF") || die "cannot resolve Base commit: $BASE_REF"
VECTOR_V1_SHA=$(resolve_exact_commit "$VECTOR_V1_REF") || die "cannot resolve Vector-v1 commit: $VECTOR_V1_REF"
HEAD_SHA=$(resolve_exact_commit "$HEAD_REF") || die "cannot resolve Head commit: $HEAD_REF"
[[ $BASE_SHA == "$EXPECTED_BASE_REF" ]] || die "Base resolves to $BASE_SHA, expected $EXPECTED_BASE_REF"
[[ $VECTOR_V1_SHA == "$EXPECTED_VECTOR_V1_REF" ]] || die "Vector-v1 resolves to $VECTOR_V1_SHA, expected $EXPECTED_VECTOR_V1_REF"
[[ $HEAD_SHA == "${HEAD_REF,,}" ]] || die "Head input must name the exact resolved commit: input=$HEAD_REF resolved=$HEAD_SHA"

RUNNER_REAL=$(realpath -- "${BASH_SOURCE[0]}")
[[ $RUNNER_REAL == "$REPO_ROOT/scripts/test-vector-storage-compat.sh" ]] || \
    die "runner must execute from its controller worktree path: $RUNNER_REAL"
CONTROLLER_SHA=$(git --git-dir="$CONTROLLER_GIT_DIR" --work-tree="$REPO_ROOT" rev-parse HEAD) || \
    die "cannot resolve controller worktree Head"
[[ $CONTROLLER_SHA == "$HEAD_SHA" ]] || \
    die "controller worktree Head differs from --head-ref: controller=$CONTROLLER_SHA head=$HEAD_SHA"

declare -A CONTROLLER_FILE_SHA256=()
verify_controller_file() {
    local relative=$1
    local worktree_file="$REPO_ROOT/$relative"
    local head_record head_mode head_type head_oid head_path
    local index_record index_mode index_oid index_stage index_path
    local head_sha256 worktree_sha256
    [[ -f $worktree_file && ! -L $worktree_file ]] || \
        die "controller file must be a regular non-symlink: $relative"
    head_record=$(git --git-dir="$COMMON_GIT_DIR" ls-tree "$HEAD_SHA" -- "$relative") || \
        die "cannot inspect controller Head tree: $relative"
    [[ $(printf '%s\n' "$head_record" | wc -l) -eq 1 && -n $head_record ]] || \
        die "controller Head must contain exactly one entry: $relative"
    read -r head_mode head_type head_oid head_path <<<"$head_record"
    [[ $head_type == blob && $head_path == "$relative" ]] || \
        die "controller Head entry is not the expected blob: $relative"
    index_record=$(git --git-dir="$CONTROLLER_GIT_DIR" --work-tree="$REPO_ROOT" \
        ls-files --stage -- "$relative") || die "cannot inspect controller index: $relative"
    [[ $(printf '%s\n' "$index_record" | wc -l) -eq 1 && -n $index_record ]] || \
        die "controller index must contain exactly one entry: $relative"
    read -r index_mode index_oid index_stage index_path <<<"$index_record"
    [[ $index_stage == 0 && $index_path == "$relative" ]] || \
        die "controller index entry is not stage zero: $relative"
    [[ $index_mode == "$head_mode" ]] || \
        die "controller index mode drift from Head: $relative index=$index_mode head=$head_mode"
    [[ $index_oid == "$head_oid" ]] || \
        die "controller index drift from Head: $relative index=$index_oid head=$head_oid"
    head_sha256=$(git --git-dir="$COMMON_GIT_DIR" cat-file blob "$head_oid" | sha256sum | awk '{print $1}') || \
        die "cannot hash Head blob: $relative"
    worktree_sha256=$(sha256sum -- "$worktree_file" | awk '{print $1}') || \
        die "cannot hash controller worktree file: $relative"
    [[ $worktree_sha256 == "$head_sha256" ]] || \
        die "controller worktree drift from Head: $relative worktree=$worktree_sha256 head=$head_sha256"
    CONTROLLER_FILE_SHA256["$relative"]=$worktree_sha256
}

verify_controller_file scripts/test-vector-storage-compat.sh
verify_controller_file scripts/test-vector-storage-compat.ps1
verify_controller_file tests/compat/vector_storage_fixture.py
printf 'CONTROLLER commit=%s shell_sha256=%s powershell_sha256=%s fixture_sha256=%s\n' \
    "$CONTROLLER_SHA" \
    "${CONTROLLER_FILE_SHA256[scripts/test-vector-storage-compat.sh]}" \
    "${CONTROLLER_FILE_SHA256[scripts/test-vector-storage-compat.ps1]}" \
    "${CONTROLLER_FILE_SHA256[tests/compat/vector_storage_fixture.py]}"

SOURCE_CARGO_HOME_CANDIDATE=${CARGO_HOME:-$HOME/.cargo}
[[ -d $SOURCE_CARGO_HOME_CANDIDATE ]] || \
    die "source Cargo cache directory is missing: $SOURCE_CARGO_HOME_CANDIDATE"
SOURCE_CARGO_HOME=$(realpath -- "$SOURCE_CARGO_HOME_CANDIDATE")
RUSTUP_HOME_CANDIDATE=${RUSTUP_HOME:-"$(dirname -- "$SOURCE_CARGO_HOME")/.rustup"}
RUSTUP_HOME_REAL=$(realpath -- "$RUSTUP_HOME_CANDIDATE") || \
    die "Rustup home is missing: $RUSTUP_HOME_CANDIDATE"
RUST_TOOLCHAIN_ROOT=$(realpath -- "$RUSTUP_HOME_REAL/toolchains/$EXPECTED_RUST_TOOLCHAIN") || \
    die "expected Rust toolchain is missing: $EXPECTED_RUST_TOOLCHAIN"
RUSTC_BIN=$(realpath -- "$RUST_TOOLCHAIN_ROOT/bin/rustc") || die "pinned rustc is missing"
CARGO_BIN=$(realpath -- "$RUST_TOOLCHAIN_ROOT/bin/cargo") || die "pinned cargo is missing"
[[ $RUSTC_BIN == "$RUST_TOOLCHAIN_ROOT/bin/rustc" && -f $RUSTC_BIN && -x $RUSTC_BIN ]] || \
    die "pinned rustc is outside the expected toolchain: $RUSTC_BIN"
[[ $CARGO_BIN == "$RUST_TOOLCHAIN_ROOT/bin/cargo" && -f $CARGO_BIN && -x $CARGO_BIN ]] || \
    die "pinned cargo is outside the expected toolchain: $CARGO_BIN"
RUSTC_VERSION=$("$RUSTC_BIN" -vV) || die "pinned rustc did not report its identity"
CARGO_VERSION=$("$CARGO_BIN" -vV) || die "pinned cargo did not report its identity"
RUSTC_SYSROOT=$("$RUSTC_BIN" --print sysroot) || die "pinned rustc did not report its sysroot"
RUSTC_SYSROOT=$(realpath -- "$RUSTC_SYSROOT")
[[ $RUSTC_SYSROOT == "$RUST_TOOLCHAIN_ROOT" ]] || \
    die "pinned rustc sysroot differs from toolchain root: sysroot=$RUSTC_SYSROOT root=$RUST_TOOLCHAIN_ROOT"
RUSTC_RELEASE=$(awk '/^release: / {print $2}' <<<"$RUSTC_VERSION")
CARGO_RELEASE=$(awk '/^release: / {print $2}' <<<"$CARGO_VERSION")
RUSTC_HOST=$(awk '/^host: / {print $2}' <<<"$RUSTC_VERSION")
CARGO_HOST=$(awk '/^host: / {print $2}' <<<"$CARGO_VERSION")
RUSTC_COMMIT=$(awk '/^commit-hash: / {print $2}' <<<"$RUSTC_VERSION")
CARGO_COMMIT=$(awk '/^commit-hash: / {print $2}' <<<"$CARGO_VERSION")
[[ $RUSTC_RELEASE == "$EXPECTED_RUST_RELEASE" && $CARGO_RELEASE == "$EXPECTED_RUST_RELEASE" ]] || \
    die "Rust release mismatch: rustc=$RUSTC_RELEASE cargo=$CARGO_RELEASE expected=$EXPECTED_RUST_RELEASE"
[[ $RUSTC_HOST == "$EXPECTED_RUST_HOST" && $CARGO_HOST == "$EXPECTED_RUST_HOST" ]] || \
    die "Rust host mismatch: rustc=$RUSTC_HOST cargo=$CARGO_HOST expected=$EXPECTED_RUST_HOST"
[[ $RUSTC_COMMIT =~ ^[0-9a-f]{40}$ && $CARGO_COMMIT =~ ^[0-9a-f]{40}$ ]] || \
    die "Rust toolchain commit identity is incomplete"
RUSTC_SHA256=$(sha256sum -- "$RUSTC_BIN" | awk '{print $1}')
CARGO_SHA256=$(sha256sum -- "$CARGO_BIN" | awk '{print $1}')
printf 'RUST_TOOLCHAIN root=%s release=%s host=%s rustc=%s rustc_sha256=%s cargo=%s cargo_sha256=%s\n' \
    "$RUST_TOOLCHAIN_ROOT" "$EXPECTED_RUST_RELEASE" "$EXPECTED_RUST_HOST" \
    "$RUSTC_BIN" "$RUSTC_SHA256" "$CARGO_BIN" "$CARGO_SHA256"
printf 'RUSTC_VERSION_BEGIN\n%s\nRUSTC_VERSION_END\nCARGO_VERSION_BEGIN\n%s\nCARGO_VERSION_END\n' \
    "$RUSTC_VERSION" "$CARGO_VERSION"
CC_BIN=$(realpath -- "$(command -v cc)")
CXX_BIN=$(realpath -- "$(command -v c++)")
export RUSTC_WRAPPER=""
export RUSTC_WORKSPACE_WRAPPER=""
export CARGO_BUILD_RUSTC_WRAPPER=""
export CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER=""
while IFS= read -r sccache_name; do
    [[ -n $sccache_name ]] || continue
    unset "$sccache_name"
done < <(compgen -A variable SCCACHE_ || true)
unset CMAKE_C_COMPILER_LAUNCHER CMAKE_CXX_COMPILER_LAUNCHER
unset LD_LIBRARY_PATH
export CC="$CC_BIN"
export CXX="$CXX_BIN"

TMP_CANDIDATE=${TMPDIR:-/tmp}
[[ -d $TMP_CANDIDATE ]] || die "TMPDIR is not an existing directory: $TMP_CANDIDATE"
TMP_BASE=$(realpath -- "$TMP_CANDIDATE")
for worktree_root in "${REGISTERED_WORKTREE_ROOTS[@]}"; do
    path_is_within "$TMP_BASE" "$worktree_root" && \
        die "TMPDIR must be outside every Git worktree: tmp=$TMP_BASE worktree=$worktree_root"
done
path_is_within "$TMP_BASE" "$REPO_ROOT" && die "TMPDIR must be outside REPO_ROOT: $TMP_BASE"
TEMP_ROOT=$(mktemp -d "$TMP_BASE/kiwi-vector-storage-compat.XXXXXX")
TEMP_ROOT=$(realpath -- "$TEMP_ROOT")
[[ $(dirname -- "$TEMP_ROOT") == "$TMP_BASE" ]] || die "temporary root escaped TMPDIR: $TEMP_ROOT"
[[ $(basename -- "$TEMP_ROOT") == kiwi-vector-storage-compat.* ]] || die "unexpected temporary root basename: $TEMP_ROOT"
for worktree_root in "${REGISTERED_WORKTREE_ROOTS[@]}"; do
    path_is_within "$TEMP_ROOT" "$worktree_root" && \
        die "temporary root is inside a Git worktree: temp=$TEMP_ROOT worktree=$worktree_root"
done

declare -a WORKTREES=()
declare -a BACKGROUND_PIDS=()
declare -a STARTED_BACKGROUND_PIDS=()
declare -a EXECUTED_GATES=()
declare -A GATE_SET=()
ACTIVE_GATE=""
ACTIVE_GATE_STARTED=0
PATH_AUTHORITY_MARKER=""

assert_under_temp() {
    local path resolved
    path=$1
    resolved=$(realpath -m -- "$path")
    case "$resolved" in
        "$TEMP_ROOT"/*) ;;
        *) die "refusing destructive cleanup outside runner temp root: $resolved" ;;
    esac
}

safe_remove() {
    local path=$1
    assert_under_temp "$path"
    rm -rf -- "$path"
}

proc_link_target_under_temp() {
    local link=$1
    local raw_target resolved_target
    raw_target=$(readlink -- "$link" 2>/dev/null) || return 1
    raw_target=${raw_target% (deleted)}
    [[ $raw_target == /* ]] || return 1
    resolved_target=$(realpath -m -- "$raw_target") || return 1
    path_is_within "$resolved_target" "$TEMP_ROOT" || return 1
    printf '%s\n' "$resolved_target"
}

scan_temp_process_references() {
    local proc_dir pid link_name resolved_target fd
    local -a fd_entries=()
    for proc_dir in /proc/[0-9]*; do
        [[ -d $proc_dir ]] || continue
        pid=${proc_dir##*/}
        [[ $pid =~ ^[0-9]+$ && $pid != $$ ]] || continue
        for link_name in cwd root exe; do
            if resolved_target=$(proc_link_target_under_temp "$proc_dir/$link_name"); then
                printf '%s:%s:%s\n' "$pid" "$link_name" "$resolved_target"
            fi
        done
        fd_entries=("$proc_dir"/fd/*)
        for fd in "${fd_entries[@]}"; do
            [[ -e $fd || -L $fd ]] || continue
            if resolved_target=$(proc_link_target_under_temp "$fd"); then
                printf '%s:fd-%s:%s\n' "$pid" "${fd##*/}" "$resolved_target"
            fi
        done
    done
}

verify_process_reference_scanner() {
    local probe_root="$TEMP_ROOT/process-scan-probe"
    local cwd_root="$probe_root/cwd"
    local fd_file="$probe_root/fd-authority"
    local cwd_pid fd_pid attempt process_ref cmdline
    local cwd_seen=0
    local fd_seen=0
    assert_under_temp "$probe_root"
    mkdir -p -- "$cwd_root"
    printf 'fd authority\n' >"$fd_file"
    (
        cd -- "$cwd_root"
        exec tail -f /dev/null
    ) &
    cwd_pid=$!
    BACKGROUND_PIDS+=("$cwd_pid")
    STARTED_BACKGROUND_PIDS+=("$cwd_pid")
    (
        cd -- /
        exec 3<"$fd_file"
        exec tail -f /dev/null
    ) &
    fd_pid=$!
    BACKGROUND_PIDS+=("$fd_pid")
    STARTED_BACKGROUND_PIDS+=("$fd_pid")

    for attempt in {1..1000}; do
        cwd_seen=0
        fd_seen=0
        while IFS= read -r process_ref; do
            case "$process_ref" in
                "$cwd_pid:cwd:$cwd_root") cwd_seen=1 ;;
                "$fd_pid:fd-3:$fd_file") fd_seen=1 ;;
            esac
        done < <(scan_temp_process_references)
        ((cwd_seen == 1 && fd_seen == 1)) && break
        kill -0 "$cwd_pid" 2>/dev/null || die "cwd process exited before scanner observed it"
        kill -0 "$fd_pid" 2>/dev/null || die "fd process exited before scanner observed it"
    done
    ((cwd_seen == 1)) || die "process scanner missed TEMP_ROOT cwd authority"
    ((fd_seen == 1)) || die "process scanner missed TEMP_ROOT fd authority"
    for pid in "$cwd_pid" "$fd_pid"; do
        cmdline=$(tr '\0' ' ' <"/proc/$pid/cmdline")
        [[ $cmdline != *"$TEMP_ROOT"* ]] || die "process scanner mutation leaked temp path into argv: $pid"
        kill "$pid"
        wait "$pid" 2>/dev/null || true
    done
    BACKGROUND_PIDS=()
    while IFS= read -r process_ref; do
        [[ -n $process_ref ]] || continue
        die "process reference remained after owned probe cleanup: $process_ref"
    done < <(scan_temp_process_references)
    safe_remove "$probe_root"
    printf 'PROCESS_SCAN PASS cwd_refs=1 fd_refs=1 argv_temp_refs=0 tracked_processes=%d\n' \
        "${#STARTED_BACKGROUND_PIDS[@]}"
}

cleanup() {
    local status=$?
    local cleanup_failed=0
    local cleanup_attempt
    local -a lingering_temp_refs=()
    trap - EXIT INT TERM
    set +e
    if [[ -n $ACTIVE_GATE ]]; then
        printf 'GATE %s FAIL duration=%ss\n' "$ACTIVE_GATE" "$(( $(date +%s) - ACTIVE_GATE_STARTED ))" >&2
    fi
    for pid in "${BACKGROUND_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            for cleanup_attempt in {1..1000}; do
                kill -0 "$pid" 2>/dev/null || break
            done
            if kill -0 "$pid" 2>/dev/null; then
                kill -KILL "$pid" 2>/dev/null
            fi
            wait "$pid" 2>/dev/null
        fi
        if kill -0 "$pid" 2>/dev/null; then
            printf 'CLEANUP FAIL runner_background_pid_alive=%s\n' "$pid" >&2
            cleanup_failed=1
        fi
    done
    while IFS= read -r process_ref; do
        [[ -n $process_ref ]] || continue
        lingering_temp_refs+=("$process_ref")
    done < <(scan_temp_process_references)
    if ((${#lingering_temp_refs[@]} > 0)); then
        printf 'CLEANUP FAIL temp_root_process_refs=%s\n' "${lingering_temp_refs[*]}" >&2
        cleanup_failed=1
    fi
    if [[ -n $PATH_AUTHORITY_MARKER && -s $PATH_AUTHORITY_MARKER ]]; then
        printf 'CLEANUP FAIL ambient_rust_tool_invoked=%s\n' "$(tr '\n' ',' <"$PATH_AUTHORITY_MARKER")" >&2
        cleanup_failed=1
    fi
    for ((index=${#WORKTREES[@]} - 1; index >= 0; index--)); do
        worktree=${WORKTREES[$index]}
        assert_under_temp "$worktree" || cleanup_failed=1
        if git --git-dir="$COMMON_GIT_DIR" worktree list --porcelain | grep -Fqx "worktree $worktree"; then
            git --git-dir="$COMMON_GIT_DIR" worktree remove --force "$worktree" || cleanup_failed=1
        fi
        if git --git-dir="$COMMON_GIT_DIR" worktree list --porcelain | grep -Fqx "worktree $worktree"; then
            printf 'CLEANUP FAIL registered_worktree=%s\n' "$worktree" >&2
            cleanup_failed=1
        fi
    done
    if [[ -d $TEMP_ROOT ]]; then
        if [[ $(dirname -- "$TEMP_ROOT") != "$TMP_BASE" || $(basename -- "$TEMP_ROOT") != kiwi-vector-storage-compat.* ]]; then
            printf 'CLEANUP FAIL unsafe_temp_root=%s\n' "$TEMP_ROOT" >&2
            cleanup_failed=1
        else
            rm -rf -- "$TEMP_ROOT" || cleanup_failed=1
        fi
    fi
    if [[ -e $TEMP_ROOT ]]; then
        printf 'CLEANUP FAIL temp_root_remains=%s\n' "$TEMP_ROOT" >&2
        cleanup_failed=1
    fi
    if ((cleanup_failed != 0)); then
        status=1
    else
        printf 'CLEANUP PASS temp_root_removed=true worktrees_removed=%d tracked_processes=%d lingering_temp_refs=0\n' \
            "${#WORKTREES[@]}" "${#STARTED_BACKGROUND_PIDS[@]}"
    fi
    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

verify_process_reference_scanner

begin_gate() {
    local gate=$1
    [[ -z ${GATE_SET[$gate]+x} ]] || die "gate executed more than once: $gate"
    ACTIVE_GATE=$gate
    ACTIVE_GATE_STARTED=$(date +%s)
    printf 'GATE %s START\n' "$gate"
}

pass_gate() {
    local gate=$1
    [[ $ACTIVE_GATE == "$gate" ]] || die "gate state mismatch: active=$ACTIVE_GATE pass=$gate"
    GATE_SET[$gate]=1
    EXECUTED_GATES+=("$gate")
    printf 'GATE %s PASS duration=%ss\n' "$gate" "$(( $(date +%s) - ACTIVE_GATE_STARTED ))"
    ACTIVE_GATE=""
    ACTIVE_GATE_STARTED=0
}

add_worktree() {
    local path=$1
    local sha=$2
    assert_under_temp "$path"
    git --git-dir="$COMMON_GIT_DIR" worktree add --detach "$path" "$sha"
    WORKTREES+=("$path")
    local actual
    actual=$(git -C "$path" rev-parse HEAD)
    [[ $actual == "$sha" ]] || die "worktree Head mismatch: path=$path expected=$sha actual=$actual"
    verify_cargo_config_chain "$path"
}

verify_cargo_config_file() {
    local config=$1
    [[ -f $config && ! -L $config ]] || die "Cargo config must be a regular non-symlink: $config"
    python3 "$FIXTURE_TOOL" verify-cargo-config --input "$config" || \
        die "Cargo config authority rejected: $config"
}

verify_cargo_config_chain() {
    local directory config
    directory=$(realpath -- "$1")
    while :; do
        for config in "$directory/.cargo/config" "$directory/.cargo/config.toml"; do
            if [[ -e $config || -L $config ]]; then
                verify_cargo_config_file "$config"
            fi
        done
        [[ $directory == / ]] && break
        directory=$(dirname -- "$directory")
    done
    for config in "$CARGO_HOME/config" "$CARGO_HOME/config.toml"; do
        if [[ -e $config || -L $config ]]; then
            verify_cargo_config_file "$config"
        fi
    done
}

verify_cargo_config_guard() {
    local mutation_root="$TEMP_ROOT/cargo-config-guard"
    local child="$mutation_root/project"
    local marker="$mutation_root/wrapper-used"
    local log="$mutation_root/rejection.log"
    assert_under_temp "$mutation_root"
    mkdir -p -- "$mutation_root/.cargo" "$child"
    cat >"$mutation_root/wrapper" <<EOF
#!/usr/bin/env bash
printf 'wrapper-used\\n' >"$marker"
exit 97
EOF
    chmod 700 "$mutation_root/wrapper"
    cat >"$mutation_root/.cargo/config.toml" <<EOF
build.rustc-workspace-wrapper = "$mutation_root/wrapper"
EOF
    if (verify_cargo_config_chain "$child") >"$log" 2>&1; then
        die "ancestor dotted Cargo wrapper mutation survived config authority"
    fi
    grep -F "Cargo config declares a compiler wrapper" "$log" >/dev/null || {
        cat -- "$log" >&2
        die "ancestor dotted Cargo wrapper failed for the wrong reason"
    }
    [[ ! -e $marker ]] || die "ancestor dotted Cargo wrapper executed before rejection"
    safe_remove "$mutation_root"
    printf 'CARGO_CONFIG_GUARD PASS ancestor_dotted_wrapper=rejected_before_execution\n'
}

setup_path_authority_guard() {
    local shim_root="$TEMP_ROOT/path-authority-guard"
    assert_under_temp "$shim_root"
    mkdir -p -- "$shim_root"
    PATH_AUTHORITY_MARKER="$shim_root/ambient-tool-used"
    cat >"$shim_root/cargo" <<EOF
#!/usr/bin/env bash
printf 'PATH cargo\\n' >>"$PATH_AUTHORITY_MARKER"
exit 97
EOF
    cat >"$shim_root/rustc" <<EOF
#!/usr/bin/env bash
printf 'PATH rustc\\n' >>"$PATH_AUTHORITY_MARKER"
exit 97
EOF
    chmod 700 "$shim_root/cargo" "$shim_root/rustc"
    cargo() {
        printf 'function cargo\n' >>"$PATH_AUTHORITY_MARKER"
        return 97
    }
    rustc() {
        printf 'function rustc\n' >>"$PATH_AUTHORITY_MARKER"
        return 97
    }
    export -f cargo rustc
    export PATH="$shim_root:$PATH"
}

verify_path_authority_guard() {
    [[ -n $PATH_AUTHORITY_MARKER && ! -e $PATH_AUTHORITY_MARKER ]] || \
        die "ambient cargo/rustc PATH or function shim was invoked"
    printf 'RUST_PATH_GUARD PASS ambient_path_shims=unused ambient_functions=unused\n'
}

emit_driver() {
    local kind=$1
    local destination=$2
    assert_under_temp "$destination"
    mkdir -p -- "$(dirname -- "$destination")"
    python3 "$FIXTURE_TOOL" emit-rust --kind "$kind" >"$destination"
    [[ -s $destination ]] || die "generated driver is empty: $destination"
}

build_test_group() {
    local group_label=$1
    local worktree=$2
    local target_dir=$3
    local features=$4
    shift 4
    local json_log="$TEMP_ROOT/build-logs/$group_label.json"
    mkdir -p -- "$(dirname -- "$json_log")" "$target_dir"
    local -a command=("$CARGO_BIN" test --locked)
    local spec package test_target
    for spec in "$@"; do
        package=${spec%%:*}
        test_target=${spec#*:}
        [[ -n $package && -n $test_target && $package != "$test_target" ]] || \
            die "invalid build group target: $spec"
        command+=(-p "$package" --test "$test_target")
    done
    command+=(--no-run --message-format=json-render-diagnostics)
    if [[ -n $features ]]; then
        command+=(--features "$features")
    fi
    if ! (
        cd -- "$worktree"
        CARGO_TARGET_DIR="$target_dir" "${command[@]}"
    ) >"$json_log"; then
        python3 "$FIXTURE_TOOL" render-cargo-diagnostics --input "$json_log"
        return 1
    fi
    printf '%s\n' "$json_log"
}

copy_group_executable() {
    local label=$1
    local worktree=$2
    local artifact_dir=$3
    local json_log=$4
    local test_target=$5
    mkdir -p -- "$artifact_dir"
    local cargo_executable executable source_head cargo_sha copied_sha
    cargo_executable=$(python3 "$FIXTURE_TOOL" extract-executable --input "$json_log" --target "$test_target")
    [[ -x $cargo_executable ]] || die "built test executable is not executable: $cargo_executable"
    source_head=$(git -C "$worktree" rev-parse HEAD)
    cargo_sha=$(sha256sum -- "$cargo_executable" | awk '{print $1}')
    executable="$artifact_dir/$label-$(basename -- "$cargo_executable")"
    cp --preserve=mode,timestamps -- "$cargo_executable" "$executable"
    [[ -x $executable ]] || die "copied per-ref test executable is not executable: $executable"
    copied_sha=$(sha256sum -- "$executable" | awk '{print $1}')
    [[ $copied_sha == "$cargo_sha" ]] || die "copied executable hash mismatch for $label"
    printf 'BUILD %s PASS source_head=%s cargo_sha256=%s copied_sha256=%s executable=%s cargo_artifact=%s\n' \
        "$label" "$source_head" "$cargo_sha" "$copied_sha" "$executable" "$cargo_executable" >&2
    printf '%s\n' "$executable"
}

run_exact_test() {
    local executable=$1
    local test_name=$2
    shift 2
    env RUST_BACKTRACE=1 "$@" "$executable" --exact "$test_name" --nocapture --test-threads=1
}

expect_exact_test_failure() {
    local label=$1
    local expected=$2
    local executable=$3
    local test_name=$4
    local log_file="$TEMP_ROOT/mutation-logs/$label.log"
    shift 4
    assert_under_temp "$log_file"
    mkdir -p -- "$(dirname -- "$log_file")"
    if run_exact_test "$executable" "$test_name" "$@" >"$log_file" 2>&1; then
        cat -- "$log_file" >&2
        die "mutation unexpectedly survived: $label"
    fi
    if ! grep -F "$expected" "$log_file" >/dev/null; then
        cat -- "$log_file" >&2
        die "mutation failed for the wrong reason: label=$label expected=$expected"
    fi
    printf 'MUTATION %s PASS expected_failure=%s\n' "$label" "$expected"
}

copy_fixture() {
    local source=$1
    local target=$2
    local target_parent
    target_parent=$(dirname -- "$target")
    assert_under_temp "$source"
    assert_under_temp "$target"
    assert_under_temp "$target_parent"
    mkdir -p -- "$target_parent"
    [[ ! -e $target ]] || die "fixture copy target already exists: $target"
    cp -a --reflink=auto -- "$source" "$target"
}

copy_temp_file() {
    local source=$1
    local target=$2
    local target_parent
    target_parent=$(dirname -- "$target")
    assert_under_temp "$source"
    assert_under_temp "$target"
    assert_under_temp "$target_parent"
    [[ -f $source && ! -L $source ]] || die "temporary copy source must be a regular file: $source"
    mkdir -p -- "$target_parent"
    [[ ! -e $target ]] || die "temporary copy target already exists: $target"
    cp --preserve=mode,timestamps -- "$source" "$target"
    [[ $(sha256sum -- "$source" | awk '{print $1}') == $(sha256sum -- "$target" | awk '{print $1}') ]] || \
        die "temporary file copy hash mismatch: source=$source target=$target"
}

setup_cargo_isolation() {
    local cache_name source_cache cargo_version_log probe_root probe_log wrapper_name
    export CARGO_HOME="$TEMP_ROOT/cargo-home"
    export CARGO_NET_OFFLINE=true
    export CARGO_BUILD_RUSTC="$RUSTC_BIN"
    export CARGO_INCREMENTAL=0
    assert_under_temp "$CARGO_HOME"
    mkdir -p -- "$CARGO_HOME"
    for cache_name in registry git; do
        source_cache="$SOURCE_CARGO_HOME/$cache_name"
        [[ -d $source_cache ]] || die "required source Cargo cache is missing: $source_cache"
        [[ ! -e $CARGO_HOME/$cache_name ]] || die "isolated Cargo cache target exists: $cache_name"
        cp -a --reflink=auto -- "$source_cache" "$CARGO_HOME/$cache_name"
    done
    cat >"$CARGO_HOME/config.toml" <<EOF
[build]
rustc = "$RUSTC_BIN"

[net]
offline = true
git-fetch-with-cli = false
EOF
    [[ ! -e $CARGO_HOME/credentials && ! -e $CARGO_HOME/credentials.toml ]] || \
        die "isolated Cargo home must not inherit registry credentials"
    verify_cargo_config_file "$CARGO_HOME/config.toml"
    for wrapper_name in RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER CARGO_BUILD_RUSTC_WRAPPER \
        CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER; do
        [[ -z ${!wrapper_name-} ]] || die "compiler wrapper environment survived isolation: $wrapper_name"
    done

    cargo_version_log="$TEMP_ROOT/cargo-version.txt"
    "$CARGO_BIN" -vV >"$cargo_version_log"
    grep -E '^cargo [0-9]' "$cargo_version_log" >/dev/null || \
        die "cargo -vV did not report Cargo identity"
    cmp -s <(printf '%s\n' "$CARGO_VERSION") "$cargo_version_log" || \
        die "pinned Cargo identity changed after isolation"
    verify_cargo_config_guard
    probe_root="$TEMP_ROOT/cargo-probe"
    probe_log="$TEMP_ROOT/cargo-probe.log"
    mkdir -p -- "$probe_root/src"
    cat >"$probe_root/Cargo.toml" <<'EOF'
[package]
name = "kiwi-compat-cargo-probe"
version = "0.0.0"
edition = "2024"

[workspace]
EOF
    cat >"$probe_root/Cargo.lock" <<'EOF'
# This file is automatically @generated by Cargo.
version = 4

[[package]]
name = "kiwi-compat-cargo-probe"
version = "0.0.0"
EOF
    cat >"$probe_root/src/main.rs" <<'EOF'
fn main() {
    println!("kiwi compatibility cargo probe");
}
EOF
    if ! (
        cd -- "$probe_root"
        verify_cargo_config_chain "$probe_root"
        CARGO_TARGET_DIR="$probe_root/target" "$CARGO_BIN" check --locked -vv
    ) >"$probe_log" 2>&1; then
        cat -- "$probe_log" >&2
        die "isolated Cargo compiler probe failed"
    fi
    python3 "$FIXTURE_TOOL" verify-cargo-probe --input "$probe_log" --rustc "$RUSTC_BIN" || \
        die "Cargo probe did not use pinned rustc as the compiler process"
    if grep -Fi 'sccache' "$probe_log" >/dev/null; then
        die "Cargo probe invoked or reported sccache"
    fi
    verify_path_authority_guard
    printf 'BUILD_CACHE mode=disabled cargo_home=%s source_cache=%s cargo=%s rustc=%s cc=%s cxx=%s tracked_processes=%d\n' \
        "$CARGO_HOME" "$SOURCE_CARGO_HOME" "$CARGO_BIN" "$RUSTC_BIN" "$CC" "$CXX" \
        "${#BACKGROUND_PIDS[@]}"
}

setup_path_authority_guard
setup_cargo_isolation

printf 'PROVENANCE base=%s vector_v1=%s head=%s controller=%s common_git_dir=%s temp_root=%s\n' \
    "$BASE_SHA" "$VECTOR_V1_SHA" "$HEAD_SHA" "$CONTROLLER_SHA" "$COMMON_GIT_DIR" "$TEMP_ROOT"

BASE_WORKTREE="$TEMP_ROOT/worktrees/base"
VECTOR_WORKTREE="$TEMP_ROOT/worktrees/vector-v1"
HEAD_WORKTREE="$TEMP_ROOT/worktrees/head"
add_worktree "$BASE_WORKTREE" "$BASE_SHA"
add_worktree "$VECTOR_WORKTREE" "$VECTOR_V1_SHA"
add_worktree "$HEAD_WORKTREE" "$HEAD_SHA"

emit_driver base-storage "$BASE_WORKTREE/src/storage/tests/vector_storage_compat_external.rs"
emit_driver vector-storage "$VECTOR_WORKTREE/src/storage/tests/vector_storage_compat_external.rs"
emit_driver base-snapshot "$BASE_WORKTREE/src/raft/tests/vector_storage_snapshot_compat_external.rs"
emit_driver head-storage "$HEAD_WORKTREE/src/storage/tests/vector_storage_compat_external.rs"
emit_driver head-snapshot "$HEAD_WORKTREE/src/raft/tests/vector_storage_snapshot_compat_external.rs"

BASE_TARGET="$TEMP_ROOT/build/base/cargo-target"
VECTOR_TARGET="$TEMP_ROOT/build/vector-v1/cargo-target"
HEAD_TARGET="$TEMP_ROOT/build/head/cargo-target"
BASE_ARTIFACTS="$TEMP_ROOT/build/base/executables"
VECTOR_ARTIFACTS="$TEMP_ROOT/build/vector-v1/executables"
HEAD_ARTIFACTS="$TEMP_ROOT/build/head/executables"

# Cargo target directories are intentionally isolated per exact ref. Reusing one
# target across these worktrees can cross-contaminate same-name path crates such
# as `conf` even though their source trees and schemas differ.
BASE_BUILD_LOG=$(build_test_group base "$BASE_WORKTREE" "$BASE_TARGET" "" \
    storage:vector_storage_compat_external raft:vector_storage_snapshot_compat_external)
BASE_STORAGE_EXE=$(copy_group_executable base-storage "$BASE_WORKTREE" "$BASE_ARTIFACTS" \
    "$BASE_BUILD_LOG" vector_storage_compat_external)
BASE_SNAPSHOT_EXE=$(copy_group_executable base-snapshot "$BASE_WORKTREE" "$BASE_ARTIFACTS" \
    "$BASE_BUILD_LOG" vector_storage_snapshot_compat_external)

VECTOR_BUILD_LOG=$(build_test_group vector-v1 "$VECTOR_WORKTREE" "$VECTOR_TARGET" "" \
    storage:vector_storage_compat_external)
VECTOR_STORAGE_EXE=$(copy_group_executable vector-storage "$VECTOR_WORKTREE" "$VECTOR_ARTIFACTS" \
    "$VECTOR_BUILD_LOG" vector_storage_compat_external)

HEAD_BUILD_LOG=$(build_test_group head "$HEAD_WORKTREE" "$HEAD_TARGET" \
    storage/test-fault-injection \
    storage:vector_storage_compat_external raft:vector_storage_snapshot_compat_external \
    raft:snapshot_roundtrip_test)
HEAD_STORAGE_EXE=$(copy_group_executable head-storage "$HEAD_WORKTREE" "$HEAD_ARTIFACTS" \
    "$HEAD_BUILD_LOG" vector_storage_compat_external)
HEAD_SNAPSHOT_EXE=$(copy_group_executable head-snapshot "$HEAD_WORKTREE" "$HEAD_ARTIFACTS" \
    "$HEAD_BUILD_LOG" vector_storage_snapshot_compat_external)
HEAD_ROUNDTRIP_EXE=$(copy_group_executable head-roundtrip "$HEAD_WORKTREE" "$HEAD_ARTIFACTS" \
    "$HEAD_BUILD_LOG" snapshot_roundtrip_test)

BASE_SOURCE="$TEMP_ROOT/fixtures/base-source"
VECTOR_SOURCE="$TEMP_ROOT/fixtures/vector-source"

begin_gate base_688d905f_creates_real_six_cf_nonempty_storage
run_exact_test "$BASE_STORAGE_EXE" create_fixture KIWI_COMPAT_ROOT="$BASE_SOURCE"
run_exact_test "$BASE_STORAGE_EXE" reopen_fixture KIWI_COMPAT_ROOT="$BASE_SOURCE"
[[ $(find "$BASE_SOURCE" -type f | wc -l) -gt 0 ]] || die "Base fixture file count is zero"
pass_gate base_688d905f_creates_real_six_cf_nonempty_storage

begin_gate vector_v1_733888fc_creates_real_seven_cf_manifest_v1_storage
run_exact_test "$VECTOR_STORAGE_EXE" create_fixture KIWI_COMPAT_ROOT="$VECTOR_SOURCE"
run_exact_test "$VECTOR_STORAGE_EXE" reopen_fixture KIWI_COMPAT_ROOT="$VECTOR_SOURCE"
[[ $(find "$VECTOR_SOURCE" -name __kiwi_storage_manifest -type f | wc -l) -eq 2 ]] || \
    die "Vector-v1 fixture must contain one v1 manifest per instance"
pass_gate vector_v1_733888fc_creates_real_seven_cf_manifest_v1_storage

begin_gate head_upgrades_and_reopens_real_base_storage
BASE_UPGRADE="$TEMP_ROOT/cases/base-upgrade"
copy_fixture "$BASE_SOURCE" "$BASE_UPGRADE"
run_exact_test "$HEAD_STORAGE_EXE" head_upgrade_and_reopen_external \
    KIWI_COMPAT_ROOT="$BASE_UPGRADE" KIWI_COMPAT_PROFILE=base
pass_gate head_upgrades_and_reopens_real_base_storage

begin_gate head_upgrades_and_reopens_real_vector_v1_storage
VECTOR_UPGRADE="$TEMP_ROOT/cases/vector-upgrade"
copy_fixture "$VECTOR_SOURCE" "$VECTOR_UPGRADE"
run_exact_test "$HEAD_STORAGE_EXE" head_upgrade_and_reopen_external \
    KIWI_COMPAT_ROOT="$VECTOR_UPGRADE" KIWI_COMPAT_PROFILE=vector
pass_gate head_upgrades_and_reopens_real_vector_v1_storage

begin_gate head_retries_every_migration_phase_for_both_source_profiles
V2_MUTATION_ROOT="$TEMP_ROOT/cases/mutation-v2-shadow-data"
V2_MUTATION_AUTHORITY="$TEMP_ROOT/cases/mutation-v2-shadow-authority"
V2_MUTATION_COPY_ROOT="$TEMP_ROOT/cases/mutation-v2-shadow-copies"
V2_MUTATION_COUNT="$TEMP_ROOT/cases/mutation-v2-shadow-count"
copy_fixture "$BASE_SOURCE" "$V2_MUTATION_ROOT"
expect_exact_test_failure v2-shadow-data \
    "V2 validation copy partitioned String mismatch for original instance 0" \
    "$HEAD_STORAGE_EXE" inject_fault_and_assert_interrupted_external \
    KIWI_COMPAT_ROOT="$V2_MUTATION_ROOT" \
    KIWI_COMPAT_AUTHORITY_ROOT="$V2_MUTATION_AUTHORITY" \
    KIWI_COMPAT_V2_AUTHORITY_ROOT="$V2_MUTATION_COPY_ROOT" \
    KIWI_COMPAT_V2_COUNT_FILE="$V2_MUTATION_COUNT" \
    KIWI_COMPAT_PROFILE=base KIWI_COMPAT_FAULT=instance-upgraded-0 \
    KIWI_COMPAT_CORRUPT_V2_VALIDATION=1
safe_remove "$V2_MUTATION_ROOT"
safe_remove "$V2_MUTATION_AUTHORITY"
safe_remove "$V2_MUTATION_COPY_ROOT"
safe_remove "$V2_MUTATION_COUNT"
COMMON_FAULTS=(
    source-detected shadow-prepared instance-copied-0 instance-copied-1
    instance-upgraded-0 instance-upgraded-1 all-verified switch-prepared
    old-moved-0 old-moved-1 shadow-promoted-0 shadow-promoted-1
    new-storage-opened committed
)
BASE_ONLY_FAULTS=(vector-cf-created-0 vector-cf-created-1)
PHASE_COUNT=0
V2_COPY_COUNT=0
for source_profile in base vector; do
    if [[ $source_profile == base ]]; then
        source_root=$BASE_SOURCE
        faults=("${COMMON_FAULTS[@]}" "${BASE_ONLY_FAULTS[@]}")
    else
        source_root=$VECTOR_SOURCE
        faults=("${COMMON_FAULTS[@]}")
    fi
    for fault in "${faults[@]}"; do
        case_root="$TEMP_ROOT/cases/retry-$source_profile-$fault"
        authority_root="$TEMP_ROOT/cases/authority-$source_profile-$fault"
        v2_authority_root="$TEMP_ROOT/cases/v2-authority-$source_profile-$fault"
        v2_count_file="$TEMP_ROOT/cases/v2-count-$source_profile-$fault"
        copy_fixture "$source_root" "$case_root"
        run_exact_test "$HEAD_STORAGE_EXE" inject_fault_and_assert_interrupted_external \
            KIWI_COMPAT_ROOT="$case_root" KIWI_COMPAT_AUTHORITY_ROOT="$authority_root" \
            KIWI_COMPAT_V2_AUTHORITY_ROOT="$v2_authority_root" \
            KIWI_COMPAT_V2_COUNT_FILE="$v2_count_file" \
            KIWI_COMPAT_PROFILE="$source_profile" KIWI_COMPAT_FAULT="$fault"
        [[ -f $v2_count_file && ! -L $v2_count_file ]] || \
            die "completed V2-copy count is missing: profile=$source_profile fault=$fault"
        v2_case_count=$(<"$v2_count_file")
        [[ $v2_case_count =~ ^[0-9]+$ ]] || \
            die "invalid V2-copy count: profile=$source_profile fault=$fault count=$v2_case_count"
        if [[ $source_profile == base ]]; then
            run_exact_test "$BASE_STORAGE_EXE" reopen_fixture KIWI_COMPAT_ROOT="$authority_root"
        else
            run_exact_test "$VECTOR_STORAGE_EXE" reopen_fixture KIWI_COMPAT_ROOT="$authority_root"
        fi
        V2_COPY_COUNT=$((V2_COPY_COUNT + v2_case_count))
        ((PHASE_COUNT += 1))
        run_exact_test "$HEAD_STORAGE_EXE" resume_after_asserted_fault_external \
            KIWI_COMPAT_ROOT="$case_root" KIWI_COMPAT_PROFILE="$source_profile"
        safe_remove "$case_root"
        safe_remove "$authority_root"
        safe_remove "$v2_authority_root"
        safe_remove "$v2_count_file"
    done
done
[[ $PHASE_COUNT -eq 30 ]] || die "migration phase execution count mismatch: $PHASE_COUNT"
[[ $V2_COPY_COUNT -eq 43 ]] || die "interrupted V2-copy execution count mismatch: $V2_COPY_COUNT"
printf 'PHASES PASS executed=%d base=16 vector=14 v2_copies=%d\n' "$PHASE_COUNT" "$V2_COPY_COUNT"
pass_gate head_retries_every_migration_phase_for_both_source_profiles

begin_gate base_reopens_verified_pre_admission_rollback
BASE_ROLLBACK="$TEMP_ROOT/cases/base-rollback"
copy_fixture "$BASE_SOURCE" "$BASE_ROLLBACK"
run_exact_test "$HEAD_STORAGE_EXE" rollback_to_legacy_external \
    KIWI_COMPAT_ROOT="$BASE_ROLLBACK" KIWI_COMPAT_PROFILE=base
run_exact_test "$BASE_STORAGE_EXE" reopen_fixture KIWI_COMPAT_ROOT="$BASE_ROLLBACK"
pass_gate base_reopens_verified_pre_admission_rollback

begin_gate vector_v1_reopens_verified_pre_admission_rollback
VECTOR_ROLLBACK="$TEMP_ROOT/cases/vector-rollback"
copy_fixture "$VECTOR_SOURCE" "$VECTOR_ROLLBACK"
run_exact_test "$HEAD_STORAGE_EXE" rollback_to_legacy_external \
    KIWI_COMPAT_ROOT="$VECTOR_ROLLBACK" KIWI_COMPAT_PROFILE=vector
run_exact_test "$VECTOR_STORAGE_EXE" reopen_fixture KIWI_COMPAT_ROOT="$VECTOR_ROLLBACK"
pass_gate vector_v1_reopens_verified_pre_admission_rollback

begin_gate head_rejects_base_rollback_after_rollback_window_closed
BASE_CLOSED="$TEMP_ROOT/cases/base-closed"
copy_fixture "$BASE_SOURCE" "$BASE_CLOSED"
run_exact_test "$HEAD_STORAGE_EXE" close_window_and_reject_rollback_external \
    KIWI_COMPAT_ROOT="$BASE_CLOSED" KIWI_COMPAT_PROFILE=base
pass_gate head_rejects_base_rollback_after_rollback_window_closed

begin_gate base_v1_snapshot_restores_on_head
BASE_ARCHIVE="$TEMP_ROOT/snapshots/base-v1.snapshot"
BASE_SNAPSHOT_META="$TEMP_ROOT/snapshots/base-v1.meta.json"
BASE_SNAPSHOT_WORK="$TEMP_ROOT/snapshots/base-build-work"
HEAD_V1_TARGET="$TEMP_ROOT/snapshots/head-v1-target"
HEAD_V1_WORK="$TEMP_ROOT/snapshots/head-v1-work"
mkdir -p -- "$(dirname -- "$BASE_ARCHIVE")"
run_exact_test "$BASE_SNAPSHOT_EXE" build_exact_base_v1_snapshot \
    KIWI_COMPAT_ROOT="$BASE_SOURCE" KIWI_COMPAT_ARCHIVE="$BASE_ARCHIVE" \
    KIWI_COMPAT_SNAPSHOT_META="$BASE_SNAPSHOT_META" \
    KIWI_COMPAT_SNAPSHOT_WORK="$BASE_SNAPSHOT_WORK"
[[ -s $BASE_ARCHIVE ]] || die "Base v1 snapshot archive is empty"
[[ -s $BASE_SNAPSHOT_META ]] || die "Base SnapshotMeta sidecar is empty"
printf 'SNAPSHOT base_v1_sha256=%s bytes=%s meta_sha256=%s meta_bytes=%s\n' \
    "$(sha256sum -- "$BASE_ARCHIVE" | awk '{print $1}')" "$(stat -c %s -- "$BASE_ARCHIVE")" \
    "$(sha256sum -- "$BASE_SNAPSHOT_META" | awk '{print $1}')" \
    "$(stat -c %s -- "$BASE_SNAPSHOT_META")"
run_exact_test "$HEAD_SNAPSHOT_EXE" restore_exact_base_v1_archive_external \
    KIWI_COMPAT_ARCHIVE="$BASE_ARCHIVE" KIWI_COMPAT_TARGET="$HEAD_V1_TARGET" \
    KIWI_COMPAT_SNAPSHOT_META="$BASE_SNAPSHOT_META" \
    KIWI_COMPAT_SNAPSHOT_WORK="$HEAD_V1_WORK"
pass_gate base_v1_snapshot_restores_on_head

begin_gate v1_snapshot_with_unknown_or_vector_schema_is_rejected
GATE10_CASE_COUNT=0
for mutation in unknown-cf vector-cf vector-meta; do
    for mutation_instance in 0 1; do
        MUTATED_BASE_ARCHIVE="$TEMP_ROOT/snapshots/base-v1-$mutation-instance$mutation_instance.snapshot"
        MUTATED_BASE_META="$TEMP_ROOT/snapshots/base-v1-$mutation-instance$mutation_instance.meta.json"
        MUTATION_ROOT="$TEMP_ROOT/snapshots/base-v1-$mutation-instance$mutation_instance-unpack"
        MUTATION_TARGET="$TEMP_ROOT/snapshots/base-v1-$mutation-instance$mutation_instance-target"
        MUTATION_WORK="$TEMP_ROOT/snapshots/base-v1-$mutation-instance$mutation_instance-work"
        copy_temp_file "$BASE_ARCHIVE" "$MUTATED_BASE_ARCHIVE"
        copy_temp_file "$BASE_SNAPSHOT_META" "$MUTATED_BASE_META"
        run_exact_test "$HEAD_SNAPSHOT_EXE" reject_mutated_exact_base_v1_archive_external \
            KIWI_COMPAT_ARCHIVE="$MUTATED_BASE_ARCHIVE" \
            KIWI_COMPAT_SNAPSHOT_META="$MUTATED_BASE_META" \
            KIWI_COMPAT_MUTATION="$mutation" \
            KIWI_COMPAT_MUTATION_INSTANCE="$mutation_instance" \
            KIWI_COMPAT_MUTATION_ROOT="$MUTATION_ROOT" \
            KIWI_COMPAT_TARGET="$MUTATION_TARGET" KIWI_COMPAT_SNAPSHOT_WORK="$MUTATION_WORK"
        ((GATE10_CASE_COUNT += 1))
    done
done
[[ $GATE10_CASE_COUNT -eq 6 ]] || die "Gate10 invalid-snapshot case count mismatch: $GATE10_CASE_COUNT"
printf 'GATE10_CASES PASS executed=%d instances=2 mutations=3\n' "$GATE10_CASE_COUNT"
for authority_mutation in hash-instance1 zset-instance1 ttl-instance1; do
    case "$authority_mutation" in
        hash-instance1) authority_expected="target authority Hash mismatch instance 1" ;;
        zset-instance1) authority_expected="target authority ZSet mismatch instance 1" ;;
        ttl-instance1) authority_expected="target authority TTL value mismatch instance 1" ;;
        *) die "unknown target-authority mutation: $authority_mutation" ;;
    esac
    AUTHORITY_ARCHIVE="$TEMP_ROOT/snapshots/authority-$authority_mutation.snapshot"
    AUTHORITY_META="$TEMP_ROOT/snapshots/authority-$authority_mutation.meta.json"
    AUTHORITY_MUTATION_ROOT="$TEMP_ROOT/snapshots/authority-$authority_mutation-unpack"
    AUTHORITY_TARGET="$TEMP_ROOT/snapshots/authority-$authority_mutation-target"
    AUTHORITY_WORK="$TEMP_ROOT/snapshots/authority-$authority_mutation-work"
    copy_temp_file "$BASE_ARCHIVE" "$AUTHORITY_ARCHIVE"
    copy_temp_file "$BASE_SNAPSHOT_META" "$AUTHORITY_META"
    expect_exact_test_failure "target-authority-$authority_mutation" "$authority_expected" \
        "$HEAD_SNAPSHOT_EXE" reject_mutated_exact_base_v1_archive_external \
        KIWI_COMPAT_ARCHIVE="$AUTHORITY_ARCHIVE" \
        KIWI_COMPAT_SNAPSHOT_META="$AUTHORITY_META" \
        KIWI_COMPAT_MUTATION=unknown-cf KIWI_COMPAT_MUTATION_INSTANCE=0 \
        KIWI_COMPAT_MUTATION_ROOT="$AUTHORITY_MUTATION_ROOT" \
        KIWI_COMPAT_TARGET="$AUTHORITY_TARGET" \
        KIWI_COMPAT_SNAPSHOT_WORK="$AUTHORITY_WORK" \
        KIWI_COMPAT_CORRUPT_TARGET_AFTER_REJECT="$authority_mutation"
done
printf 'MUTATIONS target-authority PASS executed=3 polluted_instance=1 invalid_instance=0\n'
pass_gate v1_snapshot_with_unknown_or_vector_schema_is_rejected

begin_gate head_v2_snapshot_reopens_with_exact_manifest_pairing
HEAD_V2_SOURCE="$TEMP_ROOT/snapshots/head-v2-source"
HEAD_V2_TARGET="$TEMP_ROOT/snapshots/head-v2-target"
HEAD_V2_BUILD_WORK="$TEMP_ROOT/snapshots/head-v2-build-work"
HEAD_V2_INSTALL_WORK="$TEMP_ROOT/snapshots/head-v2-install-work"
HEAD_V2_NEGATIVE_ROOT="$TEMP_ROOT/snapshots/head-v2-negative"
HEAD_V2_ARCHIVE="$TEMP_ROOT/snapshots/head-v2.snapshot"
HEAD_V2_META="$TEMP_ROOT/snapshots/head-v2.meta.json"
run_exact_test "$HEAD_SNAPSHOT_EXE" head_v2_two_instance_exact_pairing_external \
    KIWI_COMPAT_HEAD_SOURCE="$HEAD_V2_SOURCE" KIWI_COMPAT_TARGET="$HEAD_V2_TARGET" \
    KIWI_COMPAT_BUILD_WORK="$HEAD_V2_BUILD_WORK" \
    KIWI_COMPAT_SNAPSHOT_WORK="$HEAD_V2_INSTALL_WORK" \
    KIWI_COMPAT_NEGATIVE_ROOT="$HEAD_V2_NEGATIVE_ROOT" \
    KIWI_COMPAT_HEAD_ARCHIVE="$HEAD_V2_ARCHIVE" \
    KIWI_COMPAT_HEAD_SNAPSHOT_META="$HEAD_V2_META"
[[ -s $HEAD_V2_ARCHIVE ]] || die "Head v2 snapshot archive is empty"
[[ -s $HEAD_V2_META ]] || die "Head v2 SnapshotMeta sidecar is empty"
printf 'SNAPSHOT head_v2_sha256=%s bytes=%s meta_sha256=%s meta_bytes=%s instances=2 negative_cases=4\n' \
    "$(sha256sum -- "$HEAD_V2_ARCHIVE" | awk '{print $1}')" \
    "$(stat -c %s -- "$HEAD_V2_ARCHIVE")" \
    "$(sha256sum -- "$HEAD_V2_META" | awk '{print $1}')" \
    "$(stat -c %s -- "$HEAD_V2_META")"
run_exact_test "$HEAD_ROUNDTRIP_EXE" cursor_snapshot_roundtrip
pass_gate head_v2_snapshot_reopens_with_exact_manifest_pairing

GATE_ARGS=()
for gate in "${EXECUTED_GATES[@]}"; do
    GATE_ARGS+=(--executed "$gate")
done
python3 "$FIXTURE_TOOL" verify-gate-contract "${GATE_ARGS[@]}"
verify_path_authority_guard
printf 'MATRIX PASS gates=%d phases=%d v2_copies=%d gate10_cases=%d base=%s vector_v1=%s head=%s\n' \
    "${#EXECUTED_GATES[@]}" "$PHASE_COUNT" "$V2_COPY_COUNT" "$GATE10_CASE_COUNT" \
    "$BASE_SHA" "$VECTOR_V1_SHA" "$HEAD_SHA"
