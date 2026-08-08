#!/usr/bin/env bash

set -euo pipefail

readonly EXPECTED_BASE_REF="688d905fec31b54aec76f36676f55efd8b5cfa17"
readonly EXPECTED_VECTOR_V1_REF="733888fc90ad8ef039947e87b08d7500a405954a"

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

for tool in awk bash basename c++ cargo cat cc cp date dirname find git grep mkdir mktemp \
    pgrep python3 realpath rm sed sha256sum stat wc; do
    command -v "$tool" >/dev/null 2>&1 || die "required Linux tool is missing: $tool"
done

unset RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER CARGO_BUILD_RUSTC_WRAPPER
unset SCCACHE_DIR SCCACHE_CACHE_SIZE SCCACHE_ENDPOINT SCCACHE_BUCKET SCCACHE_REGION
unset SCCACHE_S3_USE_SSL SCCACHE_REDIS SCCACHE_MEMCACHED
export CC="$(command -v cc)"
export CXX="$(command -v c++)"
printf 'BUILD_CACHE mode=disabled cc=%s cxx=%s\n' "$CC" "$CXX"

if [[ -d $REPO_ROOT/.git ]]; then
    COMMON_GIT_DIR=$(realpath -- "$REPO_ROOT/.git")
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
    case "$GITDIR_RECORD" in
        */.git/worktrees/*) COMMON_GIT_DIR=${GITDIR_RECORD%/worktrees/*} ;;
        *) die "linked-worktree gitdir is outside the expected common Git layout: $GITDIR_RECORD" ;;
    esac
else
    die "repository .git metadata is missing"
fi
[[ -d $COMMON_GIT_DIR ]] || die "common Git directory is missing: $COMMON_GIT_DIR"

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
declare -a EXECUTED_GATES=()
declare -A GATE_SET=()
ACTIVE_GATE=""
ACTIVE_GATE_STARTED=0

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

cleanup() {
    local status=$?
    local cleanup_failed=0
    local -a lingering_temp_pids=()
    trap - EXIT INT TERM
    set +e
    if [[ -n $ACTIVE_GATE ]]; then
        printf 'GATE %s FAIL duration=%ss\n' "$ACTIVE_GATE" "$(( $(date +%s) - ACTIVE_GATE_STARTED ))" >&2
    fi
    for pid in "${BACKGROUND_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            printf 'CLEANUP FAIL runner_background_pid_alive=%s\n' "$pid" >&2
            cleanup_failed=1
        fi
    done
    while IFS= read -r pid; do
        [[ -n $pid && $pid != $$ && $pid != $PPID ]] || continue
        lingering_temp_pids+=("$pid")
    done < <(pgrep -f -- "$TEMP_ROOT" 2>/dev/null || true)
    if ((${#lingering_temp_pids[@]} > 0)); then
        printf 'CLEANUP FAIL temp_root_processes=%s\n' "${lingering_temp_pids[*]}" >&2
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
        printf 'CLEANUP PASS temp_root_removed=true worktrees_removed=%d tracked_processes=%d lingering_temp_processes=0\n' \
            "${#WORKTREES[@]}" "${#BACKGROUND_PIDS[@]}"
    fi
    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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
    local -a command=(cargo test)
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

printf 'PROVENANCE base=%s vector_v1=%s head=%s common_git_dir=%s temp_root=%s\n' \
    "$BASE_SHA" "$VECTOR_V1_SHA" "$HEAD_SHA" "$COMMON_GIT_DIR" "$TEMP_ROOT"

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
COMMON_FAULTS=(
    source-detected shadow-prepared instance-copied-0 instance-copied-1
    instance-upgraded-0 instance-upgraded-1 all-verified switch-prepared
    old-moved-0 old-moved-1 shadow-promoted-0 shadow-promoted-1
    new-storage-opened committed
)
BASE_ONLY_FAULTS=(vector-cf-created-0 vector-cf-created-1)
PHASE_COUNT=0
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
        copy_fixture "$source_root" "$case_root"
        run_exact_test "$HEAD_STORAGE_EXE" inject_fault_and_assert_interrupted_external \
            KIWI_COMPAT_ROOT="$case_root" KIWI_COMPAT_AUTHORITY_ROOT="$authority_root" \
            KIWI_COMPAT_PROFILE="$source_profile" KIWI_COMPAT_FAULT="$fault"
        if [[ $source_profile == base ]]; then
            run_exact_test "$BASE_STORAGE_EXE" reopen_fixture KIWI_COMPAT_ROOT="$authority_root"
        else
            run_exact_test "$VECTOR_STORAGE_EXE" reopen_fixture KIWI_COMPAT_ROOT="$authority_root"
        fi
        ((PHASE_COUNT += 1))
        run_exact_test "$HEAD_STORAGE_EXE" resume_after_asserted_fault_external \
            KIWI_COMPAT_ROOT="$case_root" KIWI_COMPAT_PROFILE="$source_profile"
        safe_remove "$case_root"
        safe_remove "$authority_root"
    done
done
[[ $PHASE_COUNT -eq 30 ]] || die "migration phase execution count mismatch: $PHASE_COUNT"
printf 'PHASES PASS executed=%d base=16 vector=14\n' "$PHASE_COUNT"
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
for mutation in unknown-cf vector-cf vector-meta; do
    MUTATED_BASE_ARCHIVE="$TEMP_ROOT/snapshots/base-v1-$mutation.snapshot"
    MUTATED_BASE_META="$TEMP_ROOT/snapshots/base-v1-$mutation.meta.json"
    MUTATION_ROOT="$TEMP_ROOT/snapshots/base-v1-$mutation-unpack"
    MUTATION_TARGET="$TEMP_ROOT/snapshots/base-v1-$mutation-target"
    MUTATION_WORK="$TEMP_ROOT/snapshots/base-v1-$mutation-work"
    copy_temp_file "$BASE_ARCHIVE" "$MUTATED_BASE_ARCHIVE"
    copy_temp_file "$BASE_SNAPSHOT_META" "$MUTATED_BASE_META"
    run_exact_test "$HEAD_SNAPSHOT_EXE" reject_mutated_exact_base_v1_archive_external \
        KIWI_COMPAT_ARCHIVE="$MUTATED_BASE_ARCHIVE" \
        KIWI_COMPAT_SNAPSHOT_META="$MUTATED_BASE_META" \
        KIWI_COMPAT_MUTATION="$mutation" KIWI_COMPAT_MUTATION_ROOT="$MUTATION_ROOT" \
        KIWI_COMPAT_TARGET="$MUTATION_TARGET" KIWI_COMPAT_SNAPSHOT_WORK="$MUTATION_WORK"
done
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
printf 'MATRIX PASS gates=%d phases=%d base=%s vector_v1=%s head=%s\n' \
    "${#EXECUTED_GATES[@]}" "$PHASE_COUNT" "$BASE_SHA" "$VECTOR_V1_SHA" "$HEAD_SHA"
