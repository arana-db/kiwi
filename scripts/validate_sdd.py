#!/usr/bin/env python3

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

"""Validate the sole-authority SDD control-plane contract."""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import urllib.request


REQ_ID = re.compile(r"\bREQ-[A-Z]+-\d{3}\b")
REQ_DEFINITION = re.compile(r"(?m)^- `(REQ-[A-Z]+-\d{3})`")
REQ_RANGE = re.compile(
    r"\b(REQ-[A-Z]+)-(\d{3})`?\s*(?:至|\.\.)\s*`?(REQ-[A-Z]+)-(\d{3})\b"
)
DECISION_ID = re.compile(r"\bD\d{3}\b")
DECISION_DEFINITION = re.compile(r"(?m)^##\s+(D\d{3})[：:]")
FRONT_MATTER_FIELD = re.compile(r"^([a-z][a-z0-9_]*):\s*(.*?)\s*$")
WP_HEADING = re.compile(r"(?m)^### (WP\d+)[：:]")
MARKDOWN_LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
SDD_PLACEHOLDER = re.compile(r"TO[D]O|TB[D]|待[定]|以后[补]|类似上[文]")

WORK_PACKAGE_IDS = tuple(f"WP{number}" for number in range(9))
WORK_PACKAGE_RANGE_TEXT = "WP0-WP8"
WORK_PACKAGE_RANGE_MARKDOWN = "`WP0`–`WP8`"

EXPECTED_WP0_ARTIFACTS = (
    ".github/pull_request_template.md",
    ".github/workflows/ci.yml",
    ".planning/DECISIONS.md",
    ".planning/KANBAN.md",
    ".planning/OPEN_QUESTIONS.md",
    ".planning/README.md",
    ".planning/REQUIREMENTS.md",
    ".planning/ROADMAP.md",
    ".planning/SDD.md",
    ".planning/STATE.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    "README.md",
    "docs/INDEX.md",
    "docs/architecture/redis-8.8.1-system-boundaries.md",
    "docs/personas-and-user-stories.md",
    "docs/prd.md",
    "docs/quality/quality-gates.md",
    "docs/quality/system-stability-gate.md",
    "scripts/validate_sdd.py",
)

CURRENT_FIELDS = (
    "current_work_package",
    "current_work_package_status",
    "current_plan",
    "current_issue",
    "current_pr",
)

WP0_EVIDENCE_FIELDS = (
    "wp0_pr_base_ref",
    "wp0_pr_head_ref",
    "wp0_merge_parent_ref",
    "wp0_merge_ref",
)

WP0_IDENTITY_FIELDS = ("wp0_pr_number",)

WP0_VERIFICATION_FIELDS = (
    "wp0_exact_main_verification_ref",
    "wp0_exact_main_verification_run",
    "wp0_exact_main_verification_status",
)

EXPECTED_WP0_IMMUTABLE_EVIDENCE = {
    "wp0_pr_number": "414",
    "wp0_pr_base_ref": "0c4795ec716299598686fc7c5e0fac03a30e044d",
    "wp0_pr_head_ref": "e2bfc7deb481590a757f0034874b7f21a4a31aa2",
    "wp0_merge_parent_ref": "cbcbadc27068634d851ab0ed63989d2214ab2408",
    "wp0_merge_ref": "9820162ebdf2d26aa6349e704efe8737b2e73e4a",
}

GITHUB_REPOSITORY = "arana-db/kiwi"
EXPECTED_BASELINE_BRANCH = "main"
EXPECTED_WP0_VERIFICATION_WORKFLOW = {
    "name": "ci",
    "path": ".github/workflows/ci.yml",
}
GithubRunLoader = Callable[[str], dict[str, object]]

ALLOWED_WP_STATUSES = {
    "proposed",
    "accepted-design",
    "ready",
    "in-progress",
    "implemented",
    "verified",
    "accepted",
    "released",
    "blocked",
    "deferred",
    "frozen",
    "superseded",
    "abandoned",
}

EXPECTED_REQUIREMENT_COUNT = 68
EXPECTED_DECISION_COUNT = 19

EXPECTED_WP0_EXIT_LINES = (
    "- front matter 是唯一机器可解析的当前状态；工作包块和状态表必须与其一致。",
    "- 所有链接和 REQ/Decision 定义、范围及引用全集闭包通过。",
    "- PR 模板要求工作包、Issue 和 REQ，并由评审门禁确认没有保留占位符。",
    "- 原草稿被吸收或删除。",
)

EXPECTED_WP0_VERIFICATION_LINES = (
    "- `git diff --check` 和 committed-diff whitespace check；",
    "- `python scripts/validate_sdd.py --self-test` 的失败路径变异测试；",
    "- `python scripts/validate_sdd.py` 的 Markdown 链接、占位词、围栏和状态断言；",
    "- WP0 exact-main 状态提升时，baseline_ref 必须推进到 verification ref 或其后的 main 提交，并在线核验 recorded GitHub Actions run 与 ci workflow、main push、精确 SHA 和 success 结论一致；",
    f"- {EXPECTED_REQUIREMENT_COUNT} 个 REQ 和 {EXPECTED_DECISION_COUNT} 个 Decision 的唯一注册、范围展开和引用全集闭包；",
    f"- WP0、primary Issue #413、PR #414 和 {len(EXPECTED_WP0_ARTIFACTS)} 个预期产物的一致性断言；",
    "- live Issue #413、开放 Issue 数量、关键 PR 状态和远端 main 复核；",
    "- 独立只读审查不得留下 Critical 或 Important finding。",
)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def duplicate_values(values: list[str]) -> list[str]:
    return sorted(value for value, count in Counter(values).items() if count > 1)


def strip_markdown_noncontract(text: str) -> str:
    without_comments = re.sub(r"(?s)<!--.*?-->", "", text)
    return re.sub(
        r"(?ms)^[ ]{0,3}(?P<fence>```|~~~)[^\n]*\n.*?^[ ]{0,3}(?P=fence)\s*$",
        "",
        without_comments,
    )


def parse_front_matter(sdd: str, errors: list[str]) -> dict[str, str]:
    lines = sdd.splitlines()
    if not lines or lines[0] != "---":
        errors.append("SDD front matter must start with ---")
        return {}

    try:
        closing = lines.index("---", 1)
    except ValueError:
        errors.append("SDD front matter is not closed")
        return {}

    pairs: list[tuple[str, str]] = []
    for line in lines[1:closing]:
        match = FRONT_MATTER_FIELD.fullmatch(line)
        if not match:
            errors.append(f"invalid front matter line: {line}")
            continue
        pairs.append((match.group(1), match.group(2)))

    keys = [key for key, _ in pairs]
    duplicates = duplicate_values(keys)
    if duplicates:
        errors.append(f"duplicate front matter fields: {duplicates}")

    fields = dict(pairs)
    required_fields = (
        CURRENT_FIELDS
        + WP0_EVIDENCE_FIELDS
        + WP0_IDENTITY_FIELDS
        + WP0_VERIFICATION_FIELDS
    )
    for field in required_fields:
        if keys.count(field) != 1:
            errors.append(f"front matter field {field} must occur exactly once")
    return fields


def expand_requirement_references(sdd: str, errors: list[str]) -> set[str]:
    references = set(REQ_ID.findall(sdd))
    for match in REQ_RANGE.finditer(sdd):
        start_prefix, start_number, end_prefix, end_number = match.groups()
        if start_prefix != end_prefix:
            errors.append(f"cross-domain requirement range is forbidden: {match.group(0)}")
            continue
        start = int(start_number)
        end = int(end_number)
        if start > end:
            errors.append(f"reverse requirement range is forbidden: {match.group(0)}")
            continue
        references.update(f"{start_prefix}-{number:03d}" for number in range(start, end + 1))
    return references


def scoped_requirement_text(sdd: str, errors: list[str]) -> str:
    scope_entries: list[str] = []
    for work_package, block in work_package_blocks(sdd).items():
        match = re.search(r"(?ms)^Requirement：\s*\n(.*?)(?=^依赖：)", block)
        if not match:
            errors.append(f"{work_package} Requirement scope cannot be parsed")
            continue
        scope_entries.extend(
            re.findall(
                r"(?m)^- (.+?)[；。]$", strip_markdown_noncontract(match.group(1))
            )
        )

    deferred = re.search(
        rf"(?ms)^Deferred Requirement：\s*\n(.*?)(?=^{re.escape(WORK_PACKAGE_RANGE_TEXT)} 的 Requirement 字段)",
        sdd,
    )
    if not deferred:
        errors.append("Deferred Requirement scope cannot be parsed")
    else:
        scope_entries.extend(
            re.findall(
                r"(?m)^- (.+?)[；。]$",
                strip_markdown_noncontract(deferred.group(1)),
            )
        )
    return "\n".join(scope_entries)


def scoped_decision_references(sdd: str, errors: list[str]) -> set[str]:
    section = re.search(r"(?ms)^## 18\..*?(?=^## 19\.)", sdd)
    if not section:
        errors.append("Decision gate section cannot be parsed")
        return set()
    return set(
        re.findall(
            r"(?m)^\| (D\d{3}) \|", strip_markdown_noncontract(section.group(0))
        )
    )


def validate_registries(root: Path, sdd: str, errors: list[str]) -> tuple[int, int]:
    requirements_path = root / ".planning/REQUIREMENTS.md"
    decisions_path = root / ".planning/DECISIONS.md"
    if not requirements_path.is_file() or not decisions_path.is_file():
        return 0, 0

    requirements = strip_markdown_noncontract(read_text(requirements_path))
    decisions = strip_markdown_noncontract(read_text(decisions_path))

    requirement_definitions = REQ_DEFINITION.findall(requirements)
    decision_definitions = DECISION_DEFINITION.findall(decisions)
    duplicate_requirements = duplicate_values(requirement_definitions)
    duplicate_decisions = duplicate_values(decision_definitions)
    if duplicate_requirements:
        errors.append(f"duplicate requirement definitions: {duplicate_requirements}")
    if duplicate_decisions:
        errors.append(f"duplicate decision definitions: {duplicate_decisions}")

    requirement_set = set(requirement_definitions)
    decision_set = set(decision_definitions)
    if len(requirement_set) != EXPECTED_REQUIREMENT_COUNT:
        errors.append(
            f"requirement registry must contain {EXPECTED_REQUIREMENT_COUNT} unique definitions, "
            f"found {len(requirement_set)}"
        )
    if len(decision_set) != EXPECTED_DECISION_COUNT:
        errors.append(
            f"decision registry must contain {EXPECTED_DECISION_COUNT} unique definitions, "
            f"found {len(decision_set)}"
        )

    requirement_references = expand_requirement_references(
        scoped_requirement_text(sdd, errors), errors
    )
    decision_references = scoped_decision_references(sdd, errors)

    undefined_requirements = sorted(requirement_references - requirement_set)
    unreferenced_requirements = sorted(requirement_set - requirement_references)
    undefined_decisions = sorted(decision_references - decision_set)
    unreferenced_decisions = sorted(decision_set - decision_references)
    if undefined_requirements:
        errors.append(f"undefined requirement references: {undefined_requirements}")
    if unreferenced_requirements:
        errors.append(f"unreferenced requirement definitions: {unreferenced_requirements}")
    if undefined_decisions:
        errors.append(f"undefined decision references: {undefined_decisions}")
    if unreferenced_decisions:
        errors.append(f"unreferenced decision definitions: {unreferenced_decisions}")

    return len(requirement_set), len(decision_set)


def work_package_blocks(sdd: str) -> dict[str, str]:
    headings = list(WP_HEADING.finditer(sdd))
    section_end_match = re.search(r"(?m)^## 13\.", sdd)
    section_end = section_end_match.start() if section_end_match else len(sdd)
    blocks: dict[str, str] = {}
    for index, heading in enumerate(headings):
        end = headings[index + 1].start() if index + 1 < len(headings) else section_end
        blocks[heading.group(1)] = sdd[heading.start() : end]
    return blocks


def validate_wp0_immutable_evidence(
    fields: dict[str, str], errors: list[str]
) -> None:
    for field, expected in EXPECTED_WP0_IMMUTABLE_EVIDENCE.items():
        actual = fields.get(field, "")
        if actual != expected:
            errors.append(f"{field} must remain {expected}, found {actual}")


def fetch_github_actions_run(run_id: str) -> dict[str, object]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "kiwi-sdd-validator",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/runs/{run_id}",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, ValueError) as error:
        raise RuntimeError(
            f"unable to load GitHub Actions run {run_id}: {type(error).__name__}"
        ) from error
    if not isinstance(payload, dict):
        raise RuntimeError(f"GitHub Actions run {run_id} returned a non-object payload")
    return payload


def validate_wp0_github_run_evidence(
    fields: dict[str, str], run: dict[str, object], errors: list[str]
) -> None:
    run_id = fields.get("wp0_exact_main_verification_run", "")
    expected: dict[str, object] = {
        "id": int(run_id),
        "status": "completed",
        "conclusion": "success",
        "event": "push",
        "head_branch": EXPECTED_BASELINE_BRANCH,
        "head_sha": fields.get("wp0_exact_main_verification_ref", ""),
        **EXPECTED_WP0_VERIFICATION_WORKFLOW,
    }
    for field, expected_value in expected.items():
        actual = run.get(field)
        if actual != expected_value:
            errors.append(
                "WP0 exact-main GitHub Actions run "
                f"{field} must be {expected_value!r}, found {actual!r}"
            )


def validate_wp0_exact_main_run(
    fields: dict[str, str],
    errors: list[str],
    loader: GithubRunLoader | None = None,
) -> None:
    if fields.get("wp0_exact_main_verification_status") != "passed":
        return
    run_id = fields.get("wp0_exact_main_verification_run", "")
    if not re.fullmatch(r"[1-9][0-9]*", run_id):
        return
    load_run = loader or fetch_github_actions_run
    try:
        run = load_run(run_id)
    except Exception as error:
        errors.append(
            f"unable to validate WP0 exact-main GitHub Actions run {run_id}: "
            f"{type(error).__name__}: {error}"
        )
        return
    validate_wp0_github_run_evidence(fields, run, errors)


def validate_current_state(sdd: str, fields: dict[str, str], errors: list[str]) -> None:
    work_package_ids = WP_HEADING.findall(sdd)
    duplicate_work_packages = duplicate_values(work_package_ids)
    if duplicate_work_packages:
        errors.append(f"duplicate work package headings: {duplicate_work_packages}")
    blocks = work_package_blocks(sdd)
    expected_work_packages = set(WORK_PACKAGE_IDS)
    if set(blocks) != expected_work_packages:
        errors.append(
            "work package headings must be "
            f"{WORK_PACKAGE_RANGE_TEXT} exactly, found {sorted(blocks)}"
        )

    statuses: dict[str, str] = {}
    required_fields = {
        "primary Issue": r"(?m)^Primary Issue(?: handling)?：",
        "Parent/Related": r"(?mi)^(?:Parent|Related|Child / Related|Existing child / related)",
        "Requirement": r"(?m)^Requirement：",
        "Dependencies": r"(?m)^依赖：",
        "Scope": r"(?m)^主要范围：",
        "Non-goals": r"(?m)^非目标：",
        "Acceptance criteria": r"(?m)^退出门禁：",
        "Verification gates": r"(?m)^验证门禁：",
    }
    for work_package, block in blocks.items():
        matches = re.findall(r"(?m)^状态：([a-z-]+)。\s*$", block)
        if len(matches) != 1:
            errors.append(f"{work_package} must contain exactly one status")
            continue
        statuses[work_package] = matches[0]
        if matches[0] not in ALLOWED_WP_STATUSES:
            errors.append(f"{work_package} uses undefined status {matches[0]}")
        for field, pattern in required_fields.items():
            count = len(re.findall(pattern, block))
            if field == "Parent/Related":
                if count < 1:
                    errors.append(f"{work_package} must contain a {field} field")
            elif count != 1:
                errors.append(f"{work_package} must contain exactly one {field} field")

    current_work_package = fields.get("current_work_package", "")
    current_status = fields.get("current_work_package_status", "")
    if statuses.get(current_work_package) != current_status:
        errors.append("front matter current status does not match the current WP block")
    in_progress = sorted(wp for wp, status in statuses.items() if status == "in-progress")
    expected_in_progress = [current_work_package] if current_status == "in-progress" else []
    if in_progress != expected_in_progress:
        errors.append(
            f"only a current in-progress work package may use that status, found {in_progress}"
        )

    current_block = blocks.get(current_work_package, "")
    issue_matches = re.findall(
        r"(?m)^- Primary Issue：\[#(?P<issue>\d+)\]\(https://github\.com/arana-db/kiwi/issues/(?P=issue)\)。$",
        current_block,
    )
    pr_matches = re.findall(
        r"(?m)^Implementation PR：\[#(?P<pr>\d+)\]\(https://github\.com/arana-db/kiwi/pull/(?P=pr)\)。$",
        current_block,
    )
    if issue_matches != [fields.get("current_issue")]:
        errors.append("current WP primary Issue must occur once and match front matter")
    if pr_matches != [fields.get("current_pr")]:
        errors.append("current WP implementation PR must occur once and match front matter")

    wp0_pr_number = fields.get("wp0_pr_number", "")
    if not re.fullmatch(r"[1-9][0-9]*", wp0_pr_number):
        errors.append("wp0_pr_number must be a positive decimal GitHub PR number")
    validate_wp0_immutable_evidence(fields, errors)
    wp0_pr_matches = re.findall(
        r"(?m)^Implementation PR：\[#(?P<pr>\d+)\]\(https://github\.com/arana-db/kiwi/pull/(?P=pr)\)。$",
        blocks.get("WP0", ""),
    )
    if wp0_pr_matches != [wp0_pr_number]:
        errors.append("WP0 implementation PR must match immutable wp0_pr_number")

    verification_ref = fields.get("wp0_exact_main_verification_ref", "")
    verification_run = fields.get("wp0_exact_main_verification_run", "")
    verification_status = fields.get("wp0_exact_main_verification_status", "")
    if verification_status not in {"pending", "passed"}:
        errors.append(
            "wp0_exact_main_verification_status must be pending or passed"
        )
    if verification_status == "pending":
        if verification_ref != "none" or verification_run != "none":
            errors.append(
                "pending WP0 exact-main verification must not claim a ref or run"
            )
    elif verification_status == "passed":
        if not re.fullmatch(r"[0-9a-f]{40}", verification_ref):
            errors.append(
                "passed WP0 exact-main verification requires a full Git SHA"
            )
        if not re.fullmatch(r"[1-9][0-9]*", verification_run):
            errors.append(
                "passed WP0 exact-main verification requires a GitHub Actions run"
            )

    wp0_block = blocks.get("WP0", "")
    evidence_projection = (
        (
            "- PR 固定区间：",
            f"- PR 固定区间：{fields.get('wp0_pr_base_ref', '')}.."
            f"{fields.get('wp0_pr_head_ref', '')}；",
        ),
        (
            "- merge 固定区间：",
            f"- merge 固定区间：{fields.get('wp0_merge_parent_ref', '')}.."
            f"{fields.get('wp0_merge_ref', '')}；",
        ),
        (
            "- WP0 exact-main verification：",
            f"- WP0 exact-main verification：status={verification_status}，"
            f"ref={verification_ref}，run={verification_run}。",
        ),
    )
    wp0_lines = wp0_block.splitlines()
    for prefix, expected_line in evidence_projection:
        matching_lines = [line for line in wp0_lines if line.startswith(prefix)]
        if matching_lines != [expected_line]:
            errors.append(
                "WP0 evidence projection must contain exactly one line matching "
                f"immutable front matter: expected={expected_line!r}, "
                f"found={matching_lines}"
            )

    if statuses.get("WP0") in {"verified", "accepted", "released"}:
        if verification_status != "passed":
            errors.append(
                f"WP0 status {statuses.get('WP0')} requires passed exact-main "
                "verification evidence"
            )

    table_expectations = {
        "Current work package": current_work_package,
        "Status": current_status,
        "WP0 exact-main verification": verification_status,
    }
    for label, expected in table_expectations.items():
        matches = re.findall(rf"(?m)^\| {re.escape(label)} \| (.*?) \|$", sdd)
        if len(matches) != 1:
            errors.append(f"current-state table field {label} must occur exactly once")
            continue
        visible_value = re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", matches[0])
        if visible_value != expected:
            errors.append(
                f"current-state table field {label} is {visible_value}, expected {expected}"
            )

    current_anchor = f"#{current_work_package.lower()}"
    anchor_matches = re.findall(
        rf'(?m)^<a id="{re.escape(current_anchor[1:])}"></a>$', sdd
    )
    if len(anchor_matches) != 1:
        errors.append(f"current work package anchor {current_anchor} must occur exactly once")

    plan_matches = re.findall(r"(?m)^\| Current plan \| \[[^]]+\]\((#[^)]+)\) \|$", sdd)
    if plan_matches != [current_anchor]:
        errors.append("current-state table plan must link to the current work package anchor")

    issue_table_matches = re.findall(
        r"(?m)^\| Current Issue \| \[#(?P<issue>\d+)\]\(https://github\.com/arana-db/kiwi/issues/(?P=issue)\) \|$",
        sdd,
    )
    if issue_table_matches != [fields.get("current_issue")]:
        errors.append("current-state table Issue label and URL must match front matter")

    pr_table_matches = re.findall(
        r"(?m)^\| Current PR \| \[#(?P<pr>\d+)\]\(https://github\.com/arana-db/kiwi/pull/(?P=pr)\) \|$",
        sdd,
    )
    if pr_table_matches != [fields.get("current_pr")]:
        errors.append("current-state table PR label and URL must match front matter")

    baseline_ref = fields.get("baseline_ref", "")
    baseline_repository = fields.get("baseline_repository", "")
    baseline_branch = fields.get("baseline_branch", "")
    if baseline_repository != GITHUB_REPOSITORY:
        errors.append(
            f"baseline_repository must remain {GITHUB_REPOSITORY}, "
            f"found {baseline_repository}"
        )
    if baseline_branch != EXPECTED_BASELINE_BRANCH:
        errors.append(
            f"baseline_branch must remain {EXPECTED_BASELINE_BRANCH}, "
            f"found {baseline_branch}"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", baseline_ref):
        errors.append("baseline_ref must be a full 40-character lowercase Git SHA")
    for field in WP0_EVIDENCE_FIELDS:
        if not re.fullmatch(r"[0-9a-f]{40}", fields.get(field, "")):
            errors.append(
                f"{field} must be a full 40-character lowercase Git SHA"
            )
    baseline_matches = re.findall(r"(?m)^\| Baseline \| ([^|]+) \|$", sdd)
    if baseline_matches != [f"{baseline_branch}@{baseline_ref}"]:
        errors.append("current-state table Baseline must match front matter")


def validate_wp0_gate_contract(sdd: str, errors: list[str]) -> None:
    wp0 = work_package_blocks(sdd).get("WP0", "")
    exit_match = re.search(r"(?ms)^退出门禁：\s*\n(.*?)(?=^验证门禁：)", wp0)
    verification_match = re.search(r"(?ms)^验证门禁：\s*\n(.*)$", wp0)
    if not exit_match or not verification_match:
        errors.append("WP0 exit/verification gate sections cannot be parsed")
        return

    exit_lines = tuple(re.findall(r"(?m)^- .+$", exit_match.group(1)))
    verification_lines = tuple(
        re.findall(r"(?m)^- .+$", verification_match.group(1))
    )
    if exit_lines != EXPECTED_WP0_EXIT_LINES:
        errors.append(
            "WP0 exit gate contract drifted: "
            f"expected={list(EXPECTED_WP0_EXIT_LINES)}, found={list(exit_lines)}"
        )
    if verification_lines != EXPECTED_WP0_VERIFICATION_LINES:
        errors.append(
            "WP0 verification gate contract drifted: "
            f"expected={list(EXPECTED_WP0_VERIFICATION_LINES)}, "
            f"found={list(verification_lines)}"
        )


def validate_invariants(sdd: str, errors: list[str]) -> int:
    rows = re.findall(r"(?m)^\| `(INV-\d{2})` \|(.+?)\|$", sdd)
    ids = [identifier for identifier, _ in rows]
    expected = [f"INV-{number:02d}" for number in range(1, 21)]
    if ids != expected:
        errors.append(f"invariant closure rows must be INV-01..INV-20, found {ids}")
    for identifier, remainder in rows:
        cells = [cell.strip() for cell in remainder.split("|")]
        if len(cells) != 5 or any(not cell for cell in cells):
            errors.append(f"{identifier} must define Target, Current, Gap, WP, and Acceptance")
        elif re.search(r"\bM\d+\b", cells[3]):
            errors.append(f"{identifier} Work Package cell must not contain a milestone")
    return len(ids)


def git_changed_paths_between(
    root: Path,
    base_ref: str,
    head_ref: str,
    label: str,
    errors: list[str],
) -> set[str]:
    unavailable: list[str] = []
    for role, ref in (("base", base_ref), ("head", head_ref)):
        object_check = subprocess.run(
            ["git", "-C", str(root), "cat-file", "-e", f"{ref}^{{commit}}"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        if object_check.returncode != 0:
            unavailable.append(f"{role}={ref}")
    if unavailable:
        errors.append(
            f"{label} references are not available as Git commits: {unavailable}"
        )
        return set()

    diff = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", base_ref, head_ref, "--"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    whitespace = subprocess.run(
        ["git", "-C", str(root), "diff", "--check", base_ref, head_ref, "--"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if whitespace.returncode != 0:
        details = (whitespace.stdout + whitespace.stderr).strip()
        errors.append(f"{label} diff has whitespace errors: {details}")
    if diff.returncode != 0:
        errors.append(f"unable to compute {label} changed paths")
        return set()
    return {
        path.strip().replace("\\", "/")
        for path in diff.stdout.splitlines()
        if path.strip()
    }


def validate_expected_git_diff(
    root: Path,
    base_ref: str,
    head_ref: str,
    label: str,
    expected: set[str],
    errors: list[str],
) -> None:
    changed = git_changed_paths_between(root, base_ref, head_ref, label, errors)
    if changed != expected:
        errors.append(
            f"{label} changed paths differ from the expected artifact registry: "
            f"missing={sorted(expected - changed)}, "
            f"unexpected={sorted(changed - expected)}"
        )


def validate_wp0_git_evidence(
    root: Path,
    fields: dict[str, str],
    expected: set[str],
    errors: list[str],
) -> None:
    pr_base_ref = fields.get("wp0_pr_base_ref", "")
    pr_head_ref = fields.get("wp0_pr_head_ref", "")
    pr_base_check = subprocess.run(
        ["git", "-C", str(root), "cat-file", "-e", f"{pr_base_ref}^{{commit}}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if pr_base_check.returncode != 0:
        errors.append(f"WP0 PR base ref is not available as a Git commit: {pr_base_ref}")

    pr_head_check = subprocess.run(
        ["git", "-C", str(root), "cat-file", "-e", f"{pr_head_ref}^{{commit}}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if pr_base_check.returncode == 0 and pr_head_check.returncode == 0:
        pr_ancestry = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "merge-base",
                "--is-ancestor",
                pr_base_ref,
                pr_head_ref,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        if pr_ancestry.returncode != 0:
            errors.append("WP0 PR base ref must be an ancestor of the PR head ref")
        validate_expected_git_diff(
            root,
            pr_base_ref,
            pr_head_ref,
            "WP0 PR base-to-head",
            expected,
            errors,
        )

    merge_parent_ref = fields.get("wp0_merge_parent_ref", "")
    merge_ref = fields.get("wp0_merge_ref", "")
    merge_parent_check = subprocess.run(
        ["git", "-C", str(root), "cat-file", "-e", f"{merge_parent_ref}^{{commit}}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if pr_base_check.returncode == 0 and merge_parent_check.returncode == 0:
        merge_ancestry = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "merge-base",
                "--is-ancestor",
                pr_base_ref,
                merge_parent_ref,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        if merge_ancestry.returncode != 0:
            errors.append(
                "WP0 PR base ref must be an ancestor of the squash-merge parent"
            )
    validate_expected_git_diff(
        root,
        merge_parent_ref,
        merge_ref,
        "WP0 merge-parent-to-merge",
        expected,
        errors,
    )

    parents = subprocess.run(
        ["git", "-C", str(root), "rev-list", "--parents", "-n", "1", merge_ref],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    expected_lineage = [merge_ref, merge_parent_ref]
    actual_lineage = parents.stdout.strip().split()
    if parents.returncode != 0 or actual_lineage != expected_lineage:
        errors.append(
            "WP0 squash-merge evidence must bind the merge commit and its parent exactly: "
            f"expected={expected_lineage}, found={actual_lineage}"
        )

    if fields.get("wp0_exact_main_verification_status") == "passed":
        verification_ref = fields.get("wp0_exact_main_verification_ref", "")
        verification_ref_check = subprocess.run(
            ["git", "-C", str(root), "cat-file", "-e", f"{verification_ref}^{{commit}}"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        if verification_ref_check.returncode != 0:
            errors.append(
                "passed WP0 exact-main verification ref is not available as a Git "
                f"commit: {verification_ref}"
            )
        else:
            if verification_ref == merge_ref:
                errors.append(
                    "passed WP0 exact-main verification ref must strictly descend "
                    "from the recorded WP0 merge commit"
                )
            verification_ancestry = subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "merge-base",
                    "--is-ancestor",
                    merge_ref,
                    verification_ref,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,
            )
            if verification_ancestry.returncode != 0:
                errors.append(
                    "passed WP0 exact-main verification ref must descend from the "
                    "recorded WP0 merge commit"
                )
            baseline_ancestry = subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "merge-base",
                    "--is-ancestor",
                    verification_ref,
                    fields.get("baseline_ref", ""),
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,
            )
            if baseline_ancestry.returncode != 0:
                errors.append(
                    "passed WP0 exact-main verification requires baseline_ref to advance "
                    "to the verification ref or a later main commit"
                )

    subject = subprocess.run(
        ["git", "-C", str(root), "show", "-s", "--format=%s", merge_ref],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    expected_pr_marker = f"(#{fields.get('wp0_pr_number', '')})"
    if subject.returncode != 0 or expected_pr_marker not in subject.stdout.strip():
        errors.append(
            "WP0 merge commit subject must identify the implementation PR: "
            f"expected marker={expected_pr_marker}, found={subject.stdout.strip()!r}"
        )


def validate_artifacts(
    root: Path,
    sdd: str,
    fields: dict[str, str],
    errors: list[str],
    check_git_diff: bool,
) -> None:
    expected = set(EXPECTED_WP0_ARTIFACTS)
    wp0 = work_package_blocks(sdd).get("WP0", "")
    scope_match = re.search(
        r"(?ms)^主要范围：\s*\n(.*?)(?=^Primary Issue handling：)", wp0
    )
    if not scope_match:
        errors.append("WP0 artifact scope cannot be parsed")
        scoped_artifacts: list[str] = []
    else:
        scoped_artifacts = re.findall(r"(?m)^- ([A-Za-z0-9._/-]+)[；。]$", scope_match.group(1))
    duplicate_artifacts = duplicate_values(scoped_artifacts)
    if duplicate_artifacts:
        errors.append(f"duplicate WP0 scoped artifacts: {duplicate_artifacts}")
    scoped_set = set(scoped_artifacts)
    if scoped_set != expected:
        errors.append(
            "WP0 SDD scope differs from the expected artifact registry: "
            f"missing={sorted(expected - scoped_set)}, unexpected={sorted(scoped_set - expected)}"
        )

    for relative in EXPECTED_WP0_ARTIFACTS:
        path = root / relative
        if not path.is_file():
            errors.append(f"missing WP0 artifact: {relative}")
        elif path.stat().st_size == 0:
            errors.append(f"empty WP0 artifact: {relative}")
        else:
            text = read_text(path)
            if any(line.rstrip(" \t") != line for line in text.splitlines()):
                errors.append(f"trailing whitespace found in {relative}")

    for relative in (".planning/STATE.md", ".planning/KANBAN.md", ".planning/ROADMAP.md"):
        path = root / relative
        if not path.is_file():
            continue
        text = read_text(path)
        if "SDD.md" not in text:
            errors.append(f"legacy pointer does not link to SDD.md: {relative}")
        if len(text.splitlines()) > 20:
            errors.append(f"legacy pointer must not maintain an independent state copy: {relative}")

    if check_git_diff:
        validate_wp0_git_evidence(root, fields, expected, errors)


def validate_markdown(
    root: Path,
    errors: list[str],
    relative_paths: tuple[str, ...] | None = None,
) -> None:
    if relative_paths is None:
        relative_paths = tuple(
            path for path in EXPECTED_WP0_ARTIFACTS if path.endswith(".md")
        )
    for relative_path in relative_paths:
        path = root / relative_path
        if not path.is_file():
            continue
        text = read_text(path)
        relative = path.relative_to(root).as_posix()
        for fence in ("```", "~~~"):
            count = sum(1 for line in text.splitlines() if line.startswith(fence))
            if count % 2:
                errors.append(f"unpaired {fence} fence in {relative}")
        for destination in MARKDOWN_LINK.findall(text):
            target = destination.strip().strip("<>").split("#", 1)[0]
            if not target or re.match(r"^[a-z][a-z0-9+.-]*:", target, re.IGNORECASE):
                continue
            if target.startswith("/"):
                continue
            resolved = (path.parent / target).resolve()
            if not resolved.exists():
                errors.append(f"broken relative link in {relative}: {destination}")


def validate_governance_terms(root: Path, errors: list[str]) -> None:
    governed_paths = (
        ".github/pull_request_template.md",
        ".planning/REQUIREMENTS.md",
        ".planning/DECISIONS.md",
        ".planning/SDD.md",
        "CONTRIBUTING.md",
        "docs/prd.md",
    )
    for relative in governed_paths:
        path = root / relative
        if not path.is_file():
            continue
        text = read_text(path)
        if re.search(
            r"(?mi)\bPart of\b\s*:?\s*(?:\[#\d+\]\(|#\d+|https://github\.com/)",
            text,
        ):
            errors.append(f"deprecated partial-Issue keyword remains in {relative}")

    milestone_pattern = re.compile(
        r"M0\s*[-–]\s*M6[^\n]*(?:工作包|work packages?)|"
        r"(?:工作包|work packages?)[^\n]*M0\s*[-–]\s*M6",
        re.IGNORECASE,
    )
    for relative in (".planning/SDD.md", ".planning/KANBAN.md", ".planning/DECISIONS.md", "docs/INDEX.md"):
        path = root / relative
        if not path.is_file():
            continue
        text = read_text(path)
        for match in milestone_pattern.finditer(text):
            sentence = match.group(0)
            if (
                WORK_PACKAGE_RANGE_TEXT not in sentence
                and WORK_PACKAGE_RANGE_MARKDOWN not in sentence
            ):
                errors.append(f"milestone/work-package conflation in {relative}: {sentence}")

    sdd = read_text(root / ".planning/SDD.md")
    placeholders = sorted(set(SDD_PLACEHOLDER.findall(sdd)))
    if placeholders:
        errors.append(f"SDD contains unresolved placeholders: {placeholders}")


def validate_planning_ci_github_access(root: Path, errors: list[str]) -> None:
    workflow_path = root / ".github/workflows/ci.yml"
    if not workflow_path.is_file():
        errors.append("missing .github/workflows/ci.yml")
        return
    workflow = read_text(workflow_path)
    match = re.search(
        r"(?ms)^  planning-docs:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        workflow,
    )
    if match is None:
        errors.append("ci workflow must define the planning-docs job")
        return
    job = match.group(0)
    if not re.search(r"(?m)^      actions: read$", job):
        errors.append("planning SDD validation job must grant actions: read")
    if not re.search(r"(?m)^      contents: read$", job):
        errors.append("planning SDD validation job must grant contents: read")
    if not re.search(
        r"(?m)^      GITHUB_TOKEN: \$\{\{ github\.token \}\}$",
        job,
    ):
        errors.append(
            "planning SDD validation job must expose github.token as GITHUB_TOKEN"
        )


def validate(
    root: Path,
    *,
    check_git_diff: bool = True,
    check_markdown: bool = True,
    markdown_paths: tuple[str, ...] | None = None,
    check_github_run: bool = True,
    github_run_loader: GithubRunLoader | None = None,
) -> tuple[list[str], dict[str, object]]:
    errors: list[str] = []
    sdd_path = root / ".planning/SDD.md"
    if not sdd_path.is_file():
        return ["missing .planning/SDD.md"], {}
    sdd = read_text(sdd_path)
    fields = parse_front_matter(sdd, errors)
    if fields.get("authority") != "sole-project-entry":
        errors.append("SDD authority must be sole-project-entry")
    expected_plan = f".planning/SDD.md#{fields.get('current_work_package', '').lower()}"
    if fields.get("current_plan") != expected_plan:
        errors.append(f"current_plan must point to {expected_plan}")
    requirement_count, decision_count = validate_registries(root, sdd, errors)
    validate_current_state(sdd, fields, errors)
    if check_github_run:
        validate_wp0_exact_main_run(fields, errors, github_run_loader)
    validate_wp0_gate_contract(sdd, errors)
    invariant_count = validate_invariants(sdd, errors)
    validate_artifacts(root, sdd, fields, errors, check_git_diff)
    if check_markdown:
        validate_markdown(root, errors, markdown_paths)
    validate_governance_terms(root, errors)
    validate_planning_ci_github_access(root, errors)
    summary: dict[str, object] = {
        "authority": fields.get("authority"),
        "baseline_ref": fields.get("baseline_ref"),
        "requirements": requirement_count,
        "decisions": decision_count,
        "invariants": invariant_count,
        "current": {
            "work_package": fields.get("current_work_package"),
            "status": fields.get("current_work_package_status"),
            "issue": fields.get("current_issue"),
            "pr": fields.get("current_pr"),
        },
        "artifacts": len(EXPECTED_WP0_ARTIFACTS),
        "errors": len(errors),
    }
    return errors, summary


def copy_contract(root: Path, destination: Path) -> None:
    for relative in EXPECTED_WP0_ARTIFACTS:
        source = root / relative
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def replace_once(text: str, pattern: str, replacement: str, label: str) -> str:
    updated, replacements = re.subn(pattern, replacement, text, count=1)
    if replacements != 1:
        raise AssertionError(f"{label} mutation requires exactly one matching line")
    return updated


def set_wp0_state_text(
    sdd: str,
    *,
    work_package_status: str,
    verification_ref: str,
    verification_run: str,
    verification_status: str,
) -> str:
    updated = replace_once(
        sdd,
        r"(?m)^current_work_package_status: [a-z-]+$",
        f"current_work_package_status: {work_package_status}",
        "current WP status",
    )
    for field, value in (
        ("wp0_exact_main_verification_ref", verification_ref),
        ("wp0_exact_main_verification_run", verification_run),
        ("wp0_exact_main_verification_status", verification_status),
    ):
        updated = replace_once(
            updated,
            rf"(?m)^{re.escape(field)}: .+$",
            f"{field}: {value}",
            field,
        )

    wp0_start = updated.find("### WP0：")
    wp1_start = updated.find("### WP1：", wp0_start)
    if wp0_start < 0 or wp1_start < 0:
        raise AssertionError("WP0 state mutation requires WP0 and WP1 headings")
    prefix = updated[:wp0_start]
    wp0 = updated[wp0_start:wp1_start]
    suffix = updated[wp1_start:]
    wp0 = replace_once(
        wp0,
        r"(?m)^状态：[a-z-]+。$",
        f"状态：{work_package_status}。",
        "WP0 block status",
    )
    wp0 = replace_once(
        wp0,
        r"(?m)^- WP0 exact-main verification：.+。$",
        "- WP0 exact-main verification："
        f"status={verification_status}，ref={verification_ref}，run={verification_run}。",
        "WP0 evidence projection",
    )
    updated = prefix + wp0 + suffix
    updated = replace_once(
        updated,
        r"(?m)^\| Status \| [a-z-]+ \|$",
        f"| Status | {work_package_status} |",
        "current-state table status",
    )
    return replace_once(
        updated,
        r"(?m)^\| WP0 exact-main verification \| [a-z-]+ \|$",
        f"| WP0 exact-main verification | {verification_status} |",
        "current-state table exact-main verification",
    )


def promote_wp0_without_exact_main_evidence_text(sdd: str) -> str:
    return set_wp0_state_text(
        sdd,
        work_package_status="verified",
        verification_ref="none",
        verification_run="none",
        verification_status="pending",
    )


def drift_wp0_evidence_projection_text(sdd: str) -> str:
    return replace_once(
        sdd,
        r"(?m)^- WP0 exact-main verification：.+。$",
        "- WP0 exact-main verification：status=invalid，ref="
        f"{EXPECTED_WP0_IMMUTABLE_EVIDENCE['wp0_merge_ref']}，run=invalid。",
        "WP0 evidence projection drift",
    )


def expect_failure(
    root: Path,
    mutation,
    expected_fragment: str,
    *,
    check_markdown: bool = False,
    markdown_paths: tuple[str, ...] | None = None,
) -> None:
    with tempfile.TemporaryDirectory(prefix="kiwi-sdd-test-") as temporary:
        candidate = Path(temporary)
        copy_contract(root, candidate)
        mutation(candidate)
        errors, _ = validate(
            candidate,
            check_git_diff=False,
            check_markdown=check_markdown,
            markdown_paths=markdown_paths,
            check_github_run=False,
        )
        if not any(expected_fragment in error for error in errors):
            raise AssertionError(
                f"mutation did not fail for {expected_fragment!r}; errors={errors}"
            )


def run_self_tests(root: Path) -> None:
    errors, _ = validate(root, check_github_run=False)
    if errors:
        raise AssertionError(f"baseline contract must pass before mutations: {errors}")

    field_errors: list[str] = []
    fields = parse_front_matter(read_text(root / ".planning/SDD.md"), field_errors)
    if field_errors:
        raise AssertionError(f"baseline front matter must parse: {field_errors}")

    stale_errors: list[str] = []
    validate_expected_git_diff(
        root,
        fields["wp0_pr_base_ref"],
        fields["wp0_merge_ref"],
        "stale-baseline regression",
        set(EXPECTED_WP0_ARTIFACTS),
        stale_errors,
    )
    if not any(
        "unexpected=" in error and "src/storage/src/storage.rs" in error
        for error in stale_errors
    ):
        raise AssertionError(
            "stale-baseline regression must expose the concurrently merged source paths: "
            f"{stale_errors}"
        )

    wrong_parent_fields = dict(fields)
    wrong_parent_fields["wp0_merge_parent_ref"] = fields["wp0_pr_base_ref"]
    wrong_parent_errors: list[str] = []
    validate_wp0_git_evidence(
        root,
        wrong_parent_fields,
        set(EXPECTED_WP0_ARTIFACTS),
        wrong_parent_errors,
    )
    if not any("squash-merge evidence" in error for error in wrong_parent_errors):
        raise AssertionError(
            "wrong merge parent must fail the immutable lineage check: "
            f"{wrong_parent_errors}"
        )

    wrong_merge_fields = dict(fields)
    wrong_merge_fields["wp0_merge_ref"] = fields["wp0_merge_parent_ref"]
    wrong_merge_errors: list[str] = []
    validate_wp0_git_evidence(
        root,
        wrong_merge_fields,
        set(EXPECTED_WP0_ARTIFACTS),
        wrong_merge_errors,
    )
    if not any("squash-merge evidence" in error for error in wrong_merge_errors):
        raise AssertionError(
            "wrong merge ref must fail the immutable lineage check: "
            f"{wrong_merge_errors}"
        )

    wrong_base_fields = dict(fields)
    wrong_base_fields["wp0_pr_base_ref"] = "0" * 40
    wrong_base_errors: list[str] = []
    validate_wp0_git_evidence(
        root,
        wrong_base_fields,
        set(EXPECTED_WP0_ARTIFACTS),
        wrong_base_errors,
    )
    if not any("PR base ref is not available" in error for error in wrong_base_errors):
        raise AssertionError(
            "wrong PR base ref must fail even when the PR head object is unavailable: "
            f"{wrong_base_errors}"
        )

    wrong_verification_fields = dict(fields)
    wrong_verification_fields.update(
        {
            "wp0_exact_main_verification_status": "passed",
            "wp0_exact_main_verification_ref": fields["wp0_pr_base_ref"],
            "wp0_exact_main_verification_run": "1",
        }
    )
    wrong_verification_errors: list[str] = []
    validate_wp0_git_evidence(
        root,
        wrong_verification_fields,
        set(EXPECTED_WP0_ARTIFACTS),
        wrong_verification_errors,
    )
    if not any(
        "verification ref must descend" in error
        for error in wrong_verification_errors
    ):
        raise AssertionError(
            "unrelated exact-main verification ref must fail ancestry validation: "
            f"{wrong_verification_errors}"
        )

    equal_verification_fields = dict(fields)
    equal_verification_fields.update(
        {
            "wp0_exact_main_verification_status": "passed",
            "wp0_exact_main_verification_ref": fields["wp0_merge_ref"],
            "wp0_exact_main_verification_run": "1",
        }
    )
    equal_verification_errors: list[str] = []
    validate_wp0_git_evidence(
        root,
        equal_verification_fields,
        set(EXPECTED_WP0_ARTIFACTS),
        equal_verification_errors,
    )
    if not any("strictly descend" in error for error in equal_verification_errors):
        raise AssertionError(
            "the merge commit itself must not satisfy exact-main verification: "
            f"{equal_verification_errors}"
        )

    current_head = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    ).stdout.strip()
    unadvanced_baseline_fields = dict(fields)
    unadvanced_baseline_fields.update(
        {
            "baseline_ref": fields["wp0_merge_ref"],
            "wp0_exact_main_verification_status": "passed",
            "wp0_exact_main_verification_ref": current_head,
            "wp0_exact_main_verification_run": "1",
        }
    )
    unadvanced_baseline_errors: list[str] = []
    validate_wp0_git_evidence(
        root,
        unadvanced_baseline_fields,
        set(EXPECTED_WP0_ARTIFACTS),
        unadvanced_baseline_errors,
    )
    if not any(
        "requires baseline_ref to advance" in error
        for error in unadvanced_baseline_errors
    ):
        raise AssertionError(
            "passed exact-main promotion must explain the required baseline advance: "
            f"{unadvanced_baseline_errors}"
        )

    advanced_baseline_fields = dict(unadvanced_baseline_fields)
    advanced_baseline_fields["baseline_ref"] = current_head
    advanced_baseline_errors: list[str] = []
    validate_wp0_git_evidence(
        root,
        advanced_baseline_fields,
        set(EXPECTED_WP0_ARTIFACTS),
        advanced_baseline_errors,
    )
    if advanced_baseline_errors:
        raise AssertionError(
            "passed exact-main promotion must be reachable after baseline_ref advances: "
            f"{advanced_baseline_errors}"
        )

    valid_run: dict[str, object] = {
        "id": 30750372362,
        "status": "completed",
        "conclusion": "success",
        "event": "push",
        "head_branch": "main",
        "head_sha": "2" * 40,
        "name": "ci",
        "path": ".github/workflows/ci.yml",
    }
    invalid_run_values: dict[str, object] = {
        "id": 30750372363,
        "status": "in_progress",
        "conclusion": "failure",
        "event": "pull_request",
        "head_branch": "codex/fix-sdd-post-merge-validation",
        "head_sha": "3" * 40,
        "name": "Benchmark",
        "path": ".github/workflows/benchmark.yml",
    }
    baseline_sdd = read_text(root / ".planning/SDD.md")
    synthetic_passed_sdd = set_wp0_state_text(
        baseline_sdd,
        work_package_status="verified",
        verification_ref="2" * 40,
        verification_run="30750372362",
        verification_status="passed",
    )
    with tempfile.TemporaryDirectory(prefix="kiwi-sdd-integration-test-") as temporary:
        candidate = Path(temporary)
        copy_contract(root, candidate)
        candidate_sdd = candidate / ".planning/SDD.md"

        tampered_head = "1" * 40
        tampered_head_sdd = replace_once(
            baseline_sdd,
            r"(?m)^wp0_pr_head_ref: [0-9a-f]{40}$",
            f"wp0_pr_head_ref: {tampered_head}",
            "tampered immutable PR head",
        )
        tampered_head_sdd = replace_once(
            tampered_head_sdd,
            r"(?m)^- PR 固定区间：.+；$",
            f"- PR 固定区间：{fields['wp0_pr_base_ref']}..{tampered_head}；",
            "tampered immutable PR range projection",
        )
        candidate_sdd.write_text(tampered_head_sdd, encoding="utf-8")
        tampered_head_errors, _ = validate(
            candidate,
            check_git_diff=False,
            check_markdown=False,
            check_github_run=False,
        )
        if not any(
            "wp0_pr_head_ref must remain" in error
            for error in tampered_head_errors
        ):
            raise AssertionError(
                "the validate entry point must reject a syntactically valid but "
                f"tampered immutable PR head: {tampered_head_errors}"
            )

        candidate_sdd.write_text(synthetic_passed_sdd, encoding="utf-8")
        loaded_runs: list[str] = []

        def load_valid_run(run_id: str) -> dict[str, object]:
            loaded_runs.append(run_id)
            return dict(valid_run)

        valid_run_errors, _ = validate(
            candidate,
            check_git_diff=False,
            check_markdown=False,
            github_run_loader=load_valid_run,
        )
        if loaded_runs != ["30750372362"] or valid_run_errors:
            raise AssertionError(
                "the validate entry point must load and accept matching exact-main "
                f"evidence once: loaded={loaded_runs}, errors={valid_run_errors}"
            )

        for field, invalid_value in invalid_run_values.items():
            invalid_run = dict(valid_run)
            invalid_run[field] = invalid_value
            invalid_run_errors, _ = validate(
                candidate,
                check_git_diff=False,
                check_markdown=False,
                github_run_loader=lambda _run_id, run=invalid_run: run,
            )
            if not any(
                f"run {field} must" in error for error in invalid_run_errors
            ):
                raise AssertionError(
                    "the validate entry point must reject GitHub Actions run "
                    f"{field} mismatch: {invalid_run_errors}"
                )

    evidence_removed_sdd = promote_wp0_without_exact_main_evidence_text(
        synthetic_passed_sdd
    )
    state_regression_lines = (
        "current_work_package_status: verified",
        "wp0_exact_main_verification_ref: none",
        "wp0_exact_main_verification_run: none",
        "wp0_exact_main_verification_status: pending",
        "状态：verified。",
        "| Status | verified |",
        "| WP0 exact-main verification | pending |",
        "- WP0 exact-main verification：status=pending，ref=none，run=none。",
    )
    missing_state_lines = [
        line for line in state_regression_lines if line not in evidence_removed_sdd
    ]
    if evidence_removed_sdd == synthetic_passed_sdd or missing_state_lines:
        raise AssertionError(
            "verified-without-evidence mutation must work from a passed baseline: "
            f"missing={missing_state_lines}"
        )
    drifted_passed_sdd = drift_wp0_evidence_projection_text(synthetic_passed_sdd)
    if (
        drifted_passed_sdd == synthetic_passed_sdd
        or "status=invalid" not in drifted_passed_sdd
    ):
        raise AssertionError(
            "evidence projection drift mutation must work from a passed baseline"
        )

    for relative in (
        ".planning/REQUIREMENTS.md",
        ".planning/DECISIONS.md",
    ):
        expect_failure(
            root,
            lambda candidate, path=relative: (candidate / path).unlink(),
            f"missing WP0 artifact: {relative}",
        )

    expect_failure(
        root,
        lambda candidate: (candidate / ".planning/SDD.md").write_text(
            read_text(candidate / ".planning/SDD.md").replace(
                "- REQ-WORK-001 至 REQ-WORK-007。",
                "- REQ-WORK-001 至 REQ-WORK-007；\n- REQ-UNKNOWN-999。",
                1,
            ),
            encoding="utf-8",
        ),
        "undefined requirement references",
    )

    def duplicate_requirement(candidate: Path) -> None:
        path = candidate / ".planning/REQUIREMENTS.md"
        text = read_text(path)
        definition = REQ_DEFINITION.search(text)
        assert definition is not None
        line = next(line for line in text.splitlines() if definition.group(1) in line)
        path.write_text(text + f"\n{line}\n", encoding="utf-8")

    expect_failure(root, duplicate_requirement, "duplicate requirement definitions")

    def add_deprecated_issue_keyword(candidate: Path) -> None:
        path = candidate / "docs/prd.md"
        path.write_text(
            read_text(path) + "\nPart of: #413\n",
            encoding="utf-8",
        )

    expect_failure(
        root,
        add_deprecated_issue_keyword,
        "deprecated partial-Issue keyword remains",
    )

    with tempfile.TemporaryDirectory(prefix="kiwi-sdd-prose-") as temporary:
        candidate = Path(temporary)
        copy_contract(root, candidate)
        path = candidate / "docs/prd.md"
        path.write_text(
            read_text(path) + "\nPart of the request path remains synchronous.\n",
            encoding="utf-8",
        )
        prose_errors, _ = validate(
            candidate,
            check_git_diff=False,
            check_markdown=False,
        )
        deprecated_errors = [
            error
            for error in prose_errors
            if "deprecated partial-Issue keyword" in error
        ]
        if deprecated_errors:
            raise AssertionError(
                "ordinary prose must not be treated as an Issue relationship: "
                f"{deprecated_errors}"
            )

    def indent_requirement_definition(candidate: Path) -> None:
        path = candidate / ".planning/REQUIREMENTS.md"
        text = read_text(path)
        definition = REQ_DEFINITION.search(text)
        assert definition is not None
        start = definition.start()
        path.write_text(text[:start] + "    " + text[start:], encoding="utf-8")

    expect_failure(root, indent_requirement_definition, "requirement registry must contain")

    def duplicate_decision(candidate: Path) -> None:
        path = candidate / ".planning/DECISIONS.md"
        text = read_text(path)
        heading = DECISION_DEFINITION.search(text)
        assert heading is not None
        path.write_text(text + f"\n## {heading.group(1)}：duplicate\n", encoding="utf-8")

    expect_failure(root, duplicate_decision, "duplicate decision definitions")

    def duplicate_current(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        path.write_text(
            text.replace("current_work_package: WP0", "current_work_package: WP0\ncurrent_work_package: WP1", 1),
            encoding="utf-8",
        )

    expect_failure(root, duplicate_current, "duplicate front matter fields")

    def second_in_progress(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        wp1 = text.index("### WP1：")
        suffix = text[wp1:].replace("状态：proposed。", "状态：in-progress。", 1)
        path.write_text(text[:wp1] + suffix, encoding="utf-8")

    expect_failure(root, second_in_progress, "only a current in-progress work package")

    def duplicate_wp0(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        path.write_text(text + "\n### WP0：duplicate\n\n状态：proposed。\n", encoding="utf-8")

    expect_failure(root, duplicate_wp0, "duplicate work package headings")

    def remove_wp8(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        wp8 = text.index("### WP8：")
        support_tracks = text.index("### 支持轨道", wp8)
        path.write_text(text[:wp8] + text[support_tracks:], encoding="utf-8")

    expect_failure(
        root,
        remove_wp8,
        "work package headings must be WP0-WP8 exactly",
    )

    def add_wp9(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        path.write_text(
            text + "\n### WP9：out-of-range work package\n\n状态：proposed。\n",
            encoding="utf-8",
        )

    expect_failure(
        root,
        add_wp9,
        "work package headings must be WP0-WP8 exactly",
    )

    def remove_wp_field(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        wp2 = text.index("### WP2：")
        suffix = text[wp2:].replace("Requirement：", "Requirements omitted：", 1)
        path.write_text(text[:wp2] + suffix, encoding="utf-8")

    expect_failure(root, remove_wp_field, "WP2 must contain exactly one Requirement field")

    def remove_planning_actions_permission(candidate: Path) -> None:
        path = candidate / ".github/workflows/ci.yml"
        text = read_text(path).replace("      actions: read\n", "", 1)
        path.write_text(text, encoding="utf-8")

    expect_failure(
        root,
        remove_planning_actions_permission,
        "planning SDD validation job must grant actions: read",
    )

    def remove_planning_github_token(candidate: Path) -> None:
        path = candidate / ".github/workflows/ci.yml"
        text = read_text(path).replace(
            "      GITHUB_TOKEN: ${{ github.token }}\n",
            "",
            1,
        )
        path.write_text(text, encoding="utf-8")

    expect_failure(
        root,
        remove_planning_github_token,
        "planning SDD validation job must expose github.token as GITHUB_TOKEN",
    )

    expect_failure(
        root,
        lambda candidate: (candidate / ".planning/KANBAN.md").unlink(),
        "missing WP0 artifact",
    )
    expect_failure(
        root,
        lambda candidate: (candidate / ".planning/SDD.md").write_text(
            read_text(candidate / ".planning/SDD.md") + "\nTODO\n",
            encoding="utf-8",
        ),
        "unresolved placeholders",
    )

    def hide_requirements_in_comment(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        text = text.replace(
            "- REQ-WORK-001 至 REQ-WORK-007。",
            "- 无。\n<!-- REQ-WORK-001 至 REQ-WORK-007 -->",
            1,
        )
        path.write_text(text, encoding="utf-8")

    expect_failure(
        root, hide_requirements_in_comment, "unreferenced requirement definitions"
    )

    def hide_requirements_in_indented_fence(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path).replace(
            "- REQ-WORK-001 至 REQ-WORK-007。",
            "- 无。\n   ~~~text\n   REQ-WORK-001 至 REQ-WORK-007\n   ~~~",
            1,
        )
        path.write_text(text, encoding="utf-8")

    expect_failure(
        root,
        hide_requirements_in_indented_fence,
        "unreferenced requirement definitions",
    )

    def remove_scoped_artifact(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path).replace("- scripts/validate_sdd.py；\n", "", 1)
        path.write_text(text, encoding="utf-8")

    expect_failure(root, remove_scoped_artifact, "WP0 SDD scope differs")

    def add_broken_relative_link(candidate: Path) -> None:
        path = candidate / ".planning/KANBAN.md"
        path.write_text(
            read_text(path) + "\n[broken](missing-contract.md)\n",
            encoding="utf-8",
        )

    expect_failure(
        root,
        add_broken_relative_link,
        "broken relative link in .planning/KANBAN.md",
        check_markdown=True,
        markdown_paths=(".planning/KANBAN.md",),
    )

    def add_unpaired_fence(candidate: Path) -> None:
        path = candidate / ".planning/KANBAN.md"
        path.write_text(read_text(path) + "\n```text\n", encoding="utf-8")

    expect_failure(
        root,
        add_unpaired_fence,
        "unpaired ``` fence in .planning/KANBAN.md",
        check_markdown=True,
        markdown_paths=(".planning/KANBAN.md",),
    )

    def break_baseline(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text, replacements = re.subn(
            r"(?m)^baseline_ref: [0-9a-f]{40}$",
            "baseline_ref: deadbeef",
            read_text(path),
            count=1,
        )
        if replacements != 1:
            raise AssertionError(
                "baseline mutation requires exactly one full baseline_ref SHA"
            )
        path.write_text(text, encoding="utf-8")

    expect_failure(root, break_baseline, "baseline_ref must be a full")

    def break_wp0_evidence_ref(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text, replacements = re.subn(
            r"(?m)^wp0_pr_head_ref: [0-9a-f]{40}$",
            "wp0_pr_head_ref: deadbeef",
            read_text(path),
            count=1,
        )
        if replacements != 1:
            raise AssertionError(
                "WP0 evidence mutation requires exactly one full wp0_pr_head_ref SHA"
            )
        path.write_text(text, encoding="utf-8")

    expect_failure(
        root,
        break_wp0_evidence_ref,
        "wp0_pr_head_ref must be a full",
    )

    def break_wp0_pr_number(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text, replacements = re.subn(
            r"(?m)^wp0_pr_number: [1-9][0-9]*$",
            "wp0_pr_number: 999",
            read_text(path),
            count=1,
        )
        if replacements != 1:
            raise AssertionError(
                "WP0 PR mutation requires exactly one wp0_pr_number field"
            )
        path.write_text(text, encoding="utf-8")

    expect_failure(
        root,
        break_wp0_pr_number,
        "WP0 implementation PR must match immutable wp0_pr_number",
    )

    def break_current_anchor(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path).replace('<a id="wp0"></a>', '<a id="wp-zero"></a>', 1)
        path.write_text(text, encoding="utf-8")

    expect_failure(root, break_current_anchor, "current work package anchor")

    def break_issue_url(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path).replace(
            "| Current Issue | [#413](https://github.com/arana-db/kiwi/issues/413) |",
            "| Current Issue | [#413](https://github.com/arana-db/kiwi/issues/999) |",
            1,
        )
        path.write_text(text, encoding="utf-8")

    expect_failure(root, break_issue_url, "Issue label and URL must match")

    def drift_gate_counts(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        text = text.replace(
            "- 68 个 REQ 和 19 个 Decision 的唯一注册、范围展开和引用全集闭包；",
            "- 69 个 REQ 和 20 个 Decision 的唯一注册、范围展开和引用全集闭包；",
            1,
        )
        path.write_text(text, encoding="utf-8")

    expect_failure(root, drift_gate_counts, "WP0 verification gate contract drifted")

    def drift_artifact_count(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path).replace("和 20 个预期产物", "和 19 个预期产物", 1)
        path.write_text(text, encoding="utf-8")

    expect_failure(root, drift_artifact_count, "WP0 verification gate contract drifted")

    def remove_self_test_gate(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path).replace(
            "- `python scripts/validate_sdd.py --self-test` 的失败路径变异测试；\n",
            "",
            1,
        )
        path.write_text(text, encoding="utf-8")

    expect_failure(root, remove_self_test_gate, "WP0 verification gate contract drifted")

    def remove_live_gate(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path).replace(
            "- live Issue #413、开放 Issue 数量、关键 PR 状态和远端 main 复核；\n",
            "",
            1,
        )
        path.write_text(text, encoding="utf-8")

    expect_failure(root, remove_live_gate, "WP0 verification gate contract drifted")

    def promote_wp0_without_exact_main_evidence(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        path.write_text(
            promote_wp0_without_exact_main_evidence_text(read_text(path)),
            encoding="utf-8",
        )

    expect_failure(
        root,
        promote_wp0_without_exact_main_evidence,
        "WP0 status verified requires passed exact-main verification evidence",
    )

    def drift_wp0_evidence_projection(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        path.write_text(
            drift_wp0_evidence_projection_text(read_text(path)),
            encoding="utf-8",
        )

    expect_failure(
        root,
        drift_wp0_evidence_projection,
        "WP0 evidence projection must contain exactly one line",
    )

    def duplicate_wp0_evidence_projection(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        conflict = (
            "- PR 固定区间："
            "1111111111111111111111111111111111111111.."
            "2222222222222222222222222222222222222222；"
        )
        text, replacements = re.subn(
            r"(?m)^- PR 固定区间：.+；$",
            lambda match: f"{match.group(0)}\n{conflict}",
            read_text(path),
            count=1,
        )
        if replacements != 1:
            raise AssertionError(
                "duplicate projection mutation requires exactly one PR range"
            )
        path.write_text(text, encoding="utf-8")

    expect_failure(
        root,
        duplicate_wp0_evidence_projection,
        "WP0 evidence projection must contain exactly one line",
    )

    print(
        "SDD validator self-tests passed "
        "(32 failure-path mutations, 1 prose guard, "
        "immutable Git/live-run/state regressions, 1 reachable promotion path)"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="repository root (defaults to the script's repository)",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run failure-path mutation tests before validation",
    )
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    if arguments.self_test:
        run_self_tests(root)
        return 0

    errors, summary = validate(root)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
