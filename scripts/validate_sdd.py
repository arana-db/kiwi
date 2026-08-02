#!/usr/bin/env python3
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
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


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

EXPECTED_REQUIREMENT_COUNT = 63
EXPECTED_DECISION_COUNT = 18

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
    for field in CURRENT_FIELDS:
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
        r"(?ms)^Deferred Requirement：\s*\n(.*?)(?=^WP0-WP7 的 Requirement 字段)",
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
    requirements = strip_markdown_noncontract(
        read_text(root / ".planning/REQUIREMENTS.md")
    )
    decisions = strip_markdown_noncontract(read_text(root / ".planning/DECISIONS.md"))

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


def validate_current_state(sdd: str, fields: dict[str, str], errors: list[str]) -> None:
    work_package_ids = WP_HEADING.findall(sdd)
    duplicate_work_packages = duplicate_values(work_package_ids)
    if duplicate_work_packages:
        errors.append(f"duplicate work package headings: {duplicate_work_packages}")
    blocks = work_package_blocks(sdd)
    expected_work_packages = {f"WP{number}" for number in range(8)}
    if set(blocks) != expected_work_packages:
        errors.append(
            f"work package headings must be WP0-WP7 exactly, found {sorted(blocks)}"
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

    table_expectations = {
        "Current work package": current_work_package,
        "Status": current_status,
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
    baseline_branch = fields.get("baseline_branch", "")
    if not re.fullmatch(r"[0-9a-f]{40}", baseline_ref):
        errors.append("baseline_ref must be a full 40-character lowercase Git SHA")
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


def git_changed_paths(root: Path, baseline_ref: str, errors: list[str]) -> set[str]:
    object_check = subprocess.run(
        ["git", "-C", str(root), "cat-file", "-e", f"{baseline_ref}^{{commit}}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if object_check.returncode != 0:
        errors.append(f"baseline_ref is not available as a Git commit: {baseline_ref}")
        return set()

    diff = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", baseline_ref, "--"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    whitespace = subprocess.run(
        ["git", "-C", str(root), "diff", "--check", baseline_ref, "--"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    untracked = subprocess.run(
        ["git", "-C", str(root), "ls-files", "--others", "--exclude-standard"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if whitespace.returncode != 0:
        details = (whitespace.stdout + whitespace.stderr).strip()
        errors.append(f"WP0 committed/working diff has whitespace errors: {details}")
    if diff.returncode != 0 or untracked.returncode != 0:
        errors.append("unable to compute WP0 changed paths from baseline_ref")
        return set()
    return {
        path.strip().replace("\\", "/")
        for path in (diff.stdout + "\n" + untracked.stdout).splitlines()
        if path.strip()
    }


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

    if check_git_diff and fields.get("current_work_package") == "WP0":
        changed = git_changed_paths(root, fields.get("baseline_ref", ""), errors)
        if changed != expected:
            errors.append(
                "WP0 changed paths differ from the expected artifact registry: "
                f"missing={sorted(expected - changed)}, unexpected={sorted(changed - expected)}"
            )


def validate_markdown(root: Path, errors: list[str]) -> None:
    markdown_paths = [root / path for path in EXPECTED_WP0_ARTIFACTS if path.endswith(".md")]
    for path in markdown_paths:
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
        if "Part of" in text:
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
            if "WP0-WP7" not in sentence and "`WP0`–`WP7`" not in sentence:
                errors.append(f"milestone/work-package conflation in {relative}: {sentence}")

    sdd = read_text(root / ".planning/SDD.md")
    placeholders = sorted(set(SDD_PLACEHOLDER.findall(sdd)))
    if placeholders:
        errors.append(f"SDD contains unresolved placeholders: {placeholders}")


def validate(
    root: Path,
    *,
    check_git_diff: bool = True,
    check_markdown: bool = True,
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
    validate_wp0_gate_contract(sdd, errors)
    invariant_count = validate_invariants(sdd, errors)
    validate_artifacts(root, sdd, fields, errors, check_git_diff)
    if check_markdown:
        validate_markdown(root, errors)
    validate_governance_terms(root, errors)
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


def expect_failure(root: Path, mutation, expected_fragment: str) -> None:
    with tempfile.TemporaryDirectory(prefix="kiwi-sdd-test-") as temporary:
        candidate = Path(temporary)
        copy_contract(root, candidate)
        mutation(candidate)
        errors, _ = validate(candidate, check_git_diff=False, check_markdown=False)
        if not any(expected_fragment in error for error in errors):
            raise AssertionError(
                f"mutation did not fail for {expected_fragment!r}; errors={errors}"
            )


def run_self_tests(root: Path) -> None:
    errors, _ = validate(root)
    if errors:
        raise AssertionError(f"baseline contract must pass before mutations: {errors}")

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

    def remove_wp_field(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        wp2 = text.index("### WP2：")
        suffix = text[wp2:].replace("Requirement：", "Requirements omitted：", 1)
        path.write_text(text[:wp2] + suffix, encoding="utf-8")

    expect_failure(root, remove_wp_field, "WP2 must contain exactly one Requirement field")
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

    def break_baseline(candidate: Path) -> None:
        path = candidate / ".planning/SDD.md"
        text = read_text(path).replace(
            "baseline_ref: 0c4795ec716299598686fc7c5e0fac03a30e044d",
            "baseline_ref: deadbeef",
            1,
        )
        path.write_text(text, encoding="utf-8")

    expect_failure(root, break_baseline, "baseline_ref must be a full")

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
            "- 63 个 REQ 和 18 个 Decision 的唯一注册、范围展开和引用全集闭包；",
            "- 64 个 REQ 和 19 个 Decision 的唯一注册、范围展开和引用全集闭包；",
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

    with tempfile.TemporaryDirectory(prefix="kiwi-sdd-lifecycle-") as temporary:
        candidate = Path(temporary)
        copy_contract(root, candidate)
        path = candidate / ".planning/SDD.md"
        text = read_text(path)
        text = text.replace("current_work_package_status: in-progress", "current_work_package_status: implemented", 1)
        wp0 = text.index("### WP0：")
        prefix, suffix = text[:wp0], text[wp0:]
        suffix = suffix.replace("状态：in-progress。", "状态：implemented。", 1)
        suffix = suffix.replace("| Status | in-progress |", "| Status | implemented |", 1)
        path.write_text(prefix + suffix, encoding="utf-8")
        lifecycle_errors, _ = validate(
            candidate, check_git_diff=False, check_markdown=False
        )
        if lifecycle_errors:
            raise AssertionError(f"implemented lifecycle state must remain valid: {lifecycle_errors}")

    print("SDD validator self-tests passed (20 failure-path mutations, 1 lifecycle transition)")


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
