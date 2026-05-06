---
name: review-pr
description: Intelligent PR code review with dynamic agent allocation. Invoke with /review-pr.
---

# Review Pull Request

Perform an intelligent code review of a PR, dynamically consulting domain experts.

## Usage

```text
/review-pr [<pr-number-or-url>]
```

If no PR specified, reviews the current branch's PR.

## Workflow

### Step 1: Fetch PR Information

```bash
# If provided, use the specified PR; otherwise default to current branch
PR_ARG="${PR_NUMBER_OR_URL:+$PR_NUMBER_OR_URL}"
gh pr view ${PR_ARG} --json title,body,files,additions,deletions
gh pr diff ${PR_ARG}
```

### Step 2: Identify Changed Crates

Categorize changed files by crate:

| File Pattern | Crate | Expert |
|--------------|-------|--------|
| `src/storage/` | storage | `storage-expert` |
| `src/raft/` | raft | `raft-expert` |
| `src/cmd/` | cmd | `cmd-expert` |
| `src/net/`, `src/resp/`, `src/executor/` | net | `net-expert` |
| `src/common/runtime/` | runtime | `runtime-expert` |
| `src/engine/` | engine | `storage-expert` |

### Step 3: Run Automated Checks

```bash
cargo fmt --all -- --check
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
cargo test
```

### Step 4: Review Each Area

For each changed crate, consult the relevant expert agent for domain-specific review.

### Step 5: Generate Review

```markdown
## PR Review: <title>

### Summary
[1-2 sentence overview]

### Automated Checks
| Check | Status |
|-------|--------|
| rustfmt | [PASS/FAIL] |
| clippy | [PASS/FAIL] |
| tests | [PASS/FAIL/SKIP] |

### Issues Found
1. **[Issue]** - `file.rs:123`
   - Severity: [critical/suggestion]
   - [description and fix]

### Positive Observations
- [what looks good]

### Verdict
[APPROVE / REQUEST_CHANGES / COMMENT]
```
