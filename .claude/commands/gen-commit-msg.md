---
name: gen-commit-msg
description: Generate intelligent commit messages based on staged changes. Invoke with /gen-commit-msg.
---

# Generate Commit Message

Generate a well-formatted commit message based on staged changes.

## Usage

```text
/gen-commit-msg [--amend] [--scope <scope>]
```

- `--amend`: Amend the last commit instead of creating a new one
- `--scope <scope>`: Override the auto-detected scope

## Workflow

### Step 1: Analyze Changes

```bash
git diff --cached --name-only
git diff --cached
git log --oneline -5
```

### Step 2: Categorize Changes

| Type | When to Use |
|------|-------------|
| `feat` | New feature or capability |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `test` | Adding or fixing tests |
| `refactor` | Code change without feature/fix |
| `perf` | Performance improvement |
| `style` | Formatting/style-only changes |
| `build` | Build system or dependencies |
| `ci` | CI pipeline or workflow changes |
| `chore` | Build, deps, config changes |
| `revert` | Revert a previous commit |
| `upgrade` | Dependency upgrade |
| `bump` | Version bump |

### Step 3: Determine Scope

If `--scope <scope>` is provided, use it verbatim. Otherwise, infer scope from changed files:

- `src/storage/` -> `storage`
- `src/raft/` -> `raft`
- `src/cmd/` -> `cmd`
- `src/net/` -> `net`
- `src/resp/` -> `resp`
- `src/executor/` -> `executor`
- `src/engine/` -> `engine`
- `src/common/runtime/` -> `runtime`
- `src/conf/` -> `conf`
- `docs/` -> `docs`
- Multiple areas -> omit scope or use broader term

### Step 4: Generate Message

**Format:**
```text
<type>(<scope>): <subject>

<body>

[Optional sections]
Key changes:
- change 1
- change 2
```

**Rules:**
- Subject: imperative mood, ~50-72 chars, no period
- Body: explain "why" not "what", wrap at 72 chars

### Step 5: Confirm and Commit

Show preview and ask user to confirm, then execute:

Without `--amend`:
```bash
git commit -m "$(cat <<'EOF'
<message>
EOF
)"
```

With `--amend`:
```bash
git commit --amend -m "$(cat <<'EOF'
<message>
EOF
)"
```
