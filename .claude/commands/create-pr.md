---
name: create-pr
description: Rebase from the latest origin/main, squash commits, and create a PR on GitHub with intelligent commit messages. Invoke with /create-pr.
---

# Create Pull Request

Rebase from the latest `origin/main`, squash commits, and create a PR on GitHub.

## Usage

```text
/create-pr [--draft] [--base <branch>]
```

## Workflow

### Step 1: Verify Prerequisites

```bash
git branch --show-current
git status --short
gh --version
```

If on `main`/`master`, abort. If uncommitted changes, ask user to commit/stash first.

### Step 2: Check for Existing PR

```bash
gh pr view --json number,title,url 2>/dev/null || echo "No existing PR"
```

If PR exists, inform user and ask permission to force-update.

### Step 3: Fetch and Rebase

```bash
git fetch origin main
git rebase origin/main
```

If rebase fails: `git rebase --abort`, inform user to resolve manually.

### Step 4: Squash Commits

```bash
git rev-list --count origin/main..HEAD
git reset --soft origin/main
```

Analyze staged changes and generate commit message following `commit-conventions` skill.

### Step 5: Analyze and Generate PR

```bash
git diff origin/main...HEAD --name-only
git diff origin/main...HEAD
```

Generate PR title and description. PR title must match:
```text
^(feat|fix|test|refactor|chore|upgrade|bump|style|docs|perf|build|ci|revert)(\(.*\))?:.*
```

### Step 6: Preview and Confirm

Show preview to user:
```text
Branch: feat/new-command -> main
Title: feat(cmd): add MGET command
```

### Step 7: Push and Create

```bash
git push -f -u origin <branch>
gh pr create --base main --title "..." --body "..."
```

For fork workflow: detect fork remote, push to fork, create cross-repo PR.
