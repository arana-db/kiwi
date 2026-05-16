#!/bin/bash
# PostToolUse hook: quick cargo check after file changes
# Only runs for .rs files to avoid unnecessary checks

set -euo pipefail

# Get the list of changed files from git (modified + untracked)
CHANGED_FILES=$(git ls-files --modified --others --exclude-standard '*.rs' 2>/dev/null || true)

# Check if any .rs files were changed
if [ -n "$CHANGED_FILES" ]; then
    echo "Running cargo check..."
    cargo check 2>&1 | tail -5
fi
