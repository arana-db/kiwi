#!/bin/bash

# Usage: ./unit_test.sh [mod_name]
# Example: ./unit_test.sh storage
#         ./unit_test.sh (runs all tests)

if [ $# -eq 0 ]; then
    echo "Running all tests..."
    # Run all tests
    cargo test -- --nocapture
else
    MOD_NAME=$1
    echo "Running tests for module: $MOD_NAME"
    # Run tests for the specified module
    cargo test --test '*' "$MOD_NAME::" -- --nocapture
fi

# Check test results
if [ $? -eq 0 ]; then
    echo "✅ Tests passed"
else
    echo "❌ Tests failed"
    exit 1
fi