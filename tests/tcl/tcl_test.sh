#!/bin/bash

# This script runs a suite of tests for the Tcl programming language.

# check params
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 [test_name]"
    echo "If test_name is provided, only that test will be run."
    exit 1
fi

dirname=$(dirname "$0")

cd "$dirname" || {
  echo "Failed to change directory to $dirname"
  exit 1
}

KIWI_BIN="../../target/release/kiwi"

if [ ! -f "$KIWI_BIN" ]; then
    echo "kiwi binary not found at $KIWI_BIN. Please build kiwi before running the tests."
    exit 1
fi

# Check if tclsh is installed
which tclsh >/dev/null 2>&1

if [ $? -ne 0 ]; then
    echo "tclsh not found. Please install Tcl to run the tests."
    exit 1
fi


cleanup() {
    echo "Cleaning up test environment..."
    rm -fr tests/db
    rm -fr tests/tmp/server*
    rm -fr tests/tmp/kiwi*
}

case $1 in
  clean)
    cleanup
    exit 0
    ;;
  all)
    test_name="all"
    ;;
  *)
    test_name="--single unit/$1"
    ;;
esac

echo "Running kiwi Tcl tests..."

tclsh test_helper.tcl --clients 1 ${test_name}

if [ $? -ne 0 ]; then
    echo "kiwi tests failed"
    exit 1
fi

echo "kiwi Tcl tests successful !!!!"
