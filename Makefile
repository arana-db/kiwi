SHELL := bash
.DELETE_ON_ERROR:
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --no-print-directory

build:
	@echo "Building project..."
	@cargo build

run:
	@echo "Running project..."
	@cargo run --bin server

test:
	@echo "Running tests..."
	@cargo test

clean:
	@echo "Cleaning project..."
	@cargo clean

fmt:
	@echo "Formatting code..."
	@cargo fmt --manifest-path ./Cargo.toml --all -- --check --unstable-features

lint:
	@echo "Linting code..."
	@cargo clippy --manifest-path ./Cargo.toml --all-features --workspace -- -D warnings

miri:
	@echo "Running miri..."
	@cargo miri test --manifest-path ./Cargo.toml --all-features --workspace

help:
	@echo "Available commands:"
	@echo "  build         - Build the project"
	@echo "  run           - Run the project"
	@echo "  test          - Run tests"
	@echo "  clean         - Clean the project"
	@echo "  fmt           - Format the code"
	@echo "  lint          - Lint the code"
	@echo "  help          - Show this help message"

.PHONY: build run test clean fmt lint help
