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

release:
	@echo "Building project in release mode..."
	@cargo build --release

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
	@cargo fmt --manifest-path ./Cargo.toml --all

fmt-check:
	@echo "Formatting code..."
	@cargo fmt --manifest-path ./Cargo.toml --all -- --check

lint:
	@echo "Linting code..."
	@cargo clippy --manifest-path ./Cargo.toml --all-features --workspace -- -D warnings

help:
	@echo "Available commands:"
	@echo "  build         - Build the project"
	@echo "  run           - Run the project"
	@echo "  test          - Run tests"
	@echo "  clean         - Clean the project"
	@echo "  fmt           - Format the code"
	@echo "  lint          - Lint the code"
	@echo "  help          - Show this help message"

.PHONY: build release run test clean fmt lint help
