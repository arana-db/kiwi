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

DEV := ./scripts/dev.sh
NODES ?= 3

check:
	@$(DEV) check

build:
	@$(DEV) build

release:
	@$(DEV) build --release

run:
	@$(DEV) run

test:
	@$(DEV) test

clean:
	@$(DEV) clean

fmt:
	@cargo fmt --manifest-path ./Cargo.toml --all

fmt-check:
	@cargo fmt --manifest-path ./Cargo.toml --all -- --check

lint:
	@cargo clippy --manifest-path ./Cargo.toml --all-features --workspace -- -D warnings -D clippy::unwrap_used

standalone:
	@$(DEV) build
	@$(DEV) run

cluster:
	@./scripts/cluster.sh $(NODES)

help:
	@echo "Available commands:"
	@echo "  check         - Quick syntax check (fast)"
	@echo "  build         - Build (debug, auto-uses sccache)"
	@echo "  release       - Build (release, auto-uses sccache)"
	@echo "  run           - Build and run"
	@echo "  standalone    - Build and run a single node"
	@echo "  cluster       - Start a local multi-node Raft cluster"
	@echo "  test          - Run tests"
	@echo "  clean         - Clean build artifacts"
	@echo "  fmt           - Format code"
	@echo "  fmt-check     - Check formatting (CI)"
	@echo "  lint          - Run clippy (CI)"
	@echo "  help          - Show this help"

.PHONY: check build release run standalone cluster test clean fmt fmt-check lint help
