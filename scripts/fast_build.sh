#!/usr/bin/env bash
# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Fast build script for Kiwi

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

# Parse arguments
RELEASE=false
CLEAN=false
CHECK=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --release)
            RELEASE=true
            shift
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --check)
            CHECK=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${CYAN}=== Kiwi Fast Build Script ===${NC}"

# Clean if requested
if [ "$CLEAN" = true ]; then
    echo -e "${YELLOW}Cleaning build artifacts...${NC}"
    cargo clean
    echo -e "${GREEN}✓ Clean complete!${NC}"
    exit 0
fi

# Set build profile
if [ "$RELEASE" = true ]; then
    PROFILE="release"
    PROFILE_FLAG="--release"
else
    PROFILE="dev"
    PROFILE_FLAG=""
fi

echo -e "${YELLOW}Building in $PROFILE mode...${NC}"

# Check if librocksdb-sys is already compiled
TARGET_DIR="target/$PROFILE"
if ls $TARGET_DIR/deps/*rocksdb*.rlib 1> /dev/null 2>&1; then
    echo -e "${GREEN}✓ librocksdb-sys already compiled, using cache${NC}"
else
    echo -e "${YELLOW}⚠ librocksdb-sys not found in cache, will compile...${NC}"
fi

# Set environment variables for faster compilation
export CARGO_BUILD_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Check if sccache is available
if command -v sccache &> /dev/null; then
    export RUSTC_WRAPPER=sccache
    # sccache doesn't support incremental compilation
    export CARGO_INCREMENTAL=0
else
    # Enable incremental compilation when not using sccache
    export CARGO_INCREMENTAL=1
fi

# Run build or check
START_TIME=$(date +%s)

if [ "$CHECK" = true ]; then
    echo -e "${CYAN}Running cargo check...${NC}"
    cargo check $PROFILE_FLAG
else
    echo -e "${CYAN}Running cargo build...${NC}"
    cargo build $PROFILE_FLAG
fi

END_TIME=$(date +%s)
BUILD_TIME=$((END_TIME - START_TIME))

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Build successful!${NC}"
    echo -e "${CYAN}Build completed in ${BUILD_TIME} seconds${NC}"
else
    echo ""
    echo -e "${RED}✗ Build failed!${NC}"
    exit 1
fi
