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

# Diagnose build issues

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}=== Kiwi Build Diagnostics ===${NC}"
echo ""

echo "[1] Checking sccache..."
if command -v sccache &> /dev/null; then
    echo -e "${GREEN}✓ sccache is installed${NC}"
    sccache --show-stats
else
    echo -e "${YELLOW}✗ sccache is NOT installed${NC}"
    echo "  Install with: cargo install sccache"
    echo "  Then configure in ~/.cargo/config.toml"
fi

echo ""
echo "[2] Checking CARGO_INCREMENTAL..."
if [ -n "$CARGO_INCREMENTAL" ]; then
    echo -e "${GREEN}✓ CARGO_INCREMENTAL = $CARGO_INCREMENTAL${NC}"
else
    echo -e "${YELLOW}✗ CARGO_INCREMENTAL is not set${NC}"
    echo "  Set with: export CARGO_INCREMENTAL=1"
fi

echo ""
echo "[3] Checking target directory..."
if [ -d "target/debug" ]; then
    echo -e "${GREEN}✓ target/debug exists${NC}"
    ROCKSDB_COUNT=$(ls target/debug/deps/*rocksdb*.rlib 2>/dev/null | wc -l)
    if [ "$ROCKSDB_COUNT" -gt 0 ]; then
        echo -e "${GREEN}✓ Found $ROCKSDB_COUNT librocksdb-sys cache files${NC}"
    else
        echo -e "${YELLOW}✗ No librocksdb-sys cache found${NC}"
    fi
else
    echo -e "${YELLOW}✗ target/debug does not exist${NC}"
fi

echo ""
echo "[4] Checking .cargo/config.toml..."
if [ -f ".cargo/config.toml" ]; then
    echo -e "${GREEN}✓ Local .cargo/config.toml exists${NC}"
    if grep -q "incremental" .cargo/config.toml; then
        echo -e "${GREEN}✓ Incremental compilation is configured${NC}"
    fi
else
    echo -e "${YELLOW}✗ Local .cargo/config.toml not found${NC}"
fi

echo ""
echo "[5] Checking global Cargo config..."
if [ -f "$HOME/.cargo/config.toml" ]; then
    echo -e "${GREEN}✓ Global Cargo config exists${NC}"
    if grep -q "rustc-wrapper.*sccache" "$HOME/.cargo/config.toml"; then
        echo -e "${GREEN}✓ sccache is configured${NC}"
    else
        echo -e "${YELLOW}✗ sccache is not configured${NC}"
    fi
else
    echo -e "${YELLOW}✗ Global Cargo config not found${NC}"
fi

echo ""
echo "[6] Testing incremental compilation..."
echo "Running: cargo build --verbose 2>&1 | grep -E 'Fresh|Compiling' | grep -E 'librocksdb|openssl|ring'"
cargo build --verbose 2>&1 | grep -E 'Fresh|Compiling' | grep -E 'librocksdb|openssl|ring' | head -10

echo ""
echo -e "${CYAN}=== Recommendations ===${NC}"
echo ""
echo "To speed up builds:"
echo "1. Install sccache: cargo install sccache"
echo "2. Run: ./scripts/setup_sccache.sh"
echo "3. Use cargo check for development: ./scripts/dev.sh check"
echo "4. Use watch mode: ./scripts/dev.sh watch"
echo ""
