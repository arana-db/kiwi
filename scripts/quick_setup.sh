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

# Quick setup script for Kiwi development

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}=== Kiwi Quick Setup ===${NC}"
echo ""

# Check if sccache is installed
if command -v sccache &> /dev/null; then
    echo -e "${GREEN}✓ sccache is already installed${NC}"
else
    echo -e "${YELLOW}Installing sccache (this may take a few minutes)...${NC}"
    cargo install sccache --quiet
    echo -e "${GREEN}✓ sccache installed${NC}"
fi

# Configure sccache
CARGO_CONFIG="$HOME/.cargo/config.toml"
mkdir -p "$HOME/.cargo"

if [ -f "$CARGO_CONFIG" ] && grep -q "rustc-wrapper.*sccache" "$CARGO_CONFIG"; then
    echo -e "${GREEN}✓ sccache is already configured${NC}"
else
    echo "" >> "$CARGO_CONFIG"
    echo "[build]" >> "$CARGO_CONFIG"
    echo 'rustc-wrapper = "sccache"' >> "$CARGO_CONFIG"
    echo -e "${GREEN}✓ sccache configured in $CARGO_CONFIG${NC}"
fi

# Check if cargo-watch is installed
if command -v cargo-watch &> /dev/null; then
    echo -e "${GREEN}✓ cargo-watch is already installed${NC}"
else
    echo -e "${YELLOW}Installing cargo-watch...${NC}"
    cargo install cargo-watch --quiet
    echo -e "${GREEN}✓ cargo-watch installed${NC}"
fi

echo ""
echo -e "${GREEN}=== Setup Complete! ===${NC}"
echo ""

# Create setup marker file
touch .kiwi_setup_done

echo "You can now use:"
echo "  ./scripts/dev.sh check  - Quick syntax check"
echo "  ./scripts/dev.sh watch  - Auto-check on file save"
echo "  ./scripts/dev.sh run    - Build and run"
echo ""
echo "Check sccache stats with:"
echo "  sccache --show-stats"
echo ""
