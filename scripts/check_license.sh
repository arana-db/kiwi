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

# Check license headers in scripts

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "Checking license headers in scripts..."
echo ""

FAILED=0

for file in scripts/*.sh scripts/*.cmd scripts/*.ps1 .cargo/config.toml; do
    if [ -f "$file" ]; then
        if head -n 20 "$file" | grep -q "Apache License, Version 2.0"; then
            echo -e "${GREEN}✓${NC} $file"
        else
            echo -e "${RED}✗${NC} $file - Missing or invalid license header"
            FAILED=1
        fi
    fi
done

echo ""
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All files have valid license headers!${NC}"
    exit 0
else
    echo -e "${RED}Some files are missing license headers!${NC}"
    exit 1
fi
