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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

stdout_file="$(mktemp)"
trap 'rm -f "$stdout_file"' EXIT

if ! cargo tree --locked --offline --target all --all-features -i rkyv@0.7.46 >"$stdout_file"; then
  printf '%s\n' 'failed to inspect the locked offline rkyv dependency graph' >&2
  exit 1
fi

if [[ -s "$stdout_file" ]]; then
  printf '%s\n' 'rkyv@0.7.46 is reachable; remove the advisory exception or upgrade the dependency:' >&2
  cat "$stdout_file" >&2
  exit 1
fi

printf '%s\n' 'rkyv@0.7.46 is not reachable in the locked all-targets all-features graph'
