#!/usr/bin/env bash
# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

set -euo pipefail

if [[ "${OSTYPE:-}" != linux* ]]; then
    printf '%s\n' 'Redis Oracle build requires Linux.' >&2
    exit 1
fi

script_path=${BASH_SOURCE[0]}
if [[ $script_path != /* ]]; then
    script_path=$PWD/$script_path
fi
script_directory=${script_path%/*}
if ! cd -P -- "$script_directory"; then
    printf '%s\n' 'Unable to resolve the Oracle controller directory.' >&2
    exit 1
fi
script_directory=$PWD
controller_path=$script_directory/oracle_controller.py

python_path=
for candidate in \
    /home/alex/miniconda3/bin/python3 \
    /usr/bin/python3 \
    /bin/python3
do
    if [[ -f $candidate && -x $candidate ]]; then
        python_path=$candidate
        break
    fi
done
if [[ -z $python_path ]]; then
    printf '%s\n' 'No controlled absolute Python interpreter is available.' >&2
    exit 1
fi
if [[ ! -f $controller_path ]]; then
    printf 'Oracle controller is missing: %s\n' "$controller_path" >&2
    exit 1
fi

exec {python_fd}<"$python_path"
exec {controller_fd}<"$controller_path"

exec "/proc/self/fd/$python_fd" -I -B "/proc/self/fd/$controller_fd" \
    --bootstrap-python-path "$python_path" \
    --bootstrap-python-fd "$python_fd" \
    --bootstrap-controller-path "$controller_path" \
    --bootstrap-controller-fd "$controller_fd" \
    "$@"
