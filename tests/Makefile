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

# Kiwi 测试 Makefile
#
# 提供标准化的测试运行命令
#

# Python 测试配置
PYTHON := python3
PIP := pip3
PYTEST := pytest
PYTHON_TEST_DIR := python
INTEGRATION_TEST_DIR := integration

# 默认目标
.PHONY: help
help:
	@echo "Kiwi 测试命令"
	@echo "================"
	@echo "make test              - 运行所有测试"
	@echo "make test-unit         - 运行 Rust 单元测试"
	@echo "make test-python       - 运行 Python 集成测试"
	@echo "make test-integration  - 运行集成测试"
	@echo "make install-deps      - 安装 Python 测试依赖"
	@echo "make clean             - 清理测试数据"
	@echo "make help              - 显示此帮助信息"

# 运行所有测试
.PHONY: test
test: test-unit test-python

# 运行 Rust 单元测试
.PHONY: test-unit
test-unit:
	@echo "运行 Rust 单元测试..."
	cargo test

# 运行 Python 集成测试
.PHONY: test-python
test-python:
	@echo "运行 Python 集成测试..."
	$(PYTEST) $(PYTHON_TEST_DIR)/ -v

# 运行特定的 MSET 测试
.PHONY: test-mset
test-mset:
	@echo "运行 MSET 测试..."
	$(PYTEST) $(PYTHON_TEST_DIR)/test_mset.py -v

# 安装 Python 依赖
.PHONY: install-deps
install-deps:
	@echo "安装 Python 测试依赖..."
	$(PIP) install -r $(PYTHON_TEST_DIR)/requirements.txt

# 运行集成测试文档中的测试
.PHONY: test-integration
test-integration:
	@echo "运行集成测试..."
	@echo "请参考 $(INTEGRATION_TEST_DIR)/ 中的文档手动执行测试"

# 清理测试数据
.PHONY: clean
clean:
	@echo "清理测试数据..."
	# 清理 Python 缓存
	rm -rf $(PYTHON_TEST_DIR)/__pycache__
	rm -rf $(PYTHON_TEST_DIR)/*.pyc
	# 清理 pytest 缓存
	rm -rf .pytest_cache
	@echo "测试数据清理完成"

# 显示测试覆盖率
.PHONY: coverage
coverage:
	@echo "生成测试覆盖率报告..."
	$(PYTEST) $(PYTHON_TEST_DIR)/ --cov=. --cov-report=html --cov-report=term

# 运行基准测试
.PHONY: benchmark
benchmark:
	@echo "运行性能基准测试..."
	$(PYTEST) $(PYTHON_TEST_DIR)/ -v -m "benchmark"

# 运行快速测试（排除慢速和基准测试）
.PHONY: test-fast
test-fast:
	@echo "运行快速测试..."
	$(PYTEST) $(PYTHON_TEST_DIR)/ -v -m "not slow and not benchmark"

.DEFAULT_GOAL := help