:: Copyright (c) 2024-present, arana-db Community.  All rights reserved.
::
:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::     http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

@echo off
REM 运行新增测试的快速脚本（Windows 版本）

echo ==========================================
echo 运行新增测试套件
echo ==========================================
echo.

REM 检查 Python 依赖
echo 📦 检查 Python 依赖...
python -c "import redis, pytest" 2>nul
if errorlevel 1 (
    echo ⚠️  缺少依赖，正在安装...
    pip install redis pytest pytest-timeout
)
echo ✅ Python 依赖已就绪
echo.

REM 检查服务器是否运行
echo 🔍 检查 Kiwi 服务器...
netstat -an | findstr ":6379" >nul
if errorlevel 1 (
    echo ❌ Kiwi 服务器未运行在 localhost:6379
    echo 请先启动服务器: cargo run --bin server --release
    exit /b 1
)
echo ✅ Kiwi 服务器正在运行
echo.

REM 运行 WRONGTYPE 错误测试
echo ==========================================
echo 1️⃣  运行 WRONGTYPE 错误测试
echo ==========================================
pytest tests/python/test_wrongtype_errors.py -v --tb=short
echo.

REM 运行 MSET 并发测试（排除慢速测试）
echo ==========================================
echo 2️⃣  运行 MSET 并发测试（快速）
echo ==========================================
pytest tests/python/test_mset_concurrent.py -v --tb=short -m "not slow"
echo.

REM 运行 Raft 网络分区测试
echo ==========================================
echo 3️⃣  运行 Raft 网络分区测试
echo ==========================================
cargo test --test raft_network_partition_tests test_network_simulator
echo.

REM 总结
echo ==========================================
echo ✅ 所有新增测试运行完成！
echo ==========================================
echo.
echo 📊 测试统计:
echo   - WRONGTYPE 错误测试: 10 个用例
echo   - MSET 并发测试: 6 个用例（快速）
echo   - Raft 网络分区测试: 1 个用例
echo.
echo 💡 提示:
echo   - 运行慢速测试: pytest tests/python/test_mset_concurrent.py -v -m slow
echo   - 运行所有并发测试: pytest tests/python/test_mset_concurrent.py -v
echo   - 查看详细输出: pytest tests/python/test_*.py -v -s
echo.
