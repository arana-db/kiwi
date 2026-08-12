"""
Copyright (c) 2024-present, arana-db Community.  All rights reserved.

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

"""
pytest 配置文件

提供测试夹具（fixtures）和通用配置
"""

import json
import os
import socket
from pathlib import Path

import pytest
import redis


def _enabled(name):
    """Return whether a CI-only test mode is explicitly enabled."""
    return os.environ.get(name) == "1"


def _required_vector_mode():
    return _enabled("KIWI_COMPAT_REQUIRE_ORACLE")


@pytest.fixture(scope="session", autouse=True)
def required_vector_endpoints():
    """Probe both required endpoints once before any Vector item can run."""
    if not _required_vector_mode():
        yield
        return

    required = (
        "KIWI_HOST",
        "KIWI_PORT",
        "KIWI_REDIS_ORACLE_HOST",
        "KIWI_REDIS_ORACLE_PORT",
        "KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.fail(
            f"required Vector Oracle identity is missing: {', '.join(missing)}",
            pytrace=False,
        )
    if os.environ["KIWI_REDIS_ORACLE_RUNTIME_EVIDENCE"] != "/runtime-evidence.json":
        pytest.fail("required Vector runtime evidence identity mismatch", pytrace=False)
    endpoints = (
        (os.environ.get("KIWI_HOST", "127.0.0.1"), int(os.environ["KIWI_PORT"])),
        (os.environ["KIWI_REDIS_ORACLE_HOST"], int(os.environ["KIWI_REDIS_ORACLE_PORT"])),
    )
    for host, port in endpoints:
        try:
            with socket.create_connection((host, port), timeout=0.5) as connection:
                connection.sendall(b"*1\r\n$4\r\nPING\r\n")
                if connection.recv(64) != b"+PONG\r\n":
                    raise OSError("PING did not return +PONG")
        except OSError as error:
            pytest.fail(
                f"required Vector endpoint unavailable at {host}:{port}: {error}",
                pytrace=False,
            )
    yield


@pytest.fixture(scope="session")
def redis_client():
    """
    创建 Redis 客户端连接
    
    作用域为 session，所有测试共享一个连接
    """
    client = redis.Redis(
        host=os.getenv("KIWI_HOST", "localhost"),
        port=int(os.getenv("KIWI_PORT", "7379")),
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    
    # 测试连接
    try:
        client.ping()
    except redis.RedisError as error:
        client.close()
        message = (
            "Redis server is not running on "
            f"{os.getenv('KIWI_HOST', 'localhost')}:{os.getenv('KIWI_PORT', '7379')}"
        )
        if _enabled("KIWI_TEST_REQUIRE_SERVER"):
            pytest.fail(f"{message}: {error}", pytrace=False)
        pytest.skip(message)
    
    yield client
    
    # 清理（如果需要）
    client.close()


@pytest.fixture(scope="function", autouse=True)
def isolate_redis_database(request):
    """Flush the dedicated CI server before and after every test."""
    if request.node.get_closest_marker("raw_vector_protocol") is not None:
        yield
        return

    redis_client = request.getfixturevalue("redis_client")
    if not _enabled("KIWI_TEST_ISOLATED_SERVER"):
        yield
        return

    redis_client.flushdb()
    try:
        yield
    finally:
        redis_client.flushdb()


@pytest.fixture(scope="function")
def redis_clean(redis_client):
    """
    每个测试函数执行前后清理数据
    
    确保测试之间互不影响
    """
    if _enabled("KIWI_TEST_ISOLATED_SERVER"):
        yield redis_client
        return

    # 普通本地模式只清理测试前缀，避免清空开发者的外部 Redis。
    keys = redis_client.keys('test_*')
    if keys:
        redis_client.delete(*keys)
    
    yield redis_client
    
    # 测试后清理
    keys = redis_client.keys('test_*')
    if keys:
        redis_client.delete(*keys)


@pytest.fixture(scope="function")
def r(redis_clean):
    """兼容旧版测试用例的别名，保持每次测试的隔离性。"""
    return redis_clean


@pytest.fixture(scope="function")
def redis_binary_client(redis_client):
    """
    创建二进制模式的 Redis 客户端
    
    用于测试二进制安全功能
    """
    client = redis.Redis(
        host=os.getenv("KIWI_HOST", "localhost"),
        port=int(os.getenv("KIWI_PORT", "7379")),
        decode_responses=False,  # 不自动解码
        protocol=2,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    
    try:
        client.ping()
    except redis.RedisError as error:
        client.close()
        message = (
            "Redis server is not running on "
            f"{os.getenv('KIWI_HOST', 'localhost')}:{os.getenv('KIWI_PORT', '7379')}"
        )
        if _enabled("KIWI_TEST_REQUIRE_SERVER"):
            pytest.fail(f"{message}: {error}", pytrace=False)
        pytest.skip(message)
    
    yield client
    client.close()


def _raw_kiwi_connection(protocol):
    from raw_resp_client import RawRespConnection

    host = os.getenv("KIWI_HOST", "127.0.0.1")
    port = int(os.getenv("KIWI_PORT", "7379"))
    try:
        return RawRespConnection.connect(host, port, protocol)
    except (OSError, ValueError, EOFError) as error:
        pytest.fail(
            f"raw Vector protocol tests require Kiwi at {host}:{port}: {error}",
            pytrace=False,
        )


@pytest.fixture(scope="function", params=[2, 3], ids=["resp2", "resp3"])
def raw_protocol(request):
    return request.param


@pytest.fixture(scope="function")
def raw_kiwi_resp2():
    client = _raw_kiwi_connection(2)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(scope="function")
def raw_kiwi_resp3():
    client = _raw_kiwi_connection(3)
    try:
        yield client
    finally:
        client.close()


def pytest_configure(config):
    """pytest 配置钩子"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "benchmark: marks tests as benchmark tests"
    )
    config.addinivalue_line(
        "markers", "concurrent: marks tests as concurrency tests"
    )
    config.addinivalue_line(
        "markers", "wrongtype: marks tests as type error tests"
    )
    config.addinivalue_line(
        "markers",
        "raw_vector_protocol: owns function-scoped raw RESP connections and fails closed",
    )


def pytest_collection_modifyitems(items):
    """Required Vector nodes must remain owned and cannot be softened."""
    if not _required_vector_mode():
        return

    vector_items = [
        item
        for item in items
        if item.nodeid.startswith("tests/python/test_vector_set_differential.py::")
    ]
    for item in vector_items:
        if item.get_closest_marker("raw_vector_protocol") is None:
            raise pytest.UsageError(
                f"required Vector node lost raw_vector_protocol ownership: {item.nodeid}"
            )
        for marker in ("skip", "skipif", "xfail"):
            if item.get_closest_marker(marker) is not None:
                raise pytest.UsageError(
                    f"required Vector node cannot carry {marker}: {item.nodeid}"
                )


def pytest_sessionfinish(session, exitstatus):
    """Publish fail-closed totals for the trusted runner."""
    if not _required_vector_mode():
        return

    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    stats = reporter.stats if reporter is not None else {}
    summary = {
        "collected": session.testscollected,
        "passed": len(stats.get("passed", [])),
        "failed": len(stats.get("failed", [])) + len(stats.get("error", [])),
        "skipped": len(stats.get("skipped", [])),
        "xfailed": len(stats.get("xfailed", [])),
        "xpassed": len(stats.get("xpassed", [])),
        "deselected": len(stats.get("deselected", [])),
    }
    summary_path = os.environ.get("KIWI_VECTOR_PYTEST_SUMMARY")
    if not summary_path:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
        return

    try:
        Path(summary_path).write_text(
            json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8"
        )
    except OSError:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
        return

    if summary["collected"] == 0 or any(
        summary[name]
        for name in ("failed", "skipped", "xfailed", "xpassed", "deselected")
    ):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
