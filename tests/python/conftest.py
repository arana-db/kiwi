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

import os

import pytest
import redis


def _enabled(name):
    """Return whether a CI-only test mode is explicitly enabled."""
    return os.environ.get(name) == "1"


@pytest.fixture(scope="session", autouse=True)
def redis_client():
    """
    创建 Redis 客户端连接
    
    作用域为 session，所有测试共享一个连接
    """
    client = redis.Redis(
        host='localhost',
        port=6379,
        decode_responses=True,
        socket_connect_timeout=5
    )
    
    # 测试连接
    try:
        client.ping()
    except redis.RedisError as error:
        client.close()
        message = "Redis server is not running on localhost:6379"
        if _enabled("KIWI_TEST_REQUIRE_SERVER"):
            pytest.fail(f"{message}: {error}", pytrace=False)
        pytest.skip(message)
    
    yield client
    
    # 清理（如果需要）
    client.close()


@pytest.fixture(scope="function", autouse=True)
def isolate_redis_database(redis_client):
    """Flush the dedicated CI server before and after every test."""
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


@pytest.fixture(scope="session")
def redis_binary_client(redis_client):
    """
    创建二进制模式的 Redis 客户端
    
    用于测试二进制安全功能
    """
    client = redis.Redis(
        host='localhost',
        port=6379,
        decode_responses=False  # 不自动解码
    )
    
    try:
        client.ping()
    except redis.RedisError as error:
        client.close()
        message = "Redis server is not running on localhost:6379"
        if _enabled("KIWI_TEST_REQUIRE_SERVER"):
            pytest.fail(f"{message}: {error}", pytrace=False)
        pytest.skip(message)
    
    yield client
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
