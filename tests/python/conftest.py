"""
pytest 配置文件

提供测试夹具（fixtures）和通用配置
"""

import pytest
import redis
import time


@pytest.fixture(scope="session")
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
    except redis.ConnectionError:
        pytest.skip("Redis server is not running on localhost:6379")
    
    yield client
    
    # 清理（如果需要）
    client.close()


@pytest.fixture(scope="function")
def redis_clean(redis_client):
    """
    每个测试函数执行前后清理数据
    
    确保测试之间互不影响
    """
    # 测试前清理
    keys = redis_client.keys('test_*')
    if keys:
        redis_client.delete(*keys)
    
    yield redis_client
    
    # 测试后清理
    keys = redis_client.keys('test_*')
    if keys:
        redis_client.delete(*keys)


@pytest.fixture(scope="session")
def redis_binary_client():
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
    except redis.ConnectionError:
        pytest.skip("Redis server is not running on localhost:6379")
    
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
