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

#!/usr/bin/env python3
"""
WRONGTYPE 错误测试

测试对非字符串类型键的操作错误处理

运行方式：
    pytest tests/python/test_wrongtype_errors.py -v
"""

import redis
import pytest

EXPECTED_WRONGTYPE_MSG = "WRONGTYPE Operation against a key holding the wrong kind of value"


def assert_wrongtype_error(exc_info):
    """Ensure the response matches Redis' WRONGTYPE error format."""
    message = str(exc_info.value).strip()
    assert (
        message == EXPECTED_WRONGTYPE_MSG
    ), f"Unexpected WRONGTYPE message: {message}"


class TestWrongTypeErrors:
    """WRONGTYPE 错误处理测试"""

    def test_mset_on_list_key(self, redis_clean):
        """测试对列表键使用 MSET"""
        r = redis_clean
        
        # 创建一个列表
        r.lpush('list_key', 'value1', 'value2')
        
        # 尝试对列表键使用 MSET 应该失败
        with pytest.raises(redis.ResponseError) as exc_info:
            r.mset({'list_key': 'new_value'})
        
        # 验证错误消息
        assert_wrongtype_error(exc_info)
        
        # 清理
        r.delete('list_key')

    def test_mset_on_hash_key(self, redis_clean):
        """测试对哈希键使用 MSET"""
        r = redis_clean
        
        # 创建一个哈希
        r.hset('hash_key', 'field1', 'value1')
        
        # 尝试对哈希键使用 MSET 应该失败
        with pytest.raises(redis.ResponseError) as exc_info:
            r.mset({'hash_key': 'new_value'})
        
        assert_wrongtype_error(exc_info)
        
        r.delete('hash_key')

    def test_mset_on_set_key(self, redis_clean):
        """测试对集合键使用 MSET"""
        r = redis_clean
        
        # 创建一个集合
        r.sadd('set_key', 'member1', 'member2')
        
        # 尝试对集合键使用 MSET 应该失败
        with pytest.raises(redis.ResponseError) as exc_info:
            r.mset({'set_key': 'new_value'})
        
        assert_wrongtype_error(exc_info)
        
        r.delete('set_key')

    def test_mset_on_zset_key(self, redis_clean):
        """测试对有序集合键使用 MSET"""
        r = redis_clean
        
        # 创建一个有序集合
        r.zadd('zset_key', {'member1': 1.0, 'member2': 2.0})
        
        # 尝试对有序集合键使用 MSET 应该失败
        with pytest.raises(redis.ResponseError) as exc_info:
            r.mset({'zset_key': 'new_value'})
        
        assert_wrongtype_error(exc_info)
        
        r.delete('zset_key')

    def test_get_on_list_key(self, redis_clean):
        """测试对列表键使用 GET"""
        r = redis_clean
        
        r.lpush('list_key', 'value')
        
        with pytest.raises(redis.ResponseError) as exc_info:
            r.get('list_key')
        
        assert_wrongtype_error(exc_info)
        
        r.delete('list_key')

    def test_incr_on_non_integer_string(self, redis_clean):
        """测试对非整数字符串使用 INCR"""
        r = redis_clean
        
        r.set('string_key', 'not_a_number')
        
        with pytest.raises(redis.ResponseError) as exc_info:
            r.incr('string_key')
        
        # 应该是值错误，不是类型错误
        assert 'not an integer' in str(exc_info.value).lower() or 'valid' in str(exc_info.value).lower()
        
        r.delete('string_key')

    def test_lpush_on_string_key(self, redis_clean):
        """测试对字符串键使用 LPUSH"""
        r = redis_clean
        
        r.set('string_key', 'value')
        
        with pytest.raises(redis.ResponseError) as exc_info:
            r.lpush('string_key', 'new_value')
        
        assert_wrongtype_error(exc_info)
        
        r.delete('string_key')

    def test_hset_on_string_key(self, redis_clean):
        """测试对字符串键使用 HSET"""
        r = redis_clean
        
        r.set('string_key', 'value')
        
        with pytest.raises(redis.ResponseError) as exc_info:
            r.hset('string_key', 'field', 'value')
        
        assert_wrongtype_error(exc_info)
        
        r.delete('string_key')

    def test_sadd_on_string_key(self, redis_clean):
        """测试对字符串键使用 SADD"""
        r = redis_clean
        
        r.set('string_key', 'value')
        
        with pytest.raises(redis.ResponseError) as exc_info:
            r.sadd('string_key', 'member')
        
        assert_wrongtype_error(exc_info)
        
        r.delete('string_key')

    def test_zadd_on_string_key(self, redis_clean):
        """测试对字符串键使用 ZADD"""
        r = redis_clean
        
        r.set('string_key', 'value')
        
        with pytest.raises(redis.ResponseError) as exc_info:
            r.zadd('string_key', {'member': 1.0})
        
        assert_wrongtype_error(exc_info)
        
        r.delete('string_key')

    def test_mset_mixed_valid_and_wrongtype(self, redis_clean):
        """测试 MSET 混合有效键和错误类型键"""
        r = redis_clean
        
        # 创建一个列表
        r.lpush('list_key', 'value')
        
        # MSET 应该是原子的，如果有一个键失败，整个操作应该失败
        with pytest.raises(redis.ResponseError) as exc_info:
            r.mset({
                'list_key': 'new_value',  # 这个会失败
                'string_key': 'value'      # 这个是有效的
            })
        
        assert_wrongtype_error(exc_info)
        
        # 验证原子性：string_key 不应该被设置
        assert r.get('string_key') is None
        
        r.delete('list_key')


class TestTypeValidation:
    """类型验证测试"""

    def test_string_operations_on_string(self, redis_clean):
        """验证字符串操作在字符串键上正常工作"""
        r = redis_clean
        
        r.set('string_key', 'value')
        assert r.get('string_key') == 'value'
        
        r.mset({'string_key': 'new_value'})
        assert r.get('string_key') == 'new_value'
        
        r.delete('string_key')

    def test_list_operations_on_list(self, redis_clean):
        """验证列表操作在列表键上正常工作"""
        r = redis_clean
        
        r.lpush('list_key', 'value1', 'value2')
        assert r.llen('list_key') == 2
        
        r.lpush('list_key', 'value3')
        assert r.llen('list_key') == 3
        
        r.delete('list_key')

    def test_hash_operations_on_hash(self, redis_clean):
        """验证哈希操作在哈希键上正常工作"""
        r = redis_clean
        
        r.hset('hash_key', 'field1', 'value1')
        assert r.hget('hash_key', 'field1') == 'value1'
        
        r.hset('hash_key', 'field2', 'value2')
        assert r.hlen('hash_key') == 2
        
        r.delete('hash_key')

    def test_set_operations_on_set(self, redis_clean):
        """验证集合操作在集合键上正常工作"""
        r = redis_clean
        
        r.sadd('set_key', 'member1', 'member2')
        assert r.scard('set_key') == 2
        
        r.sadd('set_key', 'member3')
        assert r.scard('set_key') == 3
        
        r.delete('set_key')

    def test_zset_operations_on_zset(self, redis_clean):
        """验证有序集合操作在有序集合键上正常工作"""
        r = redis_clean
        
        r.zadd('zset_key', {'member1': 1.0, 'member2': 2.0})
        assert r.zcard('zset_key') == 2
        
        r.zadd('zset_key', {'member3': 3.0})
        assert r.zcard('zset_key') == 3
        
        r.delete('zset_key')


if __name__ == '__main__':
    import sys
    try:
        import pytest
        sys.exit(pytest.main([__file__, '-v', '--tb=short']))
    except ImportError:
        print("请安装 pytest: pip install pytest")
        sys.exit(1)
