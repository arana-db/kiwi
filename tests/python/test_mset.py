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
MSET 命令测试

使用 pytest 框架测试 MSET 命令的实现

运行方式：
    pytest tests/python/test_mset.py -v
    python tests/python/test_mset.py  # 直接运行
"""

import redis
import pytest
import sys


class TestMsetBasic:
    """MSET 基本功能测试"""

    def test_mset_basic(self, redis_clean):
        """测试基本的 MSET 功能"""
        r = redis_clean
        
        # 测试基本的 MSET
        result = r.mset({
            'test_key1': 'value1',
            'test_key2': 'value2',
            'test_key3': 'value3'
        })
        assert result == True, "MSET 应该返回 True"
        
        # 验证所有键都已设置
        assert r.get('test_key1') == 'value1'
        assert r.get('test_key2') == 'value2'
        assert r.get('test_key3') == 'value3'

    def test_mset_single_pair(self, redis_clean):
        """测试 MSET 单个键值对"""
        r = redis_clean
        
        # 只设置一个键值对
        result = r.mset({'test_single_key': 'single_value'})
        assert result == True
        assert r.get('test_single_key') == 'single_value'

    def test_mset_overwrite(self, redis_clean):
        """测试 MSET 覆盖已存在的键"""
        r = redis_clean
        
        # 先设置一些键
        r.set('test_overwrite_key1', 'old_value1')
        r.set('test_overwrite_key2', 'old_value2')
        
        # 使用 MSET 覆盖
        r.mset({
            'test_overwrite_key1': 'new_value1',
            'test_overwrite_key2': 'new_value2',
            'test_overwrite_key3': 'new_value3'
        })
        
        # 验证值已被覆盖
        assert r.get('test_overwrite_key1') == 'new_value1'
        assert r.get('test_overwrite_key2') == 'new_value2'
        assert r.get('test_overwrite_key3') == 'new_value3'


class TestMsetIntegration:
    """MSET 集成测试"""

    def test_mset_with_mget(self, redis_clean):
        """测试 MSET 和 MGET 的配合"""
        r = redis_clean
        
        # 使用 MSET 设置多个键
        r.mset({
            'test_mget_key1': 'mget_value1',
            'test_mget_key2': 'mget_value2',
            'test_mget_key3': 'mget_value3'
        })
        
        # 使用 MGET 获取所有键
        values = r.mget(['test_mget_key1', 'test_mget_key2', 'test_mget_key3'])
        assert values == ['mget_value1', 'mget_value2', 'mget_value3']

    def test_mset_atomicity(self, redis_clean):
        """测试 MSET 原子性"""
        r = redis_clean
        
        # 设置初始值
        r.set('test_atomic_key1', 'initial1')
        r.set('test_atomic_key2', 'initial2')
        
        # 原子性更新所有键
        r.mset({
            'test_atomic_key1': 'atomic1',
            'test_atomic_key2': 'atomic2',
            'test_atomic_key3': 'atomic3'
        })
        
        # 使用 MGET 验证所有键都已更新
        values = r.mget(['test_atomic_key1', 'test_atomic_key2', 'test_atomic_key3'])
        assert values == ['atomic1', 'atomic2', 'atomic3']


class TestMsetPerformance:
    """MSET 性能测试"""

    @pytest.mark.slow
    def test_mset_large_batch(self, redis_clean):
        """测试 MSET 大批量操作 (100个键值对)"""
        r = redis_clean
        
        # 创建 100 个键值对
        large_dict = {f'test_batch_key_{i}': f'batch_value_{i}' for i in range(100)}
        
        # 执行 MSET
        result = r.mset(large_dict)
        assert result == True
        
        # 验证部分键
        assert r.get('test_batch_key_0') == 'batch_value_0'
        assert r.get('test_batch_key_50') == 'batch_value_50'
        assert r.get('test_batch_key_99') == 'batch_value_99'

    @pytest.mark.benchmark
    def test_mset_performance_benchmark(self, redis_clean, benchmark):
        """MSET 性能基准测试"""
        r = redis_clean
        
        # 创建测试数据
        data = {f'benchmark_key_{i}': f'benchmark_value_{i}' for i in range(1000)}
        
        # 运行基准测试
        result = benchmark(r.mset, data)
        assert result == True


class TestMsetBinary:
    """MSET 二进制安全测试"""

    def test_mset_binary_safe(self, redis_binary_client):
        """测试 MSET 二进制安全"""
        r = redis_binary_client
        
        # 使用二进制数据
        r.mset({
            b'test_binary_key1': b'binary\x00value',
            b'test_binary_key2': bytes([0, 1, 2, 3, 255]),
            b'test_utf8_key': '你好世界'.encode('utf-8')
        })
        
        # 验证二进制数据完整性
        assert r.get(b'test_binary_key1') == b'binary\x00value'
        assert r.get(b'test_binary_key2') == bytes([0, 1, 2, 3, 255])
        assert r.get(b'test_utf8_key') == '你好世界'.encode('utf-8')
        
        # 清理
        r.delete(b'test_binary_key1', b'test_binary_key2', b'test_utf8_key')


class TestMsetErrors:
    """MSET 错误处理测试"""

    def test_mset_empty_dict(self, redis_clean):
        """测试空字典（边界情况）"""
        r = redis_clean
        
        # 空字典应该返回 True（Redis 行为）
        result = r.mset({})
        assert result == True


# ============================================================================
# 独立运行支持（兼容旧版脚本）
# ============================================================================

def run_standalone_tests():
    """独立运行所有测试（不使用 pytest）"""
    print("=" * 60)
    print("开始测试 MSET 命令实现（独立模式）")
    print("=" * 60)
    
    try:
        # 连接测试
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        print("✓ 成功连接到 Redis 服务器\n")
    except redis.ConnectionError:
        print("✗ 无法连接到 Redis 服务器")
        print("请确保服务器正在运行在 localhost:6379")
        sys.exit(1)
    
    test_count = 0
    passed = 0
    
    try:
        # 运行测试
        tests = [
            ("基本 MSET 功能", lambda: test_basic(r)),
            ("MSET 与 MGET 配合", lambda: test_with_mget(r)),
            ("覆盖已存在的键", lambda: test_overwrite(r)),
            ("单个键值对", lambda: test_single_pair(r)),
            ("大批量操作", lambda: test_large_batch(r)),
            ("原子性验证", lambda: test_atomicity(r)),
        ]
        
        for name, test_func in tests:
            test_count += 1
            print(f"测试 {test_count}: {name}")
            try:
                test_func()
                print(f"✓ {name}测试通过\n")
                passed += 1
            except AssertionError as e:
                print(f"✗ {name}测试失败: {e}\n")
        
        print("=" * 60)
        print(f"测试结果: {passed}/{test_count} 通过")
        print("=" * 60)
        
        if passed == test_count:
            print("\n所有测试通过! ✓")
            sys.exit(0)
        else:
            print(f"\n{test_count - passed} 个测试失败!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # 清理测试数据
        cleanup(r)


def test_basic(r):
    r.mset({'test_key1': 'value1', 'test_key2': 'value2'})
    assert r.get('test_key1') == 'value1'
    assert r.get('test_key2') == 'value2'


def test_with_mget(r):
    r.mset({'test_mget1': 'v1', 'test_mget2': 'v2'})
    values = r.mget(['test_mget1', 'test_mget2'])
    assert values == ['v1', 'v2']


def test_overwrite(r):
    r.set('test_over', 'old')
    r.mset({'test_over': 'new'})
    assert r.get('test_over') == 'new'


def test_single_pair(r):
    r.mset({'test_single': 'value'})
    assert r.get('test_single') == 'value'


def test_large_batch(r):
    large_dict = {f'test_batch_{i}': f'val_{i}' for i in range(100)}
    r.mset(large_dict)
    assert r.get('test_batch_0') == 'val_0'
    assert r.get('test_batch_99') == 'val_99'


def test_atomicity(r):
    r.mset({'test_a1': 'v1', 'test_a2': 'v2'})
    values = r.mget(['test_a1', 'test_a2'])
    assert values == ['v1', 'v2']


def cleanup(r):
    """清理测试数据"""
    patterns = ['test_*']
    for pattern in patterns:
        keys = r.keys(pattern)
        if keys:
            r.delete(*keys)


if __name__ == '__main__':
    # 检查是否安装了 pytest
    try:
        import pytest
        # 使用 pytest 运行
        sys.exit(pytest.main([__file__, '-v']))
    except ImportError:
        # 降级到独立模式
        print("提示: 未安装 pytest，使用独立测试模式")
        print("建议: pip install pytest\n")
        run_standalone_tests()