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
MSET 并发测试

测试 MSET 命令在并发场景下的原子性和一致性

运行方式：
    pytest tests/python/test_mset_concurrent.py -v
"""

import redis
import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


class TestMsetConcurrency:
    """MSET 并发测试"""

    def test_concurrent_mset_operations(self, redis_clean):
        """测试并发 MSET 操作"""
        r = redis_clean
        num_threads = 10
        operations_per_thread = 10
        
        def mset_operation(thread_id):
            """每个线程执行的 MSET 操作"""
            results = []
            for i in range(operations_per_thread):
                key = f'thread_{thread_id}_key_{i}'
                value = f'thread_{thread_id}_value_{i}'
                result = r.mset({key: value})
                results.append(result)
            return results
        
        # 使用线程池执行并发操作
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(mset_operation, i) for i in range(num_threads)]
            all_results = []
            for future in as_completed(futures):
                all_results.extend(future.result())
        
        # 验证所有操作都成功
        assert all(all_results), "所有 MSET 操作应该成功"
        
        # 验证所有键都被正确设置
        for thread_id in range(num_threads):
            for i in range(operations_per_thread):
                key = f'thread_{thread_id}_key_{i}'
                expected_value = f'thread_{thread_id}_value_{i}'
                actual_value = r.get(key)
                assert actual_value == expected_value, f"键 {key} 的值不正确"
        
        # 清理
        keys_to_delete = [
            f'thread_{thread_id}_key_{i}'
            for thread_id in range(num_threads)
            for i in range(operations_per_thread)
        ]
        r.delete(*keys_to_delete)

    def test_concurrent_mset_same_keys(self, redis_clean):
        """测试并发 MSET 操作相同的键"""
        r = redis_clean
        num_threads = 20
        test_keys = ['shared_key_1', 'shared_key_2', 'shared_key_3']
        
        def mset_operation(thread_id):
            """每个线程尝试设置相同的键"""
            return r.mset({
                key: f'thread_{thread_id}_value'
                for key in test_keys
            })
        
        # 并发执行
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(mset_operation, i) for i in range(num_threads)]
            results = [future.result() for future in as_completed(futures)]
        
        # 所有操作都应该成功
        assert all(results)
        
        # 验证最终状态一致（所有键都应该有值）
        for key in test_keys:
            value = r.get(key)
            assert value is not None, f"键 {key} 应该有值"
            # 值应该是某个线程设置的
            assert value.startswith('thread_') and value.endswith('_value')
        
        r.delete(*test_keys)

    def test_mset_atomicity_under_concurrency(self, redis_clean):
        """测试并发场景下 MSET 的原子性"""
        r = redis_clean
        num_iterations = 50
        
        def mset_batch(batch_id):
            """设置一批键"""
            keys = {
                f'batch_{batch_id}_key_1': f'batch_{batch_id}_value_1',
                f'batch_{batch_id}_key_2': f'batch_{batch_id}_value_2',
                f'batch_{batch_id}_key_3': f'batch_{batch_id}_value_3',
            }
            r.mset(keys)
            
            # 立即验证原子性：所有键应该都存在
            for key, expected_value in keys.items():
                actual_value = r.get(key)
                if actual_value != expected_value:
                    return False
            return True
        
        # 并发执行多次
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(mset_batch, i) for i in range(num_iterations)]
            results = [future.result() for future in as_completed(futures)]
        
        # 所有批次都应该保持原子性
        assert all(results), "MSET 应该在并发场景下保持原子性"
        
        # 清理
        keys_to_delete = [
            f'batch_{i}_key_{j}'
            for i in range(num_iterations)
            for j in range(1, 4)
        ]
        r.delete(*keys_to_delete)

    def test_concurrent_mset_and_get(self, redis_clean):
        """测试并发 MSET 和 GET 操作"""
        r = redis_clean
        num_writers = 5
        num_readers = 10
        duration = 2  # 秒
        
        stop_flag = threading.Event()
        write_count = 0
        read_count = 0
        read_errors = 0
        write_lock = threading.Lock()
        read_lock = threading.Lock()
        error_lock = threading.Lock()
        
        def writer_thread(writer_id):
            """写入线程"""
            nonlocal write_count
            while not stop_flag.is_set():
                with write_lock:
                    current_write = write_count
                    write_count += 1
                r.mset({
                    f'writer_{writer_id}_key_1': f'value_{current_write}',
                    f'writer_{writer_id}_key_2': f'value_{current_write}',
                })
                time.sleep(0.01)
        
        def reader_thread():
            """读取线程"""
            nonlocal read_count, read_errors
            while not stop_flag.is_set():
                try:
                    # 随机读取某个写入线程的键
                    with read_lock:
                        current_read = read_count
                        read_count += 1
                    writer_id = current_read % num_writers
                    key1 = f'writer_{writer_id}_key_1'
                    key2 = f'writer_{writer_id}_key_2'
                    
                    val1 = r.get(key1)
                    val2 = r.get(key2)
                    
                    # 如果两个键都存在，它们应该有相同的值（原子性）
                    if val1 is not None and val2 is not None and val1 != val2:
                        with error_lock:
                            read_errors += 1
                except Exception:
                    with error_lock:
                        read_errors += 1
                time.sleep(0.01)
        
        # 启动线程
        threads = []
        for i in range(num_writers):
            t = threading.Thread(target=writer_thread, args=(i,))
            t.start()
            threads.append(t)
        
        for _ in range(num_readers):
            t = threading.Thread(target=reader_thread)
            t.start()
            threads.append(t)
        
        # 运行指定时间
        time.sleep(duration)
        stop_flag.set()
        
        # 等待所有线程结束
        for t in threads:
            t.join()
        
        # 验证没有读取错误（原子性保证）
        assert read_errors == 0, f"检测到 {read_errors} 个原子性违规"
        
        # 清理
        keys_to_delete = [
            f'writer_{i}_key_{j}'
            for i in range(num_writers)
            for j in range(1, 3)
        ]
        r.delete(*keys_to_delete)

    @pytest.mark.slow
    def test_high_concurrency_stress(self, redis_clean):
        """高并发压力测试"""
        r = redis_clean
        num_threads = 50
        operations_per_thread = 100
        
        def stress_operation(thread_id):
            """压力测试操作"""
            success_count = 0
            for i in range(operations_per_thread):
                try:
                    # 每次操作设置多个键
                    keys = {
                        f'stress_{thread_id}_{i}_key_{j}': f'value_{j}'
                        for j in range(5)
                    }
                    result = r.mset(keys)
                    if result:
                        success_count += 1
                except Exception:
                    pass
            return success_count
        
        # 执行高并发操作
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(stress_operation, i) for i in range(num_threads)]
            results = [future.result() for future in as_completed(futures)]
        end_time = time.time()
        
        # 统计
        total_operations = sum(results)
        duration = end_time - start_time
        ops_per_second = total_operations / duration
        
        print(f"\n高并发压力测试结果:")
        print(f"  总操作数: {total_operations}")
        print(f"  持续时间: {duration:.2f} 秒")
        print(f"  吞吐量: {ops_per_second:.2f} ops/sec")
        
        # 至少应该有 80% 的操作成功
        expected_total = num_threads * operations_per_thread
        success_rate = total_operations / expected_total
        assert success_rate >= 0.8, f"成功率 {success_rate:.2%} 低于 80%"

    def test_concurrent_mset_with_mget(self, redis_clean):
        """测试并发 MSET 和 MGET 操作"""
        r = redis_clean
        num_operations = 100
        
        def mset_mget_operation(op_id):
            """MSET 后立即 MGET"""
            keys = {
                f'op_{op_id}_key_1': f'op_{op_id}_value_1',
                f'op_{op_id}_key_2': f'op_{op_id}_value_2',
                f'op_{op_id}_key_3': f'op_{op_id}_value_3',
            }
            
            # MSET
            r.mset(keys)
            
            # 立即 MGET
            key_list = list(keys.keys())
            values = r.mget(key_list)
            
            # 验证值正确
            expected_values = [keys[k] for k in key_list]
            return values == expected_values
        
        # 并发执行
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(mset_mget_operation, i) for i in range(num_operations)]
            results = [future.result() for future in as_completed(futures)]
        
        # 所有操作都应该验证成功
        assert all(results), "MSET 和 MGET 应该保持一致性"
        
        # 清理
        keys_to_delete = [
            f'op_{i}_key_{j}'
            for i in range(num_operations)
            for j in range(1, 4)
        ]
        r.delete(*keys_to_delete)


class TestMsetRaceConditions:
    """MSET 竞态条件测试"""

    def test_race_condition_overwrite(self, redis_clean):
        """测试并发覆盖的竞态条件"""
        r = redis_clean
        num_threads = 10
        test_key = 'race_key'
        
        def overwrite_operation(thread_id):
            """每个线程尝试覆盖相同的键"""
            for i in range(10):
                r.mset({test_key: f'thread_{thread_id}_iteration_{i}'})
        
        # 并发执行
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(overwrite_operation, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()
        
        # 最终应该有一个有效的值
        final_value = r.get(test_key)
        assert final_value is not None
        assert final_value.startswith('thread_')
        
        r.delete(test_key)

    def test_race_condition_delete_and_set(self, redis_clean):
        """测试删除和设置的竞态条件"""
        r = redis_clean
        test_keys = ['race_key_1', 'race_key_2', 'race_key_3']
        num_iterations = 50
        
        def delete_operation():
            """删除操作"""
            for _ in range(num_iterations):
                r.delete(*test_keys)
                time.sleep(0.001)
        
        def mset_operation():
            """MSET 操作"""
            for i in range(num_iterations):
                r.mset({key: f'value_{i}' for key in test_keys})
                time.sleep(0.001)
        
        # 并发执行删除和设置
        with ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(delete_operation)
            future2 = executor.submit(mset_operation)
            future1.result()
            future2.result()
        
        # 操作应该都成功完成（不崩溃）
        # 最终状态可能是存在或不存在，但不应该有部分状态
        values = r.mget(test_keys)
        
        # 要么全部存在，要么全部不存在（原子性）
        if values[0] is not None:
            assert all(v is not None for v in values), "MSET 应该是原子的"
        
        r.delete(*test_keys)


if __name__ == '__main__':
    import sys
    try:
        import pytest
        sys.exit(pytest.main([__file__, '-v', '--tb=short']))
    except ImportError:
        print("请安装 pytest: pip install pytest")
        sys.exit(1)
