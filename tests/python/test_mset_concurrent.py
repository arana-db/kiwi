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

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

import pytest

pytestmark = pytest.mark.timeout(120)


@pytest.fixture
def register_cleanup_keys(redis_clean):
    """Register exact keys for unconditional, bounded post-test cleanup."""
    keys = []

    def register(new_keys):
        keys.extend(new_keys)

    try:
        yield register
    finally:
        for offset in range(0, len(keys), 1000):
            redis_clean.delete(*keys[offset:offset + 1000])


class TestMsetConcurrency:
    """MSET 并发测试"""

    def test_concurrent_mset_operations(
        self, redis_clean, register_cleanup_keys
    ):
        """测试并发 MSET 操作"""
        r = redis_clean
        num_threads = 10
        operations_per_thread = 10
        keys_to_delete = [
            f'test_kiwi_mset_concurrent_thread_{thread_id}_key_{i}'
            for thread_id in range(num_threads)
            for i in range(operations_per_thread)
        ]
        register_cleanup_keys(keys_to_delete)

        def mset_operation(thread_id):
            """每个线程执行的 MSET 操作"""
            results = []
            for i in range(operations_per_thread):
                key = f'test_kiwi_mset_concurrent_thread_{thread_id}_key_{i}'
                value = f'thread_{thread_id}_value_{i}'
                result = r.mset({key: value})
                results.append(result)
            return results

        # 使用线程池执行并发操作
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(mset_operation, i) for i in range(num_threads)]
            all_results = []
            for future in as_completed(futures, timeout=30):
                all_results.extend(future.result(timeout=30))
        
        # 验证所有操作都成功
        assert all(all_results), "所有 MSET 操作应该成功"
        
        # 验证所有键都被正确设置
        for thread_id in range(num_threads):
            for i in range(operations_per_thread):
                key = f'test_kiwi_mset_concurrent_thread_{thread_id}_key_{i}'
                expected_value = f'thread_{thread_id}_value_{i}'
                actual_value = r.get(key)
                assert actual_value == expected_value, f"键 {key} 的值不正确"
        
    def test_concurrent_mset_same_keys(
        self, redis_clean, register_cleanup_keys
    ):
        """测试并发 MSET 操作相同的键"""
        r = redis_clean
        num_threads = 20
        test_keys = ['test_kiwi_mset_concurrent_shared_key_1', 'test_kiwi_mset_concurrent_shared_key_2', 'test_kiwi_mset_concurrent_shared_key_3']
        register_cleanup_keys(test_keys)
        
        def mset_operation(thread_id):
            """每个线程尝试设置相同的键"""
            return r.mset({
                key: f'thread_{thread_id}_value'
                for key in test_keys
            })
        
        # 并发执行
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(mset_operation, i) for i in range(num_threads)]
            results = [
                future.result(timeout=30)
                for future in as_completed(futures, timeout=30)
            ]
        
        # 所有操作都应该成功
        assert all(results)

        # 单条 MGET 观察同一次 MSET 提交的完整结果。
        values = r.mget(test_keys)
        assert all(value is not None for value in values)
        assert len(set(values)) == 1, "所有键必须来自同一次 MSET"
        assert values[0].startswith('thread_') and values[0].endswith('_value')

    def test_concurrent_mset_batch_round_trip(
        self, redis_clean, register_cleanup_keys
    ):
        """测试并发独立批次的 MSET/MGET 往返一致性"""
        r = redis_clean
        num_iterations = 50
        keys_to_delete = [
            f'test_kiwi_mset_concurrent_batch_{i}_key_{j}'
            for i in range(num_iterations)
            for j in range(1, 4)
        ]
        register_cleanup_keys(keys_to_delete)
        
        def mset_batch(batch_id):
            """设置一批键"""
            keys = {
                f'test_kiwi_mset_concurrent_batch_{batch_id}_key_1': f'batch_{batch_id}_value_1',
                f'test_kiwi_mset_concurrent_batch_{batch_id}_key_2': f'batch_{batch_id}_value_2',
                f'test_kiwi_mset_concurrent_batch_{batch_id}_key_3': f'batch_{batch_id}_value_3',
            }
            r.mset(keys)
            
            # 只用一条 MGET 观察同一批键，避免多条 GET 之间发生状态变化。
            key_list = list(keys)
            return r.mget(key_list) == [keys[key] for key in key_list]
        
        # 并发执行多次
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(mset_batch, i) for i in range(num_iterations)]
            results = [
                future.result(timeout=30)
                for future in as_completed(futures, timeout=30)
            ]
        
        assert all(results), "每个独立批次都应该完成 MSET/MGET 往返"
        
    def test_concurrent_mset_and_get(
        self, redis_clean, register_cleanup_keys
    ):
        """测试并发 MSET 和 GET 操作"""
        r = redis_clean
        num_writers = 5
        num_readers = 10
        duration = 2  # 秒
        
        stop_flag = threading.Event()
        write_count = 0
        read_count = 0
        write_lock = threading.Lock()
        read_lock = threading.Lock()
        
        def writer_thread(writer_id):
            """写入线程"""
            nonlocal write_count
            while not stop_flag.is_set():
                with write_lock:
                    current_write = write_count
                    write_count += 1
                r.mset({
                    f'test_kiwi_mset_concurrent_writer_{writer_id}_key_1': f'value_{current_write}',
                    f'test_kiwi_mset_concurrent_writer_{writer_id}_key_2': f'value_{current_write}',
                })
                time.sleep(0.01)
        
        def reader_thread():
            """读取线程"""
            nonlocal read_count
            while not stop_flag.is_set():
                with read_lock:
                    current_read = read_count
                    read_count += 1
                writer_id = current_read % num_writers
                values = r.mget([
                    f'test_kiwi_mset_concurrent_writer_{writer_id}_key_1',
                    f'test_kiwi_mset_concurrent_writer_{writer_id}_key_2',
                ])

                # 两个键尚未创建时都是 None；创建后必须来自同一次 MSET。
                assert values[0] == values[1], (
                    f"MGET 观察到部分 MSET 结果: writer={writer_id}, values={values}"
                )
                time.sleep(0.01)

        keys_to_delete = [
            f'test_kiwi_mset_concurrent_writer_{i}_key_{j}'
            for i in range(num_writers)
            for j in range(1, 3)
        ]
        register_cleanup_keys(keys_to_delete)
        try:
            with ThreadPoolExecutor(
                max_workers=num_writers + num_readers
            ) as executor:
                futures = [
                    executor.submit(writer_thread, writer_id)
                    for writer_id in range(num_writers)
                ]
                futures.extend(
                    executor.submit(reader_thread)
                    for _ in range(num_readers)
                )
                try:
                    time.sleep(duration)
                finally:
                    stop_flag.set()

                # future.result() 将线程中的连接错误和断言回传主线程。
                for future in futures:
                    future.result(timeout=30)
        finally:
            stop_flag.set()

    @pytest.mark.slow
    def test_high_concurrency_stress(
        self, redis_clean, register_cleanup_keys
    ):
        """高并发压力测试"""
        r = redis_clean
        num_threads = 50
        operations_per_thread = 100
        keys_to_delete = [
            f'test_kiwi_mset_concurrent_stress_{thread_id}_{i}_key_{j}'
            for thread_id in range(num_threads)
            for i in range(operations_per_thread)
            for j in range(5)
        ]
        register_cleanup_keys(keys_to_delete)
        
        def stress_operation(thread_id):
            """压力测试操作"""
            success_count = 0
            for i in range(operations_per_thread):
                keys = {
                    f'test_kiwi_mset_concurrent_stress_{thread_id}_{i}_key_{j}': f'value_{j}'
                    for j in range(5)
                }
                assert r.mset(keys) is True
                success_count += 1
            return success_count
        
        # 执行高并发操作
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(stress_operation, i) for i in range(num_threads)]
            results = [
                future.result(timeout=120)
                for future in as_completed(futures, timeout=120)
            ]
        end_time = time.time()
        
        # 统计
        total_operations = sum(results)
        duration = end_time - start_time
        ops_per_second = total_operations / duration
        
        print(f"\n高并发压力测试结果:")
        print(f"  总操作数: {total_operations}")
        print(f"  持续时间: {duration:.2f} 秒")
        print(f"  吞吐量: {ops_per_second:.2f} ops/sec")
        
        # 所有 worker 异常都由 future.result() 传播，因此每次操作都必须成功。
        expected_total = num_threads * operations_per_thread
        assert total_operations == expected_total

    def test_concurrent_mset_with_mget(
        self, redis_clean, register_cleanup_keys
    ):
        """测试并发 MSET 和 MGET 操作"""
        r = redis_clean
        num_operations = 100
        keys_to_delete = [
            f'test_kiwi_mset_concurrent_op_{i}_key_{j}'
            for i in range(num_operations)
            for j in range(1, 4)
        ]
        register_cleanup_keys(keys_to_delete)
        
        def mset_mget_operation(op_id):
            """MSET 后立即 MGET"""
            keys = {
                f'test_kiwi_mset_concurrent_op_{op_id}_key_1': f'op_{op_id}_value_1',
                f'test_kiwi_mset_concurrent_op_{op_id}_key_2': f'op_{op_id}_value_2',
                f'test_kiwi_mset_concurrent_op_{op_id}_key_3': f'op_{op_id}_value_3',
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
            results = [
                future.result(timeout=30)
                for future in as_completed(futures, timeout=30)
            ]
        
        # 所有操作都应该验证成功
        assert all(results), "MSET 和 MGET 应该保持一致性"
        
class TestMsetRaceConditions:
    """MSET 竞态条件测试"""

    def test_race_condition_overwrite(
        self, redis_clean, register_cleanup_keys
    ):
        """测试并发覆盖的竞态条件"""
        r = redis_clean
        num_threads = 10
        test_key = 'test_kiwi_mset_concurrent_race_key'
        register_cleanup_keys([test_key])
        
        def overwrite_operation(thread_id):
            """每个线程尝试覆盖相同的键"""
            for i in range(10):
                r.mset({test_key: f'thread_{thread_id}_iteration_{i}'})
        
        # 并发执行
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(overwrite_operation, i) for i in range(num_threads)]
            for future in as_completed(futures, timeout=30):
                future.result(timeout=30)
        
        # 最终应该有一个有效的值
        final_value = r.get(test_key)
        assert final_value is not None
        assert final_value.startswith('thread_')
        
    def test_race_condition_delete_and_set(
        self, redis_clean, register_cleanup_keys
    ):
        """测试删除和设置的竞态条件"""
        r = redis_clean
        test_keys = [
            'test_kiwi_mset_concurrent_race_key_1',
            'test_kiwi_mset_concurrent_race_key_2',
            'test_kiwi_mset_concurrent_race_key_3',
        ]
        num_iterations = 50
        register_cleanup_keys(test_keys)

        start_barrier = threading.Barrier(3)
        observer_started = threading.Event()
        mutations_active = threading.Event()
        mutators_done = threading.Event()
        completion_lock = threading.Lock()
        completed_mutators = 0

        def finish_mutator():
            nonlocal completed_mutators
            with completion_lock:
                completed_mutators += 1
                if completed_mutators == 2:
                    mutators_done.set()

        def wait_for_observer():
            start_barrier.wait(timeout=5)
            assert observer_started.wait(timeout=5), "observer did not start"
            mutations_active.set()

        def delete_operation():
            """删除操作"""
            try:
                wait_for_observer()
                for _ in range(num_iterations):
                    r.delete(*test_keys)
                    time.sleep(0.001)
            finally:
                finish_mutator()

        def mset_operation():
            """MSET 操作"""
            try:
                wait_for_observer()
                for i in range(num_iterations):
                    r.mset({key: f'value_{i}' for key in test_keys})
                    time.sleep(0.001)
            finally:
                finish_mutator()

        def observer_operation():
            overlap_observations = 0
            start_barrier.wait(timeout=5)

            while True:
                values = r.mget(test_keys)
                all_missing = all(value is None for value in values)
                all_equal = (
                    all(value is not None for value in values)
                    and len(set(values)) == 1
                )
                assert all_missing or all_equal, (
                    f"MGET observed a partial DEL/MSET state: {values}"
                )

                observer_started.set()
                if mutations_active.is_set():
                    overlap_observations += 1
                if mutators_done.is_set():
                    break
                time.sleep(0)

            assert overlap_observations > 0, (
                "observer did not overlap with DEL/MSET operations"
            )

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(observer_operation),
                executor.submit(delete_operation),
                executor.submit(mset_operation),
            ]
            for future in futures:
                future.result(timeout=30)

if __name__ == '__main__':
    import sys
    try:
        import pytest
        sys.exit(pytest.main([__file__, '-v', '--tb=short']))
    except ImportError:
        print("请安装 pytest: pip install pytest")
        sys.exit(1)
