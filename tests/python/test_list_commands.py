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

"""
List Commands Integration Tests

Tests Redis list commands with real Redis clients to verify compatibility
and concurrent access scenarios.
"""

import redis
import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


class TestListBasicOperations:
    """Test basic list operations"""

    def test_lpush_rpush_basic(self, redis_clean):
        """Test basic LPUSH and RPUSH operations"""
        r = redis_clean
        
        # Test LPUSH
        assert r.lpush('test_list', 'item1') == 1
        assert r.lpush('test_list', 'item2') == 2
        
        # Test RPUSH
        assert r.rpush('test_list', 'item3') == 3
        assert r.rpush('test_list', 'item4') == 4
        
        # Verify list contents
        assert r.lrange('test_list', 0, -1) == ['item2', 'item1', 'item3', 'item4']

    def test_lpop_rpop_basic(self, redis_clean):
        """Test basic LPOP and RPOP operations"""
        r = redis_clean
        
        # Setup list
        r.rpush('test_list', 'a', 'b', 'c', 'd')
        
        # Test LPOP
        assert r.lpop('test_list') == 'a'
        assert r.lpop('test_list') == 'b'
        
        # Test RPOP
        assert r.rpop('test_list') == 'd'
        assert r.rpop('test_list') == 'c'
        
        # List should be empty
        assert r.lpop('test_list') is None

    def test_lrange_operations(self, redis_clean):
        """Test LRANGE with various indices"""
        r = redis_clean
        
        # Setup list with 10 items
        items = [f'item{i}' for i in range(10)]
        r.rpush('test_list', *items)
        
        # Test various ranges
        assert r.lrange('test_list', 0, 4) == items[0:5]
        assert r.lrange('test_list', 5, -1) == items[5:]
        assert r.lrange('test_list', -3, -1) == items[-3:]
        assert r.lrange('test_list', 0, -1) == items


class TestListAdvancedOperations:
    """Test advanced list operations"""

    def test_lindex_operations(self, redis_clean):
        """Test LINDEX operations"""
        r = redis_clean
        
        items = ['zero', 'one', 'two', 'three', 'four']
        r.rpush('test_list', *items)
        
        # Test positive indices
        assert r.lindex('test_list', 0) == 'zero'
        assert r.lindex('test_list', 2) == 'two'
        assert r.lindex('test_list', 4) == 'four'
        
        # Test negative indices
        assert r.lindex('test_list', -1) == 'four'
        assert r.lindex('test_list', -3) == 'two'
        
        # Test out of bounds
        assert r.lindex('test_list', 10) is None
        assert r.lindex('test_list', -10) is None

    def test_llen_operations(self, redis_clean):
        """Test LLEN operations"""
        r = redis_clean
        
        # Empty list
        assert r.llen('test_list') == 0
        
        # Add items and check length
        for i in range(5):
            r.rpush('test_list', f'item{i}')
            assert r.llen('test_list') == i + 1

    def test_lset_operations(self, redis_clean):
        """Test LSET operations"""
        r = redis_clean
        
        # Setup list
        r.rpush('test_list', 'a', 'b', 'c', 'd')
        
        # Test LSET
        assert r.lset('test_list', 0, 'new_a') == True
        assert r.lset('test_list', -1, 'new_d') == True
        
        # Verify changes
        assert r.lrange('test_list', 0, -1) == ['new_a', 'b', 'c', 'new_d']
        
        # Test out of bounds (should raise error)
        with pytest.raises(redis.ResponseError):
            r.lset('test_list', 10, 'invalid')


class TestListConcurrency:
    """Test concurrent access to lists"""

    def test_concurrent_lpush_rpush(self, redis_clean):
        """Test concurrent LPUSH and RPUSH operations"""
        r = redis_clean
        results = []
        
        def lpush_worker():
            for i in range(10):
                r.lpush('test_concurrent_list', f'left_{i}')
                
        def rpush_worker():
            for i in range(10):
                r.rpush('test_concurrent_list', f'right_{i}')
        
        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(lpush_worker),
                executor.submit(rpush_worker)
            ]
            
            for future in as_completed(futures):
                future.result()
        
        # Verify final list length
        assert r.llen('test_concurrent_list') == 20
        
        # Verify all items are present
        all_items = r.lrange('test_concurrent_list', 0, -1)
        left_items = [item for item in all_items if item.startswith('left_')]
        right_items = [item for item in all_items if item.startswith('right_')]
        
        assert len(left_items) == 10
        assert len(right_items) == 10

    def test_concurrent_pop_operations(self, redis_clean):
        """Test concurrent POP operations"""
        r = redis_clean
        
        # Setup list with 100 items
        items = [f'item_{i}' for i in range(100)]
        r.rpush('test_pop_list', *items)
        
        popped_items = []
        lock = threading.Lock()
        
        def pop_worker(pop_func):
            while True:
                try:
                    item = pop_func('test_pop_list')
                    if item is None:
                        break
                    with lock:
                        popped_items.append(item)
                except:
                    break
        
        # Run concurrent pop operations
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(pop_worker, r.lpop),
                executor.submit(pop_worker, r.rpop),
                executor.submit(pop_worker, r.lpop),
                executor.submit(pop_worker, r.rpop)
            ]
            
            for future in as_completed(futures):
                future.result()
        
        # Verify all items were popped
        assert len(popped_items) == 100
        assert r.llen('test_pop_list') == 0


class TestListDataConsistency:
    """Test data consistency for list operations"""

    def test_list_order_consistency(self, redis_clean):
        """Test that list maintains order consistency"""
        r = redis_clean
        
        # Test FIFO behavior with RPUSH/LPOP
        items = ['first', 'second', 'third', 'fourth']
        for item in items:
            r.rpush('test_fifo', item)
        
        popped = []
        while r.llen('test_fifo') > 0:
            popped.append(r.lpop('test_fifo'))
        
        assert popped == items
        
        # Test LIFO behavior with LPUSH/LPOP
        for item in items:
            r.lpush('test_lifo', item)
        
        popped = []
        while r.llen('test_lifo') > 0:
            popped.append(r.lpop('test_lifo'))
        
        assert popped == list(reversed(items))

    def test_list_atomicity(self, redis_clean):
        """Test atomicity of list operations"""
        r = redis_clean
        
        # Test that LPUSH with multiple values is atomic
        result = r.lpush('test_atomic', 'a', 'b', 'c', 'd')
        assert result == 4
        
        # All items should be present immediately
        assert r.llen('test_atomic') == 4
        assert r.lrange('test_atomic', 0, -1) == ['d', 'c', 'b', 'a']


class TestListRedisCompatibility:
    """Test Redis compatibility for list operations"""

    def test_redis_list_command_responses(self, redis_clean):
        """Test that responses match Redis behavior"""
        r = redis_clean
        
        # Test return values match Redis
        assert r.lpush('test_compat', 'item') == 1
        assert r.rpush('test_compat', 'item2') == 2
        assert r.llen('test_compat') == 2
        
        # Test LRANGE on non-existent key
        assert r.lrange('nonexistent', 0, -1) == []
        
        # Test LLEN on non-existent key
        assert r.llen('nonexistent') == 0
        
        # Test LPOP on non-existent key
        assert r.lpop('nonexistent') is None

    def test_list_error_conditions(self, redis_clean):
        """Test error conditions match Redis behavior"""
        r = redis_clean
        
        # Set a string key
        r.set('test_string', 'value')
        
        # List operations on string key should fail
        with pytest.raises(redis.ResponseError):
            r.lpush('test_string', 'item')
        
        with pytest.raises(redis.ResponseError):
            r.lrange('test_string', 0, -1)
        
        with pytest.raises(redis.ResponseError):
            r.llen('test_string')


class TestListPerformance:
    """Performance tests for list operations"""

    @pytest.mark.benchmark
    def test_large_list_operations(self, redis_clean):
        """Test operations on large lists"""
        r = redis_clean
        
        # Create large list
        batch_size = 1000
        items = [f'item_{i}' for i in range(batch_size)]
        
        start_time = time.time()
        r.rpush('test_large', *items)
        push_time = time.time() - start_time
        
        # Verify length
        assert r.llen('test_large') == batch_size
        
        # Test range operations
        start_time = time.time()
        result = r.lrange('test_large', 0, 99)
        range_time = time.time() - start_time
        
        assert len(result) == 100
        
        # Test pop operations
        start_time = time.time()
        for _ in range(100):
            r.lpop('test_large')
        pop_time = time.time() - start_time
        
        assert r.llen('test_large') == batch_size - 100
        
        print(f"Performance metrics:")
        print(f"  RPUSH {batch_size} items: {push_time:.3f}s")
        print(f"  LRANGE 100 items: {range_time:.3f}s")
        print(f"  LPOP 100 items: {pop_time:.3f}s")


if __name__ == '__main__':
    import sys
    try:
        import pytest
        sys.exit(pytest.main([__file__, '-v', '--tb=short']))
    except ImportError:
        print("pytest not available, install with: pip install pytest")
        sys.exit(1)