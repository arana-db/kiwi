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

Tests Redis list commands for compatibility and correctness
"""

import redis
import pytest
import threading
import time


class TestListBasicOperations:
    """Test basic list operations"""

    def test_lpush_rpush(self, redis_clean):
        """Test LPUSH and RPUSH operations"""
        r = redis_clean
        
        # Test LPUSH
        assert r.lpush('test_list', 'item1') == 1
        assert r.lpush('test_list', 'item2') == 2
        assert r.lpush('test_list', 'item3', 'item4') == 4
        
        # Test RPUSH
        assert r.rpush('test_list2', 'a') == 1
        assert r.rpush('test_list2', 'b', 'c') == 3
        
        # Verify list contents
        assert r.lrange('test_list', 0, -1) == ['item4', 'item3', 'item2', 'item1']
        assert r.lrange('test_list2', 0, -1) == ['a', 'b', 'c']

    def test_lpop_rpop(self, redis_clean):
        """Test LPOP and RPOP operations"""
        r = redis_clean
        
        # Setup list
        r.rpush('test_list', 'a', 'b', 'c', 'd')
        
        # Test LPOP
        assert r.lpop('test_list') == 'a'
        assert r.lpop('test_list') == 'b'
        
        # Test RPOP
        assert r.rpop('test_list') == 'd'
        assert r.rpop('test_list') == 'c'
        
        # Test empty list
        assert r.lpop('test_list') is None
        assert r.rpop('test_list') is None

    def test_llen(self, redis_clean):
        """Test LLEN operation"""
        r = redis_clean
        
        # Empty list
        assert r.llen('test_list') == 0
        
        # Add items
        r.rpush('test_list', 'a', 'b', 'c')
        assert r.llen('test_list') == 3
        
        # Remove items
        r.lpop('test_list')
        assert r.llen('test_list') == 2

    def test_lrange(self, redis_clean):
        """Test LRANGE operation"""
        r = redis_clean
        
        # Setup list
        r.rpush('test_list', 'a', 'b', 'c', 'd', 'e')
        
        # Test various ranges
        assert r.lrange('test_list', 0, 2) == ['a', 'b', 'c']
        assert r.lrange('test_list', 1, 3) == ['b', 'c', 'd']
        assert r.lrange('test_list', 0, -1) == ['a', 'b', 'c', 'd', 'e']
        assert r.lrange('test_list', -2, -1) == ['d', 'e']
        assert r.lrange('test_list', 2, 1) == []


class TestListAdvancedOperations:
    """Test advanced list operations"""

    def test_lindex(self, redis_clean):
        """Test LINDEX operation"""
        r = redis_clean
        
        # Setup list
        r.rpush('test_list', 'zero', 'one', 'two', 'three')
        
        # Test positive indices
        assert r.lindex('test_list', 0) == 'zero'
        assert r.lindex('test_list', 2) == 'two'
        
        # Test negative indices
        assert r.lindex('test_list', -1) == 'three'
        assert r.lindex('test_list', -2) == 'two'
        
        # Test out of range
        assert r.lindex('test_list', 10) is None
        assert r.lindex('test_list', -10) is None

    def test_lset(self, redis_clean):
        """Test LSET operation"""
        r = redis_clean
        
        # Setup list
        r.rpush('test_list', 'a', 'b', 'c')
        
        # Test LSET
        assert r.lset('test_list', 1, 'modified') == True
        assert r.lrange('test_list', 0, -1) == ['a', 'modified', 'c']
        
        # Test negative index
        assert r.lset('test_list', -1, 'last') == True
        assert r.lrange('test_list', 0, -1) == ['a', 'modified', 'last']

    def test_linsert(self, redis_clean):
        """Test LINSERT operation"""
        r = redis_clean
        
        # Setup list
        r.rpush('test_list', 'a', 'c', 'd')
        
        # Test LINSERT BEFORE
        assert r.linsert('test_list', 'BEFORE', 'c', 'b') == 4
        assert r.lrange('test_list', 0, -1) == ['a', 'b', 'c', 'd']
        
        # Test LINSERT AFTER
        assert r.linsert('test_list', 'AFTER', 'c', 'c2') == 5
        assert r.lrange('test_list', 0, -1) == ['a', 'b', 'c', 'c2', 'd']
        
        # Test pivot not found
        assert r.linsert('test_list', 'BEFORE', 'notfound', 'x') == -1

    def test_lrem(self, redis_clean):
        """Test LREM operation"""
        r = redis_clean
        
        # Setup list with duplicates
        r.rpush('test_list', 'a', 'b', 'a', 'c', 'a', 'b')
        
        # Remove first 2 occurrences of 'a'
        assert r.lrem('test_list', 2, 'a') == 2
        assert r.lrange('test_list', 0, -1) == ['b', 'c', 'a', 'b']
        
        # Remove all occurrences of 'b'
        assert r.lrem('test_list', 0, 'b') == 2
        assert r.lrange('test_list', 0, -1) == ['c', 'a']

    def test_ltrim(self, redis_clean):
        """Test LTRIM operation"""
        r = redis_clean
        
        # Setup list
        r.rpush('test_list', 'a', 'b', 'c', 'd', 'e', 'f')
        
        # Trim to keep middle elements
        assert r.ltrim('test_list', 1, 4) == True
        assert r.lrange('test_list', 0, -1) == ['b', 'c', 'd', 'e']


class TestListConcurrency:
    """Test list operations under concurrent access"""

    def test_concurrent_push_pop(self, redis_clean):
        """Test concurrent push and pop operations"""
        r = redis_clean
        results = []
        errors = []
        
        def push_worker():
            try:
                for i in range(100):
                    r.lpush('test_concurrent', f'item_{i}')
            except Exception as e:
                errors.append(e)
        
        def pop_worker():
            try:
                for _ in range(50):
                    item = r.rpop('test_concurrent')
                    if item:
                        results.append(item)
                    time.sleep(0.001)  # Small delay
            except Exception as e:
                errors.append(e)
        
        # Start threads
        threads = []
        threads.append(threading.Thread(target=push_worker))
        threads.append(threading.Thread(target=pop_worker))
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        # Verify no errors occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"
        
        # Verify final state is consistent
        final_length = r.llen('test_concurrent')
        assert final_length >= 0  # Should be non-negative
        assert final_length + len(results) == 100  # Total items should match

    def test_data_consistency(self, redis_clean):
        """Test data consistency across operations"""
        r = redis_clean
        
        # Setup initial state
        r.rpush('test_consistency', 'initial')
        
        def modifier():
            for i in range(10):
                r.lpush('test_consistency', f'left_{i}')
                r.rpush('test_consistency', f'right_{i}')
        
        def reader():
            for _ in range(20):
                length = r.llen('test_consistency')
                if length > 0:
                    items = r.lrange('test_consistency', 0, -1)
                    assert len(items) == length, "Length mismatch"
                time.sleep(0.001)
        
        # Run concurrent operations
        threads = [
            threading.Thread(target=modifier),
            threading.Thread(target=reader)
        ]
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        # Final consistency check
        final_length = r.llen('test_consistency')
        final_items = r.lrange('test_consistency', 0, -1)
        assert len(final_items) == final_length


class TestListCompatibility:
    """Test Redis compatibility"""

    def test_redis_client_compatibility(self, redis_clean):
        """Test compatibility with redis-py client"""
        r = redis_clean
        
        # Test all basic operations work with redis-py
        operations = [
            lambda: r.lpush('compat_test', 'a', 'b'),
            lambda: r.rpush('compat_test', 'c', 'd'),
            lambda: r.llen('compat_test'),
            lambda: r.lrange('compat_test', 0, -1),
            lambda: r.lindex('compat_test', 1),
            lambda: r.lpop('compat_test'),
            lambda: r.rpop('compat_test'),
        ]
        
        for op in operations:
            try:
                result = op()
                # Just ensure no exceptions are raised
                assert result is not None or result == 0 or result == []
            except Exception as e:
                pytest.fail(f"Operation failed: {e}")

    def test_error_responses(self, redis_clean):
        """Test proper error responses"""
        r = redis_clean
        
        # Set up non-list key
        r.set('not_a_list', 'string_value')
        
        # Test operations on wrong type should raise errors
        with pytest.raises(redis.ResponseError):
            r.lpush('not_a_list', 'item')
        
        with pytest.raises(redis.ResponseError):
            r.llen('not_a_list')
        
        with pytest.raises(redis.ResponseError):
            r.lrange('not_a_list', 0, -1)


class TestListEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_empty_list_operations(self, redis_clean):
        """Test operations on empty lists"""
        r = redis_clean
        
        # Operations on non-existent list
        assert r.llen('nonexistent') == 0
        assert r.lrange('nonexistent', 0, -1) == []
        assert r.lindex('nonexistent', 0) is None
        assert r.lpop('nonexistent') is None
        assert r.rpop('nonexistent') is None

    def test_large_list_operations(self, redis_clean):
        """Test operations on large lists"""
        r = redis_clean
        
        # Create large list
        items = [f'item_{i}' for i in range(1000)]
        r.rpush('large_list', *items)
        
        # Test operations
        assert r.llen('large_list') == 1000
        assert r.lindex('large_list', 500) == 'item_500'
        assert r.lrange('large_list', 0, 9) == items[:10]
        assert r.lrange('large_list', -10, -1) == items[-10:]

    def test_binary_data(self, redis_binary_client):
        """Test list operations with binary data"""
        r = redis_binary_client
        
        # Binary data
        binary_items = [b'binary\x00data', b'\xff\xfe\xfd', bytes(range(256))]
        
        # Test operations with binary data
        for item in binary_items:
            r.lpush(b'binary_list', item)
        
        # Verify binary data integrity
        retrieved = r.lrange(b'binary_list', 0, -1)
        assert len(retrieved) == 3
        for original, retrieved_item in zip(reversed(binary_items), retrieved):
            assert original == retrieved_item
        
        # Cleanup
        r.delete(b'binary_list')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])