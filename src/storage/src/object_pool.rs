// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use parking_lot::Mutex;
use std::sync::Arc;

pub struct ObjectPool<T> {
    objects: Arc<Mutex<Vec<T>>>,
    create_fn: Box<dyn Fn() -> T + Send + Sync>,
    max_pool_size: usize,
}

impl<T> ObjectPool<T> {
    pub fn new<F>(create_fn: F, max_pool_size: usize) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        Self {
            objects: Arc::new(Mutex::new(Vec::new())),
            create_fn: Box::new(create_fn),
            max_pool_size,
        }
    }

    pub fn acquire(&self) -> T {
        let mut objects = self.objects.lock();
        objects.pop().unwrap_or_else(|| (self.create_fn)())
    }

    pub fn release(&self, obj: T) {
        let mut objects = self.objects.lock();
        if objects.len() < self.max_pool_size {
            objects.push(obj);
        }
    }

    #[allow(dead_code)]
    pub fn pool_size(&self) -> usize {
        self.objects.lock().len()
    }
}

pub struct BufferPool {
    small_buffers: ObjectPool<Vec<u8>>,
    medium_buffers: ObjectPool<Vec<u8>>,
    large_buffers: ObjectPool<Vec<u8>>,
}

impl BufferPool {
    pub fn new() -> Self {
        Self {
            small_buffers: ObjectPool::new(|| Vec::with_capacity(1024), 100),
            medium_buffers: ObjectPool::new(|| Vec::with_capacity(8192), 50),
            large_buffers: ObjectPool::new(|| Vec::with_capacity(65536), 20),
        }
    }

    pub fn acquire_buffer(&self, size: usize) -> Vec<u8> {
        match size {
            0..=1024 => self.small_buffers.acquire(),
            1025..=8192 => self.medium_buffers.acquire(),
            _ => self.large_buffers.acquire(),
        }
    }

    pub fn release_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        let capacity = buffer.capacity();
        match capacity {
            0..=1024 => self.small_buffers.release(buffer),
            1025..=8192 => self.medium_buffers.release(buffer),
            _ => self.large_buffers.release(buffer),
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WriteBatchPool {
    pool: ObjectPool<rocksdb::WriteBatch>,
}

impl WriteBatchPool {
    pub fn new(max_pool_size: usize) -> Self {
        Self {
            pool: ObjectPool::new(rocksdb::WriteBatch::default, max_pool_size),
        }
    }

    pub fn acquire(&self) -> rocksdb::WriteBatch {
        self.pool.acquire()
    }

    pub fn release(&self, _batch: rocksdb::WriteBatch) {
        // WriteBatch没有reset方法，我们直接丢弃
        // 在实际使用中，可以考虑其他策略
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new();

        let small_buffer = pool.acquire_buffer(512);
        assert_eq!(small_buffer.capacity(), 1024);

        let medium_buffer = pool.acquire_buffer(4096);
        assert_eq!(medium_buffer.capacity(), 8192);

        let large_buffer = pool.acquire_buffer(32768);
        assert_eq!(large_buffer.capacity(), 65536);

        pool.release_buffer(small_buffer);
        pool.release_buffer(medium_buffer);
        pool.release_buffer(large_buffer);
    }
}
