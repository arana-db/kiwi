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

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use std::collections::HashMap;
use crate::error::Result;

/// 批处理操作
#[derive(Debug, Clone)]
pub struct BatchOperation {
    pub key: String,
    pub value: Vec<u8>,
    pub ttl: Option<u64>,
    pub operation_type: BatchOperationType,
}

/// 批处理操作类型
#[derive(Debug, Clone)]
pub enum BatchOperationType {
    Set,
    Delete,
    Expire,
}

/// 批处理结果
#[derive(Debug, Clone)]
pub struct BatchResult {
    pub success: bool,
    pub error: Option<String>,
    pub duration_micros: u64,
}

/// 批处理回调
pub type BatchCallback = Box<dyn FnOnce(BatchResult) + Send>;

/// 异步批处理器
pub struct AsyncBatchProcessor {
    sender: mpsc::Sender<BatchRequest>,
    processor: Arc<dyn BatchProcessor + Send + Sync>,
}

/// 批处理请求
pub struct BatchRequest {
    pub operations: Vec<BatchOperation>,
    pub callback: BatchCallback,
    pub atomic: bool,
}

/// 批处理器trait
pub trait BatchProcessor {
    fn process_batch(&self, operations: &[BatchOperation]) -> Result<()>;
}

impl AsyncBatchProcessor {
    pub fn new(processor: Arc<dyn BatchProcessor + Send + Sync>) -> Self {
        let (sender, mut receiver) = mpsc::channel(1000);
        let processor_clone = Arc::clone(&processor);
        
        // 启动批处理任务
        tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut callbacks = Vec::new();
            let mut last_flush = Instant::now();
            let flush_interval = Duration::from_millis(10);
            let max_batch_size = 100;
            
            while let Some(request) = receiver.recv().await {
                batch.extend(request.operations);
                callbacks.push(request.callback);
                
                // 当批次达到阈值或超时时处理
                let should_flush = batch.len() >= max_batch_size || 
                                 last_flush.elapsed() >= flush_interval;
                
                if should_flush {
                    Self::process_batch(&processor_clone, &mut batch, &mut callbacks).await;
                    last_flush = Instant::now();
                }
            }
            
            // 处理剩余的批次
            if !batch.is_empty() {
                Self::process_batch(&processor_clone, &mut batch, &mut callbacks).await;
            }
        });
        
        Self { sender, processor }
    }
    
    async fn process_batch(
        processor: &Arc<dyn BatchProcessor + Send + Sync>,
        batch: &mut Vec<BatchOperation>,
        callbacks: &mut Vec<BatchCallback>,
    ) {
        let start_time = Instant::now();
        
        let result = processor.process_batch(batch);
        let duration = start_time.elapsed().as_micros() as u64;
        
        let batch_result = match result {
            Ok(()) => BatchResult {
                success: true,
                error: None,
                duration_micros: duration,
            },
            Err(e) => BatchResult {
                success: false,
                error: Some(e.to_string()),
                duration_micros: duration,
            },
        };
        
        // 调用回调函数
        for callback in callbacks.drain(..) {
            callback(batch_result.clone());
        }
        
        batch.clear();
    }
    
    pub async fn submit_batch(
        &self,
        operations: Vec<BatchOperation>,
        callback: BatchCallback,
    ) -> Result<()> {
        let request = BatchRequest {
            operations,
            callback,
            atomic: true,
        };
        
        self.sender.send(request).await
            .map_err(|_| crate::error::Error::System {
                message: "Failed to submit batch".to_string(),
            })
    }
}

/// 简单的批处理器实现
pub struct SimpleBatchProcessor {
    storage: Arc<dyn StorageInterface + Send + Sync>,
}

/// 存储接口trait
pub trait StorageInterface {
    fn set(&self, key: &str, value: &[u8], ttl: Option<u64>) -> Result<()>;
    fn delete(&self, key: &str) -> Result<()>;
    fn expire(&self, key: &str, ttl: u64) -> Result<()>;
}

impl SimpleBatchProcessor {
    pub fn new(storage: Arc<dyn StorageInterface + Send + Sync>) -> Self {
        Self { storage }
    }
}

impl BatchProcessor for SimpleBatchProcessor {
    fn process_batch(&self, operations: &[BatchOperation]) -> Result<()> {
        for operation in operations {
            match operation.operation_type {
                BatchOperationType::Set => {
                    self.storage.set(&operation.key, &operation.value, operation.ttl)?;
                }
                BatchOperationType::Delete => {
                    self.storage.delete(&operation.key)?;
                }
                BatchOperationType::Expire => {
                    if let Some(ttl) = operation.ttl {
                        self.storage.expire(&operation.key, ttl)?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// 批处理统计
#[derive(Debug, Clone, Default)]
pub struct BatchStatistics {
    pub total_batches: u64,
    pub total_operations: u64,
    pub successful_batches: u64,
    pub failed_batches: u64,
    pub avg_batch_size: f64,
    pub avg_batch_duration: f64, // 微秒
    pub max_batch_duration: u64, // 微秒
    pub min_batch_duration: u64, // 微秒
}

/// 批处理管理器
pub struct BatchManager {
    processor: AsyncBatchProcessor,
    stats: Arc<tokio::sync::RwLock<BatchStatistics>>,
}

impl BatchManager {
    pub fn new(storage: Arc<dyn StorageInterface + Send + Sync>) -> Self {
        let simple_processor = SimpleBatchProcessor::new(storage);
        let processor = AsyncBatchProcessor::new(Arc::new(simple_processor));
        
        Self {
            processor,
            stats: Arc::new(tokio::sync::RwLock::new(BatchStatistics::default())),
        }
    }
    
    pub async fn set(&self, key: String, value: Vec<u8>, ttl: Option<u64>) -> Result<()> {
        let operation = BatchOperation {
            key,
            value,
            ttl,
            operation_type: BatchOperationType::Set,
        };
        
        let stats_clone = Arc::clone(&self.stats);
        let callback = Box::new(move |result: BatchResult| {
            let stats = stats_clone.blocking_read();
            // 更新统计信息
            // 这里简化处理，实际应该异步更新
        });
        
        self.processor.submit_batch(vec![operation], callback).await
    }
    
    pub async fn delete(&self, key: String) -> Result<()> {
        let operation = BatchOperation {
            key,
            value: Vec::new(),
            ttl: None,
            operation_type: BatchOperationType::Delete,
        };
        
        let stats_clone = Arc::clone(&self.stats);
        let callback = Box::new(move |result: BatchResult| {
            let stats = stats_clone.blocking_read();
            // 更新统计信息
        });
        
        self.processor.submit_batch(vec![operation], callback).await
    }
    
    pub async fn get_statistics(&self) -> BatchStatistics {
        self.stats.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    /// 模拟存储实现
    struct MockStorage {
        data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                data: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    impl StorageInterface for MockStorage {
        fn set(&self, key: &str, value: &[u8], _ttl: Option<u64>) -> Result<()> {
            self.data.write().insert(key.to_string(), value.to_vec());
            Ok(())
        }

        fn delete(&self, key: &str) -> Result<()> {
            self.data.write().remove(key);
            Ok(())
        }

        fn expire(&self, _key: &str, _ttl: u64) -> Result<()> {
            // 简化实现
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_batch_manager() {
        let storage = Arc::new(MockStorage::new());
        let manager = BatchManager::new(storage);
        
        // 测试设置操作
        let result = manager.set(
            "key1".to_string(),
            b"value1".to_vec(),
            None,
        ).await;
        
        assert!(result.is_ok());
        
        // 测试删除操作
        let result = manager.delete("key1".to_string()).await;
        assert!(result.is_ok());
    }
} 