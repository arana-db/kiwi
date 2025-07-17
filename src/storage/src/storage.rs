//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::base_value_format::DataType;
use crate::error::{MpscSnafu, Result};
use crate::lru_cache::LRUCache;
use crate::slot_indexer::SlotIndexer;
use crate::{Redis, StorageOptions};
use kstd::lock_mgr::LockMgr;
use snafu::ResultExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum BgTask {
    CleanAll {
        dtype: DataType,
    },
    CompactRange {
        dtype: DataType,
        start: String,
        end: String,
    },
    // For shutdown bg task
    Shutdown,
    None,
}

pub struct BgTaskHandler {
    sender: mpsc::Sender<BgTask>,
}

impl BgTaskHandler {
    pub fn new() -> (Self, mpsc::Receiver<BgTask>) {
        let (sender, receiver) = mpsc::channel(1000);
        (Self { sender }, receiver)
    }

    pub async fn send(&self, task: BgTask) -> Result<()> {
        self.sender.send(task).await.context(MpscSnafu)?;
        Ok(())
    }
}

#[allow(dead_code)]
pub struct Storage {
    pub insts: Vec<Arc<Redis>>,
    pub slot_indexer: Arc<SlotIndexer>,
    pub lock_mgr: Arc<LockMgr>,

    // For bg task
    pub bg_task_handler: Arc<BgTaskHandler>,
    pub bg_task: Option<tokio::task::JoinHandle<()>>,

    pub cursors_store: Mutex<LRUCache<String, String>>,

    // For scan keys in data base
    pub db_instance_num: usize,
    pub db_id: usize,
    pub scan_keynum_exit: AtomicBool,

    pub is_opened: AtomicBool,
}

#[allow(dead_code)]
impl Storage {
    pub fn new(db_instance_num: usize, db_id: usize) -> Self {
        let (bg_task_handler, receiver) = BgTaskHandler::new();
        let bg_task_handler = Arc::new(bg_task_handler);
        let bg_task = Some(tokio::spawn(Self::bg_task_worker(receiver)));

        Self {
            insts: Vec::with_capacity(db_instance_num),
            slot_indexer: Arc::new(SlotIndexer::new(db_instance_num)),
            is_opened: AtomicBool::new(false),
            lock_mgr: Arc::new(LockMgr::new(1000)),
            cursors_store: Mutex::new(LRUCache::with_capacity(1000)),
            db_instance_num,
            db_id,
            bg_task_handler,
            bg_task,
            scan_keynum_exit: AtomicBool::new(false),
        }
    }

    pub fn open(&mut self, options: Arc<StorageOptions>, db_path: impl AsRef<Path>) -> Result<()> {
        let db_path = db_path.as_ref();
        self.insts.clear();
        let mut new_insts = Vec::with_capacity(self.db_instance_num);

        for i in 0..self.db_instance_num {
            let sub_path = db_path.join(i.to_string());
            let sub_path_str = match sub_path.to_str() {
                Some(s) => s,
                None => {
                    return crate::error::UnknownSnafu {
                        message: format!("Invalid path: {:?}", sub_path),
                    }
                    .fail();
                }
            };

            let mut inst = Redis::new(
                options.clone(),
                i as i32,
                self.bg_task_handler.clone(),
                self.lock_mgr.clone(),
            );
            if let Err(e) = inst.open(sub_path_str) {
                log::error!("open RocksDB{} failed: {:?}", i, e);
                new_insts.clear();
                self.is_opened.store(false, Ordering::SeqCst);
                return Err(e);
            }
            log::info!("open RocksDB{} success!", i);
            new_insts.push(Arc::new(inst));
        }

        self.slot_indexer = Arc::new(SlotIndexer::new(self.db_instance_num));
        self.db_id = options.db_id;
        self.insts = new_insts;
        self.is_opened.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        let _ = self.bg_task_handler.send(BgTask::Shutdown).await;
        if let Some(handle) = self.bg_task.take() {
            let _ = handle.await;
        }
    }

    async fn bg_task_worker(mut receiver: mpsc::Receiver<BgTask>) {
        while let Some(event) = receiver.recv().await {
            match event {
                BgTask::CleanAll { dtype } => {
                    Self::handle_clean_all(dtype).await;
                }
                BgTask::CompactRange { dtype, start, end } => {
                    Self::handle_compact_range(dtype, &start, &end).await;
                }
                BgTask::Shutdown => {
                    log::info!("BgTaskWorker received Shutdown, exiting...");
                    break;
                }
                BgTask::None => {
                    // do nothing
                }
            }
        }
    }

    async fn handle_compact_range(dtype: DataType, start: &str, end: &str) {
        log::info!(
            "Compacting range: {} - {} for type: {:?}",
            start,
            end,
            dtype
        );
    }

    async fn handle_clean_all(dtype: DataType) {
        log::info!("Cleaning all for type: {:?}", dtype);
    }
}
