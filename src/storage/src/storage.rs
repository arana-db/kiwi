/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::base_value_format::DataType;
use crate::error::{MpscSnafu, Result};
use crate::options::OptionType;
use crate::slot_indexer::SlotIndexer;
use crate::{Redis, StorageOptions};
use kstd::lock_mgr::LockMgr;
use moka::sync::Cache;
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    pub slot_indexer: SlotIndexer,
    pub lock_mgr: Arc<LockMgr>,

    // For bg task
    pub bg_task_handler: Option<Arc<BgTaskHandler>>,
    pub bg_task: Option<tokio::task::JoinHandle<()>>,

    pub cursors_store: Arc<Cache<String, String>>,

    // For scan keys in data base
    pub db_instance_num: usize,
    pub db_id: usize,
    pub scan_keynum_exit: AtomicBool,

    pub is_opened: AtomicBool,
}

#[allow(dead_code)]
impl Storage {
    pub fn new(db_instance_num: usize, db_id: usize) -> Self {
        Self {
            insts: Vec::with_capacity(db_instance_num),
            slot_indexer: SlotIndexer::new(db_instance_num),
            is_opened: AtomicBool::new(false),
            lock_mgr: Arc::new(LockMgr::new(1000)),
            cursors_store: Arc::new(Cache::new(1000)),
            db_instance_num,
            db_id,
            bg_task_handler: None,
            bg_task: None,
            scan_keynum_exit: AtomicBool::new(false),
        }
    }

    pub fn open(
        &mut self,
        options: Arc<StorageOptions>,
        db_path: impl AsRef<Path>,
    ) -> Result<mpsc::Receiver<BgTask>> {
        let (handler, receiver) = BgTaskHandler::new();
        let handler_arc = Arc::new(handler);
        self.bg_task_handler = Some(Arc::clone(&handler_arc));

        let db_path = db_path.as_ref();
        let handler_for_redis = Arc::clone(&handler_arc);
        self.insts.clear();
        for i in 0..self.db_instance_num {
            let sub_path = db_path.join(i.to_string());
            let sub_path_str = match sub_path.to_str() {
                Some(s) => s,
                None => {
                    return crate::error::UnknownSnafu {
                        message: format!("Invalid path: {sub_path:?}"),
                    }
                    .fail();
                }
            };
            let mut inst = Redis::new(
                options.clone(),
                i as i32,
                Arc::clone(&handler_for_redis),
                Arc::clone(&self.lock_mgr),
            );
            if let Err(e) = inst.open(sub_path_str) {
                log::error!("open RocksDB{i} failed: {e:?}");
                self.insts.clear();
                self.is_opened.store(false, Ordering::SeqCst);
                return Err(e);
            }
            log::info!("open RocksDB{i} success!");
            self.insts.push(Arc::new(inst));
        }
        self.slot_indexer = SlotIndexer::new(self.db_instance_num);
        self.db_id = options.db_id;
        self.is_opened.store(true, Ordering::SeqCst);

        Ok(receiver)
    }

    pub async fn shutdown(&mut self) {
        if let Some(bg_task_handler) = self.bg_task_handler.as_ref() {
            let _ = bg_task_handler.send(BgTask::Shutdown).await;
        }
        if let Some(handle) = self.bg_task.take() {
            let _ = handle.await;
        }
    }

    /// usage:
    /// let mut storage = Storage::new(...);
    /// let receiver = storage.open(...)?;
    /// let storage = Arc::new(storage);
    /// tokio::spawn(Storage::bg_task_worker(storage.clone(), receiver));
    async fn bg_task_worker(storage: Arc<Storage>, mut receiver: mpsc::Receiver<BgTask>) {
        while let Some(event) = receiver.recv().await {
            match event {
                BgTask::CleanAll { dtype } => {
                    log::info!("Cleaning all for type: {dtype:?}");
                }
                BgTask::CompactRange { dtype, start, end } => {
                    log::info!("Compacting range: {start} - {end} for type: {dtype:?}");
                    if let Some(redis) = storage.insts.first() {
                        if let Some(db) = &redis.db {
                            db.compact_range(Some(start), Some(end));
                        }
                    }
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

    fn set_option(&self, option_type: OptionType, options: &HashMap<String, String>) -> Result<()> {
        for inst in &self.insts {
            inst.set_option(option_type, options)?;
        }
        Ok(())
    }
}
