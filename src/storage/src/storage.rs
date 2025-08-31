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
use foyer::{Cache, CacheBuilder};
use kstd::lock_mgr::LockMgr;
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub enum TaskType {
    None = 0,
    CleanAll = 1,
    CompactRange = 2,
}

impl From<u8> for TaskType {
    fn from(value: u8) -> Self {
        match value {
            1 => TaskType::CleanAll,
            2 => TaskType::CompactRange,
            _ => TaskType::None,
        }
    }
}

impl From<TaskType> for u8 {
    fn from(value: TaskType) -> Self {
        value as u8
    }
}

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
    CompactSpecificKey {
        dtype: DataType,
        key: String,
    },
    Shutdown, // For shutdown bg task
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
    pub is_opened: AtomicBool,

    // For bg task
    pub bg_task_handler: Option<Arc<BgTaskHandler>>,
    pub bg_task: Option<tokio::task::JoinHandle<()>>,
    pub current_task_type: AtomicU8,
    pub ignore_tasks: AtomicBool,

    pub cursors_store: Arc<Cache<String, String>>,

    // For scan keys in data base
    pub db_instance_num: usize,
    pub db_id: usize,
    pub scan_keynum_exit: AtomicBool,
}

#[allow(dead_code)]
impl Storage {
    pub fn new(db_instance_num: usize, db_id: usize) -> Self {
        Self {
            insts: Vec::with_capacity(db_instance_num),
            slot_indexer: SlotIndexer::new(db_instance_num),
            is_opened: AtomicBool::new(false),
            lock_mgr: Arc::new(LockMgr::new(1000)),
            cursors_store: Arc::new(CacheBuilder::new(1000).build()),
            db_instance_num,
            db_id,
            bg_task_handler: None,
            bg_task: None,
            current_task_type: AtomicU8::new(TaskType::None.into()),
            scan_keynum_exit: AtomicBool::new(false),
            ignore_tasks: AtomicBool::new(false),
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

    pub fn get_current_task_type(&self) -> String {
        let task_type = self.current_task_type.load(Ordering::Relaxed);
        match task_type.into() {
            TaskType::None => "None".to_string(),
            TaskType::CleanAll => "CleanAll".to_string(),
            TaskType::CompactRange => "CompactRange".to_string(),
        }
    }

    fn set_current_task_type(&self, task_type: TaskType) {
        self.current_task_type
            .store(task_type.into(), Ordering::Relaxed);
    }

    fn clear_current_task_type(&self) {
        self.current_task_type
            .store(TaskType::None.into(), Ordering::Relaxed);
    }

    fn set_ignore_tasks(&self, ignore: bool) {
        self.ignore_tasks.store(ignore, Ordering::SeqCst);
    }

    fn is_ignoring_tasks(&self) -> bool {
        self.ignore_tasks.load(Ordering::SeqCst)
    }

    /// usage:
    /// let mut storage = Storage::new(...);
    /// let receiver = storage.open(...)?;
    /// let storage = Arc::new(storage);
    /// tokio::spawn(Storage::bg_task_worker(storage.clone(), receiver));
    pub async fn bg_task_worker(storage: Arc<Storage>, mut receiver: mpsc::Receiver<BgTask>) {
        while let Some(event) = receiver.recv().await {
            match event {
                BgTask::CleanAll { dtype } => {
                    log::info!("BgTaskWorker received CleanAll {dtype:?}");
                    storage.set_ignore_tasks(false);
                    storage.set_current_task_type(TaskType::CleanAll);
                    if let Err(e) = storage.do_compact_range(dtype, "", "") {
                        log::error!("BgTaskWorker CleanAll failed: {e:?}");
                    }
                    storage.clear_current_task_type();
                }
                BgTask::CompactRange { dtype, start, end } => {
                    if storage.is_ignoring_tasks() {
                        log::info!("Ignoring compact range task due to ignore tasks");
                        continue;
                    }
                    log::info!("BgTaskWorker received CompactRange {dtype:?} {start} {end}");
                    storage.set_current_task_type(TaskType::CompactRange);
                    if let Err(e) = storage.do_compact_range(dtype, &start, &end) {
                        log::error!("BgTaskWorker CompactRange failed: {e:?}");
                    }
                    storage.clear_current_task_type();
                }
                BgTask::CompactSpecificKey { dtype, key } => {
                    if storage.is_ignoring_tasks() {
                        log::info!("Ignoring compact specific key task due to ignore tasks");
                        continue;
                    }
                    log::info!("BgTaskWorker received CompactSpecificKey {dtype:?} {key}");
                    storage.set_current_task_type(TaskType::CompactRange);
                    if let Err(e) = storage.do_compact_specific_key(dtype, &key) {
                        log::error!("BgTaskWorker CompactSpecificKey failed: {e:?}");
                    }
                    storage.clear_current_task_type();
                }
                BgTask::Shutdown => {
                    log::info!("BgTaskWorker received Shutdown, exiting...");
                    break;
                }
            }
        }
    }

    // use for admin command and cron job
    pub async fn compact_all(&self, sync: bool) -> Result<()> {
        if sync {
            log::info!("Executing compact ALL synchronously");
            self.do_compact_range(DataType::All, "", "")?;
        } else {
            log::info!("Adding compact ALL to background queue, setting ignore flag");
            self.set_ignore_tasks(true);
            if let Some(handler) = &self.bg_task_handler {
                handler
                    .send(BgTask::CleanAll {
                        dtype: DataType::All,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    // use for admin command
    pub async fn compact_range(
        &self,
        dtype: DataType,
        start: &str,
        end: &str,
        sync: bool,
    ) -> Result<()> {
        if sync {
            log::info!("Executing compact range synchronously: start={start}, end={end}",);
            self.do_compact_range(dtype, start, end)?;
        } else {
            log::info!("Adding compact range to background queue: start={start}, end={end}",);
            if let Some(handler) = &self.bg_task_handler {
                handler
                    .send(BgTask::CompactRange {
                        dtype,
                        start: start.to_string(),
                        end: end.to_string(),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    fn do_compact_range(&self, _dtype: DataType, _start: &str, _end: &str) -> Result<()> {
        unimplemented!("This function is not implemented yet");
    }

    fn do_compact_specific_key(&self, _dtype: DataType, _key: &str) -> Result<()> {
        unimplemented!("This function is not implemented yet");
    }

    // Used to modify rocksdb dynamic options
    pub fn set_option(
        &self,
        option_type: OptionType,
        options: &HashMap<String, String>,
    ) -> Result<()> {
        for key in options.keys() {
            StorageOptions::validate_dynamic_option(option_type, key.as_str())?;
        }

        for inst in &self.insts {
            inst.set_option(option_type, options)?;
        }
        Ok(())
    }

    pub fn create_check_point() {
        unimplemented!("This function is not implemented yet");
    }

    pub fn create_check_point_internal() {
        unimplemented!("This function is not implemented yet");
    }

    pub fn load_check_point() {
        unimplemented!("This function is not implemented yet");
    }

    pub fn load_check_point_internal() {
        unimplemented!("This function is not implemented yet");
    }

    pub fn load_cursor_start_key() {
        unimplemented!("This function is not implemented yet");
    }

    pub fn store_cursor_start_key() {
        unimplemented!("This function is not implemented yet");
    }
}
