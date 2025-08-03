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


//! Storage engine implementation using RocksDB

use rocksdb::*;
use std::path::Path;

use crate::{Result, StorageError, StorageOptions};

pub struct Engine {
    db: DB,
}

impl Engine {
    pub fn open(opts: StorageOptions) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(opts.create_if_missing);
        options.set_max_open_files(opts.max_open_files);
        options.set_write_buffer_size(opts.write_buffer_size);
        options.set_max_write_buffer_number(opts.max_write_buffer_number);
        options.set_target_file_size_base(opts.target_file_size_base);
        options.set_max_background_jobs(opts.background_jobs);
        options.set_use_direct_io_for_flush_and_compaction(opts.use_direct_io_for_flush_and_compaction);

        // Optimize RocksDB options
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576); // 1MB
        options.set_compaction_style(DBCompactionStyle::Level);
        options.set_disable_auto_compactions(false);
        
        let db = DB::open(&options, Path::new(&opts.db_path))
            .map_err(StorageError::from)?;

        Ok(Self { db })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key).map_err(StorageError::from)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value).map_err(StorageError::from)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key).map_err(StorageError::from)
    }

    pub fn batch_write(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch).map_err(StorageError::from)
    }

    pub fn new_iterator(&self, opts: ReadOptions) -> DBIterator {
        self.db.iterator(IteratorMode::Start)
    }

    pub fn snapshot(&self) -> Snapshot {
        self.db.snapshot()
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush().map_err(StorageError::from)
    }

    pub fn compact_range<K: AsRef<[u8]>>(&self, start: Option<K>, end: Option<K>) -> Result<()> {
        self.db.compact_range(start, end);
        Ok(())
    }
}