// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! RaftLogStorage implementation for openraft integration

use std::fmt::Debug;
use std::ops::RangeBounds;

use openraft::storage::{LogState, RaftLogStorage};
use openraft::{Entry, LogId, RaftLogReader, RaftTypeConfig, StorageError, Vote};
use async_trait::async_trait;

use crate::error::RaftError;
use crate::storage::{RaftStorage, StoredLogEntry};
use crate::types::{NodeId, TypeConfig};

/// Implementation of RaftLogStorage trait for RaftStorage
#[async_trait::async_trait]
impl RaftLogStorage<TypeConfig> for RaftStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        // Get the last log entry to determine the log state
        let last_log_entry = self
            .get_last_log_entry()
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })?;

        let last_log_id = match last_log_entry {
            Some(entry) => Some(LogId::new(
                openraft::CommittedLeaderId::new(entry.term, NodeId::default()),
                entry.index,
            )),
            None => None,
        };

        // Get the last purged log ID (for now, assume no purging)
        let last_purged_log_id = None;

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        _committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        // For now, we don't need to persist the committed index separately
        // as it's managed by openraft internally
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        // Return None as we don't persist committed index separately
        Ok(None)
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Save the current term and voted_for
        self.set_current_term(vote.term)
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })?;

        self.set_voted_for(vote.node_id)
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })?;

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let current_term = self.get_current_term();
        let voted_for = self.get_voted_for();

        if current_term > 0 {
            Ok(Some(Vote {
                term: current_term,
                node_id: voted_for,
            }))
        } else {
            Ok(None)
        }
    }

    async fn append<I>(&mut self, entries: I, callback: openraft::storage::LogFlushed<TypeConfig>) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries_vec: Vec<_> = entries.into_iter().collect();
        
        for entry in entries_vec {
            let stored_entry = StoredLogEntry {
                index: entry.log_id.index,
                term: entry.log_id.leader_id.term,
                payload: bincode::serialize(&entry.payload).map_err(|e| StorageError::IO {
                    source: Box::new(RaftError::Storage(crate::error::StorageError::DataInconsistency {
                        message: format!("Failed to serialize entry payload: {}", e),
                    })),
                })?,
            };

            self.append_log_entry(&stored_entry)
                .map_err(|e| StorageError::IO {
                    source: Box::new(e),
                })?;
        }

        // Call the callback to indicate the entries have been flushed
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Delete all log entries from the specified index onwards
        self.delete_log_entries_from(log_id.index)
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })?;

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // For now, purge is the same as truncate
        // In a production system, you might want to keep some metadata
        self.truncate(log_id).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self
    }
}

/// Log reader implementation for RaftStorage
pub struct RaftStorageLogReader<'a> {
    storage: &'a RaftStorage,
}

impl<'a> RaftStorageLogReader<'a> {
    pub fn new(storage: &'a RaftStorage) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl<'a> RaftLogReader<TypeConfig> for RaftStorageLogReader<'a> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let mut entries = Vec::new();

        // Convert range bounds to concrete start and end indices
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 1, // Log indices start from 1
        };

        // For the end bound, we need to determine a reasonable upper limit
        // In a production system, you'd want to optimize this
        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => n + 1,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => {
                // Get the last log entry to determine the upper bound
                match self.storage.get_last_log_entry().map_err(|e| StorageError::IO {
                    source: Box::new(e),
                })? {
                    Some(entry) => entry.index + 1,
                    None => start, // No entries exist
                }
            }
        };

        // Fetch entries in the range
        for index in start..end {
            if let Some(stored_entry) = self.storage.get_log_entry(index).map_err(|e| StorageError::IO {
                source: Box::new(e),
            })? {
                // Deserialize the payload
                let payload: openraft::EntryPayload<TypeConfig> = 
                    bincode::deserialize(&stored_entry.payload).map_err(|e| StorageError::IO {
                        source: Box::new(RaftError::Storage(crate::error::StorageError::DataInconsistency {
                            message: format!("Failed to deserialize entry payload: {}", e),
                        })),
                    })?;

                let entry = Entry {
                    log_id: LogId::new(
                        openraft::CommittedLeaderId::new(stored_entry.term, NodeId::default()),
                        stored_entry.index,
                    ),
                    payload,
                };

                entries.push(entry);
            }
        }

        Ok(entries)
    }
}

// Implement RaftLogReader for RaftStorage directly
#[async_trait::async_trait]
impl RaftLogReader<TypeConfig> for RaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let mut reader = RaftStorageLogReader::new(self);
        reader.try_get_log_entries(range).await
    }
}