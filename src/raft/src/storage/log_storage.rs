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

//! RaftLogStorage implementation for storage-v2 API

// All imports removed to fix warnings - this file contains only commented out code

// NOTE: This direct implementation is commented out because OpenRaft 0.9.21 uses sealed traits
// Use the adaptor_integration module instead for proper OpenRaft integration

/*
/// Implement RaftLogStorage for Arc<RaftStorage>
impl RaftLogStorage<TypeConfig> for Arc<RaftStorage> {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<TypeConfig>, OpenraftStorageError<NodeId>> {
        log::debug!("Getting log state");

        // Get the last log entry to determine the log state
        let last_log = self.get_last_log_entry().map_err(to_storage_error)?;

        let last_log_id = last_log.map(|entry| {
            LogId::new(
                openraft::CommittedLeaderId::new(entry.term, 0),
                entry.index,
            )
        });

        // Get the last purged log ID (we don't implement purging yet, so it's None)
        let last_purged_log_id = None;

        log::debug!("Log state: last_log_id={:?}", last_log_id);

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<NodeId>,
    ) -> Result<(), OpenraftStorageError<NodeId>> {
        log::debug!("Saving vote: {:?}", vote);

        // Save the current term and voted_for
        self.set_current_term(vote.leader_id().get_term())
            .map_err(to_storage_error)?;

        self.set_voted_for(Some(vote.leader_id().voted_for().unwrap_or(0)))
            .map_err(to_storage_error)?;

        log::debug!("Vote saved successfully");
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<NodeId>>, OpenraftStorageError<NodeId>> {
        log::debug!("Reading vote");

        let current_term = self.get_current_term();
        let voted_for = self.get_voted_for();

        if current_term == 0 {
            log::debug!("No vote found (term is 0)");
            return Ok(None);
        }

        // Create a Vote from the stored data
        let vote = Vote::new(current_term, voted_for.unwrap_or(0));

        log::debug!("Read vote: {:?}", vote);
        Ok(Some(vote))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), OpenraftStorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        log::debug!("Appending log entries");

        let mut count = 0;
        for entry in entries {
            // Convert openraft Entry to our StoredLogEntry
            let payload = match &entry.payload {
                openraft::EntryPayload::Normal(client_request) => {
                    bincode::serialize(client_request).map_err(|e| {
                        to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency { message: format!("Failed to serialize entry payload: {}", e), context: String::new(),
                        }))
                    })?
                }
                openraft::EntryPayload::Blank => {
                    // For blank entries, just store an empty payload
                    Vec::new()
                }
                openraft::EntryPayload::Membership(_) => {
                    // For membership entries, serialize the whole entry
                    bincode::serialize(&entry).map_err(|e| {
                        to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency { message: format!("Failed to serialize membership entry: {}", e), context: String::new(),
                        }))
                    })?
                }
            };

            let stored_entry = super::core::StoredLogEntry {
                index: entry.log_id.index,
                term: entry.log_id.leader_id.term,
                payload,
            };

            self.append_log_entry(&stored_entry)
                .map_err(to_storage_error)?;

            count += 1;
        }

        log::debug!("Appended {} log entries", count);

        // Call the callback to signal that entries are flushed
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), OpenraftStorageError<NodeId>> {
        log::debug!("Truncating log from index {}", log_id.index);

        // Delete all log entries from the specified index onwards
        self.delete_log_entries_from(log_id.index)
            .map_err(to_storage_error)?;

        log::debug!("Log truncated successfully");
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), OpenraftStorageError<NodeId>> {
        log::debug!("Purging log up to index {}", log_id.index);

        // For now, we don't implement purging (which is for cleaning up old logs)
        // This would typically delete logs that are older than the last snapshot
        // TODO: Implement proper log purging

        log::debug!("Log purge not yet implemented, skipping");
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        log::debug!("Getting log reader");
        self.clone()
    }
}


*/