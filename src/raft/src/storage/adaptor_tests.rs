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

//! Tests for RaftStorageAdaptor

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::state_machine::KiwiStateMachine;
    use crate::storage::adaptor::create_raft_storage_adaptor;

    #[tokio::test]
    async fn test_create_adaptor() {
        use crate::storage::core::RaftStorage as RaftStorageImpl;

        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RaftStorageImpl::new_async(temp_dir.path()).await.unwrap());
        let state_machine = Arc::new(KiwiStateMachine::new(1));

        let (_log_storage, _state_machine) = create_raft_storage_adaptor(storage, state_machine);

        // If we get here without panicking, the adaptor was created successfully
    }

    #[tokio::test]
    async fn test_adaptor_basic_operations() {
        use crate::storage::core::RaftStorage as RaftStorageImpl;
        use openraft::Vote;
        use openraft::storage::RaftLogStorage;

        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RaftStorageImpl::new_async(temp_dir.path()).await.unwrap());
        let state_machine = Arc::new(KiwiStateMachine::new(1));

        let (mut log_storage, _sm) = create_raft_storage_adaptor(storage, state_machine);

        // Test save and read vote
        let vote = Vote::new(1, 1);
        log_storage.save_vote(&vote).await.unwrap();

        let read_vote = log_storage.read_vote().await.unwrap();
        assert!(read_vote.is_some());
        assert_eq!(read_vote.unwrap().leader_id().term, 1);
    }
}
