// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use conf::raft_type::{Binlog, BinlogResponse, KiwiNode, KiwiTypeConfig};
use openraft::{Config, Raft, SnapshotPolicy};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

use arc_swap::ArcSwap;

use crate::grpc::{
    create_admin_service, create_client_service, create_core_service, create_metrics_service,
};
use crate::log_store_rocksdb::RocksdbLogStore;
use crate::network::KiwiNetworkFactory;
use crate::raft_proto::raft_admin_service_server::RaftAdminServiceServer;
use crate::raft_proto::raft_client_service_server::RaftClientServiceServer;
use crate::raft_proto::raft_core_service_server::RaftCoreServiceServer;
use crate::raft_proto::raft_metrics_service_server::RaftMetricsServiceServer;
use crate::state_machine::{KiwiStateMachine, PauseController};
use storage::storage::Storage;

pub struct RaftApp {
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub raft: Raft<KiwiTypeConfig>,
    pub(crate) storage_swap: Arc<ArcSwap<Storage>>,
    pub(crate) pause_controller: Arc<dyn PauseController>,
}

impl RaftApp {
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        let guard = metrics.borrow();
        matches!(guard.current_leader, Some(id) if id == self.node_id)
    }

    pub fn get_leader(&self) -> Option<(u64, KiwiNode)> {
        let metrics = self.raft.metrics();
        let guard = metrics.borrow();
        if let Some(leader_id) = guard.current_leader {
            let membership = guard.membership_config.membership();
            if let Some(node) = membership.get_node(&leader_id) {
                return Some((leader_id, node.clone()));
            }
        }
        None
    }

    pub async fn client_write(&self, binlog: Binlog) -> Result<BinlogResponse, anyhow::Error> {
        let res = self.raft.client_write(binlog).await?;
        let log_id = Some(res.log_id.index);
        Ok(BinlogResponse {
            success: res.data.success,
            message: res.data.message,
            log_id,
        })
    }

    pub fn create_grpc_services(
        app: Arc<RaftApp>,
    ) -> (
        RaftCoreServiceServer<crate::grpc::core::RaftCoreServiceImpl>,
        RaftAdminServiceServer<crate::grpc::admin::RaftAdminServiceImpl>,
        RaftClientServiceServer<crate::grpc::client::RaftClientServiceImpl>,
        RaftMetricsServiceServer<crate::grpc::client::RaftMetricsServiceImpl>,
    ) {
        (
            create_core_service(app.raft.clone()),
            create_admin_service(app.clone()),
            create_client_service(app.clone()),
            create_metrics_service(app),
        )
    }
}

impl crate::leader_gate::LeaderGate for RaftApp {
    fn is_leader(&self) -> bool {
        RaftApp::is_leader(self)
    }

    fn leader_resp_addr(&self) -> Option<String> {
        self.get_leader().map(|(_, node)| node.resp_addr)
    }
}

pub struct RaftConfig {
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub heartbeat_interval: u64,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub snapshot_logs_threshold: u64,
    pub snapshot_max_chunk_size: u64,
    pub install_snapshot_timeout: u64,
    pub max_in_snapshot_log_to_keep: u64,
    pub replication_lag_threshold: u64,
}

const SNAPSHOT_LOGS_THRESHOLD: u64 = 5000;
const SNAPSHOT_MAX_CHUNK_SIZE: u64 = 3 * 1024 * 1024;
const INSTALL_SNAPSHOT_TIMEOUT: u64 = 200;
const MAX_IN_SNAPSHOT_LOG_TO_KEEP: u64 = 1000;
const REPLICATION_LAG_THRESHOLD: u64 = 5000;

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_addr: "127.0.0.1:8081".to_string(),
            resp_addr: "127.0.0.1:6379".to_string(),
            data_dir: PathBuf::from("/tmp/kiwi/raft"),
            db_path: PathBuf::from("/tmp/kiwi/db"),
            heartbeat_interval: 200,
            election_timeout_min: 500,
            election_timeout_max: 1500,
            snapshot_logs_threshold: SNAPSHOT_LOGS_THRESHOLD,
            snapshot_max_chunk_size: SNAPSHOT_MAX_CHUNK_SIZE,
            install_snapshot_timeout: INSTALL_SNAPSHOT_TIMEOUT,
            max_in_snapshot_log_to_keep: MAX_IN_SNAPSHOT_LOG_TO_KEEP,
            replication_lag_threshold: REPLICATION_LAG_THRESHOLD,
        }
    }
}

fn build_raft_config(config: &RaftConfig) -> Result<Arc<Config>, anyhow::Error> {
    // Validate snapshot configuration parameters
    if config.snapshot_logs_threshold == 0 {
        return Err(anyhow::anyhow!("snapshot_logs_threshold must be > 0"));
    }
    if config.snapshot_max_chunk_size == 0 {
        return Err(anyhow::anyhow!("snapshot_max_chunk_size must be > 0"));
    }
    if config.install_snapshot_timeout == 0 {
        return Err(anyhow::anyhow!("install_snapshot_timeout must be > 0"));
    }
    if config.replication_lag_threshold == 0 {
        return Err(anyhow::anyhow!("replication_lag_threshold must be > 0"));
    }
    // max_in_snapshot_log_to_keep: 0 is intentionally allowed (keep no in-snapshot logs)

    let raft_config = Config {
        heartbeat_interval: config.heartbeat_interval,
        election_timeout_min: config.election_timeout_min,
        election_timeout_max: config.election_timeout_max,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_threshold),
        replication_lag_threshold: config.replication_lag_threshold,
        snapshot_max_chunk_size: config.snapshot_max_chunk_size,
        install_snapshot_timeout: config.install_snapshot_timeout,
        max_in_snapshot_log_to_keep: config.max_in_snapshot_log_to_keep,
        ..Default::default()
    };
    Ok(Arc::new(raft_config.validate()?))
}

pub async fn create_raft_node(
    config: RaftConfig,
    storage_swap: Arc<ArcSwap<Storage>>,
    pause_controller: Arc<dyn PauseController>,
    append_log_fn: Option<Arc<OnceLock<storage::AppendLogFn>>>,
) -> Result<Arc<RaftApp>, anyhow::Error> {
    let raft_config = build_raft_config(&config)?;
    let snapshot_work_dir = config.data_dir.join("snapshots");
    fs::create_dir_all(&snapshot_work_dir)?;

    // Per-instance LogIndex collectors / cf_trackers live in the Storage; the state
    // machine looks them up through storage_swap so it sees the right ones after a
    // snapshot install hot-swaps Storage.
    let mut state_machine = KiwiStateMachine::new(
        config.node_id,
        storage_swap.clone(),
        config.db_path.clone(),
        snapshot_work_dir,
        append_log_fn,
    );

    state_machine.set_pause_controller(Arc::clone(&pause_controller));

    let network = KiwiNetworkFactory::new();

    let legacy_log_store_path = config.data_dir.join("raft_logs");
    if legacy_log_store_path.try_exists()? {
        return Err(anyhow::anyhow!(
            "cannot safely migrate legacy in-memory Raft log state in place; use a new node ID and clean data-dir/raft-data-dir to rejoin from a healthy leader"
        ));
    }

    let log_store_path = config.data_dir.join("raft_logs_rocksdb");
    std::fs::create_dir_all(&log_store_path)?;
    let log_store = RocksdbLogStore::open(&log_store_path)?;

    let raft = Raft::new(
        config.node_id,
        raft_config,
        network,
        log_store,
        state_machine,
    )
    .await?;

    Ok(Arc::new(RaftApp {
        node_id: config.node_id,
        raft_addr: config.raft_addr,
        resp_addr: config.resp_addr,
        raft,
        storage_swap,
        pause_controller,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::StorageAccessPermit;
    use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
    use openraft::{Entry, EntryPayload, LeaderId, LogId, Vote};
    use tempfile::TempDir;

    struct NoopPauseController;
    struct NoopStorageAccessPermit;

    impl StorageAccessPermit for NoopStorageAccessPermit {}

    impl PauseController for NoopPauseController {
        fn request_pause(
            &self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }

        fn enter(
            self: Arc<Self>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>,
        > {
            Box::pin(async { Box::new(NoopStorageAccessPermit) as Box<dyn StorageAccessPermit> })
        }

        fn resume(&self) {}
    }

    fn noop_pause_controller() -> Arc<dyn PauseController> {
        Arc::new(NoopPauseController)
    }

    #[derive(Clone, Copy)]
    enum DurableStateFixture {
        VoteOnly,
        CommittedOnly,
        LogOnly,
        PurgedOnly,
        Complete,
    }

    impl DurableStateFixture {
        fn name(self) -> &'static str {
            match self {
                Self::VoteOnly => "vote-only",
                Self::CommittedOnly => "committed-only",
                Self::LogOnly => "log-only",
                Self::PurgedOnly => "purged-only",
                Self::Complete => "complete",
            }
        }
    }

    fn test_log_entries() -> Vec<Entry<KiwiTypeConfig>> {
        (1..=2)
            .map(|index| Entry {
                log_id: LogId::new(LeaderId::new(1, 1), index),
                payload: EntryPayload::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            })
            .collect()
    }

    #[tokio::test]
    async fn legacy_memory_log_without_durable_raft_state_is_rejected() {
        let temp_dir = TempDir::new().expect("test should create a temporary directory");
        let raft_data_dir = temp_dir.path().join("raft-data");
        fs::create_dir_all(raft_data_dir.join("raft_logs"))
            .expect("test should create the legacy memory log directory");
        let log_store_path = raft_data_dir.join("raft_logs_rocksdb");

        let config = RaftConfig {
            data_dir: raft_data_dir,
            db_path: temp_dir.path().join("data"),
            ..Default::default()
        };
        let storage_swap = Arc::new(ArcSwap::from_pointee(Storage::new(1, 0)));

        let error = match create_raft_node(config, storage_swap, noop_pause_controller(), None)
            .await
        {
            Ok(app) => {
                app.raft
                    .shutdown()
                    .await
                    .expect("test Raft node should shut down cleanly");
                panic!("legacy memory log directory without durable Raft state must be rejected");
            }
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("cannot safely migrate"),
            "unexpected error: {error}"
        );
        assert!(
            !log_store_path
                .try_exists()
                .expect("test should inspect the RocksDB log store path"),
            "legacy marker rejection must happen before creating the RocksDB log store"
        );
    }

    #[tokio::test]
    async fn legacy_memory_log_marker_is_rejected_even_with_rocksdb_state() {
        for fixture in [
            DurableStateFixture::VoteOnly,
            DurableStateFixture::CommittedOnly,
            DurableStateFixture::LogOnly,
            DurableStateFixture::PurgedOnly,
            DurableStateFixture::Complete,
        ] {
            let temp_dir = TempDir::new().expect("test should create a temporary directory");
            let raft_data_dir = temp_dir.path().join("raft-data");
            fs::create_dir_all(raft_data_dir.join("raft_logs"))
                .expect("test should create the legacy memory log directory");

            {
                let mut log_store = RocksdbLogStore::open(raft_data_dir.join("raft_logs_rocksdb"))
                    .expect("test should open the durable log store");
                let log_id = LogId::new(LeaderId::new(1, 1), 1);

                match fixture {
                    DurableStateFixture::VoteOnly => log_store
                        .save_vote(&Vote::new(1, 1))
                        .await
                        .expect("test should persist a vote"),
                    DurableStateFixture::CommittedOnly => log_store
                        .save_committed(Some(log_id))
                        .await
                        .expect("test should persist a committed log ID"),
                    DurableStateFixture::LogOnly => log_store
                        .blocking_append(test_log_entries())
                        .await
                        .expect("test should persist log entries"),
                    DurableStateFixture::PurgedOnly => log_store
                        .purge(log_id)
                        .await
                        .expect("test should persist a purged log ID"),
                    DurableStateFixture::Complete => {
                        log_store
                            .blocking_append(test_log_entries())
                            .await
                            .expect("test should persist log entries");
                        log_store
                            .save_vote(&Vote::new(1, 1))
                            .await
                            .expect("test should persist a vote");
                        log_store
                            .save_committed(Some(log_id))
                            .await
                            .expect("test should persist a committed log ID");
                        log_store
                            .purge(log_id)
                            .await
                            .expect("test should persist a purged log ID");
                    }
                }
            }

            let config = RaftConfig {
                data_dir: raft_data_dir,
                db_path: temp_dir.path().join("data"),
                ..Default::default()
            };
            let storage_swap = Arc::new(ArcSwap::from_pointee(Storage::new(1, 0)));

            let error =
                match create_raft_node(config, storage_swap, noop_pause_controller(), None).await {
                    Ok(app) => {
                        app.raft
                            .shutdown()
                            .await
                            .expect("test Raft node should shut down cleanly");
                        panic!(
                            "legacy memory log marker with {} RocksDB state must be rejected",
                            fixture.name()
                        );
                    }
                    Err(error) => error,
                };

            assert!(
                error.to_string().contains("cannot safely migrate"),
                "unexpected error for {} RocksDB state: {error}",
                fixture.name()
            );
        }
    }
}
