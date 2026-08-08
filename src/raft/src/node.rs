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
use openraft::storage::RaftLogStorage;
use openraft::{Config, LogId, Raft, SnapshotPolicy, StoredMembership};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;

use arc_swap::ArcSwap;

use crate::durable_state_machine_meta::{DurableStateMachineMeta, DurableStateMachineStore};
use crate::grpc::{
    create_admin_service, create_client_service, create_core_service, create_metrics_service,
};
use crate::log_store_rocksdb::RocksdbLogStore;
use crate::network::KiwiNetworkFactory;
use crate::raft_proto::raft_admin_service_server::RaftAdminServiceServer;
use crate::raft_proto::raft_client_service_server::RaftClientServiceServer;
use crate::raft_proto::raft_core_service_server::RaftCoreServiceServer;
use crate::raft_proto::raft_metrics_service_server::RaftMetricsServiceServer;
use crate::state_machine::{KiwiStateMachine, PauseController, load_current_snapshot};
use storage::storage::Storage;

#[derive(Debug, thiserror::Error)]
pub enum ClientWriteError {
    #[error("invalid binlog: {0}")]
    InvalidBinlog(#[source] storage::error::Error),
    #[error("Raft client write failed: {0}")]
    Raft(#[source] anyhow::Error),
}

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

    pub async fn client_write(&self, binlog: Binlog) -> Result<BinlogResponse, ClientWriteError> {
        self.storage_swap
            .load_full()
            .validate_binlog(&binlog)
            .map_err(ClientWriteError::InvalidBinlog)?;
        let res = self
            .raft
            .client_write(binlog)
            .await
            .map_err(|error| ClientWriteError::Raft(anyhow::Error::new(error)))?;
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

    fn ensure_linearizable_read(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            // openraft's `ensure_linearizable` confirms leadership with a
            // quorum and then blocks until the state machine has applied up
            // to the read log id (`wait().applied_index_at_least` on the
            // raft-core metrics, which advance only after
            // `KiwiStateMachine::apply` has written the entries to RocksDB
            // and returned), so no extra applied-watch is needed here.
            self.raft
                .ensure_linearizable()
                .await
                .map(|_| ())
                .map_err(|e| format!("linearizable read failed: {e}"))
        })
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

/// Recover and validate the durable applied frontier at startup.
/// note(guozhihao-224) Corrupt or unknown-version metadata, a frontier ahead of
/// the last log, or a snapshot ahead of the frontier all refuse startup. Without a
/// durable frontier, bootstrap from a persisted snapshot; with neither, first start.
async fn recover_applied_state(
    log_store: &mut RocksdbLogStore,
    durable_meta: &Arc<DurableStateMachineStore>,
    snapshot_work_dir: &Path,
) -> Result<(Option<LogId<u64>>, StoredMembership<u64, KiwiNode>), anyhow::Error> {
    let snapshot = load_current_snapshot(snapshot_work_dir)
        .map_err(|error| anyhow::anyhow!("failed to read current snapshot metadata: {error}"))?;

    let frontier = durable_meta
        .validate()
        .map_err(|error| anyhow::anyhow!("durable state machine metadata is unusable: {error}"))?;

    let (last_applied, last_membership) = match frontier {
        Some(meta) => (meta.last_applied, meta.last_membership),
        None => match &snapshot {
            Some(snap) => {
                let meta = DurableStateMachineMeta::new(
                    snap.meta.last_log_id,
                    snap.meta.last_membership.clone(),
                );
                durable_meta.save_meta(&meta).map_err(|error| {
                    anyhow::anyhow!("failed to bootstrap applied frontier from snapshot: {error}")
                })?;
                log::info!(
                    "Bootstrapped applied frontier from persisted snapshot: last_applied={:?}",
                    meta.last_applied.map(|log_id| log_id.index)
                );
                (meta.last_applied, meta.last_membership)
            }
            None => (None, StoredMembership::default()),
        },
    };

    let log_state = log_store
        .get_log_state()
        .await
        .map_err(|error| anyhow::anyhow!("failed to read Raft log state: {error}"))?;
    let last_log_index = log_state.last_log_id.map(|log_id| log_id.index);

    if let Some(applied) = last_applied {
        if last_log_index.is_none_or(|last| applied.index > last) {
            return Err(anyhow::anyhow!(
                "durable applied frontier {} is ahead of the last Raft log {:?}, refusing to start",
                applied.index,
                last_log_index
            ));
        }
        if let Some(snap) = &snapshot
            && let Some(snap_last) = snap.meta.last_log_id
            && snap_last.index > applied.index
        {
            return Err(anyhow::anyhow!(
                "persisted snapshot index {} is ahead of the durable applied frontier {}, refusing to start",
                snap_last.index,
                applied.index
            ));
        }
    }

    Ok((last_applied, last_membership))
}

pub async fn create_raft_node(
    config: RaftConfig,
    storage_swap: Arc<ArcSwap<Storage>>,
    pause_controller: Arc<dyn PauseController>,
    append_log_fn: Option<Arc<OnceLock<storage::AppendLogFn>>>,
) -> Result<Arc<RaftApp>, anyhow::Error> {
    crate::state_machine::preflight_snapshot_install(&config.db_path)?;
    let raft_config = build_raft_config(&config)?;
    let snapshot_work_dir = config.data_dir.join("snapshots");
    fs::create_dir_all(&snapshot_work_dir)?;

    let legacy_log_store_path = config.data_dir.join("raft_logs");
    if legacy_log_store_path.try_exists()? {
        return Err(anyhow::anyhow!(
            "cannot safely migrate legacy in-memory Raft log state in place; use a new node ID and clean data-dir/raft-data-dir to rejoin from a healthy leader"
        ));
    }

    let log_store_path = config.data_dir.join("raft_logs_rocksdb");
    std::fs::create_dir_all(&log_store_path)?;
    let mut log_store = RocksdbLogStore::open(&log_store_path)?;

    // note(guozhihao-224) Validate the durable frontier before serving; a
    // violation refuses startup.
    let durable_meta = Arc::new(DurableStateMachineStore::new(log_store.db()));
    let (last_applied, last_membership) =
        recover_applied_state(&mut log_store, &durable_meta, &snapshot_work_dir).await?;
    log::info!(
        "Recovered applied frontier: last_applied={:?}",
        last_applied.map(|log_id| log_id.index)
    );

    // Per-instance LogIndex collectors / cf_trackers live in the Storage; the state
    // machine looks them up through storage_swap so it sees the right ones after a
    // snapshot install hot-swaps Storage.
    let state_machine = KiwiStateMachine::new(
        config.node_id,
        storage_swap.clone(),
        config.db_path.clone(),
        snapshot_work_dir,
        Arc::clone(&pause_controller),
        append_log_fn,
    )
    .with_durable_meta(Arc::clone(&durable_meta))
    .with_recovered_state(last_applied, last_membership);

    let network = KiwiNetworkFactory::new();

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
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::*;
    use crate::state_machine::{StorageAccessPermit, snapshot_install_marker_path};
    use conf::raft_type::{BinlogEntry, OperateType};
    use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
    use openraft::{Entry, EntryPayload, LeaderId, LogId, SnapshotMeta, Vote};
    use storage::BaseMetaKey;
    use storage::ColumnFamilyIndex;
    use storage::format_strings_value::StringValue;
    use storage::slot_indexer::key_to_slot_id;
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

    fn string_put_binlog(key: &[u8], value: &[u8]) -> Binlog {
        Binlog {
            db_id: 0,
            slot_idx: key_to_slot_id(key) as u32,
            entries: vec![BinlogEntry {
                cf_idx: ColumnFamilyIndex::MetaCF as u32,
                op_type: OperateType::Put,
                key: BaseMetaKey::new(key)
                    .encode()
                    .expect("test string key should encode")
                    .to_vec(),
                value: Some(StringValue::new(value.to_vec()).encode().to_vec()),
            }],
        }
    }

    #[tokio::test]
    async fn invalid_binlog_is_rejected_before_raft_and_next_valid_write_succeeds() {
        let temp_dir = TempDir::new().expect("test should create a temporary directory");
        let db_path = temp_dir.path().join("data");
        let mut storage = Storage::new(1, 0);
        let storage_rx = storage
            .open(Arc::new(storage::StorageOptions::default()), &db_path)
            .expect("test storage should open");
        let storage_swap = Arc::new(ArcSwap::from_pointee(storage));
        let config = RaftConfig {
            node_id: 1,
            data_dir: temp_dir.path().join("raft-data"),
            db_path: db_path.clone(),
            ..Default::default()
        };
        let app = create_raft_node(
            config,
            Arc::clone(&storage_swap),
            noop_pause_controller(),
            None,
        )
        .await
        .expect("single-node Raft should start");

        let mut nodes = BTreeMap::new();
        nodes.insert(
            1,
            KiwiNode {
                raft_addr: "127.0.0.1:0".to_string(),
                resp_addr: "127.0.0.1:0".to_string(),
            },
        );
        app.raft
            .initialize(nodes)
            .await
            .expect("single-node Raft should initialize");
        app.raft
            .wait(Some(Duration::from_secs(10)))
            .current_leader(1, "single-node test should elect itself")
            .await
            .expect("single-node Raft should become leader");

        let mut invalid = string_put_binlog(b"poison", b"bad");
        invalid.entries[0].cf_idx = 99;
        app.client_write(invalid)
            .await
            .expect_err("invalid binlog must be rejected before proposal");

        let valid = string_put_binlog(b"healthy", b"value");
        let response = app
            .client_write(valid)
            .await
            .expect("a valid write after rejection should still reach consensus");
        assert!(response.success);
        assert_eq!(
            storage_swap
                .load_full()
                .get(b"healthy")
                .expect("valid write should be applied"),
            "value"
        );

        app.raft
            .shutdown()
            .await
            .expect("test Raft should shut down cleanly");
        drop(app);
        let storage = storage_swap.load_full();
        drop(storage_swap);
        let mut storage = Arc::try_unwrap(storage)
            .unwrap_or_else(|_| panic!("test storage should not retain Arc references"));
        storage.shutdown().await;
        storage.close();
        drop(storage_rx);
    }

    #[tokio::test]
    async fn malformed_snapshot_install_marker_is_rejected_before_log_store_creation() {
        let temp_dir = TempDir::new().expect("test should create a temporary directory");
        let raft_data_dir = temp_dir.path().join("raft-data");
        let db_path = temp_dir.path().join("data");
        fs::create_dir_all(db_path.parent().expect("test DB path should have a parent"))
            .expect("test should create the DB parent");
        let marker_path =
            snapshot_install_marker_path(&db_path).expect("test DB path should support a marker");
        fs::write(&marker_path, b"{not-json")
            .expect("test should write a malformed install marker");
        let log_store_path = raft_data_dir.join("raft_logs_rocksdb");
        let config = RaftConfig {
            data_dir: raft_data_dir,
            db_path,
            ..Default::default()
        };
        let storage_swap = Arc::new(ArcSwap::from_pointee(Storage::new(1, 0)));

        let error =
            match create_raft_node(config, storage_swap, noop_pause_controller(), None).await {
                Ok(app) => {
                    app.raft
                        .shutdown()
                        .await
                        .expect("unexpected test Raft node should shut down cleanly");
                    panic!("malformed install marker must reject Raft startup");
                }
                Err(error) => error,
            };

        assert!(
            error
                .to_string()
                .contains(&marker_path.display().to_string()),
            "startup refusal must identify the marker path: {error}"
        );
        assert!(
            !log_store_path
                .try_exists()
                .expect("test should inspect the log-store path"),
            "marker preflight must run before creating the RocksDB log store"
        );
    }

    #[tokio::test]
    async fn unknown_snapshot_install_marker_version_is_rejected_before_log_store_creation() {
        let temp_dir = TempDir::new().expect("test should create a temporary directory");
        let raft_data_dir = temp_dir.path().join("raft-data");
        let db_path = temp_dir.path().join("data");
        let marker_path =
            snapshot_install_marker_path(&db_path).expect("test DB path should support a marker");
        let marker = serde_json::json!({
            "version": 2,
            "id": "future-snapshot",
            "index": 8,
            "term": 3,
            "db": db_path,
            "workdir": temp_dir.path().join("snapshots"),
            "instances": 1
        });
        fs::write(
            &marker_path,
            serde_json::to_vec(&marker).expect("test marker should serialize"),
        )
        .expect("test should write an unknown-version install marker");
        let log_store_path = raft_data_dir.join("raft_logs_rocksdb");
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
                        .expect("unexpected test Raft node should shut down cleanly");
                    panic!("unknown install marker version must reject Raft startup");
                }
                Err(error) => error,
            };
        assert!(
            error.to_string().contains("unsupported marker version 2"),
            "unexpected startup refusal: {error}"
        );
        assert!(
            !log_store_path
                .try_exists()
                .expect("test should inspect the log-store path"),
            "marker version preflight must run before creating the RocksDB log store"
        );
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

    fn write_snapshot_meta(snap_root: &std::path::Path, last_index: u64) {
        std::fs::create_dir_all(snap_root).expect("snapshot root should be created");
        let meta: SnapshotMeta<u64, KiwiNode> = SnapshotMeta {
            last_log_id: Some(LogId::new(LeaderId::new(1, 1), last_index)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snapshot-test".to_string(),
        };
        let json = serde_json::to_string_pretty(&meta).expect("snapshot meta should serialize");
        std::fs::write(snap_root.join("current_snapshot_meta.json"), json)
            .expect("snapshot meta file should be written");
        std::fs::write(snap_root.join("current_snapshot.tar"), b"")
            .expect("snapshot data file should be written");
    }

    #[tokio::test]
    async fn fresh_db_no_snapshot_recovers_none() {
        let temp_dir = TempDir::new().expect("temporary directory should be created");
        let mut log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
            .expect("log store should open");
        let store = Arc::new(DurableStateMachineStore::new(log_store.db()));

        let (applied, _membership) =
            recover_applied_state(&mut log_store, &store, &temp_dir.path().join("snapshots"))
                .await
                .expect("first start with no durable state should recover cleanly");
        assert_eq!(applied, None, "first start must recover an empty frontier");
        assert!(
            store.load_meta().expect("load should work").is_none(),
            "first start must not fabricate metadata"
        );
    }

    #[tokio::test]
    async fn empty_sm_meta_with_snapshot_bootstraps_from_snapshot() {
        let temp_dir = TempDir::new().expect("temporary directory should be created");
        let mut log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
            .expect("log store should open");
        log_store
            .blocking_append([Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 5),
                payload: EntryPayload::Blank,
            }])
            .await
            .expect("a log entry should be appended");
        let store = Arc::new(DurableStateMachineStore::new(log_store.db()));
        write_snapshot_meta(&temp_dir.path().join("snapshots"), 5);

        let (applied, _membership) =
            recover_applied_state(&mut log_store, &store, &temp_dir.path().join("snapshots"))
                .await
                .expect("bootstrap from a persisted snapshot should recover");
        assert_eq!(
            applied.map(|l| l.index),
            Some(5),
            "bootstrap must recover the snapshot last log id"
        );
        let persisted = store
            .load_meta()
            .expect("load should work")
            .expect("must be persisted");
        assert_eq!(
            persisted.last_applied.map(|l| l.index),
            Some(5),
            "bootstrap must persist the snapshot-derived frontier"
        );
    }

    #[tokio::test]
    async fn corrupt_sm_meta_fails_closed() {
        let temp_dir = TempDir::new().expect("temporary directory should be created");
        let mut log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
            .expect("log store should open");
        let db = log_store.db();
        {
            let cf = db.cf_handle("sm_meta").expect("sm_meta CF should exist");
            db.put_cf(&cf, b"state_machine_meta", b"not-valid-json")
                .expect("corrupt bytes should be written");
        }
        let store = Arc::new(DurableStateMachineStore::new(db));

        let error =
            recover_applied_state(&mut log_store, &store, &temp_dir.path().join("snapshots"))
                .await
                .expect_err("corrupt metadata must refuse startup");
        let _ = error;
    }

    #[tokio::test]
    async fn unknown_format_version_fails_closed() {
        let temp_dir = TempDir::new().expect("temporary directory should be created");
        let mut log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
            .expect("log store should open");
        let store = Arc::new(DurableStateMachineStore::new(log_store.db()));
        store
            .save_meta(&DurableStateMachineMeta {
                format_version: 99,
                last_applied: Some(LogId::new(LeaderId::new(1, 1), 7)),
                last_membership: StoredMembership::default(),
            })
            .expect("meta should be persisted");

        let error =
            recover_applied_state(&mut log_store, &store, &temp_dir.path().join("snapshots"))
                .await
                .expect_err("unsupported format version must refuse startup");
        let _ = error;
    }

    #[tokio::test]
    async fn frontier_ahead_of_log_store_fails_closed() {
        let temp_dir = TempDir::new().expect("temporary directory should be created");
        let mut log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
            .expect("log store should open");
        let store = Arc::new(DurableStateMachineStore::new(log_store.db()));
        store
            .save_meta(&DurableStateMachineMeta::new(
                Some(LogId::new(LeaderId::new(1, 1), 100)),
                StoredMembership::default(),
            ))
            .expect("meta should be persisted");

        let error =
            recover_applied_state(&mut log_store, &store, &temp_dir.path().join("snapshots"))
                .await
                .expect_err("frontier ahead of the last log must refuse startup");
        let _ = error;
    }

    #[tokio::test]
    async fn snapshot_ahead_of_frontier_fails_closed() {
        let temp_dir = TempDir::new().expect("temporary directory should be created");
        let mut log_store = RocksdbLogStore::open(temp_dir.path().join("raft_logs_rocksdb"))
            .expect("log store should open");
        let store = Arc::new(DurableStateMachineStore::new(log_store.db()));
        store
            .save_meta(&DurableStateMachineMeta::new(
                Some(LogId::new(LeaderId::new(1, 1), 3)),
                StoredMembership::default(),
            ))
            .expect("meta should be persisted");
        write_snapshot_meta(&temp_dir.path().join("snapshots"), 5);

        let error =
            recover_applied_state(&mut log_store, &store, &temp_dir.path().join("snapshots"))
                .await
                .expect_err("snapshot ahead of the durable frontier must refuse startup");
        let _ = error;
    }
}
