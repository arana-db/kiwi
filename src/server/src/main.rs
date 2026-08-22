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

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

use clap::Parser;
use conf::config::Config;
use log::{debug, error, info, warn};
use runtime::{
    DualRuntimeError, GlobalStorage, RuntimeManager,
    StorageAccessPermit as RuntimeStorageAccessPermit, StorageServer, StorageServerPauseController,
};
use storage::StorageOptions;
use storage::storage::Storage;

use raft::node::{RaftApp, RaftConfig, create_raft_node};
use raft::raft_proto;
use raft::state_machine::{PauseController, StorageAccessPermit};

struct PauseControllerWrapper(StorageServerPauseController);
struct PausePermitWrapper {
    _permit: RuntimeStorageAccessPermit,
}

mod server_info;

/// Build the command-table feature gates from the loaded configuration:
/// vector commands follow `vector-enabled` and remain rejected in cluster
/// mode until their Raft apply/replay correctness contract is complete.
/// FLUSHDB/FLUSHALL are only allowed outside cluster mode unless
/// `cluster-flush-enabled` is set.
fn command_table_gates(config: &Config) -> cmd::table::CommandTableGates {
    let vector_enabled = config.vector.enabled;
    let vector_cluster_allowed = config.raft.is_none();
    let cluster_flush_allowed = config.raft.is_none() || config.cluster_flush_enabled;
    cmd::table::CommandTableGates::from_flags(
        vector_enabled,
        vector_cluster_allowed,
        cluster_flush_allowed,
    )
}

fn vector_admission_limits(config: &Config) -> cmd::vector::admission::VectorAdmissionLimits {
    cmd::vector::admission::VectorAdmissionLimits::from(&config.vector)
}

impl StorageAccessPermit for PausePermitWrapper {}

impl PauseController for PauseControllerWrapper {
    fn request_pause(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(self.0.request_pause())
    }

    fn enter(
        self: Arc<Self>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>,
    > {
        Box::pin(async move {
            Box::new(PausePermitWrapper {
                _permit: self.0.enter().await,
            }) as Box<dyn StorageAccessPermit>
        })
    }

    fn resume(&self) {
        self.0.resume();
    }
}

#[derive(Parser)]
#[command(name = "kiwi")]
#[command(about = "A Redis-compatible key-value database built in Rust")]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Args {
    #[arg(short, long)]
    config: Option<String>,

    #[arg(long)]
    single_node: bool,

    #[arg(long)]
    init_cluster: bool,

    #[arg(long)]
    sample_config: bool,

    #[arg(long)]
    full_sample_config: bool,
}

fn main() -> std::io::Result<()> {
    env_logger::init();

    let args = Args::parse();

    if args.full_sample_config {
        print!("{}", Config::full_sample_config());
        return Ok(());
    }

    if args.sample_config {
        print!("{}", Config::sample_config());
        return Ok(());
    }

    let config_file = args
        .config
        .clone()
        .map(|path| {
            std::fs::canonicalize(&path)
                .map(|abs| abs.to_string_lossy().into_owned())
                .unwrap_or(path)
        })
        .unwrap_or_default();
    let config = if let Some(config_path) = args.config {
        Config::load(&config_path).map_err(|_e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to load config file '{}': {}", config_path, _e),
            )
        })?
    } else {
        Config::default()
    };

    preflight_server_startup(&config)?;

    let server_info_provider = Arc::new(server_info::KiwiServerInfoProvider::new(
        &config,
        config_file,
    ));

    let addr = format!("{}:{}", config.binding, config.port);
    let protocol = "tcp";

    let runtime_config = config.runtime.clone();
    info!(
        "Creating RuntimeManager with {} network threads and {} storage threads",
        runtime_config.network_threads, runtime_config.storage_threads
    );

    let mut runtime_manager = RuntimeManager::new(runtime_config)
        .map_err(|e| std::io::Error::other(format!("Failed to create RuntimeManager: {}", e)))?;

    let basic_rt = tokio::runtime::Runtime::new()
        .map_err(|e| std::io::Error::other(format!("Failed to create basic runtime: {}", e)))?;

    basic_rt.block_on(async {
        if let Err(e) = runtime_manager.start().await {
            error!("Failed to start RuntimeManager: {}", e);
            return Err(std::io::Error::other(format!(
                "Failed to start RuntimeManager: {}",
                e
            )));
        }
        Ok(())
    })?;

    info!("RuntimeManager started successfully");

    let network_handle = runtime_manager
        .network_handle()
        .map_err(|e| std::io::Error::other(format!("Failed to get network handle: {}", e)))?;
    let storage_handle = runtime_manager
        .storage_handle()
        .map_err(|e| std::io::Error::other(format!("Failed to get storage handle: {}", e)))?;

    let storage_receiver = runtime_manager
        .initialize_storage_components()
        .map_err(|e| {
            std::io::Error::other(format!("Failed to initialize storage components: {}", e))
        })?;

    let result = network_handle.block_on(async {
        let storage = initialize_storage(&config)
            .await
            .map_err(|e| std::io::Error::other(format!("Failed to initialize storage: {}", e)))?;

        info!("Storage components initialized, starting storage server...");

        let pause_controller = StorageServerPauseController::new();
        let pause_controller_for_raft = pause_controller.clone();

        let storage_for_server = storage.clone();
        let requirepass_for_storage_server = config.requirepass.clone();
        let gates_for_storage_server = command_table_gates(&config);
        let info_provider_for_storage =
            Arc::clone(&server_info_provider) as cmd::server_info::ServerInfoProviderRef;
        storage_handle.spawn(async move {
            info!("Initializing storage server...");
            match initialize_storage_server(
                storage_receiver,
                storage_for_server,
                pause_controller,
                requirepass_for_storage_server,
                info_provider_for_storage,
                gates_for_storage_server,
            )
            .await
            {
                Ok(_) => {
                    error!("Storage server exited unexpectedly - this should never happen!");
                }
                Err(e) => {
                    error!("Storage server failed: {}", e);
                }
            }
        });

        std::thread::sleep(std::time::Duration::from_millis(100));

        info!("Storage server started in background");
        info!("Starting Kiwi server in single-node mode on {}", addr);

        match start_server(
            protocol,
            &addr,
            &mut runtime_manager,
            &storage,
            &config,
            pause_controller_for_raft,
            Arc::clone(&server_info_provider),
        )
        .await
        {
            Ok(_) => info!("Server started successfully"),
            Err(e) => {
                error!("Failed to start server: {}", e);
                return Err(e);
            }
        }

        info!("Press Ctrl+C to stop.");
        tokio::signal::ctrl_c().await.map_err(|e| {
            std::io::Error::other(format!("Failed to listen for shutdown signal: {}", e))
        })?;
        info!("Received shutdown signal, stopping...");

        Ok(())
    });

    if let Err(e) = network_handle.block_on(runtime_manager.stop()) {
        warn!("Error during RuntimeManager shutdown: {}", e);
    }

    result
}

fn preflight_server_startup(config: &Config) -> std::io::Result<()> {
    let db_path = std::path::Path::new(&config.data_dir);
    let marker_path = raft::snapshot_install::snapshot_install_marker_path(db_path)?;
    let pending_instances = raft::snapshot_install::validate_snapshot_install_marker(db_path)
        .map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!(
                    "invalid snapshot install marker {}: {error}",
                    marker_path.display()
                ),
            )
        })?;
    if let Some(marker_instances) = pending_instances {
        if marker_instances != config.db_instance_num {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "snapshot install marker {} describes {marker_instances} instances, but startup config requires {}",
                    marker_path.display(),
                    config.db_instance_num
                ),
            ));
        }
        if config.raft.is_some() {
            return Ok(());
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "snapshot install marker {} requires Raft startup recovery, but Raft is disabled",
                marker_path.display()
            ),
        ));
    }
    Ok(())
}

async fn initialize_storage(config: &Config) -> Result<GlobalStorage, DualRuntimeError> {
    info!("Initializing storage...");

    let storage_options = Arc::new(StorageOptions::from_config(config));
    let data_dir = PathBuf::from(&config.data_dir);

    if let Some(raft_config) = config.raft.as_ref() {
        let snapshot_work_dir = PathBuf::from(&raft_config.data_dir).join("snapshots");
        let recovery = raft::snapshot_install::recover_snapshot_install(
            &data_dir,
            &snapshot_work_dir,
            Arc::clone(&storage_options),
        )
        .await
        .map_err(|error| {
            DualRuntimeError::storage_runtime(format!(
                "Failed to recover interrupted snapshot install before admission: {error}"
            ))
        })?;
        info!("Snapshot install startup recovery result: {:?}", recovery);
    }

    let mut storage = Storage::new(config.db_instance_num, 0);

    info!("Opening storage at path: {:?}", data_dir);
    let mut bg_task_receiver = match storage.open(Arc::clone(&storage_options), &data_dir) {
        Ok(receiver) => receiver,
        Err(error) => {
            let rollback = storage::recover_or_rollback_before_admission(
                &data_dir,
                config.db_instance_num,
                &storage_options,
            );
            let rollback_context = match rollback {
                Ok(true) => "; verified legacy backup restored before admission".to_string(),
                Ok(false) => String::new(),
                Err(rollback_error) => format!("; rollback also failed: {rollback_error}"),
            };
            return Err(DualRuntimeError::storage_runtime(format!(
                "Failed to open storage: {error}{rollback_context}"
            )));
        }
    };
    if storage::close_rollback_window(&data_dir).map_err(|error| {
        DualRuntimeError::storage_runtime(format!("Failed to close rollback window: {error}"))
    })? {
        drop(bg_task_receiver);
        bg_task_receiver = storage
            .reopen(storage_options, &data_dir)
            .map_err(|error| {
                DualRuntimeError::storage_runtime(format!(
                    "Failed to reopen storage after closing rollback window: {error}"
                ))
            })?;
    }
    info!("Storage opened successfully");

    tokio::spawn(async move {
        let mut receiver = bg_task_receiver;
        while let Some(_task) = receiver.recv().await {
            debug!("Processing background task");
        }
        info!("Background task receiver closed");
    });

    Ok(GlobalStorage::new(storage))
}

async fn initialize_storage_server(
    request_receiver: tokio::sync::mpsc::Receiver<runtime::StorageRequest>,
    global_storage: GlobalStorage,
    pause_controller: StorageServerPauseController,
    requirepass: Option<String>,
    info_provider: cmd::server_info::ServerInfoProviderRef,
    gates: cmd::table::CommandTableGates,
) -> Result<(), DualRuntimeError> {
    info!("Initializing storage server...");

    // Initialize the storage-runtime command table with the same password
    // provider used by the network runtime, so AUTH behaves consistently, plus
    // the read-only server-state provider backing the INFO command.
    runtime::initialize_storage_command_table_with_gates(
        Arc::new(move || requirepass.clone()),
        info_provider,
        gates,
    );

    let storage_server =
        StorageServer::with_pause_controller(global_storage, request_receiver, pause_controller);

    info!("Storage server created, starting processing...");
    storage_server.run().await?;

    Ok(())
}

async fn start_server(
    protocol: &str,
    addr: &str,
    runtime_manager: &mut RuntimeManager,
    global_storage: &GlobalStorage,
    config: &Config,
    pause_controller: StorageServerPauseController,
    server_info_provider: Arc<server_info::KiwiServerInfoProvider>,
) -> std::io::Result<()> {
    use conf::raft_type::{Binlog, BinlogResponse};
    use tokio::sync::{mpsc, oneshot};

    // Cluster mode: stand up Raft, wire the append-log bridge, expose a leader gate.
    let leader_gate: Option<Arc<dyn raft::leader_gate::LeaderGate>> = if let Some(raft_config) =
        &config.raft
    {
        let raft_config = RaftConfig {
            node_id: raft_config.node_id,
            raft_addr: raft_config.raft_addr.clone(),
            resp_addr: raft_config.resp_addr.clone(),
            data_dir: PathBuf::from(&raft_config.data_dir),
            db_path: PathBuf::from(&config.data_dir),
            heartbeat_interval: raft_config.heartbeat_interval_ms.unwrap_or(200),
            election_timeout_min: raft_config.election_timeout_min_ms.unwrap_or(500),
            election_timeout_max: raft_config.election_timeout_max_ms.unwrap_or(1500),
            ..RaftConfig::default()
        };

        let storage_swap = global_storage.arc_swap();
        let pause_controller_wrapper = Arc::new(PauseControllerWrapper(pause_controller));
        let append_log_fn_holder = Arc::new(OnceLock::new());

        let raft_app = create_raft_node(
            raft_config,
            storage_swap,
            pause_controller_wrapper,
            Some(append_log_fn_holder.clone()),
        )
        .await
        .map_err(|e| std::io::Error::other(format!("Failed to create Raft node: {}", e)))?;
        server_info_provider.set_raft(raft_app.clone());

        // Bridge: storage runtime -> (channel) -> network runtime drain task -> client_write.
        let (log_tx, mut log_rx) =
            mpsc::unbounded_channel::<(Binlog, oneshot::Sender<Result<BinlogResponse, String>>)>();

        // NOTE: this drains serially — one Raft consensus round-trip at a time.
        // Acceptable for now; a throughput follow-up may spawn per-message or add
        // backpressure via a bounded channel.
        let raft_for_drain = raft_app.clone();
        tokio::spawn(async move {
            loop {
                let received = log_rx.recv().await;
                let Some((binlog, resp_tx)) = received else {
                    break;
                };
                let result = raft_for_drain
                    .client_write(binlog)
                    .await
                    .map_err(|e| e.to_string());
                let _ = resp_tx.send(result);
            }
            warn!("Raft append-log drain task exited: channel closed");
        });

        // append_log_fn is invoked synchronously from BinlogBatch::commit, which
        // runs inside a tokio task on the (multi-threaded) storage runtime. A bare
        // blocking_recv would panic ("Cannot block ... within an asynchronous
        // execution context"), so we wrap it in block_in_place; the drain task that
        // resolves the oneshot runs on the separate network runtime.
        let append_log_fn: storage::AppendLogFn = Arc::new(move |binlog| {
            let (tx, rx) = oneshot::channel();
            log_tx
                .send((binlog, tx))
                .map_err(|_| "raft log channel closed".to_string())?;
            tokio::task::block_in_place(|| rx.blocking_recv())
                .map_err(|_| "raft response channel closed".to_string())?
        });
        let _ = append_log_fn_holder.set(append_log_fn.clone());
        global_storage.load().set_append_log_fn(append_log_fn);

        let raft_addr = raft_app.raft_addr.clone();
        let grpc_addr = raft_addr.parse::<std::net::SocketAddr>().map_err(|e| {
            std::io::Error::other(format!("Invalid Raft address '{}': {}", raft_addr, e))
        })?;

        let (core_svc, admin_svc, client_svc, metrics_svc) =
            RaftApp::create_grpc_services(raft_app.clone());

        info!("Starting Raft gRPC server on {}", raft_addr);

        let reflect_svc = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(raft_proto::FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|e| {
                std::io::Error::other(format!("Failed to create reflection service: {}", e))
            })?;

        let grpc_listener = tokio::net::TcpListener::bind(grpc_addr)
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to bind Raft gRPC server on {}: {}",
                    grpc_addr, e
                ))
            })?;

        tokio::spawn(async move {
            use tonic::transport::Server;

            let incoming = tokio_stream::wrappers::TcpListenerStream::new(grpc_listener);

            info!("Raft gRPC server listening on {}", grpc_addr);

            if let Err(e) = Server::builder()
                .add_service(reflect_svc)
                .add_service(core_svc)
                .add_service(admin_svc)
                .add_service(client_svc)
                .add_service(metrics_svc)
                .serve_with_incoming(incoming)
                .await
            {
                let error_message = e.to_string();
                error!("Raft gRPC server error: {}", error_message);
            }
        });

        let raft_leader_gate = raft_app as Arc<dyn raft::leader_gate::LeaderGate>;
        Some(raft_leader_gate)
    } else {
        None
    };

    match net::ServerFactory::create_server(
        protocol,
        Some(addr.to_string()),
        runtime_manager,
        config.requirepass.clone(),
        leader_gate,
        command_table_gates(config),
        vector_admission_limits(config),
    ) {
        Some(server) => {
            tokio::spawn(async move {
                if let Err(e) = server.run().await {
                    error!("Redis server error: {}", e);
                }
            });
            Ok(())
        }
        _ => Err(std::io::Error::other(format!(
            "Failed to create server for protocol '{}' on address '{}'",
            protocol, addr
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use raft::snapshot_install::{
        SNAPSHOT_INSTALL_MARKER_VERSION, SnapshotInstallMarkerV2, SnapshotInstallPhase,
        SnapshotInstallStorageIdentity,
    };
    use storage::{ManifestDigest, SnapshotInstanceManifest};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn vector_admission_limits_follow_config() {
        let config = Config {
            vector: conf::vector_config::VectorConfig {
                max_dimension: 17,
                max_element_bytes: 23,
                max_vector_bytes: 29,
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            vector_admission_limits(&config),
            cmd::vector::admission::VectorAdmissionLimits {
                max_dimension: 17,
                max_element_bytes: 23,
                max_vector_bytes: 29,
            }
        );
    }

    fn write_valid_install_marker(db_path: &std::path::Path) -> PathBuf {
        let digest = ManifestDigest::compute(b"server startup preflight");
        let marker = SnapshotInstallMarkerV2 {
            version: SNAPSHOT_INSTALL_MARKER_VERSION,
            phase: SnapshotInstallPhase::StagedValidated,
            snapshot_id: "server-startup-preflight".to_string(),
            last_log_index: 10,
            last_log_term: 2,
            db_instance_num: 1,
            target_name: db_path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("test DB path should have a UTF-8 basename")
                .to_string(),
            staged_name: ".restore_temp_server-preflight".to_string(),
            backup_name: format!(
                ".{}.snapshot-install-backup-server-preflight",
                db_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .expect("test DB path should have a UTF-8 basename")
            ),
            pending_snapshot_data_name: ".snapshot-install-server-preflight.tar".to_string(),
            pending_raft_meta_name: ".snapshot-install-server-preflight.raft-meta.json".to_string(),
            pending_checkpoint_meta_name: ".snapshot-install-server-preflight.checkpoint-meta.json"
                .to_string(),
            snapshot_archive_digest: digest.clone(),
            raft_metadata_digest: digest.clone(),
            checkpoint_metadata_digest: digest.clone(),
            old_storage: None,
            new_storage: SnapshotInstallStorageIdentity {
                root_manifest_id: "server-preflight-root".to_string(),
                root_manifest_digest: digest.clone(),
                instance_manifests: vec![SnapshotInstanceManifest {
                    instance_id: 0,
                    manifest_digest: digest.clone(),
                    storage_incarnation: 1,
                }],
                logical_instance_digests: vec![digest],
            },
        };
        let marker_path = raft::snapshot_install::snapshot_install_marker_path(db_path)
            .expect("test DB path should support a marker");
        std::fs::write(
            &marker_path,
            serde_json::to_vec(&marker).expect("test marker should serialize"),
        )
        .expect("test marker should be written");
        marker_path
    }

    #[test]
    fn startup_preflight_rejects_marker_even_when_raft_is_disabled() {
        let root = std::env::temp_dir().join(format!(
            "kiwi-server-preflight-{}-{}",
            std::process::id(),
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let db_path = root.join("data");
        std::fs::create_dir_all(&root).expect("test should create its temporary root");
        let marker_path = raft::state_machine::snapshot_install_marker_path(&db_path)
            .expect("test DB path should support a marker");
        std::fs::write(&marker_path, b"{not-json")
            .expect("test should write a malformed install marker");
        let config = Config {
            data_dir: db_path.display().to_string(),
            raft: None,
            ..Default::default()
        };

        let error = preflight_server_startup(&config)
            .expect_err("server startup must reject an install marker without Raft configured");
        assert!(
            error
                .to_string()
                .contains(&marker_path.display().to_string()),
            "server refusal must identify the marker path: {error}"
        );

        std::fs::remove_file(marker_path).expect("test should remove its marker");
        std::fs::remove_dir_all(root).expect("test should remove its temporary root");
    }

    #[test]
    fn startup_preflight_allows_valid_marker_only_for_raft_recovery() {
        let root = std::env::temp_dir().join(format!(
            "kiwi-server-valid-preflight-{}-{}",
            std::process::id(),
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let db_path = root.join("data");
        std::fs::create_dir_all(&root).expect("test should create its temporary root");
        let marker_path = write_valid_install_marker(&db_path);

        let standalone = Config {
            data_dir: db_path.display().to_string(),
            db_instance_num: 1,
            raft: None,
            ..Default::default()
        };
        let error = preflight_server_startup(&standalone)
            .expect_err("standalone startup cannot recover a Raft snapshot install");
        assert!(error.to_string().contains("Raft is disabled"));

        let mismatched = Config {
            data_dir: db_path.display().to_string(),
            db_instance_num: 2,
            raft: Some(Default::default()),
            ..Default::default()
        };
        let error = preflight_server_startup(&mismatched)
            .expect_err("startup must reject marker/config instance-count drift");
        assert!(error.to_string().contains("describes 1 instances"));
        assert!(error.to_string().contains("startup config requires 2"));

        let clustered = Config {
            data_dir: db_path.display().to_string(),
            db_instance_num: 1,
            raft: Some(Default::default()),
            ..Default::default()
        };
        preflight_server_startup(&clustered)
            .expect("clustered startup should defer a valid marker to startup recovery");

        std::fs::remove_file(marker_path).expect("test should remove its marker");
        std::fs::remove_dir_all(root).expect("test should remove its temporary root");
    }
}
