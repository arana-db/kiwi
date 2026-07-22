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

use std::future::Future;
use std::sync::Arc;

use conf::raft_type::{Binlog, BinlogEntry, OperateType};
use storage::slot_indexer::key_to_slot_id;
use tonic::{Request, Response as TonicResponse, Status};

use crate::node::RaftApp;
use crate::raft_proto::{
    LeaderRequest, LeaderResponse, MembersRequest, MembersResponse, MetricsRequest,
    MetricsResponse, NodeConfig, ReadRequest, ReadResponse, Response as ProtoResponse,
    WriteRequest, WriteResponse,
    raft_client_service_server::{RaftClientService, RaftClientServiceServer},
    raft_metrics_service_server::{RaftMetricsService, RaftMetricsServiceServer},
};
use crate::state_machine::{PauseController, StorageAccessPermit};

pub struct RaftClientServiceImpl {
    app: Arc<RaftApp>,
}

impl RaftClientServiceImpl {
    pub fn new(app: Arc<RaftApp>) -> Self {
        Self { app }
    }
}

pub fn create_client_service(app: Arc<RaftApp>) -> RaftClientServiceServer<RaftClientServiceImpl> {
    RaftClientServiceServer::new(RaftClientServiceImpl::new(app))
}

pub struct RaftMetricsServiceImpl {
    app: Arc<RaftApp>,
}

impl RaftMetricsServiceImpl {
    pub fn new(app: Arc<RaftApp>) -> Self {
        Self { app }
    }
}

pub fn create_metrics_service(
    app: Arc<RaftApp>,
) -> RaftMetricsServiceServer<RaftMetricsServiceImpl> {
    RaftMetricsServiceServer::new(RaftMetricsServiceImpl::new(app))
}

fn ok_response() -> ProtoResponse {
    ProtoResponse {
        success: true,
        message: "OK".to_string(),
    }
}

fn error_response(msg: String) -> ProtoResponse {
    ProtoResponse {
        success: false,
        message: msg,
    }
}

async fn ensure_linearizable_then_enter<F>(
    pause_controller: Arc<dyn PauseController>,
    ensure_linearizable: F,
) -> Result<Box<dyn StorageAccessPermit>, Status>
where
    F: Future<Output = Result<(), Status>>,
{
    ensure_linearizable.await?;
    Ok(pause_controller.enter().await)
}

#[tonic::async_trait]
impl RaftClientService for RaftClientServiceImpl {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<TonicResponse<WriteResponse>, Status> {
        let proto_req = request.into_inner();
        let binlog = proto_binlog_to_binlog(proto_req.binlog)?;

        match self.app.client_write(binlog).await {
            Ok(response) => {
                let proto_response = if response.success {
                    ok_response()
                } else {
                    error_response(
                        response
                            .message
                            .unwrap_or_else(|| "Unknown error".to_string()),
                    )
                };

                let log_id = response.log_id.map(|idx| crate::raft_proto::LogId {
                    leader_id: None,
                    index: idx,
                });

                Ok(TonicResponse::new(WriteResponse {
                    response: Some(proto_response),
                    log_id,
                }))
            }
            Err(e) => {
                log::error!("Failed to write to Raft: {}", e);
                Err(Status::internal(format!("Write failed: {}", e)))
            }
        }
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<TonicResponse<ReadResponse>, Status> {
        let proto_req = request.into_inner();
        let key = &proto_req.key;

        if !self.app.is_leader() {
            if let Some((_, node)) = self.app.get_leader() {
                return Err(Status::failed_precondition(format!(
                    "Not leader, redirect to: {}",
                    node.raft_addr
                )));
            }
            return Err(Status::unavailable("No leader available".to_string()));
        }

        let _storage_access =
            ensure_linearizable_then_enter(Arc::clone(&self.app.pause_controller), async {
                self.app
                    .raft
                    .ensure_linearizable()
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        Status::failed_precondition(format!("Linearizable read failed: {}", e))
                    })
            })
            .await?;

        if !self.app.is_leader() {
            if let Some((_, node)) = self.app.get_leader() {
                return Err(Status::failed_precondition(format!(
                    "Not leader, redirect to: {}",
                    node.raft_addr
                )));
            }
            return Err(Status::unavailable("No leader available".to_string()));
        }

        let storage = self.app.storage_swap.load_full();
        let slot_id = key_to_slot_id(key);
        let instance_id = storage.slot_indexer.get_instance_id(slot_id);
        let instance = storage.insts.get(instance_id).cloned().ok_or_else(|| {
            Status::internal(format!(
                "Storage instance {instance_id} is unavailable for slot {slot_id}"
            ))
        })?;
        let key = key.to_vec();
        let key_for_read = key.clone();

        let read_result = tokio::task::spawn_blocking(move || instance.get_binary(&key_for_read))
            .await
            .map_err(|e| Status::internal(format!("Read task failed: {}", e)))?;

        match read_result {
            Ok(val) => Ok(TonicResponse::new(ReadResponse {
                response: Some(ok_response()),
                value: val,
            })),
            Err(storage::error::Error::KeyNotFound { .. }) => {
                Ok(TonicResponse::new(ReadResponse {
                    response: Some(error_response(format!(
                        "Key not found: {:?}",
                        String::from_utf8_lossy(&key)
                    ))),
                    value: vec![],
                }))
            }
            Err(e) => Err(Status::internal(format!("Read failed: {}", e))),
        }
    }
}

#[tonic::async_trait]
impl RaftMetricsService for RaftMetricsServiceImpl {
    async fn metrics(
        &self,
        _request: Request<MetricsRequest>,
    ) -> Result<TonicResponse<MetricsResponse>, Status> {
        let is_leader = self.app.is_leader();
        let metrics = self.app.raft.metrics();
        let guard = metrics.borrow();
        let current_leader = guard.current_leader.unwrap_or(0);
        drop(guard);

        Ok(TonicResponse::new(MetricsResponse {
            response: Some(ok_response()),
            is_leader,
            replication_lag: None,
            current_leader,
        }))
    }

    async fn leader(
        &self,
        _request: Request<LeaderRequest>,
    ) -> Result<TonicResponse<LeaderResponse>, Status> {
        match self.app.get_leader() {
            Some((leader_id, node)) => Ok(TonicResponse::new(LeaderResponse {
                response: Some(ok_response()),
                leader_id,
                leader_node: Some(NodeConfig {
                    node_id: leader_id,
                    raft_addr: node.raft_addr.clone(),
                    resp_addr: node.resp_addr.clone(),
                }),
            })),
            None => Err(Status::unavailable("No leader available".to_string())),
        }
    }

    async fn members(
        &self,
        _request: Request<MembersRequest>,
    ) -> Result<TonicResponse<MembersResponse>, Status> {
        let metrics = self.app.raft.metrics();
        let guard = metrics.borrow();
        let membership = guard.membership_config.membership();

        let members: Vec<NodeConfig> = membership
            .nodes()
            .map(|(id, node)| NodeConfig {
                node_id: *id,
                raft_addr: node.raft_addr.clone(),
                resp_addr: node.resp_addr.clone(),
            })
            .collect();

        let learner_ids = membership.learner_ids();
        let learners: Vec<u64> = learner_ids.collect();

        drop(guard);

        Ok(TonicResponse::new(MembersResponse {
            response: Some(ok_response()),
            members,
            learners,
        }))
    }
}

fn proto_binlog_to_binlog(
    proto_binlog: Option<crate::raft_proto::Binlog>,
) -> Result<Binlog, Status> {
    let proto_binlog =
        proto_binlog.ok_or_else(|| Status::invalid_argument("Missing binlog".to_string()))?;

    let entries: Vec<BinlogEntry> = proto_binlog
        .entries
        .into_iter()
        .map(|proto_entry| {
            let (op_type, value) = match proto_entry.op_type.as_str() {
                "Put" => (OperateType::Put, Some(proto_entry.value)),
                "Delete" => (OperateType::Delete, None),
                other => {
                    return Err(Status::invalid_argument(format!(
                        "Unsupported op_type: {other}"
                    )));
                }
            };

            Ok(BinlogEntry {
                cf_idx: proto_entry.cf_idx,
                op_type,
                key: proto_entry.key,
                value,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;

    Ok(Binlog {
        db_id: proto_binlog.db_id as u32,
        slot_idx: proto_binlog.slot_idx as u32,
        entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_proto::{Binlog as ProtoBinlog, BinlogEntry as ProtoBinlogEntry};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct TestPauseController {
        active: AtomicUsize,
    }

    struct TestStorageAccessPermit(Arc<TestPauseController>);

    impl StorageAccessPermit for TestStorageAccessPermit {}

    impl Drop for TestStorageAccessPermit {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl PauseController for TestPauseController {
        fn request_pause(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }

        fn enter(
            self: Arc<Self>,
        ) -> std::pin::Pin<Box<dyn Future<Output = Box<dyn StorageAccessPermit>> + Send + 'static>>
        {
            Box::pin(async move {
                self.active.fetch_add(1, Ordering::SeqCst);
                Box::new(TestStorageAccessPermit(self)) as Box<dyn StorageAccessPermit>
            })
        }

        fn resume(&self) {}
    }

    #[tokio::test]
    async fn linearizable_wait_does_not_hold_storage_access() {
        let controller = Arc::new(TestPauseController::default());
        let trait_controller: Arc<dyn PauseController> = controller.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let observed_controller = controller.clone();

        let acquire = tokio::spawn(async move {
            ensure_linearizable_then_enter(trait_controller, async move {
                assert_eq!(observed_controller.active.load(Ordering::SeqCst), 0);
                started_tx
                    .send(())
                    .expect("test should observe linearizable wait");
                release_rx
                    .await
                    .expect("test should release linearizable wait");
                assert_eq!(observed_controller.active.load(Ordering::SeqCst), 0);
                Ok(())
            })
            .await
        });

        started_rx
            .await
            .expect("linearizable wait should start before permit acquisition");
        assert_eq!(controller.active.load(Ordering::SeqCst), 0);
        release_tx
            .send(())
            .expect("linearizable wait task should still be alive");

        let permit = acquire
            .await
            .expect("permit task should not panic")
            .expect("linearizable wait should succeed");
        assert_eq!(controller.active.load(Ordering::SeqCst), 1);
        drop(permit);
        assert_eq!(controller.active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn proto_binlog_rejects_unknown_operation() {
        let err = proto_binlog_to_binlog(Some(ProtoBinlog {
            db_id: 1,
            slot_idx: 2,
            entries: vec![ProtoBinlogEntry {
                cf_idx: 0,
                op_type: "NoOp".to_string(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        }))
        .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn proto_binlog_clears_delete_value() {
        let binlog = proto_binlog_to_binlog(Some(ProtoBinlog {
            db_id: 1,
            slot_idx: 2,
            entries: vec![ProtoBinlogEntry {
                cf_idx: 0,
                op_type: "Delete".to_string(),
                key: b"k".to_vec(),
                value: b"stale".to_vec(),
            }],
        }))
        .unwrap();

        assert_eq!(binlog.entries.len(), 1);
        assert_eq!(binlog.entries[0].op_type, OperateType::Delete);
        assert_eq!(binlog.entries[0].value, None);
    }
}
