// RaftClientService 和 RaftMetricsService 实现
// 用途：客户端读写数据、查询集群状态

use conf::raft_type::{Binlog, BinlogEntry, OperateType};
use openraft::Raft;
use std::sync::Arc;
use storage::slot_indexer::key_to_slot_id;
use storage::ColumnFamilyIndex;

// 导入 proto 生成的类型
use crate::raft_proto::{
    raft_client_service_server::{RaftClientService, RaftClientServiceServer},
    raft_metrics_service_server::{RaftMetricsService, RaftMetricsServiceServer},
    WriteRequest, WriteResponse,
    ReadRequest, ReadResponse,
    MetricsRequest, MetricsResponse,
    LeaderRequest, LeaderResponse,
    MembersRequest, MembersResponse,
    Response as ProtoResponse, LogId, NodeConfig,
};
use tonic::{Request, Response as TonicResponse, Status};

use crate::node::RaftApp;

/// Raft 客户端服务实现
pub struct RaftClientServiceImpl {
    app: Arc<RaftApp>,
}

impl RaftClientServiceImpl {
    pub fn new(app: Arc<RaftApp>) -> Self {
        Self { app }
    }
}

/// 创建 RaftClientService 服务器
pub fn create_client_service(app: Arc<RaftApp>) -> RaftClientServiceServer<RaftClientServiceImpl> {
    RaftClientServiceServer::new(RaftClientServiceImpl::new(app))
}

/// Raft 指标服务实现
pub struct RaftMetricsServiceImpl {
    app: Arc<RaftApp>,
}

impl RaftMetricsServiceImpl {
    pub fn new(app: Arc<RaftApp>) -> Self {
        Self { app }
    }
}

/// 创建 RaftMetricsService 服务器
pub fn create_metrics_service(app: Arc<RaftApp>) -> RaftMetricsServiceServer<RaftMetricsServiceImpl> {
    RaftMetricsServiceServer::new(RaftMetricsServiceImpl::new(app))
}

/// 成功响应
fn ok_response() -> ProtoResponse {
    ProtoResponse {
        success: true,
        message: "OK".to_string(),
    }
}

/// 错误响应
fn error_response(msg: String) -> ProtoResponse {
    ProtoResponse {
        success: false,
        message: msg,
    }
}

/// LogId 转 Proto
fn log_id_to_proto(log_id: Option<openraft::LogId<u64>>) -> Option<LogId> {
    log_id.map(|lid| LogId {
        leader_id: Some(crate::raft_proto::LeaderId {
            term: lid.leader_id.term,
            node_id: Some(crate::raft_proto::NodeId { id: lid.leader_id.node_id }),
        }),
        index: lid.index,
    })
}

// ============================================================================
// RaftClientService 实现
// ============================================================================

#[tonic::async_trait]
impl RaftClientService for RaftClientServiceImpl {
    /// 写入数据（通过 Raft 共识）
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<TonicResponse<WriteResponse>, Status> {
        let proto_req = request.into_inner();

        // Proto Binlog → 实际 Binlog
        let binlog = proto_binlog_to_binlog(proto_req.binlog)?;

        match self.app.client_write(binlog).await {
            Ok(response) => {
                if response.success {
                    Ok(TonicResponse::new(WriteResponse {
                        response: Some(ok_response()),
                        log_id: log_id_to_proto(None), // TODO: 返回实际的 log_id
                    }))
                } else {
                    Err(Status::internal(response.message.unwrap_or_else(|| "Write failed".to_string())))
                }
            }
            Err(e) => {
                log::error!("Failed to write to Raft: {}", e);
                Err(Status::internal(format!("Write failed: {}", e)))
            }
        }
    }

    /// 读取数据
    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<TonicResponse<ReadResponse>, Status> {
        let proto_req = request.into_inner();
        let key = &proto_req.key;

        // 检查是否为 leader
        if !self.app.is_leader() {
            if let Some((_, node)) = self.app.get_leader() {
                return Err(Status::failed_precondition(format!(
                    "Not leader, redirect to: {}", node.raft_addr
                )));
            }
            return Err(Status::unavailable("No leader available".to_string()));
        }

        // 计算实例 ID
        let slot_id = key_to_slot_id(key);
        let instance_id = self.app.storage.slot_indexer.get_instance_id(slot_id);
        let instance = &self.app.storage.insts[instance_id];

        // 从 MetaCF 读取
        match instance.get_cf_handle(ColumnFamilyIndex::MetaCF) {
            Some(cf) => {
                let db = instance.db.as_ref().ok_or_else(|| {
                    Status::internal("Database not initialized".to_string())
                })?;
                match db.get_cf(&cf, key) {
                    Ok(Some(val)) => {
                        Ok(TonicResponse::new(ReadResponse {
                            response: Some(ok_response()),
                            value: val,
                        }))
                    }
                    Ok(None) => {
                        Ok(TonicResponse::new(ReadResponse {
                            response: Some(error_response(format!(
                                "Key not found: {:?}",
                                String::from_utf8_lossy(key)
                            ))),
                            value: vec![],
                        }))
                    }
                    Err(e) => {
                        Err(Status::internal(format!("Read failed: {}", e)))
                    }
                }
            }
            None => {
                Err(Status::internal("MetaCF not found".to_string()))
            }
        }
    }
}

// ============================================================================
// RaftMetricsService 实现
// ============================================================================

#[tonic::async_trait]
impl RaftMetricsService for RaftMetricsServiceImpl {
    /// 获取集群指标
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
            replication_lag: if !is_leader { 0 } else { 0 },
            current_leader,
        }))
    }

    /// 获取 Leader 信息
    async fn leader(
        &self,
        _request: Request<LeaderRequest>,
    ) -> Result<TonicResponse<LeaderResponse>, Status> {
        match self.app.get_leader() {
            Some((leader_id, node)) => {
                Ok(TonicResponse::new(LeaderResponse {
                    response: Some(ok_response()),
                    leader_id,
                    leader_node: Some(NodeConfig {
                        node_id: leader_id,
                        raft_addr: node.raft_addr.clone(),
                        resp_addr: node.resp_addr.clone(),
                    }),
                }))
            }
            None => {
                Err(Status::unavailable("No leader available".to_string()))
            }
        }
    }

    /// 获取集群成员列表
    async fn members(
        &self,
        _request: Request<MembersRequest>,
    ) -> Result<TonicResponse<MembersResponse>, Status> {
        let metrics = self.app.raft.metrics();
        let guard = metrics.borrow();
        let membership = guard.membership_config.membership();

        // 获取所有节点配置
        let members: Vec<NodeConfig> = membership
            .nodes()
            .map(|(id, node)| NodeConfig {
                node_id: *id,
                raft_addr: node.raft_addr.clone(),
                resp_addr: node.resp_addr.clone(),
            })
            .collect();

        // 获取 learner ids
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

// ============================================================================
// 辅助函数
// ============================================================================

/// Proto Binlog → 实际 Binlog
fn proto_binlog_to_binlog(proto_binlog: Option<crate::raft_proto::Binlog>) -> Result<Binlog, Status> {
    let proto_binlog = proto_binlog.ok_or_else(|| {
        Status::invalid_argument("Missing binlog".to_string())
    })?;

    // 转换 Proto Binlog → 实际 Binlog
    let entries: Vec<BinlogEntry> = proto_binlog
        .entries
        .into_iter()
        .map(|proto_entry| {
            let op_type = match proto_entry.op_type.as_str() {
                "Put" => OperateType::Put,
                "Delete" => OperateType::Delete,
                _ => OperateType::Put, // 默认
            };
            BinlogEntry {
                cf_idx: proto_entry.cf_idx,
                op_type,
                key: proto_entry.key,
                value: Some(proto_entry.value),
            }
        })
        .collect();

    Ok(Binlog {
        db_id: proto_binlog.db_id as u32,
        slot_idx: proto_binlog.slot_idx as u32,
        entries,
    })
}
