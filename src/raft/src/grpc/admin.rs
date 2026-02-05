// RaftAdminService 实现 - 集群管理服务
// 用途：集群初始化、成员变更等管理操作

use conf::raft_type::KiwiNode;
use openraft::ChangeMembers;
use std::collections::BTreeMap;
use std::sync::Arc;

// 导入 proto 生成的类型
use crate::raft_proto::{
    raft_admin_service_server::{RaftAdminService, RaftAdminServiceServer},
    InitializeRequest, InitializeResponse,
    AddLearnerRequest, AddLearnerResponse,
    ChangeMembershipRequest, ChangeMembershipResponse,
    RemoveNodeRequest, RemoveNodeResponse,
    Response as ProtoResponse,
};
use tonic::{Request, Response as TonicResponse, Status};

use crate::node::RaftApp;

/// Raft 管理服务实现
pub struct RaftAdminServiceImpl {
    app: Arc<RaftApp>,
}

impl RaftAdminServiceImpl {
    pub fn new(app: Arc<RaftApp>) -> Self {
        Self { app }
    }
}

/// 创建 RaftAdminService 服务器
pub fn create_admin_service(app: Arc<RaftApp>) -> RaftAdminServiceServer<RaftAdminServiceImpl> {
    RaftAdminServiceServer::new(RaftAdminServiceImpl::new(app))
}

/// NodeConfig 转 KiwiNode
fn proto_to_node(node: &crate::raft_proto::NodeConfig) -> KiwiNode {
    KiwiNode {
        raft_addr: node.raft_addr.clone(),
        resp_addr: node.resp_addr.clone(),
    }
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

#[tonic::async_trait]
impl RaftAdminService for RaftAdminServiceImpl {
    /// 初始化集群
    async fn initialize(
        &self,
        request: Request<InitializeRequest>,
    ) -> Result<TonicResponse<InitializeResponse>, Status> {
        let proto_req = request.into_inner();

        log::info!("Initializing cluster with {} nodes", proto_req.nodes.len());

        // 转换 Proto → BTreeMap<u64, KiwiNode>
        let mut nodes = BTreeMap::new();
        for node_config in &proto_req.nodes {
            let node = proto_to_node(node_config);
            log::info!("  Node {}: raft_addr={}, resp_addr={}",
                node_config.node_id, node_config.raft_addr, node_config.resp_addr);
            nodes.insert(node_config.node_id, node);
        }

        // 调用 OpenRaft initialize
        match self.app.raft.initialize(nodes).await {
            Ok(_) => {
                log::info!("Cluster initialized successfully");

                // 获取 leader 信息
                let leader_id = self.app.raft.metrics().borrow().current_leader;

                Ok(TonicResponse::new(InitializeResponse {
                    response: Some(ok_response()),
                    leader_id: leader_id.unwrap_or(0),
                }))
            }
            Err(e) => {
                log::error!("Failed to initialize cluster: {}", e);
                Err(Status::internal(format!("Failed to initialize cluster: {}", e)))
            }
        }
    }

    /// 添加学习者节点
    async fn add_learner(
        &self,
        request: Request<AddLearnerRequest>,
    ) -> Result<TonicResponse<AddLearnerResponse>, Status> {
        let proto_req = request.into_inner();

        log::info!("Adding learner node: {}", proto_req.node_id);

        let node = match proto_req.node {
            Some(n) => proto_to_node(&n),
            None => return Err(Status::invalid_argument("Missing node config")),
        };

        match self.app.raft.add_learner(proto_req.node_id, node, true).await {
            Ok(_) => {
                log::info!("Learner node {} added successfully", proto_req.node_id);
                Ok(TonicResponse::new(AddLearnerResponse {
                    response: Some(ok_response()),
                }))
            }
            Err(e) => {
                log::error!("Failed to add learner: {}", e);
                Err(Status::internal(format!("Failed to add learner: {}", e)))
            }
        }
    }

    /// 修改集群成员
    async fn change_membership(
        &self,
        request: Request<ChangeMembershipRequest>,
    ) -> Result<TonicResponse<ChangeMembershipResponse>, Status> {
        let proto_req = request.into_inner();

        log::info!(
            "Changing membership with {} members, retain={}",
            proto_req.members.len(),
            proto_req.retain
        );

        // 转换 Proto → BTreeMap<u64, KiwiNode>
        let mut members = BTreeMap::new();
        for node_config in &proto_req.members {
            let node = proto_to_node(node_config);
            members.insert(node_config.node_id, node);
        }

        let changes = ChangeMembers::ReplaceAllNodes(members);

        match self.app.raft.change_membership(changes, proto_req.retain).await {
            Ok(_) => {
                log::info!("Membership changed successfully");
                Ok(TonicResponse::new(ChangeMembershipResponse {
                    response: Some(ok_response()),
                }))
            }
            Err(e) => {
                log::error!("Failed to change membership: {}", e);
                Err(Status::internal(format!("Failed to change membership: {}", e)))
            }
        }
    }

    /// 移除节点
    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<TonicResponse<RemoveNodeResponse>, Status> {
        let proto_req = request.into_inner();

        log::info!("Removing node: {}", proto_req.node_id);

        // 获取当前成员并提前提取数据
        let new_members = {
            let metrics = self.app.raft.metrics();
            let guard = metrics.borrow();
            let membership = guard.membership_config.membership();

            // 构建不包含要移除节点的新成员集合
            let mut new_members = BTreeMap::new();
            let nodes_iter = membership.nodes().collect::<Vec<_>>();
            for (node_id, node) in nodes_iter {
                if *node_id != proto_req.node_id {
                    new_members.insert(*node_id, node.clone());
                }
            }
            new_members
        };

        // 使用 ReplaceAllNodes 替换成员（排除要移除的节点）
        let changes = ChangeMembers::ReplaceAllNodes(new_members);

        match self.app.raft.change_membership(changes, false).await {
            Ok(_) => {
                log::info!("Node {} removed successfully", proto_req.node_id);
                Ok(TonicResponse::new(RemoveNodeResponse {
                    response: Some(ok_response()),
                }))
            }
            Err(e) => {
                log::error!("Failed to remove node: {}", e);
                Err(Status::internal(format!("Failed to remove node: {}", e)))
            }
        }
    }
}
