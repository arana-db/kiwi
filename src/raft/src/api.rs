use std::collections::BTreeMap;
use std::sync::Arc;

use conf::raft_type::{Binlog, KiwiNode, KiwiTypeConfig};
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::ChangeMembers;
use serde::{Deserialize, Serialize};

use crate::node::RaftApp;
use actix_web::{
    HttpResponse, Responder, get, post,
    web::{self, Json},
};

#[derive(Clone)]
pub struct RaftAppData {
    pub app: Arc<RaftApp>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteRequest {
    pub binlog: Binlog,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadRequest {
    pub key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadResponse {
    pub value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaderResponse {
    pub leader_id: u64,
    pub node: Option<KiwiNode>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub is_leader: bool,
    pub replication_lag: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitRequest {
    pub nodes: Vec<(u64, KiwiNode)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddLearnerRequest {
    pub node_id: u64,
    pub node: KiwiNode,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeMembershipRequest {
    pub members: Vec<(u64, KiwiNode)>,
    pub retain: bool,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub message: String,
    pub data: Option<T>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            message: "OK".to_string(),
            data: Some(data),
        }
    }

    fn error(message: String) -> Self {
        Self {
            success: false,
            message,
            data: None,
        }
    }
}

#[post("/raft/write")]
pub async fn write(app_data: web::Data<RaftAppData>, req: Json<WriteRequest>) -> impl Responder {
    match app_data.app.client_write(req.0.binlog).await {
        Ok(response) => HttpResponse::Ok().json(ApiResponse::success(response)),
        Err(e) => {
            log::error!("Failed to write to Raft: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse::<()>::error(e.to_string()))
        }
    }
}

#[post("/raft/read")]
pub async fn read(app_data: web::Data<RaftAppData>, req: Json<ReadRequest>) -> impl Responder {
    if !app_data.app.is_leader() {
        if let Some((_, node)) = app_data.app.get_leader() {
            let message = format!("Not leader, redirect to: {}", node.raft_addr);
            return HttpResponse::TemporaryRedirect()
                .insert_header(("Location", node.raft_addr))
                .json(ApiResponse::<()>::error(message));
        }
        return HttpResponse::ServiceUnavailable()
            .json(ApiResponse::<()>::error("No leader available".to_string()));
    }

    let value = app_data.app.storage.get(&req.key);
    match value {
        Ok(v) => HttpResponse::Ok().json(ApiResponse::success(ReadResponse { value: Some(v) })),
        Err(e) => {
            log::error!("Failed to read from storage: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse::<()>::error(e.to_string()))
        }
    }
}

#[get("/raft/metrics")]
pub async fn metrics(app_data: web::Data<RaftAppData>) -> impl Responder {
    let is_leader = app_data.app.is_leader();
    let leader_info = app_data.app.get_leader();

    let replication_lag =
        leader_info.and_then(|(leader_id, _)| if !is_leader { Some(0) } else { None });

    HttpResponse::Ok().json(ApiResponse::success(MetricsResponse {
        is_leader,
        replication_lag,
    }))
}

#[get("/raft/leader")]
pub async fn leader(app_data: web::Data<RaftAppData>) -> impl Responder {
    match app_data.app.get_leader() {
        Some((leader_id, node)) => HttpResponse::Ok().json(ApiResponse::success(LeaderResponse {
            leader_id,
            node: Some(node),
        })),
        None => HttpResponse::ServiceUnavailable()
            .json(ApiResponse::<()>::error("No leader available".to_string())),
    }
}

#[post("/raft/init")]
pub async fn init(app_data: web::Data<RaftAppData>, req: Json<InitRequest>) -> impl Responder {
    log::info!("Initializing cluster with {} nodes", req.nodes.len());

    let raft = app_data.app.raft.clone();
    let init_req = req.into_inner();
    let nodes: BTreeMap<u64, KiwiNode> = init_req.nodes.into_iter().collect();

    match raft.initialize(nodes).await {
        Ok(_) => HttpResponse::Ok().json(ApiResponse::success(())),
        Err(e) => {
            log::error!("Failed to initialize cluster: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse::<()>::error(e.to_string()))
        }
    }
}

#[post("/raft/add_learner")]
pub async fn add_learner(
    app_data: web::Data<RaftAppData>,
    req: Json<AddLearnerRequest>,
) -> impl Responder {
    let add_req = req.into_inner();
    log::info!("Adding learner node: {}", add_req.node_id);
    let raft = app_data.app.raft.clone();

    match raft.add_learner(add_req.node_id, add_req.node, true).await {
        Ok(_) => HttpResponse::Ok().json(ApiResponse::success(())),
        Err(e) => {
            log::error!("Failed to add learner: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse::<()>::error(e.to_string()))
        }
    }
}

#[post("/raft/change_membership")]
pub async fn change_membership(
    app_data: web::Data<RaftAppData>,
    req: Json<ChangeMembershipRequest>,
) -> impl Responder {
    let change_req = req.into_inner();
    log::info!(
        "Changing membership with {} members, retain={}",
        change_req.members.len(),
        change_req.retain
    );

    let raft = app_data.app.raft.clone();
    let members_map: BTreeMap<u64, KiwiNode> = change_req.members.into_iter().collect();
    let changes = ChangeMembers::ReplaceAllNodes(members_map);

    match raft.change_membership(changes, change_req.retain).await {
        Ok(_) => HttpResponse::Ok().json(ApiResponse::success(())),
        Err(e) => {
            log::error!("Failed to change membership: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse::<()>::error(e.to_string()))
        }
    }
}

// --- Raft protocol RPC (used by KiwiNetwork when nodes talk to each other)

#[post("/raft/vote")]
pub async fn raft_vote(
    app_data: web::Data<RaftAppData>,
    req: Json<VoteRequest<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>>,
) -> impl Responder {
    match app_data.app.raft.vote(req.0).await {
        Ok(res) => HttpResponse::Ok().json(res),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[post("/raft/append")]
pub async fn raft_append(
    app_data: web::Data<RaftAppData>,
    req: Json<AppendEntriesRequest<KiwiTypeConfig>>,
) -> impl Responder {
    match app_data.app.raft.append_entries(req.0).await {
        Ok(res) => HttpResponse::Ok().json(res),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
