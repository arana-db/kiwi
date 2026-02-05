use conf::raft_type::{KiwiNode, KiwiTypeConfig};
use openraft::error::{NetworkError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::io;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::raft_proto::raft_service_client::RaftServiceClient;
use tonic::transport::Channel;


// 类型别名，简化 RaftNetwork 的返回类型（参考 openraft 示例）
type NodeId = <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId;
type Node = KiwiNode;
type RPCErr = openraft::error::RPCError<NodeId, Node, RaftError<NodeId>>;
type RPCErrSnapshot = openraft::error::RPCError<
    NodeId,
    Node,
    RaftError<NodeId, openraft::error::InstallSnapshotError>,
>;

pub struct KiwiNetworkFactory {
    // Connection pool: NodeId -> RaftServiceClient<Channel>(gRPC Client)
    clients: Arc<RwLock<HashMap<NodeId, RaftServiceClient<Channel>>>>,
    // NodeId -> raft address
    node_addrs: Arc<RwLock<HashMap<NodeId, String>>>,
    // The current node id
    node_id: NodeId,
}

impl KiwiNetworkFactory {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
            node_id,
        }
    }
}

impl RaftNetworkFactory<KiwiTypeConfig> for KiwiNetworkFactory {
    type Network = KiwiNetwork;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        // Get or create gRPC client for the target node
        let addr = node.raft_addr.clone();

        let mut clients_guard = self.clients.write().await;
        let client = if let Some(client) = clients_guard.get(&target) {
            client.clone()
        } else {
            let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
                .unwrap()
                .connect_lazy();
            let client = RaftServiceClient::new(endpoint);
            clients_guard.insert(target, client.clone());
            client
        };
        KiwiNetwork {
            target_id: target,
            client,
            target_addr: addr,
        }
    }
}

pub struct KiwiNetwork {
    target_id: u64,
    client: RaftServiceClient<Channel>,
    target_addr: String,
}

// TODO: Change to tonic(gRPC)

// Impl the RaftNetwork trait for KiwiNetwork according to openraft requirements
impl RaftNetwork<KiwiTypeConfig> for KiwiNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<KiwiTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCErr> {
       Err(RPCErr::Network(NetworkError::new(&io::Error::new(
            io::ErrorKind::Unsupported,
            "Append entries rpc version not implemented",
        ))))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<KiwiTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCErrSnapshot> {
        Err(RPCErrSnapshot::Network(NetworkError::new(&io::Error::new(
            io::ErrorKind::Unsupported,
            "Install snapshot not implemented",
        ))))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCErr> {
       todo!() 
    }
}
