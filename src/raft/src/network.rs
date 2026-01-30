use conf::raft_type::KiwiTypeConfig;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use reqwest::Client;
use std::io;
use std::sync::Arc;

pub struct KiwiNetworkFactory {
    client: Client,
}

impl KiwiNetworkFactory {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }
}

impl RaftNetworkFactory<KiwiTypeConfig> for KiwiNetworkFactory {
    type Network = KiwiNetwork;

    async fn new_client(
        &mut self,
        _target: <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
        node: &<KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
    ) -> Self::Network {
        KiwiNetwork {
            client: self.client.clone(),
            target_addr: node.raft_addr.clone(),
        }
    }
}

pub struct KiwiNetwork {
    client: Client,
    target_addr: String,
}

impl KiwiNetwork {
    async fn post<Req, Res>(
        &self,
        endpoint: &str,
        req: &Req,
    ) -> Result<
        Res,
        RPCError<
            <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
            <KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
        >,
    >
    where
        Req: serde::Serialize,
        Res: serde::de::DeserializeOwned,
    {
        let url = format!("{}{}", self.target_addr, endpoint);

        let resp = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(RPCError::Network(NetworkError::new(&io::Error::new(
                io::ErrorKind::Other,
                format!("HTTP error {}: {}", status, body),
            ))));
        }

        resp.json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

impl RaftNetwork<KiwiTypeConfig> for KiwiNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<KiwiTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        AppendEntriesResponse<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        RPCError<
            <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
            <KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        self.post("/raft/append", &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<KiwiTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        RPCError<
            <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
            <KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        todo!("Install snapshot not implemented")
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        VoteResponse<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        RPCError<
            <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
            <KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        self.post("/raft/vote", &rpc).await
    }
}
