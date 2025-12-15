use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::{Response, Status};

use openraft::BasicNode;
use openraft::error::{RPCError, RaftError, RemoteError};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

use crate::raft::{RivetRaft, RivetRaftConfig};
use crate::rpc::service::rivet_raft_server::RivetRaft as RivetRaftApi;
use crate::rpc::service::{RaftRequest, RaftResponse};

/// Raft RPC surface backed by tonic, used for cross-process replication traffic.
pub struct RivetRaftService {
    node_id: u64,
    node: BasicNode,
    raft: RivetRaft,
}

impl RivetRaftService {
    pub fn new(node_id: u64, listen_addr: impl ToString, raft: RivetRaft) -> Self {
        Self {
            node_id,
            node: BasicNode::new(listen_addr),
            raft,
        }
    }

    #[allow(clippy::result_large_err)]
    fn decode<T: DeserializeOwned>(bytes: Vec<u8>, label: &str) -> Result<T, Status> {
        serde_json::from_slice(&bytes).map_err(|err| {
            Status::invalid_argument(format!("failed to decode {label} payload: {err}"))
        })
    }

    #[allow(clippy::result_large_err)]
    fn encode<T, E>(
        &self,
        result: Result<T, RaftError<u64, E>>,
        label: &str,
    ) -> Result<RaftResponse, Status>
    where
        T: Serialize,
        E: Serialize + std::error::Error + Send + Sync + 'static,
    {
        let wrapped: Result<T, RPCError<u64, BasicNode, RaftError<u64, E>>> = match result {
            Ok(resp) => Ok(resp),
            Err(err) => Err(self.remote_error(err)),
        };

        let data = serde_json::to_vec(&wrapped)
            .map_err(|err| Status::internal(format!("failed to encode {label} response: {err}")))?;
        Ok(RaftResponse { data })
    }

    fn remote_error<E>(&self, err: RaftError<u64, E>) -> RPCError<u64, BasicNode, RaftError<u64, E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        RPCError::from(RemoteError::new_with_node(
            self.node_id,
            self.node.clone(),
            err,
        ))
    }
}

#[tonic::async_trait]
impl RivetRaftApi for RivetRaftService {
    async fn append_entries(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let rpc: AppendEntriesRequest<RivetRaftConfig> =
            Self::decode(request.into_inner().data, "append_entries")?;
        let result = self.raft.append_entries(rpc).await;
        let response = self.encode(result, "append_entries")?;
        Ok(Response::new(response))
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let rpc: InstallSnapshotRequest<RivetRaftConfig> =
            Self::decode(request.into_inner().data, "install_snapshot")?;
        let result = self.raft.install_snapshot(rpc).await;
        let response = self.encode(result, "install_snapshot")?;
        Ok(Response::new(response))
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let rpc: VoteRequest<u64> = Self::decode(request.into_inner().data, "vote")?;
        let result = self.raft.vote(rpc).await;
        let response = self.encode(result, "vote")?;
        Ok(Response::new(response))
    }
}
