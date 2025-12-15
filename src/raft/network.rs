use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use openraft::BasicNode;
use openraft::error::{
    Infallible, InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::metrics::RaftMetrics;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::rpc::service::RaftRequest;
use crate::rpc::service::rivet_raft_client::RivetRaftClient;

use super::{RivetRaft, RivetRaftConfig};

/// Shared singleton routing table for in-memory Raft RPC forwarding during tests.
static GLOBAL_REGISTRY: Lazy<Arc<RaftRegistry>> = Lazy::new(|| Arc::new(RaftRegistry::default()));

/// Expose the global registry so node construction can share the same routing table.
pub fn registry() -> Arc<RaftRegistry> {
    GLOBAL_REGISTRY.clone()
}

/// Helper to wipe the registry between integration tests.
pub async fn reset_registry() {
    GLOBAL_REGISTRY.clear().await;
}

/// Representation of a node registered with the in-memory router.
#[derive(Clone)]
pub(crate) struct NodeEntry {
    pub(crate) raft: Option<RivetRaft>,
    pub(crate) node: Option<BasicNode>,
}

impl NodeEntry {
    fn new(raft: Option<RivetRaft>, node: Option<BasicNode>) -> Self {
        Self { raft, node }
    }
}

#[derive(Default)]
pub struct RaftRegistry {
    nodes: RwLock<HashMap<u64, NodeEntry>>,
}

impl RaftRegistry {
    pub async fn register(&self, node_id: u64, raft: RivetRaft, node: BasicNode) {
        self.nodes
            .write()
            .await
            .insert(node_id, NodeEntry::new(Some(raft), Some(node)));
    }

    pub async fn unregister(&self, node_id: u64) {
        self.nodes.write().await.remove(&node_id);
    }

    pub async fn set_node_info(&self, node_id: u64, node: BasicNode) {
        let mut guard = self.nodes.write().await;
        guard
            .entry(node_id)
            .and_modify(|entry| entry.node = Some(node.clone()))
            .or_insert_with(|| NodeEntry::new(None, Some(node)));
    }

    pub(crate) async fn get(&self, node_id: u64) -> Option<NodeEntry> {
        self.nodes.read().await.get(&node_id).cloned()
    }

    pub async fn clear(&self) {
        self.nodes.write().await.clear();
    }
}

/// Network factory handing out client transports backed by the in-memory registry.
#[derive(Clone)]
pub struct RivetNetworkFactory {
    registry: Arc<RaftRegistry>,
}

impl RivetNetworkFactory {
    pub fn new(registry: Arc<RaftRegistry>) -> Self {
        Self { registry }
    }
}

#[derive(Clone)]
pub struct RivetNetwork {
    registry: Arc<RaftRegistry>,
    target: u64,
    endpoint: String,
}

impl RaftNetworkFactory<RivetRaftConfig> for RivetNetworkFactory {
    type Network = RivetNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.registry.set_node_info(target, node.clone()).await;
        RivetNetwork {
            registry: self.registry.clone(),
            target,
            endpoint: normalize_endpoint(&node.addr),
        }
    }
}

impl RaftNetwork<RivetRaftConfig> for RivetNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RivetRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if let Some(entry) = self.registry.get(self.target).await
            && let Some(raft) = entry.raft.clone()
        {
            let node_info = entry.node.clone();
            return match raft.append_entries(rpc).await {
                Ok(resp) => Ok(resp),
                Err(err) => {
                    let remote = match node_info {
                        Some(node) => RemoteError::new_with_node(self.target, node, err),
                        None => RemoteError::new(self.target, err),
                    };
                    Err(RPCError::from(remote))
                }
            };
        }

        self.forward_append_entries(rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<RivetRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        if let Some(entry) = self.registry.get(self.target).await
            && let Some(raft) = entry.raft.clone()
        {
            let node_info = entry.node.clone();
            return match raft.install_snapshot(rpc).await {
                Ok(resp) => Ok(resp),
                Err(err) => {
                    let remote = match node_info {
                        Some(node) => RemoteError::new_with_node(self.target, node, err),
                        None => RemoteError::new(self.target, err),
                    };
                    Err(RPCError::from(remote))
                }
            };
        }

        self.forward_install_snapshot(rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if let Some(entry) = self.registry.get(self.target).await
            && let Some(raft) = entry.raft.clone()
        {
            let node_info = entry.node.clone();
            return match raft.vote(rpc).await {
                Ok(resp) => Ok(resp),
                Err(err) => {
                    let remote = match node_info {
                        Some(node) => RemoteError::new_with_node(self.target, node, err),
                        None => RemoteError::new(self.target, err),
                    };
                    Err(RPCError::from(remote))
                }
            };
        }

        self.forward_vote(rpc).await
    }
}

/// Surface Raft metrics from the registry for debugging.
pub async fn collect_metrics(node_id: u64) -> Option<RaftMetrics<u64, BasicNode>> {
    registry().get(node_id).await.and_then(|entry| {
        entry
            .raft
            .as_ref()
            .map(|raft| raft.metrics().borrow().clone())
    })
}

fn normalize_endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}

fn unreachable_error<E>(
    target: u64,
    reason: impl Into<String>,
) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let message = format!("Raft peer {target}: {}", reason.into());
    let err = std::io::Error::new(std::io::ErrorKind::NotFound, message);
    RPCError::Unreachable(Unreachable::new(&err))
}

fn serialization_error<E>(
    target: u64,
    context: &str,
    err: impl std::fmt::Display,
) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let msg = format!("{context}: {err}");
    unreachable_error(target, msg)
}

fn status_error<E>(
    target: u64,
    context: &str,
    status: tonic::Status,
) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let msg = format!("{context}: {status}");
    unreachable_error(target, msg)
}

impl RivetNetwork {
    async fn raft_client<E>(
        &self,
    ) -> Result<RivetRaftClient<Channel>, RPCError<u64, BasicNode, RaftError<u64, E>>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        RivetRaftClient::connect(self.endpoint.clone())
            .await
            .map_err(|err| {
                unreachable_error::<E>(
                    self.target,
                    format!("connect to peer {}: {err}", self.target),
                )
            })
    }

    async fn forward_append_entries(
        &self,
        rpc: AppendEntriesRequest<RivetRaftConfig>,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = serde_json::to_vec(&rpc).map_err(|err| {
            serialization_error::<Infallible>(self.target, "encode append_entries request", err)
        })?;
        let mut client = self.raft_client::<Infallible>().await?;
        let response = client
            .append_entries(RaftRequest { data: payload })
            .await
            .map_err(|status| {
                status_error::<Infallible>(self.target, "append_entries RPC failed", status)
            })?;

        let envelope = response.into_inner();
        let result: Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> =
            serde_json::from_slice(&envelope.data).map_err(|err| {
                serialization_error::<Infallible>(
                    self.target,
                    "decode append_entries response",
                    err,
                )
            })?;
        result
    }

    async fn forward_install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<RivetRaftConfig>,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let payload = serde_json::to_vec(&rpc).map_err(|err| {
            serialization_error::<InstallSnapshotError>(
                self.target,
                "encode install_snapshot request",
                err,
            )
        })?;
        let mut client = self.raft_client::<InstallSnapshotError>().await?;
        let response = client
            .install_snapshot(RaftRequest { data: payload })
            .await
            .map_err(|status| {
                status_error::<InstallSnapshotError>(
                    self.target,
                    "install_snapshot RPC failed",
                    status,
                )
            })?;

        let envelope = response.into_inner();
        let result: Result<
            InstallSnapshotResponse<u64>,
            RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
        > = serde_json::from_slice(&envelope.data).map_err(|err| {
            serialization_error::<InstallSnapshotError>(
                self.target,
                "decode install_snapshot response",
                err,
            )
        })?;
        result
    }

    async fn forward_vote(
        &self,
        rpc: VoteRequest<u64>,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = serde_json::to_vec(&rpc).map_err(|err| {
            serialization_error::<Infallible>(self.target, "encode vote request", err)
        })?;
        let mut client = self.raft_client::<Infallible>().await?;
        let response = client
            .vote(RaftRequest { data: payload })
            .await
            .map_err(|status| status_error::<Infallible>(self.target, "vote RPC failed", status))?;

        let envelope = response.into_inner();
        let result: Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> =
            serde_json::from_slice(&envelope.data).map_err(|err| {
                serialization_error::<Infallible>(self.target, "decode vote response", err)
            })?;
        result
    }
}
