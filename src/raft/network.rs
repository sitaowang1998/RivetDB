use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use openraft::BasicNode;
use openraft::error::{
    Infallible, InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::metrics::RaftMetrics;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};

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
}

impl RaftNetworkFactory<RivetRaftConfig> for RivetNetworkFactory {
    type Network = RivetNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.registry.set_node_info(target, node.clone()).await;
        RivetNetwork {
            registry: self.registry.clone(),
            target,
        }
    }
}

impl RaftNetwork<RivetRaftConfig> for RivetNetwork {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<RivetRaftConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        let entry = self
            .registry
            .get(self.target)
            .await
            .ok_or_else(|| unreachable_error::<Infallible>(self.target))?;
        let node_info = entry.node.clone();
        let raft = entry
            .raft
            .clone()
            .ok_or_else(|| unreachable_error::<Infallible>(self.target))?;

        match raft.append_entries(rpc).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                let remote = match node_info {
                    Some(node) => RemoteError::new_with_node(self.target, node, err),
                    None => RemoteError::new(self.target, err),
                };
                Err(RPCError::from(remote))
            }
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<RivetRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let entry = self
            .registry
            .get(self.target)
            .await
            .ok_or_else(|| unreachable_error::<InstallSnapshotError>(self.target))?;
        let node_info = entry.node.clone();
        let raft = entry
            .raft
            .clone()
            .ok_or_else(|| unreachable_error::<InstallSnapshotError>(self.target))?;

        match raft.install_snapshot(rpc).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                let remote = match node_info {
                    Some(node) => RemoteError::new_with_node(self.target, node, err),
                    None => RemoteError::new(self.target, err),
                };
                Err(RPCError::from(remote))
            }
        }
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let entry = self
            .registry
            .get(self.target)
            .await
            .ok_or_else(|| unreachable_error::<Infallible>(self.target))?;
        let node_info = entry.node.clone();
        let raft = entry
            .raft
            .clone()
            .ok_or_else(|| unreachable_error::<Infallible>(self.target))?;

        match raft.vote(rpc).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                let remote = match node_info {
                    Some(node) => RemoteError::new_with_node(self.target, node, err),
                    None => RemoteError::new(self.target, err),
                };
                Err(RPCError::from(remote))
            }
        }
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

fn unreachable_error<E>(target: u64) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let err = std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("Raft peer {target} is not registered"),
    );
    RPCError::Unreachable(Unreachable::new(&err))
}
