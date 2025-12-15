use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use openraft::BasicNode;
use openraft::ServerState;
use openraft::error::ClientWriteError;
use thiserror::Error;
use tracing::info;

use crate::config::RivetConfig;
use crate::raft::{
    PersistenceError, RaftCommand, RaftRegistry, ReplicatedTransaction, RivetNetworkFactory,
    RivetRaft, RivetStore, default_raft_config, encode_command, registry as global_registry,
};
use crate::storage::StorageEngine;
use crate::transaction::{CollectedTransaction, CommitReceipt, TransactionManager};

/// Logical role within the Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Leader,
    Follower,
    Learner,
}

/// Errors raised while bootstrapping the Raft core.
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("failed to construct Raft core: {0}")]
    Raft(#[from] openraft::error::Fatal<u64>),
    #[error("failed to initialize Raft membership: {0}")]
    Initialize(
        #[from] openraft::error::RaftError<u64, openraft::error::InitializeError<u64, BasicNode>>,
    ),
    #[error("failed to bootstrap Raft storage: {0}")]
    Storage(#[from] PersistenceError),
}

/// Errors surfaced while replicating a transaction through Raft.
#[derive(Debug, Error)]
pub enum CommitError {
    #[error("failed to encode Raft command: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("validation failed: {0}")]
    Validation(#[from] crate::storage::StorageError),
    #[error("Raft client write failed: {0}")]
    Raft(#[from] Box<openraft::error::RaftError<u64, ClientWriteError<u64, BasicNode>>>),
}

/// High-level node abstraction composing storage + consensus + RPC layers.
pub struct RivetNode<S: StorageEngine> {
    config: RivetConfig,
    txn_manager: TransactionManager<S>,
    raft: RivetRaft,
    registry: Arc<RaftRegistry>,
    endpoints: HashMap<u64, String>,
}

impl<S: StorageEngine + 'static> RivetNode<S> {
    pub async fn new(config: RivetConfig, storage: Arc<S>) -> Result<Self, NodeError> {
        let registry = global_registry();
        let raft_cfg = default_raft_config();

        let (log_store, state_machine, clock) =
            RivetStore::handles(storage.clone(), config.data_dir.clone()).await?;
        info!(
            node_id = config.node_id,
            recovered_ts = clock.load(),
            "storage recovered for node"
        );
        let network = RivetNetworkFactory::new(registry.clone());

        let raft = openraft::Raft::new(config.node_id, raft_cfg, network, log_store, state_machine)
            .await?;

        registry
            .register(
                config.node_id,
                raft.clone(),
                BasicNode::new(config.listen_addr.clone()),
            )
            .await;
        for peer in &config.raft_peers {
            registry
                .set_node_info(peer.node_id, BasicNode::new(peer.listen_addr.clone()))
                .await;
        }

        ensure_initial_membership(&raft, &config).await?;

        let txn_manager = TransactionManager::new(storage.clone(), clock.clone());

        let mut endpoints = HashMap::new();
        endpoints.insert(config.node_id, config.listen_addr.clone());
        for peer in &config.raft_peers {
            endpoints.insert(peer.node_id, peer.listen_addr.clone());
        }

        Ok(Self {
            config,
            txn_manager,
            raft,
            registry,
            endpoints,
        })
    }

    pub fn role(&self) -> NodeRole {
        match self.raft.metrics().borrow().state {
            ServerState::Leader => NodeRole::Leader,
            ServerState::Follower | ServerState::Candidate => NodeRole::Follower,
            ServerState::Learner | ServerState::Shutdown => NodeRole::Learner,
        }
    }

    pub fn config(&self) -> &RivetConfig {
        &self.config
    }

    pub fn transaction_manager(&self) -> &TransactionManager<S> {
        &self.txn_manager
    }

    pub fn raft(&self) -> &RivetRaft {
        &self.raft
    }

    pub async fn replicate_commit(
        &self,
        collected: CollectedTransaction,
    ) -> Result<CommitReceipt, CommitError> {
        self.txn_manager
            .validate_metadata(&collected.metadata)
            .await?;
        let commit_ts = self.txn_manager.next_ts();
        let writes = collected.writes;
        let command = RaftCommand::ApplyTransaction(ReplicatedTransaction {
            txn_id: collected.metadata.id().clone(),
            commit_ts,
            writes,
        });
        let payload = encode_command(&command)?;
        self.raft
            .client_write(payload)
            .await
            .map_err(|err| CommitError::Raft(Box::new(err)))?;
        Ok(CommitReceipt { commit_ts })
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    pub fn endpoint_for(&self, node_id: u64) -> Option<String> {
        self.endpoints.get(&node_id).cloned()
    }

    pub fn leader_endpoint(&self) -> Option<String> {
        self.leader_id().and_then(|id| self.endpoint_for(id))
    }

    pub fn leader_endpoint_url(&self) -> Option<String> {
        self.leader_endpoint().map(|addr| {
            if addr.starts_with("http://") || addr.starts_with("https://") {
                addr
            } else {
                format!("http://{}", addr)
            }
        })
    }
}

fn membership_nodes(config: &RivetConfig) -> BTreeMap<u64, BasicNode> {
    let mut members = BTreeMap::new();
    members.insert(config.node_id, BasicNode::new(config.listen_addr.clone()));
    for peer in &config.raft_peers {
        if peer.node_id == config.node_id {
            continue;
        }
        members.insert(peer.node_id, BasicNode::new(peer.listen_addr.clone()));
    }
    members
}

async fn ensure_initial_membership(
    raft: &RivetRaft,
    config: &RivetConfig,
) -> Result<(), NodeError> {
    use openraft::error::InitializeError;

    let members = membership_nodes(config);
    if members.is_empty() {
        return Ok(());
    }

    match raft.initialize(members).await {
        Ok(()) => Ok(()),
        Err(err) => {
            if matches!(err.api_error(), Some(InitializeError::NotAllowed(_))) {
                Ok(())
            } else {
                Err(NodeError::Initialize(err))
            }
        }
    }
}

impl<S: StorageEngine> Drop for RivetNode<S> {
    fn drop(&mut self) {
        let registry = self.registry.clone();
        let node_id = self.config.node_id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                registry.unregister(node_id).await;
            });
        } else {
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new()
                    .expect("failed to initialise runtime for node drop");
                runtime.block_on(async move {
                    registry.unregister(node_id).await;
                });
            });
        }
    }
}
