use std::sync::Arc;

use openraft::BasicNode;
use openraft::ServerState;
use thiserror::Error;

use crate::config::RivetConfig;
use crate::raft::{
    RaftRegistry, RivetNetworkFactory, RivetRaft, RivetStore, default_raft_config,
    registry as global_registry,
};
use crate::storage::StorageEngine;
use crate::transaction::{TransactionManager, TransactionMetadata};

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
}

/// High-level node abstraction composing storage + consensus + RPC layers.
pub struct RivetNode<S: StorageEngine> {
    config: RivetConfig,
    storage: Arc<S>,
    txn_manager: TransactionManager<S>,
    raft: RivetRaft,
    registry: Arc<RaftRegistry>,
}

impl<S: StorageEngine> RivetNode<S> {
    pub async fn new(config: RivetConfig, storage: Arc<S>) -> Result<Self, NodeError> {
        let registry = global_registry();
        let raft_cfg = default_raft_config();

        let (log_store, state_machine) = RivetStore::handles();
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

        let txn_manager = TransactionManager::new(storage.clone());

        Ok(Self {
            config,
            storage,
            txn_manager,
            raft,
            registry,
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

    /// Placeholder for leader-side commit path wiring storage + Raft.
    pub async fn handle_commit(&self, txn: TransactionMetadata) {
        let commit_ts = txn.snapshot_ts();
        let _ = self.storage.validate(&txn).await;
        let _ = self.storage.commit(&txn, commit_ts).await;
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
