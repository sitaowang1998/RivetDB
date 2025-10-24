use std::sync::Arc;

use crate::config::RivetConfig;
use crate::storage::StorageEngine;
use crate::transaction::TransactionMetadata;

/// Logical role within the Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Leader,
    Follower,
    Learner,
}

/// High-level node abstraction composing storage + consensus + RPC layers.
pub struct RivetNode<S: StorageEngine> {
    config: RivetConfig,
    storage: Arc<S>,
    role: NodeRole,
}

impl<S: StorageEngine> RivetNode<S> {
    pub fn new(config: RivetConfig, storage: Arc<S>) -> Self {
        Self {
            config,
            storage,
            role: NodeRole::Learner,
        }
    }

    pub fn role(&self) -> NodeRole {
        self.role
    }

    pub fn config(&self) -> &RivetConfig {
        &self.config
    }

    /// Placeholder for leader-side commit path wiring storage + Raft.
    pub async fn handle_commit(&self, txn: TransactionMetadata) {
        let commit_ts = txn.snapshot_ts();
        let _ = self.storage.validate(&txn).await;
        let _ = self.storage.commit(&txn, commit_ts).await;
    }
}
