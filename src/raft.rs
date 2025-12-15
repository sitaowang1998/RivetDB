use std::io::Cursor;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use openraft::Config;

use crate::transaction::WriteIntent;
use crate::types::{Timestamp, TxnId};

mod network;
mod rpc;
mod store;

pub use network::{
    RaftRegistry, RivetNetwork, RivetNetworkFactory, collect_metrics, registry, reset_registry,
};
pub use rpc::RivetRaftService;
pub use store::{
    PersistenceError, RivetLogReader, RivetLogStore, RivetSnapshotBuilder, RivetStateMachine,
    RivetStore,
};

openraft::declare_raft_types!(
    /// Openraft type configuration used by RivetDB during early integration.
    ///
    /// The payloads remain opaque byte buffers for now.
    pub RivetRaftConfig:
        D = Vec<u8>,
        R = (),
);

/// Convenience alias for the Openraft handle parameterised with [`RivetRaftConfig`].
pub type RivetRaft = openraft::Raft<RivetRaftConfig>;

/// Commands replicated through Raft to update the storage engine.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftCommand {
    ApplyTransaction(ReplicatedTransaction),
}

/// Transaction payload recorded in the Raft log.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicatedTransaction {
    pub txn_id: TxnId,
    pub commit_ts: Timestamp,
    pub writes: Vec<WriteIntent>,
}

pub fn encode_command(command: &RaftCommand) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(command)
}

fn decode_command(bytes: &[u8]) -> Result<RaftCommand, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Build a default Openraft configuration tuned for single-process integration tests.
pub fn default_raft_config() -> Arc<Config> {
    let config = Config {
        enable_tick: true,
        ..Config::default()
    };
    Arc::new(
        config
            .validate()
            .expect("default Raft config should validate"),
    )
}
