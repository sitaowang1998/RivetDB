use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::storage::StorageEngine;
use crate::types::Timestamp;

use super::{RaftCommand, decode_command};

mod core;
mod log;
mod state_machine;

pub use core::PersistenceError;
pub use log::{RivetLogReader, RivetLogStore};
pub use state_machine::{RivetSnapshotBuilder, RivetStateMachine};

use core::RivetStorageCore;

pub struct RivetStore;

impl RivetStore {
    pub async fn handles<S: StorageEngine>(
        storage: Arc<S>,
        data_dir: Option<PathBuf>,
    ) -> Result<(RivetLogStore, RivetStateMachine<S>, Timestamp), PersistenceError> {
        let core = RivetStorageCore::load(data_dir)?;
        let applied = core.data.state.applied_commands.clone();
        let initial_ts = core.data.state.last_commit_ts;

        if !applied.is_empty() {
            replay_applied_commands(storage.clone(), &applied).await?;
        }

        let core = Arc::new(RwLock::new(core));
        Ok((
            RivetLogStore { core: core.clone() },
            RivetStateMachine { core, storage },
            initial_ts,
        ))
    }
}

async fn replay_applied_commands<S: StorageEngine>(
    storage: Arc<S>,
    payloads: &[Vec<u8>],
) -> Result<(), PersistenceError> {
    for payload in payloads {
        match decode_command(payload)? {
            RaftCommand::ApplyTransaction(txn) => {
                storage
                    .apply_committed(txn.commit_ts, txn.writes)
                    .await
                    .map_err(PersistenceError::from)?;
            }
        }
    }
    Ok(())
}
