use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::info;

use crate::storage::StorageEngine;
use crate::transaction::CommitClock;

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
    ) -> Result<(RivetLogStore, RivetStateMachine<S>, CommitClock), PersistenceError> {
        let dir_label = data_dir
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<memory>".into());
        let core = RivetStorageCore::load(data_dir)?;
        let applied = core.data.state.applied_commands.clone();
        let initial_ts = core.data.state.last_commit_ts;
        let clock = CommitClock::new(initial_ts);

        let replayed = if applied.is_empty() {
            0
        } else {
            replay_applied_commands(storage.clone(), &applied, &clock).await?
        };
        info!(
            path = %dir_label,
            replayed_entries = replayed,
            recovered_commit_ts = initial_ts,
            "recovered persisted Raft state"
        );

        let core = Arc::new(RwLock::new(core));
        Ok((
            RivetLogStore { core: core.clone() },
            RivetStateMachine {
                core,
                storage,
                clock: clock.clone(),
            },
            clock,
        ))
    }
}

async fn replay_applied_commands<S: StorageEngine>(
    storage: Arc<S>,
    payloads: &[Vec<u8>],
    clock: &CommitClock,
) -> Result<usize, PersistenceError> {
    let mut applied = 0;
    for payload in payloads {
        match decode_command(payload)? {
            RaftCommand::ApplyTransaction(txn) => {
                storage
                    .apply_committed(txn.commit_ts, txn.writes)
                    .await
                    .map_err(PersistenceError::from)?;
                clock.advance(txn.commit_ts);
                applied += 1;
            }
        }
    }
    Ok(applied)
}
