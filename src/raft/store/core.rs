use std::fs;
use std::path::PathBuf;

use openraft::{BasicNode, Entry, LogId, SnapshotMeta, StoredMembership, Vote};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::storage::StorageError;
use crate::types::Timestamp;

use crate::raft::RivetRaftConfig;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[derive(Clone)]
struct PersistenceLayer {
    state_path: PathBuf,
}

impl PersistenceLayer {
    fn new(dir: PathBuf) -> Result<Self, PersistenceError> {
        fs::create_dir_all(&dir)?;
        Ok(Self {
            state_path: dir.join("raft_state.json"),
        })
    }

    fn load(&self) -> Result<Option<RivetStorageCoreData>, PersistenceError> {
        match fs::read(&self.state_path) {
            Ok(bytes) => {
                let data = serde_json::from_slice(&bytes)?;
                Ok(Some(data))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(PersistenceError::Io(err)),
        }
    }

    fn persist(&self, data: &RivetStorageCoreData) -> Result<(), PersistenceError> {
        let serialized = serde_json::to_vec(data)?;
        let tmp_path = self.state_path.with_extension("tmp");
        fs::write(&tmp_path, serialized)?;
        fs::rename(tmp_path, &self.state_path)?;
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct RivetStorageCore {
    pub(crate) data: RivetStorageCoreData,
    persistence: Option<PersistenceLayer>,
}

impl RivetStorageCore {
    pub(crate) fn load(data_dir: Option<PathBuf>) -> Result<Self, PersistenceError> {
        if let Some(dir) = data_dir {
            let layer = PersistenceLayer::new(dir)?;
            let data = layer.load()?.unwrap_or_default();
            Ok(Self {
                data,
                persistence: Some(layer),
            })
        } else {
            Ok(Self::default())
        }
    }

    pub(crate) fn persist(&self) -> Result<(), PersistenceError> {
        if let Some(layer) = &self.persistence {
            layer.persist(&self.data)?;
        }
        Ok(())
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct RivetStorageCoreData {
    pub(crate) vote: Option<Vote<u64>>,
    pub(crate) committed: Option<LogId<u64>>,
    pub(crate) log: Vec<Entry<RivetRaftConfig>>,
    pub(crate) last_purged: Option<LogId<u64>>,
    pub(crate) state: RivetStateData,
    pub(crate) snapshot: Option<StoredSnapshot>,
    pub(crate) snapshot_seq: u64,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct RivetStateData {
    pub(crate) last_applied: Option<LogId<u64>>,
    pub(crate) last_membership: StoredMembership<u64, BasicNode>,
    pub(crate) applied_commands: Vec<Vec<u8>>,
    pub(crate) last_commit_ts: Timestamp,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct StoredSnapshot {
    pub(crate) meta: SnapshotMeta<u64, BasicNode>,
    pub(crate) data: Vec<u8>,
}
