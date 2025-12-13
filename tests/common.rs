use std::path::PathBuf;
use std::sync::Arc;

use rivetdb::storage::StorageAdapter;
use rivetdb::StorageConfig;
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Memory,
    Disk,
}

pub fn all_backends() -> [BackendKind; 2] {
    [BackendKind::Memory, BackendKind::Disk]
}

/// Holds storage state and keeps tempdirs alive for disk-backed runs.
#[allow(dead_code)]
pub struct TestStorage {
    backend: BackendKind,
    storage_path: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    _guard: Option<TempDir>,
}

#[allow(dead_code)]
impl TestStorage {
    pub fn new(backend: BackendKind) -> Self {
        match backend {
            BackendKind::Memory => Self {
                backend,
                storage_path: None,
                data_dir: None,
                _guard: None,
            },
            BackendKind::Disk => {
                let dir = TempDir::new().expect("create temp dir for disk storage");
                let storage_path = dir.path().join("storage");
                let data_dir = dir.path().join("raft");
                Self {
                    backend,
                    storage_path: Some(storage_path),
                    data_dir: Some(data_dir),
                    _guard: Some(dir),
                }
            }
        }
    }

    pub fn backend(&self) -> BackendKind {
        self.backend
    }

    pub fn storage(&self) -> Arc<StorageAdapter> {
        match self.backend {
            BackendKind::Memory => Arc::new(StorageAdapter::memory()),
            BackendKind::Disk => Arc::new(
                StorageAdapter::disk(self.storage_path.as_ref().expect("storage path"))
                    .expect("open disk storage"),
            ),
        }
    }

    pub fn storage_config(&self) -> StorageConfig {
        match self.backend {
            BackendKind::Memory => StorageConfig::memory(),
            BackendKind::Disk => StorageConfig::disk(self.storage_path.as_ref().expect("path")),
        }
    }

    pub fn data_dir(&self) -> Option<PathBuf> {
        self.data_dir.clone()
    }

    /// Reopen storage using the same path (used for restart/persistence tests).
    pub fn reopen(&self) -> Arc<StorageAdapter> {
        self.storage()
    }
}
