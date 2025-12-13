use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Cluster-wide configuration loaded at startup.
///
/// Establishes the configuration contract that later components (RPC server,
/// storage engine, Raft layer) can rely on. Values will eventually be hydrated
/// from TOML/JSON files or environment variables.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RivetConfig {
    /// Stable identifier for the node within the Raft cluster.
    pub node_id: u64,
    /// Address the RPC server will bind to (e.g. `127.0.0.1:50051`).
    pub listen_addr: String,
    /// Peer Raft endpoints used when establishing the cluster.
    pub raft_peers: Vec<PeerConfig>,
    /// Optional on-disk path for persisting snapshots and logs.
    pub data_dir: Option<PathBuf>,
    /// Storage backend configuration.
    pub storage: StorageConfig,
}

/// Definition of a remote Raft peer supplied through configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerConfig {
    pub node_id: u64,
    pub listen_addr: String,
}

impl PeerConfig {
    pub fn new(node_id: u64, listen_addr: impl Into<String>) -> Self {
        Self {
            node_id,
            listen_addr: listen_addr.into(),
        }
    }
}

impl RivetConfig {
    /// Construct a configuration with explicit values for all fields.
    pub fn new(
        node_id: u64,
        listen_addr: impl Into<String>,
        raft_peers: Vec<PeerConfig>,
        data_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            node_id,
            listen_addr: listen_addr.into(),
            raft_peers,
            data_dir,
            storage: StorageConfig::memory(),
        }
    }

    pub fn with_storage(mut self, storage: StorageConfig) -> Self {
        self.storage = storage;
        self
    }
}

impl Default for RivetConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            listen_addr: "127.0.0.1:50051".into(),
            raft_peers: Vec::new(),
            data_dir: None,
            storage: StorageConfig::memory(),
        }
    }
}

/// Supported storage backends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageBackend {
    Memory,
    Disk,
}

/// Storage configuration specifying the backend and optional path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub path: Option<PathBuf>,
}

impl StorageConfig {
    pub fn memory() -> Self {
        Self {
            backend: StorageBackend::Memory,
            path: None,
        }
    }

    pub fn disk(path: impl Into<PathBuf>) -> Self {
        Self {
            backend: StorageBackend::Disk,
            path: Some(path.into()),
        }
    }
}
