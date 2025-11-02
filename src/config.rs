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
    pub raft_peers: Vec<String>,
    /// Optional on-disk path for persisting snapshots and logs.
    pub data_dir: Option<PathBuf>,
}

impl RivetConfig {
    /// Construct a configuration with explicit values for all fields.
    pub fn new(
        node_id: u64,
        listen_addr: impl Into<String>,
        raft_peers: Vec<String>,
        data_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            node_id,
            listen_addr: listen_addr.into(),
            raft_peers,
            data_dir,
        }
    }
}

impl Default for RivetConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            listen_addr: "127.0.0.1:50051".into(),
            raft_peers: Vec::new(),
            data_dir: None,
        }
    }
}
