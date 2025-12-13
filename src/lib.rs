//! Core crate exports for RivetDB.
//!
//! The modules exposed here define the boundaries between storage, transaction
//! management, Raft coordination, and RPC services. Implementations will be
//! filled in as the project evolves.

pub mod client;
pub mod config;
pub mod node;
pub mod raft;
pub mod rpc;
pub mod storage;
pub mod transaction;
pub mod types;

pub use client::{ClientConfig, ClientError, ClientTransaction, GetResult, RivetClient};
pub use config::{PeerConfig, RivetConfig, StorageBackend, StorageConfig};
pub use node::{NodeError, NodeRole, RivetNode};
pub use raft::{
    RaftRegistry, RivetNetworkFactory, RivetRaft, RivetRaftConfig, RivetStore, collect_metrics,
    default_raft_config, registry, reset_registry,
};
pub use rpc::server::RivetKvService;
pub use rpc::service::rivet_kv_client::RivetKvClient;
pub use rpc::service::rivet_kv_server::{RivetKv, RivetKvServer};
pub use transaction::{
    CollectedTransaction, CommitReceipt, TransactionContext, TransactionManager,
    TransactionMetadata, TransactionState, WriteIntent,
};
pub use types::{Key, Timestamp, TxnId, Value};
