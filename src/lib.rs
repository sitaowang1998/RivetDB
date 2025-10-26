//! Core crate exports for RivetDB.
//!
//! The modules exposed here define the boundaries between storage, transaction
//! management, Raft coordination, and RPC services. Implementations will be
//! filled in as the project evolves.

pub mod config;
pub mod node;
pub mod rpc;
pub mod storage;
pub mod transaction;
pub mod types;

pub use config::RivetConfig;
pub use node::{NodeRole, RivetNode};
pub use rpc::service::rivet_kv_client::RivetKvClient;
pub use rpc::service::rivet_kv_server::{RivetKv, RivetKvServer};
pub use transaction::{
    CommitReceipt, TransactionContext, TransactionManager, TransactionMetadata, TransactionState,
    WriteIntent,
};
pub use types::{Key, Timestamp, TxnId, Value};
