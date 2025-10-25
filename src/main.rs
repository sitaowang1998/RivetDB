use std::sync::Arc;

use tracing::info;
use tracing_subscriber::EnvFilter;

use rivetdb::storage::StorageEngine;
use rivetdb::{RivetConfig, RivetNode};

#[tokio::main]
async fn main() {
    setup_tracing();

    let config = RivetConfig::default();
    info!("starting RivetDB node with config {:?}", config);

    // Placeholder storage implementation keeps the node wiring compiling while
    // a real engine is under development.
    let storage = Arc::new(NoopStorage {});
    let node = RivetNode::new(config, storage);

    info!("node initialized with role {:?}", node.role());
}

fn setup_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

struct NoopStorage;

#[async_trait::async_trait]
impl StorageEngine for NoopStorage {
    async fn read(
        &self,
        _key: &rivetdb::Key,
        _snapshot_ts: rivetdb::Timestamp,
    ) -> Result<Option<rivetdb::storage::VersionedValue>, rivetdb::storage::StorageError> {
        Ok(None)
    }

    async fn stage_write(
        &self,
        _txn: &rivetdb::TransactionMetadata,
        _intent: rivetdb::transaction::WriteIntent,
    ) -> Result<(), rivetdb::storage::StorageError> {
        Err(rivetdb::storage::StorageError::Unimplemented)
    }

    async fn validate(
        &self,
        _txn: &rivetdb::TransactionMetadata,
    ) -> Result<(), rivetdb::storage::StorageError> {
        Err(rivetdb::storage::StorageError::Unimplemented)
    }

    async fn commit(
        &self,
        _txn: &rivetdb::TransactionMetadata,
        _commit_ts: rivetdb::Timestamp,
    ) -> Result<(), rivetdb::storage::StorageError> {
        Err(rivetdb::storage::StorageError::Unimplemented)
    }

    async fn abort(&self, _txn_id: &rivetdb::TxnId) {}
}
