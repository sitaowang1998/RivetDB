use std::sync::Arc;

use rivetdb::storage::InMemoryStorage;
use rivetdb::{RivetConfig, RivetNode};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    setup_tracing();

    let config = RivetConfig::default();
    info!("starting RivetDB node with config {:?}", config);

    let storage = Arc::new(InMemoryStorage::new());
    let node = RivetNode::new(config, storage);

    info!("node initialized with role {:?}", node.role());
}

fn setup_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
