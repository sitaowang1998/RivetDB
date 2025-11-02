use std::net::SocketAddr;
use std::sync::Arc;

use rivetdb::rpc::server::RivetKvService;
use rivetdb::rpc::service::rivet_kv_server::RivetKvServer;
use rivetdb::storage::InMemoryStorage;
use rivetdb::{RivetConfig, RivetNode};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();

    let config = RivetConfig::default();
    info!("starting RivetDB node with config {:?}", config);

    let storage = Arc::new(InMemoryStorage::new());
    let node = Arc::new(RivetNode::new(config, storage));

    info!("node initialized with role {:?}", node.role());

    let addr: SocketAddr = node
        .config()
        .listen_addr
        .parse()
        .expect("configuration listen_addr must be a valid socket address");

    let service = RivetKvService::new(node.clone());
    info!("starting gRPC server on {}", addr);

    Server::builder()
        .add_service(RivetKvServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

fn setup_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
