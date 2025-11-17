use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use rivetdb::rpc::server::RivetKvService;
use rivetdb::rpc::service::rivet_kv_server::RivetKvServer;
use rivetdb::storage::InMemoryStorage;
use rivetdb::{PeerConfig, RivetConfig, RivetNode};
use std::path::PathBuf;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();

    let args = Cli::parse();
    let config = args.into_config()?;
    info!("starting RivetDB node with config {:?}", config);

    let storage = Arc::new(InMemoryStorage::new());
    let node = Arc::new(RivetNode::new(config, storage).await?);

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

#[derive(Parser, Debug)]
#[command(name = "rivetdb", version, about = "Run a RivetDB node")]
struct Cli {
    /// Unique identifier for this node in the cluster.
    #[arg(long, default_value_t = 0)]
    node_id: u64,

    /// Address the gRPC server binds to (e.g. 127.0.0.1:50051).
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen_addr: String,

    /// Optional data directory for persisting Raft state.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Peer definitions in the form `<id>=<addr>`, e.g. `1=127.0.0.1:6001`.
    #[arg(long = "peer")]
    peers: Vec<String>,
}

impl Cli {
    fn into_config(self) -> Result<RivetConfig, String> {
        let peers = self
            .peers
            .iter()
            .map(|peer| parse_peer(peer))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RivetConfig::new(
            self.node_id,
            self.listen_addr,
            peers,
            self.data_dir,
        ))
    }
}

fn parse_peer(input: &str) -> Result<PeerConfig, String> {
    let (id_part, addr_part) = input
        .split_once('=')
        .ok_or_else(|| format!("peer '{input}' must be formatted as <id>=<addr>"))?;
    let node_id = id_part
        .parse::<u64>()
        .map_err(|err| format!("invalid peer id '{id_part}': {err}"))?;
    Ok(PeerConfig::new(node_id, addr_part.to_string()))
}
