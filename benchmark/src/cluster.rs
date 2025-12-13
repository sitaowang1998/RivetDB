use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use openraft::ServerState;
use rivetdb::{
    PeerConfig, RivetConfig, RivetNode, StorageConfig, collect_metrics, registry, reset_registry,
    rpc::server::RivetKvService, rpc::service::rivet_kv_server::RivetKvServer,
    storage::OnDiskStorage,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tracing::info;

pub struct BenchmarkCluster {
    servers: Vec<NodeServer>,
}

impl BenchmarkCluster {
    pub async fn start(node_count: usize) -> Result<Self> {
        reset_registry().await;

        let mut reserved = Vec::new();
        for idx in 0..node_count {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .context("bind cluster listener")?;
            let addr = listener.local_addr().context("read listener address")?;
            reserved.push((idx as u64, addr, listener));
        }

        let layout: Vec<(u64, SocketAddr)> =
            reserved.iter().map(|(id, addr, _)| (*id, *addr)).collect();

        let mut servers = Vec::new();
        for (node_id, addr, listener) in reserved {
            let peers = layout
                .iter()
                .filter(|(peer_id, _)| peer_id != &node_id)
                .map(|(peer_id, peer_addr)| PeerConfig::new(*peer_id, peer_addr.to_string()))
                .collect::<Vec<_>>();

            servers.push(
                NodeServer::start(node_id, addr, listener, peers)
                    .await
                    .with_context(|| format!("start node {node_id}"))?,
            );
        }

        let cluster = Self { servers };
        cluster.wait_for_leader(Duration::from_secs(5)).await?;
        Ok(cluster)
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<u64> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(id) = self.leader_id().await? {
                return Ok(id);
            }

            if Instant::now() >= deadline {
                return Err(anyhow!("leader not elected within {:?}", timeout));
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn leader_id(&self) -> Result<Option<u64>> {
        for server in &self.servers {
            if !server.is_running() {
                continue;
            }

            if let Some(metrics) = collect_metrics(server.node_id).await {
                if metrics.state == ServerState::Leader {
                    return Ok(Some(server.node_id));
                }
            }
        }

        Ok(None)
    }

    pub async fn leader_endpoint(&self) -> Option<String> {
        match self.leader_id().await {
            Ok(Some(id)) => self
                .servers
                .iter()
                .find(|srv| srv.node_id == id)
                .map(|srv| srv.http_endpoint()),
            _ => None,
        }
    }

    pub fn endpoints(&self) -> Vec<String> {
        self.servers
            .iter()
            .filter(|srv| srv.is_running())
            .map(|srv| srv.http_endpoint())
            .collect()
    }

    pub async fn kill_node(&mut self, node_id: u64) -> Result<()> {
        if let Some(server) = self.servers.iter_mut().find(|srv| srv.node_id == node_id) {
            server.stop().await?;
            info!(node_id, "stopped node");
            return Ok(());
        }

        Err(anyhow!("node {node_id} not found"))
    }

    pub async fn restart_node(&mut self, node_id: u64) -> Result<()> {
        if let Some(server) = self.servers.iter_mut().find(|srv| srv.node_id == node_id) {
            server.restart().await?;
            info!(node_id, "restarted node");
            return Ok(());
        }

        Err(anyhow!("node {node_id} not found"))
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        for server in &mut self.servers {
            server.stop().await?;
        }
        self.servers.clear();
        Ok(())
    }
}

struct NodeServer {
    node_id: u64,
    addr: SocketAddr,
    config: RivetConfig,
    storage: Arc<OnDiskStorage>,
    node: Option<Arc<RivetNode<OnDiskStorage>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
    data_dir: TempDir,
}

impl NodeServer {
    async fn start(
        node_id: u64,
        addr: SocketAddr,
        listener: TcpListener,
        peers: Vec<PeerConfig>,
    ) -> Result<Self> {
        let data_dir = tempfile::tempdir().context("create data dir")?;
        let storage = Arc::new(OnDiskStorage::open(data_dir.path())?);
        let config = RivetConfig::new(
            node_id,
            addr.to_string(),
            peers,
            Some(data_dir.path().to_path_buf()),
        )
        .with_storage(StorageConfig::disk(data_dir.path()));
        let (node, shutdown_tx, handle) =
            Self::launch(config.clone(), storage.clone(), listener).await?;

        Ok(Self {
            node_id,
            addr,
            config,
            storage,
            node: Some(node),
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
            data_dir,
        })
    }

    fn http_endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn is_running(&self) -> bool {
        self.node.is_some()
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }

        if let Some(node) = self.node.take() {
            registry().unregister(self.node_id).await;
            node.raft().clone().shutdown().await?;
        }

        Ok(())
    }

    async fn restart(&mut self) -> Result<()> {
        if self.is_running() {
            self.stop().await?;
        }

        let listener = TcpListener::bind(self.addr)
            .await
            .with_context(|| format!("bind listener for restart on {}", self.addr))?;
        self.storage = Arc::new(OnDiskStorage::open(self.data_dir.path())?);
        let (node, shutdown_tx, handle) =
            Self::launch(self.config.clone(), self.storage.clone(), listener).await?;

        self.node = Some(node);
        self.shutdown_tx = Some(shutdown_tx);
        self.handle = Some(handle);
        Ok(())
    }

    async fn launch(
        config: RivetConfig,
        storage: Arc<OnDiskStorage>,
        listener: TcpListener,
    ) -> Result<(
        Arc<RivetNode<OnDiskStorage>>,
        oneshot::Sender<()>,
        JoinHandle<()>,
    )> {
        let addr = listener
            .local_addr()
            .context("read listener address for launch")?;
        let incoming =
            TcpIncoming::from_listener(listener, true, None).context("construct TcpIncoming")?;

        let node = Arc::new(RivetNode::new(config, storage).await?);
        let service = RivetKvService::new(node.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(RivetKvServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
            info!(addr = %addr, "gRPC server terminated");
        });

        Ok((node, shutdown_tx, handle))
    }
}
