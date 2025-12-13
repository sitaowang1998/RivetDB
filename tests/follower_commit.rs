use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[path = "common.rs"]
mod common;

use rivetdb::rpc::server::RivetKvService;
use rivetdb::rpc::service::rivet_kv_server::RivetKvServer;
use rivetdb::{
    ClientConfig, PeerConfig, RivetClient, RivetConfig, RivetNode, collect_metrics, reset_registry,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Error as TransportError, Server};

struct ClusterNode {
    id: u64,
    addr: SocketAddr,
    _storage: common::TestStorage,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<Result<(), TransportError>>>,
}

impl ClusterNode {
    async fn spawn(config: RivetConfig, storage: common::TestStorage) -> Self {
        let addr: SocketAddr = config
            .listen_addr
            .parse()
            .expect("config.listen_addr must be socket address");
        let listener = TcpListener::bind(addr)
            .await
            .expect("failed to bind cluster node listener");
        let addr = listener.local_addr().expect("read listener addr");
        let incoming = TcpIncoming::from_listener(listener, true, None).expect("build TcpIncoming");

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let node_config = config.clone();
        let node = Arc::new(
            RivetNode::new(node_config, storage.storage())
                .await
                .expect("node bootstrap"),
        );
        let service = RivetKvService::new(node);

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(RivetKvServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        Self {
            id: config.node_id,
            addr,
            _storage: storage,
            shutdown: Some(shutdown_tx),
            handle: Some(handle),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => panic!("server exited with error: {err}"),
                Err(err) => panic!("server task panicked: {err}"),
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_commit_is_forwarded() {
    for backend in common::all_backends() {
        reset_registry().await;

        let layout = vec![
            (0_u64, "127.0.0.1:6600".to_string()),
            (1_u64, "127.0.0.1:6601".to_string()),
            (2_u64, "127.0.0.1:6602".to_string()),
        ];

        let mut nodes = Vec::new();
        for (node_id, addr) in &layout {
            let peers = layout
                .iter()
                .filter(|(peer_id, _)| peer_id != node_id)
                .map(|(peer_id, peer_addr)| PeerConfig::new(*peer_id, peer_addr.clone()))
                .collect::<Vec<_>>();

            let storage = common::TestStorage::new(backend);
            let config = RivetConfig::new(*node_id, addr.clone(), peers, storage.data_dir())
                .with_storage(storage.storage_config());
            let node = ClusterNode::spawn(config, storage).await;
            nodes.push(node);
        }

        let ids: Vec<u64> = layout.iter().map(|(id, _)| *id).collect();
        let leader = wait_for_unique_leader(&ids)
            .await
            .expect("cluster elects leader");
        let follower_id = ids
            .into_iter()
            .find(|&id| id != leader)
            .expect("requires follower");

        let follower_endpoint = nodes
            .iter()
            .find(|node| node.id == follower_id)
            .map(|node| node.endpoint())
            .expect("follower endpoint");
        let leader_endpoint = nodes
            .iter()
            .find(|node| node.id == leader)
            .map(|node| node.endpoint())
            .expect("leader endpoint");

        let client = RivetClient::connect(ClientConfig::new(follower_endpoint.clone()))
            .await
            .expect("client connect");
        let txn = client
            .begin_transaction("follower-client")
            .await
            .expect("begin follower txn");
        txn.put("shipped", b"value".to_vec())
            .await
            .expect("put via follower");
        let receipt = txn.commit().await.expect("commit via follower");

        let leader_client = RivetClient::connect(ClientConfig::new(leader_endpoint))
            .await
            .expect("leader client connect");
        let reader = leader_client
            .begin_transaction("reader")
            .await
            .expect("begin reader");
        let result = reader
            .get("shipped")
            .await
            .expect("leader read")
            .expect("value present");
        assert_eq!(result.value, b"value");
        assert_eq!(result.commit_ts, receipt.commit_ts);
        reader.abort().await.expect("abort reader");

        for node in nodes.iter_mut() {
            node.shutdown().await;
        }
    }
}

async fn wait_for_unique_leader(node_ids: &[u64]) -> Option<u64> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut leaders = Vec::new();
        for &node_id in node_ids {
            if let Some(metrics) = collect_metrics(node_id).await
                && metrics.state == openraft::ServerState::Leader
            {
                leaders.push(node_id);
            }
        }

        if leaders.len() == 1 {
            return leaders.pop();
        }

        if Instant::now() >= deadline {
            return None;
        }

        sleep(Duration::from_millis(100)).await;
    }
}
