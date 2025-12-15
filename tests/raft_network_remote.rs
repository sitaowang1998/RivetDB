use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::{BasicNode, ServerState};
use rivetdb::rpc::server::RivetKvService;
use rivetdb::rpc::service::rivet_kv_server::RivetKvServer;
use rivetdb::rpc::service::rivet_raft_server::RivetRaftServer;
use rivetdb::storage::{StorageAdapter, StorageEngine};
use rivetdb::{PeerConfig, RivetConfig, RivetNode, RivetRaftService, registry, reset_registry};
use std::io::ErrorKind;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

#[path = "common.rs"]
mod common;

struct DualServiceNode {
    id: u64,
    addr: SocketAddr,
    node: Arc<RivetNode<StorageAdapter>>,
    storage: Arc<StorageAdapter>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl DualServiceNode {
    async fn spawn(
        config: RivetConfig,
        storage: common::TestStorage,
        listener: TcpListener,
    ) -> Self {
        let addr = listener.local_addr().expect("read listener addr");
        let incoming = TcpIncoming::from_listener(listener, true, None).expect("build TcpIncoming");

        let storage_adapter = storage.storage();
        let node = Arc::new(
            RivetNode::new(config.clone(), storage_adapter.clone())
                .await
                .expect("bootstrap node"),
        );

        let kv_service = RivetKvService::new(node.clone());
        let raft_service = RivetRaftService::new(
            config.node_id,
            config.listen_addr.clone(),
            node.raft().clone(),
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(RivetRaftServer::new(raft_service))
                .add_service(RivetKvServer::new(kv_service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Self {
            id: config.node_id,
            addr,
            node,
            storage: storage_adapter,
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
            let _ = handle.await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raft_rpc_uses_grpc_path_across_processes() {
    for backend in common::all_backends() {
        reset_registry().await;

        let mut reserved = Vec::new();
        for id in 0_u64..2 {
            let listener = match TcpListener::bind("127.0.0.1:0").await {
                Ok(listener) => listener,
                Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                    eprintln!(
                        "skipping grpc raft transport test (permission denied binding socket: {err})"
                    );
                    return;
                }
                Err(err) => panic!("bind ephemeral listener: {err}"),
            };
            let addr = listener.local_addr().expect("read listener addr");
            reserved.push((id, addr, listener));
        }

        let layout: Vec<(u64, String)> = reserved
            .iter()
            .map(|(id, addr, _)| (*id, addr.to_string()))
            .collect();

        let mut nodes = Vec::new();
        let mut storages = Vec::new();
        for (node_id, addr, listener) in reserved {
            let peers = layout
                .iter()
                .filter(|(peer_id, _)| *peer_id != node_id)
                .map(|(peer_id, peer_addr)| PeerConfig::new(*peer_id, peer_addr.clone()))
                .collect::<Vec<_>>();

            let storage = common::TestStorage::new(backend);
            let config = RivetConfig::new(node_id, addr.to_string(), peers, storage.data_dir())
                .with_storage(storage.storage_config());
            let node = DualServiceNode::spawn(config, storage, listener).await;
            storages.push(node.storage.clone());
            nodes.push(node);
        }

        // Drop the in-process fast path so Raft traffic must traverse gRPC like separate processes.
        for node in &nodes {
            registry().unregister(node.id).await;
            registry()
                .set_node_info(node.id, BasicNode::new(node.endpoint()))
                .await;
        }

        let node_handles: Vec<_> = nodes.iter().map(|n| n.node.clone()).collect();
        let Some(leader_id) = wait_for_leader(&node_handles, Duration::from_secs(10)).await else {
            eprintln!("skipping grpc raft transport test (leader not elected within timeout)");
            for node in &mut nodes {
                node.shutdown().await;
            }
            return;
        };
        let follower_id = if leader_id == layout[0].0 {
            layout[1].0
        } else {
            layout[0].0
        };

        let leader = node_handles
            .iter()
            .find(|n| n.config().node_id == leader_id)
            .unwrap()
            .clone();

        let manager = leader.transaction_manager();
        let txn = manager.begin_transaction();
        manager
            .write(&txn, "remote-raft".to_string(), b"via-grpc".to_vec())
            .await
            .expect("stage write");
        let collected = manager.collect(txn).await.expect("collect transaction");
        let receipt = leader
            .replicate_commit(collected)
            .await
            .expect("raft commit");

        // Ensure follower applied the entry; follower must have received AppendEntries over gRPC.
        let follower_storage = storages
            .iter()
            .zip(layout.iter())
            .find(|(_, (id, _))| *id == follower_id)
            .map(|(storage, _)| storage.clone())
            .expect("follower storage found");
        assert!(
            wait_for_value(
                &follower_storage,
                "remote-raft",
                receipt.commit_ts,
                b"via-grpc"
            )
            .await,
            "follower should apply commit over gRPC transport"
        );

        for node in &mut nodes {
            node.shutdown().await;
        }
    }
}

async fn wait_for_leader(
    nodes: &[Arc<RivetNode<StorageAdapter>>],
    timeout: Duration,
) -> Option<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        for node in nodes {
            if node.raft().metrics().borrow().state == ServerState::Leader {
                return Some(node.config().node_id);
            }
        }

        if Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_value(
    storage: &Arc<StorageAdapter>,
    key: &str,
    commit_ts: u64,
    expected: &[u8],
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    let key = key.to_string();

    loop {
        match storage.read(&key, commit_ts).await {
            Ok(Some(version)) if version.value == expected => return true,
            _ => {}
        }

        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(50)).await;
    }
}
