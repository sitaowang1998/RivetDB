use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[path = "common.rs"]
mod common;

use rivetdb::rpc::server::RivetKvService;
use rivetdb::rpc::service::rivet_kv_server::RivetKvServer;
use rivetdb::{ClientConfig, RivetClient, RivetConfig, RivetNode, reset_registry};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Error as TransportError, Server};

struct TestServer {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<Result<(), TransportError>>>,
    _storage: common::TestStorage,
}

impl TestServer {
    async fn spawn_with_addr(
        addr: SocketAddr,
        data_dir: Option<PathBuf>,
        backend: common::BackendKind,
    ) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        let bound_addr = listener
            .local_addr()
            .expect("failed to read listener address");
        let incoming = TcpIncoming::from_listener(listener, true, None)
            .expect("failed to construct TcpIncoming");

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let storage = common::TestStorage::new(backend);
        let config = RivetConfig::new(
            0,
            bound_addr.to_string(),
            Vec::new(),
            data_dir.or_else(|| storage.data_dir()),
        )
        .with_storage(storage.storage_config());
        let node = Arc::new(
            RivetNode::new(config, storage.storage())
                .await
                .expect("raft bootstrap for test server"),
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

        Ok(Self {
            addr: bound_addr,
            shutdown: Some(shutdown_tx),
            handle: Some(handle),
            _storage: storage,
        })
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
async fn client_commit_round_trip() {
    for backend in common::all_backends() {
        reset_registry().await;
        let mut server = match TestServer::spawn_with_addr(
            "127.0.0.1:0".parse().unwrap(),
            None,
            backend,
        )
        .await
        {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!(
                    "skipping client_commit_round_trip (permission denied binding socket: {err})"
                );
                return;
            }
            Err(err) => panic!("failed to start test server: {err}"),
        };
        let client = RivetClient::connect(ClientConfig::new(server.endpoint()))
            .await
            .expect("client connect");

        let txn = client
            .begin_transaction("commit-test")
            .await
            .expect("begin transaction");
        assert!(
            txn.get("missing").await.expect("get result").is_none(),
            "should not find missing key"
        );

        txn.put("item", b"value".to_vec()).await.expect("put value");
        let receipt = txn.commit().await.expect("commit succeeded");
        assert!(receipt.commit_ts > 0, "commit timestamp set");

        let reader = client
            .begin_transaction("reader")
            .await
            .expect("begin reader transaction");
        let fetched = reader
            .get("item")
            .await
            .expect("reader get")
            .expect("value present");
        assert_eq!(fetched.value, b"value");
        reader.abort().await.expect("abort reader");

        drop(client);
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn client_abort_discards_writes() {
    for backend in common::all_backends() {
        reset_registry().await;
        let mut server = match TestServer::spawn_with_addr(
            "127.0.0.1:0".parse().unwrap(),
            None,
            backend,
        )
        .await
        {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!(
                    "skipping client_abort_discards_writes (permission denied binding socket: {err})"
                );
                return;
            }
            Err(err) => panic!("failed to start test server: {err}"),
        };
        let client = RivetClient::connect(ClientConfig::new(server.endpoint()))
            .await
            .expect("client connect");

        let txn = client
            .begin_transaction("writer")
            .await
            .expect("begin writer");
        txn.put("temp", b"123".to_vec()).await.expect("put value");
        txn.abort().await.expect("abort writer");

        let reader = client
            .begin_transaction("reader")
            .await
            .expect("begin reader tx");
        assert!(
            reader.get("temp").await.expect("reader get").is_none(),
            "aborted write should not be visible"
        );
        reader.abort().await.expect("abort reader");

        drop(client);
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn client_recovers_after_restart() {
    for backend in common::all_backends() {
        reset_registry().await;
        let tmp = tempdir().expect("create temp dir");
        let data_dir = tmp.path().join("node");

        let mut server = match TestServer::spawn_with_addr(
            "127.0.0.1:0".parse().unwrap(),
            Some(data_dir.clone()),
            backend,
        )
        .await
        {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!(
                    "skipping client_recovers_after_restart (permission denied binding socket: {err})"
                );
                return;
            }
            Err(err) => panic!("failed to start test server: {err}"),
        };
        let endpoint = server.endpoint();
        let client = RivetClient::connect(ClientConfig::new(endpoint.clone()))
            .await
            .expect("client connect");

        let writer = client
            .begin_transaction("initial-writer")
            .await
            .expect("begin writer");
        writer
            .put("survive", b"v1".to_vec())
            .await
            .expect("stage value");
        writer.commit().await.expect("commit initial write");
        drop(client);

        server.shutdown().await;
        reset_registry().await;

        let mut server = match TestServer::spawn_with_addr(
            "127.0.0.1:0".parse().unwrap(),
            Some(data_dir),
            backend,
        )
        .await
        {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!(
                    "skipping client_recovers_after_restart (permission denied binding socket: {err})"
                );
                return;
            }
            Err(err) => panic!("failed to restart test server: {err}"),
        };
        let client = RivetClient::connect(ClientConfig::new(server.endpoint()))
            .await
            .expect("client reconnect");

        let reader = client
            .begin_transaction("reader")
            .await
            .expect("begin reader");
        let value = reader
            .get("survive")
            .await
            .expect("read result")
            .expect("value present after restart");
        assert_eq!(value.value, b"v1");
        reader.abort().await.expect("abort reader");

        let writer = client
            .begin_transaction("writer-2")
            .await
            .expect("begin writer 2");
        writer
            .put("survive", b"v2".to_vec())
            .await
            .expect("stage new value");
        writer.commit().await.expect("commit second value");

        drop(client);
        server.shutdown().await;
    }
}
