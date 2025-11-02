use std::net::SocketAddr;
use std::sync::Arc;

use rivetdb::rpc::server::RivetKvService;
use rivetdb::rpc::service::rivet_kv_server::RivetKvServer;
use rivetdb::storage::InMemoryStorage;
use rivetdb::{ClientConfig, RivetClient, RivetConfig, RivetNode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Error as TransportError, Server};

struct TestServer {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<Result<(), TransportError>>>,
}

impl TestServer {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind test listener");
        let addr = listener
            .local_addr()
            .expect("failed to read listener address");
        let incoming = TcpIncoming::from_listener(listener, true, None)
            .expect("failed to construct TcpIncoming");

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let storage = Arc::new(InMemoryStorage::new());
        let config = RivetConfig::new(0, addr.to_string(), Vec::new(), None);
        let node = Arc::new(RivetNode::new(config, storage));
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
            addr,
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
async fn client_commit_round_trip() {
    let mut server = TestServer::spawn().await;
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

#[tokio::test(flavor = "multi_thread")]
async fn client_abort_discards_writes() {
    let mut server = TestServer::spawn().await;
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
