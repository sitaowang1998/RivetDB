# RivetDB Client Library

The Rust client library provides an asynchronous interface for interacting with a RivetDB node over gRPC. It wraps the generated Tonic client with ergonomic helpers for transaction management and error handling.

## Connecting to a Node

```rust
use rivetdb::{ClientConfig, RivetClient};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let client =
    RivetClient::connect(ClientConfig::new("http://127.0.0.1:50051")).await?;
# Ok(())
# }
```

## Transaction Lifecycle

```rust
use rivetdb::{ClientConfig, RivetClient};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let client = RivetClient::connect(ClientConfig::new("http://127.0.0.1:50051")).await?;

let txn = client.begin_transaction("example-client").await?;
txn.put("greeting", b"hello".to_vec()).await?;
let receipt = txn.commit().await?;

println!("commit timestamp: {}", receipt.commit_ts);
# Ok(())
# }
```

If you need to roll back staged changes, call `abort()` on the transaction instead of `commit()`.

## Error Handling

`RivetClient` operations return `Result<_, ClientError>`. Distinguish transport or RPC issues from logical failures (for example, validation conflicts) via pattern matching:

```rust
match txn.commit().await {
    Ok(receipt) => println!("committed at {}", receipt.commit_ts),
    Err(rivetdb::ClientError::OperationFailed { message, .. }) => {
        eprintln!("transaction failed: {message}");
    }
    Err(other) => return Err(other.into()),
}
```
