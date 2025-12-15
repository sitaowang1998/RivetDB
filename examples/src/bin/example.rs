use clap::Parser;
use rivetdb::{ClientConfig, RivetClient};

/// Minimal client that reads, writes, and re-reads a key against a RivetDB cluster.
#[derive(Parser, Debug)]
#[command(name = "example", about = "Example RivetDB client")]
struct Args {
    /// gRPC endpoint of any node in the cluster (leader or follower).
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    /// Client identifier used in begin_transaction.
    #[arg(long, default_value = "example-client")]
    client_id: String,

    /// Key to read and overwrite.
    #[arg(long, default_value = "example-key")]
    key: String,

    /// Value to write for the provided key.
    #[arg(long, default_value = "hello-from-rivet-example")]
    value: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let client = RivetClient::connect(ClientConfig::new(&args.endpoint)).await?;
    println!("connected to {}", args.endpoint);

    let txn = client.begin_transaction(&args.client_id).await?;
    println!(
        "transaction {} started at snapshot_ts {}",
        txn.id(),
        txn.snapshot_ts()
    );

    let existing = txn.get(&args.key).await?;
    match existing {
        Some(result) => println!(
            "found existing value for '{}': '{}' (commit_ts {})",
            args.key,
            String::from_utf8_lossy(&result.value),
            result.commit_ts
        ),
        None => println!("no value found for '{}'", args.key),
    }

    txn.put(&args.key, args.value.clone().into_bytes()).await?;
    let receipt = txn.commit().await?;
    println!(
        "wrote '{}' for '{}' (commit_ts {})",
        args.value, args.key, receipt.commit_ts
    );

    let verify_txn = client
        .begin_transaction(format!("{}-verify", args.client_id))
        .await?;
    let confirmed = verify_txn.get(&args.key).await?;
    match confirmed {
        Some(result) => println!(
            "after commit, read '{}': '{}' (commit_ts {})",
            args.key,
            String::from_utf8_lossy(&result.value),
            result.commit_ts
        ),
        None => println!(
            "after commit, key '{}' still missing (likely commit failed)",
            args.key
        ),
    }

    Ok(())
}
