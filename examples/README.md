# RivetDB Examples

`example` connects to a running cluster, reads a key (printing a miss if it does not exist), writes a new value, commits, and then opens a fresh transaction to read the same key.

Run it against any node (leader or follower) in your cluster:

```bash
cargo run --manifest-path examples/Cargo.toml --bin example -- \
  --endpoint http://127.0.0.1:50051 \
  --key demo-key \
  --value "hello from example"
```

Cluster startup commands live in the top-level `README.md`; once a node is listening, point the example at its gRPC address via `--endpoint`.
